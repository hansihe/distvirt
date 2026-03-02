# Networking Fabric

## Current State

**Phases 1, 2, 3 (route table + frame buffering), and 4 (service entities) are implemented.** The fabric is a per-namespace userspace L2 switch with a smoltcp-based IP gateway providing ARP, DNS service discovery, and internet egress via TUN+NAT. Each worker creates one fabric instance per namespace. Pod TAP devices are added as ports on the switch. The fabric includes a route table for destinations that aren't local — frames to placeholder destinations are buffered per policy, and route miss events propagate to the orchestrator for scale-to-zero activation. Fabric-level service entities provide readiness gating: traffic to service IPs is buffered until the orchestrator signals readiness, at which point frames are flushed to the backing pod.

**Note**: Direct pod-to-pod route table buffering (via `add_port_with_ip`) still flushes immediately when the TAP port is added, without readiness gating. Service entities are the recommended path for inter-service communication where readiness gating matters.

---

## Architecture

```
┌─────────────────────────────────────────────────┐
│                  Fabric (L2 switch)              │
│  Owns ports, MAC table, frame forwarding         │
├─────────────────────────────────────────────────┤
│              FabricGateway (smoltcp)             │
│  ARP · DNS (service registry) · TUN egress/NAT  │
├─────────────────────────────────────────────────┤
│              Port abstraction                    │
│  Each port = AF_PACKET socket on TAP, via        │
│  tokio AsyncFd. Per-port read task.              │
└─────────────────────────────────────────────────┘
```

The fabric is **decoupled from VMM and container code**. It only knows about ports (L2 frame sources/sinks). The worker is the glue — it creates namespaces, launches VMs, and hands TAP devices to the fabric.

---

## Modules (`distvirt-worker/src/fabric/`)

### `port.rs` — Async L2 port

`FramePort` trait: async `recv_frame()` and `send_frame()` abstraction. `Port` struct wraps an AF_PACKET socket fd in tokio `AsyncFd` via `dup()` — the original fd stays in `TapDevice` (owns Drop cleanup for the TAP device), the dup'd fd goes into AsyncFd. Both set `O_NONBLOCK`.

### `switch.rs` — MAC table + Ethernet parsing

`MacTable`: `HashMap<[u8; 6], PortId>` for MAC→port lookups. `learn()` updates entries from frame source MACs (ignores broadcast/multicast). Helper functions for Ethernet header parsing, broadcast detection, MAC formatting. `extract_ipv4_dst()` parses the destination IPv4 address from an Ethernet frame (checks ethertype=0x0800, reads IP header bytes 16-19).

Constants: `GATEWAY_IP = [172, 16, 0, 1]`, `GATEWAY_MAC = [0x02, 0x00, 0x00, 0x00, 0x00, 0x01]`.

### `route.rs` — Route table + frame buffering

`RouteTable`: `HashMap<Ipv4Addr, RouteState>` mapping destination IPs to route entries with optional frame buffers. Each entry contains a `FabricRouteEntry` (from protocol types), a `VecDeque<Vec<u8>>` frame buffer, and a buffer start time for timeout tracking. Per-IP debounce tracking (default 1s window) prevents route miss event floods.

`RouteAction` enum: `Buffered` (frame accepted into buffer), `Drop` (policy says drop), `RemoteWorker { worker_id }` (stub for multi-worker, log + drop), `NoRoute` (no entry, caller should flood).

Key methods:
- `sync(entries)` — full replacement of route table
- `update(added, removed_ips)` — incremental delta
- `lookup_and_buffer(dst_ip, frame) -> (RouteAction, bool)` — returns action + whether miss event should fire (respecting debounce). Handles buffer limits and timeout expiry.
- `flush_buffer(ip) -> Vec<Vec<u8>>` — drains buffered frames (called when pod activates)

### `service.rs` — Service entities + service table

`ServiceEntity`: holds service_id, virtual IP/MAC, `ServicePolicy` (buffer_frames, timeout_ms), optional backend (pod IP/MAC), readiness state, and a `VecDeque<Vec<u8>>` frame buffer with timeout tracking.

`ServiceTable`: `HashMap<Ipv4Addr, ServiceEntity>` for fast frame-path lookup, plus `HashMap<String, Ipv4Addr>` for command dispatch by service_id. Per-IP activation debounce (default 1s window) prevents activation event floods.

`ServiceAction` enum: `Forward { pod_ip, pod_mac }` (service is ready, forward to backend), `Buffered` (frame accepted into buffer), `Drop` (buffer full or timed out).

Key methods:
- `create(service_id, ip, mac, policy)` — register a new service entity
- `destroy(service_id)` — remove a service entity
- `update_backend(service_id, backend)` — assign or remove backing pod; clears readiness and resets buffer
- `mark_ready(service_id) -> Option<(Vec<Vec<u8>>, [u8; 6])>` — mark ready, returns buffered frames + backend MAC for flushing
- `lookup_and_buffer(dst_ip, frame) -> Option<(ServiceAction, bool)>` — `None` if not a service IP; `bool` is `should_activate` (debounced)
- `get_mac(ip)` — returns service MAC for ARP replies

### `mod.rs` — `Fabric` struct + port read loops

Owns ports (`Arc<Mutex<HashMap<PortId, SharedPort>>>`), MAC table (`Arc<Mutex<MacTable>>`), route table (`Arc<Mutex<RouteTable>>`), and service table (`Arc<Mutex<ServiceTable>>`). Each port spawns its own tokio task for frame reading and forwarding. Shared state locks held only briefly for lookups. Lock ordering: never hold service_table while acquiring mac_table; release service_table first, then lock mac_table for forwarding.

`FabricEvent` enum:
- `RouteMiss { dst_ip, dst_mac }` — frame hit a placeholder route or unknown pod IP
- `ServiceActivation { service_id, dst_ip }` — frame hit a service with no ready backend

Both forwarded to worker via an optional `mpsc::Sender<FabricEvent>` set with `set_event_channel(tx)`. Uses `try_send` — these are hints, silent drop under backpressure is acceptable.

Frame forwarding logic:
- **Gateway MAC destination**: send only to gateway channel
- **Broadcast/multicast**: flood to all ports + gateway. Additionally, ARP requests for service IPs receive a synthetic ARP reply (`try_service_arp_reply`), making service IPs ARP-resolvable.
- **Known unicast**: MAC lookup → direct forward to port
- **Unknown unicast (IPv4, service IP)**: consult service table → forward (rewrite dst MAC to backend pod MAC) / buffer / drop. Emit `ServiceActivation` if no ready backend (debounced).
- **Unknown unicast (IPv4 with route entry)**: consult route table → buffer/drop/remote-stub per policy
- **Unknown unicast (IPv4, no route entry)**: flood to all other ports (preserves existing behavior)
- **Unknown unicast (non-IPv4)**: flood to all other ports
- **No loopback**: never sends frame back to source port

`flush_service_frames(frames, backend_mac)` — looks up backend MAC in mac_table to find port, rewrites dst MAC in each frame, sends via spawned async task.

Port lifecycle: `add_port(TapDevice) -> (PortId, TaskHandle)`, `add_port_with_ip(TapDevice, Ipv4Addr)` — the latter flushes route-table-buffered frames immediately to the new port via a spawned async task. For service-backed traffic, flushing is gated on `ServiceReady` instead. `PortGuard` provides RAII cleanup — automatically removes port from map when the port task exits or panics.

Gateway connection: `set_gateway(egress_tx, ingress_rx)` — bidirectional channel pair. Separate ingress task reads frames from gateway and injects them back into the switch for forwarding. Both port read loops and gateway ingress task are service-table-aware and route-aware.

All frames include 10-byte vhost VNET header before the Ethernet frame.

### `gateway.rs` — FabricGateway (smoltcp IP stack)

The gateway provides L3 services for the pod subnet:

**smoltcp interface** with two IPs:
- `172.16.0.1/24` — external gateway (for host TUN egress)
- Pod subnet gateway IP (configurable per namespace)

**ARP**: smoltcp handles ARP for the gateway MAC (`02:00:00:00:00:01`).

**DNS server** (UDP port 53):
- Queries checked against local `DnsRegistry` (service name → IP)
- Local hits: synthesize A-record response (TTL=60)
- Misses: forward to upstream DNS (query-ID tracking for response routing)

**Internet egress via TUN**:
- IP packets destined outside the pod subnet are forwarded to a TUN device
- Egress: strip Ethernet header, write IP packet + vnet header to TUN
- Ingress: read from TUN, lookup destination MAC in `ip_mac_table`, rebuild Ethernet header, inject back to fabric
- `ip_mac_table` (IP→MAC) learned from egress frame sources

**Checksum handling**: Adjusts vnet header checksum offsets when adding/removing Ethernet headers (virtio-net offload).

### `dns.rs` — DNS query parsing + response synthesis

`DnsRegistry`: `Arc<RwLock<HashMap<String, Ipv4Addr>>>`. Synced from orchestrator via `RegistrySync`/`RegistryUpdate` commands.

`parse_qname()`: extracts domain name from wire-format DNS query. `synthesize_a_response()`: builds minimal A-record response. Case-insensitive matching. Rejects compression pointers.

### `tun.rs` — TUN device creation + async I/O

Opens `/dev/net/tun` with `IFF_TUN | IFF_NO_PI | IFF_VNET_HDR`, enables `TUN_F_CSUM` offload. IP address configured via ioctls. Async read/write via tokio AsyncFd.

### `tap.rs` — TAP device creation

`create_persistent_tap()`: creates TAP device that survives fd closure (TUNSETPERSIST). `open_packet_socket()`: opens AF_PACKET socket bound to the TAP interface with `PACKET_VNET_HDR`. `TapDevice` struct with Drop impl for cleanup.

---

## Integration with Worker

**Namespace creation** (`worker.rs:handle_create_namespace`):
1. Create `Fabric` instance
2. Create fabric event channel, call `fabric.set_event_channel(tx)`
3. Get `route_table` and `service_table` references from fabric
4. Create `DnsRegistry` (shared `Arc<RwLock<HashMap>>`)
5. Spawn `FabricGateway` as background tokio task
6. Connect fabric ↔ gateway via channel pair
7. Spawn event bridge task: maps `FabricEvent::RouteMiss` → `WorkerEvent::FabricRouteMiss` and `FabricEvent::ServiceActivation` → `WorkerEvent::ServiceActivation`, forwards to `bg_event_tx`
8. Store in `NamespaceState` (includes `route_table`, `service_table`, and `_event_bridge_task`)

**Service management** (`worker.rs`):
- `handle_create_service` — locks service table, calls `create(service_id, ip, mac, policy)`
- `handle_update_service_backend` — locks service table, calls `update_backend(service_id, backend)`
- `handle_service_ready` — locks service table, calls `mark_ready(service_id)` to get buffered frames + backend MAC; if frames, calls `fabric.flush_service_frames(frames, backend_mac)` to deliver them
- `handle_destroy_service` — locks service table, calls `destroy(service_id)`

**Route management** (`worker.rs`):
- `handle_fabric_route_sync` — locks route table, calls `sync(routes)` for full replacement
- `handle_fabric_route_update` — locks route table, calls `update(added, removed_ips)` for incremental delta

**Pod launch** (`worker.rs:pod_launch`):
1. VM launches with TAP device (virtio-net, vhost-net backend)
2. `take_tap()` transfers TAP ownership from VM to fabric
3. `fabric.add_port_with_ip(tap, network.ip)` — flushes any route-table-buffered frames for this IP, then starts port forwarding task
4. Guest configures interface via `ConfigureNetwork` command (IP/netmask/gateway)

**Pod shutdown**: Port task cleaned up automatically via RAII when supervisor exits.

---

## Service Entities

Services are first-class network entities on the fabric with their own virtual IP and MAC, separate from backing pod IPs. A service entity is the boundary at which application-level traffic management happens — buffering, activation, and readiness gating all live here rather than on the pod directly. See `docs/worker-protocol.md` for the full protocol design.

```
Client pod → Service IP (virtual) → [buffer / activate / ready?] → Pod IP (real)
```

**Why this separation matters**:
- **Clean lifecycle boundary**: Pod lifecycle (VM booted, network configured) is distinct from service readiness (application listening, health check passed).
- **Readiness gating**: Buffered frames are only flushed to the backing pod once the orchestrator sends `ServiceReady`, not immediately at port-add time.
- **Flexibility**: Multiple services can back the same pod. Scale-to-zero is "no backing pod assigned to service IP" rather than "pod doesn't exist."
- **Protocol activators (future)**: Protocol-aware logic (TCP SYN detection, HTTP/2 stream parsing) runs on the service entity where the expected protocols are declared.

**Service states**: No backend (buffering + activation event) → Backend assigned, not ready (buffering) → Ready (traffic flows through). The orchestrator drives transitions via `CreateService`, `UpdateServiceBackend`, `ServiceReady`, `DestroyService` commands.

**Coexistence with pod routes**: Pods remain directly addressable by IP. The existing route table with placeholder entries provides basic best-effort buffering for direct pod-to-pod traffic. Services get the rich activation path; pod routes preserve the flat L2 network illusion. Traffic resolution order: local TAP → service entity → route table → flood.

**ARP resolution**: Service IPs are ARP-resolvable. When a broadcast ARP request targets a service IP, the fabric constructs an ARP reply with the service's virtual MAC and sends it back to the requesting port. This allows guest network stacks to resolve service IPs without any special configuration.

**IP allocation in compose mode**: Each service gets two IPs from the namespace subnet — a service IP (virtual, used for DNS and the service entity) and a pod IP (assigned to the VM network interface). Service IPs are allocated first (.2 to .N+1), pod IPs after (.N+2 to .2N+1). This limits compose deployments to 126 services per namespace (using the default /24 subnet).

**Compose orchestration flow**: On `CreateNamespace`, the orchestrator sends `CreateService` for each planned service. DNS entries map names to service IPs. On `PodRunning`, the orchestrator sends `UpdateServiceBackend` (with pod IP/MAC) followed by `ServiceReady` to flush buffered frames and enable traffic flow.

---

## Future Work

### Protocol Activators

Protocol-aware activation and traffic management for service entities, implemented as WASM components loaded at runtime. TCP-level activation (SYN detection, flow tracking) and HTTP/2 stream-level activation (per-request activation on multiplexed connections) are the primary targets.

See **[Protocol Activators](protocol-activators.md)** for the full design.

### Ingress Adapters

External access into the fabric for developer access, shareable URLs, and infrastructure integration. Adapters are worker-level components that demultiplex to per-namespace virtual ports on the fabric. WireGuard (via boringtun) is the primary strategy for staging environments.

See **[Ingress Adapters](ingress-adapters.md)** for the full design.

### Multi-Worker Tunneling

For distributed mode, fabric segments on different workers need to communicate:
- Tunnel ports connecting fabric segments across workers (e.g. VXLAN, or custom over TCP/TLS)
- Orchestrator pushes routing table updates so each worker knows which MACs/IPs are remote
- Frames for remote destinations forwarded through tunnel to the appropriate worker
