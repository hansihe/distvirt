# Networking Fabric

## Current State

**Phases 1, 2, and 3 (route table + frame buffering) are implemented.** The fabric is a per-namespace userspace L2 switch with a smoltcp-based IP gateway providing ARP, DNS service discovery, and internet egress via TUN+NAT. Each worker creates one fabric instance per namespace. Pod TAP devices are added as ports on the switch. The fabric includes a route table for destinations that aren't local — frames to placeholder destinations are buffered per policy, and route miss events propagate to the orchestrator for scale-to-zero activation.

**Known limitation**: Buffered frames are currently flushed as soon as the pod's TAP port is added to the fabric, before the guest has configured its network or the application has started listening. This needs a readiness gate — see Service Network Entities below.

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

### `mod.rs` — `Fabric` struct + port read loops

Owns ports (`Arc<Mutex<HashMap<PortId, SharedPort>>>`), MAC table (`Arc<Mutex<MacTable>>`), and route table (`Arc<Mutex<RouteTable>>`). Each port spawns its own tokio task for frame reading and forwarding. Shared state locks held only briefly for lookups. Lock ordering: mac_table always acquired before route_table.

`FabricEvent::RouteMiss { dst_ip, dst_mac }` — fabric-internal event emitted when frames hit placeholder routes. Forwarded to worker via an optional `mpsc::Sender<FabricEvent>` set with `set_event_channel(tx)`. Uses `try_send` — misses are hints, silent drop under backpressure is acceptable.

Frame forwarding logic:
- **Gateway MAC destination**: send only to gateway channel
- **Broadcast/multicast**: flood to all ports + gateway
- **Known unicast**: MAC lookup → direct forward to port
- **Unknown unicast (IPv4 with route entry)**: consult route table → buffer/drop/remote-stub per policy
- **Unknown unicast (IPv4, no route entry)**: flood to all other ports (preserves existing behavior)
- **Unknown unicast (non-IPv4)**: flood to all other ports
- **No loopback**: never sends frame back to source port

Port lifecycle: `add_port(TapDevice) -> (PortId, TaskHandle)`, `add_port_with_ip(TapDevice, Ipv4Addr)` — the latter currently flushes buffered frames immediately to the new port via a spawned async task (this will move to service entities once implemented). `PortGuard` provides RAII cleanup — automatically removes port from map when the port task exits or panics.

Gateway connection: `set_gateway(egress_tx, ingress_rx)` — bidirectional channel pair. Separate ingress task reads frames from gateway and injects them back into the switch for forwarding. Both port read loops and gateway ingress task are route-aware.

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
3. Get `route_table` reference from fabric
4. Create `DnsRegistry` (shared `Arc<RwLock<HashMap>>`)
5. Spawn `FabricGateway` as background tokio task
6. Connect fabric ↔ gateway via channel pair
7. Spawn event bridge task: maps `FabricEvent::RouteMiss` → `WorkerEvent::FabricRouteMiss` and forwards to `bg_event_tx`
8. Store in `NamespaceState` (includes `route_table` and `_event_bridge_task`)

**Route management** (`worker.rs`):
- `handle_fabric_route_sync` — locks route table, calls `sync(routes)` for full replacement
- `handle_fabric_route_update` — locks route table, calls `update(added, removed_ips)` for incremental delta

**Pod launch** (`worker.rs:pod_launch`):
1. VM launches with TAP device (virtio-net, vhost-net backend)
2. `take_tap()` transfers TAP ownership from VM to fabric
3. `fabric.add_port_with_ip(tap, network.ip)` — flushes any buffered frames for this IP, then starts port forwarding task
4. Guest configures interface via `ConfigureNetwork` command (IP/netmask/gateway)

**Pod shutdown**: Port task cleaned up automatically via RAII when supervisor exits.

---

## Service Entities (Next)

Services are first-class network entities on the fabric with their own virtual IP and MAC, separate from backing pod IPs. A service entity is the boundary at which application-level traffic management happens — buffering, activation, and readiness gating all live here rather than on the pod directly. See `docs/worker-protocol.md` for the full protocol design.

```
Client pod → Service IP (virtual) → [buffer / activate / ready?] → Pod IP (real)
```

**Why this separation matters**:
- **Clean lifecycle boundary**: Pod lifecycle (VM booted, network configured) is distinct from service readiness (application listening, health check passed). The current model conflates them.
- **Readiness gating**: Buffered frames are only flushed to the backing pod once the orchestrator sends `ServiceReady`. This replaces the current behavior where flush happens immediately at port-add time.
- **Flexibility**: Multiple services can back the same pod. Scale-to-zero is "no backing pod assigned to service IP" rather than "pod doesn't exist."
- **Protocol activators (future)**: Protocol-aware logic (TCP SYN detection, HTTP/2 stream parsing) runs on the service entity where the expected protocols are declared.

**Service states**: No backend (buffering + activation event) → Backend assigned, not ready (buffering) → Ready (traffic flows through). The orchestrator drives transitions via `CreateService`, `UpdateServiceBackend`, `ServiceReady`, `DestroyService` commands.

**Coexistence with pod routes**: Pods remain directly addressable by IP. The existing route table with placeholder entries provides basic best-effort buffering for direct pod-to-pod traffic. Services get the rich activation path; pod routes preserve the flat L2 network illusion. Traffic resolution order: local TAP → service entity → route table → flood.

**How this relates to existing code**: The route table and frame buffering from Phase 3 are the foundation. Service entities are a new construct alongside the route table (not a replacement). The `FabricEvent::RouteMiss` mechanism splits into two: `ServiceActivation` for service IPs, `FabricRouteMiss` for pod IPs. The existing `add_port_with_ip` flush-on-add behavior will be replaced by service-driven readiness gating for service-backed pods.

### What Needs to Change

- **New `service.rs` module**: Service entity struct holding IP/MAC, policy, backend binding, readiness state, and frame buffer. Keyed by service ID, stored in fabric.
- **Frame forwarding in `mod.rs`**: Before consulting the route table for unknown unicast, check if the destination IP matches a service entity. If so, delegate to the service entity (buffer/activate/forward).
- **New `FabricEvent::ServiceActivation`**: Emitted when traffic hits a service with no ready backend. Mapped to `WorkerEvent::ServiceActivation` by the event bridge.
- **Worker command handlers**: `handle_create_service`, `handle_update_service_backend`, `handle_service_ready`, `handle_destroy_service` in `worker.rs`.
- **Orchestrator changes**: Compose orchestrator creates services, assigns backends, signals readiness.

---

## Future Work

### Protocol Activators

Protocol activators live on service entities and provide protocol-aware activation and traffic management. They are configured per-service based on the declared application protocols. This is additive — basic frame buffering works first, protocol awareness layers on top.

#### TCP-Level Activation

- SYN detection on the service IP triggers activation events
- Frames buffered at the service entity during pod boot
- Flushed to backing pod once readiness gate passes
- Non-TCP traffic to a TCP-only service can be dropped

#### HTTP/2 Activation

A single H2 connection multiplexes many requests. Activating on TCP means waking the VM for every new connection. Per-stream activation means the VM only wakes when an actual request arrives.

Requirements:
1. Track TCP state and reassemble the stream at the service entity
2. Parse H2 frame headers (9 bytes each) — only needs HEADERS frame detection
3. Maintain the H2 connection to the client (respond to SETTINGS, PING, WINDOW_UPDATE)
4. On new stream (HEADERS frame): emit activation event, buffer the frame
5. Once pod is ready: replay buffered frames or splice the connection through

### Multi-Worker Tunneling

For distributed mode, fabric segments on different workers need to communicate:
- Tunnel ports connecting fabric segments across workers (e.g. VXLAN, or custom over TCP/TLS)
- Orchestrator pushes routing table updates so each worker knows which MACs/IPs are remote
- Frames for remote destinations forwarded through tunnel to the appropriate worker
