# Networking Fabric

## Current State

**Phases 1, 2, 3 (route table + frame buffering), and 4 (service entities) are implemented.** The fabric is a per-namespace userspace L2 switch with a smoltcp-based IP gateway providing ARP, DNS service discovery, and internet egress via TUN+NAT. Each worker creates one fabric instance per namespace. Pod TAP devices are added as ports on the switch. The fabric includes a route table for destinations that aren't local — frames to placeholder destinations are buffered per policy, and route miss events propagate to the orchestrator for scale-to-zero activation. Fabric-level service entities provide readiness gating: traffic to service IPs is buffered until the orchestrator signals readiness, at which point frames are flushed to the backing pod. Protocol activators (WASM components) optionally provide protocol-aware activation on service entities — see [Protocol Activators](protocol-activators.md).

**Note**: Direct pod-to-pod route table buffering (via `add_port_with_ip`) still flushes immediately when the TAP port is added, without readiness gating. Service entities are the recommended path for inter-service communication where readiness gating matters.

---

## Architecture

```
┌─────────────────────────────────────────────────┐
│                  Fabric (L2 switch)              │
│  Owns ports, MAC table, NAT table,              │
│  frame forwarding (DNAT/SNAT for services)      │
├─────────────────────────────────────────────────┤
│              FabricGateway (smoltcp)             │
│  ARP · DNS (service registry) · TUN egress/NAT  │
├─────────────────────────────────────────────────┤
│              Port abstraction                    │
│  TAP (AF_PACKET + AsyncFd) or Virtual (channel) │
│  Per-port read task.                             │
└─────────────────────────────────────────────────┘
```

The fabric is **decoupled from VMM and container code**. It only knows about ports (L2 frame sources/sinks). The worker is the glue — it creates namespaces, launches VMs, and hands TAP devices to the fabric.

---

## Modules (`distvirt-worker/src/fabric/`)

### `port.rs` — Async L2 port

`FramePort` trait: async `recv_frame()` and `send_frame()` abstraction.

`Port` struct wraps an AF_PACKET socket fd in tokio `AsyncFd` via `dup()` — the original fd stays in `TapDevice` (owns Drop cleanup for the TAP device), the dup'd fd goes into AsyncFd. Both set `O_NONBLOCK`.

`ChannelPort` struct wraps `mpsc` channels for virtual (non-TAP) ports — used by ingress adapters (e.g. WireGuard). Returns `(port, adapter_tx, adapter_rx)` for bidirectional communication.

`FabricPort` enum dispatches between `Tap(Port)` and `Virtual(ChannelPort)`, implementing `FramePort` for both.

### `switch.rs` — MAC table + Ethernet parsing

`MacTable`: `HashMap<[u8; 6], (PortId, Instant)>` for MAC→port lookups with timestamps. `learn()` updates entries from frame source MACs (ignores broadcast/multicast). `gc(max_age)` removes stale entries — runs every 60 seconds after gateway is connected. Helper functions for Ethernet header parsing, broadcast detection, MAC formatting. `extract_ipv4_dst()` parses the destination IPv4 address from an Ethernet frame (checks ethertype=0x0800, reads IP header bytes 16-19).

Constants: `GATEWAY_MAC = [0x02, 0x00, 0x00, 0xff, 0xff, 0x01]`. The gateway IP is configurable per namespace (passed to `FabricGateway`).

### `route.rs` — Route table + frame buffering

`RouteTable`: `HashMap<Ipv4Addr, RouteState>` mapping destination IPs to route entries with optional frame buffers. Each entry contains a `FabricRouteEntry` (from protocol types), a `VecDeque<Vec<u8>>` frame buffer, and a buffer start time for timeout tracking. Per-IP debounce tracking (default 1s window) prevents route miss event floods.

`RouteAction` enum: `Buffered` (frame accepted into buffer), `Drop` (policy says drop), `RemoteWorker { worker_id }` (stub for multi-worker, log + drop), `NoRoute` (no entry, caller should flood).

Key methods:
- `sync(entries)` — full replacement of route table
- `update(added, removed_ips)` — incremental delta
- `lookup_and_buffer(dst_ip, frame) -> (RouteAction, bool)` — returns action + whether miss event should fire (respecting debounce). Handles buffer limits and timeout expiry.
- `flush_buffer(ip) -> Vec<Vec<u8>>` — drains buffered frames (called when pod activates)

### `service.rs` — Service entities + service table

`ServiceEntity`: holds service_id, virtual IP/MAC, `ServicePolicy` (buffer_frames, timeout_ms, optional `ActivatorConfig`), separate `backend_ip: Option<Ipv4Addr>` and `backend_mac: Option<[u8; 6]>`, readiness state, a `VecDeque<Vec<u8>>` frame buffer with timeout tracking, and a `ServiceProcessor` (Passthrough/L3/L4) for protocol-aware activation.

`ServiceTable`: `HashMap<Ipv4Addr, ServiceEntity>` for fast frame-path lookup, plus `HashMap<String, Ipv4Addr>` for command dispatch by service_id. Per-IP activation debounce (default 1s window) prevents activation event floods.

`ServiceAction` enum:
- `Forward { pod_ip, pod_mac, service_ip, service_mac }` — service is ready, forward to backend (caller applies DNAT)
- `Buffered` — frame accepted into buffer
- `Drop` — buffer full or timed out
- `ActivatorActions { actions, service_id }` — L3 activator processed the frame, returned actions for fabric to execute
- `L4Result { actions, frames, service_id, poll_delay }` — L4 stream manager produced outgoing frames + non-L4 actions

`MarkReadyResult` enum:
- `Passthrough { frames, backend_mac, backend_ip, service_ip, service_mac, actions }` — buffered frames + backend info + activator actions
- `L4(ServiceAction)` — L4 stream mode result

Key methods:
- `create(service_id, ip, mac, policy, processor)` — register a new service entity with its processor mode
- `destroy(service_id)` — remove a service entity
- `update_backend(service_id, Option<(Ipv4Addr, [u8; 6])>)` — assign or remove backing pod; clears readiness. Preserves buffer on first backend assignment (None → Some); clears buffer when backend removed or MAC changes.
- `mark_ready(service_id) -> Option<MarkReadyResult>` — mark ready; pushes `BackendAvailable(true)` to activator if present, returns buffered frames + actions (Passthrough) or L4 result
- `lookup_and_buffer(dst_ip, frame, is_reachable) -> Option<(ServiceAction, bool)>` — `None` if not a service IP; `bool` is `should_activate` (debounced). Delegates to `ServiceProcessor` for L3/L4 paths.
- `get_mac(ip)` — returns service MAC for ARP replies
- `get_service_id(ip)` — returns service_id for activation events
- `get_nat_info_by_id(service_id)` — returns `(service_ip, service_mac, backend_ip, backend_mac)` for NAT setup
- `flush_by_backend_mac(mac) -> Vec<ServiceFlushData>` — drain buffers for ready services matching a backend MAC (called when a port is added)

### `service_activator.rs` — Service processor (activator integration)

`ServiceProcessor` enum determines how a service entity processes incoming frames:
- `Passthrough` — no activator, pure buffer/forward (default for services with no protocol declaration)
- `L3 { activator: ActivatorInstance, flow_tracker: FlowTracker }` — L3 packet-level processing via WASM activator. `FlowTracker` assigns stable flow IDs by 5-tuple for `packet-flow` handles.
- `L4 { activator: Option<ActivatorInstance>, stream_manager: StreamManager }` — L4 stream-level processing. The `StreamManager` (smoltcp-backed TCP stack) handles TCP connection management; the activator operates on byte streams.

Key methods:
- `process_frame(service_id, eth_payload, raw_frame) -> Option<ServiceAction>` — parses frame, delegates to L3 (packet event) or L4 (stream manager)
- `on_mark_ready(service_id) -> Option<ServiceAction>` — pushes `BackendAvailable(true)` event, processes activator response
- `on_backend_update(has_backend, backend_ip, backend_mac)` — pushes `BackendAvailable` event, updates stream manager
- `handle_timeout(service_id) -> Option<ServiceAction>` — L4 only: polls stream manager for TCP timeouts

For L4 mode, a bounded event loop (4 rounds max) separates L4 actions (executed by the stream manager) from non-L4 actions (returned to the fabric for dispatch).

### `nat.rs` — NAT connection tracking

`NatTable`: `HashMap<NatFlowKey, NatEntry>` for reverse-direction NAT lookup. Used for service DNAT/SNAT — when a frame is forwarded from a service IP to a backend pod IP, a reverse NAT entry is inserted so return traffic from the backend can be SNATted back to the service IP.

`NatFlowKey`: 5-tuple `(src_ip, dst_ip, protocol, src_port, dst_port)`. `NatEntry`: `(service_ip, service_mac, backend_ip, last_seen)`.

Key methods:
- `insert(key, entry)` — insert reverse-direction entry (key is `(backend_ip, client_ip, proto, service_port, client_port)`)
- `lookup(key) -> Option<&NatEntry>` — look up and update `last_seen`
- `gc(max_age)` — remove stale entries; runs every 60 seconds alongside MAC table GC

### `forwarding.rs` — Frame dispatch + action execution

Contains the core frame forwarding logic, extracted from `mod.rs`.

`FabricContextInner` struct: shared forwarding state holding `ports`, `mac_table`, `route_table`, `service_table`, `nat_table`, `gateway_tx`, and `event_tx`. Wrapped in `Arc` for sharing across port read tasks.

`dispatch_frame(frame, source, ctx)` — the main forwarding function called by every port read loop and the gateway ingress task:
1. Parse frame, learn source MAC (port source only)
2. Broadcast/multicast → flood to all ports + gateway; check for service ARP requests
3. Gateway MAC destination → send to gateway channel
4. Known unicast → MAC table lookup. If hit: check NAT table for return traffic SNAT (rewrite src MAC + src IP), then forward. Loopback avoidance (never send back to source port).
5. Unknown unicast → `handle_unknown_unicast`

`handle_unknown_unicast` — resolution order for IPv4 frames:
- **Service table**: consult first. If ready + reachable → DNAT (rewrite dst IP from service_ip to backend_ip) + insert reverse NAT entry + forward. If activator → dispatch actions (ReplayPacket, SetBackendNeed, Log). If L4 → dispatch frames + set poll timer.
- **Route table**: buffer/drop per policy, emit `RouteMiss` event if debounce allows
- **No match**: flood to all other ports
- Non-IPv4 unknown unicast: flood to all other ports

`dispatch_action(action, service_id, dst_ip, ctx)` — executes activator actions:
- `ReplayPacket` — DNAT to backend, insert reverse NAT entry, send to backend port
- `SetBackendNeed` — emit `ServiceBackendNeed` event
- `Log` — log at appropriate level

Port read loop: spawns per-port, reads frames in a loop, calls `dispatch_frame`. `PortGuard` provides RAII cleanup — removes port from map when task exits or panics.

Gateway ingress task: reads frames from gateway channel, calls `dispatch_frame` with `FrameSource::Gateway`.

### `mod.rs` — `Fabric` struct

Owns the shared context (`FabricContextInner`) and manages port/gateway lifecycle.

`FabricEvent` enum:
- `RouteMiss { dst_ip, dst_mac }` — frame hit a placeholder route or unknown pod IP
- `ServiceActivation { service_id, dst_ip }` — frame hit a service with no ready backend
- `ServiceBackendNeed { service_id, dst_ip, need }` — activator signaled a backend need level change

Events forwarded to worker via `mpsc::Sender<FabricEvent>` set with `set_event_channel(tx)`. Uses `try_send` — these are hints, silent drop under backpressure is acceptable.

Key methods:
- `add_tap_port(tap, pod_ip, pod_mac) -> (PortId, TaskHandle)` — wraps TAP as `FabricPort::Tap`, pre-registers MAC in mac_table, flushes route-table and service-table buffered frames, spawns port read loop
- `add_port_raw(port) -> (PortId, TaskHandle)` — add a pre-constructed `FabricPort` (e.g. `Virtual` for adapters)
- `set_gateway(egress_tx, ingress_rx)` — connect gateway; spawns gateway ingress task and 60-second GC task for MAC and NAT tables
- `set_event_channel(tx)` — set event emission channel
- `tables()` — get `Arc<FabricContextInner>` reference for external access to tables
- `flush_service_frames(frames, backend_mac, backend_ip, service_ip, service_mac)` — resolves backend MAC to port, applies DNAT (rewrite dst IP from service_ip to backend_ip), inserts reverse NAT entries for each frame's flow, sends via spawned async task
- `send_l4_frames(frames)` — send raw Ethernet frames from L4 stream manager (prepends vnet headers)
- `dispatch_actions(actions, service_id)` — dispatch activator actions (replay, log, backend need)

All frames include 10-byte vhost VNET header before the Ethernet frame.

### `gateway/mod.rs` — FabricGateway (smoltcp IP stack)

The gateway provides L3 services for the pod subnet:

**smoltcp interface** with the pod subnet gateway IP (configurable per namespace), using `GATEWAY_MAC` (`02:00:00:ff:ff:01`).

**ARP**: smoltcp handles ARP for the gateway MAC.

**DNS server** (UDP port 53):
- Queries checked against local `DnsRegistry` (service name → IP)
- Local hits: synthesize A-record response (TTL=60)
- Misses: forward to upstream DNS via hickory-resolver (async, result routed back via query-ID tracking)
- NXDOMAIN synthesized for upstream misses

**Internet egress via TUN**:
- IP packets destined outside the pod subnet are forwarded to a TUN device
- Egress: strip Ethernet header, write IP packet + vnet header to TUN
- Ingress: read from TUN, lookup destination MAC in `ip_mac_table`, rebuild Ethernet header, inject back to fabric
- `ip_mac_table` (IP→MAC) learned from egress frame sources, with 300-second TTL and periodic sweep

**Checksum handling**: Adjusts vnet header checksum offsets when adding/removing Ethernet headers (virtio-net offload).

### `gateway/dns.rs` — DNS query parsing + response synthesis

`DnsRegistry`: `Arc<RwLock<HashMap<String, Ipv4Addr>>>`. Synced from orchestrator via `RegistrySync`/`RegistryUpdate` commands.

`DnsForwarder`: wraps `DnsRegistry` + `TokioResolver` (hickory-resolver). Processes DNS queries from the smoltcp UDP socket — local registry hits get immediate synthetic responses, misses are forwarded asynchronously.

`parse_qname()`: extracts domain name from wire-format DNS query (lowercased). `synthesize_a_response()`: builds minimal A-record response. `synthesize_nxdomain_response()`: builds NXDOMAIN response. Case-insensitive matching. Rejects compression pointers.

### `gateway/tun.rs` — TUN device creation + async I/O

Opens `/dev/net/tun` with `IFF_TUN | IFF_NO_PI | IFF_VNET_HDR`, enables `TUN_F_CSUM` offload. IP address configured via ioctls. Async read/write via tokio AsyncFd. Warns if `ip_forward` sysctl is not enabled.

`TunEgress` struct: owns the TUN fd, manages `ip_mac_table` for return traffic MAC lookup, provides `write_egress()` (strip Ethernet, write to TUN), `read_ingress()` + `build_ingress_frame()` (read from TUN, rebuild Ethernet frame with learned MAC), and `sweep_stale()` for periodic cleanup.

### `tap.rs` — TAP device creation

`create_persistent_tap()`: creates TAP device that survives fd closure (TUNSETPERSIST). `open_packet_socket()`: opens AF_PACKET socket bound to the TAP interface with `PACKET_VNET_HDR`. `TapDevice` struct with Drop impl for cleanup.

### `tests.rs` — Fabric unit tests

Comprehensive test suite using `ChannelPort` virtual ports to test MAC learning, unicast forwarding, broadcast flooding, gateway forwarding, route table buffering, service table behavior, NAT/DNAT, and loopback avoidance without requiring real TAP devices.

---

## NAT for Service Traffic

Service entities have virtual IPs separate from backend pod IPs. The fabric transparently translates between them:

```
Client pod → Service IP (DNAT to Pod IP) → Backend pod
Backend pod → Pod IP (SNAT to Service IP) → Client pod
```

**Forward path (DNAT)**: When a frame is forwarded from a service IP to its backend pod, the fabric rewrites the destination IP from the service IP to the backend pod IP and inserts a reverse NAT entry keyed by `(backend_ip, client_ip, protocol, service_port, client_port)`.

**Return path (SNAT)**: When a known-unicast frame is forwarded, the fabric checks the NAT table. On a hit, it rewrites the source IP from the backend pod IP back to the service IP and rewrites the source MAC to the service MAC. This makes the translation transparent to both client and backend.

**GC**: NAT entries track `last_seen` timestamps and are garbage-collected every 60 seconds alongside MAC table entries.

---

## Integration with Worker

**Namespace creation** (`worker/namespace.rs:NamespaceState::new`):
1. Create `Fabric` instance
2. Create fabric event channel, call `fabric.set_event_channel(tx)`
3. Get `tables()` reference from fabric for external table access
4. Create `DnsRegistry` (shared `Arc<RwLock<HashMap>>`)
5. Spawn `FabricGateway` as background tokio task
6. Connect fabric ↔ gateway via channel pair
7. Create ingress adapter virtual ports (`ChannelPort`) and plug into fabric
8. Spawn event bridge task: maps `FabricEvent` to `WorkerEvent`:
   - `FabricEvent::RouteMiss` → `WorkerEvent::FabricRouteMiss`
   - `FabricEvent::ServiceActivation` → `WorkerEvent::ServiceActivation`
   - `FabricEvent::ServiceBackendNeed` → `WorkerEvent::ServiceBackendNeed`
9. Store in `NamespaceState` (includes `tables`, `registry`, `_gateway_task`, `_event_bridge_task`, `_adapter_tasks`, `_adapter_ports`, `pods`)

**Service management** (`worker/namespace.rs`):
- `create_service` — locks service table, creates `ServiceProcessor` based on `ActivatorConfig` (Passthrough/L3/L4), calls `create(service_id, ip, mac, policy, processor)`
- `update_service_backend` — locks service table, calls `update_backend(service_id, Option<(Ipv4Addr, [u8; 6])>)`
- `service_ready` — locks service table, calls `mark_ready(service_id)` → returns `MarkReadyResult::Passthrough` (flush frames with DNAT + dispatch actions) or `MarkReadyResult::L4` (dispatch L4 frames + actions)
- `destroy_service` — locks service table, calls `destroy(service_id)`

**Route management** (`worker/namespace.rs`):
- `route_sync` — locks route table, calls `sync(routes)` for full replacement
- `route_update` — locks route table, calls `update(added, removed_ips)` for incremental delta

**Pod launch** (`worker/supervisor.rs:pod_supervisor`):
1. VM launches with TAP device (virtio-net, vhost-net backend)
2. `take_tap()` transfers TAP ownership from VM to fabric
3. `fabric.add_tap_port(tap, network.ip, network.mac)` — pre-registers MAC, flushes route-table and service-table buffered frames, spawns port read loop
4. Guest configures interface via `ConfigureNetwork` command (IP/netmask/gateway)
5. Port task monitored by pod supervisor — if it exits, the pod is failed

**Pod shutdown**: Port task cleaned up automatically via RAII when supervisor exits.

---

## Service Entities

Services are first-class network entities on the fabric with their own virtual IP and MAC, separate from backing pod IPs. A service entity is the boundary at which application-level traffic management happens — buffering, activation, readiness gating, and protocol-aware processing all live here rather than on the pod directly. See `docs/worker-protocol.md` for the full protocol design.

```
Client pod → Service IP (virtual) → [buffer / activate / ready?] → Pod IP (real)
```

**Why this separation matters**:
- **Clean lifecycle boundary**: Pod lifecycle (VM booted, network configured) is distinct from service readiness (application listening, health check passed).
- **Readiness gating**: Buffered frames are only flushed to the backing pod once the orchestrator sends `ServiceReady`, not immediately at port-add time.
- **Flexibility**: Multiple services can back the same pod. Scale-to-zero is "no backing pod assigned to service IP" rather than "pod doesn't exist."
- **Protocol activators**: Protocol-aware logic (TCP SYN detection, HTTP/2 stream parsing) runs on the service entity via `ServiceProcessor`. See [Protocol Activators](protocol-activators.md).
- **Transparent NAT**: DNAT/SNAT between service IPs and backend pod IPs is handled automatically by the fabric. Clients address the service IP; the backend sees its own pod IP.

**Service states**: No backend (buffering + activation event) → Backend assigned, not ready (buffering) → Ready (traffic flows through, with DNAT). The orchestrator drives transitions via `CreateService`, `UpdateServiceBackend`, `ServiceReady`, `DestroyService` commands.

**Coexistence with pod routes**: Pods remain directly addressable by IP. The existing route table with placeholder entries provides basic best-effort buffering for direct pod-to-pod traffic. Services get the rich activation path; pod routes preserve the flat L2 network illusion. Traffic resolution order: local TAP → service entity → route table → flood.

**ARP resolution**: Service IPs are ARP-resolvable. When a broadcast ARP request targets a service IP, the fabric constructs an ARP reply with the service's virtual MAC and sends it back to the requesting port. This allows guest network stacks to resolve service IPs without any special configuration.

**IP allocation in compose mode**: Each service gets two IPs from the namespace subnet — a service IP (virtual, used for DNS and the service entity) and a pod IP (assigned to the VM network interface). Service IPs are allocated first (.2 to .N+1), pod IPs after (.N+2 to .2N+1). This limits compose deployments to 126 services per namespace (using the default /24 subnet).

**Compose orchestration flow**: On `CreateNamespace`, the orchestrator sends `CreateService` for each planned service. DNS entries map names to service IPs. On `PodRunning`, the orchestrator sends `UpdateServiceBackend` (with pod IP/MAC) followed by `ServiceReady` to flush buffered frames and enable traffic flow.

---

## Future Work

### Ingress Adapters

External access into the fabric for developer access, shareable URLs, and infrastructure integration. Adapters are worker-level components that demultiplex to per-namespace virtual ports on the fabric via `ChannelPort`. WireGuard (via boringtun) is the primary strategy for staging environments.

See **[Ingress Adapters](ingress-adapters.md)** for the full design.

### Multi-Worker Tunneling

For distributed mode, fabric segments on different workers need to communicate:
- Tunnel ports connecting fabric segments across workers (e.g. VXLAN, or custom over TCP/TLS)
- Orchestrator pushes routing table updates so each worker knows which MACs/IPs are remote
- Frames for remote destinations forwarded through tunnel to the appropriate worker
