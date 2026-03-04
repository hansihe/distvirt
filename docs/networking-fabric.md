# Networking Fabric

## Current State

**Phases 1, 2, 3 (route table + packet buffering), and 4 (service entities) are implemented.** The fabric is a per-namespace userspace L3 IP router with a smoltcp-based gateway providing DNS service discovery and internet egress via TUN+NAT. Each worker creates one fabric instance per namespace. Pod TAP devices are added as ports on the router. The fabric includes a route table for destinations that aren't local — packets to placeholder destinations are buffered per policy, and route miss events propagate to the orchestrator for scale-to-zero activation. Fabric-level service entities provide readiness gating: traffic to service IPs is buffered until the orchestrator signals readiness, at which point packets are flushed to the backing pod. Protocol activators (WASM components) optionally provide protocol-aware activation on service entities — see [Protocol Activators](protocol-activators.md).

**Note**: Direct pod-to-pod route table buffering (via `add_port_with_ip`) still flushes immediately when the TAP port is added, without readiness gating. Service entities are the recommended path for inter-service communication where readiness gating matters.

---

## Architecture

```
┌─────────────────────────────────────────────────┐
│                  Fabric (L3 router)              │
│  Owns ports, IP table, NAT table,               │
│  packet forwarding (DNAT/SNAT for services)     │
├─────────────────────────────────────────────────┤
│              FabricGateway (smoltcp)             │
│  DNS (service registry) · TUN egress/NAT        │
├─────────────────────────────────────────────────┤
│              Port abstraction                    │
│  TAP (AF_PACKET + AsyncFd) or Virtual (channel) │
│  Per-port read task.                             │
└─────────────────────────────────────────────────┘
```

The fabric is **decoupled from VMM and container code**. It only knows about ports (IP packet sources/sinks). The worker is the glue — it creates namespaces, launches VMs, and hands TAP devices to the fabric.

---

## Packet Format

The fabric operates on IP packets with a lightweight 3-byte fabric header, not Ethernet frames:

```
[fabric_hdr (3 bytes)][IP packet]
```

`FabricHeader` fields:
- `flags` (u8): `NEEDS_CSUM` bit for deferred checksum completion
- `segment_id` (u16): for future inter-worker routing

`FabricPacket` wraps a buffer and provides accessors: `fabric_header()`, `ip_packet()`, `dst_ip()`, `src_ip()`, `protocol()`, `transport_ports()`.

This replaces the previous L2 format which used a 10-byte vnet header + 14-byte Ethernet header. The 3-byte fabric header is significantly more efficient — Ethernet framing is only applied at TAP device boundaries where guest network stacks need it.

---

## Modules (`distvirt-worker/src/fabric/`)

### `port.rs` — Async fabric port

`FramePort` trait: async `recv_frame()` and `send_frame()` abstraction.

`Port` struct wraps an AF_PACKET socket fd in tokio `AsyncFd` via `dup()` — the original fd stays in `TapDevice` (owns Drop cleanup for the TAP device), the dup'd fd goes into AsyncFd. Both set `O_NONBLOCK`.

`ChannelPort` struct wraps `mpsc` channels for virtual (non-TAP) ports — used by ingress adapters (e.g. WireGuard). Returns `(port, adapter_tx, adapter_rx)` for bidirectional communication.

`FabricPort` enum dispatches between `Tap(Port)` and `Virtual(ChannelPort)`, implementing `FramePort` for both.

### `switch.rs` — IP-to-port table

`IpPortTable`: bidirectional `HashMap<Ipv4Addr, PortId>` (`by_ip`) and `HashMap<PortId, Ipv4Addr>` (`by_port`) for IP↔port lookups. Unlike the previous MAC learning table, entries are statically configured when ports are added — no learning, no aging.

Key methods:
- `insert(ip, port)` — register an IP↔port mapping
- `lookup(ip) -> Option<PortId>` — look up port by IP
- `contains_ip(ip) -> bool` — check if IP is registered
- `remove_by_port(port)` — remove mapping when port is cleaned up

The gateway IP is configurable per namespace (passed to `FabricGateway`).

### `route.rs` — Route table + packet buffering

`RouteTable`: `HashMap<Ipv4Addr, RouteState>` mapping destination IPs to route entries with optional packet buffers. Each entry contains a `FabricRouteEntry` (from protocol types), a `VecDeque<Vec<u8>>` packet buffer, and a buffer start time for timeout tracking. Per-IP debounce tracking (default 1s window) prevents route miss event floods.

`RouteAction` enum: `Buffered` (packet accepted into buffer), `Drop` (policy says drop), `RemoteWorker { worker_id }` (stub for multi-worker, log + drop), `NoRoute` (no entry, no match).

Key methods:
- `sync(entries)` — full replacement of route table
- `update(added, removed_ips)` — incremental delta
- `lookup_and_buffer(dst_ip, packet) -> (RouteAction, bool)` — returns action + whether miss event should fire (respecting debounce). Handles buffer limits and timeout expiry.
- `flush_buffer(ip) -> Vec<Vec<u8>>` — drains buffered packets (called when pod activates)

### `service.rs` — Service entities + service table

`ServiceEntity`: holds service_id, virtual IP, `ServicePolicy` (buffer_frames, timeout_ms, optional `ActivatorConfig`), `backend_ip: Option<Ipv4Addr>`, readiness state, a `VecDeque<Vec<u8>>` packet buffer with timeout tracking, and a `ServiceProcessor` (Passthrough/L3/L4) for protocol-aware activation.

`ServiceTable`: `HashMap<Ipv4Addr, ServiceEntity>` for fast packet-path lookup, plus `HashMap<String, Ipv4Addr>` for command dispatch by service_id. Per-IP activation debounce (default 1s window) prevents activation event floods.

`ServiceAction` enum:
- `Forward { pod_ip, service_ip }` — service is ready, forward to backend (caller applies DNAT)
- `Buffered` — packet accepted into buffer
- `Drop` — buffer full or timed out
- `ActivatorActions { actions, service_id }` — L3 activator processed the packet, returned actions for fabric to execute
- `L4Result { actions, frames, service_id, poll_delay }` — L4 stream manager produced outgoing packets + non-L4 actions

`MarkReadyResult` enum:
- `Passthrough { frames, backend_ip, service_ip, actions }` — buffered packets + backend info + activator actions
- `L4(ServiceAction)` — L4 stream mode result

Key methods:
- `create(service_id, ip, policy, processor)` — register a new service entity with its processor mode
- `destroy(service_id)` — remove a service entity
- `update_backend(service_id, Option<Ipv4Addr>)` — assign or remove backing pod; clears readiness. Preserves buffer on first backend assignment (None → Some); clears buffer when backend removed or IP changes.
- `mark_ready(service_id) -> Option<MarkReadyResult>` — mark ready; pushes `BackendAvailable(true)` to activator if present, returns buffered packets + actions (Passthrough) or L4 result
- `lookup_and_buffer(dst_ip, packet, is_reachable) -> Option<(ServiceAction, bool)>` — `None` if not a service IP; `bool` is `should_activate` (debounced). Delegates to `ServiceProcessor` for L3/L4 paths.
- `get_service_id(ip)` — returns service_id for activation events
- `get_nat_info_by_id(service_id)` — returns `(service_ip, backend_ip)` for NAT setup
- `flush_by_backend_ip(ip) -> Vec<ServiceFlushData>` — drain buffers for ready services matching a backend IP (called when a port is added)

### `service_activator.rs` — Service processor (activator integration)

`ServiceProcessor` enum determines how a service entity processes incoming packets:
- `Passthrough` — no activator, pure buffer/forward (default for services with no protocol declaration)
- `L3 { activator: ActivatorInstance, flow_tracker: FlowTracker }` — L3 packet-level processing via WASM activator. `FlowTracker` assigns stable flow IDs by 5-tuple for `packet-flow` handles.
- `L4 { activator: Option<ActivatorInstance>, stream_manager: StreamManager }` — L4 stream-level processing. The `StreamManager` (smoltcp-backed TCP stack) handles TCP connection management; the activator operates on byte streams.

Key methods:
- `process_packet(service_id, ip_payload, raw_packet) -> Option<ServiceAction>` — parses packet, delegates to L3 (packet event) or L4 (stream manager)
- `on_mark_ready(service_id) -> Option<ServiceAction>` — pushes `BackendAvailable(true)` event, processes activator response
- `on_backend_update(has_backend, backend_ip)` — pushes `BackendAvailable` event, updates stream manager
- `handle_timeout(service_id) -> Option<ServiceAction>` — L4 only: polls stream manager for TCP timeouts

For L4 mode, a bounded event loop (4 rounds max) separates L4 actions (executed by the stream manager) from non-L4 actions (returned to the fabric for dispatch).

### `nat.rs` — NAT connection tracking

`NatTable`: `HashMap<NatFlowKey, NatEntry>` for reverse-direction NAT lookup. Used for service DNAT/SNAT — when a packet is forwarded from a service IP to a backend pod IP, a reverse NAT entry is inserted so return traffic from the backend can be SNATted back to the service IP.

`NatFlowKey`: 5-tuple `(src_ip, dst_ip, protocol, src_port, dst_port)`. `NatEntry`: `(service_ip, backend_ip, last_seen)`.

Key methods:
- `insert(key, entry)` — insert reverse-direction entry (key is `(backend_ip, client_ip, proto, service_port, client_port)`)
- `lookup(key) -> Option<&NatEntry>` — look up and update `last_seen`
- `gc(max_age)` — remove stale entries; runs every 60 seconds

### `forwarding.rs` — Packet dispatch + action execution

Contains the core packet forwarding logic, extracted from `mod.rs`.

`FabricContextInner` struct: shared forwarding state holding `ports`, `ip_port_table`, `route_table`, `service_table`, `nat_table`, `gateway_tx`, `event_tx`, `subnet`, and `prefix_len`. Wrapped in `Arc` for sharing across port read tasks.

`dispatch_packet(packet, source, ctx)` — the main forwarding function called by every port read loop and the gateway ingress task:
1. Parse packet, extract destination IP
2. Check if destination is in the fabric subnet (`is_in_subnet()`)
3. IP table lookup → if hit: check NAT table for return traffic SNAT (rewrite src IP), then forward to port
4. If not in IP table → `handle_unresolved_dst`

`handle_unresolved_dst` — resolution order:
- **Service table**: consult first. If ready + reachable → DNAT (rewrite dst IP from service_ip to backend_ip) + insert reverse NAT entry + forward. If activator → dispatch actions (ReplayPacket, SetBackendNeed, Log). If L4 → dispatch packets + set poll timer.
- **Route table**: buffer/drop per policy, emit `RouteMiss` event if debounce allows
- **No match**: drop (no flooding in L3 model)

`dispatch_action(action, service_id, dst_ip, ctx)` — executes activator actions:
- `ReplayPacket` — DNAT to backend, insert reverse NAT entry, send to backend port
- `SetBackendNeed` — emit `ServiceBackendNeed` event
- `Log` — log at appropriate level

Port read loop: spawns per-port, reads packets in a loop, calls `dispatch_packet`. `PortGuard` provides RAII cleanup — removes port from IP table when task exits or panics.

Gateway ingress task: reads packets from gateway channel, calls `dispatch_packet` with `PacketSource::Gateway`.

### `mod.rs` — `Fabric` struct

Owns the shared context (`FabricContextInner`) and manages port/gateway lifecycle.

`FabricEvent` enum:
- `RouteMiss { dst_ip }` — packet hit a placeholder route or unknown pod IP
- `ServiceActivation { service_id, dst_ip }` — packet hit a service with no ready backend
- `ServiceBackendNeed { service_id, dst_ip, need }` — activator signaled a backend need level change

Events forwarded to worker via `mpsc::Sender<FabricEvent>` set with `set_event_channel(tx)`. Uses `try_send` — these are hints, silent drop under backpressure is acceptable.

Key methods:
- `add_tap_port(tap, pod_ip) -> (PortId, TaskHandle)` — wraps TAP as `FabricPort::Tap`, registers IP in ip_port_table, flushes route-table and service-table buffered packets, spawns port read loop
- `add_port_raw(port) -> (PortId, TaskHandle)` — add a pre-constructed `FabricPort` (e.g. `Virtual` for adapters)
- `set_gateway(egress_tx, ingress_rx)` — connect gateway; spawns gateway ingress task and 60-second GC task for NAT table
- `set_event_channel(tx)` — set event emission channel
- `tables()` — get `Arc<FabricContextInner>` reference for external access to tables
- `flush_service_packets(packets, backend_ip, service_ip)` — resolves backend IP to port, applies DNAT (rewrite dst IP from service_ip to backend_ip), inserts reverse NAT entries for each packet's flow, sends via spawned async task
- `send_l4_packets(packets)` — send packets from L4 stream manager (prepends fabric headers)
- `dispatch_actions(actions, service_id)` — dispatch activator actions (replay, log, backend need)

All packets include 3-byte fabric header before the IP packet.

### `gateway/mod.rs` — FabricGateway (smoltcp IP stack)

The gateway provides IP services for the pod subnet:

**smoltcp interface** with the pod subnet gateway IP (configurable per namespace).

**DNS server** (UDP port 53):
- Queries checked against local `DnsRegistry` (service name → IP)
- Local hits: synthesize A-record response (TTL=60)
- Misses: forward to upstream DNS via hickory-resolver (async, result routed back via query-ID tracking)
- NXDOMAIN synthesized for upstream misses

**Internet egress via TUN**:
- IP packets destined outside the pod subnet are forwarded to a TUN device
- Egress: write IP packet to TUN
- Ingress: read from TUN, route back to correct port via IP table

**Checksum handling**: Fabric header `NEEDS_CSUM` flag tracks deferred checksum completion for virtio-net offload.

### `gateway/dns.rs` — DNS query parsing + response synthesis

`DnsRegistry`: `Arc<RwLock<HashMap<String, Ipv4Addr>>>`. Synced from orchestrator via `RegistrySync`/`RegistryUpdate` commands.

`DnsForwarder`: wraps `DnsRegistry` + `TokioResolver` (hickory-resolver). Processes DNS queries from the smoltcp UDP socket — local registry hits get immediate synthetic responses, misses are forwarded asynchronously.

`parse_qname()`: extracts domain name from wire-format DNS query (lowercased). `synthesize_a_response()`: builds minimal A-record response. `synthesize_nxdomain_response()`: builds NXDOMAIN response. Case-insensitive matching. Rejects compression pointers.

### `gateway/tun.rs` — TUN device creation + async I/O

Opens `/dev/net/tun` with `IFF_TUN | IFF_NO_PI | IFF_VNET_HDR`, enables `TUN_F_CSUM` offload. IP address configured via ioctls. Async read/write via tokio AsyncFd. Warns if `ip_forward` sysctl is not enabled.

`TunEgress` struct: owns the TUN fd, provides `write_egress()` (write IP packet to TUN) and `read_ingress()` (read from TUN, wrap with fabric header, route back via IP table).

### `tap.rs` — TAP device creation

`create_persistent_tap()`: creates TAP device that survives fd closure (TUNSETPERSIST). `open_packet_socket()`: opens AF_PACKET socket bound to the TAP interface with `PACKET_VNET_HDR`. `TapDevice` struct with Drop impl for cleanup.

### `tests.rs` — Fabric unit tests

Comprehensive test suite using `ChannelPort` virtual ports to test IP-based forwarding, gateway forwarding, route table buffering, service table behavior, NAT/DNAT, and loopback avoidance without requiring real TAP devices.

---

## NAT for Service Traffic

Service entities have virtual IPs separate from backend pod IPs. The fabric transparently translates between them:

```
Client pod → Service IP (DNAT to Pod IP) → Backend pod
Backend pod → Pod IP (SNAT to Service IP) → Client pod
```

**Forward path (DNAT)**: When a packet is forwarded from a service IP to its backend pod, the fabric rewrites the destination IP from the service IP to the backend pod IP and inserts a reverse NAT entry keyed by `(backend_ip, client_ip, protocol, service_port, client_port)`.

**Return path (SNAT)**: When a packet is forwarded to a known port, the fabric checks the NAT table. On a hit, it rewrites the source IP from the backend pod IP back to the service IP. This makes the translation transparent to both client and backend.

**GC**: NAT entries track `last_seen` timestamps and are garbage-collected every 60 seconds.

---

## Integration with Worker

**Namespace creation** (`worker/namespace.rs:NamespaceState::new`):
1. Create `Fabric` instance with subnet and prefix_len
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
- `create_service` — locks service table, creates `ServiceProcessor` based on `ActivatorConfig` (Passthrough/L3/L4), calls `create(service_id, ip, policy, processor)`
- `update_service_backend` — locks service table, calls `update_backend(service_id, Option<Ipv4Addr>)`
- `service_ready` — locks service table, calls `mark_ready(service_id)` → returns `MarkReadyResult::Passthrough` (flush packets with DNAT + dispatch actions) or `MarkReadyResult::L4` (dispatch L4 packets + actions)
- `destroy_service` — locks service table, calls `destroy(service_id)`

**Route management** (`worker/namespace.rs`):
- `route_sync` — locks route table, calls `sync(routes)` for full replacement
- `route_update` — locks route table, calls `update(added, removed_ips)` for incremental delta

**Pod launch** (`worker/supervisor.rs:pod_supervisor`):
1. VM launches with TAP device (virtio-net, vhost-net backend)
2. `take_tap()` transfers TAP ownership from VM to fabric
3. `fabric.add_tap_port(tap, network.ip)` — registers IP in ip_port_table, flushes route-table and service-table buffered packets, spawns port read loop
4. Guest configures interface via `ConfigureNetwork` command (IP/netmask/gateway)
5. Port task monitored by pod supervisor — if it exits, the pod is failed

**Pod shutdown**: Port task cleaned up automatically via RAII when supervisor exits.

---

## Service Entities

Services are first-class network entities on the fabric with their own virtual IP, separate from backing pod IPs. A service entity is the boundary at which application-level traffic management happens — buffering, activation, readiness gating, and protocol-aware processing all live here rather than on the pod directly. See `docs/worker-protocol.md` for the full protocol design.

```
Client pod → Service IP (virtual) → [buffer / activate / ready?] → Pod IP (real)
```

**Why this separation matters**:
- **Clean lifecycle boundary**: Pod lifecycle (VM booted, network configured) is distinct from service readiness (application listening, health check passed).
- **Readiness gating**: Buffered packets are only flushed to the backing pod once the orchestrator sends `ServiceReady`, not immediately at port-add time.
- **Flexibility**: Multiple services can back the same pod. Scale-to-zero is "no backing pod assigned to service IP" rather than "pod doesn't exist."
- **Protocol activators**: Protocol-aware logic (TCP SYN detection, HTTP/2 stream parsing) runs on the service entity via `ServiceProcessor`. See [Protocol Activators](protocol-activators.md).
- **Transparent NAT**: DNAT/SNAT between service IPs and backend pod IPs is handled automatically by the fabric. Clients address the service IP; the backend sees its own pod IP.

**Service states**: No backend (buffering + activation event) → Backend assigned, not ready (buffering) → Ready (traffic flows through, with DNAT). The orchestrator drives transitions via `CreateService`, `UpdateServiceBackend`, `ServiceReady`, `DestroyService` commands.

**Coexistence with pod routes**: Pods remain directly addressable by IP. The existing route table with placeholder entries provides basic best-effort buffering for direct pod-to-pod traffic. Services get the rich activation path. Traffic resolution order: local port (IP table) → service entity → route table → drop.

**IP allocation in compose mode**: Each service gets two IPs from the namespace subnet — a service IP (virtual, used for DNS and the service entity) and a pod IP (assigned to the VM network interface). Service IPs are allocated first (.2 to .N+1), pod IPs after (.N+2 to .2N+1). This limits compose deployments to 126 services per namespace (using the default /24 subnet).

**Compose orchestration flow**: On `CreateNamespace`, the orchestrator sends `CreateService` for each planned service. DNS entries map names to service IPs. On `PodRunning`, the orchestrator sends `UpdateServiceBackend` (with pod IP) followed by `ServiceReady` to flush buffered packets and enable traffic flow.

---

## Future Work

### Ingress Adapters

External access into the fabric for developer access, shareable URLs, and infrastructure integration. Adapters are worker-level components that demultiplex to per-namespace virtual ports on the fabric via `ChannelPort`. WireGuard (via boringtun) is the primary strategy for staging environments.

See **[Ingress Adapters](ingress-adapters.md)** for the full design.

### Multi-Worker Tunneling

For distributed mode, fabric segments on different workers need to communicate. Each worker runs one fabric instance per namespace — when a namespace spans multiple workers, those fabric instances need a data plane link.

#### Architecture

```
Worker A                                    Worker B
┌──────────────────┐                       ┌──────────────────┐
│ Namespace "foo"  │                       │ Namespace "foo"  │
│  Fabric          │                       │  Fabric          │
│   └─ TunnelPort ─┼── segment_id=1 ──┐   │   └─ TunnelPort  │
│                  │                   │   │                  │
│ Namespace "bar"  │                   │   │ Namespace "bar"  │
│  Fabric          │                   │   │  Fabric          │
│   └─ TunnelPort ─┼── segment_id=2 ──┤   │   └─ TunnelPort  │
└──────────────────┘                   │   └──────────────────┘
                                       ▼
                              ┌─────────────────┐
                              │ TunnelTransport  │
                              │ Single UDP socket│
                              │ per worker pair  │
                              │                  │
                              │ Noise encryption │
                              │ segment_id demux │
                              └─────────────────┘
```

One `TunnelTransport` per remote worker peer, multiplexing all namespaces over a single encrypted UDP socket. The `segment_id` field in `FabricHeader` (reserved since the L3 rewrite) demultiplexes packets to the correct namespace's fabric instance.

#### Segment ID Allocation

`segment_id` is a 16-bit field assigned **globally per namespace** by the orchestrator. Each namespace gets a unique segment ID at creation time, valid across all tunnels in the cluster. This means the same segment ID always refers to the same namespace regardless of which worker pair tunnel it traverses.

The orchestrator maintains a simple `u16` allocator (incrementing counter + set of active IDs). On namespace creation, allocate the next free ID; on destruction, return it to the pool. With 65536 values and typical namespace lifetimes, wraparound is infrequent — the allocator just skips IDs still in use.

Workers receive the segment ID as part of namespace setup and use it directly when stamping outbound fabric frames and demuxing inbound ones. No per-tunnel negotiation needed.

#### Wire Format

Fabric frames pass through the tunnel as-is — the existing 3-byte fabric header is the tunnel multiplexing header:

```
UDP datagram (after Noise decrypt):
  [fabric_hdr (3 bytes)][IP packet]
   ├─ flags (u8):       NEEDS_CSUM etc.
   └─ segment_id (u16): namespace demux key
```

One UDP datagram = one fabric frame. No additional framing or length prefixes needed.

#### Encryption: Noise Protocol via `snow`

The tunnel uses the [Noise protocol framework](https://noiseprotocol.org/) (Noise_IK pattern) for authenticated encryption, via the `snow` crate.

**Why not boringtun's noise module**: Boringtun's useful internals (`Session`, `Handshake`) are `pub(crate)` — inaccessible from outside the crate. The only public API is `Tunn`, which validates decrypted payloads as IP packets (`validate_decapsulated_packet` checks the IP version nibble). Fabric frames start with a `flags` byte (not an IP header), so decapsulation rejects them. Boringtun is a WireGuard tunnel implementation, not a reusable Noise library.

**Why `snow`**: Purpose-built Noise protocol framework that operates on arbitrary byte buffers with no payload validation. Uses `ring` for crypto (same as boringtun already pulls in), so no new crypto dependency. The `TransportState` provides exactly what we need — encrypt/decrypt with counter-based nonces and replay protection.

**Noise_IK pattern**: The initiator's static key is sent encrypted in the first message, and the responder's static key is known ahead of time (pre-distributed by the orchestrator). This provides mutual authentication and forward secrecy with a 1-RTT handshake.

**Overhead**: ~16 bytes (Poly1305 auth tag) + ~8 bytes (counter/header) ≈ 24 bytes per datagram, on top of the outer UDP/IP headers (28 bytes for IPv4). Total tunnel overhead ≈ 52 bytes — less than WireGuard's ~60 bytes.

#### MTU Considerations

The inner MTU (guest network interface) must account for tunnel overhead to avoid IP-layer fragmentation on the outer path. With ~52 bytes of overhead, an inner MTU of 1420 (matching the WireGuard convention) provides comfortable margin on standard 1500-byte Ethernet links.

IP-layer fragmentation of outer UDP datagrams works as a fallback (the receiving kernel reassembles before the UDP socket sees the data), but should be avoided for performance — especially since PMTUD is often broken by middleboxes. The guest MTU is configured via the existing `ConfigureNetwork` command.

#### Components

**`TunnelTransport`** (worker-level, one per remote worker peer):
- Owns a single UDP socket bound to the worker's tunnel listen port
- Manages Noise_IK handshake lifecycle (initiation, response, rekey)
- Recv loop: decrypt → read `segment_id` from fabric header → dispatch to correct namespace's `TunnelPort`
- Send path: encrypt → `sendto` peer endpoint
- Timer task for Noise rekeys and keepalives
- Keys assigned by orchestrator (same pattern as WireGuard adapter key distribution)

**`TunnelPort`** (namespace-level, plugs into fabric via `ChannelPort`):
- Created when the orchestrator assigns a namespace to span multiple workers
- Integrates with the fabric as a `FabricPort::Virtual` via the existing `ChannelPort` mechanism
- Egress: fabric dispatches packet to tunnel port → channel → `TunnelTransport` encrypts and sends
- Ingress: `TunnelTransport` demuxes by `segment_id` → channel → fabric port read loop dispatches packet
- One `TunnelPort` per remote worker per namespace

**Orchestrator coordination** (extends existing protocol):
- Assigns a globally unique `segment_id` per namespace at creation time
- Distributes tunnel keys to workers (extends existing `WorkerAccepted` / `AdapterConfig` pattern)
- Sends `segment_id` to workers as part of namespace setup (e.g. in `CreateNamespace` or a dedicated command)
- Existing `FabricRouteSync`/`FabricRouteUpdate` with `RouteDestination::RemoteWorker { worker_id }` already exist — the worker resolves `worker_id` to the corresponding `TunnelTransport`
- New worker command: `EstablishTunnel { peer_worker_id, endpoint, peer_public_key }`

#### Existing Infrastructure

The following pieces are already in place:
- `segment_id` field in `FabricHeader` — reserved for this purpose, currently always 0
- `RouteDestination::RemoteWorker { worker_id }` in route table — currently a stub (logs and drops)
- `RouteAction::RemoteWorker { worker_id }` in `forwarding.rs` dispatch — stub handler exists
- `ChannelPort` — proven virtual port integration pattern (used by WireGuard ingress adapter)
- `public_endpoint` in `WorkerCapabilities` — workers announce their reachable endpoint at connect time
- Orchestrator tracks `WorkerWgConfig` with per-worker keys — same pattern extends to tunnel keys

#### Implementation Plan

1. **Global `segment_id` allocator** in orchestrator, assigned per namespace at creation time
2. **Add `TunnelTransport`** with plaintext UDP first (get routing working end-to-end)
3. **Wire up `segment_id` demux** → `ChannelPort` per namespace
4. **Resolve `RouteAction::RemoteWorker`** from log+drop to forward-to-tunnel-port
5. **Add Noise encryption** via `snow` crate (Noise_IK, `ring` backend)
6. **Add orchestrator commands** for tunnel lifecycle (`EstablishTunnel`, `TeardownTunnel`)
7. **MTU configuration** — propagate tunnel overhead to guest `ConfigureNetwork`

### Pod Splice (Local Development)

Splice allows a developer to replace a running pod with their local machine, receiving and sending traffic through the fabric as if they were the pod. This reuses the existing WireGuard ingress adapter — no new data plane work required.

#### How It Works

The developer already has a WireGuard tunnel into the fabric (via `connect`), which assigns them a peer IP on the namespace's subnet. Splicing can operate at two levels:

**Service-level splice**: Redirect a service's backend to the developer's machine.
1. Orchestrator calls `UpdateServiceBackend(service_id, Some(wireguard_peer_ip))` — pointing the service at the developer's WireGuard peer IP instead of the original pod
2. Orchestrator calls `ServiceReady(service_id)` — flushes any buffered packets toward the developer
3. DNAT/SNAT between the service IP and the WireGuard peer IP works identically to a pod backend

**Pod-level splice**: Redirect direct pod-to-pod traffic to the developer's machine. Since pods are directly addressable by IP on the fabric, this is even simpler — just update the route table so the pod's IP resolves to the worker hosting the WireGuard peer. No service entity involvement needed.

From the fabric's perspective, nothing is special in either case — the WireGuard `ChannelPort` is just another `FabricPort::Virtual`.

#### Routing Considerations

The orchestrator must ensure the worker hosting the WireGuard adapter has routes for traffic destined to the spliced service. If the service was previously on a different worker, `FabricRouteSync`/`FabricRouteUpdate` directs traffic to the correct worker via the multi-worker tunnel.

#### CLI Side (Platform-Specific)

The CLI is responsible for setting up the developer's machine to send and receive traffic:

- **WireGuard tunnel**: established via the existing `connect` flow
- **Route setup**: routes for the namespace subnet through the WireGuard interface (platform-specific — `ip route` on Linux, `route` on macOS, etc.)
- **Local process**: developer runs their service locally, listening on the expected port

The splice operation is symmetrical with unsplice — the orchestrator points `UpdateServiceBackend` back at the original pod IP and marks it ready again.
