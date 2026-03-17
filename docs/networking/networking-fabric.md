---
title: "Networking Fabric"
---

## Current State

The fabric is a per-namespace userspace L3 IP router with a smoltcp-based gateway providing DNS service discovery and internet egress via TUN+NAT. Each worker creates one fabric instance per namespace. Pod TAP devices are added as ports on the router.

All destination types — services, pods, WireGuard peers — are managed through a unified **endpoint model**. An `EndpointTable` replaces the former separate `ServiceTable` and `RouteTable`. The orchestrator computes a canonical set of `EndpointSpec`s per namespace and broadcasts them to all workers. Each worker derives its local endpoint table based on its own identity (local pod, remote segment, local adapter, etc.).

Protocol activators (WASM components) optionally provide protocol-aware activation on service endpoints — see [Protocol Activators](protocol-activators.md).

---

## Architecture

```
┌─────────────────────────────────────────────────┐
│                  Fabric (L3 router)              │
│  Owns ports, NAT table, endpoint table,          │
│  table, packet forwarding (DNAT/SNAT)           │
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
- `segment_id` (u16): for inter-worker routing

`FabricPacket` wraps a buffer and provides accessors: `fabric_header()`, `ip_packet()`, `dst_ip()`, `src_ip()`, `protocol()`, `transport_ports()`.

Ethernet framing is only applied at TAP device boundaries where guest network stacks need it.

---

## Unified Endpoint Model

### Problem Solved

Previously, the fabric had two parallel systems for handling traffic to destinations that aren't locally connected:

1. **Service entities** (`ServiceTable`) — rich lifecycle: buffering, activation events, readiness gating, protocol activators, NAT.
2. **Route table** (`RouteTable`) — simple placeholder buffering with debounced route miss events for pod-to-pod traffic.

These shared the same fundamental behavior but had completely different implementations. Direct pod traffic lacked idle detection, connection tracking, and readiness gating.

### Design

An **endpoint** is any IP destination on the fabric that needs lifecycle management. Every endpoint shares the same front-end behavior:

1. **Packet arrives for destination IP**
2. **Endpoint decides what to do** — buffer, forward to local backend, forward to remote segment
3. **Endpoint tracks liveness** — "are there active flows to this destination?"

The difference is only in the **backend strategy**: how packets reach their final destination once the endpoint is ready.

### Endpoint Structure

```rust
struct Endpoint {
    ip: Ipv4Addr,
    state: EndpointState,
    buffer: VecDeque<Vec<u8>>,
    buffer_start: Option<Instant>,
    backend: EndpointBackend,
}

enum EndpointState {
    /// No backend available. Packets are buffered, activation events emitted.
    Buffering,
    /// Backend assigned but not yet ready. Packets still buffered.
    Pending,
    /// Backend ready. Packets forwarded.
    Ready,
}

enum EndpointBackend {
    /// Service with virtual IP → pod IP NAT, optional protocol activator.
    Service {
        service_id: String,
        policy: ServicePolicy,
        backend_ip: Option<Ipv4Addr>,
        processor: ServiceProcessor,
    },
    /// Pod not running anywhere. Buffer + emit activation event.
    UnplacedPod {
        buffer_policy: BufferPolicy,
    },
    /// Destination reachable via another worker's fabric segment.
    /// Used for remote pods AND remote WireGuard peers.
    RemoteSegment {
        worker_id: String,
    },
    /// Pod running on this worker. When `port_id` is `None`, the pod is
    /// launching and frames are buffered. When `Some`, the port is attached
    /// and frames are forwarded directly.
    LocalPod {
        port_id: Option<PortId>,
    },
    /// WireGuard peer or splice target connected locally via channel port.
    LocalAdapter {
        port_id: PortId,
    },
}
```

### EndpointTable

```rust
struct EndpointTable {
    by_ip: HashMap<Ipv4Addr, Endpoint>,
    service_id_to_ip: HashMap<String, Ipv4Addr>,
    last_activation: HashMap<Ipv4Addr, Instant>,
    activation_debounce: Duration,  // default 1s
}
```

### Endpoint Transitions

| Spec | Placement | Local Backend |
|---|---|---|
| Pod, local worker | `LocalPod` → Pending (launching) / Ready (port attached) | TAP port |
| Pod, remote worker | `RemoteSegment` → Ready | — |
| Pod, unplaced | `UnplacedPod` → Buffering | — |
| WireGuardPeer, local | `LocalAdapter` → Ready | channel port |
| WireGuardPeer, remote | `RemoteSegment` → Ready | — |
| WireGuardPeer, unplaced | `UnplacedPod` → Buffering | — |
| Service, no backend | `Service` → Buffering | — |
| Service, backend assigned | `Service` → Pending | preserves buffer |
| Service, marked ready | `Service` → Ready | drains buffer |

### Buffer Policy

- **LocalPod**: 64 frames, 30-second timeout (same as UnplacedPod)
- **UnplacedPod**: 64 frames, 30-second timeout
- **Service**: Configurable via `ServicePolicy.buffer_frames` and `timeout_ms`
- **Timeout**: Entire buffer cleared when first frame exceeds timeout
- **Capacity**: New frames dropped when buffer at max capacity
- **Activation debounce**: One activation event per IP per second

---

## Modules (`distvirt-worker/src/fabric/`)

### `endpoint.rs` — Endpoint table + lifecycle management

`EndpointTable`: unified table replacing the former `ServiceTable` and `RouteTable`. Maps destination IPs to `Endpoint` structs with state, buffer, and backend variant. Indexed by IP for packet-path lookup and by service_id for command dispatch.

`EndpointAction` enum — what the fabric should do with a frame matching an endpoint IP:
- `ServiceForward { pod_ip, service_ip }` — service is ready, forward to backend (caller applies DNAT)
- `Buffered` — frame accepted into buffer
- `Drop` — buffer full or timed out
- `ActivatorActions` / `L4Result` — activator processed the frame
- `RemoteWorker { worker_id }` — forward via tunnel port
- `LocalPod { port_id }` — deliver to local pod port
- `LocalAdapter { port_id }` — forward via channel port
- `NotFound` — no endpoint matches IP

`EndpointSyncEffect` enum — side-effects from applying sync/update:
- `ServiceReady { service_id }` — service became ready, flush buffers
- `FlushPodBuffer { ip }` — pod became locally reachable
- `FlushAdapterBuffer { ip, port_id, frames }` — adapter buffer should flush

Key methods:
- `apply_endpoint_sync(specs, my_worker_id, make_processor, adapter_port_id)` — full replacement from orchestrator spec list
- `apply_endpoint_update(upserted, removed_ips, ...)` — incremental delta
- `lookup_and_buffer(dst_ip, frame) -> (EndpointAction, bool)` — packet-path lookup, returns action + whether activation event should fire. Reachability is checked internally via the endpoint table.
- `mark_service_ready(service_id) -> Option<MarkReadyResult>` — mark ready, returns buffered frames/actions (Passthrough) or L4Result
- `attach_port(ip, port_id)` — attach a port to a LocalPod endpoint, transitioning it to Ready
- `detach_port(port_id)` — detach a port from a LocalPod endpoint, transitioning it back to Pending
- `get_port_id(ip)` — look up the port ID for a LocalPod endpoint by IP
- `is_backend_reachable(ip)` — check whether an endpoint's backend is currently reachable
- `flush_pod_buffer(ip)` — drain buffered frames from UnplacedPod endpoint
- `flush_by_backend_ip(ip)` — drain buffers from all ready services matching a backend IP

### `forwarding.rs` — Packet dispatch + action execution

`FabricContextInner` struct: shared forwarding state holding `ports`, `endpoint_table`, `nat_table`, `gateway_tx`, `event_tx`, `subnet`, `prefix_len`, `gateway_ip`, and `tunnel_ports`. Wrapped in `Arc` for sharing across port read tasks.

`FabricContext` wraps `Arc<FabricContextInner>`, cheap to clone.

`PortGuard` provides RAII cleanup — removes port from port map and calls `endpoint_table.detach_port()` on drop.

**Lock ordering**: endpoint_table → nat_table → ports → tunnel_ports.

`dispatch_frame(packet, source, ctx)` — the main forwarding function:
1. Parse packet, extract destination IP
2. Endpoint table lookup via `lookup_and_buffer()`
3. Route by action:
   - `ServiceForward`: DNAT (rewrite dst IP), insert reverse NAT entry, send to backend port
   - `Buffered`/`Drop`: Emit activation event if debounce allows
   - `ActivatorActions`: Execute each action (replay, set backend need, log)
   - `L4Result`: Send outgoing frames, execute actions, reschedule poll timer
   - `RemoteWorker`: Send via tunnel port
   - `LocalAdapter`: Send via channel port
   - `LocalPod`: Check NAT table for SNAT (return traffic for services), deliver to port
   - `NotFound`: If destination is gateway IP or outside subnet, forward to TUN device; otherwise drop

Port read loop: spawns per-port, reads packets in a loop, calls `dispatch_frame`. `PortGuard` cleans up on exit.

Gateway ingress task: reads packets from gateway channel, calls `dispatch_frame` with `PacketSource::Gateway`.

### `flow.rs` — TCP flow tracking

`FlowTracker`: lightweight TCP flow tracker providing "is this endpoint actively in use?" signals.

`FlowKey`: 5-tuple `(src_ip, dst_ip, protocol, src_port, dst_port)`.

`TcpFlowState` enum: `Opening` → `Established` → `HalfClosed` → `Closed`. Transitions on SYN, SYN+ACK, FIN, RST.

Key methods:
- `track_packet(key, tcp_flags)` — track TCP packet by flags
- `has_active_flows() -> bool` — the demand signal to the orchestrator
- `gc(now)` — remove expired flows

Constants:
- Idle timeout: 300 seconds (hard upper bound even without FIN/RST)
- Closed linger: 5 seconds (allow retransmits after FIN/RST)

Design decisions:
- TCP only for now (UDP deferred — no explicit close signals)
- Flows tracked only on the worker hosting the pod (`LocalPod` backend equivalent). `RemoteSegment` is a dumb forwarder.
- For services with activators, the activator's `BackendNeed` signal takes precedence over flow tracking.

### `port.rs` — Async fabric port

`FramePort` trait: async `recv_frame()` and `send_frame()` abstraction.

`Port` struct wraps an AF_PACKET socket fd in tokio `AsyncFd` via `dup()` — the original fd stays in `TapDevice`, the dup'd fd goes into AsyncFd.

`ChannelPort` struct wraps `mpsc` channels for virtual (non-TAP) ports — used by ingress adapters (WireGuard) and tests.

`FabricPort` enum dispatches between `Tap(Port)` and `Virtual(ChannelPort)`.

### `service_activator.rs` — Service processor (activator integration)

`ServiceProcessor` enum determines how a service endpoint processes incoming packets:
- `Passthrough` — no activator, pure buffer/forward
- `L3 { activator, flow_tracker }` — L3 packet-level processing via WASM activator
- `L4 { activator, stream_manager }` — L4 stream-level processing (smoltcp-backed TCP stack)

Key methods:
- `process_frame(service_id, ip_payload, raw_packet) -> Option<ServiceAction>` — delegates to L3 or L4 path
- `on_mark_ready(service_id)` — pushes `BackendAvailable(true)` event
- `on_backend_update(has_backend, backend_ip)` — pushes `BackendAvailable` event, updates stream manager
- `handle_timeout(service_id)` — L4 only: polls stream manager for TCP timeouts

### `nat.rs` — NAT connection tracking

`NatTable`: `HashMap<NatFlowKey, NatEntry>` for reverse-direction NAT lookup. Used for service DNAT/SNAT.

`NatFlowKey`: 5-tuple. `NatEntry`: `(service_ip, backend_ip, last_seen)`.

Key methods:
- `insert(key, entry)` — insert reverse-direction entry
- `lookup(key)` — look up and update `last_seen`
- `gc(max_age)` — remove stale entries; runs every 60 seconds

### `tunnel.rs` — Inter-worker tunnel support

`TunnelTransport` handles encapsulation for inter-worker traffic. Tunnel ports are registered via `add_tunnel_port(worker_id, port)` and looked up by worker_id when forwarding to `RemoteSegment` endpoints.

### `mod.rs` — `Fabric` struct

Owns the shared context (`FabricContextInner`) and manages port/gateway lifecycle.

`FabricEvent` enum:
- `EndpointActivation { dst_ip, service_id }` — pulse: frame hit endpoint needing activation. Covers both traffic-based activation (buffer miss) and activator `BackendNeed::Traffic` signals.
- `EndpointDemand { ip, service_id, active }` — level: demand changed for an endpoint. Covers both flow tracking transitions (TCP flows started/stopped) and activator `BackendNeed::Active`/`BackendNeed::None` signals.

Events forwarded to worker via `mpsc::Sender<FabricEvent>`. `EndpointActivation` uses `try_send` (lossy pulse). `EndpointDemand` uses `send` for activator-sourced signals (reliable) and `try_send` for flow-tracking signals.

Key methods:
- `new(gateway_ip, prefix_len)` — create fabric with subnet config
- `add_tap_port(tap, pod_ip, guest_mac) -> (PortId, TaskHandle)` — register TAP, flush buffers, spawn read loop
- `add_port_raw(port) -> (PortId, TaskHandle)` — add pre-constructed port
- `add_tunnel_port(worker_id, port) -> (PortId, TaskHandle)` — register tunnel port for remote worker
- `set_gateway(egress_tx, ingress_rx)` — connect gateway; spawns gateway ingress task + NAT GC
- `set_event_channel(tx)` — set event emission channel
- `tables()` — get `Arc<FabricContextInner>` reference
- `flush_service_frames(frames, backend_ip, service_ip)` — drain buffered frames with DNAT
- `send_l4_frames(frames)` — send L4 stream manager packets
- `dispatch_actions(actions, service_id)` — dispatch activator actions

### `gateway/mod.rs` — FabricGateway (smoltcp IP stack)

**smoltcp interface** with the pod subnet gateway IP (configurable per namespace).

**DNS server** (UDP port 53):
- Queries checked against local `DnsRegistry` (service name → IP)
- Local hits: synthesize A-record response (TTL=60)
- Misses: forward to upstream DNS via hickory-resolver
- NXDOMAIN synthesized for upstream misses

**Internet egress via TUN**:
- IP packets destined outside the pod subnet forwarded to TUN device
- Return traffic routed back via endpoint table

**Checksum handling**: Fabric header `NEEDS_CSUM` flag tracks deferred checksum completion for virtio-net offload.

### `gateway/dns.rs` — DNS query parsing + response synthesis

`DnsRegistry`: `Arc<RwLock<HashMap<String, Ipv4Addr>>>`. Synced from orchestrator via `RegistrySync`/`RegistryUpdate` commands.

`DnsForwarder`: wraps `DnsRegistry` + `TokioResolver`. Processes DNS queries — local registry hits get immediate synthetic responses, misses forwarded asynchronously.

### `gateway/tun.rs` — TUN device creation + async I/O

Opens `/dev/net/tun` with `IFF_TUN | IFF_NO_PI | IFF_VNET_HDR`, enables `TUN_F_CSUM` offload. Async read/write via tokio AsyncFd.

### `tap.rs` — TAP device creation

`create_persistent_tap()`: creates TAP device that survives fd closure. `open_packet_socket()`: opens AF_PACKET socket bound to the TAP interface. `TapDevice` struct with Drop impl for cleanup.

### `tests.rs` — Fabric unit tests

Test suite using `ChannelPort` virtual ports covering: IP-based forwarding, gateway forwarding, endpoint buffering, activation debouncing, service NAT (DNAT/SNAT), activator integration, and tunnel port forwarding. No real TAP devices required.

---

## Orchestrator Protocol

### Endpoint Specification

The orchestrator computes one endpoint table per namespace and broadcasts it to all workers. Each worker derives its local endpoint table from the spec using its own `worker_id`.

```rust
struct EndpointSpec {
    ip: Ipv4Addr,
    kind: EndpointKind,
}

enum EndpointKind {
    Service {
        service_id: ServiceId,
        policy: ServicePolicy,
        backend: Option<EndpointPodBackend>,
    },
    Pod {
        placement: Option<EndpointPlacement>,
    },
    WireGuardPeer {
        placement: Option<EndpointPlacement>,
    },
}

struct EndpointPodBackend {
    pod_ip: Ipv4Addr,
    placement: Option<EndpointPlacement>,
    ready: bool,
}

struct EndpointPlacement {
    worker_id: WorkerId,
}
```

**Same data to every worker.** Workers derive local behavior from the spec:

| EndpointKind | `placement == self` | `placement == other` | `placement == None` |
|---|---|---|---|
| Pod | LocalPod (buffer + ready) | RemoteSegment | UnplacedPod (buffer + activate) |
| WireGuardPeer | LocalAdapter | RemoteSegment | UnplacedPod (buffer) |
| Service (backend local) | Service + local backend | — | — |
| Service (backend remote) | Service + RemoteSegment | Service + RemoteSegment | — |
| Service (no backend) | Service, buffering | Service, buffering | Service, buffering |

### Commands (Orchestrator → Worker)

```rust
enum WorkerCommand {
    /// Full endpoint table replacement. Sent on worker connect
    /// and namespace creation.
    EndpointSync {
        namespace_id: NamespaceId,
        endpoints: Vec<EndpointSpec>,
    },
    /// Incremental endpoint updates.
    EndpointUpdate {
        namespace_id: NamespaceId,
        upserted: Vec<EndpointSpec>,
        removed_ips: Vec<Ipv4Addr>,
    },
    // ... other non-endpoint commands (RegistrySync, etc.)
}
```

### Events (Worker → Orchestrator)

```rust
enum WorkerEvent {
    /// Pulse: endpoint received traffic but has no backend.
    /// Replaces both former FabricRouteMiss and ServiceActivation.
    /// Activator BackendNeed::Traffic also maps to this event.
    EndpointActivation {
        namespace_id: NamespaceId,
        ip: Ipv4Addr,
        service_id: Option<ServiceId>,
    },
    /// Level: demand changed for an endpoint.
    /// Replaces both former EndpointFlowStatus and ServiceBackendNeed.
    /// Emitted by flow tracking (TCP flow transitions) and activator
    /// BackendNeed::Active/None signals.
    EndpointDemand {
        namespace_id: NamespaceId,
        ip: Ipv4Addr,
        service_id: Option<ServiceId>,
        active: bool,
    },
}
```

### Orchestrator Endpoint Generation

The orchestrator maintains the canonical endpoint table per namespace via `build_endpoint_specs()`, which produces:

- **Pod endpoints**: one per workload, placement from pod_map
- **Service endpoints**: one per service, backend derived from service state machine
- **WireGuard peer endpoints**: one per connected peer, placement set to the hosting worker

Broadcast triggers:
- **Namespace activation**: `EndpointSync` to all workers
- **New worker joins**: `EndpointSync` to that worker
- **Pod state change** (running/stopped): `EndpointUpdate` for affected workload
- **Service state change**: `EndpointUpdate` for affected service
- **WireGuard peer add/remove**: `EndpointSync` to all workers

### Demand Model

The orchestrator's demand computation is uniform:

```
effective_demand = services_wanting_backend_count
                 + (has_active_flows ? 1 : 0)
```

`EndpointActivation` is the pulse wake signal. The orchestrator routes it to the correct workload/service based on whether `service_id` is present.

`EndpointDemand` is the level signal. When `active` transitions false, the orchestrator knows the endpoint has no active demand (no TCP flows, or activator signals idle).

### DNS Registry

`RegistrySync`/`RegistryUpdate` remain separate from endpoint sync — DNS names map to endpoint IPs but are a different concern (name resolution vs. packet handling).

---

## NAT for Service Traffic

Service endpoints have virtual IPs separate from backend pod IPs. The fabric transparently translates between them:

```
Client pod → Service IP (DNAT to Pod IP) → Backend pod
Backend pod → Pod IP (SNAT to Service IP) → Client pod
```

**Forward path (DNAT)**: When a packet is forwarded from a service IP to its backend pod, the fabric rewrites the destination IP and inserts a reverse NAT entry.

**Return path (SNAT)**: When a packet from a backend hits the NAT table, the source IP is rewritten back to the service IP.

**GC**: NAT entries track `last_seen` timestamps and are garbage-collected every 60 seconds (300-second idle expiry).

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
   - `FabricEvent::EndpointActivation` → `WorkerEvent::EndpointActivation`
   - `FabricEvent::EndpointDemand` → `WorkerEvent::EndpointDemand`
9. Store in `NamespaceState` (includes `tables`, `registry`, adapter tasks/ports, `pods`)

**Endpoint management** (`worker/namespace.rs`):
- `endpoint_sync(namespace_id, endpoints, my_worker_id, activator_runtime)` — applies full endpoint sync, processes effects (ServiceReady, FlushPodBuffer, FlushAdapterBuffer)
- `endpoint_update(namespace_id, upserted, removed_ips, ...)` — applies incremental update, processes effects

**Effect handling** (`handle_endpoint_effects`):
- `ServiceReady`: calls `mark_service_ready()`, flushes buffered frames with DNAT, dispatches activator actions
- `FlushPodBuffer`: resolves IP to port, spawns async task to drain buffered frames
- `FlushAdapterBuffer`: resolves adapter port, spawns async flush task

**Pod launch** (`worker/supervisor.rs:pod_supervisor`):
1. VM launches with TAP device (virtio-net, vhost-net backend)
2. `take_tap()` transfers TAP ownership from VM to fabric
3. `fabric.add_tap_port(tap, network.ip, guest_mac)` — calls `endpoint_table.attach_port()` to transition the LocalPod endpoint to Ready, flushes buffered frames, spawns port read loop
4. Guest configures interface via `ConfigureNetwork` command
5. Port task monitored by pod supervisor — if it exits, the pod is failed

**Pod shutdown**: Port task cleaned up automatically via RAII when supervisor exits.

---

## Multi-Worker Tunneling

For distributed mode, fabric segments on different workers communicate via UDP tunnels. `RemoteSegment` endpoints forward to tunnel ports looked up by `worker_id` in `FabricContextInner.tunnel_ports`.

The orchestrator pushes a **worker registry** to each worker. Workers autonomously establish tunnels to peers with overlapping segment sets. `TunnelTransport` handles encapsulation; the `segment_id` field in the fabric header supports demux.

See **[Fabric Tunnels](fabric-tunnels.md)** for the full design.

---

## Ingress Adapters

External access into the fabric for developer access, shareable URLs, and infrastructure integration. Adapters are worker-level components that demultiplex to per-namespace virtual ports on the fabric via `ChannelPort`. WireGuard peers connected locally become `LocalAdapter` endpoints; peers on remote workers become `RemoteSegment` endpoints.

See **[Ingress Adapters](ingress-adapters.md)** for the full design.

---

## Pod Splice (Local Development)

Splice allows a developer to replace a running pod with their local machine, receiving and sending traffic through the fabric as if they were the pod. This reuses the existing WireGuard ingress adapter.

**Service-level splice**: Redirect a service's backend to the developer's WireGuard peer IP via `EndpointUpdate`. DNAT/SNAT works identically.

**Pod-level splice**: Update the endpoint spec so the pod's IP resolves to the worker hosting the WireGuard peer.

From the fabric's perspective, nothing is special — the WireGuard `ChannelPort` is just another `FabricPort::Virtual`.

---

## Known Issues & Remaining Work

1. ~~`route_miss_wake` demand leak~~ — **Fixed**. `EndpointActivation` with no `service_id` now sets demand, which is naturally cleared by `EndpointDemand { active: false }`.
2. **Hardcoded buffer policy** — UnplacedPod buffer policy (64 frames, 30s) should be configurable.
3. **Lock ordering** — consider type-safe enforcement for fabric locks.
4. **Flow tracker memory bounds** — with many concurrent connections the flow tracker could grow large. Per-endpoint flow limits or LRU eviction may be needed for production.

## Open Design Questions

1. **UDP flow tracking** — TCP has explicit close signals. UDP needs idle timeouts. Per-endpoint configuration or single default? Deferred.
2. **DNS registry derivation** — could be derived from endpoint table, but kept separate to avoid coupling. Only services have DNS names.
