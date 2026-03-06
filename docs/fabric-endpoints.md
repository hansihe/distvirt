# Fabric Endpoints

## Problem

The fabric has two parallel systems for handling traffic to destinations that aren't locally connected:

1. **Service entities** (`ServiceTable`) — rich lifecycle: buffering, activation events, readiness gating, protocol activators, NAT. Used for service-addressed traffic.
2. **Route table** (`RouteTable`) — simple placeholder buffering with debounced route miss events. Used for direct pod-to-pod traffic.

These systems share the same fundamental behavior (buffer packets for an unavailable destination, signal the orchestrator, flush when ready) but have completely different implementations and capabilities. Direct pod traffic gets none of the lifecycle awareness that services enjoy:

- **No idle detection** — `route_miss_wake` is set but never cleared (Known Issue #5 in orchestrator-policy.md). The orchestrator has no way to know when a pod has no active direct connections.
- **No connection tracking** — no equivalent of `BackendNeed` for direct pod traffic. The orchestrator can't distinguish "pod has active TCP sessions" from "pod is idle."
- **No local buffering for unplaced pods** — route table entries are either `Placeholder` (buffer) or `RemoteWorker` (forward). When a pod isn't running anywhere, the placeholder buffers, but there's no readiness gating — packets flush immediately when the TAP port is added, before the application may be ready.
- **No unified demand signal** — services drive demand through `ServiceActivation` and `BackendNeed`; direct pod traffic drives demand through `FabricRouteMiss` which feeds the broken `route_miss_wake` flag. Two separate paths for the same concept.

## Design

### Unified Endpoint Model

Replace both `ServiceTable` and `RouteTable` with a single `EndpointTable` that handles all destination types uniformly. An **endpoint** is any IP destination on the fabric that needs lifecycle management — services, pods, WireGuard peers, future splice targets.

Every endpoint shares the same front-end behavior:

1. **Packet arrives for destination IP**
2. **Endpoint decides what to do** — buffer, forward to local backend, forward to remote segment
3. **Endpoint tracks liveness** — "are there active flows to this destination?"

The difference is only in the **backend strategy**: how packets reach their final destination once the endpoint is ready.

### Endpoint Structure

```rust
struct Endpoint {
    ip: Ipv4Addr,

    // Front-end (shared across all endpoint types)
    buffer: PacketBuffer,
    flow_tracker: FlowTracker,
    state: EndpointState,

    // Back-end (varies by type)
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
        service_id: ServiceId,
        policy: ServicePolicy,
        backend_ip: Option<Ipv4Addr>,
        processor: ServiceProcessor,
    },
    /// Pod running on this worker. Packets forwarded directly to TAP port.
    LocalPod {
        port_id: PortId,
    },
    /// Destination reachable via another worker's fabric segment.
    /// Used for remote pods AND remote WireGuard peers — the local worker
    /// doesn't care what's on the other end, just that it forwards there.
    RemoteSegment {
        worker_id: WorkerId,
    },
    /// Pod not running anywhere. Buffer + emit activation event.
    /// Equivalent to current Placeholder route entries.
    UnplacedPod,
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
    // Service ID → IP index (services are also addressable by ID for commands)
    service_id_to_ip: HashMap<ServiceId, Ipv4Addr>,
    // Activation debounce (shared across all endpoint types)
    last_activation: HashMap<Ipv4Addr, Instant>,
    activation_debounce: Duration,
}
```

The endpoint table replaces:
- `ServiceTable` (services become endpoints with `Service` backend)
- `RouteTable` (placeholder routes become endpoints with `UnplacedPod` backend, remote worker routes become `RemoteSegment` backend)

The `IpPortTable` (direct IP → port lookup for already-connected local pods) remains — it's the fast path for local delivery and is orthogonal to the endpoint concept.

### Packet Dispatch

The dispatch path in `forwarding.rs` becomes:

```
1. Parse packet, extract dst_ip
2. Check IpPortTable (local port?) → fast-path delivery (+ NAT check for return traffic)
3. Check EndpointTable → handle based on endpoint state and backend type
4. Gateway or drop (outside subnet)
```

Step 3 replaces the current two-step "check service table, then check route table" with a single lookup. The endpoint's state and backend type determine the action:

| State | Backend | Action |
|---|---|---|
| Ready | Service | DNAT to backend_ip, insert reverse NAT, forward |
| Ready | LocalPod | Forward to port (already in IpPortTable, shouldn't reach here) |
| Ready | RemoteSegment | Forward via tunnel port |
| Ready | LocalAdapter | Forward to channel port |
| Buffering/Pending | Any | Buffer packet, emit activation event (debounced) |
| Buffering/Pending | Service (with activator) | Delegate to ServiceProcessor |

Service-specific behavior (NAT, activators, L3/L4 processing) stays in the `Service` backend variant. The endpoint model doesn't flatten services into something simpler — it lifts pod traffic up to the same lifecycle standard.

### Flow Tracking

A lightweight flow tracker shared across all endpoint types, providing "is this endpoint actively in use?" signals:

```rust
struct FlowTracker {
    flows: HashMap<FlowKey, FlowState>,
}

struct FlowKey {
    src_ip: Ipv4Addr,
    dst_ip: Ipv4Addr,
    protocol: u8,
    src_port: u16,
    dst_port: u16,
}

struct FlowState {
    first_seen: Instant,
    last_seen: Instant,
    tcp_state: TcpFlowState, // for TCP only
}

enum TcpFlowState {
    /// SYN seen, connection establishing
    Opening,
    /// Established (SYN+ACK seen or data flowing)
    Established,
    /// FIN seen from one side
    HalfClosed,
    /// FIN seen from both sides or RST
    Closed,
}
```

**Tracking rules:**
- **TCP**: Track on SYN. Transition on FIN (half-close → close) and RST (immediate close). Remove on close + brief linger (allow retransmits).
- **UDP**: Track on first packet. Remove on idle timeout (e.g., 30s no packets).
- **Idle timeout**: Hard upper bound (e.g., 5min) even without FIN/RST, preventing orphaned flow entries from keeping an endpoint "active" forever.
- **Signal**: `has_active_flows() -> bool` — the demand signal to the orchestrator.

The same flow tracker can serve services without activators (Passthrough mode). Currently, Passthrough services have no idle detection — the orchestrator relies on activator-driven `BackendNeed`. With flow tracking, all endpoints get idle detection uniformly.

For services *with* activators, the activator's `BackendNeed` signal takes precedence over flow tracking (activators have richer protocol-level knowledge). Flow tracking serves as the fallback for services without activators and as the primary signal for pod endpoints.

**Interaction with NAT**: For service endpoints with NAT, the flow tracker operates on the pre-NAT (service IP) addresses. This means the tracker sees the same 5-tuple as the client, which is correct — we want to track the client's view of the connection, not the backend's.

### Orchestrator Protocol

#### Uniform Endpoint Sync

Replace the separate `FabricRouteSync`/`FabricRouteUpdate`, `CreateService`/`UpdateServiceBackend`/`ServiceReady`/`DestroyService` commands with a unified endpoint sync:

```rust
enum WorkerCommand {
    // ... existing non-fabric commands ...

    /// Full endpoint table replacement. Sent on worker connect
    /// and namespace creation.
    EndpointSync {
        namespace_id: NamespaceId,
        endpoints: Vec<EndpointSpec>,
    },

    /// Incremental endpoint updates.
    EndpointUpdate {
        namespace_id: NamespaceId,
        changed: Vec<EndpointSpec>,
        removed_ips: Vec<Ipv4Addr>,
    },
}
```

The `EndpointSpec` is what the orchestrator sends — a declarative description that every worker receives identically:

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
        pod_id: PodId,
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

**Same data to every worker.** The orchestrator computes one endpoint table and broadcasts it to all workers in the namespace. Each worker knows its own `worker_id` and derives its local endpoint table:

| EndpointKind | `placement.worker_id == self` | `placement.worker_id != self` | `placement == None` |
|---|---|---|---|
| Pod | LocalPod (plug in TAP) | RemoteSegment { worker_id } | UnplacedPod (buffer + wake) |
| Service, backend.placement == self | Service + local backend | Service + RemoteSegment backend | Service, no backend (buffer + wake) |
| Service, backend.placement == other | Service + RemoteSegment backend | Service + RemoteSegment backend | Service, no backend (buffer + wake) |

The derivation is pure and deterministic. Workers never need to reason about scheduling or placement — they just interpret the spec relative to their own identity.

**Readiness gating** is handled by the `ready: bool` field on `EndpointPodBackend`. The orchestrator sets this to `true` when the pod reports `PodRunning` (or passes a future readiness probe). Until then, the endpoint stays in `Pending` state — packets buffer even though placement is known. This replaces the current `ServiceReady` command.

For pod endpoints, readiness is implicit — the TAP port being added to the fabric is the readiness signal. Packets buffer in the `UnplacedPod` or `RemoteSegment` state until the TAP port registers in the `IpPortTable` and the endpoint transitions to `Ready`.

#### DNS Registry

`RegistrySync`/`RegistryUpdate` remain separate — DNS names map to endpoint IPs but are a different concern (name resolution vs. packet handling). The registry could be derived from the endpoint table, but keeping it separate avoids coupling DNS with packet-path changes.

#### Events (Worker → Orchestrator)

```rust
enum WorkerEvent {
    // ... existing non-fabric events ...

    /// An endpoint received traffic but has no backend.
    /// Replaces both FabricRouteMiss and ServiceActivation.
    EndpointActivation {
        namespace_id: NamespaceId,
        ip: Ipv4Addr,
        /// For service endpoints, include the service_id for
        /// orchestrator-side routing to the correct service SM.
        service_id: Option<ServiceId>,
    },

    /// Flow tracking status changed for an endpoint.
    /// Replaces BackendNeed for non-activator endpoints.
    /// Provides the missing "is this pod in use?" signal.
    EndpointFlowStatus {
        namespace_id: NamespaceId,
        ip: Ipv4Addr,
        has_active_flows: bool,
    },

    /// Activator-driven backend need (services with activators only).
    /// Kept separate because activators have richer semantics than
    /// flow counting (e.g., BackendNeed::Traffic vs Active).
    ServiceBackendNeed {
        namespace_id: NamespaceId,
        service_id: ServiceId,
        need: BackendNeed,
    },
}
```

### Orchestrator Impact

#### Demand Model

The orchestrator's demand computation becomes uniform:

```
effective_demand = services_wanting_backend_count
                 + (any_pod_endpoint_has_active_flows ? 1 : 0)
```

This replaces the current `current_demand` (from services) + `route_miss_wake` (broken boolean) with a clean two-source model. Both sources flow through the same activation → demand pipeline.

`EndpointActivation` replaces both `FabricRouteMiss` and `ServiceActivation` as the wake signal. The orchestrator routes it to the correct workload/service SM based on whether `service_id` is present.

`EndpointFlowStatus` provides the missing idle signal for direct pod traffic. When `has_active_flows` transitions from `true` to `false`, the orchestrator knows the pod has no active direct connections — analogous to `BackendNeed::None` for services.

#### Endpoint Table Generation

The orchestrator maintains the canonical endpoint table per namespace. On state changes (pod placed, pod running, service activated, etc.), it recomputes affected endpoints and broadcasts `EndpointUpdate` to all workers. The generation logic lives in the namespace SM's output layer, replacing the current per-command emission in `output.rs`.

```
Namespace state change
  → recompute affected EndpointSpecs
  → diff against last-broadcast state
  → emit EndpointUpdate { changed, removed_ips } to all active workers
```

Full `EndpointSync` is sent when a worker first joins the namespace (on `FabricCreated`).

## Interaction with Existing Systems

### Service Activators

Protocol activators (L3/L4) are preserved. They operate on the `Service` backend variant. The `ServiceProcessor` enum stays as-is — it's the service-specific processing pipeline that runs *within* an endpoint.

The change is structural: `ServiceProcessor` is no longer associated with `ServiceEntity` (which goes away) but with the `Service` variant of `EndpointBackend`.

### NAT Table

The NAT table (`NatTable`) stays as-is. It's an optimization for return-path traffic that's orthogonal to endpoint management. Service endpoints with NAT continue to insert reverse NAT entries on forward, and the dispatch path continues to check NAT before endpoint lookup.

### IpPortTable

The IP-to-port table remains the fast path for local delivery. When a pod's TAP port is added, it registers in the `IpPortTable`. The dispatch path checks this first — most packets in steady state hit this path and never touch the endpoint table.

The endpoint table is for packets that *can't* be delivered via the fast path: the destination isn't locally connected (unplaced pod, remote pod, service VIP).

### Multi-Worker Tunnels

`RemoteSegment` backend replaces the current `RouteDestination::RemoteWorker`. The tunnel port mapping (`tunnel_ports: HashMap<WorkerId, PortId>`) stays in `FabricContextInner`. When an endpoint has a `RemoteSegment` backend, the dispatch path looks up the tunnel port for that worker and forwards.

### WireGuard / Ingress Adapters

WireGuard peers connected locally are `LocalAdapter` endpoints. Peers on remote workers become `RemoteSegment` endpoints on the local worker. This is transparent — the local worker just forwards to the remote segment, which knows how to deliver to the WireGuard channel port.

## Migration Path

### Phase 1: Introduce EndpointTable with Service Backend ✅

- ~~Create `EndpointTable` alongside existing `ServiceTable`~~
- ~~Implement `Endpoint` struct with `Service` backend variant~~
- ~~Migrate service creation/update/ready/destroy to operate on `EndpointTable`~~
- ~~Remove `ServiceTable`~~
- ~~Verify service tests pass with the new structure~~

Completed: `ServiceTable` replaced by `EndpointTable` in `fabric/endpoint.rs` with `EndpointState` enum (`Buffering`/`Pending`/`Ready`), `EndpointBackend::Service` variant (exhaustive match ensures Phase 2 gets compile errors), and shared buffer on the `Endpoint` struct. All 117 tests pass. `service.rs` deleted.

### Phase 2: Add Pod Backend Variants ✅

- ~~Add `UnplacedPod`, `RemoteSegment` backend variants~~
- ~~Migrate route table entries to endpoints~~
- ~~Add flow tracking (shared implementation)~~
- ~~Remove `RouteTable`~~
- ~~Wire `EndpointActivation` event (replaces both `FabricRouteMiss` and `ServiceActivation`)~~

Completed: `RouteTable` removed (`route.rs` deleted). `EndpointBackend` now has `UnplacedPod { buffer_policy }` and `RemoteSegment { worker_id }` variants. `ServiceAction` renamed to `EndpointAction` with new variants `RemoteWorker` and `NotFound`. Route management moved to `EndpointTable` via `route_sync()`, `route_update()`, and `flush_pod_buffer()`. Shared buffer logic extracted into `try_buffer_frame()` helper. `FabricEvent` unified: `RouteMiss` and `ServiceActivation` replaced by single `EndpointActivation { dst_ip, service_id: Option<String> }`. Event bridge in `namespace.rs` maps back to protocol events (`ServiceActivation` / `FabricRouteMiss`). `FlowTracker` added in `fabric/flow.rs` (TCP-only, structural, not wired to events). `LocalPod` variant deferred — local pods use the `IpPortTable` fast path. All 123 tests pass (115 unit + 8 flow tracker).

### Phase 3: Unified Protocol Commands ✅

- ~~Add `EndpointSync`/`EndpointUpdate` to worker protocol~~
- ~~Implement orchestrator-side endpoint table generation~~
- ~~Implement worker-side derivation (spec → local endpoint table)~~
- ~~Wire `EndpointFlowStatus` event~~
- ~~Remove old commands (`FabricRouteSync`, `FabricRouteUpdate`, `CreateService`, `UpdateServiceBackend`, `ServiceReady`, `DestroyService`)~~
- ~~Update orchestrator demand model (remove `route_miss_wake`)~~

Completed: `EndpointSync`/`EndpointUpdate` added to worker protocol with full Cap'n Proto serialization. Old commands removed from Rust types (deprecated stubs in schema return errors on deserialization). Orchestrator generates `EndpointSpec` via `build_endpoint_specs()` in namespace `mod.rs` and broadcasts via `emit_endpoint_sync()`/`emit_endpoint_update_for_workload()`/`emit_endpoint_update_for_service()`. Worker applies specs via `apply_endpoint_sync()`/`apply_endpoint_update()` with effect processing (`ServiceReady`, `FlushPodBuffer`). `EndpointActivation` replaces both `FabricRouteMiss` and `ServiceActivation`. `EndpointFlowStatus` wired for flow tracking. Demand model unified: `effective_demand = service_demand + route_miss` using `has_active_flows` and `route_miss_wake`. All tests migrated to new protocol, stateright model updated. Compose orchestrator uses new protocol. Known issue: `route_miss_wake` demand leak (flag not always cleared when service takes over demand), documented in `endpoint-migration-audit.md` with a `#[should_panic]` test.

### Phase 4: LocalAdapter Backend

- Add `LocalAdapter` variant for WireGuard peers and splice targets
- Unify WireGuard peer management with endpoint model

### Remaining work

1. **Fix `route_miss_wake`** — clear when service activates and takes over demand (item 2)
2. **Hardcoded buffer policy** — extract UnplacedPod buffer policy to a configurable constant (item 7)
3. **Lock ordering** — consider type-safe enforcement for fabric locks (item 7)

## Open Questions

1. **Flow tracker memory bounds** — With many concurrent connections, the flow tracker could grow large. Should there be a per-endpoint flow limit? LRU eviction? For the staging use case (few concurrent users), this is unlikely to matter, but production would need bounds.
Decision: Deferred.

2. **UDP flow timeout** — TCP has explicit close signals (FIN/RST). UDP doesn't. What's the right idle timeout for UDP flows? DNS is fire-and-forget (short timeout). Long-lived UDP streams (game servers, etc.) need longer timeouts. Per-endpoint configuration, or a single reasonable default (30s)?
Decision: Deferred, only support TCP flow tracking for now.

3. **Flow tracking for RemoteSegment** — When a pod is on a remote worker, should the local worker track flows? The remote worker's endpoint (with LocalPod backend) already tracks flows and reports to the orchestrator. Double-tracking wastes memory. But the local worker can detect faster when *its* clients stop talking. Recommendation: only track flows on the worker hosting the pod (LocalPod backend). RemoteSegment is a dumb forwarder.
Decision: RemoteSegment is a dumb forwarder, flows are only tracked on host worker.

4. **Endpoint readiness for pod endpoints** — For service endpoints, `ready: bool` gates packet delivery (orchestrator controls it). For pod endpoints, readiness is implicit (TAP port added). Should pod endpoints also have explicit readiness gating? This matters if we add readiness probes — a pod's TAP is connected but the application inside isn't ready yet. Currently the packet hits IpPortTable directly; with explicit readiness, we'd need to gate at the endpoint level even for local pods.
Decision: We want readiness gating in the future, system should be prepared for this.

5. **Backward compatibility** — The protocol change (EndpointSync replacing multiple commands) breaks wire compatibility. Since the protocol is internal and not versioned, this is acceptable. But it means the worker and orchestrator must be upgraded together.
Decision: This is not a concern, system is not deployed anywhere yet.

6. **DNS registry derivation** — Should the DNS registry be derived from the endpoint table? Every service endpoint has an IP; the registry maps names to those IPs. Deriving it would eliminate the separate RegistrySync/RegistryUpdate commands. Trade-off: coupling DNS updates with endpoint updates vs. eliminating a parallel sync mechanism. Note: pods don't typically have DNS names, so this would only cover services.
Decision: DNS stays as a separate registy, orchestrator manages mapping.
