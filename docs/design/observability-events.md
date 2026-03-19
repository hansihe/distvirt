# Observability Events — Design Document

## Context

The orchestrator needs to emit structured events for debugging, CLI streaming
(`StreamEvents` gRPC), and future monitoring. The old orchestrator emitted
events imperatively at each state transition — fragile, easy to miss spots, and
diverges from actual state over time.

The new orchestrator is signal-based: state machines expose output signals, the
router tracks changes via incremental aggregation, and adapters reconcile
deltas. Events should be derived from this existing machinery rather than
hand-emitted.

## Goal

A future-proof event system that:

- Is **complete by construction** — every state transition produces events
  automatically, no risk of forgetting emission in a code path.
- Is **consistent** — events always reflect actual state, not a parallel
  bookkeeping path.
- Is **zero maintenance** — new signals produce events without touching event
  code. Adding a new SM field = adding a signal + match arm.
- Is **efficient** — piggybacks on the router's existing change tracking.
- Is **easy to extend** — adding new observable dimensions is additive.

## Design

### Observability Port

Add a single singleton `Observability` port to the router. All SMs create an
edge to this port on initialization. The port receives multiple distinct input
types — one per "observable concern," not one per SM type.

This is the same pattern as the existing singleton ports (`Timer`,
`ScheduleRequest`, `FabricEndpoint`, `DnsRegistry`). The observability port
is just another consumer of signals that SMs already produce (or should
produce as proper output signals).

```
Port: Observability(ObservabilityId)  // singleton, like Timer or ScheduleRequest
```

### Signals, Not Internals

Events are derived from **output signals** on the SMs, not by inspecting SM
internal fields. Where a signal doesn't exist yet for an observable concern,
it is added as a proper output signal on the SM — it becomes part of the SM's
public interface. Other SMs or adapters may find it useful too.

This means:

- Pod phase transitions → derived from `Pod::Status` signal (already exists).
- Pod worker assignment → derived from `Pod::AssignedWorker` signal (already
  exists).
- Workload status → derived from `Workload::Status` signal (already exists).
- Workload demand → derived from `Workload::DemandInput` aggregation (the
  demand count is already computed).
- Endpoint state → derived from `Endpoint::Status` signal (already exists).
- Endpoint idle timer → derived from `Endpoint::IdleTimerActive` signal
  (already exists).

Some concerns need new signals:

- `Workload::Spliced(bool)` — whether the workload is currently spliced.
- `Workload::BackoffState(Option<BackoffInfo>)` — current backoff attempt +
  delay, or None if not in backoff.
- `Endpoint::Phase(EndpointPhase)` — higher-level phase than the raw
  `EndpointStatus` (Idle / NeedBackend / Active).

The key principle: if something is worth observing, it's worth being a proper
signal. Don't create "shadow" state just for events.

### Router Integration

Each SM makes an edge to the observability port on init. The port declares
incremental aggregator inputs over the signals it cares about. The router's
existing change tracking produces deltas automatically.

The `router!` macro supports duplicate block keywords — multiple `ports`,
`edges`, and `inputs` blocks are merged. This means observability declarations
live in their own section at the end of the topology, cleanly separated from
core logic:

```rust
router! {
    // ── Core topology ──────────────────────────────────
    state_machines { ... }
    ports { ... }
    signals { ... }
    edges { ... }
    events { ... }
    inputs { ... }

    // ── Observability ──────────────────────────────────
    ports {
        Observability(ObservabilityId),
    }
    edges {
        PodObservability: Pod -> Observability,
        WorkloadObservability: Workload -> Observability,
        EndpointObservability: Endpoint -> Observability,
    }
    inputs {
        // Pod lifecycle tracking
        Observability::PodStatusInput {
            sources: [(PodObservability, Pod::Status)],
            incremental_aggregator: PodStatusObservabilityAggregator,
        },
        Observability::PodWorkerInput {
            sources: [(PodObservability, Pod::AssignedWorker)],
            incremental_aggregator: PodWorkerObservabilityAggregator,
        },

        // Workload lifecycle tracking
        Observability::WorkloadStatusInput {
            sources: [(WorkloadObservability, Workload::Status)],
            incremental_aggregator: WorkloadStatusObservabilityAggregator,
        },
        Observability::WorkloadSplicedInput {
            sources: [(WorkloadObservability, Workload::Spliced)],
            incremental_aggregator: WorkloadSplicedObservabilityAggregator,
        },

        // Endpoint lifecycle tracking
        Observability::EndpointStatusInput {
            sources: [(EndpointObservability, Endpoint::Status)],
            incremental_aggregator: EndpointStatusObservabilityAggregator,
        },
    }
}
```

Each input uses an incremental aggregator. The aggregator receives add/change/
remove deltas and produces event records. This is the same mechanism used by
`PodAssignmentIncrementalAggregator`, `ScheduleRequestIncrementalAggregator`,
etc.

### Incremental Aggregators Produce Events

The observability incremental aggregators don't reduce to a single value —
they accumulate a `Vec<ObservabilityEvent>` that the adapter drains after
propagation. Each delta (signal changed from A to B for SM instance X) maps
to one or more events.

```rust
struct PodStatusObservabilityAggregator {
    events: Vec<ObservabilityEvent>,
}

impl IncrementalAggregator for PodStatusObservabilityAggregator {
    // ...
    fn on_change(&mut self, pod_id: PodId, old: &PodStatus, new: &PodStatus) {
        self.events.push(ObservabilityEvent::Pod {
            pod_id,
            event: pod_transition_event(old, new),
        });
    }

    fn on_add(&mut self, pod_id: PodId, value: &PodStatus) {
        self.events.push(ObservabilityEvent::Pod {
            pod_id,
            event: PodEvent::Created,
        });
    }

    fn on_remove(&mut self, pod_id: PodId, value: &PodStatus) {
        self.events.push(ObservabilityEvent::Pod {
            pod_id,
            event: PodEvent::Reaped,
        });
    }
}
```

### Observability Adapter

The observability adapter is structurally identical to other adapters. It runs
in the reconcile loop (last, since it's read-only and never writes back to the
router). It drains the accumulated events from the incremental aggregators.

```rust
pub struct ObservabilityAdapter;

impl ObservabilityAdapter {
    pub fn reconcile(router: &mut DRouter) -> Vec<ObservabilityEvent> {
        let mut events = Vec::new();

        // Drain each incremental aggregator's accumulated events
        router.drain_observability_pod_status_events(&mut events);
        router.drain_observability_pod_worker_events(&mut events);
        router.drain_observability_workload_status_events(&mut events);
        router.drain_observability_workload_spliced_events(&mut events);
        router.drain_observability_endpoint_status_events(&mut events);

        events
    }
}
```

Since the adapter never writes back to the router, it cannot trigger
re-propagation. It has no ordering dependencies with other adapters —
it just reads the final converged state.

### Event Flow

```
SM signal changes during propagate()
  → Router's incremental aggregator on Observability port detects delta
  → Aggregator accumulates ObservabilityEvent
  → Reconcile phase: ObservabilityAdapter drains events
  → Events added to InternalNamespaceEffects
  → NamespaceWithBoundary translates router IDs → protocol names
  → Events added to NamespaceEffects
  → Shell publishes to EventBus
  → gRPC StreamEvents subscribers receive events
```

### Adding Context (Workload ID on Pod Events, etc.)

Pod events should carry `workload_id` as context. The observability adapter can
look up the pod's owner from the router's edge graph (the `PodOwnership` edge
from Workload → Pod). This is a read of existing topology, not SM internals.

Similarly, endpoint events carry context about their owning service or workload
via the `ServiceEndpointOwnership` / `WorkloadEndpointOwnership` edges.

The adapter performs these lookups when constructing events. This keeps the SMs
clean — they don't need to emit their parent's ID as a signal.

## Event Taxonomy

### Entity Scoping

Events are scoped to four entity levels. Each event carries its entity's ID
plus optional context IDs for the parent entity.

### Namespace Events

Top-level events about the namespace itself.

| Event | Trigger |
|---|---|
| `SpecUpdated` | Management adapter applies new spec |
| `WorkerJoined { worker_id }` | Worker port activated for this namespace |
| `WorkerLost { worker_id }` | Worker port deactivated |

Namespace events are not derived from the observability port — they come from
the management adapter and shell directly. They are included here for
completeness of the event taxonomy.

### Workload Events

| Event | Source Signal |
|---|---|
| `DemandChanged { count }` | `Workload::Status` transition involving demand |
| `SpecUpdated` | `Workload::SpecStale` or management adapter |
| `BackoffStarted { attempt, delay }` | New: `Workload::BackoffState` |
| `BackoffCleared` | New: `Workload::BackoffState` → None |
| `MaxRetriesExhausted` | `Workload::Status` → Failed |
| `Spliced { worker_id }` | New: `Workload::Spliced` |
| `Unspliced` | New: `Workload::Spliced` → false |

### Pod Events

Pods are first-class entities. Events carry `workload_id: Option` as context.

| Event | Source Signal |
|---|---|
| `Created` | Aggregator `on_add` |
| `Scheduled { worker_id }` | `Pod::AssignedWorker` None → Some |
| `Launching { worker_id }` | `Pod::Status` Pending → Running (when worker assigned) |
| `Running { worker_id }` | `Pod::Status` → Running |
| `Stopped { exit_code }` | `Pod::Status` → Finished |
| `Failed { reason }` | `Pod::Status` → Failed |
| `Suspending { worker_id }` | `Pod::Status` → Suspending |
| `Suspended { artifact_id }` | `Pod::Status` → Suspended |
| `SuspendFailed { reason }` | `Pod::Status` → Failed (from Suspending) |
| `Resuming { worker_id }` | Aggregator `on_add` with resume artifact |
| `Displaced { worker_id }` | `Pod::Status` → Displaced |
| `Reaped` | Aggregator `on_remove` |

### Endpoint Events

Endpoints are first-class entities. Events carry `service_id: Option` and
`workload_id: Option` as context.

| Event | Source Signal |
|---|---|
| `Activated { trigger }` | `Endpoint::Status` → NeedBackend or Active |
| `BackendReady` | `Endpoint::Status` → Active (has readiness) |
| `IdleTimerStarted { timeout }` | `Endpoint::IdleTimerActive` → true |
| `IdleTimerCancelled { reason }` | `Endpoint::IdleTimerActive` → false (while active) |
| `IdleTimeoutFired` | `Endpoint::Status` transition from timer |
| `Deactivated { reason }` | `Endpoint::Status` → Idle |

## EventBus

The EventBus follows the same pattern as the existing `LogBus`: a per-namespace
ring buffer with subscriber fan-out. It sits in the async shell, outside the
pure core.

```rust
pub struct EventBusHandle {
    inner: Arc<Mutex<EventBusInner>>,
}

impl EventBusHandle {
    pub fn publish(&self, namespace_id: &NamespaceId, events: Vec<ObservabilityEvent>);

    pub fn subscribe(
        &self,
        namespace_id: &NamespaceId,
        filter: EventFilter,
    ) -> (Vec<ObservabilityEvent>, mpsc::Receiver<ObservabilityEvent>);
}

pub struct EventFilter {
    pub workload_ids: Option<Vec<String>>,
    pub service_ids: Option<Vec<String>>,
    // Empty = all events
}
```

The gRPC `StreamEvents` implementation subscribes to the EventBus, converts
events to proto, and streams them to the client.

## Proto Changes

```protobuf
message NamespaceEvent {
    int64 timestamp_unix_ms = 1;
    oneof event {
        NamespaceLevelEvent namespace = 2;
        WorkloadEvent workload = 3;
        PodEvent pod = 4;
        EndpointEvent endpoint = 5;
    }
}

message PodEvent {
    string pod_id = 1;
    optional string workload_id = 2;  // context, not guaranteed
    oneof event {
        PodCreated created = 3;
        PodScheduled scheduled = 4;
        PodLaunching launching = 5;
        PodRunning running = 6;
        PodStopped stopped = 7;
        PodFailed failed = 8;
        PodSuspending suspending = 9;
        PodSuspended suspended = 10;
        PodSuspendFailed suspend_failed = 11;
        PodResuming resuming = 12;
        PodDisplaced displaced = 13;
        PodReaped reaped = 14;
    }
}

message EndpointEvent {
    string endpoint_id = 1;
    optional string service_id = 2;
    optional string workload_id = 3;
    oneof event {
        EndpointActivated activated = 4;
        EndpointBackendReady backend_ready = 5;
        EndpointIdleTimerStarted idle_timer_started = 6;
        EndpointIdleTimerCancelled idle_timer_cancelled = 7;
        EndpointIdleTimeoutFired idle_timeout_fired = 8;
        EndpointDeactivated deactivated = 9;
    }
}

message WorkloadEvent {
    string workload_id = 1;
    oneof event {
        WorkloadDemandChanged demand_changed = 2;
        WorkloadSpecUpdated spec_updated = 3;
        WorkloadBackoffStarted backoff_started = 4;
        WorkloadBackoffCleared backoff_cleared = 5;
        WorkloadSpliced spliced = 6;
        WorkloadUnspliced unspliced = 7;
        WorkloadMaxRetriesExhausted max_retries_exhausted = 8;
    }
}

message NamespaceLevelEvent {
    oneof event {
        NamespaceSpecUpdated spec_updated = 1;
        NamespaceWorkerJoined worker_joined = 2;
        NamespaceWorkerLost worker_lost = 3;
    }
}
```

## Extension Pattern

Adding a new observable dimension:

1. **Add or identify the signal** on the SM (e.g. `Workload::NewThing(NewThingState)`).
   This is a proper output signal — it's part of the SM's public interface.
2. **Add the edge and input** to the observability port in the `router!` macro.
3. **Implement the incremental aggregator** — map (old, new) → events.
4. **Add a drain call** in the observability adapter.
5. **Add proto messages** if exposing to clients.

Steps 1-2 are topology declaration (a few lines in the macro). Step 3 is a
small aggregator impl. Step 4 is one line. Step 5 is proto additions.

No changes to SM logic, no changes to other adapters, no changes to the
reconcile loop structure.

## Implementation Status

### Core infrastructure (done)

- `Observability` port, edges, and 5 incremental inputs in `router!` macro
- `ObservabilityAdapter` in `src/adapter/observability/mod.rs` — drains events,
  runs last in reconcile loop (read-only, never re-triggers propagation)
- 5 incremental aggregators deriving events from existing signals:
  `Pod::Status`, `Pod::AssignedWorker`, `Workload::Status`,
  `Endpoint::Status`, `Endpoint::IdleTimerActive`
- Event types: `ObservabilityEvent` enum with `Pod`/`Workload`/`Endpoint` variants
- `EventBusHandle` in `src/event_bus.rs` — per-namespace ring buffer (1024 events)
  with multi-subscriber fan-out via tokio mpsc
- Wired end-to-end: SM init edges → router → adapter → namespace effects →
  boundary passthrough → orchestrator effects → async shell → EventBus

### Not yet implemented

- New SM signals (`Workload::Spliced`, `Workload::BackoffState`, etc.)
- Context enrichment (workload name on pod events, service name on endpoint events)
- Proto message definitions and gRPC `StreamEvents` integration
- `EventFilter` on subscribe (currently subscribes to all events in a namespace)
- Namespace-level events (spec updated, worker joined/lost)
