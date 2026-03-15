# Port Adapters — Design Document

## Context

The signal router framework replaces the old output-driven SM architecture with
a declarative signal/edge system. SMs communicate through the router; the
outside world communicates through **ports**. Ports are the boundary between the
deterministic, stateright-testable SM graph and the imperative async runtime
(tokio, worker connections, gRPC, timers).

This document designs the **adapter layer** — the thin code that bridges each
port type to the async shell. Each adapter has a simple contract:

1. **Push** external events into the router (set signals, send events,
   create/remove ports).
2. **Drain** port inputs after `propagate()`.
3. **Diff** the drained aggregated values against cached state.
4. **Perform** trivial imperative actions (spawn timer, send wire command, etc.).

All interesting sequencing lives in the SMs. Adapters are intentionally dumb —
they contain no decision logic, no state machine behavior, and no ordering
subtleties. This is critical: ports fall outside stateright verification, so
their correctness must be obvious by inspection.

## Architecture

### One router per namespace

Each namespace gets its own `Router` instance. This enables:

- Parallel reconciliation across namespaces.
- Independent lifecycle (create/destroy namespace = create/destroy router).
- Each router has its own set of port instances and adapter state.

### Shell event loop

```
loop {
    msg = recv()

    // Phase 1: Push external event into router.
    match msg {
        WorkerProtocolEvent => worker_adapter.push(router, event),
        WorkerConnected     => worker_adapter.add(router, conn),
        WorkerDisconnected  => worker_adapter.remove(router, worker_id),
        ClientCommand       => management_adapter.push(router, cmd),
        TimerFired          => timer_adapter.fire(router, timer_id, sm_id, key),
    }

    // Phase 2: Propagate all cascading effects within the SM graph.
    router.propagate()

    // Phase 3: Drain port outputs, reconcile external state.
    timer_adapter.reconcile(router)
    scheduler_adapter.reconcile(router)
    worker_adapter.reconcile(router)
    endpoint_adapter.reconcile(router)

    // Phase 4: Scheduler may have created lease ports — propagate again.
    // The pod SM picks up the lease, sets PodToWorker edge, etc.
    router.propagate()

    // Phase 5: Reconcile again — worker adapter now sees new pod assignments.
    worker_adapter.reconcile(router)
    endpoint_adapter.reconcile(router)
    // (Timer/scheduler are stable — no further round needed.)
}
```

The double-propagate is intentional. The scheduler is an external component that
reacts to router output (pod schedule requests) and feeds back in (lease
grants). This round-trip must go through the SM graph so the Pod SM can
sequence the "lease received → set edge to worker → worker sees pod" flow.
That sequencing is stateright-testable.

Each "component" (adapter) has a single responsibility and trivial sequencing.
Complex interactions are modelled through SMs where they can be verified.

## Topology Changes (Done)

The following topology changes have been implemented to support the adapter layer.

### 1. Worker port: assigned-pods input ✓

The Worker port now has an `AssignedPodsInput` that aggregates `Pod::ScheduleRequest`
signals from pods connected via `PodToWorker` edges:

```
Worker::AssignedPodsInput {
    sources: [(PodToWorker, Pod::ScheduleRequest)],
    aggregator: ListAggregator<PodId, PodScheduleRequest>,
}
```

This gives the worker adapter, after draining `WorkerPortInput`, the current
set of pods assigned to this worker along with their schedule request (which
includes `resume_artifact` for resume-vs-cold-boot decisions).

The adapter diffs this against its cached set:
- Pod appeared → send `LaunchPod` or `ResumePod` to the worker.
- Pod disappeared → send `StopPod` to the worker.

Note: the pod's spec/image is not carried by `PodScheduleRequest` today. We
may need to extend it, or add another signal that carries the pod's launch
config. Alternatively, the spec could be part of the Pod SM's initial state and
exposed as a signal read by the worker adapter. Design this when implementing.

### 2. BackendNeed: separate port type ✓

`BackendNeed` is now a dedicated port type with its own signal (`BackendNeed::Level`)
and edge type (`BackendNeedToService`). The old `Worker::BackendNeed` signal and
`WorkerToService` edge have been removed.

```
ports {
    ...
    BackendNeed(auto),
}
signals {
    ...
    BackendNeed::Level(BackendNeed),
}
edges {
    ...
    BackendNeedToService: BackendNeed -> Service,
}
inputs {
    Service::BackendNeedInput {
        sources: [(BackendNeedToService, BackendNeed::Level)],
        aggregator: BackendNeedAggregator,
    },
}
```

The worker adapter creates one `BackendNeed` port per (worker, service) pair
when the worker reports `ServiceBackendNeed`. It sets the `Level` signal and a
`BackendNeedToService` edge to the target service. When the worker disconnects,
all its `BackendNeed` ports are removed, which naturally clears the need signal
to the services.

This maintains the per-service granularity while keeping the port logic trivial.

## Adapter Specifications

### Timer Adapter (Implemented)

**Location:** `distvirt-orchestrator/src/adapter/timer/mod.rs`

**Port type:** `Timer` (one instance per router)

**Implementation notes:**

The timer adapter is split into a pure reconciliation layer (implemented) and
the async shell integration (not yet implemented). The pure layer returns
`Vec<TimerAction>` — the shell will iterate over these to spawn/cancel tokio
timers and manage `JoinHandle`s.

**Core types:**

```rust
/// Identifies a specific timer across all SM kinds.
enum TimerIdentity {
    Workload(WorkloadId, WorkloadTimerKey),
    Service(ServiceId, ServiceTimerKey),
    Pod(PodId, PodTimerKey),
}

/// Action returned by reconcile — shell executes these.
enum TimerAction {
    Start { identity: TimerIdentity, generation: u64, duration: Duration },
    Cancel { identity: TimerIdentity },
}

/// Duration configuration — adapter owns timer durations, not the SMs.
struct TimerConfig {
    pub retry_backoff: Duration,
    pub launch_timeout: Duration,
    pub suspend_timeout: Duration,
    pub idle_timeout: Duration,
}

/// Pure adapter state. The shell wraps this and adds JoinHandle tracking.
struct TimerAdapter {
    timer_id: TimerId,
    config: TimerConfig,
    /// Active timers: identity → generation.
    active: HashMap<TimerIdentity, u64>,
}
```

**Push (inward):**
- `fire(router, identity)` — called when a tokio timer fires. Dispatches to
  `router.send_workload_timer_fired(...)`, `send_service_timer_fired(...)`, or
  `send_pod_timer_fired(...)` based on the `TimerIdentity` variant.

**Reconcile (outward):**
- `reconcile(&mut self, router) -> Vec<TimerAction>` — drains
  `router.drain_timer_inputs()`, builds the wanted set, diffs against
  `self.active`, returns Start/Cancel actions.
- Each delivery is `(TimerId, TimerPortInput)`. Variants:
  - `WorkloadTimersInput(Vec<(WorkloadId, Vec<TimerRequest>)>)` — flatten
    to a set of `(WorkloadId, key, generation)` tuples.
  - `ServiceTimersInput(...)` — same pattern.
  - `PodTimersInput(...)` — same pattern.
- Diff against `self.active`:
  - Present in wanted but not active (or generation changed) → `TimerAction::Start`.
  - Present in active but not wanted → `TimerAction::Cancel`.
- If no deliveries were received (nothing changed), returns empty vec.

**Partial delivery handling:** Signal dedup means a reconcile call may receive
deliveries for only some input variants (e.g., workload timers changed but
service timers didn't). The adapter preserves its cached state for variants
that had no new delivery, only clearing and rebuilding the portions that did.

**Timer durations:** The adapter owns the mapping from timer key to duration
via `TimerConfig`. This is configuration, not SM logic. The SM declares *what*
timer it wants; the adapter decides *how long*.

**Why generation matters:** When a timer is cancelled and re-requested (e.g.,
pod restarts and gets a new launch timeout), the generation increments. The
adapter sees a generation mismatch, cancels the old timer, and starts a new
one. Without generations, a stale fire from the old timer could be
misdelivered.

**Tests:** `adapter/timer/tests.rs` — 8 unit tests exercise the pure diff
logic using a real `Router` (no tokio). Tests cover: no-op, start, cancel,
dedup stability, generation restart, multiple SM kinds, and fire dispatch for
both workload and service timers.

**Aggregator change:** The timer port inputs were changed from `ListAggregator`
(which drops source IDs) to `IdListAggregator` (which preserves `(Id, V)`
pairs). This is required so the adapter can map timer requests back to their
source SM to build `TimerIdentity`. See "Infrastructure changes" section below.

### Scheduler Adapter

**Port types:** `ScheduleRequest` (singleton, manual ID), `ScheduleLease`
(auto, one per scheduled pod)

**State:**
```rust
struct SchedulerAdapter {
    schedule_request_id: ScheduleRequestId,
    /// Currently scheduled pods. Maps PodId → ScheduleLeaseId.
    scheduled: HashMap<PodId, ScheduleLeaseId>,
}
```

**Reconcile (outward):**
- Drains `router.drain_schedule_request_inputs()`.
- Gets `ScheduleRequestPortInput::PodRequestsInput(Vec<(PodId, PodScheduleRequest)>)`.
- This is the **current** set of pods wanting scheduling (signal-derived, so
  it reflects the full wanted set, not a delta).
- Diff against `self.scheduled`:
  - Pod in wanted but not scheduled → run scheduling decision (pick worker
    based on capacity, locality, etc.), create `ScheduleLease` port, set
    `Lease` signal with assigned `WorkerId`, set `ScheduleLeaseToPod` edge.
    Record in `self.scheduled`.
  - Pod in scheduled but not wanted → remove `ScheduleLease` port. Pod SM
    sees lease revoked via `LeaseInput(None)` and handles preemption.
    Remove from `self.scheduled`.

**No direct worker interaction.** The scheduler never sends commands to
workers. The flow is:
1. Scheduler creates lease → propagate.
2. Pod SM receives `LeaseInput(Some(worker))` → sets `PodToWorker` edge.
3. Worker adapter sees pod in `AssignedPodsInput` → sends `LaunchPod` to wire.

This means the entire scheduling → assignment → launch sequence flows through
the SM graph and is stateright-testable (except the trivial scheduling decision
itself).

### Worker Adapter

**Port type:** `Worker` (one instance per connected worker)

**State:**
```rust
struct WorkerAdapter {
    /// Maps WorkerId → (Worker port ID, protocol writer, cached assigned pods).
    workers: HashMap<WorkerId, WorkerState>,
}

struct WorkerState {
    port_id: WorkerId,  // same as WorkerId in current topology
    writer: OrchestratorWriter,
    /// Cached set of pods assigned to this worker (from last reconcile).
    assigned_pods: HashMap<PodId, PodScheduleRequest>,
}
```

**Push (inward):**
- `add(router, conn)` — worker connects. Create Worker port via
  `router.create_worker()`, set `router.set_worker_info(id, info)`. Spawn
  reader task for protocol events.
- `remove(router, worker_id)` — worker disconnects. Call
  `router.remove_worker(id)`. This clears all `WorkerToPod` edges,
  causing Pod SMs to see `WorkerInput(None)` → Failed. Also remove any
  associated `BackendNeed` ports.
- Protocol events → router calls:
  - `PodRunning` → `router.send_notify_pod_status(worker_id, pod_id, PodStatus::Running)`
  - `PodFailed` → `router.send_notify_pod_status(worker_id, pod_id, PodStatus::Failed)`
  - `PodExited { code: 0 }` → `router.send_notify_pod_status(..., PodStatus::Finished)`
  - `PodExited { code: !0 }` → `router.send_notify_pod_status(..., PodStatus::Failed)`
  - `PodSuspended { artifact_id }` → `router.send_notify_pod_suspended(worker_id, pod_id, artifact_id)`
  - `ServiceBackendNeed` → create/update BackendNeed port (see below)

**Reconcile (outward):**
- Drains `router.drain_worker_inputs()`.
- Gets `(WorkerId, WorkerPortInput::AssignedPodsInput(Vec<(PodId, PodScheduleRequest)>))`.
- Diff against `self.workers[worker_id].assigned_pods`:
  - Pod appeared → send `LaunchPod` or `ResumePod` (based on
    `schedule_request.resume_artifact`) to the worker via protocol writer.
  - Pod disappeared → send `StopPod` to the worker.
- Update cached set.

**Edge management:** The adapter also manages `WorkerToPod` edges. When the
adapter sends `LaunchPod` to a worker for a pod, it should set the
`WorkerToPod` edge so the pod can receive `WorkerInput`. The Pod SM sets
`PodToWorker` (pod → worker direction) when it gets a lease; the adapter sets
`WorkerToPod` (worker → pod direction) when it actually sends the launch
command. This bidirectional edge setup happens naturally:
- Pod sets `PodToWorker` → worker adapter sees pod in `AssignedPodsInput` →
  adapter sets `WorkerToPod` and sends launch command.

Note: setting `WorkerToPod` is a router mutation that needs another propagate.
This can be batched into the phase 5 propagate in the event loop, or the
adapter can set edges during reconcile and a final propagate handles it.

### BackendNeed Adapter

**Port type:** `BackendNeed` (auto, one per worker×service pair)

**State:**
```rust
struct BackendNeedAdapter {
    /// Maps (WorkerId, ServiceId) → BackendNeedPortId.
    ports: HashMap<(WorkerId, ServiceId), BackendNeedId>,
}
```

**Push (inward):**
- Worker reports `ServiceBackendNeed { service_id, need }`:
  - If no port for (worker, service) → create one, set edge to service.
  - Set `BackendNeed::Need` signal to the reported need level.
- Worker disconnects → remove all BackendNeed ports for that worker. The
  signal naturally falls to `None` (aggregated from empty set) on the service.

**Reconcile:** None needed — this adapter is purely push-based. The service SM
reads the aggregated need via `BackendNeedInput` and reacts (start/cancel idle
timer, activate on traffic).

### Management Adapter

**Port type:** `Management` (auto, one or more per namespace)

**State:**
```rust
struct ManagementAdapter {
    /// Management port for workloads.
    workload_mgmt: HashMap<WorkloadId, ManagementId>,
    /// Management port for services.
    service_mgmt: HashMap<ServiceId, ManagementId>,
}
```

**Push (inward):**
- `UpdateSpec` from client:
  - For new workloads: create Workload SM, create Management port, set
    `ManagementToWorkload` edge, set `WlSpec` signal.
  - For new services: create Service SM, create Management port, set
    `ManagementToService` edge, set `SvcSpec` signal.
  - For updated specs: update the signal value. The SM detects the change via
    `SpecInput` and reacts.
  - For removed workloads/services: remove the Management port. The SM sees
    spec go to `None` and self-destructs.
- `AdminRestart` → `router.send_admin_command(mgmt_id, workload_id, AdminCmd::Restart)`
- `ActivateService` → `router.send_activate_service(mgmt_id, service_id, active)`
- `Scavenge` → `router.send_admin_command(mgmt_id, workload_id, AdminCmd::Scavenge)`

**Reconcile:** None needed — purely push-based.

**Port-per-SM vs shared port:** The management adapter can use one port per SM
or share ports. Using one port per workload/service is simplest — the edge
targets exactly one SM, and removing the port cleanly triggers self-destruct
on that SM. A shared port would need careful edge management but could be more
efficient for bulk operations.

### Endpoint Adapter

**Not a port** — this is a post-propagation reader that diffs SM state.

**State:**
```rust
struct EndpointAdapter {
    /// Cached service readiness state.
    cached: HashMap<ServiceId, Option<ReadyInfo>>,
}
```

**Reconcile (outward):**
- After propagate, iterate over all Service SMs.
- For each service, read `SvcStatusSignal` and the readiness info (from the
  service's `ReadyInfo` in its `Active` state).
- Diff against `self.cached`:
  - Service became Active (or changed worker/pod) → send endpoint update to
    relevant workers.
  - Service left Active → send endpoint removal to workers.
- Update cache.

**Alternative:** Instead of reading SM state directly, this could be modelled
as a port with an input aggregating service status signals. That would give us
the drain-and-diff pattern consistently. However, since endpoints are a
broadcast to multiple workers (not a 1:1 relationship), the port model doesn't
fit as naturally. Reading SM state post-propagation is simpler and adequate.

## Infrastructure Changes (Done)

Changes made to support the adapter layer beyond topology changes.

### 1. IdListAggregator ✓

`ListAggregator<Id, V>` drops the source ID during aggregation (`Output =
Vec<V>`). Adapters need source IDs to map port input entries back to their
originating SM (e.g., to build `TimerIdentity::Workload(wl_id, key)`).

`IdListAggregator<Id, V>` preserves pairs (`Output = Vec<(Id, V)>`). Currently
used by all three timer port inputs. Future adapters that need source IDs from
`ListAggregator`-style inputs should use `IdListAggregator` instead.

**Location:** Defined in `sm_new/mod.rs` alongside the other aggregators.

### 2. Macro visibility: pub use for Router ✓

The `router!` macro generates types inside a private `mod __router` and
re-exports `Router` and `RouterSnapshot` into the calling module. These
re-exports were `use` (private) — changed to `pub use` so adapter code in
sibling modules (`adapter::timer`) can access them via `crate::sm_new::Router`.

**Location:** `distvirt-sm-router-macros/src/generate/router.rs`

### 3. Field visibility for adapter access ✓

Several types needed visibility adjustments for adapter use:

- `ServiceTimerKey`: added `Eq, Hash` derives (needed as `HashMap` key in
  `TimerIdentity`).
- `ServiceTimerRequest::key`, `PodTimerRequest::key`: changed to `pub(crate)`
  (adapter reads these fields during reconcile).
- `ServiceId`, `WorkloadId`: inner `u64` field changed to `pub(crate)` (adapter
  tests construct IDs directly).
- `sm_new` submodule re-exports: changed `use service::*` etc. to
  `pub(crate) use service::*` so types are accessible from `adapter::*`.

## Notes for Implementing Future Adapters

### Module structure

Follow the timer adapter pattern:
```
distvirt-orchestrator/src/adapter/
├── mod.rs              # pub(crate) mod timer; pub(crate) mod scheduler; ...
├── timer/
│   ├── mod.rs          # Pure adapter logic
│   └── tests.rs        # Unit tests (no tokio)
└── scheduler/          # Next adapter to implement
    ├── mod.rs
    └── tests.rs
```

### Pure vs shell separation

Keep adapters as pure functions that return action enums. This pattern:
- Makes tests trivial (assert on returned actions, no async runtime needed).
- Keeps the shell event loop simple (iterate actions, perform side effects).
- Ensures adapters stay "dumb" per the design philosophy.

The shell integration (tokio spawning, `JoinHandle` management, channel wiring)
lives outside the adapter, likely in `shell/`.

### Aggregator choice: ListAggregator vs IdListAggregator

- Use `ListAggregator` when the adapter doesn't need to know which SM produced
  each value (e.g., the BackendNeed aggregator already reduces to a single
  priority level).
- Use `IdListAggregator` when the adapter needs source SM IDs to build identity
  keys, dispatch events back, or make per-SM decisions. The timer adapter
  needed this; the scheduler adapter will likely need it too (for
  `ScheduleRequest::PodRequestsInput` — the current `ListAggregator` drops
  `PodId`).

### Reconcile partial-delivery semantics

Port inputs are signal-derived — each delivery is a **full replacement** for
that input variant, not a delta. But a reconcile call may only receive some
variants (signal dedup suppresses unchanged ones). The adapter must:

1. Start from its cached `active` state.
2. For each received variant, clear the corresponding portion of the wanted
   set, then rebuild from the delivery.
3. Leave untouched portions for variants with no delivery.
4. Diff wanted vs active to produce actions.

The timer adapter demonstrates this pattern with `had_workload`/`had_service`/
`had_pod` flags.

### Type visibility

If a future adapter needs access to types defined in `sm_new` submodules
(service.rs, workload.rs, pod.rs), those types should already be accessible
via `crate::sm_new::*` thanks to the `pub(crate) use` re-exports. If the
macro-generated types (e.g., new port input enums) aren't visible, check the
`__router` module re-exports in `distvirt-sm-router-macros/src/generate/router.rs`.

### Testing

Use the same `Router::new(16)` setup as `sm_new/tests/`. Create real SMs,
propagate, and call the adapter's reconcile method. Assert on the returned
action list. The existing test helpers in `sm_new/tests/mod.rs` (like
`setup_workload_with_pending_pod`) are not directly reusable from adapter tests
(they're in a `#[cfg(test)]` submodule), so adapter tests define their own
lightweight setup helpers.

## Event Loop Phases — Detailed

```
recv external event
  │
  ▼
Phase 1: Push into router
  │  (set signals, send events, create/remove ports)
  │
  ▼
Phase 2: router.propagate()
  │  (all SM cascades resolve)
  │
  ▼
Phase 3: Reconcile round 1
  │  timer_adapter.reconcile()    — start/cancel tokio timers
  │  scheduler_adapter.reconcile() — create/remove lease ports
  │  (worker_adapter: nothing yet — pods don't have PodToWorker edges yet)
  │
  ▼
Phase 4: router.propagate()
  │  (Pod SMs receive leases, set PodToWorker edges, etc.)
  │
  ▼
Phase 5: Reconcile round 2
  │  worker_adapter.reconcile()   — see new pods, send LaunchPod
  │  endpoint_adapter.reconcile() — see readiness changes, send updates
  │
  ▼
done (back to recv)
```

In steady state (no new pods being scheduled), phases 3-5 are no-ops: the
scheduler sees no changes, the second propagate has nothing to do, and the
worker/endpoint adapters see no diffs.

## Properties

### Testability boundary

Everything inside the router (SM handlers, signal propagation, edge changes,
event delivery) is deterministic and stateright-verifiable. Everything outside
(adapters) is trivial imperative code: "if X appeared in the diff, do Y."

The adapters never make decisions that affect correctness. The Pod SM decides
when to set `PodToWorker` edges. The Workload SM decides when to create pods.
The Service SM decides when to set demand. Adapters just translate these
decisions into wire commands and timers.

### Adapter correctness by inspection

Each adapter's reconcile method is a pure diff: compare wanted state (from
port drain) against cached state, emit imperative actions for differences.
No conditionals on SM internal state, no ordering dependencies between
adapters, no accumulation of state beyond the cache.

### Failure handling

- **Worker disconnect:** adapter calls `router.remove_worker()`. The router
  clears all edges involving that worker. Pod SMs see `WorkerInput(None)` →
  transition to Failed. BackendNeed ports for that worker are removed →
  services see need drop. All cascades handled by SMs.

- **Stale timer fire:** the timer adapter ignores fires for timers not in
  its active set (already cancelled). If it does deliver a fire event, the
  SM's generation-based guard rejects it (the SM's wanted timer has a
  different generation than the fired one). Belt and suspenders.

- **Scheduler failure:** if the scheduler crashes or is slow, pods remain in
  Pending with a launch timeout. The Pod SM's timer fires → Failed. The
  Workload SM sees the failure and enters retry backoff. No adapter
  involvement needed.
