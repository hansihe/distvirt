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

### One router per namespace, one task per namespace

Each namespace gets its own `Router` instance **and its own tokio task**. This
enables:

- Parallel reconciliation across namespaces (true concurrency).
- Independent lifecycle (create/destroy namespace = spawn/drop task).
- Each router has its own set of port instances and adapter state.
- Namespaces can continue processing events while waiting on external
  decisions (e.g., scheduling).

### Task topology

The system is organized as a set of communicating tasks:

```
                          ┌─────────────────┐
                          │  Worker Reader   │ (one per connection)
                          │     Task         │
                          │                  │
  wire protocol ─────────►│  decode event    │
                          │       │          │
                          │  ┌────┴────┐     │
                          │  │ ns_id?  │     │
                          │  └─┬─────┬─┘     │
                          └────┼─────┼────────┘
               namespace      │     │  global
               events         │     │  events
               ▼              │     ▼
    ┌──────────────┐    │    ┌────────────────────┐
    │ Namespace    │    │    │ Worker State        │
    │ Task         │    │    │ Tracker             │
    │              │    │    │                     │
    │ owns Router  │    │    │ pressure, draining, │
    │ + adapters   │    │    │ conditions, pools   │
    └──────┬───────┘    │    └────────┬────────────┘
           │            │             │ candidates
           │ request/   │             ▼
           │ drop       │    ┌────────────────────┐
           └────────────┼───►│ Global Scheduler   │
                        │    │ Task               │
           ┌────────────┼────│                    │
           │ grant/     │    │ pending, granted   │
           │ revoke     │    └────────────────────┘
           ▼            │
    ┌──────────────┐    │
    │ Namespace    │    │
    │ Task         │    │
    │ (receives    │    │
    │  lease)      │    │
    └──────────────┘
```

**Worker reader tasks** (one per connection) decode wire protocol and demux
events. Namespace-scoped events (pod status, backend need, etc.) route to the
appropriate namespace task's channel. Global events (pressure, conditions,
pool capacity) route to the worker state tracker.

**Namespace tasks** (one per namespace) own their `Router` and all
namespace-local adapters (timer, pod assignment, backend need, endpoint,
management). They run an event loop: receive external events, push into
router, propagate, reconcile adapters. Scheduling is asynchronous — the
namespace sends incremental requests to the global scheduler and receives
lease decisions back without blocking.

**Global scheduler task** receives lease requests from all namespaces,
worker candidate state from the worker state tracker, and makes scheduling
decisions. It never touches a `Router` — it operates purely on pod IDs and
worker IDs.

**Worker state tracker** aggregates global worker events (pressure, conditions,
draining, pool capacity) and provides `WorkerCandidate` snapshots to the
scheduler.

### Namespace event loop

```
loop {
    msg = recv()  // worker events, client commands, timer fires, lease decisions

    // Phase 1: Push external event into router.
    match msg {
        WorkerNamespaceEvent => push into router (send event, set signal, etc.)
        ClientCommand        => management_adapter.push(router, cmd)
        TimerFired           => timer_adapter.fire(router, identity)
        LeaseGranted         => create lease port, set edge
        LeaseRevoked         => destroy lease port
    }

    // Phase 2: Propagate all cascading effects within the SM graph.
    router.propagate()

    // Phase 3: Reconcile adapters, perform side effects.
    timer_actions = timer_adapter.reconcile(router)
    pod_actions = pod_assignment_adapter.reconcile(router)
    schedule_deltas = schedule_request_adapter.reconcile(router)
    // ... execute timer_actions, pod_actions, send schedule_deltas to scheduler
}
```

There is no double-propagate within a single tick. Scheduling is asynchronous:
the namespace sends a lease request to the global scheduler and continues
processing. When the scheduler responds with a grant, it arrives as a new
event in a future iteration of the loop. The namespace pushes the lease into
the router, propagates, and the pod assignment adapter sees the new pod
assignment. This naturally separates the two propagation phases across time.

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
    aggregator: IdListAggregator<PodId, PodScheduleRequest>,
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

### Scheduler — Global Task (Implemented)

**Location:** `distvirt-orchestrator/src/adapter/scheduler/mod.rs` (pure
logic), `distvirt-orchestrator/src/task/scheduler/mod.rs` (async task)

**Port types used by namespaces:** `ScheduleRequest` (singleton, manual ID),
`ScheduleLease` (auto, one per scheduled pod)

**Implementation notes:**

The scheduler runs as a **global task**, separate from any namespace. It
receives incremental lease requests from namespace tasks and worker state
updates from the worker state tracker. It never touches a `Router` — it
operates purely on pod/worker IDs and sends decisions back to namespaces.

The existing `decide()` and `select_worker()` pure functions are reused
as-is. The old `collect()` and `apply()` methods (which operated on a single
`Router`) are replaced by channel-based communication.

**Namespace → Scheduler interface (incremental):**

```rust
/// Sent by namespace tasks to the global scheduler.
enum SchedulerInput {
    /// A pod wants a worker.
    RequestLease {
        namespace_id: NamespaceId,
        pod_id: PodId,
        request: PodScheduleRequest,
        /// Channel to send decisions back to this namespace.
        reply_tx: mpsc::Sender<SchedulerDecision>,
    },
    /// A pod no longer wants a worker (failed, removed, etc.).
    DropRequest {
        namespace_id: NamespaceId,
        pod_id: PodId,
    },
    /// Worker state changed (from WorkerStateTracker).
    WorkerUpdate(WorkerId, WorkerCandidateSnapshot),
    /// Worker disconnected.
    WorkerRemoved(WorkerId),
}

/// Sent by the global scheduler back to a namespace task.
enum SchedulerDecision {
    Grant { pod_id: PodId, worker_id: WorkerId },
    Revoke { pod_id: PodId },
}
```

The namespace side diffs the `ScheduleRequest` port's `PodRequestsInput`
against its cached set to produce incremental `RequestLease`/`DropRequest`
messages. This is the same diff pattern used by other adapters — just instead
of producing action enums, it produces channel messages to the scheduler.

**Scheduler task state:**

```rust
struct Scheduler {
    /// Pods waiting for a worker.
    pending: HashMap<PodId, (NamespaceId, PodScheduleRequest, mpsc::Sender<SchedulerDecision>)>,
    /// Pods already granted.
    granted: HashMap<PodId, (NamespaceId, WorkerId, mpsc::Sender<SchedulerDecision>)>,
    /// Current worker state.
    workers: HashMap<WorkerId, WorkerCandidate>,
}
```

**Scheduler task loop:**

```
loop {
    msg = recv()
    match msg {
        RequestLease { pod_id, request, reply_tx, .. } =>
            try select_worker() → if found, send Grant via reply_tx
            else add to pending (retry when WorkerUpdate arrives)

        DropRequest { pod_id, .. } =>
            if in granted → send Revoke via reply_tx, remove
            if in pending → just remove

        WorkerUpdate { worker_id, snapshot } =>
            update worker state
            retry all pending (a worker may have become available)

        WorkerRemoved { worker_id } =>
            remove from workers
            // Grants on this worker are cleaned up naturally:
            // namespace sees worker disconnect → pod fails →
            // workload recreates → new RequestLease
    }
}
```

**Lease lifecycle on the namespace side:**

When the namespace receives `SchedulerDecision::Grant { pod_id, worker_id }`,
it creates a `ScheduleLease` port in its router, sets the `Lease` signal and
`ScheduleLeaseToPod` edge, then propagates. The Pod SM picks up the lease
and sets `PodToWorker`. When the namespace receives `Revoke` (or the pod
disappears), it destroys the `ScheduleLease` port.

The namespace tracks `scheduled: HashMap<PodId, ScheduleLeaseId>` to map
scheduler decisions back to router lease ports.

**Core pure functions (unchanged):**

```rust
/// Snapshot of a single worker's scheduling-relevant state.
struct WorkerCandidate {
    pub worker_id: WorkerId,
    pub max_pressure_band: PressureBand,
    pub pod_count: usize,
    pub draining: bool,
    pub active: bool,
}

/// select_worker(candidates, pod) -> Option<WorkerId>
/// Hard filter: active && !draining && pressure < High.
/// Soft sort: lowest pressure band, then lowest pod count, then lowest worker ID.
```

**No direct worker interaction.** The scheduler never sends commands to
workers. The flow is:
1. Scheduler sends `Grant` to namespace → namespace creates lease port →
   propagate.
2. Pod SM receives `LeaseInput(Some(worker))` → sets `PodToWorker` edge.
3. Pod assignment adapter sees pod in `AssignedPodsInput` → sends `LaunchPod`.

This means the entire scheduling → assignment → launch sequence flows through
the SM graph and is stateright-testable (except the trivial scheduling decision
itself). The async hop between scheduler and namespace is the only
non-deterministic timing — but the SM graph handles it correctly regardless
of when the grant arrives.

**Aggregator change:** `ScheduleRequest::PodRequestsInput` was changed from
`ListAggregator` (which drops `PodId`) to `IdListAggregator` (which preserves
`(PodId, PodScheduleRequest)` pairs). This is required so the namespace-side
diff logic knows which pod each request belongs to.

**Tests:** `adapter/scheduler/tests.rs` — 8 unit tests exercise the pure
`decide()`/`select_worker()` logic using a real `Router`. Tests cover: no-op,
grant, revoke, pressure-based selection, draining exclusion, no-eligible-worker,
stable state, and inactive worker exclusion. These tests remain valid — they
test the pure scheduling logic independent of the task topology.

### Worker — Reader Task, State Tracker, and Namespace-side Adapters

Worker interaction is split across multiple components because a single worker
connection is cross-namespace (one TCP connection carries events for all
namespaces on that worker), while routers are per-namespace. The components:

1. **Worker reader task** (per connection) — decodes wire protocol, demuxes events.
2. **Worker state tracker** (global) — aggregates health/pressure state.
3. **Pod assignment adapter** (per namespace) — pure diff of assigned pods.
4. **BackendNeed adapter** (per namespace) — push-based, see below.
5. **Writer handle** (per connection, shared) — sends commands to workers.

#### Worker Reader Task

One task per connected worker. Owns the read half of the protocol connection.
Receives raw `WorkerEvent` from the wire, classifies each event, and routes
it to the appropriate consumer.

**State:**

```rust
struct WorkerReaderTask {
    worker_id: WorkerId,
    reader: OrchestratorReader,
    /// Route namespace events to the right namespace task.
    namespace_routes: HashMap<NamespaceId, mpsc::Sender<WorkerNamespaceEvent>>,
    /// Global events (pressure, conditions, etc.)
    global_tx: mpsc::Sender<WorkerGlobalEvent>,
    /// Control channel for registering/unregistering namespace routes.
    control_rx: mpsc::Receiver<WorkerControl>,
}
```

**Event classification:**

Namespace-scoped events (carry `namespace_id` on the wire) are converted to
`WorkerNamespaceEvent` and sent to the namespace task's channel:

```rust
enum WorkerNamespaceEvent {
    worker_id: WorkerId,  // included so namespace knows which worker
    event: WorkerNamespaceEventKind,
}

enum WorkerNamespaceEventKind {
    PodRunning { pod_id: PodId },
    PodExited { pod_id: PodId, exit_code: i32 },
    PodFailed { pod_id: PodId, error: String },
    PodSuspended { pod_id: PodId, artifact_id: ArtifactId, pool_id: PoolId },
    PodSuspendFailed { pod_id: PodId, error: String },
    NamespaceCreated,
    NamespaceFailed { error: String },
    NamespaceDestroyed,
    ServiceBackendNeed { service_id: ServiceId, need: BackendNeed },
    ArtifactWriteStarted { artifact_id: ArtifactId, pool_id: PoolId },
    ArtifactWriteCommitted { artifact_id: ArtifactId, pool_id: PoolId, size_bytes: u64 },
    EndpointActivation { ip: Ipv4Addr, service_id: Option<ServiceId> },
    EndpointFlowStatus { ip: Ipv4Addr, service_id: Option<ServiceId>, has_active_flows: bool },
}
```

Global events (no `namespace_id`) are sent to the worker state tracker:

```rust
enum WorkerGlobalEvent {
    PressureUpdate { worker_id: WorkerId, cpu: Psi, memory: Psi, io: Psi },
    PoolCapacityUpdate { worker_id: WorkerId, pools: Vec<PoolCapacity> },
    ConditionUpdate { worker_id: WorkerId, key: String, active: bool, message: String },
    Disconnected { worker_id: WorkerId },
}
```

**Namespace routing:** The reader task listens on a `control_rx` channel for
`AddNamespace(ns_id, sender)` / `RemoveNamespace(ns_id)` messages. When a
worker is assigned to a new namespace, the orchestrator sends `AddNamespace`
to the reader task. This keeps the reader task decoupled — it doesn't know
about routers or SMs, only about channels.

**Task loop:**

```rust
loop {
    select! {
        event = reader.recv_event() => {
            match event {
                Ok(event) => classify and route,
                Err(_) => {
                    global_tx.send(WorkerGlobalEvent::Disconnected { .. });
                    // also notify all namespace routes
                    break;
                }
            }
        }
        control = control_rx.recv() => {
            match control {
                AddNamespace(ns_id, tx) => namespace_routes.insert(ns_id, tx),
                RemoveNamespace(ns_id) => namespace_routes.remove(&ns_id),
            }
        }
    }
}
```

#### Worker State Tracker

Global component that aggregates health state from all workers. Feeds the
scheduler with `WorkerCandidate` snapshots.

```rust
struct WorkerStateTracker {
    workers: HashMap<WorkerId, WorkerState>,
    /// Notify scheduler when worker state changes.
    scheduler_tx: mpsc::Sender<SchedulerInput>,
}

struct WorkerState {
    pressure: PressureBands,  // cpu, memory, io
    draining: bool,
    conditions: HashSet<String>,
    pool_capacity: Vec<PoolCapacity>,
    capabilities: WorkerCapabilities,
    writer: WorkerWriterHandle,
}
```

On receiving `WorkerGlobalEvent`, updates internal state and sends
`SchedulerInput::WorkerUpdate` or `WorkerRemoved` to the scheduler.

#### Writer Handle

The write half of the protocol connection, wrapped in a channel for safe
sharing across namespace tasks:

```rust
struct WorkerWriterHandle {
    tx: mpsc::Sender<WorkerCommand>,
}
```

Namespace tasks hold `WorkerWriterHandle` clones. When the pod assignment
adapter produces a `LaunchPod` action, the namespace task sends it through
the handle. A dedicated writer task drains the channel and serializes
commands to the wire.

#### Pod Assignment Adapter (per namespace, Implemented)

**Location:** `distvirt-orchestrator/src/adapter/pod_assignment/mod.rs`

**Port type:** `Worker` (one instance per connected worker in this namespace)

This is the namespace-local component that follows the standard adapter
pattern: drain port inputs, diff against cache, return actions.

**State:**

```rust
struct PodAssignmentAdapter {
    /// Cached set of pods assigned to each worker (from last reconcile).
    assigned_pods: HashMap<WorkerId, HashMap<PodId, PodScheduleRequest>>,
}
```

**Reconcile (outward):**

```rust
fn reconcile(&mut self, router: &mut Router) -> Vec<PodAssignmentAction>
```

- Drains `router.drain_worker_inputs()`.
- Gets `(WorkerId, WorkerPortInput::AssignedPodsInput(Vec<(PodId, PodScheduleRequest)>))`.
- Diff against `self.assigned_pods[worker_id]`:
  - Pod appeared → `PodAssignmentAction::Launch` or `Resume` (based on
    `schedule_request.resume_artifact`).
  - Pod disappeared → `PodAssignmentAction::Stop`.
- Update cached set.

```rust
enum PodAssignmentAction {
    Launch { worker_id: WorkerId, pod_id: PodId, request: PodScheduleRequest },
    Resume { worker_id: WorkerId, pod_id: PodId, artifact_id: ArtifactId },
    Stop { worker_id: WorkerId, pod_id: PodId },
}
```

The namespace task iterates these actions and sends the corresponding
`WorkerCommand` through the `WorkerWriterHandle` for each worker.

**Implementation notes:**

The pure reconciliation layer is implemented. The adapter drains
`router.drain_worker_inputs()`, diffs `AssignedPodsInput` deliveries against
a `HashMap<WorkerId, HashMap<PodId, PodScheduleRequest>>` cache, and returns
`Vec<PodAssignmentAction>`. The shell integration (sending wire commands via
`WorkerWriterHandle`) is not yet implemented.

**Aggregator change:** `Worker::AssignedPodsInput` was changed from
`ListAggregator` (which drops `PodId`) to `IdListAggregator` (which preserves
`(PodId, PodScheduleRequest)` pairs). This is the same change already applied
to `ScheduleRequest::PodRequestsInput` and timer inputs.

**Tests:** `adapter/pod_assignment/tests.rs` — 5 unit tests exercise the pure
diff logic using a real `Router`. Tests cover: no-op, launch, stop, stable
state dedup, and multiple workers in one reconcile. The Resume branch (for
`resume_artifact.is_some()`) is not directly unit-tested because `ArtifactId`
has private fields; this flow is covered by orchestrator scenario tests.

**Edge management:** The adapter also manages `WorkerToPod` edges. When it
produces a `Launch` action, the namespace task sets the `WorkerToPod` edge
so the pod can receive `WorkerInput`. The Pod SM sets `PodToWorker` (pod →
worker direction) when it gets a lease; the namespace sets `WorkerToPod`
(worker → pod direction) when it sends the launch command. This bidirectional
edge setup happens naturally:
- Pod sets `PodToWorker` → pod assignment adapter sees pod in
  `AssignedPodsInput` → namespace sets `WorkerToPod` and sends launch command.

Setting `WorkerToPod` is a router mutation. The namespace task can batch this
with the action processing and do a final `router.propagate()` if needed.

**Push (inward) — handled by namespace task, not this adapter:**

The namespace task receives `WorkerNamespaceEvent` from the worker reader
task and translates them to router mutations:
- `PodRunning` → `router.send_notify_pod_status(worker_id, pod_id, Running)`
- `PodFailed` → `router.send_notify_pod_status(worker_id, pod_id, Failed)`
- `PodExited { code: 0 }` → `router.send_notify_pod_status(..., Finished)`
- `PodExited { code: !0 }` → `router.send_notify_pod_status(..., Failed)`
- `PodSuspended { artifact_id }` → `router.send_notify_pod_suspended(...)`
- `ServiceBackendNeed` → BackendNeed adapter (see below)

**Worker connect/disconnect — handled by namespace task:**
- Worker added to namespace → create Worker port via
  `router.create_worker()`, set `router.set_worker_info(id, info)`.
- Worker disconnected (notified via `WorkerNamespaceEvent`) → call
  `router.remove_worker(id)`. This clears all `WorkerToPod` edges,
  causing Pod SMs to see `WorkerInput(None)` → Failed. Also remove any
  associated `BackendNeed` ports.

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

### Schedule Request Adapter (per namespace, Implemented)

**Location:** `distvirt-orchestrator/src/adapter/schedule_request/mod.rs`

**Port type:** `ScheduleRequest` (singleton, manual ID)

This is the namespace-side component that bridges the `ScheduleRequest` port
to the global scheduler task. It follows the standard diff pattern and returns
delta enums that the caller sends over a channel to the global scheduler.

**State:**

```rust
struct ScheduleRequestAdapter {
    schedule_request_id: ScheduleRequestId,
    /// What we've told the scheduler about: PodId → PodScheduleRequest.
    sent_requests: HashMap<PodId, PodScheduleRequest>,
}
```

**Reconcile:**

```rust
fn reconcile(&mut self, router: &mut Router) -> Vec<ScheduleRequestDelta>
```

- Drains `router.drain_schedule_request_inputs()`, filters for this adapter's
  `schedule_request_id`.
- Diffs the new request set against `self.sent_requests`:
  - Pod in new but not sent → `ScheduleRequestDelta::Request { pod_id, request }`.
  - Pod in sent but not new → `ScheduleRequestDelta::Drop { pod_id }`.
- Update `self.sent_requests`.

**Implementation notes:**

The pure reconciliation layer is implemented. Per the design rule in "Source
Organization", the adapter returns `Vec<ScheduleRequestDelta>` (not channel
messages). The *caller* (namespace task) translates these deltas into
`SchedulerInput::RequestLease` / `DropRequest` channel messages to the global
scheduler. This keeps the adapter pure and testable without channel
infrastructure.

**Tests:** `adapter/schedule_request/tests.rs` — 5 unit tests exercise the
pure diff logic using a real `Router`. Tests cover: no-op, new request,
drop, stable state dedup, and multiple pods changing in one cycle.

The namespace task also listens for `SchedulerDecision` on a reply channel and
handles `Grant`/`Revoke` by creating/destroying `ScheduleLease` ports in the
router.

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
used by all three timer port inputs, `ScheduleRequest::PodRequestsInput`, and
`Worker::AssignedPodsInput` (both changed from `ListAggregator` for their
respective adapters).

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

## Source Organization

### Three layers

The implementation is organized into three layers with clear responsibilities
and dependency direction:

```
  ┌──────────────────────────────────────────────────┐
  │  shell/          Startup, wiring, connection     │  depends on: task/, adapter/
  │                  accept, handshake               │
  ├──────────────────────────────────────────────────┤
  │  task/           Async tasks, channel interfaces │  depends on: adapter/
  ├──────────────────────────────────────────────────┤
  │  adapter/        Pure logic, no async, no I/O    │  depends on: sm_new/
  │  sm_new/         Router, SM definitions          │
  └──────────────────────────────────────────────────┘
```

Dependencies flow downward only. `adapter/` never imports from `task/`.
`task/` never imports from `shell/`.

### Layer 1: `adapter/` — Pure logic

No async, no channels, no tokio. Each adapter is a struct with methods that
take `&mut Router` and return action enums or delta lists. Fully testable
with just a `Router`.

```
adapter/
├── mod.rs
├── timer/
│   ├── mod.rs              # TimerAdapter, TimerAction, TimerIdentity
│   └── tests.rs
├── scheduler/
│   ├── mod.rs              # decide(), select_worker(), WorkerCandidate
│   └── tests.rs
├── pod_assignment/
│   ├── mod.rs              # PodAssignmentAdapter, PodAssignmentAction
│   └── tests.rs
├── schedule_request/
│   ├── mod.rs              # ScheduleRequestAdapter, ScheduleRequestDelta
│   └── tests.rs
├── backend_need/
│   ├── mod.rs              # BackendNeedAdapter
│   └── tests.rs
├── endpoint/
│   ├── mod.rs              # EndpointAdapter
│   └── tests.rs
└── management/
    ├── mod.rs              # ManagementAdapter
    └── tests.rs
```

**Key design rule:** The schedule_request adapter returns
`Vec<ScheduleRequestDelta>` (Request/Drop), not channel messages. The
*caller* (namespace task) sends these over the channel. This keeps the
adapter pure and testable without any channel infrastructure:

```rust
// adapter/schedule_request/mod.rs
pub(crate) enum ScheduleRequestDelta {
    Request { pod_id: PodId, request: PodScheduleRequest },
    Drop { pod_id: PodId },
}

impl ScheduleRequestAdapter {
    pub(crate) fn reconcile(&mut self, router: &mut Router) -> Vec<ScheduleRequestDelta>
}
```

### Layer 2: `task/` — Async tasks and interfaces

Contains the async task implementations and, critically, the **interface
types** that define all inter-task communication boundaries. Reading
`task/mod.rs` tells you how every component connects.

```
task/
├── mod.rs                  # Interface types (all channel message enums)
├── namespace.rs            # Namespace task event loop
├── scheduler/              # Global scheduler task (implemented)
│   ├── mod.rs
│   └── tests.rs
├── worker_reader.rs        # Per-connection reader task
├── worker_writer.rs        # Per-connection writer task (may be a simple fn)
└── worker_state.rs         # Global worker state tracker
```

#### `task/mod.rs` — Interface types (scheduler types implemented)

All channel message enums live here. This is the single place that defines
every communication boundary in the system. Currently contains `SchedulerInput`
and `SchedulerDecision`; other types will be added as tasks are built:

```rust
// === Namespace task input ===

/// Everything a namespace task can receive.
pub(crate) enum NamespaceEvent {
    /// Worker protocol event routed by a worker reader task.
    WorkerEvent(WorkerNamespaceEvent),
    /// Scheduler decided on a lease.
    SchedulerDecision(SchedulerDecision),
    /// A tokio timer fired.
    TimerFired(TimerIdentity),
    /// Client issued a command (gRPC).
    ClientCommand(ClientCommand, oneshot::Sender<ClientResponse>),
    /// A worker was added to this namespace.
    WorkerConnected { worker_id: WorkerId, info: WorkerInfo, writer: WorkerWriterHandle },
    /// A worker was removed from this namespace.
    WorkerDisconnected { worker_id: WorkerId },
}

// === Worker reader → namespace ===

pub(crate) struct WorkerNamespaceEvent {
    pub worker_id: WorkerId,
    pub event: WorkerNamespaceEventKind,
}

pub(crate) enum WorkerNamespaceEventKind {
    PodRunning { pod_id: PodId },
    PodExited { pod_id: PodId, exit_code: i32 },
    PodFailed { pod_id: PodId, error: String },
    PodSuspended { pod_id: PodId, artifact_id: ArtifactId, pool_id: PoolId },
    PodSuspendFailed { pod_id: PodId, error: String },
    NamespaceCreated,
    NamespaceFailed { error: String },
    NamespaceDestroyed,
    ServiceBackendNeed { service_id: ServiceId, need: BackendNeed },
    ArtifactWriteStarted { artifact_id: ArtifactId, pool_id: PoolId },
    ArtifactWriteCommitted { artifact_id: ArtifactId, pool_id: PoolId, size_bytes: u64 },
    EndpointActivation { ip: Ipv4Addr, service_id: Option<ServiceId> },
    EndpointFlowStatus { ip: Ipv4Addr, service_id: Option<ServiceId>, has_active_flows: bool },
}

// === Worker reader → worker state tracker ===

pub(crate) enum WorkerGlobalEvent {
    PressureUpdate { worker_id: WorkerId, cpu: Psi, memory: Psi, io: Psi },
    PoolCapacityUpdate { worker_id: WorkerId, pools: Vec<PoolCapacity> },
    ConditionUpdate { worker_id: WorkerId, key: String, active: bool, message: String },
    Disconnected { worker_id: WorkerId },
}

// === Namespace → scheduler ===

pub(crate) enum SchedulerInput {
    RequestLease { namespace_id: NamespaceId, pod_id: PodId, request: PodScheduleRequest },
    DropRequest { namespace_id: NamespaceId, pod_id: PodId },
    WorkerUpdate(WorkerId, WorkerCandidateSnapshot),
    WorkerRemoved(WorkerId),
}

// === Scheduler → namespace ===

pub(crate) enum SchedulerDecision {
    Grant { pod_id: PodId, worker_id: WorkerId },
    Revoke { pod_id: PodId },
}

// === Shell → worker reader task ===

pub(crate) enum WorkerControl {
    AddNamespace(NamespaceId, mpsc::Sender<NamespaceEvent>),
    RemoveNamespace(NamespaceId),
}

// === Shared handle for sending commands to a worker ===

#[derive(Clone)]
pub(crate) struct WorkerWriterHandle {
    tx: mpsc::Sender<WorkerCommand>,
}
```

#### `task/namespace.rs` — Namespace task

Owns a `Router` and all pure adapters. Runs the event loop described in the
Architecture section. Translates adapter outputs into side effects (send
channel messages, spawn timers, send worker commands).

**Owns:**
- `Router`
- `TimerAdapter`, `PodAssignmentAdapter`, `ScheduleRequestAdapter`,
  `BackendNeedAdapter`, `EndpointAdapter`, `ManagementAdapter`
- `HashMap<PodId, ScheduleLeaseId>` — tracks granted leases in the router
- `HashMap<WorkerId, WorkerWriterHandle>` — for sending commands to workers
- Timer `JoinHandle` management
- `mpsc::Receiver<NamespaceEvent>` — input channel
- `mpsc::Sender<SchedulerInput>` — to global scheduler
- `mpsc::Receiver<SchedulerDecision>` — from global scheduler

#### `task/scheduler/` — Global scheduler task (Implemented)

Simple event loop. Receives `SchedulerInput`, maintains pending/granted/worker
state, calls `adapter::scheduler::select_worker()` for pure decisions, sends
`SchedulerDecision` back to namespaces.

**Owns:**
- `pending: HashMap<(NamespaceId, PodId), PendingEntry>`,
  `granted: HashMap<(NamespaceId, PodId), GrantedEntry>`,
  `workers: HashMap<WorkerId, WorkerCandidate>`
- `mpsc::Receiver<SchedulerInput>`
- Per-namespace `mpsc::Sender<SchedulerDecision>` discovered lazily from
  `RequestLease` messages (embedded in pending/granted entries)

**Key detail:** PodId is per-Router (not globally unique), so maps are keyed by
`(NamespaceId, PodId)`. Tests use `PodId::test()` / `WorkerId::test()` helpers
since the macro-generated ID types have private fields.

#### `task/worker_reader.rs` — Per-connection reader task

Decodes wire protocol, classifies events, routes to namespace channels or
global channel. Listens for `WorkerControl` messages to update namespace
routing. See the Worker Reader Task section above for details.

#### `task/worker_writer.rs` — Per-connection writer task

Drains `mpsc::Receiver<WorkerCommand>` and serializes to the wire via
`OrchestratorWriter`. May be simple enough to be a single async function
rather than a full module.

#### `task/worker_state.rs` — Global worker state tracker

Receives `WorkerGlobalEvent` from all worker reader tasks. Maintains
per-worker health state. Forwards `SchedulerInput::WorkerUpdate` /
`WorkerRemoved` to the scheduler when state changes.

### Layer 3: `shell/` — Startup and wiring

The thinnest layer. Accepts worker connections, performs the handshake,
spawns tasks, and wires channels together. Does not contain business logic.

```
shell/
├── mod.rs                  # accept connections, spawn tasks, wire channels
└── subscriptions.rs        # log/event streaming (kept from current code)
```

**Responsibilities:**
- Listen for incoming worker TCP connections.
- Perform the 3-step handshake (WorkerHello → WorkerAccepted → WorkerReady).
- Split connection into reader/writer, spawn `WorkerReaderTask` and writer.
- Send `WorkerControl::AddNamespace` to reader tasks for each active namespace.
- Send `NamespaceEvent::WorkerConnected` to namespace tasks.
- Create/destroy namespace tasks on client commands.
- Hold the `mpsc::Sender<SchedulerInput>` and pass clones to namespace tasks.

### Communication map

Every arrow is a typed channel. Every type lives in `task/mod.rs`.

```
                    SchedulerInput          SchedulerDecision
Namespace Task ──────────────────► Scheduler Task ──────────────► Namespace Task
     ▲                                  ▲
     │ NamespaceEvent                   │ SchedulerInput::WorkerUpdate/Removed
     │                                  │
Worker Reader ─── WorkerGlobalEvent ──► Worker State Tracker
     ▲
     │ WorkerControl
     │
Shell (connection accept, handshake)
     │
     ├── WorkerWriterHandle ──► Worker Writer Task ──► wire
     │
     └── NamespaceEvent::WorkerConnected ──► Namespace Task
```

### Relationship to old code

The old modules are being replaced:

| Old module | Replaced by | Notes |
|------------|-------------|-------|
| `orchestrator/` | `shell/` + `task/scheduler.rs` + `task/worker_state.rs` | Old orchestrator SM splits into thin wiring + global tasks |
| `orchestrator/workers.rs` | `task/worker_state.rs` + `task/worker_reader.rs` | Worker lifecycle splits into reader task + state tracker |
| `orchestrator/scheduling.rs` | `task/scheduler.rs` + `adapter/scheduler/` | Scheduling logic was already extracted to adapter |
| `namespace/` | `task/namespace.rs` + pure adapters in `adapter/` | Old namespace SM becomes a task driving pure adapters |
| `namespace/events.rs` | `task/namespace.rs` event dispatch | Worker event → router mutation mapping |
| `namespace/commands.rs` | `adapter/pod_assignment/` | Pod assignment diff logic |
| `types/namespace_io.rs` | `task/mod.rs` interface types | Channel message enums replace old I/O types |
| `types/orchestrator_io.rs` | `task/mod.rs` interface types | Channel message enums replace old I/O types |
| `shell/worker_protocol.rs` | `task/worker_reader.rs` | Same event classification, now in reader task |
| `shell/mod.rs` | `shell/mod.rs` (rewritten, much thinner) | Only wiring and handshake remain |
| `sm/` | `sm_new/` | Already replaced |
| `types/states.rs`, `types/specs.rs` | Kept where still needed | Some types may be superseded by `sm_new` types |

Old modules can be removed incrementally as new code takes over their
responsibilities.

## Implementation Notes

### Adapter design patterns

#### Aggregator choice: ListAggregator vs IdListAggregator

- Use `ListAggregator` when the adapter doesn't need to know which SM produced
  each value (e.g., the BackendNeed aggregator already reduces to a single
  priority level).
- Use `IdListAggregator` when the adapter needs source SM IDs to build identity
  keys, dispatch events back, or make per-SM decisions. The timer adapter
  needed this; the scheduler adapter will likely need it too (for
  `ScheduleRequest::PodRequestsInput` — the current `ListAggregator` drops
  `PodId`).

#### Reconcile partial-delivery semantics

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

#### Type visibility

If a future adapter needs access to types defined in `sm_new` submodules
(service.rs, workload.rs, pod.rs), those types should already be accessible
via `crate::sm_new::*` thanks to the `pub(crate) use` re-exports. If the
macro-generated types (e.g., new port input enums) aren't visible, check the
`__router` module re-exports in `distvirt-sm-router-macros/src/generate/router.rs`.

### Testing strategy

Four levels of testing, each with a clear scope:

| Layer | Location | What it tests | Async? |
|-------|----------|---------------|--------|
| SM behavior | `sm_new/tests/` | Signal propagation, edge changes, SM transitions | No |
| Pure adapters | `adapter/*/tests.rs` | Diff logic: drain → diff → actions | No |
| Task wiring | `task/tests/` | Channel plumbing: send event in, assert output on another channel | Yes (`#[tokio::test]`) |
| Full scenarios | `tests/` (crate root) | End-to-end: spawn real tasks, simulate worker connections, verify system behavior | Yes |

**SM tests** (`sm_new/tests/`): Create a `Router`, set up SMs, propagate,
assert on signals and SM state. Already exist and are comprehensive.

**Adapter tests** (`adapter/*/tests.rs`): Create a `Router`, set up enough
SMs to produce port inputs, call `reconcile()`, assert on the returned action
list. No async runtime needed. The timer and scheduler adapter tests follow
this pattern today.

**Task tests** (`task/tests/`): Create channels, instantiate a task, send
events into its input channel, assert on what comes out of its output
channels. These test the event loop logic and channel wiring without needing
a real `Router` — the task can be driven with synthetic `NamespaceEvent`s.
Alternatively, task tests can use a real `Router` for more realistic coverage.

**Scenario tests** (`tests/`): Spawn the full task graph (shell, namespace
tasks, scheduler, worker reader/writer). Use in-process duplex connections
instead of TCP. Exercise end-to-end flows like "deploy workload → pod gets
scheduled → pod launches on worker → pod reports running." These replace the
current scenario tests in `tests/`.

## Event Flow — Detailed

### Namespace task event loop

```
recv event (worker event, client cmd, timer fire, scheduler decision)
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
Phase 3: Reconcile adapters
  │  timer_adapter.reconcile()           — returns Start/Cancel actions
  │  pod_assignment_adapter.reconcile()  — returns Launch/Stop actions
  │  schedule_request_adapter.reconcile() — sends RequestLease/DropRequest to scheduler
  │  endpoint_adapter.reconcile()        — returns endpoint update actions
  │
  ▼
Phase 4: Execute actions
  │  spawn/cancel tokio timers
  │  send LaunchPod/StopPod via WorkerWriterHandle
  │  set WorkerToPod edges (if any Launch actions)
  │  send endpoint updates to workers
  │
  ▼
Phase 5: Final propagate (only if router was mutated in phase 4)
  │  (e.g., WorkerToPod edge was set)
  │
  ▼
done (back to recv)
```

### Scheduling flow across tasks (async)

```
Namespace task                    Global Scheduler         Worker Reader
     │                                  │                       │
     │  pod enters Pending              │                       │
     │  propagate()                     │                       │
     │  schedule_request_adapter        │                       │
     │    diffs → RequestLease ────────►│                       │
     │                                  │ select_worker()       │
     │                                  │ found worker          │
     │  ◄──── Grant { pod, worker } ────│                       │
     │                                  │                       │
     │  create lease port               │                       │
     │  propagate()                     │                       │
     │  Pod SM sets PodToWorker         │                       │
     │  pod_assignment_adapter          │                       │
     │    diffs → Launch                │                       │
     │  send LaunchPod ──────────────────────────────────────►  │
     │  set WorkerToPod edge            │                       │
     │  propagate()                     │                       │
     │                                  │                       │
     │  ◄─────────────── PodRunning ────────────────────────────│
     │  send_notify_pod_status          │                       │
     │  propagate()                     │                       │
     │  (pod now Running)               │                       │
```

In steady state, the namespace task loop is a no-op: no events arrive, no
adapters see diffs, no actions are produced. The scheduling flow only triggers
when pod state actually changes.

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

- **Worker disconnect:** The worker reader task detects the broken connection
  and sends `WorkerGlobalEvent::Disconnected` to the worker state tracker
  (which notifies the scheduler) and a disconnect event to all namespace
  routes. Each namespace task calls `router.remove_worker()`. The router
  clears all edges involving that worker. Pod SMs see `WorkerInput(None)` →
  transition to Failed. BackendNeed ports for that worker are removed →
  services see need drop. All cascades handled by SMs.

- **Stale timer fire:** the timer adapter ignores fires for timers not in
  its active set (already cancelled). If it does deliver a fire event, the
  SM's generation-based guard rejects it (the SM's wanted timer has a
  different generation than the fired one). Belt and suspenders.

- **Scheduler slow or unavailable:** pods remain in Pending with a launch
  timeout. The Pod SM's timer fires → Failed. The Workload SM sees the
  failure and enters retry backoff. The namespace is not blocked — it
  continues processing other events while waiting for scheduling decisions.

- **Namespace task crash:** The scheduler holds `mpsc::Sender` handles for
  reply channels. If a namespace task dies, the sender fails and the
  scheduler can clean up pending/granted state for that namespace.

## Remaining Work — Old → New Migration

Tracks what remains before the old codepaths (`orchestrator/`, `namespace/`,
`shell/`, `sm/`) can be removed. The core reconciliation loop (adapters, tasks,
SM layer, event loop) is complete. The gaps below are features the old code
provides that the new code does not yet cover.

### Must have (system non-functional without)

- [x] **Endpoint delivery to workers** — `EndpointAdapter` produces actions and
  the namespace task translates them to `WorkerCommand::EndpointUpdate` via
  `build_endpoint_command()`, broadcasting to all connected workers. `Update`
  actions send an `EndpointSpec` with the service backend populated; `Remove`
  actions send an `EndpointSpec` with `backend: None` (preserving the service
  in the worker's endpoint table for traffic buffering). Protocol worker IDs
  are tracked via `proto_worker_ids` map, populated from
  `NamespaceEvent::WorkerConnected`.

- [x] **Service registry sync** — Folded into `EndpointAdapter`. The adapter
  tracks a `registry: HashMap<String, Ipv4Addr>` cache alongside the endpoint
  cache. `update_registry()` diffs against the cache and returns
  `RegistryAction::Update { added, removed }`. `build_registry_sync()` returns
  a full `RegistryAction::Sync` for initial worker population. The namespace
  task calls `update_registry()` on `UpdateSpec` and broadcasts the delta to
  all workers. On `WorkerConnected`, it sends `build_registry_sync()` to the
  new worker. The namespace task translates `RegistryAction` to
  `WorkerCommand::RegistrySync`/`RegistryUpdate` via `build_registry_command()`.

- [x] **Worker registry sync (inter-worker tunnels)** — Implemented in
  `task/worker_state.rs`. The `WorkerStateTracker` now tracks tunnel info
  (public key, listen port from `WorkerReady` handshake), protocol worker IDs,
  writer handles, and per-worker namespace segment sets. On worker
  connect/disconnect or segment assignment changes, it rebuilds
  `Vec<WorkerPeerInfo>` and broadcasts `WorkerCommand::WorkerRegistrySync` to
  all workers. The shell sends `WorkerStateEvent::RegisterNamespaceSegment` /
  `UnregisterNamespaceSegment` on namespace create/destroy, and
  `NamespaceAssigned` / `NamespaceUnassigned` when workers join namespaces.
  Segment IDs are allocated sequentially by the shell (starting at 1).

- [x] **Artifact placement tracking** — `PlacementTable` in the global
  scheduler tracks which artifacts exist on which workers. Worker reader
  routes `ArtifactWriteStarted`/`ArtifactWriteCommitted`/
  `ArtifactTransferReceived`/`TransferFailed` to the scheduler via
  `SchedulerInput::ArtifactEvent`. `select_worker()` uses soft affinity:
  workers with a ready copy of the pod's `resume_artifact` are preferred,
  with graceful fallback to any eligible worker. Placements are purged on
  `WorkerRemoved`. Artifact ID conversion between `sm_new::ArtifactId(u64)`
  and `protocol::ArtifactId(String)` happens at the namespace task boundary
  via bidirectional maps in `IdMaps`. The scheduler and placement table
  operate exclusively on protocol artifact IDs — no type conversion in
  pure scheduling logic. Protocol artifact IDs should include a UUID or
  namespace prefix for global uniqueness (generated when sending
  `SuspendPod` — not yet implemented).

- [x] **Endpoint flow event routing** — `EndpointActivation` and
  `EndpointFlowStatus` events are now routed from `task/worker_reader.rs` to
  namespace tasks. `EndpointActivation` with `service_id` sends
  `ActivateService(true)` to the service SM (triggering idle→active).
  `EndpointFlowStatus` with `service_id` uses `FlowDemandAdapter`
  (`adapter/flow_demand/`) — a push-based adapter that creates BackendNeed
  ports (reusing the existing port type). The service SM's
  `BackendNeedAggregator` sees both worker-reported need and flow-sourced
  need, taking the max — this keeps services alive while flows exist and
  prevents idle timeout. Ports are cleaned up on worker disconnect. Events
  without `service_id` (direct IP access) are not yet supported.

- [x] **Namespace creation on workers** — The shell (`task/shell.rs`) sends
  `WorkerCommand::CreateNamespace` (with `NetworkConfig` including segment_id)
  to workers during namespace-worker assignment, before sending
  `WorkerConnected` to the namespace task. The namespace task defers
  `router.create_worker()` until `NamespaceCreated` arrives — workers don't
  exist in the router until fabric is ready, naturally preventing pod
  scheduling. `NamespaceFailed` removes the worker from pending and logs the
  error. Worker reader routes `NamespaceCreated`/`NamespaceFailed` events to
  namespace tasks.

- [x] **Segment ID allocation** — `task/shell.rs` allocates segment IDs with
  wrapping and reuse (mirrors old `alloc_segment_id()`/`free_segment_id()`).
  `task/worker_state.rs` tracks namespace→segment mappings and per-worker
  segment sets for worker registry broadcasts.

### Important (features break without)

- [ ] **WireGuard client VPN** — Old code has `WgPeerManager` for client
  connect/disconnect: allocates IPs from subnet, sends
  `AddWireGuardPeer`/`RemoveWireGuardPeer` to workers. `ClientCommand` enum
  needs Connect/Disconnect variants, namespace task needs WG peer state.
  Old code: `namespace/wireguard.rs`.

- [ ] **Client command coverage** — Old code handles: `ListNamespaces`,
  `ListWorkers`, `GetWorker`, `ListPods`, `GetNamespaceStatus`,
  `DrainWorker`/`UndrainWorker`, `Connect`/`Disconnect`. Audit new
  `ClientCommand` enum and shell for coverage. Old code:
  `orchestrator/client.rs`.

- [ ] **Preemption** — Old code has `try_preempt_for_workload()` with priority
  scoring (active traffic > idle-but-demanded > always-on). The new SM layer
  may handle demand via signals, but the triggering logic (when capacity is
  tight) needs to exist somewhere. Old code:
  `orchestrator/scheduling.rs` `try_preempt_for_workload()`.

### Nice to have (observability, minor events)

- [ ] **Log and event subscriptions** — Old `shell/subscriptions.rs` manages
  streaming pod logs and SM events to clients. Needed for `StreamLogs` and
  observability dashboards.

- [ ] **Unhandled protocol events** — `ShuttingDown`, `TunnelStatus`,
  `PodLogStreamError` are silently dropped in `worker_reader.rs`.
