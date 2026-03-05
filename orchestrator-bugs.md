# Orchestrator Bug Analysis

Analysis of 6 bugs documented in `distvirt-orchestrator/tests/scenarios/` test files.

## Bug 1: Worker reconnect fails to reschedule WaitingForCapacity workloads

**File:** `scenarios/worker_disconnect.rs`

When a worker disconnects, `handle_worker_lost` moves affected workloads to
`WaitingForCapacity`. When a new worker connects, `handle_worker_connected`
calls `schedule_waiting_pods` (workers.rs:51). However, the new worker's fabric
is still in `Creating` — `select_worker_for_pod` only picks `Active` workers.
The worker becomes `Active` after reporting `NamespaceCreated`, but by then
`schedule_waiting_pods` has already run.

The `NamespaceCreated` handler for an already-Active namespace (events.rs:57-61)
only syncs registry/routes — it does NOT re-emit `PodRequest`s or call
`reconcile_all_services`.

**Root cause:** Temporal ordering mismatch — scheduling runs before the new
worker is eligible. The "already-Active namespace" path skips reconciliation.

## Bug 2: PodSuspended leaves state as Dormant instead of Suspended

**File:** `scenarios/transition_intents.rs`

The `PodSuspended` handler uses `mem::replace(&mut self.state, Dormant)` to
destructure the old state. For `PendingIntent::Demand` and `PendingIntent::None`
(with demand > 0), it emits `ResumeRequest` but never sets state to `Suspended`.
The state remains `Dormant` from the `mem::replace`.

When the namespace processes the `ResumeRequest`, it calls `handle_resume_pod`,
which checks `if !matches!(self.state, WorkloadState::Suspended { .. })` and
returns early. The `ResumePod` input is silently rejected.

**Root cause:** The `mem::replace` pattern sets a temporary `Dormant` placeholder
that must be overwritten in every code path. The `Demand` and `None`-with-demand
branches forgot to set `self.state = Suspended { artifact_id }` before emitting
`ResumeRequest`.

## Bug 3: Suspended workloads with stale artifact_id after worker disconnect

**File:** `scenarios/worker_failures.rs`

`handle_worker_lost` (events.rs:333-341) identifies affected workloads by
checking `worker_id()` on their state. `WorkloadState::Suspended` only stores
`artifact_id`, not `worker_id`, so it is never matched. The placement table
entry is cleaned up (line 324), but the workload stays in `Suspended` with a
stale `artifact_id` pointing to a nonexistent placement.

**Root cause:** `Suspended` state lost its association with the worker. The
placement table cleanup is worker-centric, and so is the workload notification,
but `Suspended` doesn't carry worker info.

## Bug 4: Second service on already-Running workload stuck in NeedBackend

**File:** `scenarios/multi_service.rs`

When a second service activates and sends `DemandUp`, the workload is already
`Running`. The `DemandUp` handler (workload.rs:202) is a no-op for `Running` —
it increments `demand_count` but emits nothing. No `BecameReady` is re-emitted,
so the new service never receives `WorkloadReady`.

**Root cause:** `BecameReady` is only emitted during state transitions
(Launching→Running, Resuming→Running), not when demand increases on an
already-running workload. No mechanism for late joiners to query current state.

## Bug 5: Resume failure causes Dormant instead of RetryBackoff

**File:** `scenarios/resume_failure.rs`

When `PodGone` fires during `Resuming`, the workload emits `BecameUnready`
(workload.rs:597). In `forward_workload_outputs`, `BecameUnready` is forwarded
to services, which respond with `DemandDown` via `forward_service_outputs`. This
`DemandDown` is processed immediately/synchronously, decrementing `demand_count`
to 0. When `transition_on_intent` → `transition_on_demand` finally runs, it sees
`demand_count == 0` and goes `Dormant`.

**Root cause:** Side effects are processed eagerly/synchronously. The
`BecameUnready` → service `DemandDown` → workload `demand_count` decrement
happens in the same call stack as the failure handling, before retry logic acts.
This is a re-entrant state mutation problem.

## Bug 6: FabricRouteMiss causes orphaned demand

**File:** `scenarios/fabric_route_miss.rs`

Two issues:

1. **Orphaned demand:** `FabricRouteMiss` sends `DemandUp` directly to the
   workload (events.rs:~270) with no corresponding entity that will ever send
   `DemandDown`. The demand count is permanently elevated.

2. **NeedBackend (same as Bug 4):** When a service subsequently activates on an
   already-running workload, it gets stuck in `NeedBackend`.

**Root cause:** `FabricRouteMiss` acts as an anonymous demand source with no
lifecycle management.

---

## Structural Analysis: Why Did These Bugs Happen?

### 1. The `mem::replace` pattern (Bug 2)

The workload SM uses `mem::replace(&mut self.state, Dormant)` extensively to
destructure enum variants. `Dormant` is the placeholder but also a valid state.
If any code path forgets to overwrite `self.state`, the workload silently lands
in `Dormant` with no error. A sentinel/invalid placeholder or a builder pattern
that forces setting the new state would catch this at compile time.

### 2. Synchronous/eager output processing (Bug 5)

The namespace layer processes workload outputs immediately and recursively —
`forward_workload_outputs` can trigger service SM steps, which produce
`DemandDown`, which is immediately fed back to the workload SM. This re-entrant
mutation means ordering of outputs matters enormously. Bug 5 is a direct
consequence: `BecameUnready` triggers a chain that zeroes demand before retry
logic runs.

### 3. Event-only notification model (Bugs 1, 4, 6)

`BecameReady` is only emitted as a one-shot event during state transitions.
There's no mechanism for late joiners to query current state:

- Bug 4: A second service can't learn the workload is already running.
- Bug 1: A new worker can't learn workloads are waiting for capacity.

The reconciliation system (`reconcile_all_services`) partially addresses this
but is only called in specific code paths, not consistently.

### 4. Missing cross-reference between state and placement table (Bug 3)

The `Suspended` state stores only `artifact_id`, losing the worker association.
The placement table knows which worker holds each artifact, but
`handle_worker_lost` doesn't cross-reference it.

### 5. Anonymous demand sources (Bug 6)

The demand model assumes every `DemandUp` has a corresponding entity that will
eventually `DemandDown`. `FabricRouteMiss` violates this contract.

---

## Fixes

### Fix 1: Schedule on NamespaceCreated, not on worker connect (Bug 1)

Remove `schedule_waiting_pods()` from `handle_worker_connected`
(`orchestrator/scheduling.rs`). Instead, call it from the `NamespaceCreated`
handler when a worker's fabric becomes `Active` — specifically in the
`else if self.status == Active` branch (`namespace/events.rs:57-61`).

This eliminates the temporal ordering problem entirely: scheduling only runs
when the worker is actually eligible (`FabricStatus::Active`), so
`select_worker_for_pod` will find it.

Note: `handle_worker_lost` with the last worker sets status to `Creating` and
skips reconciliation. When a new worker arrives and triggers `NamespaceCreated`,
the namespace goes `Creating` → `Active`, which calls `reconcile_all_services`.
However, `reconcile_service` only handles `(NeedBackend, Dormant)` — not
`(NeedBackend, WaitingForCapacity)`. Calling `schedule_waiting_pods` on
`NamespaceCreated` covers this gap.

### Fix 2: Collect-then-process output model (Bug 5, systemic)

Replace the recursive `forward_workload_outputs` ↔ `forward_service_outputs`
call chain (`namespace/output.rs:9-54, 56+`) with a queue-based round
processing loop:

```
let mut wl_queue: VecDeque<(WorkloadId, WorkloadInput)> = ...;
let mut svc_queue: VecDeque<(ServiceId, ServiceInput)> = ...;

while !wl_queue.is_empty() || !svc_queue.is_empty() {
    // drain workload queue, collect outputs
    // translate workload outputs → service inputs, push to svc_queue
    // drain service queue, collect outputs
    // translate service outputs → workload inputs, push to wl_queue
}
```

This gives a clear separation between "compute next state" and "propagate
effects". Bug 5 becomes impossible — the `BecameUnready` → `DemandDown` chain
happens in a subsequent round, after retry logic has already set the state.

Add an iteration cap as a convergence guard. In practice the demand model is
monotonic per round, so it should converge quickly.

**Demand model invariant comments** — add at key locations:
- `WorkloadInput::DemandUp` handler (`workload.rs:184`): every `DemandUp` must
  have a corresponding entity that will eventually `DemandDown`
- `forward_service_outputs` (`namespace/output.rs:19`): services are the
  canonical demand holders
- `FabricRouteMiss` handler (`namespace/events.rs:250`): note this bypasses the
  service demand model (to be fixed by Fix 8)

### Fix 3: Late-joiner WorkloadReady at namespace layer (Bugs 4, 6)

Handle in `forward_service_outputs` (`namespace/output.rs:19-38`): after
processing `DemandUp`, if the target workload is already `Running`,
immediately enqueue `ServiceInput::WorkloadReady` back to the originating
service.

This keeps the workload SM's concern narrow — it doesn't need to know about
services. The namespace layer is the right place for this translation since it
already mediates all cross-SM communication.

### Fix 4: Add `Transitioning` sentinel variant (Bug 2, systemic)

Add `WorkloadState::Transitioning` to `types/states.rs`. Replace all 18
`mem::replace(&mut self.state, WorkloadState::Dormant)` call sites in
`workload.rs` with `mem::replace(&mut self.state, WorkloadState::Transitioning)`.

Make `Transitioning` panic in `worker_id()`, `pod_id()` and other state
helpers, so any forgotten overwrite is caught immediately rather than silently
landing in a valid quiescent state.

Add a stateright property:
```rust
Property::always("no transitioning state", |_model, state| {
    state.namespace.workloads.values()
        .all(|wl| !matches!(wl.state, WorkloadState::Transitioning))
})
```

This catches the entire bug class at model-checking time — any code path that
forgets to set the final state will be found exhaustively.

### Fix 5: Set state to Suspended before ResumeRequest (Bug 2)

In the `PodSuspended` handler (`workload.rs:497-504`):
- `PendingIntent::Demand` branch: add
  `self.state = WorkloadState::Suspended { artifact_id: artifact_id.clone() }`
  before emitting `ResumeRequest`.
- `PendingIntent::None` with `demand_count > 0` branch: same.

This ensures `handle_resume_pod`'s
`if !matches!(self.state, WorkloadState::Suspended { .. })` guard passes.

### Fix 6: Store worker_id in Suspended (Bug 3)

Change `WorkloadState::Suspended { artifact_id }` to
`Suspended { artifact_id, worker_id }` in `types/states.rs:116`. Update all
construction sites (PodSuspended handler branches, etc.).

`handle_worker_lost` (`namespace/events.rs:338`) uses
`wl.state.worker_id() == Some(worker_id)` to find affected workloads.
Currently `Suspended` doesn't return a `worker_id`, so it's never matched.
With this change, `Suspended` workloads on a lost worker will be naturally
found and transitioned to `WaitingForCapacity` (if `demand_count > 0`) or
`Dormant`.

The existing stateright property "suspended workloads have valid placement"
(`stateright_model.rs:770`) will catch regressions. Update the model's
`WorkloadSnapshot` accordingly.

### Fix 7: Don't emit BecameUnready during Resuming→PodGone (Bug 5)

Remove `outputs.push(WorkloadOutput::BecameUnready)` from the `Resuming`
`PodGone` handler (`workload.rs:597`). The workload will retry via
`RetryBackoff` — services don't need to know about transient resume failures.

This is the targeted fix for Bug 5. Fix 2 (collect-then-process) prevents the
root cause generically and should be preferred long-term. This fix is simpler
and can be applied independently/first.

### Fix 8: Bypass demand system for FabricRouteMiss (Bug 6)

In the `FabricRouteMiss` handler (`namespace/events.rs:250-277`): instead of
calling `wl.step(WorkloadInput::DemandUp)`, directly check the workload state
at the namespace level and issue a `PodRequest` (for `Dormant`) or
`ResumeRequest` (for `Suspended`).

Route misses are wake-up hints, not persistent demand — they should not touch
`demand_count`. The demand model's invariant (every `DemandUp` has a
corresponding entity that will `DemandDown`) would be violated by anonymous
sources.

---

## Additional Concerns

### `reconcile_service` doesn't handle `(NeedBackend, Suspended)`

`reconcile_service` matches on `(ServiceState, WorkloadState)` pairs but only
handles `(NeedBackend, Dormant)` and `(Pending, Dormant)`. If a workload is
`Suspended` and a service is in `NeedBackend`, reconciliation won't issue a
`ResumeRequest`. Verify this is covered by the `Suspended` → `DemandUp` path
outside reconciliation, or add a `(NeedBackend, Suspended)` case.

### Interaction between last-worker-lost and Fix 1

When the last worker is lost, `handle_worker_lost` sets namespace status to
`Creating` and skips `reconcile_all_services`. When a new worker arrives and
its `NamespaceCreated` transitions the namespace `Creating` → `Active`,
`reconcile_all_services` runs — but it won't match workloads already in
`WaitingForCapacity` (reconciliation only handles `Dormant`). Fix 1 (calling
`schedule_waiting_pods` on `NamespaceCreated`) covers this gap.
