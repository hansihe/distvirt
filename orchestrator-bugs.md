# Orchestrator Bug Analysis

Analysis of 6 bugs documented in `distvirt-orchestrator/tests/scenarios/` test files.

## Bug 1: Worker reconnect fails to reschedule WaitingForCapacity workloads

**File:** `scenarios/worker_disconnect.rs`
**Status:** FIXED

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
**Status:** FIXED

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
**Status:** FIXED

When a second service activates and sends `DemandUp`, the workload is already
`Running`. The `DemandUp` handler (workload.rs:202) is a no-op for `Running` —
it increments `demand_count` but emits nothing. No `BecameReady` is re-emitted,
so the new service never receives `WorkloadReady`.

**Root cause:** `BecameReady` is only emitted during state transitions
(Launching→Running, Resuming→Running), not when demand increases on an
already-running workload. No mechanism for late joiners to query current state.

## Bug 5: Resume failure causes Dormant instead of RetryBackoff

**File:** `scenarios/resume_failure.rs`
**Status:** FIXED

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
**Status:** PARTIALLY FIXED (NeedBackend issue fixed, orphaned demand remains)

Two issues:

1. **Orphaned demand:** `FabricRouteMiss` sends `DemandUp` directly to the
   workload (events.rs:~270) with no corresponding entity that will ever send
   `DemandDown`. The demand count is permanently elevated.

2. ~~**NeedBackend (same as Bug 4):** When a service subsequently activates on an
   already-running workload, it gets stuck in `NeedBackend`.~~
   **FIXED:** Late-joiner WorkloadReady now notifies services when DemandUp
   arrives on an already-Running workload.

**Root cause:** `FabricRouteMiss` acts as an anonymous demand source with no
lifecycle management.

---

## Structural Analysis: Why Did These Bugs Happen?

### 1. The `mem::replace` pattern (Bug 2) — FIXED

The workload SM used `mem::replace(&mut self.state, Dormant)` extensively to
destructure enum variants. `Dormant` was the placeholder but also a valid state.
If any code path forgot to overwrite `self.state`, the workload silently landed
in `Dormant` with no error.

**Fix applied:** Replaced the `Dormant` placeholder with a `Transitioning`
sentinel variant that panics in all state accessors. All code paths now
explicitly set the final state. Stateright property `"no transitioning state"`
catches any future regressions at model-checking time.

### 2. Synchronous/eager output processing (Bug 5) — FIXED

The namespace layer previously processed workload outputs immediately and
recursively — `forward_workload_outputs` could trigger service SM steps, which
produced `DemandDown`, which was immediately fed back to the workload SM.

**Fix applied:** Replaced with queue-based `process_outputs` using
`VecDeque<PendingOutput>`. All outputs from a single `step()` call are now
processed before side effects run. Additionally, when `BecameUnready` fires on
a retrying workload (RetryBackoff/WaitingForCapacity), activation services are
re-activated to NeedBackend to preserve demand through failure recovery.

### 3. Event-only notification model (Bugs 1, 4, 6) — FIXED

`BecameReady` is only emitted as a one-shot event during state transitions.
There's no mechanism for late joiners to query current state:

- ~~Bug 4: A second service can't learn the workload is already running.~~
  **FIXED:** Late-joiner WorkloadReady in `translate_service_outputs`.
- ~~Bug 1: A new worker can't learn workloads are waiting for capacity.~~
  **FIXED:** `schedule_waiting_pods` called in `process_namespace_output`.

### 4. Missing cross-reference between state and placement table (Bug 3)

The `Suspended` state stores only `artifact_id`, losing the worker association.
The placement table knows which worker holds each artifact, but
`handle_worker_lost` doesn't cross-reference it.

### 5. Anonymous demand sources (Bug 6)

The demand model assumes every `DemandUp` has a corresponding entity that will
eventually `DemandDown`. `FabricRouteMiss` violates this contract.

---

## Fixes

### Fix 1: Schedule on NamespaceCreated, not on worker connect (Bug 1) — IMPLEMENTED

Removed `schedule_waiting_pods()` from `handle_worker_connected`
(`orchestrator/workers.rs`). Added it to `process_namespace_output`
(`orchestrator/scheduling.rs`) after processing pod/resume requests. This runs
after every namespace step, which covers NamespaceCreated (worker becomes
Active), WorkerLost (workloads move to WaitingForCapacity), etc.

The call is idempotent — it scans for WaitingForCapacity workloads and only
acts when an Active worker is available. Most calls find nothing to schedule.

### Fix 2: Collect-then-process output model (Bug 5, systemic) — IMPLEMENTED

Replaced the recursive `forward_workload_outputs` ↔ `forward_service_outputs`
call chain (`namespace/output.rs`) with a queue-based processing loop using
`VecDeque<PendingOutput>`. The `PendingOutput` enum has `Workload` and `Service`
variants. A single `process_outputs` method drains the queue iteratively with
a MAX_ITERATIONS=100 convergence guard.

`translate_workload_outputs` and `translate_service_outputs` are non-recursive
versions of the old functions that push to the queue instead of calling each
other directly.

**Additional fix for Bug 5:** Queue-based processing alone doesn't fully fix
the resume failure case because the service's DemandDown still runs (just
deferred). The fix adds **retry-aware BecameUnready handling**: when
`BecameUnready` fires on a retrying workload (RetryBackoff or
WaitingForCapacity), DemandDown from activation services is filtered out, and
the service is re-activated (Idle → NeedBackend via ServiceActivation) to
preserve demand through failure recovery. Always-on services are unaffected
(they already stay in NeedBackend on WorkloadUnready).

**Demand model invariant comments** added:
- `WorkloadInput::DemandUp` handler (`workload.rs`): every `DemandUp` must
  have a corresponding entity that will eventually `DemandDown`
- `translate_service_outputs` DemandUp/DemandDown arm: services are the
  canonical demand holders

### Fix 3: Late-joiner WorkloadReady at namespace layer (Bugs 4, 6) — IMPLEMENTED

In `translate_service_outputs` (`namespace/output.rs`), after processing
`DemandUp` and stepping the workload SM: if the workload is already `Running`,
immediately step the originating service with `ServiceInput::WorkloadReady`
(constructing `ServiceBackend` from the workload spec). DemandUp/DemandDown
from the service's response are filtered (same as BecameReady handling).

This fixes Bug 4 completely and Bug 6's NeedBackend issue. Bug 6's orphaned
demand from FabricRouteMiss remains unfixed (see Fix 8).

### Fix 4: Add `Transitioning` sentinel variant (Bug 2, systemic) — IMPLEMENTED

Added `WorkloadState::Transitioning` to `types/states.rs`. Replaced all 18
`mem::replace(&mut self.state, WorkloadState::Dormant)` call sites in
`workload.rs` with `mem::replace(&mut self.state, WorkloadState::Transitioning)`.

Every code path that previously relied on the `Dormant` placeholder being the
final state now explicitly sets `self.state = WorkloadState::Dormant`.

`Transitioning` panics in `as_str()`, `worker_id()`, `pod_id()`, and
`artifact_id()`, so any forgotten overwrite is caught immediately rather than
silently landing in a valid quiescent state.

Added stateright property `"no transitioning state"` to both the workload-level
and namespace-level model checkers, catching the entire bug class exhaustively.

### Fix 5: Set state to Suspended before ResumeRequest (Bug 2) — IMPLEMENTED

In the `PodSuspended` handler (`workload.rs`):
- `PendingIntent::Demand` branch: added
  `self.state = WorkloadState::Suspended { artifact_id: artifact_id.clone() }`
  before emitting `ResumeRequest`.
- `PendingIntent::None` with `demand_count > 0` branch: same.

This ensures `handle_resume_pod`'s
`if !matches!(self.state, WorkloadState::Suspended { .. })` guard passes.

Updated `test_demand_during_suspend_immediate_resume` from asserting the buggy
`Dormant` state to asserting `Resuming` or `Running`.

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

**Superseded by Fix 2's retry-aware BecameUnready handling.** The queue-based
output model with retry compensation addresses this more generically — it
preserves demand through any failure recovery path (not just Resuming→PodGone),
while still forwarding WorkloadUnready to services so they clear backend info
on workers.

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
`schedule_waiting_pods` in `process_namespace_output`) covers this gap.

### Demand preservation through failures (new behavior from Fix 2)

With the retry-aware BecameUnready handling, activation services now stay in
NeedBackend (instead of going Idle) when a workload fails and enters a retry
state. This means:

- **Worker loss**: workload stays WaitingForCapacity, service stays NeedBackend.
  When a new worker joins, the workload is automatically scheduled.
- **Launch timeout**: workload stays WaitingForCapacity, service stays NeedBackend.
  The workload is immediately re-schedulable.
- **Pod failure during retry**: workload enters RetryBackoff, service stays
  NeedBackend. After backoff, the workload retries.

This is strictly better than the old behavior where failures caused activation
services to drop demand, requiring a new ServiceActivation event to restart.
