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

## Potential Fixes

### Structural fixes (address multiple bugs)

1. **Post-NamespaceCreated reconciliation for existing namespaces** (fixes Bug 1):
   When a new worker joins an already-Active namespace, call
   `reconcile_all_services` or at minimum `schedule_waiting_pods` after fabric
   becomes Active. Small change in the `NamespaceCreated` handler.

2. **Collect-then-process output model** (fixes Bug 5, prevents future similar
   bugs): Instead of processing workload/service outputs immediately and
   recursively, collect all outputs into a batch, then process them in a second
   pass. Prevents re-entrant state mutation. Significant structural change.

3. **WorkloadReady notification for late-binding services** (fixes Bugs 4 and
   6's NeedBackend issue): When `DemandUp` arrives on an already-Running
   workload, emit `BecameReady`. Alternatively, handle in the namespace layer —
   when stepping a service produces `DemandUp` and the target workload is
   already `Running`, immediately send `WorkloadReady` back to the service.

### Specific fixes

4. **Bug 2:** In PodSuspended handler's `Demand` and `None`-with-demand
   branches, add `self.state = WorkloadState::Suspended { artifact_id }` before
   emitting `ResumeRequest`.

5. **Bug 3:** In `handle_worker_lost`, after removing placements, iterate
   workloads in `Suspended` state and check if their `artifact_id` was in the
   removed set. Transition to `Dormant` (or `WaitingForCapacity` if demand > 0).

6. **Bug 5 (targeted fix):** Don't emit `BecameUnready` from `PodGone` in
   `Resuming` state before `transition_on_intent` — or emit it after the next
   state is determined. Since the workload will retry via `RetryBackoff`, the
   service doesn't need to know it was briefly unready.

7. **Bug 6 orphaned demand:** Either (a) don't use `DemandUp` for route misses
   — have the namespace layer directly check if the workload should be woken
   and issue a `PodRequest` bypassing the demand system, or (b) track
   route-miss demand separately with a timer-based auto-expire.
