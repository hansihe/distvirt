# Orchestrator Refactor: Reconciliation Over Routing

## Motivation

The current architecture uses event-driven routing between state machines. The
namespace layer (`process_outputs`, `translate_workload_outputs`,
`translate_service_outputs`) acts as a mediator that forwards outputs between
workload and service SMs, transforming them along the way.

This works, but the mediation layer has become the primary source of bugs:

- **Bug 4 (late joiner):** `BecameReady` is a one-shot event. A service that
  activates on an already-running workload misses it. Fixed with ad-hoc
  late-joiner detection in `translate_service_outputs`.
- **Bug 5 (resume failure):** Recursive output processing caused
  `BecameUnready → DemandDown → demand_count=0` before retry logic ran. Fixed
  by queue-based processing + retry-aware DemandDown filtering.
- **Bug 6 (orphaned demand):** `FabricRouteMiss` sends `DemandUp` directly
  with no entity that will ever `DemandDown`. Partially fixed, orphaned demand
  remains.
- **Bug 1 (worker reconnect):** Scheduling ran before the worker was eligible.
  Fixed by calling `schedule_waiting_pods` after every namespace step.

These are all symptoms of the same architectural issue: **we're using events
where we need observable state**. Events are temporal — they can be missed,
processed in wrong order, or produced by anonymous sources. State is persistent
and can be observed at any time.

The fixes work, but they're compensatory. Each new interaction pattern between
SMs risks another missed-event or wrong-ordering bug. The `translate_*_outputs`
functions now contain significant domain logic (demand filtering, late-joiner
detection, retry-aware re-activation) that isn't part of any individual SM and
is only model-checked at the full namespace level.

## Core Idea: Replace Event Routing with Reconciliation

Instead of SMs communicating through routed events, each SM produces state
changes, and a reconciliation pass observes all current state and computes what
needs to happen.

### What Changes

**Before (event routing):**
1. Workload SM `step()` → emits `BecameReady`
2. Namespace `translate_workload_outputs` catches `BecameReady`
3. Namespace steps each service with `WorkloadReady`
4. Service emits `DemandUp`/`DemandDown`
5. Namespace `translate_service_outputs` catches demand events
6. Namespace steps workload with `DemandUp`/`DemandDown`
7. Ad-hoc fixes for edge cases at each routing point

**After (reconciliation):**
1. Workload SM `step()` → transitions to `Running` (no event emitted)
2. Namespace runs reconciliation pass
3. Reconciliation sees `(ServiceState::NeedBackend, WorkloadState::Running)` →
   steps service with `WorkloadReady`
4. Reconciliation sees service `wants_backend()` → derives demand count →
   updates workload if changed
5. Repeat until stable (bounded convergence loop)

### What This Eliminates

- `WorkloadOutput::BecameReady` / `WorkloadOutput::BecameUnready` — replaced
  by observing workload state transitions
- `ServiceOutput::DemandUp` / `ServiceOutput::DemandDown` — replaced by
  derived demand count
- `translate_workload_outputs` BecameReady/BecameUnready handling (all the
  late-joiner logic, retry-aware filtering, re-activation)
- `translate_service_outputs` DemandUp/DemandDown handling (all the demand
  routing, workload stepping, filtering)
- `PendingOutput` enum and `process_outputs` queue loop

### What Stays

- Workload SM `step()` for lifecycle events (`PodRunning`, `PodGone`,
  `PodSuspended`, `WorkerLost`, timers, etc.)
- Service SM `step()` for worker-facing events (`ServiceActivation`,
  `ServiceBackendNeed`, `TimerFired`, etc.)
- Output translation for pass-through effects: `WorkerCommand`,
  `BroadcastWorkerCommand`, `TimerSet`, `TimerCancel`, `PodRequest`,
  `SuspendRequest`, `ResumeRequest`, `DeleteArtifact`
- `Transitioning` sentinel (still valuable)
- `PendingIntent` pattern (still valuable)

## Detailed Design

### Demand as a Derived Property

Currently `demand_count` lives inside `WorkloadStateMachine` and is
incremented/decremented by `DemandUp`/`DemandDown` inputs routed through the
namespace layer. This is the source of demand-accounting bugs.

**New model:** The namespace computes effective demand from service state:

```rust
fn effective_demand(&self, workload_id: &WorkloadId) -> u32 {
    self.service_workload
        .iter()
        .filter(|(_, wl_id)| *wl_id == workload_id)
        .filter(|(svc_id, _)| {
            self.services.get(svc_id)
                .map(|s| s.wants_backend())
                .unwrap_or(false)
        })
        .count() as u32
}
```

Where `wants_backend()` is true for `NeedBackend` and `Active` states.

The workload SM receives `SetDemand { count: u32 }` (or a bool
`wants_to_run`) from the reconciliation pass instead of incremental
`DemandUp`/`DemandDown`. This makes demand accounting impossible to get wrong
— it's always consistent with the actual service states.

### FabricRouteMiss as a Flag

`FabricRouteMiss` currently calls `WorkloadInput::DemandUp` directly, creating
orphaned demand (Bug 6). In the new model, this becomes a simple flag:

```rust
pub struct WorkloadStateMachine {
    // ...
    /// Set by FabricRouteMiss, cleared when workload reaches Running.
    /// Reconciliation treats this as additional demand (+1) so the workload
    /// wakes up even with no service-driven demand.
    pub route_miss_wake: bool,
}
```

The namespace sets `route_miss_wake = true` on route miss. Reconciliation
includes it in the effective demand calculation:

```rust
fn effective_demand(&self, workload_id: &WorkloadId) -> u32 {
    let service_demand = /* count from services */;
    let wake_demand = if self.workloads.get(workload_id)
        .map(|wl| wl.route_miss_wake)
        .unwrap_or(false)
    { 1 } else { 0 };
    service_demand + wake_demand
}
```

For now we preserve this bug. A proper fix involves adding a new event from worker
which transitions back to `route_miss_wake = false`. This is out of scope for this
refactor. Note bug clearly in comments in code.

**Future (requires worker-side changes):** The worker can send a
`FabricRouteMissResolved` event when a route is installed, or more precisely,
the route miss wake can be tied to a proper activation service with idle
timeout. For now the flag + clear-on-Running is correct and simple. The
reconciliation model makes this a trivial change when the worker support lands.

### Reconciliation Pass

After any SM step (workload or service), the namespace runs a reconciliation
pass for the affected workload group:

```rust
fn reconcile_workload_group(
    &mut self,
    workload_id: &WorkloadId,
    placement_table: &mut PlacementTable,
    out: &mut NamespaceOutput,
) {
    let mut iterations = 0;
    const MAX_ITERATIONS: usize = 20;

    loop {
        iterations += 1;
        assert!(iterations <= MAX_ITERATIONS, "reconciliation did not converge");
        let mut changed = false;

        // 1. Compute effective demand and update workload if needed.
        let demand = self.effective_demand(workload_id);
        if let Some(wl) = self.workloads.get(workload_id) {
            if wl.current_demand != demand {
                let wl = self.workloads.get_mut(workload_id).unwrap();
                let outputs = wl.step(
                    WorkloadInput::SetDemand { count: demand },
                    &self.namespace_id,
                );
                self.translate_effects(workload_id, outputs, placement_table, out);
                changed = true;
            }
        }

        // 2. Sync service readiness with workload state.
        let is_ready = self.workloads.get(workload_id)
            .map(|wl| matches!(wl.state, WorkloadState::Running { .. }))
            .unwrap_or(false);

        let ready_info = if is_ready {
            // Gather pod_id, worker_id, backend from workload + spec
            self.get_ready_info(workload_id)
        } else {
            None
        };

        let svc_ids: Vec<ServiceId> = self.services_for(workload_id);
        for svc_id in svc_ids {
            if let Some(svc) = self.services.get(&svc_id) {
                let needs_ready = matches!(svc.state,
                    ServiceState::NeedBackend | ServiceState::Pending)
                    && ready_info.is_some();
                let needs_unready = matches!(svc.state, ServiceState::Active { .. })
                    && ready_info.is_none();

                if needs_ready {
                    let info = ready_info.clone().unwrap();
                    let svc = self.services.get_mut(&svc_id).unwrap();
                    let outputs = svc.step(
                        ServiceInput::WorkloadReady { .. info },
                        &self.namespace_id,
                    );
                    self.translate_effects_svc(&svc_id, outputs, out);
                    changed = true;
                } else if needs_unready {
                    let svc = self.services.get_mut(&svc_id).unwrap();
                    let outputs = svc.step(
                        ServiceInput::WorkloadUnready,
                        &self.namespace_id,
                    );
                    self.translate_effects_svc(&svc_id, outputs, out);
                    changed = true;
                }
            }
        }

        if !changed {
            break;
        }
    }
}
```

In practice this loop converges in 1-2 iterations. The typical flow:
- Iteration 1: Workload transitions to Running → service gets WorkloadReady
- Iteration 2: Demand count unchanged, service state stable → done

### translate_effects (replaces translate_*_outputs)

The effect translation functions become much simpler because they only handle
pass-through effects. No more demand routing, no more readiness forwarding:

```rust
fn translate_effects(
    &mut self,
    workload_id: &WorkloadId,
    outputs: Vec<WorkloadOutput>,
    placement_table: &mut PlacementTable,
    out: &mut NamespaceOutput,
) {
    for wl_out in outputs {
        match wl_out {
            WorkloadOutput::PodRequest => { out.pod_requests.push(..); }
            WorkloadOutput::SuspendRequest { .. } => { /* resolve pool, emit cmd */ }
            WorkloadOutput::ResumeRequest { .. } => { out.resume_requests.push(..); }
            WorkloadOutput::DeleteArtifact { .. } => { /* resolve placement, emit cmd */ }
            WorkloadOutput::WorkerCommand(..) => { out.worker_commands.push(..); }
            WorkloadOutput::TimerSet(..) => { out.timers_set.push(..); }
            WorkloadOutput::TimerCancel(..) => { out.timers_cancel.push(..); }
            // No BecameReady, BecameUnready, or demand events to handle
        }
    }
}
```

### Workload SM Changes

Remove:
- `WorkloadInput::DemandUp` / `WorkloadInput::DemandDown`
- `WorkloadOutput::BecameReady` / `WorkloadOutput::BecameUnready`
- `demand_count` field

Add:
- `WorkloadInput::SetDemand { count: u32 }` — reconciliation tells the
  workload what its demand is
- `route_miss_wake: bool` field — set externally, included in demand
  calculation, cleared on Running
- `current_demand: u32` field — tracks what demand was last set to, so
  reconciliation can detect changes

The workload SM's `transition_on_demand` logic stays largely the same, but
it's driven by `SetDemand` instead of incremental up/down. The key simplification:
there's no way to get demand_count out of sync because it's set
authoritatively from service state, not accumulated from events.

### Service SM Changes

Remove:
- `ServiceOutput::DemandUp` / `ServiceOutput::DemandDown`

The service SM no longer needs to signal demand. Its state (`NeedBackend`,
`Active`, `Idle`) is directly observed by reconciliation. The service's
`WorkloadUnready` handler still transitions `Active → Idle` (activation) or
`Active → NeedBackend` (always-on), but doesn't emit demand events.

Similarly, `ServiceActivation` transitions `Idle → NeedBackend` without
emitting `DemandUp`. Reconciliation observes the state change and updates
workload demand accordingly.

This means the retry-aware demand preservation logic in `translate_workload_outputs`
(BecameUnready handler) goes away entirely. When a workload enters RetryBackoff:
- Always-on services stay in `NeedBackend` (already the case)
- Activation services go to `Idle` (their natural response to WorkloadUnready)

But reconciliation recomputes demand from service state. The activation service
in `Idle` doesn't contribute demand, so the workload's demand drops. But this
is actually correct if we handle it right in the workload SM: a workload in
`RetryBackoff` should remember that it *had* demand and stay in retry even if
current demand is 0. When the backoff timer fires, it re-enters
`WaitingForCapacity`, at which point if there's no demand it goes `Dormant`.

Actually, the simpler approach: **don't send WorkloadUnready to activation
services during retry.** The reconciliation pass can check whether the workload
is in a retrying state and skip the unready notification:

```rust
let needs_unready = matches!(svc.state, ServiceState::Active { .. })
    && ready_info.is_none()
    && !is_retrying;  // Don't drop demand during retry
```

This keeps activation services in their current state through retry, which is
exactly the current fix's intent but expressed as a simple boolean condition
rather than output filtering + re-activation.

## Impact on Stateright Model Checking

### Smaller Individual State Spaces

The workload SM loses `DemandUp`/`DemandDown` inputs and
`BecameReady`/`BecameUnready` outputs. The service SM loses
`DemandUp`/`DemandDown` outputs. Fewer inputs/outputs means fewer transitions,
meaning the state graph is smaller and can be explored more deeply.

### Independent Checkability

Each SM is more self-contained:
- Workload SM: pure lifecycle management. Only external input is `SetDemand`.
- Service SM: pure service routing state. No cross-SM side effects.

These can be model-checked independently with smaller state spaces.

### Reconciliation is Separately Testable

The reconciliation function is a pure function of `(workload_states,
service_states) → commands`. This can be exhaustively checked:
"for every reachable (workload_state, service_state) pair, reconciliation
produces correct commands."

This is a small state space because it's just the cartesian product of
workload states x service states, not the full interleaving of all possible
event orderings.

### Simpler Composition Model

The namespace-level stateright model becomes:
1. Pick a non-deterministic event
2. Step the relevant SM
3. Run reconciliation (deterministic)
4. Check properties

No queue-based output processing, no forwarding chains. The model checker
explores SM transitions; reconciliation is deterministic glue.

## Implementation Plan

### Phase 1: Remove demand routing ✅ COMPLETE

1. ✅ Add `wants_backend()` method to `ServiceStateMachine`
2. ✅ Add `effective_demand()` to `NamespaceStateMachine`
3. ✅ Replace `WorkloadInput::DemandUp/DemandDown` with `SetDemand { count: u32 }`
4. ✅ Add `current_demand` field to `WorkloadStateMachine` (renamed from `demand_count`)
5. ✅ Remove `ServiceOutput::DemandUp/DemandDown`
6. ✅ Remove demand routing from `translate_service_outputs`
7. ✅ Add demand reconciliation (`reconcile_demand`) after every SM step
8. ✅ Replace `FabricRouteMiss` DemandUp with `route_miss_wake` flag
9. ✅ Update stateright models, proptests, scenario tests

**All 147 tests pass** (72 lib, 45 e2e/scenario, 14 stateright workload,
7 stateright model, 4 stateright service, 4 proptest, 1 shell integration).

**Key implementation decisions:**

- `route_miss_wake` is NOT cleared on PodRunning (preserves Bug 6). Clearing
  it on PodRunning caused demand to drop to 0 before any service could
  activate, immediately shutting down the workload.
- Added `notify_late_joiner_services()` in `reconcile_demand`: when the
  workload is Running and any service is in NeedBackend, sends WorkloadReady.
  This replaces the old late-joiner logic from `translate_service_outputs`.
- BecameUnready handler during retry: sends WorkloadUnready normally (so
  services clean up backend references), then re-activates activation services
  that went Idle (Idle→NeedBackend via ServiceActivation). This preserves
  demand through `wants_backend()` while keeping backend references clean.

**Files changed:**
- `src/service.rs` — removed DemandUp/DemandDown outputs, added `wants_backend()`
- `src/workload.rs` — replaced DemandUp/DemandDown with SetDemand, renamed
  `demand_count` → `current_demand`, added `route_miss_wake`
- `src/namespace/reconciliation.rs` — added `effective_demand()`,
  `reconcile_demand()`, `reconcile_all_demand()`, `notify_late_joiner_services()`
- `src/namespace/output.rs` — simplified translate_service_outputs (pass-through
  only), simplified BecameUnready handler (send+reactivate pattern)
- `src/namespace/events.rs` — added reconcile_demand after every process_outputs,
  FabricRouteMiss uses route_miss_wake
- `src/namespace/commands.rs` — added reconcile_demand calls, renamed demand_count
- `tests/stateright_workload.rs` — SetDemand actions, current_demand, route_miss_wake
- `tests/stateright_model.rs` — updated WorkloadSnapshot mappings
- `tests/proptest.rs` — updated demand consistency check

### Phase 2: Remove readiness routing — 🔧 IN PROGRESS (partially applied, 1 test failing)

**What's done (changes are on the working tree, unstaged):**

1. ✅ Renamed `notify_late_joiner_services()` → `reconcile_readiness()` in
   `reconciliation.rs`, expanded to handle:
   - WorkloadReady: workload Running + service NeedBackend → send WorkloadReady
   - WorkloadUnready: workload not Running + service Active → send WorkloadUnready
   - Re-activation during retry: after sending WorkloadUnready, re-activate
     activation services that went Idle → NeedBackend (preserves demand)
   - BackendReady observability event emitted from reconcile_readiness
2. ✅ Removed `BecameReady`/`BecameUnready` from `WorkloadOutput` enum
3. ✅ Removed all ~18 emission sites in `workload.rs` `step()`
4. ✅ Removed BecameReady/BecameUnready match arms from `translate_workload_outputs`
   in `output.rs`
5. ✅ Removed unused `ServiceInput` import from `output.rs`
6. ✅ Updated scenario test comments in `multi_service.rs`, `resume_failure.rs`

**Test results: 1 failure**

- 72/72 lib tests pass ✅
- 44/45 scenario tests pass, **1 FAILS**: `test_demand_up_during_resume`
- Stateright, proptest not yet re-run (lib tests cover them)

**The failing test: `scenarios::transition_intents::test_demand_up_during_resume`**

```
workload 'ns/shared': expected Suspended, got Running
```

Test flow (uses `multi_service_spec` — two activation services svc-a/svc-b on
one suspend-on-idle workload, custom handler that hangs ResumePod):
1. Activate svc-a → workload launches → Running, svc-a Active
2. BackendNeed::None on svc-a → idle timer starts
3. `advance_time(31s)` → idle timer fires → svc-a Active → Idle
4. Expected: demand drops to 0 → SetDemand(0) → Suspending → SuspendPod →
   worker responds → Suspended
5. Actual: workload stays Running

**Root cause not yet identified.** The SetDemand(0) handler and Suspending
transition logic look correct after the edits. The idle timer path
(`IdleTimeout` → service step → `reconcile_demand`) is unchanged. Confirmed
test passes on the pre-change commit. Need to investigate why the workload
never leaves Running — either demand isn't dropping to 0, or
`reconcile_readiness` is interfering (e.g., re-sending WorkloadReady
somewhere it shouldn't).

**Key correction from plan:** The plan said "when retrying, do NOT send
WorkloadUnready" but the actual old behavior was: always send WorkloadUnready
(Active → Idle), then re-activate activation services (Idle → NeedBackend).
The initial implementation matched the plan and caused 2 test failures
(`test_worker_loss_during_active_service`, `test_namespace_failed_treats_like_worker_loss`).
Fixed by restructuring reconcile_readiness to always send WorkloadUnready when
not Running, then re-activate during retry as a second pass. Those 2 tests
now pass.

**Files changed:**
- `src/namespace/reconciliation.rs` — `reconcile_readiness()` replacing
  `notify_late_joiner_services()`
- `src/workload.rs` — removed BecameReady/BecameUnready enum variants + all
  emission sites
- `src/namespace/output.rs` — removed BecameReady/BecameUnready match arms,
  removed unused ServiceInput import
- `tests/scenarios/multi_service.rs` — updated comments
- `tests/scenarios/resume_failure.rs` — updated comments

### Phase 3: Simplify output translation

1. Remove `PendingOutput` enum and `process_outputs` queue
2. Collapse `translate_workload_outputs` and `translate_service_outputs` into
   simple effect pass-through (`translate_effects`)
3. Replace `reconcile_all_services` with the new reconciliation pass
4. Update stateright models, verify properties still hold

### Phase 4: Clean up

1. Remove dead code from service/workload SMs
2. Verify stateright model checking covers the same or better properties
3. Verify all existing scenario tests pass
4. Add new scenario test for FabricRouteMiss demand lifecycle

## Risks and Tradeoffs

**Reconciliation runs after every step.** This is cheap — just state
comparisons and counting. The current queue-based processing is actually more
expensive (allocates VecDeque, iterates outputs, filters demand events).

**Convergence loop.** Similar to the current queue but semantically clearer.
Bounded at ~20 iterations (in practice 1-2).

**Less explicit causality in traces.** With event routing, you can trace
exactly which event caused which state change. With reconciliation, the causal
chain is "something changed → reconciliation ran → these updates happened."
This is a debugging tradeoff. Mitigated by good logging in the reconciliation
pass.

**Bigger refactor than incremental fixes.** But the incremental fixes are
getting more complex and fragile. Each new fix adds special cases to the
routing layer. The refactor eliminates the need for these cases.
