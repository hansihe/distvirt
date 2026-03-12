---
title: "Workload / Pod Separation"
---

## Problem

The `WorkloadStateMachine` conflates two concerns:

1. **Intent management** — should this workload be running? What spec version? Suspend on idle?
2. **Pod lifecycle** — what state is this specific pod in? Launching, running, suspending?

Today, workload state *is* pod state (`Launching { pod_id }`, `Running { pod_id }`). This means a workload can only have one pod at a time. On spec change, the sequence is:

```
Running(old_pod) → StopPod → WaitingForCapacity → Launching(new_pod) → Running(new_pod)
```

This creates an availability gap. Services cycle `Active → NeedBackend → Active`, traffic gets buffered by activation or dropped for always-on services. A routine deploy violates the "slow first request, not an error" DX contract.

The `PendingIntent` mechanism partially compensates — it records contradicting signals during transitions — but it exists because the workload SM can't hold two pods. With a clean separation, most of `PendingIntent`'s complexity goes away.

## Implementation Phases

### Phase 1: Extract PodSlot — DONE

Mechanical refactor: moved pod lifecycle fields out of `WorkloadState` variants into `PodSlot` and `PodState` structs. No behavior change.

**Types introduced** (in `src/types/states.rs`):

```rust
pub enum PodState {
    Launching { launch_timeout: TimerKey },
    Running,
    Suspending { artifact_id: ArtifactId, suspend_timeout: TimerKey },
    Resuming { artifact_id: ArtifactId, resume_timeout: TimerKey },
}

pub struct PodSlot {
    pub pod_id: PodId,
    pub worker_id: WorkerId,
    pub pod_state: PodState,
}

pub enum WorkloadState {
    Dormant,
    WaitingForCapacity,
    Active { pod: PodSlot, pending: PendingIntent },
    Suspended { artifact_id: ArtifactId },
    RetryBackoff { backoff_timer: TimerKey },
    Failed,
    Transitioning, // sentinel for mem::replace destructuring
}
```

Helper methods on `WorkloadState`: `pod_id()`, `worker_id()`, `artifact_id()`, `is_running()`, `active_pod()`, `as_str()`.

All existing tests (unit, scenario, stateright model, stateright workload) pass unchanged.

### Phase 2: Add retiring list — DONE

When a pod is stopped, instead of immediately forgetting it, track it in `retiring: Vec<RetiredPod>` on `WorkloadStateMachine` until PodGone confirms it's dead. This decouples the workload SM's "forget about this pod" from the actual pod lifecycle completing.

```rust
pub struct RetiredPod {
    pub pod_id: PodId,
    pub worker_id: WorkerId,
}
```

When `step()` emits `StopPod`, the pod moves to `retiring` via `retire_pod()` helper instead of being forgotten. The workload SM owns all retiring logic internally — the namespace layer forwards events as normal (`PodGone`, `PodRunning`, `PodSuspendFailed`) and the workload SM checks the retiring list in its handlers:

- `PodGone`: if pod is retiring, clean up entry and return (no failure counting, no state change)
- `PodRunning`: if pod is retiring, ignore (StopPod is in flight, pod will eventually exit)
- `PodSuspendFailed`: if pod is retiring, clean up entry and return
- `WorkerLost`: clean up all retiring pods on the lost worker

Timer handlers (LaunchTimeout, SuspendTimeout, ResumeTimeout) no longer eagerly remove pods from `pod_map` — pods stay tracked until PodExited/PodFailed confirms they're actually gone.

**Design principle:** The namespace is a thin routing/coordination layer. It does not inspect workload internals (`is_retiring`) to decide event routing — the workload SM handles that internally.

Low risk — additive change, no external behavior difference. All existing tests pass.

### Phase 3: Replacement pod slot + concurrent spec change

Add `replacement: Option<PodSlot>` to `WorkloadStateMachine`. On `SpecChanged` during `Active` with `PodState::Running`, launch a replacement concurrently instead of stop-then-launch.

**Concurrent replacement flow:**

1. Spec changes while workload has a Running pod
2. Workload emits `PodRequest` for replacement with new spec
3. Old pod stays primary, services stay `Active` — **no availability gap**
4. Replacement reaches `Running` → swap: replacement becomes primary, old → retiring
5. Services updated to point to new pod

**Sequential fallback** for workloads with `exclusive_resources: true` — stops first, then launches. This is the current behavior.

**Rapid spec changes:** If new spec arrives while replacement is launching, stop current replacement (→ retiring), launch new replacement with latest spec. Only one replacement at a time.

**SpecChanged in non-Running states:** Unchanged — `PendingIntent::Restart` handles transitions in progress, `DeleteArtifact` handles suspended state.

**Event routing:** The namespace forwards events as normal via `pod_map` lookup. The workload SM's handlers check `pod_id` against primary, replacement, and retiring list internally. The namespace's `pod_map` continues to track all pods until confirmed gone.

### Phase 4: Stateright model updates (incremental with each phase)

- Phase 2: Add retiring tracking to model state
- Phase 3: Add replacement actions, new properties:
  - Safety: "at most one primary and one replacement"
  - Liveness: "retiring list eventually empties"
  - Liveness: "replacement running implies swap"

## Design Details

### Pod State Machine

Pod lifecycle as an independent state machine. A pod SM manages a single pod from launch to termination:

```
Launching → Running → Stopping → Gone
                   → Suspending → Suspended
                                → SuspendFailed
```

The pod SM is simple, linear, and independently testable. It receives events from the worker (`PodRunning`, `PodGone`, `PodSuspended`) and emits commands (`StopPod`). It does not know about demand, activation, or spec versions.

After Phase 1, the `PodState` enum already captures this shape. The question is whether to extract a full `PodStateMachine` with its own `step()` or keep pod transitions inline in `WorkloadStateMachine.step()`. The current inline approach works well — the pod state transitions are simple enough that a separate SM may be over-abstraction. Revisit if `step()` grows unwieldy.

### Workload State Machine

The workload SM is a **pod coordinator**. It manages intent and decides when to launch/stop/suspend pods:

```rust
// After Phase 3:
pub struct WorkloadStateMachine {
    pub state: WorkloadState,      // intent-level
    // pod is inside Active variant as PodSlot
    pub replacement: Option<PodSlot>,
    pub retiring: Vec<RetiredPod>,
    pub consecutive_failures: u32, // stays at workload level
    // ...
}
```

`primary_pod` is derived: whichever pod is in `Running` state. Services always point to the primary.

After Phase 3, `PendingIntent` can be simplified — the `Restart` variant is only needed for transitions where concurrent replacement isn't possible (Launching, Suspending, Resuming). For Running state, the replacement slot handles spec changes directly.

### Reconciliation

- `reconcile_readiness` checks whether the workload's primary pod is Running (uses `is_running()`)
- During concurrent replacement, primary pod is still the old Running pod — services stay Active
- On swap, primary changes — `reconcile_readiness` updates services to point to new pod
- `emit_endpoint_update_for_workload` needs updating in Phase 3 to pick the primary pod when multiple pods exist

### Test Impact

- Phase 1: All existing tests pass unchanged (done)
- Phase 2: All existing tests pass; stateright model updated with retiring tracking (done)
- Phase 3: Existing tests pass (sequential fallback is default for existing specs); new scenarios for concurrent replacement, exclusive resources, rapid spec changes
- Stateright models updated incrementally per phase
