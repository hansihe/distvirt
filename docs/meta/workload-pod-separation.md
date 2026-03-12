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

**Types** (defined in `src/sm/pod.rs`, re-exported via `types::*`):

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
```

**Types** (defined in `src/sm/workload.rs`, re-exported via `types::*`):

```rust
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

### Phase 2.5: Extract pod SM + module reorganization — DONE

Extracted pod lifecycle into its own state machine with `PodSlot::step()`, and reorganized all state machines under `src/sm/`:

**Module structure:**
- `src/sm/mod.rs` — re-exports `pod`, `workload`, `service` modules
- `src/sm/pod.rs` — pod lifecycle SM (`PodSlot::step()`, `PodInput`, `PodOutput`, `PodOutcome`), owns `PodState` and `PodSlot` types
- `src/sm/workload.rs` — workload coordinator SM, delegates pod lifecycle to `PodSlot::step()`, owns `WorkloadState`, `PendingIntent`, `RetiredPod` types
- `src/sm/service.rs` — service SM, owns `ServiceState` type

**Pod SM design:** Rather than a separate `PodStateMachine` struct, methods were added directly to `PodSlot` via `impl PodSlot` in `pod.rs`. This avoids touching all pattern matches that destructure `PodSlot` fields. The pod SM handles:
- State transitions: Launching→Running, Resuming→Running, Running→Suspending, timeouts
- Timer management: sets/cancels launch, suspend, resume timeouts
- Artifact cleanup: deletes artifacts on successful resume or timeout

The pod SM returns `(PodOutcome, Vec<PodOutput>)` from `step()`. The workload SM interprets the outcome for workload-level decisions (retry, demand check, intent handling) and converts `PodOutput`s to `WorkloadOutput`s via `collect_pod_outputs()`.

**Key helper methods on PodSlot:**
- `new_launching()` / `new_resuming()` — constructors that return `(PodSlot, Vec<PodOutput>)` with initial timer setup
- `initiate_suspend()` — transitions Running→Suspending with timer and suspend request
- `step()` — processes `PodInput` events, returns outcome + side effects

**Type ownership:** SM-specific types (`PodState`, `PodSlot`, `WorkloadState`, `PendingIntent`, `RetiredPod`, `ServiceState`) are defined in their respective SM modules. `src/types/states.rs` re-exports them via `pub use crate::sm::*` so `use crate::types::*` continues to work everywhere — no import changes needed outside the SM modules.

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

Pod lifecycle as an independent state machine implemented on `PodSlot` in `src/sm/pod.rs`. A pod SM manages a single pod from launch to termination:

```
Launching → Running → Stopping → Gone
                   → Suspending → Suspended
                                → SuspendFailed
Resuming  → Running
```

The pod SM is simple, linear, and independently testable. It receives events from the worker (`PodRunning`, `PodGone`, `PodSuspended`) and emits side effects (`TimerSet`, `TimerCancel`, `DeleteArtifact`, `SuspendRequest`). It does not know about demand, activation, or spec versions. Retirement (`StopPod`) is handled by the workload SM — the pod SM's `TimedOut` outcome signals the caller to retire the pod.

**Design decision:** The pod SM is implemented as methods on `PodSlot` rather than a separate struct. This keeps pattern matching on `PodSlot` fields ergonomic throughout the workload SM and namespace layer. The `worker_lost` flag on `PodInput::PodGone` suppresses artifact deletion for Resuming pods when the worker is lost (namespace layer handles cleanup for lost workers).

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
- Phase 2.5: All existing tests pass; re-exports mean no import changes outside SM modules (done)
- Phase 3: Existing tests pass (sequential fallback is default for existing specs); new scenarios for concurrent replacement, exclusive resources, rapid spec changes
- Stateright models updated incrementally per phase
