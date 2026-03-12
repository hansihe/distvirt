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

## Design

### Pod State Machine

Extract pod lifecycle into its own state machine. A pod SM manages a single pod from launch to termination:

```
Launching → Running → Stopping → Gone
                   → Suspending → Suspended
                                → SuspendFailed
```

The pod SM is simple, linear, and independently testable. It receives events from the worker (`PodRunning`, `PodGone`, `PodSuspended`) and emits commands (`StopPod`). It does not know about demand, activation, or spec versions.

### Workload State Machine

The workload SM becomes a **pod coordinator**. It manages intent and decides when to launch/stop/suspend pods. It tracks:

```rust
pod: Option<(PodId, PodState)>          // the pod being driven toward Running
replacement: Option<(PodId, PodState)>  // temporary, during spec change transitions
```

The workload SM's own states simplify to intent-level concerns: does it have demand? Is it waiting for capacity? Is it in a failure/retry cycle? The pod-level details (launch timeout, suspend timeout) move into the pod SM.

`primary_pod` is derived: whichever pod is in `Running` state. Services always point to the primary.

### Spec Change Flow (Default — Concurrent)

1. Spec changes while workload has a Running pod
2. Workload emits `PodRequest` for replacement with new spec
3. Old pod stays primary, services stay `Active` — **no availability gap**
4. Replacement pod reaches `Running` → swap: replacement becomes primary
5. Old pod gets `StopPod`, workload SM forgets it — the pod SM drives it to completion independently
6. Services updated to point to new pod

### Spec Change Flow (Exclusive Resources — Sequential)

Some workloads have exclusive resource constraints (e.g., a DB pod with a persistent volume that can't be mounted by two pods simultaneously). These use sequential replacement:

1. Spec changes while workload has a Running pod
2. Workload stops primary pod first
3. After `PodGone`, launch new pod with new spec
4. Same as today's behavior — availability gap exists but is required for correctness

This is controlled by a workload-level property (e.g., `exclusive_resources: bool`). Sequential replacement is the current behavior, so it's already implemented.

### Rapid Spec Changes

If a new spec arrives while a replacement is still launching:

1. Send stop command to current replacement — pod SM drives it to completion
2. Launch new replacement with latest spec

The workload SM only ever tracks one replacement at a time. Stopped/retiring pods are "forgotten" by the workload SM — the pod SM and `pod_map` handle cleanup independently.

This could cause resource churn if specs change very rapidly (many pods launched and immediately cancelled). In practice this is unlikely to be a problem — launches are fast to cancel, and image pulls are typically cached. If needed, a debounce at the spec update level can be added later without changing the model.

## What Changes

### New: `PodStateMachine`

Small state machine managing a single pod's lifecycle. States roughly:

- `Launching { worker_id, launch_timeout }`
- `Running { worker_id }`
- `Suspending { worker_id, artifact_id, suspend_timeout }`
- `Stopping { worker_id }`

Inputs: `PodRunning`, `PodGone`, `PodSuspended`, `PodSuspendFailed`, `TimerFired`, `Stop`, `Suspend`.

Outputs: `StopPod`, `SetTimer`, `CancelTimer`, `BecameRunning`, `BecameGone`, `BecameSuspended`.

### Changed: `WorkloadStateMachine`

- States simplified to intent-level: `Dormant`, `WaitingForCapacity`, `Active`, `RetryBackoff`, `Failed`
- Pod tracking via `pod: Option<(PodId, PodState)>` + `replacement: Option<(PodId, PodState)>`
- `PendingIntent` simplified or removed — the replacement slot handles the spec-change-during-transition case directly
- `consecutive_failures` stays at the workload level (it's about whether the spec is launchable)
- Suspend/resume logic delegates to the pod SM

### Changed: `NamespaceStateMachine`

- `pod_map` continues to track all pods (including retiring ones the workload SM has forgotten)
- `PodGone` events for forgotten pods are handled by `pod_map` cleanup, no workload routing needed
- Reconciliation logic unchanged — it reads workload state, not pod state directly

### Changed: Reconciliation

- `reconcile_readiness` checks whether the workload's primary pod is Running
- During concurrent replacement, primary pod is still the old Running pod — services stay Active
- On swap, primary changes — `reconcile_readiness` updates services to point to new pod

### Stateright Model Checking

- `PodStateMachine`: trivial to check (~4 states, linear transitions)
- `WorkloadStateMachine`: state space shrinks — intent states are fewer than current combined intent+pod states
- Composition is checked at namespace level as before

### Test Impact

- Existing scenario tests should be largely unaffected — the external behavior is the same (except for the new zero-gap replacement)
- New scenarios for concurrent replacement, exclusive resources, rapid spec changes
- Workload stateright models need updating for new state shape
- Pod stateright models are new but small
