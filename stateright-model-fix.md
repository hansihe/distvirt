# Stateright Model Fix for Retry Backoff (Task 1.2)

## Status

- **Library code (`workload.rs`, `types/`, `namespace/`)**: Complete and compiles clean.
- **Workload-level stateright tests** (`stateright_workload.rs`): All 14 pass (including 3 new retry tests).
- **Unit/integration tests** (`--lib`): All 72 pass.
- **Namespace-level stateright model** (`stateright_model.rs`): 3 of 6 tests failing — `check_two_services`, `check_two_workers_two_services`, `check_delete_with_worker_failure`.

## Issue 1: Snapshot Missing `consecutive_failures` (FIXED)

`WorkloadSnapshot` did not include `consecutive_failures`. The model would reconstruct the SM with `consecutive_failures = 0` every step, losing backoff progress.

**Fix applied**: Added `consecutive_failures` to `WorkloadSnapshot`, `from_state_machine()`, and `to_state_machine()`.

## Issue 2: `"workloads have valid pods"` Property Violation (OPEN)

Even after the snapshot fix, the namespace-level model fails the `"workloads have valid pods"` property. The violation is **not directly about the backoff state** — it's about a different workload ending up in `Launching` with a `pod_id` that's not in the `pods` map.

### Last State from Counterexample (`check_two_services`)

```
svc-1 workload: Launching { pod_id: "pod-0", ... }, consecutive_failures: 0
svc-2 workload: RetryBackoff { ... }, consecutive_failures: 1
pods map: { "pod-1": svc-1, "pod-2": svc-1 }  ← pod-0 NOT present
pending_timers: {
    LaunchTimeout { svc-1, pod-0 },
    LaunchTimeout { svc-1, pod-3 },   ← two launch timeouts for same workload!
    LaunchTimeout { svc-2, pod-0 },
    RetryBackoffTimeout { svc-2 },
}
```

### Analysis

- svc-1 has **two** launch timeouts (pod-0 and pod-3), but a workload can only launch one pod at a time. This suggests the model explored a path where svc-1 was scheduled twice somehow.
- svc-1 is `Launching { pod_id: pod-0 }` but pod-0 is not in the pods map — there are stale pod-1/pod-2 entries instead.
- This may be a pre-existing latent issue now exposed because the new backoff behavior changes the state space the model explores. Previously, PodFailed → immediate WaitingForCapacity → PodRequest (same step). Now, PodFailed → RetryBackoff (timer set) → timer fires later → WaitingForCapacity. The async timer path creates new interleavings.

### Possible Root Causes to Investigate

1. **Pod ID reuse**: `next_free_pod_id()` scans for the lowest unused ID in pod_map. After a pod is removed (PodFailed), its ID can be reused for a different workload. If a stale event arrives for the reused ID, it could be routed to the wrong workload.

2. **Multiple scheduling in one step**: The model processes `pod_requests` synchronously in `next_state()`. If a single step produces multiple PodRequests (e.g., timer fires for svc-2 while svc-1 also has a pending request), both are scheduled, potentially creating conflicting state.

3. **Timer ordering**: The model fires one timer per action. But when multiple timers are pending, the order of firing may create states where stale timers interact with re-scheduled pods.

## Files Modified

- `distvirt-orchestrator/src/types/mod.rs` — `TimerKey::RetryBackoffTimeout`
- `distvirt-orchestrator/src/types/states.rs` — `WorkloadState::RetryBackoff`, `WorkloadState::Failed`
- `distvirt-orchestrator/src/workload.rs` — core backoff logic, `consecutive_failures` field, `SpecChanged`/`ManualRestart` inputs, `ConditionSet`/`ConditionClear` outputs
- `distvirt-orchestrator/src/namespace/events.rs` — `RetryBackoffTimeout` timer handling
- `distvirt-orchestrator/src/namespace/output.rs` — `ConditionSet`/`ConditionClear` forwarding (log only)
- `distvirt-orchestrator/src/namespace/commands.rs` — `RetryBackoff`/`Failed` in delete/remove-workload cleanup
- `distvirt-orchestrator/tests/stateright_workload.rs` — full model update, 3 new tests
- `distvirt-orchestrator/tests/stateright_model.rs` — snapshot fix applied, still needs debugging

## Key Design Decisions

- **Clean exit (exit_code 0) does NOT count as a failure** for backoff. Only non-zero exits, errors, worker loss, and timeouts increment `consecutive_failures`. This was needed because `test_always_on_service_lifecycle` expects immediate re-launch after exit code 0.
- **`consecutive_failures` resets to 0** on: `PodRunning` (success), `SpecChanged`, `ManualRestart`, or demand drops to 0 from `RetryBackoff`/`Failed` (→ `Dormant`).
- **`MAX_RETRIES = 5`**, backoff delay is `2^(failures-1)` seconds capped at 32s.

## Next Steps

1. Debug the namespace-level model failure — focus on the pod ID reuse / stale timer interaction
2. Consider whether the stateright_model needs a `"workloads have valid pods"` property relaxation or if there's a real SM bug exposed
3. Run full `cargo check` on workspace after fixes
