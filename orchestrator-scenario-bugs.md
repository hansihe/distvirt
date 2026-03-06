# Orchestrator Scenario Test Bugs

Bugs exposed by tightening scenario test assertions to assert on correct behavior
rather than vague negative assertions (e.g. "not Suspending" → "Dormant").

## 1. `route_miss_wake` demand leak

DO NOT FIX YET

**Test:** `test_route_miss_demand_leak`
**Expected:** `Suspended` — **Got:** `Running`

`FabricRouteMiss` sets `route_miss_wake` on the workload, contributing +1 to
`effective_demand`. This flag is never cleared, even after a service takes over
demand. When all services go idle and send `BackendNeed::None`, the workload
stays `Running` because `route_miss_wake` keeps `effective_demand >= 1`.

**Fix:** Clear `route_miss_wake` once a service activates and takes over demand
(i.e. on `ServiceActivation` or when any service transitions to `Active`).

## 2. Pod failure during suspend counted as crash

**Tests:** `test_pod_exit_during_suspend`, `test_pod_exited_during_suspend`
**Expected:** `Dormant` — **Got:** `RetryBackoff`

When a pod crashes or exits during an intentional suspend (demand is already 0),
the orchestrator treats it as a regular pod failure and increments
`consecutive_failures`, entering `RetryBackoff`. Since the suspend was
intentionally initiated and demand is 0, the pod loss should not count as a
failure — the workload should transition directly to `Dormant`.

**Fix:** In the `PodFailed`/`PodExited` handler, check if the workload is in
`Suspending` state. If so, treat it as a clean deactivation (not a failure).

## 3. `PodSuspendFailed` triggers retry backoff instead of stop fallback

**Test:** `test_suspend_failure_fallback_to_stop`
**Expected:** `Dormant` — **Got:** `RetryBackoff`

When `PodSuspendFailed` is received, the orchestrator should fall back to
`StopPod` and transition to `Dormant` (demand is 0). Instead, it enters
`RetryBackoff`, suggesting `PodSuspendFailed` is being routed through the
generic failure path rather than the suspend-specific fallback path.

**Fix:** Handle `PodSuspendFailed` distinctly from `PodFailed` — issue `StopPod`
and transition to `Dormant` without incrementing `consecutive_failures`.

## 4. Spec change during launch loses demand signal

**Test:** `test_spec_change_during_launch`
**Expected:** `WaitingForCapacity` or `Launching` — **Got:** `Dormant`

For an always-on workload: spec change sets `PendingIntent::Restart` during
launch. When `PodRunning` arrives, `StopPod` is issued and the workload goes
`Dormant`. But reconciliation fails to re-assert demand for the always-on
service, so the workload stays `Dormant` instead of transitioning to
`WaitingForCapacity`.

**Fix:** After a `Restart` intent completes and the workload goes `Dormant`,
`transition_on_demand` should check current service demand. Alternatively,
reconciliation should re-evaluate always-on service demand after spec changes.

## 5. Worker disconnect during suspend sets `WaitingForCapacity` with no demand

**Test:** `test_worker_disconnect_during_suspend`
**Expected:** `Dormant` — **Got:** `WaitingForCapacity`

When the worker disconnects during a suspend (demand is 0, service went idle),
the workload transitions to `WaitingForCapacity` instead of `Dormant`. This
suggests the worker-lost handler unconditionally sets a flag like
`needs_successful_boot` that artificially keeps demand alive.

**Fix:** Worker-lost handling during `Suspending` should check current demand.
If demand is 0, transition to `Dormant` rather than `WaitingForCapacity`.

## 6. Worker disconnect on Suspended workload creates phantom demand

**Test:** `test_worker_disconnect_clears_placements`
**Expected:** `Dormant` — **Got:** `WaitingForCapacity`

When a worker disconnects while holding an artifact for a cleanly-Suspended
workload (demand=0, service went idle), `WorkerLost` sets
`needs_successful_boot = true` (workload.rs:671). Reconciliation then sees
`needs_boot` is true and re-activates the idle activation service back to
`NeedBackend` (reconciliation.rs:245), creating phantom demand that keeps the
workload alive in `WaitingForCapacity`.

`needs_successful_boot` should not be set for a workload that was cleanly
suspended — it was never "trying to boot."

**Fix:** In the `WorkerLost` handler, only set `needs_successful_boot` for
states where the workload was actively running or booting (Launching, Running,
Resuming), not for Suspended (which has demand=0 by definition).

## 7. Service removal doesn't immediately drop demand

**Test:** `test_remove_only_active_service_drops_demand`

When the only active service is removed via spec update, demand should drop to
0 immediately via reconciliation (the service is gone, not idling). The workload
should begin suspending right after `converge()` without needing an idle timer.
The old test used `advance_time()` which may have been masking a delay in demand
recalculation.
