use super::*;
use std::time::Duration;

// ============================================================================
// Retry backoff + Failed terminal state tests
// ============================================================================

/// 20. Pod fails → workload enters backoff → timer fires → new pod created → succeeds.
#[test]
fn pod_failure_backoff_and_retry() {
    let mut router = Router::new(16);
    let (mgmt, worker) = setup_running_workload(&mut router, 5);

    // Kill worker → pod fails.
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.pod_running);
    assert!(wl.in_backoff);
    assert_eq!(wl.consecutive_failures, 1);
    assert!(wl.pod_id.is_none()); // pod released

    // Workload should have signaled a retry backoff timer.
    assert_timer_requested(&mut router, &[TimerRequest {
        key: WorkloadTimerKey::RetryBackoff,
        generation: 1,
        ..Default::default()
    }]);

    // Timer fires — backoff cleared, reconcile creates new pod.
    router.send_workload_timer_fired(TIMER, W1, WorkloadTimerKey::RetryBackoff);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.in_backoff);
    assert!(wl.pod_id.is_some());
    let new_pod = wl.pod_id.unwrap();

    // Timer signal should now be empty (backoff cleared).
    assert_no_timers_wanted(&mut router);

    // New worker + make pod running.
    let worker2 = router.create_worker();
    router.set_worker_info(worker2, WorkerInfo { capacity: 10 });
    make_pod_running(&mut router, worker2, new_pod);

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);
    assert_eq!(wl.consecutive_failures, 0); // reset on success

    // No timers while running.
    assert_no_timer_output(&mut router);
}

/// 21. Multiple failures increment the counter.
#[test]
fn consecutive_failures_increment() {
    let mut router = Router::new(16);
    let (mgmt, worker) = setup_running_workload(&mut router, 5);

    // First failure.
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 1);
    assert!(wl.in_backoff);

    // Generation 1 timer requested.
    assert_timer_requested(&mut router, &[TimerRequest {
        key: WorkloadTimerKey::RetryBackoff,
        generation: 1,
        ..Default::default()
    }]);

    // Timer fires → retry.
    router.send_workload_timer_fired(TIMER, W1, WorkloadTimerKey::RetryBackoff);
    router.propagate();

    let pod2 = router.get_workload(&W1).unwrap().pod_id.unwrap();

    // Second failure (via direct status, not worker loss).
    let worker2 = router.create_worker();
    router.set_worker_info(worker2, WorkerInfo { capacity: 10 });
    make_pod_failed(&mut router, worker2, pod2);

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 2);
    assert!(wl.in_backoff);

    // Generation 2 timer requested (new backoff cycle).
    assert_timer_requested(&mut router, &[TimerRequest {
        key: WorkloadTimerKey::RetryBackoff,
        generation: 2,
        ..Default::default()
    }]);
}

/// 22. After max_retries failures, workload stops retrying (terminal Failed).
#[test]
fn max_retries_enters_failed() {
    let mut router = Router::new(16);
    let (mgmt, worker) = setup_running_workload(&mut router, 2);

    // First failure.
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 1);
    assert!(wl.in_backoff); // still under limit

    // Timer requested for first backoff.
    assert_timer_requested(&mut router, &[TimerRequest {
        key: WorkloadTimerKey::RetryBackoff,
        generation: 1,
        ..Default::default()
    }]);

    // Timer fires → retry.
    router.send_workload_timer_fired(TIMER, W1, WorkloadTimerKey::RetryBackoff);
    router.propagate();

    let pod2 = router.get_workload(&W1).unwrap().pod_id.unwrap();

    // Second failure — hits max_retries (2).
    let worker2 = router.create_worker();
    router.set_worker_info(worker2, WorkerInfo { capacity: 10 });
    make_pod_failed(&mut router, worker2, pod2);

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 2);
    assert!(!wl.in_backoff); // not in backoff — terminal
    assert!(wl.pod_id.is_none()); // no new pod
    assert!(!wl.wants_pod); // reconcile says no

    // Terminal failure: no timer requested (timers cleared).
    assert_no_timers_wanted(&mut router);
}

/// 23. Failed state + spec change → resets failures and retries.
#[test]
fn failed_recovery_via_spec_change() {
    let mut router = Router::new(16);
    let (mgmt, worker) = setup_running_workload(&mut router, 1);

    // One failure → hits max_retries (1) → terminal.
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 1);
    assert!(!wl.in_backoff);
    assert!(wl.pod_id.is_none());

    // Spec change resets failures.
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v2".into(), ..Default::default() });
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 0);
    assert!(!wl.in_backoff);
    assert!(wl.pod_id.is_some()); // new pod created
}

/// 24. Failed state + restart command → resets failures and retries.
#[test]
fn failed_recovery_via_restart() {
    let mut router = Router::new(16);
    let (mgmt, worker) = setup_running_workload(&mut router, 1);

    // One failure → terminal.
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_id.is_none());
    assert_eq!(wl.consecutive_failures, 1);

    // Restart resets failures.
    router.send_admin_command(mgmt, W1, AdminCmd::Restart);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 0);
    assert!(!wl.in_backoff);
    assert!(wl.pod_id.is_some()); // new pod created
}

/// 25. Failed + demand drops (clears) + demand returns → fresh start.
#[test]
fn failed_recovery_via_demand_cycle() {
    let mut router = Router::new(16);
    router.create_timer(TIMER);
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();
    router.create_schedule_request(SCHEDULE_REQUEST);
    router.create_workload(W1, WorkloadSm::with_max_retries(1));
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v1".into(), ..Default::default() });

    // Activation-based service so we can toggle demand.
    router.create_service(S1, ServiceSm::new(true));
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: true, ..Default::default() },
    );
    router.propagate();

    // Activate → demand → pod.
    router.send_activate_service(mgmt, S1, true);
    router.propagate();

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    make_pod_running(&mut router, worker, pod_id);

    // Fail → terminal (max_retries=1).
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 1);

    // Drop demand — clears failure state.
    router.send_activate_service(mgmt, S1, false);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 0);
    assert!(!wl.in_backoff);

    // Re-activate — fresh start, creates new pod.
    router.send_activate_service(mgmt, S1, true);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_id.is_some());
    assert_eq!(wl.consecutive_failures, 0);
}

/// 26. Failed + demand still present + more demand → stays Failed.
#[test]
fn failed_ignores_new_demand() {
    let mut router = Router::new(16);
    let (mgmt, worker) = setup_running_workload(&mut router, 1);

    // Fail → terminal.
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 1);
    assert!(wl.pod_id.is_none());

    // Add another service with demand — still Failed, no new pod.
    router.create_service(S2, ServiceSm::new(false));
    router.set_management_to_service_edges(mgmt, vec![S1, S2]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: false, ..Default::default() },
    );
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_demand);
    assert!(wl.pod_id.is_none()); // still Failed, no retry
    assert_eq!(wl.consecutive_failures, 1);
}

/// 27. In backoff + demand drops → goes dormant (clears backoff and failures).
#[test]
fn backoff_cleared_on_demand_drop() {
    let mut router = Router::new(16);
    router.create_timer(TIMER);
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();
    router.create_schedule_request(SCHEDULE_REQUEST);
    router.create_workload(W1, WorkloadSm::with_max_retries(5));
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v1".into(), ..Default::default() });

    router.create_service(S1, ServiceSm::new(true));
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: true, ..Default::default() },
    );
    router.propagate();

    // Activate → running pod.
    router.send_activate_service(mgmt, S1, true);
    router.propagate();

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    make_pod_running(&mut router, worker, pod_id);

    // Fail → enters backoff.
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.in_backoff);
    assert_eq!(wl.consecutive_failures, 1);

    // Timer requested during backoff.
    assert_timer_requested(&mut router, &[TimerRequest {
        key: WorkloadTimerKey::RetryBackoff,
        generation: 1,
        ..Default::default()
    }]);

    // Drop demand → clears everything.
    router.send_activate_service(mgmt, S1, false);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.in_backoff);
    assert_eq!(wl.consecutive_failures, 0);
    assert!(wl.pod_id.is_none());

    // Timer cleared after demand drop.
    assert_no_timers_wanted(&mut router);
}

/// 28. In backoff + spec change → clears backoff, immediate retry.
#[test]
fn backoff_cleared_on_spec_change() {
    let mut router = Router::new(16);
    let (mgmt, worker) = setup_running_workload(&mut router, 5);

    // Fail → enters backoff.
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.in_backoff);

    // Timer requested during backoff.
    assert_timer_requested(&mut router, &[TimerRequest {
        key: WorkloadTimerKey::RetryBackoff,
        generation: 1,
        ..Default::default()
    }]);

    // Spec change clears backoff + failures → immediate retry.
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v2".into(), ..Default::default() });
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.in_backoff);
    assert_eq!(wl.consecutive_failures, 0);
    assert!(wl.pod_id.is_some()); // new pod created immediately

    // Timer cleared after spec change.
    assert_no_timers_wanted(&mut router);
}

/// 29. Scavenge during backoff clears everything, goes dormant.
#[test]
fn scavenge_during_backoff() {
    let mut router = Router::new(16);
    let (mgmt, worker) = setup_running_workload(&mut router, 5);

    // Fail → enters backoff.
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.in_backoff);

    // Timer requested during backoff.
    assert_timer_requested(&mut router, &[TimerRequest {
        key: WorkloadTimerKey::RetryBackoff,
        generation: 1,
        ..Default::default()
    }]);

    // Scavenge is noop when demand is present (always-on service).
    // So scavenge won't do anything here — demand is still active.
    router.send_admin_command(mgmt, W1, AdminCmd::Scavenge);
    router.propagate();

    // Still in backoff because demand is active — timer unchanged (dedup suppresses).
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.in_backoff);
    assert_no_timer_output(&mut router);
}

/// 30. Scavenge during Failed clears failures (when no demand).
#[test]
fn scavenge_during_failed() {
    let mut router = Router::new(16);
    router.create_timer(TIMER);
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();
    router.create_schedule_request(SCHEDULE_REQUEST);
    router.create_workload(W1, WorkloadSm::with_max_retries(1));
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v1".into(), ..Default::default() });

    router.create_service(S1, ServiceSm::new(true));
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: true, ..Default::default() },
    );
    router.propagate();

    // Activate → pod → running.
    router.send_activate_service(mgmt, S1, true);
    router.propagate();

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    make_pod_running(&mut router, worker, pod_id);

    // Fail → terminal (max_retries=1).
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 1);

    // Drop demand first so scavenge doesn't noop.
    router.send_activate_service(mgmt, S1, false);
    router.propagate();

    // Scavenge clears everything.
    router.send_admin_command(mgmt, W1, AdminCmd::Scavenge);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 0);
    assert!(!wl.in_backoff);
    assert!(wl.pod_id.is_none());
}

/// 31. Success resets failure counter: fail, retry, succeed, fail again → counter=1.
#[test]
fn success_resets_failure_counter() {
    let mut router = Router::new(16);
    let (mgmt, worker) = setup_running_workload(&mut router, 5);

    // First failure.
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 1);

    // First backoff: generation 1.
    assert_timer_requested(&mut router, &[TimerRequest {
        key: WorkloadTimerKey::RetryBackoff,
        generation: 1,
        ..Default::default()
    }]);

    // Timer fires → retry.
    router.send_workload_timer_fired(TIMER, W1, WorkloadTimerKey::RetryBackoff);
    router.propagate();

    let pod2 = router.get_workload(&W1).unwrap().pod_id.unwrap();

    // Succeed — counter resets.
    let worker2 = router.create_worker();
    router.set_worker_info(worker2, WorkerInfo { capacity: 10 });
    make_pod_running(&mut router, worker2, pod2);

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 0);
    assert!(wl.pod_running);

    // Fail again — counter should be 1, not 2.
    router.destroy_worker(worker2);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 1);
    assert!(wl.in_backoff);

    // Second backoff: generation 2 (incremented again after success reset).
    assert_timer_requested(&mut router, &[TimerRequest {
        key: WorkloadTimerKey::RetryBackoff,
        generation: 2,
        ..Default::default()
    }]);
}
