use super::*;

// ============================================================================
// Graceful exit (Finished) + Worker identity tests
// ============================================================================

/// 46. Graceful pod exit (Finished) does not increment failure counter and
///     does not enter backoff. Workload creates new pod if demand exists.
#[test]
fn graceful_exit_no_failure_count() {
    let mut router = Router::new(16);
    let (mgmt, worker) = setup_running_workload(&mut router, 5);

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);
    let pod_id = wl.pod_id.unwrap();

    // Pod exits gracefully.
    router.send_notify_pod_status(worker, pod_id, PodStatus::Finished);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.pod_running);
    assert_eq!(wl.consecutive_failures, 0); // no failure increment
    assert!(!wl.in_backoff); // no backoff

    // Workload should have created a new pod (demand still exists).
    assert!(wl.pod_id.is_some());
    assert_ne!(wl.pod_id.unwrap(), pod_id); // new pod

    // No retry timer needed.
    assert_no_timers_wanted(&mut router);
}

/// 47. Graceful exit (Finished) does not count as failure even after prior
///     failures — consecutive_failures stays unchanged.
#[test]
fn graceful_exit_after_failures_preserves_count() {
    let mut router = Router::new(16);
    let (_mgmt, worker) = setup_running_workload(&mut router, 5);

    // First failure via worker loss.
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 1);
    assert!(wl.in_backoff);

    // Timer fires → retry.
    router.send_workload_timer_fired(TIMER, W1, WorkloadTimerKey::RetryBackoff);
    router.propagate();

    let pod2 = router.get_workload(&W1).unwrap().pod_id.unwrap();

    // New worker, make pod running.
    let worker2 = router.create_worker();
    router.set_worker_info(worker2, WorkerInfo { capacity: 10 });
    make_pod_running(&mut router, worker2, pod2);

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);
    assert_eq!(wl.consecutive_failures, 0); // reset on Running

    // Pod finishes gracefully.
    router.send_notify_pod_status(worker2, pod2, PodStatus::Finished);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 0); // still 0, not incremented
    assert!(!wl.in_backoff); // no backoff for graceful exit
    assert!(wl.pod_id.is_some()); // new pod created (demand exists)
}

/// 48. Finished vs Failed: Finished after Running doesn't enter backoff,
///     Failed after Running does.
#[test]
fn finished_vs_failed_backoff_behavior() {
    // Finished path: no backoff.
    let mut router = Router::new(16);
    let (_mgmt, worker) = setup_running_workload(&mut router, 5);
    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();

    router.send_notify_pod_status(worker, pod_id, PodStatus::Finished);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.in_backoff);
    assert_eq!(wl.consecutive_failures, 0);
    assert!(wl.pod_id.is_some()); // immediately created new pod

    // Failed path: enters backoff.
    let mut router = Router::new(16);
    let (_mgmt, worker) = setup_running_workload(&mut router, 5);
    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();

    router.send_notify_pod_status(worker, pod_id, PodStatus::Failed);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.in_backoff);
    assert_eq!(wl.consecutive_failures, 1);
    assert!(wl.pod_id.is_none()); // waiting for backoff timer
}

/// 49. Pod self-destructs on Finished + no owner (same reaping rule as Failed).
#[test]
fn finished_pod_self_destructs() {
    let mut router = Router::new(16);
    router.create_timer(TIMER);
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);

    // Create a standalone pod (no workload owner).
    let pod_id = router.create_pod(PodSm::new());
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Running);
    router.propagate();

    assert_eq!(router.get_pod(&pod_id).unwrap().status, PodStatus::Running);

    // Pod finishes gracefully — terminal + no owner → self-destruct.
    router.send_notify_pod_status(worker, pod_id, PodStatus::Finished);
    router.propagate();

    assert!(router.get_pod(&pod_id).is_none());
}

/// 50. Worker identity: readiness carries the correct worker ID from the pod's
///     assigned worker, not a placeholder.
#[test]
fn worker_identity_in_readiness() {
    let mut router = Router::new(16);
    router.create_timer(TIMER);
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new());
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v1".into(), ..Default::default() });

    router.create_service(S1, ServiceSm::new(false)); // always-on
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: false, ..Default::default() },
    );
    router.propagate();

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();

    // Wire worker to pod and make running.
    make_pod_running(&mut router, worker, pod_id);

    // Pod should know its worker.
    let pod = router.get_pod(&pod_id).unwrap();
    assert_eq!(pod.worker_id, Some(worker));

    // Workload should have the worker ID.
    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.pod_worker_id, Some(worker));

    // Service readiness should carry the real worker ID.
    let s1 = router.get_service(&S1).unwrap();
    match &s1.state {
        ServiceState::Active { ready } => {
            assert_eq!(ready.worker_id, worker);
            assert_eq!(ready.pod_id, pod_id);
        }
        other => panic!("expected Active, got {:?}", other),
    }
}

/// 51. Worker identity updates when pod moves to a different worker
///     (e.g., after failure and re-creation on new worker).
#[test]
fn worker_identity_updates_on_new_worker() {
    let mut router = Router::new(16);
    let (_mgmt, worker1) = setup_running_workload(&mut router, 5);

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.pod_worker_id, Some(worker1));

    // Worker1 dies → pod fails → backoff.
    router.destroy_worker(worker1);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.in_backoff);
    assert_eq!(wl.pod_worker_id, None); // cleared on failure

    // Timer fires → new pod created.
    router.send_workload_timer_fired(TIMER, W1, WorkloadTimerKey::RetryBackoff);
    router.propagate();

    let new_pod = router.get_workload(&W1).unwrap().pod_id.unwrap();

    // New worker takes over.
    let worker2 = router.create_worker();
    router.set_worker_info(worker2, WorkerInfo { capacity: 10 });
    make_pod_running(&mut router, worker2, new_pod);

    // Workload should now report worker2.
    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.pod_worker_id, Some(worker2));

    // Service readiness should carry worker2.
    let s1 = router.get_service(&S1).unwrap();
    match &s1.state {
        ServiceState::Active { ready } => {
            assert_eq!(ready.worker_id, worker2);
        }
        other => panic!("expected Active, got {:?}", other),
    }
}

/// 52. Pod tracks worker from WorkerInput signal (not from event).
#[test]
fn pod_tracks_worker_from_input() {
    let mut router = Router::new(16);
    router.create_timer(TIMER);
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);

    let pod_id = router.create_pod(PodSm::new());
    router.propagate();

    // No worker assigned yet.
    let pod = router.get_pod(&pod_id).unwrap();
    assert_eq!(pod.worker_id, None);

    // Assign worker via edge.
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.propagate();

    // Pod should now know its worker.
    let pod = router.get_pod(&pod_id).unwrap();
    assert_eq!(pod.worker_id, Some(worker));

    // Remove worker edge → worker_id cleared.
    router.set_worker_to_pod_edges(worker, vec![]);
    router.propagate();

    // Pod should have failed (worker lost) and self-destructed (no owner).
    assert!(router.get_pod(&pod_id).is_none());
}
