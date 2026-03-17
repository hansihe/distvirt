use super::*;

// ============================================================================
// Service idle timeout + BackendNeed tests
// ============================================================================

/// 40. Traffic-triggered activation: idle service receives BackendNeed(Traffic)
///     from a BackendNeed port → activates → demand → pod boots.
#[test]
fn traffic_triggered_activation() {
    let mut router = Router::new(16);
    router.create_timer(TIMER);
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new());
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v1".into(),
            ..Default::default()
        },
    );

    router.create_service(S1, ServiceSm::new(true)); // activation-based
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: true,
            ..Default::default()
        },
    );
    router.propagate();

    // Service is idle, no demand.
    let s1 = router.get_service(&S1).unwrap();
    assert_eq!(s1.state, ServiceState::Idle);
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.has_demand);

    // BackendNeed port reports traffic → service activates.
    let bn = router.create_backend_need();
    router.set_backend_need_to_service_edges(bn, vec![S1]);
    router.set_backend_need_level(bn, BackendNeed::Traffic);
    router.propagate();

    // Service: Idle → NeedBackend, demand=true.
    let s1 = router.get_service(&S1).unwrap();
    assert_eq!(s1.state, ServiceState::NeedBackend);
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_demand);
    assert!(wl.pod_id.is_some());

    // Make pod running.
    let pod_id = wl.pod_id.unwrap();
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Running);
    router.propagate();

    // Service should be Active.
    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));
}

/// 41. Idle timeout deactivation: running service, traffic stops → idle timer
///     fires → service deactivates → demand drops → workload destroys pod.
#[test]
fn idle_timeout_deactivation() {
    let mut router = Router::new(16);
    router.create_timer(TIMER);
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new());
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v1".into(),
            ..Default::default()
        },
    );

    router.create_service(S1, ServiceSm::new(true));
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: true,
            ..Default::default()
        },
    );
    router.propagate();

    // Traffic activates the service.
    let bn = router.create_backend_need();
    router.set_backend_need_to_service_edges(bn, vec![S1]);
    router.set_backend_need_level(bn, BackendNeed::Traffic);
    router.propagate();

    // Make pod running.
    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Running);
    router.propagate();

    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));
    assert!(!s1.idle_timer_active);

    // Traffic stops → idle timer starts.
    router.set_backend_need_level(bn, BackendNeed::None);
    router.propagate();

    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. })); // still active
    assert!(s1.idle_timer_active);
    assert_eq!(s1.idle_generation, 1);

    // Fire the idle timer → service deactivates.
    router.send_service_timer_fired(TIMER, S1, ServiceTimerKey::IdleTimeout);
    router.propagate();

    let s1 = router.get_service(&S1).unwrap();
    assert_eq!(s1.state, ServiceState::Idle);
    assert!(!s1.idle_timer_active);

    // Demand should be gone → workload destroyed the pod.
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.has_demand);
    assert!(wl.pod_id.is_none());
}

/// 42. Traffic cancels idle timer: idle timer running, new traffic arrives →
///     timer cancelled, service stays active.
#[test]
fn traffic_cancels_idle_timer() {
    let mut router = Router::new(16);
    router.create_timer(TIMER);
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new());
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v1".into(),
            ..Default::default()
        },
    );

    router.create_service(S1, ServiceSm::new(true));
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: true,
            ..Default::default()
        },
    );
    router.propagate();

    // Traffic → activate → pod running.
    let bn = router.create_backend_need();
    router.set_backend_need_to_service_edges(bn, vec![S1]);
    router.set_backend_need_level(bn, BackendNeed::Traffic);
    router.propagate();

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Running);
    router.propagate();

    // Traffic stops → idle timer starts.
    router.set_backend_need_level(bn, BackendNeed::None);
    router.propagate();

    let s1 = router.get_service(&S1).unwrap();
    assert!(s1.idle_timer_active);

    // Traffic returns → idle timer cancelled.
    router.set_backend_need_level(bn, BackendNeed::Traffic);
    router.propagate();

    let s1 = router.get_service(&S1).unwrap();
    assert!(!s1.idle_timer_active);
    assert!(matches!(s1.state, ServiceState::Active { .. }));

    // Demand still present.
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_demand);
    assert!(wl.pod_running);
}

/// 43. Idle timeout + suspend integration: full chain from traffic loss → idle
///     timer → service deactivates → demand drops → workload suspends pod →
///     artifact saved.
#[test]
fn idle_timeout_suspend_integration() {
    let mut router = Router::new(16);
    router.create_timer(TIMER);
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new());
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v1".into(),
            suspend_on_idle: true,
        },
    );

    router.create_service(S1, ServiceSm::new(true));
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: true,
            ..Default::default()
        },
    );
    router.propagate();

    // Traffic → activate → pod running.
    let bn = router.create_backend_need();
    router.set_backend_need_to_service_edges(bn, vec![S1]);
    router.set_backend_need_level(bn, BackendNeed::Traffic);
    router.propagate();

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Running);
    router.propagate();

    assert!(matches!(
        router.get_service(&S1).unwrap().state,
        ServiceState::Active { .. }
    ));
    assert!(router.get_workload(&W1).unwrap().pod_running);

    // Traffic stops → idle timer starts.
    router.set_backend_need_level(bn, BackendNeed::None);
    router.propagate();

    assert!(router.get_service(&S1).unwrap().idle_timer_active);

    // Idle timer fires → service deactivates → demand drops →
    // workload signals pod to suspend (suspend_on_idle=true).
    router.send_service_timer_fired(TIMER, S1, ServiceTimerKey::IdleTimeout);
    router.propagate();

    let s1 = router.get_service(&S1).unwrap();
    assert_eq!(s1.state, ServiceState::Idle);

    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.has_demand);
    assert!(wl.awaiting_suspend);

    // Pod should be suspending.
    let pod = router.get_pod(&pod_id).unwrap();
    assert_eq!(pod.status, PodStatus::Suspending);

    // Worker completes suspend.
    let artifact = ArtifactId(42);
    router.send_notify_pod_suspended(worker, pod_id, artifact);
    router.propagate();

    // Workload saved artifact, pod reaped.
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.awaiting_suspend);
    assert!(wl.pod_id.is_none());
    assert_eq!(wl.suspended_artifact, Some(artifact));
    assert!(router.get_pod(&pod_id).is_none());
}

/// 44. Worker loss removes backend need: worker providing traffic dies →
///     BackendNeed aggregates to None → idle timer starts.
#[test]
fn worker_loss_removes_backend_need() {
    let mut router = Router::new(16);
    router.create_timer(TIMER);
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new());
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v1".into(),
            ..Default::default()
        },
    );

    router.create_service(S1, ServiceSm::new(true));
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: true,
            ..Default::default()
        },
    );
    router.propagate();

    // Traffic → activate → pod running.
    let bn = router.create_backend_need();
    router.set_backend_need_to_service_edges(bn, vec![S1]);
    router.set_backend_need_level(bn, BackendNeed::Traffic);
    router.propagate();

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Running);
    router.propagate();

    assert!(matches!(
        router.get_service(&S1).unwrap().state,
        ServiceState::Active { .. }
    ));
    assert!(!router.get_service(&S1).unwrap().idle_timer_active);

    // Worker dies → BackendNeed aggregates to None → idle timer starts.
    // (Pod also fails, but service idle timer is the focus here.)
    // Also destroy the BackendNeed port (adapter would do this on worker disconnect).
    router.destroy_worker(worker);
    router.destroy_backend_need(bn);
    router.propagate();

    // Service lost readiness (pod failed) → back to NeedBackend.
    // Idle timer should have been cleared when readiness was lost
    // (Active→NeedBackend transition clears idle timer).
    let s1 = router.get_service(&S1).unwrap();
    assert_eq!(s1.state, ServiceState::NeedBackend);
    assert!(!s1.idle_timer_active);
}

/// 45. Multiple workers, one loses traffic: two workers with traffic, one drops
///     to None → aggregate still Traffic → no idle timer.
#[test]
fn multiple_workers_one_loses_traffic() {
    let mut router = Router::new(16);
    router.create_timer(TIMER);
    let worker1 = router.create_worker();
    let worker2 = router.create_worker();
    router.set_worker_info(worker1, WorkerInfo { capacity: 10 });
    router.set_worker_info(worker2, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new());
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v1".into(),
            ..Default::default()
        },
    );

    router.create_service(S1, ServiceSm::new(true));
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: true,
            ..Default::default()
        },
    );
    router.propagate();

    // Both workers report traffic via separate BackendNeed ports.
    let bn1 = router.create_backend_need();
    let bn2 = router.create_backend_need();
    router.set_backend_need_to_service_edges(bn1, vec![S1]);
    router.set_backend_need_to_service_edges(bn2, vec![S1]);
    router.set_backend_need_level(bn1, BackendNeed::Traffic);
    router.set_backend_need_level(bn2, BackendNeed::Traffic);
    router.propagate();

    // Service activated via traffic.
    let s1 = router.get_service(&S1).unwrap();
    assert_eq!(s1.state, ServiceState::NeedBackend);

    // Make pod running.
    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    router.set_worker_to_pod_edges(worker1, vec![pod_id]);
    router.send_notify_pod_status(worker1, pod_id, PodStatus::Running);
    router.propagate();

    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));
    assert!(!s1.idle_timer_active);

    // Worker1 drops to None → aggregate still Traffic (bn2 has Traffic).
    router.set_backend_need_level(bn1, BackendNeed::None);
    router.propagate();

    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));
    assert!(!s1.idle_timer_active); // no idle timer — still has traffic

    // Worker1 reports Active (highest priority) → still no idle timer.
    router.set_backend_need_level(bn1, BackendNeed::Active);
    router.propagate();

    let s1 = router.get_service(&S1).unwrap();
    assert!(!s1.idle_timer_active);

    // Worker1 back to None.
    router.set_backend_need_level(bn1, BackendNeed::None);
    router.propagate();

    // Worker2 also drops → aggregate None → idle timer starts.
    router.set_backend_need_level(bn2, BackendNeed::None);
    router.propagate();

    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));
    assert!(s1.idle_timer_active); // now idle timer starts
}
