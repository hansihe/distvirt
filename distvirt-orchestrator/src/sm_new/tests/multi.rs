use super::*;

// ============================================================================
// Multi-workload tests
// ============================================================================

/// 53. Two workloads sharing a worker — worker dies, both workloads fail
///     independently and can recover on a new worker without interference.
#[test]
fn shared_worker_death_independent_failure() {
    let mut router = Router::new(16);
    router.create_timer(TIMER);
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);

    let mgmt = router.create_management();

    // Create two workloads, each with their own always-on service.
    router.create_workload(W1, WorkloadSm::new());
    router.create_workload(W2, WorkloadSm::new());

    router.set_management_to_workload_edges(mgmt, vec![W1, W2]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v1".into(), ..Default::default() });

    router.create_service(S1, ServiceSm::new(false));
    router.create_service(S2, ServiceSm::new(false));

    // S1 → W1, S2 → W2 (different management ports for different specs).
    let mgmt_s1 = router.create_management();
    router.set_management_to_service_edges(mgmt_s1, vec![S1]);
    router.set_management_svc_spec(
        mgmt_s1,
        ServiceSpec {
            workload: W1,
            has_activation: false,
        },
    );

    let mgmt_s2 = router.create_management();
    router.set_management_to_service_edges(mgmt_s2, vec![S2]);
    router.set_management_svc_spec(
        mgmt_s2,
        ServiceSpec {
            workload: W2,
            has_activation: false,
        },
    );
    router.propagate();

    // Both workloads should have demand and created pods.
    let wl1 = router.get_workload(&W1).unwrap();
    let wl2 = router.get_workload(&W2).unwrap();
    assert!(wl1.has_demand);
    assert!(wl2.has_demand);
    let pod1 = wl1.pod_id.unwrap();
    let pod2 = wl2.pod_id.unwrap();
    assert_ne!(pod1, pod2);

    // Both pods on same worker.
    router.set_worker_to_pod_edges(worker, vec![pod1, pod2]);
    router.send_notify_pod_status(worker, pod1, PodStatus::Running);
    router.send_notify_pod_status(worker, pod2, PodStatus::Running);
    router.propagate();

    // Both services should be active.
    let s1 = router.get_service(&S1).unwrap();
    let s2 = router.get_service(&S2).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));
    assert!(matches!(s2.state, ServiceState::Active { .. }));

    // Both workloads should report the correct worker.
    let wl1 = router.get_workload(&W1).unwrap();
    let wl2 = router.get_workload(&W2).unwrap();
    assert_eq!(wl1.pod_worker_id, Some(worker));
    assert_eq!(wl2.pod_worker_id, Some(worker));

    // Worker dies.
    router.destroy_worker(worker);
    router.propagate();

    // Both workloads should have failed independently.
    let wl1 = router.get_workload(&W1).unwrap();
    let wl2 = router.get_workload(&W2).unwrap();
    assert!(!wl1.pod_running);
    assert!(!wl2.pod_running);
    assert!(wl1.in_backoff);
    assert!(wl2.in_backoff);
    assert_eq!(wl1.consecutive_failures, 1);
    assert_eq!(wl2.consecutive_failures, 1);
    assert!(wl1.pod_id.is_none());
    assert!(wl2.pod_id.is_none());

    // Both services should be back to NeedBackend.
    let s1 = router.get_service(&S1).unwrap();
    let s2 = router.get_service(&S2).unwrap();
    assert_eq!(s1.state, ServiceState::NeedBackend);
    assert_eq!(s2.state, ServiceState::NeedBackend);

    // Fire both backoff timers.
    router.send_workload_timer_fired(TIMER, W1, WorkloadTimerKey::RetryBackoff);
    router.send_workload_timer_fired(TIMER, W2, WorkloadTimerKey::RetryBackoff);
    router.propagate();

    // Both should have created new pods.
    let wl1 = router.get_workload(&W1).unwrap();
    let wl2 = router.get_workload(&W2).unwrap();
    assert!(wl1.pod_id.is_some());
    assert!(wl2.pod_id.is_some());
    let new_pod1 = wl1.pod_id.unwrap();
    let new_pod2 = wl2.pod_id.unwrap();
    assert_ne!(new_pod1, new_pod2);

    // New worker — recover both.
    let worker2 = router.create_worker();
    router.set_worker_info(worker2, WorkerInfo { capacity: 10 });
    router.set_worker_to_pod_edges(worker2, vec![new_pod1, new_pod2]);
    router.send_notify_pod_status(worker2, new_pod1, PodStatus::Running);
    router.send_notify_pod_status(worker2, new_pod2, PodStatus::Running);
    router.propagate();

    // Both workloads should be running again.
    let wl1 = router.get_workload(&W1).unwrap();
    let wl2 = router.get_workload(&W2).unwrap();
    assert!(wl1.pod_running);
    assert!(wl2.pod_running);
    assert_eq!(wl1.consecutive_failures, 0);
    assert_eq!(wl2.consecutive_failures, 0);

    // Both services active again.
    let s1 = router.get_service(&S1).unwrap();
    let s2 = router.get_service(&S2).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));
    assert!(matches!(s2.state, ServiceState::Active { .. }));
}

/// 54. Service retargeting: service spec changes from workload W1 to W2.
///     Demand should transfer cleanly — old workload loses demand, new one gains it.
#[test]
fn service_retarget_workload() {
    let mut router = Router::new(16);
    router.create_timer(TIMER);
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);

    let mgmt = router.create_management();

    // Create two workloads with specs.
    router.create_workload(W1, WorkloadSm::new());
    router.create_workload(W2, WorkloadSm::new());
    router.set_management_to_workload_edges(mgmt, vec![W1, W2]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v1".into(), ..Default::default() });

    // One always-on service pointing at W1.
    router.create_service(S1, ServiceSm::new(false));
    let mgmt_s1 = router.create_management();
    router.set_management_to_service_edges(mgmt_s1, vec![S1]);
    router.set_management_svc_spec(
        mgmt_s1,
        ServiceSpec {
            workload: W1,
            has_activation: false,
        },
    );
    router.propagate();

    // W1 should have demand, W2 should not.
    let wl1 = router.get_workload(&W1).unwrap();
    let wl2 = router.get_workload(&W2).unwrap();
    assert!(wl1.has_demand);
    assert!(!wl2.has_demand);
    assert!(wl1.pod_id.is_some());
    assert!(wl2.pod_id.is_none());

    // Make W1's pod running.
    let pod1 = wl1.pod_id.unwrap();
    make_pod_running(&mut router, worker, pod1);

    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));

    // Retarget S1 from W1 → W2.
    router.set_management_svc_spec(
        mgmt_s1,
        ServiceSpec {
            workload: W2,
            has_activation: false,
        },
    );
    router.propagate();

    // W1 should have lost demand, W2 should have gained it.
    let wl1 = router.get_workload(&W1).unwrap();
    let wl2 = router.get_workload(&W2).unwrap();
    assert!(!wl1.has_demand);
    assert!(wl2.has_demand);

    // W2 should have created a pod.
    assert!(wl2.pod_id.is_some());

    // W1's pod should be destroyed (demand dropped, not suspendable).
    assert!(wl1.pod_id.is_none());

    // Make W2's pod running.
    let pod2 = wl2.pod_id.unwrap();
    router.set_worker_to_pod_edges(worker, vec![pod2]);
    router.send_notify_pod_status(worker, pod2, PodStatus::Running);
    router.propagate();

    // S1 should be active with W2's readiness.
    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));

    let wl2 = router.get_workload(&W2).unwrap();
    assert!(wl2.pod_running);
}

/// 55. Two independent workload-service subgraphs coexist without interference.
#[test]
fn independent_workload_subgraphs() {
    let mut router = Router::new(16);
    router.create_timer(TIMER);
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);

    // W1 + S1 subgraph.
    router.create_workload(W1, WorkloadSm::new());
    router.create_service(S1, ServiceSm::new(true)); // activation-based

    let mgmt1 = router.create_management();
    router.set_management_to_workload_edges(mgmt1, vec![W1]);
    router.set_management_wl_spec(mgmt1, WorkloadSpec { image: "app-a:v1".into(), ..Default::default() });
    router.set_management_to_service_edges(mgmt1, vec![S1]);
    router.set_management_svc_spec(
        mgmt1,
        ServiceSpec {
            workload: W1,
            has_activation: true,
        },
    );

    // W2 + S2 subgraph.
    router.create_workload(W2, WorkloadSm::new());
    router.create_service(S2, ServiceSm::new(false)); // always-on

    let mgmt2 = router.create_management();
    router.set_management_to_workload_edges(mgmt2, vec![W2]);
    router.set_management_wl_spec(mgmt2, WorkloadSpec { image: "app-b:v1".into(), ..Default::default() });
    router.set_management_to_service_edges(mgmt2, vec![S2]);
    router.set_management_svc_spec(
        mgmt2,
        ServiceSpec {
            workload: W2,
            has_activation: false,
        },
    );
    router.propagate();

    // W1 has no demand (activation-based, not activated).
    // W2 has demand (always-on).
    let wl1 = router.get_workload(&W1).unwrap();
    let wl2 = router.get_workload(&W2).unwrap();
    assert!(!wl1.has_demand);
    assert!(wl1.pod_id.is_none());
    assert!(wl2.has_demand);
    assert!(wl2.pod_id.is_some());

    // Make W2's pod running.
    let pod2 = wl2.pod_id.unwrap();
    make_pod_running(&mut router, worker, pod2);

    let s2 = router.get_service(&S2).unwrap();
    assert!(matches!(s2.state, ServiceState::Active { .. }));

    // W1 is still idle — W2's activity doesn't affect it.
    let wl1 = router.get_workload(&W1).unwrap();
    assert!(!wl1.has_demand);
    let s1 = router.get_service(&S1).unwrap();
    assert_eq!(s1.state, ServiceState::Idle);

    // Activate S1 — now W1 also gets demand.
    router.send_activate_service(mgmt1, S1, true);
    router.propagate();

    let wl1 = router.get_workload(&W1).unwrap();
    assert!(wl1.has_demand);
    assert!(wl1.pod_id.is_some());

    let pod1 = wl1.pod_id.unwrap();
    router.set_worker_to_pod_edges(worker, vec![pod1, pod2]);
    router.send_notify_pod_status(worker, pod1, PodStatus::Running);
    router.propagate();

    // Both active, independent.
    let s1 = router.get_service(&S1).unwrap();
    let s2 = router.get_service(&S2).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));
    assert!(matches!(s2.state, ServiceState::Active { .. }));

    // Deactivate S1 — only W1 affected.
    router.send_activate_service(mgmt1, S1, false);
    router.propagate();

    let wl1 = router.get_workload(&W1).unwrap();
    let wl2 = router.get_workload(&W2).unwrap();
    assert!(!wl1.has_demand);
    assert!(wl1.pod_id.is_none());
    assert!(wl2.pod_running); // W2 unaffected

    let s1 = router.get_service(&S1).unwrap();
    let s2 = router.get_service(&S2).unwrap();
    assert_eq!(s1.state, ServiceState::Idle);
    assert!(matches!(s2.state, ServiceState::Active { .. }));
}

/// 56. Multiple services sharing a workload, one retargets away — demand
///     aggregation correctly updates for both the source and target workloads.
#[test]
fn service_fan_in_with_retarget() {
    let mut router = Router::new(16);
    router.create_timer(TIMER);
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);

    // Both workloads get specs.
    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new());
    router.create_workload(W2, WorkloadSm::new());
    router.set_management_to_workload_edges(mgmt, vec![W1, W2]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v1".into(), ..Default::default() });

    // Two always-on services both pointing at W1.
    router.create_service(S1, ServiceSm::new(false));
    router.create_service(S2, ServiceSm::new(false));

    let mgmt_s1 = router.create_management();
    router.set_management_to_service_edges(mgmt_s1, vec![S1]);
    router.set_management_svc_spec(
        mgmt_s1,
        ServiceSpec {
            workload: W1,
            has_activation: false,
        },
    );

    let mgmt_s2 = router.create_management();
    router.set_management_to_service_edges(mgmt_s2, vec![S2]);
    router.set_management_svc_spec(
        mgmt_s2,
        ServiceSpec {
            workload: W1,
            has_activation: false,
        },
    );
    router.propagate();

    // W1 has demand from both services, W2 has none.
    let wl1 = router.get_workload(&W1).unwrap();
    let wl2 = router.get_workload(&W2).unwrap();
    assert!(wl1.has_demand);
    assert!(!wl2.has_demand);
    assert!(wl1.pod_id.is_some());
    assert!(wl2.pod_id.is_none());

    // Make W1's pod running.
    let pod1 = wl1.pod_id.unwrap();
    make_pod_running(&mut router, worker, pod1);

    // Both services active via W1.
    let s1 = router.get_service(&S1).unwrap();
    let s2 = router.get_service(&S2).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));
    assert!(matches!(s2.state, ServiceState::Active { .. }));

    // Retarget S2 from W1 → W2.
    router.set_management_svc_spec(
        mgmt_s2,
        ServiceSpec {
            workload: W2,
            has_activation: false,
        },
    );
    router.propagate();

    // W1 still has demand (S1 still points at it).
    let wl1 = router.get_workload(&W1).unwrap();
    assert!(wl1.has_demand);
    assert!(wl1.pod_running); // still running

    // W2 now has demand (S2 retargeted to it).
    let wl2 = router.get_workload(&W2).unwrap();
    assert!(wl2.has_demand);
    assert!(wl2.pod_id.is_some());

    // S1 should still be active (W1 still has a running pod).
    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));

    // S2 should be NeedBackend (W2's pod is Pending, no readiness yet).
    let s2 = router.get_service(&S2).unwrap();
    assert_eq!(s2.state, ServiceState::NeedBackend);

    // Make W2's pod running.
    let pod2 = wl2.pod_id.unwrap();
    router.set_worker_to_pod_edges(worker, vec![pod1, pod2]);
    router.send_notify_pod_status(worker, pod2, PodStatus::Running);
    router.propagate();

    // Now S2 should also be active.
    let s2 = router.get_service(&S2).unwrap();
    assert!(matches!(s2.state, ServiceState::Active { .. }));

    // Both workloads running on same worker.
    let wl1 = router.get_workload(&W1).unwrap();
    let wl2 = router.get_workload(&W2).unwrap();
    assert!(wl1.pod_running);
    assert!(wl2.pod_running);
}

/// 57. Service self-destructs when its management spec is removed.
#[test]
fn service_self_destructs_on_spec_removal() {
    let mut router = Router::new(16);
    router.create_timer(TIMER);
    router.create_schedule_request(SCHEDULE_REQUEST);
    router.create_workload(W1, WorkloadSm::new());
    router.create_service(S1, ServiceSm::new(false)); // always-on

    // Use separate mgmt ports so we can remove the service spec independently.
    let mgmt_wl = router.create_management();
    router.set_management_to_workload_edges(mgmt_wl, vec![W1]);
    router.set_management_wl_spec(mgmt_wl, WorkloadSpec { image: "app:v1".into(), ..Default::default() });

    let mgmt_svc = router.create_management();
    router.set_management_to_service_edges(mgmt_svc, vec![S1]);
    router.set_management_svc_spec(
        mgmt_svc,
        ServiceSpec {
            workload: W1,
            has_activation: false,
        },
    );
    router.propagate();

    // Service is alive and workload has demand.
    assert!(router.get_service(&S1).is_some());
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_demand);

    // Remove service management port — service spec becomes None.
    router.destroy_management(mgmt_svc);
    router.propagate();

    // Service should have self-destructed.
    assert!(router.get_service(&S1).is_none());

    // Workload should have lost demand (service's outgoing edges vanished).
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.has_demand);
}

/// 58. Workload self-destructs when its management spec is removed, pod cleans up.
#[test]
fn workload_self_destructs_on_spec_removal() {
    let mut router = Router::new(16);
    let (mgmt, worker) = setup_running_workload(&mut router, 5);

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);
    let pod_id = wl.pod_id.unwrap();

    // Service is active.
    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));

    // Remove management port — workload spec becomes None.
    router.destroy_management(mgmt);
    router.propagate();

    // Workload should have self-destructed.
    assert!(router.get_workload(&W1).is_none());

    // Service should also have self-destructed (its spec also came from mgmt).
    assert!(router.get_service(&S1).is_none());

    // Pod lost its owner edge → terminal + no owner → self-destruct.
    // The pod may need a worker status update to reach terminal first.
    // Send a Failed status to trigger the reaping rule.
    router.send_notify_pod_status(worker, pod_id, PodStatus::Failed);
    router.propagate();

    assert!(router.get_pod(&pod_id).is_none());
}

/// 59. Full teardown cascade: management → service → workload → pod, all gone.
#[test]
fn full_teardown_cascade() {
    let mut router = Router::new(16);
    router.create_timer(TIMER);
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);

    // Set up two independent service→workload subgraphs via separate mgmt ports.
    let mgmt_wl = router.create_management();
    router.create_workload(W1, WorkloadSm::new());
    router.create_workload(W2, WorkloadSm::new());
    router.set_management_to_workload_edges(mgmt_wl, vec![W1, W2]);
    router.set_management_wl_spec(mgmt_wl, WorkloadSpec { image: "app:v1".into(), ..Default::default() });

    let mgmt_s1 = router.create_management();
    router.create_service(S1, ServiceSm::new(false));
    router.set_management_to_service_edges(mgmt_s1, vec![S1]);
    router.set_management_svc_spec(
        mgmt_s1,
        ServiceSpec {
            workload: W1,
            has_activation: false,
        },
    );

    let mgmt_s2 = router.create_management();
    router.create_service(S2, ServiceSm::new(false));
    router.set_management_to_service_edges(mgmt_s2, vec![S2]);
    router.set_management_svc_spec(
        mgmt_s2,
        ServiceSpec {
            workload: W2,
            has_activation: false,
        },
    );
    router.propagate();

    // Both workloads have pods.
    let pod1 = router.get_workload(&W1).unwrap().pod_id.unwrap();
    let pod2 = router.get_workload(&W2).unwrap().pod_id.unwrap();

    // Make both pods running.
    router.set_worker_to_pod_edges(worker, vec![pod1, pod2]);
    router.send_notify_pod_status(worker, pod1, PodStatus::Running);
    router.send_notify_pod_status(worker, pod2, PodStatus::Running);
    router.propagate();

    assert!(router.get_workload(&W1).unwrap().pod_running);
    assert!(router.get_workload(&W2).unwrap().pod_running);

    // Remove all management ports.
    router.destroy_management(mgmt_wl);
    router.destroy_management(mgmt_s1);
    router.destroy_management(mgmt_s2);
    router.propagate();

    // Both services and workloads should have self-destructed.
    assert!(router.get_service(&S1).is_none());
    assert!(router.get_service(&S2).is_none());
    assert!(router.get_workload(&W1).is_none());
    assert!(router.get_workload(&W2).is_none());

    // Pods lost owners — send terminal status to trigger reaping.
    router.send_notify_pod_status(worker, pod1, PodStatus::Failed);
    router.send_notify_pod_status(worker, pod2, PodStatus::Failed);
    router.propagate();

    assert!(router.get_pod(&pod1).is_none());
    assert!(router.get_pod(&pod2).is_none());
}

/// 60. Workload in retry backoff self-destructs cleanly on spec removal.
#[test]
fn teardown_during_backoff() {
    let mut router = Router::new(16);
    let (mgmt, worker) = setup_running_workload(&mut router, 5);

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();

    // Pod fails → workload enters backoff.
    make_pod_failed(&mut router, worker, pod_id);

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.in_backoff);
    assert!(wl.pod_id.is_none());

    // Remove management port during backoff.
    router.destroy_management(mgmt);
    router.propagate();

    // Workload and service should have self-destructed.
    assert!(router.get_workload(&W1).is_none());
    assert!(router.get_service(&S1).is_none());
}

/// 61. Workload awaiting suspend self-destructs on spec removal, pod cleans up.
#[test]
fn teardown_during_suspend() {
    let mut router = Router::new(16);
    let (mgmt, worker) = setup_running_suspendable_workload(&mut router);

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);
    let pod_id = wl.pod_id.unwrap();

    // Deactivate service → demand drops → workload signals Suspend.
    router.send_activate_service(mgmt, S1, false);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.awaiting_suspend);

    // Remove management port while awaiting suspend.
    router.destroy_management(mgmt);
    router.propagate();

    // Workload and service should have self-destructed.
    assert!(router.get_workload(&W1).is_none());
    assert!(router.get_service(&S1).is_none());

    // Pod lost owner — send terminal status to trigger reaping.
    router.send_notify_pod_status(worker, pod_id, PodStatus::Failed);
    router.propagate();

    assert!(router.get_pod(&pod_id).is_none());
}
