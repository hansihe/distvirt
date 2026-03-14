use super::*;

/// 1. Demand aggregation: 3 activation-based services → 1 workload, toggle demand.
#[test]
fn demand_aggregation() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    router.create_workload(W1, WorkloadSm::new(timer));
    router.create_service(S1, ServiceSm::new(timer, true));
    router.create_service(S2, ServiceSm::new(timer, true));
    router.create_service(S3, ServiceSm::new(timer, true));

    // Deliver specs through management port — services get edges to W1.
    let mgmt = router.create_management();
    router.set_management_to_service_edges(mgmt, vec![S1, S2, S3]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: true,
        },
    );
    router.propagate();

    // No demand yet (all activation-based, none activated).
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.has_demand);

    // S1 activates.
    router.send_activate_service(mgmt, S1, true);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_demand);

    // S2 also activates.
    router.send_activate_service(mgmt, S2, true);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_demand);

    // Both deactivate.
    router.send_activate_service(mgmt, S1, false);
    router.send_activate_service(mgmt, S2, false);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.has_demand);
}

/// 2. Reactive readiness edges: workload creates WorkloadToService edges
///    based on which services point at it, then readiness propagates back.
#[test]
fn reactive_readiness_edges() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new(timer));
    router.create_service(S1, ServiceSm::new(timer, false)); // always-on
    router.create_service(S2, ServiceSm::new(timer, false));

    // Deliver workload spec.
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "test:latest".into() });

    // Deliver service specs — always-on services auto-set demand + edges.
    router.set_management_to_service_edges(mgmt, vec![S1, S2]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: false,
        },
    );
    router.propagate();

    // Workload should have received demand (from always-on services).
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_demand);

    // Both services should be in NeedBackend (demand set, no readiness yet).
    let s1 = router.get_service(&S1).unwrap();
    assert_eq!(s1.state, ServiceState::NeedBackend);

    // Workload created a pod in reconcile(). Wire worker and make it running.
    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Running);
    router.propagate();

    // Both services should be active now (readiness propagated via reactive edges).
    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));
    let s2 = router.get_service(&S2).unwrap();
    assert!(matches!(s2.state, ServiceState::Active { .. }));

    // Add a third service — it should immediately get readiness.
    router.create_service(S3, ServiceSm::new(timer, false));
    // Use same mgmt port, update edges to include S3.
    router.set_management_to_service_edges(mgmt, vec![S1, S2, S3]);
    router.propagate();

    // S3 got its spec, set demand + edges, workload re-aggregated,
    // readiness propagated to all three services including S3.
    let s3 = router.get_service(&S3).unwrap();
    assert!(matches!(s3.state, ServiceState::Active { .. }));
}

/// 3. Pod lifecycle through signals: workload creates pod in handler,
///    pod status flows back, readiness propagates to services.
#[test]
fn pod_lifecycle() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new(timer));
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "test:latest".into() });
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_spec);
    assert!(!wl.has_demand);

    // Add an always-on service with demand.
    router.create_service(S1, ServiceSm::new(timer, false));
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: false,
        },
    );
    router.propagate();

    // Workload should have created a pod (has spec + demand).
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.wants_pod);
    let pod_id = wl.pod_id.unwrap();

    // Pod is pending — workload sees PodStatus::Pending.
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.pod_running);

    // Wire worker to pod and report running.
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Running);
    router.propagate();

    // Workload should be ready now.
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);

    // Readiness should have propagated to S1.
    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));
}

/// 4. Worker port removal: worker dies, pod sees empty WorkerInput,
///    status goes to Failed, workload sees readiness lost.
#[test]
fn worker_loss_via_port_removal() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new(timer));
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v1".into() });

    router.create_service(S1, ServiceSm::new(timer, false));
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: false,
        },
    );
    router.propagate();

    // Workload created pod. Wire worker and start it.
    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Running);
    router.propagate();

    // Verify everything is active.
    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));

    // No timers wanted while running.
    assert_no_timers_wanted(&mut router, timer);

    // Worker dies — remove the port.
    router.destroy_worker(worker);
    router.propagate();

    // Pod was failed and workload released it (on_pod_failed → self-destruct).
    // Workload should have lost readiness and entered backoff for retry.
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.pod_running);
    assert!(wl.in_backoff);

    // Timer signal should show a retry backoff request.
    assert_timer_requested(&mut router, timer, &[TimerRequest {
        key: WorkloadTimerKey::RetryBackoff,
        generation: 1,
    }]);

    // Service should be back to NeedBackend.
    let s1 = router.get_service(&S1).unwrap();
    assert_eq!(s1.state, ServiceState::NeedBackend);
}

/// 5. Spec delivery via management port: init and update use same path.
#[test]
fn spec_via_management_port() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    router.create_workload(W1, WorkloadSm::new(timer));

    let mgmt = router.create_management();
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "v1".into() });
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_spec);

    // Update spec.
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "v2".into() });
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_spec);

    // Remove management port — workload self-destructs.
    router.destroy_management(mgmt);
    router.propagate();

    assert!(router.get_workload(&W1).is_none());
}

/// 6. Service spec via management port: service reads its spec, creates
///    edges to the target workload reactively.
#[test]
fn service_spec_creates_edges_reactively() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    router.create_workload(W1, WorkloadSm::new(timer));
    router.create_service(S1, ServiceSm::new(timer, false));

    // Management port delivers service spec that points at W1.
    let mgmt = router.create_management();
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: false,
        },
    );
    router.propagate();

    // Service should have reactively created edges and set demand=true.
    // Verify indirectly: workload received demand via the edge.
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_demand);

    // Service should be in NeedBackend (always-on with demand set).
    let s1 = router.get_service(&S1).unwrap();
    assert_eq!(s1.state, ServiceState::NeedBackend);
}

/// 7. Admin command event: management port sends restart to workload.
#[test]
fn admin_restart_event() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new(timer));
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v1".into() });

    router.create_service(S1, ServiceSm::new(timer, false));
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: false,
        },
    );
    router.propagate();

    // Workload created pod. Wire worker and start it.
    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Running);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);

    // Send admin restart.
    router.send_admin_command(mgmt, W1, AdminCmd::Restart);
    router.propagate();

    // Workload should have cleared readiness (needs new pod).
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.pod_running);
}

/// 8. Full end-to-end: service activation → demand → pod creation → readiness →
///    service active → worker dies → readiness lost → service back to NeedBackend.
#[test]
fn full_end_to_end() {
    let mut router = Router::new(16);
    let timer = router.create_timer();

    // Infrastructure.
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    // Management ports: one for workload, one per service (different specs).
    let mgmt_wl = router.create_management();
    let mgmt_s1 = router.create_management();
    let mgmt_s2 = router.create_management();

    // Create SMs.
    router.create_workload(W1, WorkloadSm::new(timer));
    router.create_service(S1, ServiceSm::new(timer, false));
    router.create_service(S2, ServiceSm::new(timer, true)); // activation-based

    // Wire management → SMs.
    router.set_management_to_workload_edges(mgmt_wl, vec![W1]);
    router.set_management_wl_spec(mgmt_wl, WorkloadSpec { image: "app:v1".into() });
    router.set_management_to_service_edges(mgmt_s1, vec![S1]);
    router.set_management_svc_spec(
        mgmt_s1,
        ServiceSpec {
            workload: W1,
            has_activation: false,
        },
    );
    router.set_management_to_service_edges(mgmt_s2, vec![S2]);
    router.set_management_svc_spec(
        mgmt_s2,
        ServiceSpec {
            workload: W1,
            has_activation: true,
        },
    );
    router.propagate();

    // S1 (always-on) should have created edges and set demand.
    // Workload has spec + demand → created pod.
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_spec);
    assert!(wl.has_demand); // S1 always-on → demand=true
    let pod_id = wl.pod_id.unwrap();

    // S2 (activation) is idle — no demand yet.
    let s2 = router.get_service(&S2).unwrap();
    assert_eq!(s2.state, ServiceState::Idle);

    // Wire worker to pod and start it.
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Running);
    router.propagate();

    // S1 should be active (always-on, backend ready).
    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));

    // S2 should still be Idle (has_activation=true, no activation event sent).
    let s2 = router.get_service(&S2).unwrap();
    assert_eq!(s2.state, ServiceState::Idle);

    // Activate S2 via event.
    router.send_activate_service(mgmt_s2, S2, true);
    router.propagate();

    // S2 should now be in NeedBackend (demand set) or Active (readiness already available).
    // Since workload already has readiness and S2 is now connected via demand,
    // the workload will re-aggregate demand (2 services now) and re-target readiness edges.
    let s2 = router.get_service(&S2).unwrap();
    // S2 transitions Idle→NeedBackend on activation. Then it receives readiness
    // from the workload (which already has a running pod), so NeedBackend→Active.
    assert!(
        matches!(s2.state, ServiceState::Active { .. })
            || matches!(s2.state, ServiceState::NeedBackend)
    );

    // No timers requested while running normally.
    assert_no_timers_wanted(&mut router, timer);

    // Worker dies.
    router.destroy_worker(worker);
    router.propagate();

    // Pod failed — workload released it and entered backoff.
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.pod_running);
    assert!(wl.in_backoff);

    // Timer signal: workload wants a retry backoff timer.
    assert_timer_requested(&mut router, timer, &[TimerRequest {
        key: WorkloadTimerKey::RetryBackoff,
        generation: 1,
    }]);

    // S1 goes back to NeedBackend.
    let s1 = router.get_service(&S1).unwrap();
    assert_eq!(s1.state, ServiceState::NeedBackend);

    // Workload wants a pod but is in backoff — not creating one yet.
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.wants_pod); // backoff suppresses want_pod
    assert!(wl.has_demand); // demand is still there
}

/// 9. Workload creates pod directly from handler when it has spec + demand.
#[test]
fn handler_driven_pod_creation() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();

    router.create_workload(W1, WorkloadSm::new(timer));
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v1".into() });

    // Always-on service
    router.create_service(S1, ServiceSm::new(timer, false));
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: false,
        },
    );
    router.propagate();

    // Workload has spec + demand → created pod in reconcile().
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.wants_pod);
    assert!(wl.has_spec);
    assert!(wl.has_demand);
    let pod_id = wl.pod_id.unwrap();

    // Pod should exist and know its owner workload (via OwnerInput).
    let pod = router.get_pod(&pod_id).unwrap();
    assert_eq!(pod.workload_id, Some(W1));
    assert_eq!(pod.status, PodStatus::Pending);

    // Wire worker and start pod.
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Running);
    router.propagate();

    // Service is active.
    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));

    let pod = router.get_pod(&pod_id).unwrap();
    assert_eq!(pod.status, PodStatus::Running);
}

/// 10. Handler creates SM with auto-ID and the ID counter is properly
///     shared between handler-created and router-created SMs.
#[test]
fn handler_and_router_share_id_counter() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let mgmt = router.create_management();

    // Create first pod via router.
    let p1 = router.create_pod(PodSm::new(timer));

    // Create workload and wire it.
    router.create_workload(W1, WorkloadSm::new(timer));
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "test".into() });

    // Create service to give workload demand → workload creates pod in handler.
    router.create_service(S1, ServiceSm::new(timer, false));
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: false,
        },
    );
    router.propagate();

    // Workload created a pod (p2) in its handler.
    let p2 = router.get_workload(&W1).unwrap().pod_id.unwrap();
    // p2 should have a different ID from p1.
    assert_ne!(p1, p2);
    // p2's ID should be after p1's (both use the same counter).
    assert!(p2.0 > p1.0);

    // Create another pod via router — should continue the counter.
    let p3 = router.create_pod(PodSm::new(timer));
    assert!(p3.0 > p2.0);
}
