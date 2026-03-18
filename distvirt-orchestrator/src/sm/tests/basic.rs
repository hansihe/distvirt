use super::*;
use super::super::endpoint::EndpointState;

/// 1. Demand aggregation: 3 activation-based services → 1 workload, toggle demand.
#[test]
fn demand_aggregation() {
    let mut router = Router::new(16);
    router.create_timer(TIMER);
    router.create_schedule_request(SCHEDULE_REQUEST);
    router.create_workload(W1, WorkloadSm::new());
    router.create_service(S1, ServiceSm::new());
    router.create_service(S2, ServiceSm::new());
    router.create_service(S3, ServiceSm::new());

    // Deliver specs through management port — services get edges to W1.
    let mgmt = router.create_management();
    router.set_service_config_edges(mgmt, vec![S1, S2, S3]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: true,
            ..Default::default()
        },
    );
    router.propagate();

    // No demand yet (all activation-based, none activated).
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.has_demand);

    // Create demand ports for S1 and S2.
    let ep1 = router.get_service(&S1).unwrap().endpoint_id.unwrap();
    let ep2 = router.get_service(&S2).unwrap().endpoint_id.unwrap();
    let demand1 = router.create_endpoint_demand();
    router.set_endpoint_port_demand_edges(demand1, vec![ep1]);
    let demand2 = router.create_endpoint_demand();
    router.set_endpoint_port_demand_edges(demand2, vec![ep2]);

    // S1 activates.
    router.set_endpoint_demand_active(demand1, true);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_demand);

    // S2 also activates.
    router.set_endpoint_demand_active(demand2, true);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_demand);

    // Both deactivate.
    router.set_endpoint_demand_active(demand1, false);
    router.set_endpoint_demand_active(demand2, false);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.has_demand);
}

/// 2. Reactive readiness edges: workload creates WorkloadToService edges
///    based on which services point at it, then readiness propagates back.
#[test]
fn reactive_readiness_edges() {
    let mut router = Router::new(16);
    router.create_timer(TIMER);
    let worker = WK1;
    router.create_worker(worker);
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new());
    router.create_service(S1, ServiceSm::new()); // always-on
    router.create_service(S2, ServiceSm::new());

    // Deliver workload spec.
    router.set_workload_config_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "test:latest".into(),
            ..Default::default()
        },
    );

    // Deliver service specs — always-on services auto-set demand + edges.
    router.set_service_config_edges(mgmt, vec![S1, S2]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: false,
            ..Default::default()
        },
    );
    router.propagate();

    // Workload should have received demand (from always-on services).
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_demand);

    // Both services should be in NeedBackend (demand set, no readiness yet).
    let ep_id = router.get_service(&S1).unwrap().endpoint_id.unwrap();
    let ep = router.get_endpoint(&ep_id).unwrap();
    assert_eq!(ep.state, EndpointState::NeedBackend);

    // Workload created a pod in reconcile(). Wire worker and make it running.
    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    router.set_worker_assignment_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Running);
    router.propagate();

    // Both services should be active now (readiness propagated via reactive edges).
    let ep_id = router.get_service(&S1).unwrap().endpoint_id.unwrap();
    let ep = router.get_endpoint(&ep_id).unwrap();
    assert!(matches!(ep.state, EndpointState::Active { .. }));
    let ep_id = router.get_service(&S2).unwrap().endpoint_id.unwrap();
    let ep = router.get_endpoint(&ep_id).unwrap();
    assert!(matches!(ep.state, EndpointState::Active { .. }));

    // Add a third service — it should immediately get readiness.
    router.create_service(S3, ServiceSm::new());

    // Use same mgmt port, update edges to include S3.
    router.set_service_config_edges(mgmt, vec![S1, S2, S3]);
    router.propagate();

    // S3 got its spec, set demand + edges, workload re-aggregated,
    // readiness propagated to all three services including S3.
    let ep_id = router.get_service(&S3).unwrap().endpoint_id.unwrap();
    let ep = router.get_endpoint(&ep_id).unwrap();
    assert!(matches!(ep.state, EndpointState::Active { .. }));
}

/// 3. Pod lifecycle through signals: workload creates pod in handler,
///    pod status flows back, readiness propagates to services.
#[test]
fn pod_lifecycle() {
    let mut router = Router::new(16);
    router.create_timer(TIMER);
    let worker = WK1;
    router.create_worker(worker);
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new());
    router.set_workload_config_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "test:latest".into(),
            ..Default::default()
        },
    );
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_spec);
    assert!(!wl.has_demand);

    // Add an always-on service with demand.
    router.create_service(S1, ServiceSm::new());
    router.set_service_config_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: false,
            ..Default::default()
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
    router.set_worker_assignment_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Running);
    router.propagate();

    // Workload should be ready now.
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);

    // Readiness should have propagated to S1.
    let ep_id = router.get_service(&S1).unwrap().endpoint_id.unwrap();
    let ep = router.get_endpoint(&ep_id).unwrap();
    assert!(matches!(ep.state, EndpointState::Active { .. }));
}

/// 4. Worker port removal: worker dies, pod sees empty WorkerInput,
///    status goes to Failed, workload sees readiness lost.
#[test]
fn worker_loss_via_port_removal() {
    let mut router = Router::new(16);
    router.create_timer(TIMER);
    let worker = WK1;
    router.create_worker(worker);
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new());
    router.set_workload_config_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v1".into(),
            ..Default::default()
        },
    );

    router.create_service(S1, ServiceSm::new());
    router.set_service_config_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: false,
            ..Default::default()
        },
    );
    router.propagate();

    // Workload created pod. Wire worker and start it.
    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    router.set_worker_assignment_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Running);
    router.propagate();

    // Verify everything is active.
    let ep_id = router.get_service(&S1).unwrap().endpoint_id.unwrap();
    let ep = router.get_endpoint(&ep_id).unwrap();
    assert!(matches!(ep.state, EndpointState::Active { .. }));

    // No timers wanted while running.
    assert_no_timers_wanted(&mut router);

    // Worker dies — remove the port.
    router.destroy_worker(worker);
    router.propagate();

    // Pod was displaced (infrastructure loss) — workload immediately reschedules.
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.pod_running);
    assert!(!wl.in_backoff); // no backoff for infrastructure loss
    assert_eq!(wl.consecutive_failures, 0); // not counted as failure
    assert!(wl.wants_pod); // immediately wants a new pod

    // No retry timer needed — immediate rescheduling.
    assert_no_timers_wanted(&mut router);

    // Service should be back to NeedBackend.
    let ep_id = router.get_service(&S1).unwrap().endpoint_id.unwrap();
    let ep = router.get_endpoint(&ep_id).unwrap();
    assert_eq!(ep.state, EndpointState::NeedBackend);
}

/// 5. Spec delivery via management port: init and update use same path.
#[test]
fn spec_via_management_port() {
    let mut router = Router::new(16);
    router.create_timer(TIMER);
    router.create_schedule_request(SCHEDULE_REQUEST);
    router.create_workload(W1, WorkloadSm::new());

    let mgmt = router.create_management();
    router.set_workload_config_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "v1".into(),
            ..Default::default()
        },
    );
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_spec);

    // Update spec.
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "v2".into(),
            ..Default::default()
        },
    );
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
    router.create_timer(TIMER);
    router.create_schedule_request(SCHEDULE_REQUEST);
    router.create_workload(W1, WorkloadSm::new());
    router.create_service(S1, ServiceSm::new());

    // Management port delivers service spec that points at W1.
    let mgmt = router.create_management();
    router.set_service_config_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: false,
            ..Default::default()
        },
    );
    router.propagate();

    // Service should have reactively created edges and set demand=true.
    // Verify indirectly: workload received demand via the edge.
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_demand);

    // Service should be in NeedBackend (always-on with demand set).
    let ep_id = router.get_service(&S1).unwrap().endpoint_id.unwrap();
    let ep = router.get_endpoint(&ep_id).unwrap();
    assert_eq!(ep.state, EndpointState::NeedBackend);
}

/// 7. Admin command event: management port sends restart to workload.
#[test]
fn admin_restart_event() {
    let mut router = Router::new(16);
    router.create_timer(TIMER);
    let worker = WK1;
    router.create_worker(worker);
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new());
    router.set_workload_config_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v1".into(),
            ..Default::default()
        },
    );

    router.create_service(S1, ServiceSm::new());
    router.set_service_config_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: false,
            ..Default::default()
        },
    );
    router.propagate();

    // Workload created pod. Wire worker and start it.
    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    router.set_worker_assignment_edges(worker, vec![pod_id]);
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
    router.create_timer(TIMER);

    // Infrastructure.
    let worker = WK1;
    router.create_worker(worker);
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);

    // Management ports: one for workload, one per service (different specs).
    let mgmt_wl = router.create_management();
    let mgmt_s1 = router.create_management();
    let mgmt_s2 = router.create_management();

    // Create SMs.
    router.create_workload(W1, WorkloadSm::new());
    router.create_service(S1, ServiceSm::new());
    router.create_service(S2, ServiceSm::new()); // activation-based

    // Wire management → SMs.
    router.set_workload_config_edges(mgmt_wl, vec![W1]);
    router.set_management_wl_spec(
        mgmt_wl,
        WorkloadSpec {
            image: "app:v1".into(),
            ..Default::default()
        },
    );
    router.set_service_config_edges(mgmt_s1, vec![S1]);
    router.set_management_svc_spec(
        mgmt_s1,
        ServiceSpec {
            workload: W1,
            has_activation: false,
            ..Default::default()
        },
    );
    router.set_service_config_edges(mgmt_s2, vec![S2]);
    router.set_management_svc_spec(
        mgmt_s2,
        ServiceSpec {
            workload: W1,
            has_activation: true,
            ..Default::default()
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
    let ep_id = router.get_service(&S2).unwrap().endpoint_id.unwrap();
    let ep = router.get_endpoint(&ep_id).unwrap();
    assert_eq!(ep.state, EndpointState::Idle);

    // Wire worker to pod and start it.
    router.set_worker_assignment_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Running);
    router.propagate();

    // S1 should be active (always-on, backend ready).
    let ep_id = router.get_service(&S1).unwrap().endpoint_id.unwrap();
    let ep = router.get_endpoint(&ep_id).unwrap();
    assert!(matches!(ep.state, EndpointState::Active { .. }));

    // S2 receives readiness from the workload (backend ready), even without demand.
    let ep_id = router.get_service(&S2).unwrap().endpoint_id.unwrap();
    let ep = router.get_endpoint(&ep_id).unwrap();
    assert!(matches!(ep.state, EndpointState::Active { .. }));

    // Activate S2 via EndpointDemand port.
    let s2_ep_id = router.get_service(&S2).unwrap().endpoint_id.unwrap();
    let demand_s2 = router.create_endpoint_demand();
    router.set_endpoint_port_demand_edges(demand_s2, vec![s2_ep_id]);
    router.set_endpoint_demand_active(demand_s2, true);
    router.propagate();

    // S2 should now be in NeedBackend (demand set) or Active (readiness already available).
    // Since workload already has readiness and S2 is now connected via demand,
    // the workload will re-aggregate demand (2 services now) and re-target readiness edges.
    let ep_id = router.get_service(&S2).unwrap().endpoint_id.unwrap();
    let ep = router.get_endpoint(&ep_id).unwrap();
    // S2 transitions Idle→NeedBackend on activation. Then it receives readiness
    // from the workload (which already has a running pod), so NeedBackend→Active.
    assert!(
        matches!(ep.state, EndpointState::Active { .. })
            || matches!(ep.state, EndpointState::NeedBackend)
    );

    // No timers requested while running normally.
    assert_no_timers_wanted(&mut router);

    // Worker dies.
    router.destroy_worker(worker);
    router.propagate();

    // Pod displaced — workload immediately reschedules (no backoff).
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.pod_running);
    assert!(!wl.in_backoff); // no backoff for infrastructure loss
    assert_eq!(wl.consecutive_failures, 0);
    assert!(wl.wants_pod); // immediately wants a new pod
    assert!(wl.has_demand); // demand is still there

    // No retry timer needed.
    assert_no_timers_wanted(&mut router);

    // S1 goes back to NeedBackend.
    let ep_id = router.get_service(&S1).unwrap().endpoint_id.unwrap();
    let ep = router.get_endpoint(&ep_id).unwrap();
    assert_eq!(ep.state, EndpointState::NeedBackend);
}

/// 9. Workload creates pod directly from handler when it has spec + demand.
#[test]
fn handler_driven_pod_creation() {
    let mut router = Router::new(16);
    router.create_timer(TIMER);
    let worker = WK1;
    router.create_worker(worker);
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);

    let mgmt = router.create_management();

    router.create_workload(W1, WorkloadSm::new());
    router.set_workload_config_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v1".into(),
            ..Default::default()
        },
    );

    // Always-on service
    router.create_service(S1, ServiceSm::new());
    router.set_service_config_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: false,
            ..Default::default()
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
    router.set_worker_assignment_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Running);
    router.propagate();

    // Service is active.
    let ep_id = router.get_service(&S1).unwrap().endpoint_id.unwrap();
    let ep = router.get_endpoint(&ep_id).unwrap();
    assert!(matches!(ep.state, EndpointState::Active { .. }));

    let pod = router.get_pod(&pod_id).unwrap();
    assert_eq!(pod.status, PodStatus::Running);
}

/// 10. Handler creates SM with auto-ID and the ID counter is properly
///     shared between handler-created and router-created SMs.
#[test]
fn handler_and_router_share_id_counter() {
    let mut router = Router::new(16);
    router.create_timer(TIMER);
    router.create_schedule_request(SCHEDULE_REQUEST);
    let mgmt = router.create_management();

    // Create first pod via router.
    let p1 = router.create_pod(PodSm::new());

    // Create workload and wire it.
    router.create_workload(W1, WorkloadSm::new());
    router.set_workload_config_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "test".into(),
            ..Default::default()
        },
    );

    // Create service to give workload demand → workload creates pod in handler.
    router.create_service(S1, ServiceSm::new());
    router.set_service_config_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: false,
            ..Default::default()
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
    let p3 = router.create_pod(PodSm::new());
    assert!(p3.0 > p2.0);
}
