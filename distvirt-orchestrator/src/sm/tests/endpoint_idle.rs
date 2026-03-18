use super::*;
use super::super::endpoint::{EndpointState, EndpointTimerKey};

// Grounding tests for EndpointSm activation/idle lifecycle.
// These complement the stateright model by verifying key scenarios
// through the full router.

/// Traffic-triggered activation: idle endpoint receives BackendNeed(Traffic)
/// → endpoint activates → demand → pod boots.
#[test]
fn traffic_activates_endpoint() {
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
            has_activation: true,
            ..Default::default()
        },
    );
    router.propagate();

    // Endpoint is idle, no demand.
    let ep_id = router.get_service(&S1).unwrap().endpoint_id.unwrap();
    let ep = router.get_endpoint(&ep_id).unwrap();
    assert_eq!(ep.state, EndpointState::Idle);
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.has_demand);

    // BackendNeed port reports traffic → endpoint activates.
    let bn = router.create_backend_need();
    router.set_traffic_demand_edges(bn, vec![ep_id]);
    router.set_backend_need_level(bn, BackendNeed::Traffic);
    router.propagate();

    // Endpoint: Idle → NeedBackend, demand=true.
    let ep = router.get_endpoint(&ep_id).unwrap();
    assert_eq!(ep.state, EndpointState::NeedBackend);
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_demand);
    assert!(wl.pod_id.is_some());

    // Make pod running.
    let pod_id = wl.pod_id.unwrap();
    router.set_worker_assignment_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Running);
    router.propagate();

    // Endpoint should be Active.
    let ep = router.get_endpoint(&ep_id).unwrap();
    assert!(matches!(ep.state, EndpointState::Active { .. }));
}

/// Idle timeout deactivation: running endpoint, traffic stops → idle timer
/// fires → endpoint deactivates → demand drops → workload destroys pod.
#[test]
fn idle_timeout_deactivates_endpoint() {
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
            has_activation: true,
            ..Default::default()
        },
    );
    router.propagate();

    let ep_id = router.get_service(&S1).unwrap().endpoint_id.unwrap();

    // Traffic activates the endpoint.
    let bn = router.create_backend_need();
    router.set_traffic_demand_edges(bn, vec![ep_id]);
    router.set_backend_need_level(bn, BackendNeed::Traffic);
    router.propagate();

    // Make pod running.
    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    router.set_worker_assignment_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Running);
    router.propagate();

    let ep = router.get_endpoint(&ep_id).unwrap();
    assert!(matches!(ep.state, EndpointState::Active { .. }));
    assert!(!ep.idle_timer_active);

    // Traffic stops → idle timer starts.
    router.set_backend_need_level(bn, BackendNeed::None);
    router.propagate();

    let ep = router.get_endpoint(&ep_id).unwrap();
    assert!(matches!(ep.state, EndpointState::Active { .. })); // still active
    assert!(ep.idle_timer_active);
    assert_eq!(ep.idle_generation, 1);

    // Fire the idle timer → endpoint deactivates.
    router.send_endpoint_timer_fired(TIMER, ep_id, EndpointTimerKey::IdleTimeout);
    router.propagate();

    let ep = router.get_endpoint(&ep_id).unwrap();
    assert_eq!(ep.state, EndpointState::Idle);
    assert!(!ep.idle_timer_active);

    // Demand should be gone → workload destroyed the pod.
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.has_demand);
    assert!(wl.pod_id.is_none());
}

/// Traffic cancels idle timer: idle timer running, new traffic arrives →
/// timer cancelled, endpoint stays active.
#[test]
fn traffic_cancels_endpoint_idle_timer() {
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
            has_activation: true,
            ..Default::default()
        },
    );
    router.propagate();

    let ep_id = router.get_service(&S1).unwrap().endpoint_id.unwrap();

    // Traffic → activate → pod running.
    let bn = router.create_backend_need();
    router.set_traffic_demand_edges(bn, vec![ep_id]);
    router.set_backend_need_level(bn, BackendNeed::Traffic);
    router.propagate();

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    router.set_worker_assignment_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Running);
    router.propagate();

    // Traffic stops → idle timer starts.
    router.set_backend_need_level(bn, BackendNeed::None);
    router.propagate();

    let ep = router.get_endpoint(&ep_id).unwrap();
    assert!(ep.idle_timer_active);

    // Traffic returns → idle timer cancelled.
    router.set_backend_need_level(bn, BackendNeed::Traffic);
    router.propagate();

    let ep = router.get_endpoint(&ep_id).unwrap();
    assert!(!ep.idle_timer_active);
    assert!(matches!(ep.state, EndpointState::Active { .. }));

    // Demand still present.
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_demand);
    assert!(wl.pod_running);
}
