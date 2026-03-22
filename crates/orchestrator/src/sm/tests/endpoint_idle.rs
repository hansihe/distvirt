use super::super::endpoint::{EndpointState, EndpointTimerKey};
use super::*;

// Grounding tests for EndpointSm activation/idle lifecycle.
// These complement the stateright model by verifying key scenarios
// through the full router.

/// Helper: set up a router with one worker, one workload, one service
/// (has_activation=true), propagate, and return (router, mgmt, ep_id).
fn setup_activation_endpoint() -> (Router, ManagementId, EndpointId) {
    let mut router = Router::new(16);
    router.create_timer(TIMER);
    let worker = WK1;
    router.create_worker(worker);
    router.set_worker_info(worker, WorkerInfo { capacity: 10, ..Default::default() });
    router.create_schedule_request(SCHEDULE_REQUEST);

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new());
    router.set_workload_config_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            pod_spec: PodSpec { image: "app:v1".into(), ..Default::default() },
            config: WorkloadConfig { respects_demand: true, ..Default::default() },
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
    (router, mgmt, ep_id)
}

/// Helper: make the pod running for the workload. Returns pod_id.
fn make_pod_running(router: &mut Router) -> PodId {
    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    router.set_worker_assignment_edges(WK1, vec![pod_id]);
    router.send_notify_pod_status(WK1, pod_id, PodStatus::Running);
    router.propagate();
    pod_id
}

/// Active level activates endpoint: idle endpoint receives active level
/// high → endpoint activates → demand → pod boots.
#[test]
fn active_level_activates_endpoint() {
    let (mut router, _mgmt, ep_id) = setup_activation_endpoint();

    // Endpoint is idle, no demand.
    let ep = router.get_endpoint(&ep_id).unwrap();
    assert_eq!(ep.state, EndpointState::Idle);
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.has_demand);

    // Active level goes high → endpoint activates.
    let bn = router.create_endpoint_demand();
    router.set_endpoint_port_demand_edges(bn, vec![ep_id]);
    router.set_endpoint_demand_active(bn, true);
    router.propagate();

    // Endpoint: Idle → NeedBackend, demand=true.
    let ep = router.get_endpoint(&ep_id).unwrap();
    assert_eq!(ep.state, EndpointState::NeedBackend);
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_demand);
    assert!(wl.pod_id.is_some());

    // Make pod running.
    make_pod_running(&mut router);

    // Endpoint should be Active.
    let ep = router.get_endpoint(&ep_id).unwrap();
    assert!(matches!(ep.state, EndpointState::Active { .. }));
    // No idle timer — active level is sustained.
    assert!(!ep.idle_timer_active);
}

/// Traffic event activates endpoint: idle endpoint receives traffic event
/// → idle timer starts → demand high → pod boots → Active with timer running.
#[test]
fn traffic_event_activates_endpoint() {
    let (mut router, _mgmt, ep_id) = setup_activation_endpoint();

    let ep = router.get_endpoint(&ep_id).unwrap();
    assert_eq!(ep.state, EndpointState::Idle);

    // Create demand port and send traffic event.
    let bn = router.create_endpoint_demand();
    router.set_endpoint_port_demand_edges(bn, vec![ep_id]);
    router.send_endpoint_demand_traffic(bn, ep_id, ());
    router.propagate();

    // Traffic event starts idle timer → demand=true → NeedBackend.
    let ep = router.get_endpoint(&ep_id).unwrap();
    assert_eq!(ep.state, EndpointState::NeedBackend);
    assert!(ep.idle_timer_active);
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_demand);

    // Make pod running.
    make_pod_running(&mut router);

    // Active with idle timer still running (impulse-driven).
    let ep = router.get_endpoint(&ep_id).unwrap();
    assert!(matches!(ep.state, EndpointState::Active { .. }));
    assert!(ep.idle_timer_active);
}

/// Traffic event idle timeout cycle: traffic event → active → timer fires
/// → demand drops → workload kills pod → idle.
#[test]
fn traffic_event_idle_timeout_deactivates() {
    let (mut router, _mgmt, ep_id) = setup_activation_endpoint();

    // Traffic event activates.
    let bn = router.create_endpoint_demand();
    router.set_endpoint_port_demand_edges(bn, vec![ep_id]);
    router.send_endpoint_demand_traffic(bn, ep_id, ());
    router.propagate();

    make_pod_running(&mut router);

    let ep = router.get_endpoint(&ep_id).unwrap();
    assert!(matches!(ep.state, EndpointState::Active { .. }));
    assert!(ep.idle_timer_active);
    assert_eq!(ep.idle_generation, 1);

    // Fire the idle timer → demand drops → workload tears down → idle.
    router.send_endpoint_timer_fired(TIMER, ep_id, EndpointTimerKey::IdleTimeout);
    router.propagate();

    let ep = router.get_endpoint(&ep_id).unwrap();
    assert_eq!(ep.state, EndpointState::Idle);
    assert!(!ep.idle_timer_active);

    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.has_demand);
    assert!(wl.pod_id.is_none());
}

/// Active level cancels idle timer: traffic event starts timer, active level
/// high cancels it, demand sustained by active level.
#[test]
fn active_level_cancels_idle_timer() {
    let (mut router, _mgmt, ep_id) = setup_activation_endpoint();

    // Traffic event → timer starts.
    let bn = router.create_endpoint_demand();
    router.set_endpoint_port_demand_edges(bn, vec![ep_id]);
    router.send_endpoint_demand_traffic(bn, ep_id, ());
    router.propagate();

    make_pod_running(&mut router);

    let ep = router.get_endpoint(&ep_id).unwrap();
    assert!(ep.idle_timer_active);

    // Active level high → timer cancelled.
    router.set_endpoint_demand_active(bn, true);
    router.propagate();

    let ep = router.get_endpoint(&ep_id).unwrap();
    assert!(!ep.idle_timer_active);
    assert!(matches!(ep.state, EndpointState::Active { .. }));

    // Demand still present (sustained by active level).
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_demand);
}

/// Traffic event sustains demand after active level drops: active level on,
/// traffic event arrives, active level drops → timer sustains demand.
#[test]
fn traffic_sustains_demand_after_active_level_drops() {
    let (mut router, _mgmt, ep_id) = setup_activation_endpoint();

    // Active level on → demand.
    let bn = router.create_endpoint_demand();
    router.set_endpoint_port_demand_edges(bn, vec![ep_id]);
    router.set_endpoint_demand_active(bn, true);
    router.propagate();

    make_pod_running(&mut router);

    let ep = router.get_endpoint(&ep_id).unwrap();
    assert!(matches!(ep.state, EndpointState::Active { .. }));
    assert!(!ep.idle_timer_active);

    // Traffic event while active level is high → timer starts.
    // Both active_level=true and idle_timer=true can coexist; they're
    // independent inputs. Demand is high from either.
    router.send_endpoint_demand_traffic(bn, ep_id, ());
    router.propagate();

    let ep = router.get_endpoint(&ep_id).unwrap();
    assert!(matches!(ep.state, EndpointState::Active { .. }));
    assert!(ep.idle_timer_active);

    // Active level drops → timer sustains demand.
    router.set_endpoint_demand_active(bn, false);
    router.propagate();

    let ep = router.get_endpoint(&ep_id).unwrap();
    assert!(matches!(ep.state, EndpointState::Active { .. }));
    assert!(ep.idle_timer_active);
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_demand);

    // Timer fires → demand drops → deactivates.
    router.send_endpoint_timer_fired(TIMER, ep_id, EndpointTimerKey::IdleTimeout);
    router.propagate();

    let ep = router.get_endpoint(&ep_id).unwrap();
    assert_eq!(ep.state, EndpointState::Idle);
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.has_demand);
}
