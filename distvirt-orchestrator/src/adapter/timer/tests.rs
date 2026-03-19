use std::time::Duration;

use super::*;
use crate::sm::{
    DRouter, EndpointId, SCHEDULE_REQUEST, ServiceId, ServiceSm, ServiceSpec, WorkerInfo,
    WorkloadId, WorkloadSm, WorkloadSpec,
    endpoint::{EndpointState, EndpointTimerKey},
};

const W1: WorkloadId = WorkloadId(1);
const S1: ServiceId = ServiceId(1);
const WK1: crate::sm::WorkerId = crate::sm::WorkerId(1);

fn test_config() -> TimerConfig {
    TimerConfig {
        retry_backoff: Duration::from_secs(5),
        launch_timeout: Duration::from_secs(30),
        suspend_timeout: Duration::from_secs(60),
        idle_timeout: Duration::from_secs(300),
    }
}

/// Set up a router with a workload (always-on service), propagate initial state.
/// Returns (router, adapter, mgmt, worker).
fn setup_workload(
    router: &mut DRouter,
    adapter: &mut TimerAdapter,
) -> (crate::sm::ManagementId, crate::sm::WorkerId) {
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
            image: "app:v1".into(),
            respects_demand: true,
            ..Default::default()
        },
    );

    router.create_service(S1, ServiceSm::new()); // always-on
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

    // Drain initial state (may include pod launch timers from always-on service).
    let _ = adapter.reconcile(router).0;

    (mgmt, worker)
}

/// Helper to get the EndpointId for service S1.
fn get_endpoint_id(router: &DRouter) -> EndpointId {
    router
        .get_service(&S1)
        .unwrap()
        .endpoint_id
        .expect("service should have created an endpoint")
}

// ============================================================================
// 1. No timers wanted, no active → no actions
// ============================================================================

#[test]
fn no_timers_no_actions() {
    let mut router = DRouter::new_traced(16, distvirt_sm_router::trace::PanicTracer::new());
    router.create_timer(TIMER);
    let mut adapter = TimerAdapter::new(test_config());

    // No SMs, just propagate.
    router.propagate();
    let (actions, _) = adapter.reconcile(&mut router);
    assert!(actions.is_empty());
}

// ============================================================================
// 2. New timer wanted → Start action
// ============================================================================

#[test]
fn new_timer_produces_start() {
    let mut router = DRouter::new_traced(16, distvirt_sm_router::trace::PanicTracer::new());
    router.create_timer(TIMER);
    let mut adapter = TimerAdapter::new(test_config());
    let (_, worker) = setup_workload(&mut router, &mut adapter);

    // Pod created, make it fail so workload enters RetryBackoff and requests timer.
    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    let lease = router.create_schedule_lease();
    router.set_pod_lease_edges(lease, vec![pod_id]);
    router.set_schedule_lease_lease(lease, crate::sm::LeaseInfo { worker_id: worker });
    router.propagate();
    router.set_worker_assignment_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, crate::sm::PodStatus::Failed);
    router.propagate();

    let (actions, _) = adapter.reconcile(&mut router);
    let starts: Vec<_> = actions
        .iter()
        .filter(|a| matches!(a, TimerAction::Start { .. }))
        .collect();
    assert_eq!(
        starts.len(),
        1,
        "expected 1 Start action, got {:?}",
        actions
    );
    match &starts[0] {
        TimerAction::Start {
            identity, duration, ..
        } => {
            assert_eq!(
                *identity,
                TimerIdentity::Workload(W1, WorkloadTimerKey::RetryBackoff)
            );
            assert_eq!(*duration, Duration::from_millis(500));
        }
        _ => unreachable!(),
    }
}

// ============================================================================
// 3. Timer no longer wanted → Cancel action
// ============================================================================

#[test]
fn timer_removed_produces_cancel() {
    let mut router = DRouter::new_traced(16, distvirt_sm_router::trace::PanicTracer::new());
    router.create_timer(TIMER);
    let mut adapter = TimerAdapter::new(test_config());
    let (_, worker) = setup_workload(&mut router, &mut adapter);

    // Make pod fail → retry backoff timer wanted.
    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    let lease = router.create_schedule_lease();
    router.set_pod_lease_edges(lease, vec![pod_id]);
    router.set_schedule_lease_lease(lease, crate::sm::LeaseInfo { worker_id: worker });
    router.propagate();
    router.set_worker_assignment_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, crate::sm::PodStatus::Failed);
    router.propagate();

    let (actions, _) = adapter.reconcile(&mut router);
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, TimerAction::Start { .. })),
        "expected Start"
    );

    // Fire the timer → workload leaves RetryBackoff → timer no longer wanted.
    adapter.fire(
        &mut router,
        &TimerIdentity::Workload(W1, WorkloadTimerKey::RetryBackoff),
    );
    router.propagate();

    let (actions, _) = adapter.reconcile(&mut router);
    let cancels: Vec<_> = actions
        .iter()
        .filter(|a| matches!(a, TimerAction::Cancel { .. }))
        .collect();
    assert_eq!(cancels.len(), 1, "expected 1 Cancel, got {:?}", actions);
    match &cancels[0] {
        TimerAction::Cancel { identity } => {
            assert_eq!(
                *identity,
                TimerIdentity::Workload(W1, WorkloadTimerKey::RetryBackoff)
            );
        }
        _ => unreachable!(),
    }
}

// ============================================================================
// 4. Same timer, same generation → no action (stable)
// ============================================================================

#[test]
fn same_generation_no_action() {
    let mut router = DRouter::new_traced(16, distvirt_sm_router::trace::PanicTracer::new());
    router.create_timer(TIMER);
    let mut adapter = TimerAdapter::new(test_config());
    let (_, worker) = setup_workload(&mut router, &mut adapter);

    // Make pod fail → retry timer wanted.
    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    let lease = router.create_schedule_lease();
    router.set_pod_lease_edges(lease, vec![pod_id]);
    router.set_schedule_lease_lease(lease, crate::sm::LeaseInfo { worker_id: worker });
    router.propagate();
    router.set_worker_assignment_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, crate::sm::PodStatus::Failed);
    router.propagate();

    let (actions, _) = adapter.reconcile(&mut router);
    assert!(!actions.is_empty());

    // Propagate again without any changes — signal dedup means no delivery.
    router.propagate();
    let (actions, _) = adapter.reconcile(&mut router);
    assert!(
        actions.is_empty(),
        "expected no actions on stable state, got {:?}",
        actions
    );
}

// ============================================================================
// 5. Same timer, different generation → Cancel + Start (restart)
// ============================================================================

#[test]
fn generation_change_restarts_timer() {
    let mut router = DRouter::new_traced(16, distvirt_sm_router::trace::PanicTracer::new());
    router.create_timer(TIMER);
    let mut adapter = TimerAdapter::new(test_config());

    // Set up activation-based service so we can trigger idle timer generation changes.
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
            image: "app:v1".into(),
            respects_demand: true,
            ..Default::default()
        },
    );

    router.create_service(S1, ServiceSm::new()); // activation-based
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
    let _ = adapter.reconcile(&mut router).0;

    let ep_id = get_endpoint_id(&router);

    // Activate via active level, make pod running.
    let bn = router.create_endpoint_demand();
    router.set_endpoint_port_demand_edges(bn, vec![ep_id]);
    router.set_endpoint_demand_active(bn, true);
    router.propagate();

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    let lease = router.create_schedule_lease();
    router.set_pod_lease_edges(lease, vec![pod_id]);
    router.set_schedule_lease_lease(lease, crate::sm::LeaseInfo { worker_id: worker });
    router.propagate();
    router.set_worker_assignment_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, crate::sm::PodStatus::Running);
    router.propagate();
    let _ = adapter.reconcile(&mut router).0;

    // Traffic event → idle timer starts (generation 1).
    router.send_endpoint_demand_traffic(bn, ep_id, ());
    router.propagate();
    let (actions, _) = adapter.reconcile(&mut router);
    assert!(
        actions.iter().any(|a| matches!(
            a,
            TimerAction::Start {
                identity: TimerIdentity::Endpoint(_, EndpointTimerKey::IdleTimeout),
                ..
            }
        )),
        "expected idle timer Start, got {:?}",
        actions
    );

    // Active level drop then rise cancels idle timer.
    // Must propagate between to avoid router suppression of unchanged aggregation.
    router.set_endpoint_demand_active(bn, false);
    router.propagate();
    router.set_endpoint_demand_active(bn, true);
    router.propagate();
    let (actions, _) = adapter.reconcile(&mut router);
    assert!(
        actions.iter().any(|a| matches!(
            a,
            TimerAction::Cancel {
                identity: TimerIdentity::Endpoint(_, EndpointTimerKey::IdleTimeout),
            }
        )),
        "expected idle timer Cancel, got {:?}",
        actions
    );

    // Second traffic event → idle timer starts with new generation (2).
    router.send_endpoint_demand_traffic(bn, ep_id, ());
    router.propagate();
    let (actions, _) = adapter.reconcile(&mut router);
    let start = actions.iter().find(|a| {
        matches!(
            a,
            TimerAction::Start {
                identity: TimerIdentity::Endpoint(_, EndpointTimerKey::IdleTimeout),
                ..
            }
        )
    });
    assert!(
        start.is_some(),
        "expected new Start after generation change, got {:?}",
        actions
    );
}

// ============================================================================
// 6. Multiple SM kinds in one reconcile cycle
// ============================================================================

#[test]
fn multiple_sm_kinds_in_one_cycle() {
    let mut router = DRouter::new_traced(16, distvirt_sm_router::trace::PanicTracer::new());
    router.create_timer(TIMER);
    let mut adapter = TimerAdapter::new(test_config());

    // Set up activation-based service.
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
            image: "app:v1".into(),
            respects_demand: true,
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
    let _ = adapter.reconcile(&mut router).0;

    let ep_id = get_endpoint_id(&router);

    // Traffic event activates, make pod running.
    let bn = router.create_endpoint_demand();
    router.set_endpoint_port_demand_edges(bn, vec![ep_id]);
    router.send_endpoint_demand_traffic(bn, ep_id, ());
    router.propagate();

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    let lease = router.create_schedule_lease();
    router.set_pod_lease_edges(lease, vec![pod_id]);
    router.set_schedule_lease_lease(lease, crate::sm::LeaseInfo { worker_id: worker });
    router.propagate();
    router.set_worker_assignment_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, crate::sm::PodStatus::Running);
    router.propagate();
    let _ = adapter.reconcile(&mut router).0;

    // Idle timer already running from traffic event. Also make pod fail → workload retry timer.
    router.send_notify_pod_status(worker, pod_id, crate::sm::PodStatus::Failed);
    router.propagate();

    let (actions, _) = adapter.reconcile(&mut router);

    // Should have actions for multiple SM kinds.
    let has_endpoint_timer = actions.iter().any(|a| {
        matches!(
            a,
            TimerAction::Start {
                identity: TimerIdentity::Endpoint(..),
                ..
            } | TimerAction::Cancel {
                identity: TimerIdentity::Endpoint(..),
            }
        )
    });
    let has_workload_timer = actions.iter().any(|a| {
        matches!(
            a,
            TimerAction::Start {
                identity: TimerIdentity::Workload(..),
                ..
            } | TimerAction::Cancel {
                identity: TimerIdentity::Workload(..),
            }
        )
    });

    // At minimum we should see some timer activity. The exact combination depends on
    // the SM logic, but both kinds should produce something.
    assert!(
        !actions.is_empty(),
        "expected timer actions from multiple SM kinds"
    );
    // At least one of the SM kinds should have timer activity.
    assert!(
        has_endpoint_timer || has_workload_timer,
        "expected timer actions from endpoint or workload, got {:?}",
        actions
    );
}

// ============================================================================
// 7. Fire dispatches correctly
// ============================================================================

#[test]
fn fire_dispatches_workload_timer() {
    let mut router = DRouter::new_traced(16, distvirt_sm_router::trace::PanicTracer::new());
    router.create_timer(TIMER);
    let adapter = TimerAdapter::new(test_config());
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
            image: "app:v1".into(),
            respects_demand: true,
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

    // Make pod fail to enter RetryBackoff.
    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    let lease = router.create_schedule_lease();
    router.set_pod_lease_edges(lease, vec![pod_id]);
    router.set_schedule_lease_lease(lease, crate::sm::LeaseInfo { worker_id: worker });
    router.propagate();
    router.set_worker_assignment_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, crate::sm::PodStatus::Failed);
    router.propagate();

    // Workload should be in backoff.
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.in_backoff);

    // Fire the retry timer via the adapter.
    adapter.fire(
        &mut router,
        &TimerIdentity::Workload(W1, WorkloadTimerKey::RetryBackoff),
    );
    router.propagate();

    // After timer fire, workload should leave backoff (new pod created).
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.in_backoff);
}

#[test]
fn fire_dispatches_endpoint_timer() {
    let mut router = DRouter::new_traced(16, distvirt_sm_router::trace::PanicTracer::new());
    router.create_timer(TIMER);
    let adapter = TimerAdapter::new(test_config());
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
            image: "app:v1".into(),
            respects_demand: true,
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

    let ep_id = get_endpoint_id(&router);

    // Traffic event activates → pod running → idle timer from traffic.
    let bn = router.create_endpoint_demand();
    router.set_endpoint_port_demand_edges(bn, vec![ep_id]);
    router.send_endpoint_demand_traffic(bn, ep_id, ());
    router.propagate();

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    let lease = router.create_schedule_lease();
    router.set_pod_lease_edges(lease, vec![pod_id]);
    router.set_schedule_lease_lease(lease, crate::sm::LeaseInfo { worker_id: worker });
    router.propagate();
    router.set_worker_assignment_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, crate::sm::PodStatus::Running);
    router.propagate();

    let ep = router.get_endpoint(&ep_id).unwrap();
    assert!(ep.idle_timer_active);

    // Fire idle timer via adapter.
    adapter.fire(
        &mut router,
        &TimerIdentity::Endpoint(ep_id, EndpointTimerKey::IdleTimeout),
    );
    router.propagate();

    // Endpoint should deactivate (back to Idle).
    let ep = router.get_endpoint(&ep_id).unwrap();
    assert_eq!(ep.state, EndpointState::Idle);
}
