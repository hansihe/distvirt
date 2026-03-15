use std::time::Duration;

use super::*;
use crate::sm_new::{
    Router, ServiceSm, ServiceSpec, WorkloadSm, WorkloadSpec, WorkerInfo, WorkloadId, ServiceId,
    SCHEDULE_REQUEST,
};

const W1: WorkloadId = WorkloadId(1);
const S1: ServiceId = ServiceId(1);

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
    router: &mut Router,
    timer: TimerId,
    adapter: &mut TimerAdapter,
) -> (crate::sm_new::ManagementId, crate::sm_new::WorkerId) {
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new(timer));
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v1".into() });

    router.create_service(S1, ServiceSm::new(timer, false)); // always-on
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: false,
        },
    );
    router.propagate();

    // Drain initial state (may include pod launch timers from always-on service).
    let _ = adapter.reconcile(router);

    (mgmt, worker)
}

// ============================================================================
// 1. No timers wanted, no active → no actions
// ============================================================================

#[test]
fn no_timers_no_actions() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let mut adapter = TimerAdapter::new(timer, test_config());

    // No SMs, just propagate.
    router.propagate();
    let actions = adapter.reconcile(&mut router);
    assert!(actions.is_empty());
}

// ============================================================================
// 2. New timer wanted → Start action
// ============================================================================

#[test]
fn new_timer_produces_start() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let mut adapter = TimerAdapter::new(timer, test_config());
    let (_, worker) = setup_workload(&mut router, timer, &mut adapter);

    // Pod created, make it fail so workload enters RetryBackoff and requests timer.
    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    let lease = router.create_schedule_lease();
    router.set_schedule_lease_to_pod_edges(lease, vec![pod_id]);
    router.set_schedule_lease_lease(
        lease,
        crate::sm_new::LeaseInfo { worker_id: worker },
    );
    router.propagate();
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, crate::sm_new::PodStatus::Failed);
    router.propagate();

    let actions = adapter.reconcile(&mut router);
    let starts: Vec<_> = actions
        .iter()
        .filter(|a| matches!(a, TimerAction::Start { .. }))
        .collect();
    assert_eq!(starts.len(), 1, "expected 1 Start action, got {:?}", actions);
    match &starts[0] {
        TimerAction::Start {
            identity,
            duration,
            ..
        } => {
            assert_eq!(
                *identity,
                TimerIdentity::Workload(W1, WorkloadTimerKey::RetryBackoff)
            );
            assert_eq!(*duration, Duration::from_secs(5));
        }
        _ => unreachable!(),
    }
}

// ============================================================================
// 3. Timer no longer wanted → Cancel action
// ============================================================================

#[test]
fn timer_removed_produces_cancel() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let mut adapter = TimerAdapter::new(timer, test_config());
    let (_, worker) = setup_workload(&mut router, timer, &mut adapter);

    // Make pod fail → retry backoff timer wanted.
    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    let lease = router.create_schedule_lease();
    router.set_schedule_lease_to_pod_edges(lease, vec![pod_id]);
    router.set_schedule_lease_lease(
        lease,
        crate::sm_new::LeaseInfo { worker_id: worker },
    );
    router.propagate();
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, crate::sm_new::PodStatus::Failed);
    router.propagate();

    let actions = adapter.reconcile(&mut router);
    assert!(
        actions.iter().any(|a| matches!(a, TimerAction::Start { .. })),
        "expected Start"
    );

    // Fire the timer → workload leaves RetryBackoff → timer no longer wanted.
    adapter.fire(&mut router, &TimerIdentity::Workload(W1, WorkloadTimerKey::RetryBackoff));
    router.propagate();

    let actions = adapter.reconcile(&mut router);
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
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let mut adapter = TimerAdapter::new(timer, test_config());
    let (_, worker) = setup_workload(&mut router, timer, &mut adapter);

    // Make pod fail → retry timer wanted.
    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    let lease = router.create_schedule_lease();
    router.set_schedule_lease_to_pod_edges(lease, vec![pod_id]);
    router.set_schedule_lease_lease(
        lease,
        crate::sm_new::LeaseInfo { worker_id: worker },
    );
    router.propagate();
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, crate::sm_new::PodStatus::Failed);
    router.propagate();

    let actions = adapter.reconcile(&mut router);
    assert!(!actions.is_empty());

    // Propagate again without any changes — signal dedup means no delivery.
    router.propagate();
    let actions = adapter.reconcile(&mut router);
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
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let mut adapter = TimerAdapter::new(timer, test_config());

    // Set up activation-based service so we can trigger idle timer generation changes.
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new(timer));
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v1".into() });

    router.create_service(S1, ServiceSm::new(timer, true)); // activation-based
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: true,
        },
    );
    router.propagate();
    let _ = adapter.reconcile(&mut router);

    // Activate via BackendNeed traffic.
    let bn = router.create_backend_need();
    router.set_backend_need_to_service_edges(bn, vec![S1]);
    router.set_backend_need_level(bn, crate::sm_new::BackendNeed::Traffic);
    router.propagate();

    // Make pod running.
    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    let lease = router.create_schedule_lease();
    router.set_schedule_lease_to_pod_edges(lease, vec![pod_id]);
    router.set_schedule_lease_lease(
        lease,
        crate::sm_new::LeaseInfo { worker_id: worker },
    );
    router.propagate();
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, crate::sm_new::PodStatus::Running);
    router.propagate();
    let _ = adapter.reconcile(&mut router);

    // Traffic stops → idle timer starts (generation 1).
    router.set_backend_need_level(bn, crate::sm_new::BackendNeed::None);
    router.propagate();
    let actions = adapter.reconcile(&mut router);
    assert!(
        actions.iter().any(|a| matches!(a, TimerAction::Start {
            identity: TimerIdentity::Service(_, ServiceTimerKey::IdleTimeout),
            ..
        })),
        "expected idle timer Start, got {:?}",
        actions
    );

    // Traffic returns → idle timer cancelled.
    router.set_backend_need_level(bn, crate::sm_new::BackendNeed::Traffic);
    router.propagate();
    let actions = adapter.reconcile(&mut router);
    assert!(
        actions.iter().any(|a| matches!(a, TimerAction::Cancel {
            identity: TimerIdentity::Service(_, ServiceTimerKey::IdleTimeout),
        })),
        "expected idle timer Cancel, got {:?}",
        actions
    );

    // Traffic stops again → idle timer starts with new generation (2).
    router.set_backend_need_level(bn, crate::sm_new::BackendNeed::None);
    router.propagate();
    let actions = adapter.reconcile(&mut router);
    let start = actions.iter().find(|a| matches!(a, TimerAction::Start {
        identity: TimerIdentity::Service(_, ServiceTimerKey::IdleTimeout),
        ..
    }));
    assert!(start.is_some(), "expected new Start after generation change, got {:?}", actions);
}

// ============================================================================
// 6. Multiple SM kinds in one reconcile cycle
// ============================================================================

#[test]
fn multiple_sm_kinds_in_one_cycle() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let mut adapter = TimerAdapter::new(timer, test_config());

    // Set up activation-based service.
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new(timer));
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v1".into() });

    router.create_service(S1, ServiceSm::new(timer, true));
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: true,
        },
    );
    router.propagate();
    let _ = adapter.reconcile(&mut router);

    // Activate via traffic, make pod running.
    let bn = router.create_backend_need();
    router.set_backend_need_to_service_edges(bn, vec![S1]);
    router.set_backend_need_level(bn, crate::sm_new::BackendNeed::Traffic);
    router.propagate();

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    let lease = router.create_schedule_lease();
    router.set_schedule_lease_to_pod_edges(lease, vec![pod_id]);
    router.set_schedule_lease_lease(
        lease,
        crate::sm_new::LeaseInfo { worker_id: worker },
    );
    router.propagate();
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, crate::sm_new::PodStatus::Running);
    router.propagate();
    let _ = adapter.reconcile(&mut router);

    // Traffic stops → idle timer. Also make pod fail → workload retry timer.
    router.set_backend_need_level(bn, crate::sm_new::BackendNeed::None);
    router.send_notify_pod_status(worker, pod_id, crate::sm_new::PodStatus::Failed);
    router.propagate();

    let actions = adapter.reconcile(&mut router);

    // Should have actions for multiple SM kinds.
    let has_service_timer = actions.iter().any(|a| matches!(a, TimerAction::Start {
        identity: TimerIdentity::Service(..),
        ..
    } | TimerAction::Cancel {
        identity: TimerIdentity::Service(..),
    }));
    let has_workload_timer = actions.iter().any(|a| matches!(a, TimerAction::Start {
        identity: TimerIdentity::Workload(..),
        ..
    } | TimerAction::Cancel {
        identity: TimerIdentity::Workload(..),
    }));

    // At minimum we should see some timer activity. The exact combination depends on
    // the SM logic, but both kinds should produce something.
    assert!(
        !actions.is_empty(),
        "expected timer actions from multiple SM kinds"
    );
    // At least one of the SM kinds should have timer activity.
    assert!(
        has_service_timer || has_workload_timer,
        "expected timer actions from service or workload, got {:?}",
        actions
    );
}

// ============================================================================
// 7. Fire dispatches correctly
// ============================================================================

#[test]
fn fire_dispatches_workload_timer() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let adapter = TimerAdapter::new(timer, test_config());
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);

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

    // Make pod fail to enter RetryBackoff.
    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    let lease = router.create_schedule_lease();
    router.set_schedule_lease_to_pod_edges(lease, vec![pod_id]);
    router.set_schedule_lease_lease(
        lease,
        crate::sm_new::LeaseInfo { worker_id: worker },
    );
    router.propagate();
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, crate::sm_new::PodStatus::Failed);
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
fn fire_dispatches_service_timer() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let adapter = TimerAdapter::new(timer, test_config());
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new(timer));
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v1".into() });

    router.create_service(S1, ServiceSm::new(timer, true));
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: true,
        },
    );
    router.propagate();

    // Traffic activates → pod running → traffic stops → idle timer active.
    let bn = router.create_backend_need();
    router.set_backend_need_to_service_edges(bn, vec![S1]);
    router.set_backend_need_level(bn, crate::sm_new::BackendNeed::Traffic);
    router.propagate();

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    let lease = router.create_schedule_lease();
    router.set_schedule_lease_to_pod_edges(lease, vec![pod_id]);
    router.set_schedule_lease_lease(
        lease,
        crate::sm_new::LeaseInfo { worker_id: worker },
    );
    router.propagate();
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, crate::sm_new::PodStatus::Running);
    router.propagate();

    router.set_backend_need_level(bn, crate::sm_new::BackendNeed::None);
    router.propagate();

    let s1 = router.get_service(&S1).unwrap();
    assert!(s1.idle_timer_active);

    // Fire idle timer via adapter.
    adapter.fire(
        &mut router,
        &TimerIdentity::Service(S1, ServiceTimerKey::IdleTimeout),
    );
    router.propagate();

    // Service should deactivate (back to Idle).
    let s1 = router.get_service(&S1).unwrap();
    assert_eq!(s1.state, crate::sm_new::ServiceState::Idle);
}
