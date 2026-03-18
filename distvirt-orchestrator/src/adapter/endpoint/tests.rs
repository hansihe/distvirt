use super::*;
use crate::sm::{
    DRouter, FABRIC_ENDPOINT, PodId, SCHEDULE_REQUEST, ServiceId, ServiceSm, ServiceSpec, TIMER,
    WorkerInfo, WorkloadId, WorkloadSm, WorkloadSpec,
};

const W1: WorkloadId = WorkloadId(1);
const S1: ServiceId = ServiceId(1);
const WK1: crate::sm::WorkerId = crate::sm::WorkerId(1);

/// Set up a router with a workload and always-on service, propagate initial state.
/// Returns (worker_id, pod_id).
fn setup_workload(router: &mut DRouter) -> (crate::sm::WorkerId, PodId) {
    router.create_timer(TIMER);
    let worker = WK1;
    router.create_worker(worker);
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);
    router.create_fabric_endpoint(FABRIC_ENDPOINT);

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

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    (worker, pod_id)
}

/// Make a pod reach Running state so the service becomes Active.
fn make_pod_running(router: &mut DRouter, worker: crate::sm::WorkerId, pod_id: PodId) {
    // Grant lease
    let lease = router.create_schedule_lease();
    router.set_pod_lease_edges(lease, vec![pod_id]);
    router.set_schedule_lease_lease(lease, crate::sm::LeaseInfo { worker_id: worker });
    router.propagate();

    // Assign worker to pod
    router.set_worker_assignment_edges(worker, vec![pod_id]);
    router.propagate();

    // Notify running
    router.send_notify_pod_status(worker, pod_id, crate::sm::PodStatus::Running);
    router.propagate();
}

// ============================================================================
// 1. No active services → no actions
// ============================================================================

#[test]
fn no_active_services_no_actions() {
    let mut router = DRouter::new_traced(16, distvirt_sm_router::trace::PanicTracer::new());
    router.create_schedule_request(SCHEDULE_REQUEST);
    router.create_fabric_endpoint(FABRIC_ENDPOINT);
    let mut adapter = EndpointAdapter::new(FABRIC_ENDPOINT);

    router.propagate();
    let (actions, _) = adapter.reconcile(&mut router);
    assert!(actions.is_empty());
}

// ============================================================================
// 2. Service becomes active → Update action
// ============================================================================

#[test]
fn service_becomes_active_update() {
    let mut router = DRouter::new_traced(16, distvirt_sm_router::trace::PanicTracer::new());
    let (worker, pod_id) = setup_workload(&mut router);
    let mut adapter = EndpointAdapter::new(FABRIC_ENDPOINT);

    // Initially, service is NeedBackend — no active endpoints.
    let (actions, _) = adapter.reconcile(&mut router);
    assert!(
        actions.is_empty(),
        "expected no actions before pod running, got {:?}",
        actions
    );

    // Make pod running → service becomes Active.
    make_pod_running(&mut router, worker, pod_id);
    let (actions, _) = adapter.reconcile(&mut router);

    assert_eq!(actions.len(), 1);
    match &actions[0] {
        EndpointAction::Update { endpoint_id: _, info } => {
            assert!(matches!(
                info.kind,
                crate::sm::endpoint::EndpointKind::Service { service_id, .. } if service_id == S1
            ));
            assert_eq!(info.backend.as_ref().unwrap().worker_id, worker);
        }
        other => panic!("expected Update, got {:?}", other),
    }
}

// ============================================================================
// 3. Service leaves active → Remove action
// ============================================================================

#[test]
fn service_leaves_active_remove() {
    let mut router = DRouter::new_traced(16, distvirt_sm_router::trace::PanicTracer::new());
    let (worker, pod_id) = setup_workload(&mut router);
    let mut adapter = EndpointAdapter::new(FABRIC_ENDPOINT);

    // Make active.
    make_pod_running(&mut router, worker, pod_id);
    let _ = adapter.reconcile(&mut router).0;

    // Pod fails → service goes to NeedBackend.
    router.send_notify_pod_status(worker, pod_id, crate::sm::PodStatus::Failed);
    router.propagate();

    let (actions, _) = adapter.reconcile(&mut router);
    let removes: Vec<_> = actions
        .iter()
        .filter(|a| matches!(a, EndpointAction::Remove { .. }))
        .collect();
    // Verify the old_info is populated on Remove actions.
    for r in &removes {
        if let EndpointAction::Remove { old_info, .. } = r {
            assert_eq!(old_info.backend.as_ref().unwrap().worker_id, worker);
        }
    }
    assert!(
        !removes.is_empty(),
        "expected Remove action, got {:?}",
        actions
    );
}

// ============================================================================
// 4. Stable state → no actions (dedup)
// ============================================================================

#[test]
fn stable_state_no_actions() {
    let mut router = DRouter::new_traced(16, distvirt_sm_router::trace::PanicTracer::new());
    let (worker, pod_id) = setup_workload(&mut router);
    let mut adapter = EndpointAdapter::new(FABRIC_ENDPOINT);

    // Make active and reconcile.
    make_pod_running(&mut router, worker, pod_id);
    let _ = adapter.reconcile(&mut router).0;

    // Propagate again — signal dedup, no new delivery.
    router.propagate();
    let (actions, _) = adapter.reconcile(&mut router);
    assert!(
        actions.is_empty(),
        "expected no actions on stable state, got {:?}",
        actions
    );
}

// ============================================================================
// 5. Multiple services change in one cycle
// ============================================================================

#[test]
fn multiple_services_change() {
    let mut router = DRouter::new_traced(16, distvirt_sm_router::trace::PanicTracer::new());
    router.create_timer(TIMER);
    let worker = WK1;
    router.create_worker(worker);
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);
    router.create_fabric_endpoint(FABRIC_ENDPOINT);

    let w2_id = WorkloadId(2);
    let s2_id = ServiceId(2);

    // Two independent workloads + services.
    let mgmt1 = router.create_management();
    router.create_workload(W1, WorkloadSm::new());
    router.set_workload_config_edges(mgmt1, vec![W1]);
    router.set_management_wl_spec(
        mgmt1,
        WorkloadSpec {
            image: "app:v1".into(),
            ..Default::default()
        },
    );
    router.create_service(S1, ServiceSm::new());
    router.set_service_config_edges(mgmt1, vec![S1]);
    router.set_management_svc_spec(
        mgmt1,
        ServiceSpec {
            workload: W1,
            has_activation: false,
            ..Default::default()
        },
    );

    let mgmt2 = router.create_management();
    router.create_workload(w2_id, WorkloadSm::new());
    router.set_workload_config_edges(mgmt2, vec![w2_id]);
    router.set_management_wl_spec(
        mgmt2,
        WorkloadSpec {
            image: "app:v2".into(),
            ..Default::default()
        },
    );
    router.create_service(s2_id, ServiceSm::new());
    router.set_service_config_edges(mgmt2, vec![s2_id]);
    router.set_management_svc_spec(
        mgmt2,
        ServiceSpec {
            workload: w2_id,
            has_activation: false,
            ..Default::default()
        },
    );

    router.propagate();

    let pod1 = router.get_workload(&W1).unwrap().pod_id.unwrap();
    let pod2 = router.get_workload(&w2_id).unwrap().pod_id.unwrap();

    // Make both pods running.
    let lease1 = router.create_schedule_lease();
    router.set_pod_lease_edges(lease1, vec![pod1]);
    router.set_schedule_lease_lease(lease1, crate::sm::LeaseInfo { worker_id: worker });
    let lease2 = router.create_schedule_lease();
    router.set_pod_lease_edges(lease2, vec![pod2]);
    router.set_schedule_lease_lease(lease2, crate::sm::LeaseInfo { worker_id: worker });
    router.propagate();

    router.set_worker_assignment_edges(worker, vec![pod1, pod2]);
    router.propagate();

    router.send_notify_pod_status(worker, pod1, crate::sm::PodStatus::Running);
    router.send_notify_pod_status(worker, pod2, crate::sm::PodStatus::Running);
    router.propagate();

    let mut adapter = EndpointAdapter::new(FABRIC_ENDPOINT);
    let (actions, _) = adapter.reconcile(&mut router);

    let update_count = actions
        .iter()
        .filter(|a| matches!(a, EndpointAction::Update { .. }))
        .count();
    assert!(
        update_count >= 2,
        "expected at least 2 Update actions, got {:?}",
        actions
    );
}
