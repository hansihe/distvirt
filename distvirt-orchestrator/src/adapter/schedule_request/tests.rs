use super::*;
use crate::sm_new::{
    Router, ServiceSm, ServiceSpec, WorkerInfo, WorkloadId, WorkloadSm, WorkloadSpec,
    SCHEDULE_REQUEST, TIMER,
};

const W1: WorkloadId = WorkloadId(1);
const S1: crate::sm_new::ServiceId = crate::sm_new::ServiceId(1);

/// Set up a router with a workload (always-on service), propagate initial state.
/// Returns (worker, pod_id).
fn setup_workload(router: &mut Router) -> (crate::sm_new::WorkerId, PodId) {
    router.create_timer(TIMER);
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new());
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v1".into() });

    router.create_service(S1, ServiceSm::new(false)); // always-on
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: false,
        },
    );
    router.propagate();

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    (worker, pod_id)
}

// ============================================================================
// 1. No requests → no deltas
// ============================================================================

#[test]
fn no_requests_no_deltas() {
    let mut router = Router::new(16);
    router.create_schedule_request(SCHEDULE_REQUEST);
    let mut adapter = ScheduleRequestAdapter::new(SCHEDULE_REQUEST);

    router.propagate();
    let deltas = adapter.reconcile(&mut router);
    assert!(deltas.is_empty());
}

// ============================================================================
// 2. New pod request → Request delta
// ============================================================================

#[test]
fn new_pod_request_delta() {
    let mut router = Router::new(16);
    let (_worker, pod_id) = setup_workload(&mut router);
    let mut adapter = ScheduleRequestAdapter::new(SCHEDULE_REQUEST);

    let deltas = adapter.reconcile(&mut router);
    assert_eq!(deltas.len(), 1);
    match &deltas[0] {
        ScheduleRequestDelta::Request {
            pod_id: req_pod,
            request,
        } => {
            assert_eq!(*req_pod, pod_id);
            assert!(request.resume_artifact.is_none());
        }
        other => panic!("expected Request, got {:?}", other),
    }

    // Should be tracked in sent_requests.
    assert!(adapter.sent_requests().contains_key(&pod_id));
}

// ============================================================================
// 3. Pod removed → Drop delta
// ============================================================================

#[test]
fn pod_removed_drop_delta() {
    let mut router = Router::new(16);
    let (worker, pod_id) = setup_workload(&mut router);
    let mut adapter = ScheduleRequestAdapter::new(SCHEDULE_REQUEST);

    // Populate cache.
    let deltas = adapter.reconcile(&mut router);
    assert_eq!(deltas.len(), 1);

    // Make pod fail → schedule request disappears.
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

    let deltas = adapter.reconcile(&mut router);
    let drop_deltas: Vec<_> = deltas
        .iter()
        .filter(|d| matches!(d, ScheduleRequestDelta::Drop { .. }))
        .collect();
    assert!(
        !drop_deltas.is_empty(),
        "expected Drop delta, got {:?}",
        deltas
    );
    assert!(!adapter.sent_requests().contains_key(&pod_id));
}

// ============================================================================
// 4. Stable state → no deltas
// ============================================================================

#[test]
fn stable_state_no_deltas() {
    let mut router = Router::new(16);
    let (_worker, _pod_id) = setup_workload(&mut router);
    let mut adapter = ScheduleRequestAdapter::new(SCHEDULE_REQUEST);

    // Initial reconcile.
    let _ = adapter.reconcile(&mut router);

    // Propagate again — signal dedup, no new delivery.
    router.propagate();
    let deltas = adapter.reconcile(&mut router);
    assert!(
        deltas.is_empty(),
        "expected no deltas on stable state, got {:?}",
        deltas
    );
}

// ============================================================================
// 5. Multiple pods change in one cycle
// ============================================================================

#[test]
fn multiple_pods_change() {
    let mut router = Router::new(16);
    router.create_timer(TIMER);
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);

    let w2_id = WorkloadId(2);
    let s2_id = crate::sm_new::ServiceId(2);

    // Two separate management ports for independent workloads.
    let mgmt1 = router.create_management();
    router.create_workload(W1, WorkloadSm::new());
    router.set_management_to_workload_edges(mgmt1, vec![W1]);
    router.set_management_wl_spec(mgmt1, WorkloadSpec { image: "app:v1".into() });
    router.create_service(S1, ServiceSm::new(false));
    router.set_management_to_service_edges(mgmt1, vec![S1]);
    router.set_management_svc_spec(
        mgmt1,
        ServiceSpec {
            workload: W1,
            has_activation: false,
        },
    );

    let mgmt2 = router.create_management();
    router.create_workload(w2_id, WorkloadSm::new());
    router.set_management_to_workload_edges(mgmt2, vec![w2_id]);
    router.set_management_wl_spec(mgmt2, WorkloadSpec { image: "app:v2".into() });
    router.create_service(s2_id, ServiceSm::new(false));
    router.set_management_to_service_edges(mgmt2, vec![s2_id]);
    router.set_management_svc_spec(
        mgmt2,
        ServiceSpec {
            workload: w2_id,
            has_activation: false,
        },
    );

    router.propagate();

    let mut adapter = ScheduleRequestAdapter::new(SCHEDULE_REQUEST);
    let deltas = adapter.reconcile(&mut router);

    let request_count = deltas
        .iter()
        .filter(|d| matches!(d, ScheduleRequestDelta::Request { .. }))
        .count();
    assert!(
        request_count >= 2,
        "expected at least 2 Request deltas, got {:?}",
        deltas
    );
    assert!(adapter.sent_requests().len() >= 2);
}
