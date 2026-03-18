use super::*;
use crate::sm::{
    DRouter, LeaseInfo, SCHEDULE_REQUEST, ServiceSm, ServiceSpec, TIMER, WorkerInfo, WorkloadId,
    WorkloadSm, WorkloadSpec,
};

const W1: WorkloadId = WorkloadId(1);
const S1: crate::sm::ServiceId = crate::sm::ServiceId(1);
const WK1: crate::sm::WorkerId = crate::sm::WorkerId(1);
const WK2: crate::sm::WorkerId = crate::sm::WorkerId(2);

/// Set up a router with a workload (always-on service), propagate initial state.
/// Returns (worker, pod_id).
fn setup_workload(router: &mut DRouter) -> (crate::sm::WorkerId, crate::sm::PodId) {
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

/// Schedule a pod to a worker by creating a lease, then propagate.
fn schedule_pod(router: &mut DRouter, worker: crate::sm::WorkerId, pod_id: crate::sm::PodId) {
    let lease = router.create_schedule_lease();
    router.set_pod_lease_edges(lease, vec![pod_id]);
    router.set_schedule_lease_lease(lease, LeaseInfo { worker_id: worker });
    router.propagate();
}

// ============================================================================
// 1. No pods → no actions
// ============================================================================

#[test]
fn no_pods_no_actions() {
    let mut router = DRouter::new_traced(16, distvirt_sm_router::trace::PanicTracer::new());
    router.create_schedule_request(SCHEDULE_REQUEST);
    let mut adapter = PodAssignmentAdapter::new();

    router.propagate();
    let (actions, _) = adapter.reconcile(&mut router);
    assert!(actions.is_empty());
}

// ============================================================================
// 2. Pod appears on worker → Launch action
// ============================================================================

#[test]
fn pod_appears_launch() {
    let mut router = DRouter::new_traced(16, distvirt_sm_router::trace::PanicTracer::new());
    let (worker, pod_id) = setup_workload(&mut router);

    // Schedule pod to worker → pod sets pod_to_worker_edges.
    schedule_pod(&mut router, worker, pod_id);

    let mut adapter = PodAssignmentAdapter::new();
    let (actions, _) = adapter.reconcile(&mut router);

    assert_eq!(actions.len(), 1);
    match &actions[0] {
        PodAssignmentAction::Launch {
            worker_id,
            pod_id: launched_pod,
            request,
            ..
        } => {
            assert_eq!(*worker_id, worker);
            assert_eq!(*launched_pod, pod_id);
            assert!(request.resume_artifact.is_none());
        }
        other => panic!("expected Launch, got {:?}", other),
    }
}

// ============================================================================
// 3. Pod with resume_artifact → Resume action
// ============================================================================
//
// The Resume vs Launch branch is determined by `resume_artifact.is_some()` on the
// PodScheduleRequest. Driving the full suspend/resume flow through the SM graph
// requires SM-internal types (ArtifactId). Instead, we test the adapter's branch
// indirectly: a pod created after a suspend cycle carries resume_artifact, and
// the adapter should emit Resume. This is covered by the orchestrator scenario
// tests. The unit test for the Launch branch above exercises the core diff logic.

// ============================================================================
// 4. Pod disappears → Stop action
// ============================================================================

#[test]
fn pod_disappears_stop() {
    let mut router = DRouter::new_traced(16, distvirt_sm_router::trace::PanicTracer::new());
    let (worker, pod_id) = setup_workload(&mut router);

    schedule_pod(&mut router, worker, pod_id);

    let mut adapter = PodAssignmentAdapter::new();
    // Consume the initial Launch delta.
    let (actions, _) = adapter.reconcile(&mut router);
    assert_eq!(actions.len(), 1);
    assert!(matches!(&actions[0], PodAssignmentAction::Launch { .. }));

    // Make pod fail → it leaves the worker's assigned pods.
    router.set_worker_assignment_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, crate::sm::PodStatus::Failed);
    router.propagate();

    let (actions, _) = adapter.reconcile(&mut router);
    let stop_actions: Vec<_> = actions
        .iter()
        .filter(|a| matches!(a, PodAssignmentAction::Stop { .. }))
        .collect();
    assert!(
        !stop_actions.is_empty(),
        "expected Stop action, got {:?}",
        actions
    );
}

// ============================================================================
// 5. Stable state → no actions
// ============================================================================

#[test]
fn stable_state_no_actions() {
    let mut router = DRouter::new_traced(16, distvirt_sm_router::trace::PanicTracer::new());
    let (worker, pod_id) = setup_workload(&mut router);

    schedule_pod(&mut router, worker, pod_id);

    let mut adapter = PodAssignmentAdapter::new();
    let _ = adapter.reconcile(&mut router).0;

    // Propagate again — no new deltas since nothing changed.
    router.propagate();
    let (actions, _) = adapter.reconcile(&mut router);
    assert!(
        actions.is_empty(),
        "expected no actions on stable state, got {:?}",
        actions
    );
}

// ============================================================================
// 6. Multiple workers in one reconcile
// ============================================================================

#[test]
fn multiple_workers() {
    let mut router = DRouter::new_traced(16, distvirt_sm_router::trace::PanicTracer::new());
    router.create_timer(TIMER);
    router.create_schedule_request(SCHEDULE_REQUEST);

    let worker1 = WK1;
    router.create_worker(worker1);
    router.set_worker_info(worker1, WorkerInfo { capacity: 10, ..Default::default() });
    let worker2 = WK2;
    router.create_worker(worker2);
    router.set_worker_info(worker2, WorkerInfo { capacity: 10, ..Default::default() });

    // Two separate management ports, each with its own workload+service.
    let w2_id = WorkloadId(2);
    let s2_id = crate::sm::ServiceId(2);

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

    // Schedule pods to different workers.
    let lease1 = router.create_schedule_lease();
    router.set_pod_lease_edges(lease1, vec![pod1]);
    router.set_schedule_lease_lease(lease1, LeaseInfo { worker_id: worker1 });

    let lease2 = router.create_schedule_lease();
    router.set_pod_lease_edges(lease2, vec![pod2]);
    router.set_schedule_lease_lease(lease2, LeaseInfo { worker_id: worker2 });

    router.propagate();

    let mut adapter = PodAssignmentAdapter::new();
    let (actions, _) = adapter.reconcile(&mut router);

    // Should have launches for both workers.
    let launch_count = actions
        .iter()
        .filter(|a| matches!(a, PodAssignmentAction::Launch { .. }))
        .count();
    assert!(
        launch_count >= 2,
        "expected at least 2 Launch actions, got {:?}",
        actions
    );
}
