use super::*;

mod basic;
mod misc;
mod multi;
mod retry;
// mod service_idle; // transplanted to EndpointSm grounding tests
mod endpoint_idle;
mod stateright_endpoint;
mod stateright_pod;
mod stateright_service;
mod stateright_workload;
mod suspend;
mod transitions;

const S1: ServiceId = ServiceId(1);
const S2: ServiceId = ServiceId(2);
const S3: ServiceId = ServiceId(3);
const W1: WorkloadId = WorkloadId(1);
const W2: WorkloadId = WorkloadId(2);
const WK1: WorkerId = WorkerId(1);
const WK2: WorkerId = WorkerId(2);

// ============================================================================
// Test helpers
// ============================================================================

use crate::adapter::timer::{TimerAction, TimerIdentity};

/// Drain workload timer actions from the timer port.
/// Other timer types (service, pod) are consumed but not returned.
fn drain_workload_timer_actions(router: &mut Router) -> Vec<TimerAction> {
    router
        .drain_timer_inputs()
        .into_iter()
        .filter(|(id, _)| *id == TIMER)
        .flat_map(|(_, input)| match input {
            TimerPortInput::WorkloadTimersInput(actions) => actions,
            TimerPortInput::EndpointTimersInput(_) => vec![],
            TimerPortInput::PodTimersInput(_) => vec![],
        })
        .collect()
}

/// Assert that the timer port received Start actions matching the expected timer requests.
fn assert_timer_requested(router: &mut Router, expected: &[TimerRequest]) {
    let actions = drain_workload_timer_actions(router);
    let starts: Vec<_> = actions
        .iter()
        .filter(|a| matches!(a, TimerAction::Start { .. }))
        .collect();
    assert!(
        !starts.is_empty(),
        "expected timer Start actions for {:?}, got {:?}",
        expected,
        actions
    );
    for req in expected {
        let found = starts.iter().any(|a| match a {
            TimerAction::Start {
                identity: TimerIdentity::Workload(_, key),
                generation,
                duration,
            } => *key == req.key && *generation == req.generation && *duration == req.duration,
            _ => false,
        });
        assert!(found, "expected Start for {:?}, got {:?}", req, starts);
    }
}

/// Assert no Start actions are in the output (timers cleared or nothing changed).
fn assert_no_timers_wanted(router: &mut Router) {
    let actions = drain_workload_timer_actions(router);
    let starts: Vec<_> = actions
        .iter()
        .filter(|a| matches!(a, TimerAction::Start { .. }))
        .collect();
    assert!(
        starts.is_empty(),
        "expected no timer Start actions, got {:?}",
        starts
    );
}

/// Assert no timer-related port inputs were delivered at all (dedup suppressed).
fn assert_no_timer_output(router: &mut Router) {
    let actions = drain_workload_timer_actions(router);
    assert!(
        actions.is_empty(),
        "expected no timer output, got {:?}",
        actions
    );
}

// ============================================================================
// Setup helpers
// ============================================================================

/// Helper: set up a workload with an activation-based service that has been
/// activated via an EndpointDemand port, return (mgmt, worker, pod_id, demand_port).
/// After this, workload has spec + demand, a pod exists in Pending state.
/// Use router.set_endpoint_demand_active(demand_port, false) to drop demand.
fn setup_workload_with_pending_pod(
    router: &mut Router,
) -> (ManagementId, WorkerId, PodId, EndpointDemandId) {
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

    // Create demand via EndpointDemand port.
    let ep_id = router.get_service(&S1).unwrap().endpoint_id.unwrap();
    let demand_port = router.create_endpoint_demand();
    router.set_endpoint_port_demand_edges(demand_port, vec![ep_id]);
    router.set_endpoint_demand_active(demand_port, true);
    router.propagate();

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    assert_eq!(router.get_pod(&pod_id).unwrap().status, PodStatus::Pending);

    (mgmt, worker, pod_id, demand_port)
}

/// Helper: schedule a pod to a worker by creating a lease port.
fn schedule_pod(router: &mut Router, worker: WorkerId, pod_id: PodId) -> ScheduleLeaseId {
    let lease = router.create_schedule_lease();
    router.set_pod_lease_edges(lease, vec![pod_id]);
    router.set_schedule_lease_lease(lease, LeaseInfo { worker_id: worker });
    router.propagate();
    lease
}

/// Helper: make a pending pod Running (schedule + worker assignment + status).
fn make_pod_running(router: &mut Router, worker: WorkerId, pod_id: PodId) -> ScheduleLeaseId {
    let lease = schedule_pod(router, worker, pod_id);
    router.set_worker_assignment_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Running);
    router.propagate();
    lease
}

/// Helper: make a pending pod fail via worker notification.
fn make_pod_failed(router: &mut Router, worker: WorkerId, pod_id: PodId) -> ScheduleLeaseId {
    let lease = schedule_pod(router, worker, pod_id);
    router.set_worker_assignment_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(
        worker,
        pod_id,
        PodStatus::Failed {
            exit_code: Some(1),
            reason: "test failure".to_string(),
        },
    );
    router.propagate();
    lease
}

/// Helper: set up a workload (with configurable max_retries) with an always-on
/// service, a running pod, and return (mgmt, worker).
fn setup_running_workload(router: &mut Router, max_retries: u32) -> (ManagementId, WorkerId) {
    router.create_timer(TIMER);
    let worker = WK1;
    router.create_worker(worker);
    router.set_worker_info(worker, WorkerInfo { capacity: 10, ..Default::default() });
    router.create_schedule_request(SCHEDULE_REQUEST);

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::with_max_retries(max_retries));
    router.set_workload_config_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            pod_spec: PodSpec { image: "app:v1".into(), ..Default::default() },
            config: WorkloadConfig { respects_demand: true, ..Default::default() },
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

    // Make the pod running.
    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    make_pod_running(router, worker, pod_id);

    assert!(router.get_workload(&W1).unwrap().pod_running);
    (mgmt, worker)
}

/// Helper: set up a suspendable workload (suspend_on_idle=true via spec) with an
/// activation-based service, a running pod, and return (mgmt, worker, demand_port).
fn setup_running_suspendable_workload(
    router: &mut Router,
) -> (ManagementId, WorkerId, EndpointDemandId) {
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
            config: WorkloadConfig { suspend_on_idle: true, respects_demand: true, ..Default::default() },
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

    // Create demand via EndpointDemand port.
    let ep_id = router.get_service(&S1).unwrap().endpoint_id.unwrap();
    let demand_port = router.create_endpoint_demand();
    router.set_endpoint_port_demand_edges(demand_port, vec![ep_id]);
    router.set_endpoint_demand_active(demand_port, true);
    router.propagate();

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    make_pod_running(router, worker, pod_id);

    assert!(router.get_workload(&W1).unwrap().pod_running);
    (mgmt, worker, demand_port)
}
