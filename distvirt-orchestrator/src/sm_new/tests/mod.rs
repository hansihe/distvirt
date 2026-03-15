use super::*;

mod basic;
mod transitions;
mod retry;
mod suspend;
mod service_idle;
mod misc;
mod multi;
mod stateright_workload;
mod stateright_service;
mod stateright_pod;

const S1: ServiceId = ServiceId(1);
const S2: ServiceId = ServiceId(2);
const S3: ServiceId = ServiceId(3);
const W1: WorkloadId = WorkloadId(1);
const W2: WorkloadId = WorkloadId(2);

// ============================================================================
// Test helpers
// ============================================================================

/// Extract workload timer requests from timer port inputs.
/// Each delivery is a `Vec<(WorkloadId, Vec<TimerRequest>)>` — one entry per workload
/// connected to the timer port.
fn drain_timer_requests(router: &mut Router) -> Vec<Vec<(WorkloadId, Vec<TimerRequest>)>> {
    router
        .drain_timer_inputs()
        .into_iter()
        .filter(|(id, _)| *id == TIMER)
        .filter_map(|(_, input)| match input {
            TimerPortInput::WorkloadTimersInput(timers) => Some(timers),
            _ => None,
        })
        .collect()
}

/// Assert that the timer port received a timer delivery where the workload
/// declared exactly the expected timer requests.
fn assert_timer_requested(router: &mut Router, expected: &[TimerRequest]) {
    let deliveries = drain_timer_requests(router);
    assert!(
        !deliveries.is_empty(),
        "expected timer delivery {:?}, got nothing",
        expected
    );
    // Last delivery should have one workload's timer list matching expected.
    let last = deliveries.last().unwrap();
    assert_eq!(last.len(), 1, "expected 1 workload's timers, got {:?}", last);
    assert_eq!(last[0].1.as_slice(), expected, "timer requests mismatch");
}

/// Assert that timer output is either absent or empty (no active timers).
fn assert_no_timers_wanted(router: &mut Router) {
    let deliveries = drain_timer_requests(router);
    for delivery in &deliveries {
        for (_, workload_timers) in delivery {
            assert!(
                workload_timers.is_empty(),
                "expected no timers wanted, got {:?}",
                workload_timers
            );
        }
    }
}

/// Assert no timer-related port inputs were delivered at all (dedup suppressed).
fn assert_no_timer_output(router: &mut Router) {
    let deliveries = drain_timer_requests(router);
    assert!(
        deliveries.is_empty(),
        "expected no timer output, got {:?}",
        deliveries
    );
}

// ============================================================================
// Setup helpers
// ============================================================================

/// Helper: set up a workload with an activation-based service that has been
/// activated, return (mgmt, worker, pod_id).
/// After this, workload has spec + demand, a pod exists in Pending state.
/// Use send_activate_service(mgmt, S1, false) to drop demand.
fn setup_workload_with_pending_pod(router: &mut Router) -> (ManagementId, WorkerId, PodId) {
    router.create_timer(TIMER);
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new());
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v1".into() });

    router.create_service(S1, ServiceSm::new(true)); // activation-based
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: true,
        },
    );
    router.propagate();

    // Activate the service to create demand.
    router.send_activate_service(mgmt, S1, true);
    router.propagate();

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    assert_eq!(router.get_pod(&pod_id).unwrap().status, PodStatus::Pending);

    (mgmt, worker, pod_id)
}

/// Helper: schedule a pod to a worker by creating a lease port.
fn schedule_pod(router: &mut Router, worker: WorkerId, pod_id: PodId) -> ScheduleLeaseId {
    let lease = router.create_schedule_lease();
    router.set_schedule_lease_to_pod_edges(lease, vec![pod_id]);
    router.set_schedule_lease_lease(lease, LeaseInfo { worker_id: worker });
    router.propagate();
    lease
}

/// Helper: make a pending pod Running (schedule + worker assignment + status).
fn make_pod_running(router: &mut Router, worker: WorkerId, pod_id: PodId) -> ScheduleLeaseId {
    let lease = schedule_pod(router, worker, pod_id);
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Running);
    router.propagate();
    lease
}

/// Helper: make a pending pod fail via worker notification.
fn make_pod_failed(router: &mut Router, worker: WorkerId, pod_id: PodId) -> ScheduleLeaseId {
    let lease = schedule_pod(router, worker, pod_id);
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Failed);
    router.propagate();
    lease
}

/// Helper: set up a workload (with configurable max_retries) with an always-on
/// service, a running pod, and return (mgmt, worker).
fn setup_running_workload(router: &mut Router, max_retries: u32) -> (ManagementId, WorkerId) {
    router.create_timer(TIMER);
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::with_max_retries(max_retries));
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

    // Make the pod running.
    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    make_pod_running(router, worker, pod_id);

    assert!(router.get_workload(&W1).unwrap().pod_running);
    (mgmt, worker)
}

/// Helper: set up a suspendable workload (suspend_on_idle=true) with an
/// activation-based service, a running pod, and return (mgmt, worker).
fn setup_running_suspendable_workload(router: &mut Router) -> (ManagementId, WorkerId) {
    router.create_timer(TIMER);
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new_suspendable());
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v1".into() });

    router.create_service(S1, ServiceSm::new(true)); // activation-based
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: true,
        },
    );
    router.propagate();

    // Activate → demand → pod created.
    router.send_activate_service(mgmt, S1, true);
    router.propagate();

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    make_pod_running(router, worker, pod_id);

    assert!(router.get_workload(&W1).unwrap().pod_running);
    (mgmt, worker)
}
