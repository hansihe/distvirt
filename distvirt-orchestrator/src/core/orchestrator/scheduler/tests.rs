use super::*;
use crate::types::PressureBand;

fn ns(name: &str) -> NamespaceId {
    NamespaceId::from(name)
}

#[test]
fn grant_immediate_when_worker_available() {
    let mut sched = SchedulerCore::new();

    // Add a worker.
    let effects = sched.process(SchedulerCoreInput::WorkerUpdate(
        GlobalWorkerId::from(1),
        WorkerCandidate {
            worker_id: GlobalWorkerId::from(1),
            max_pressure_band: PressureBand::Normal,
            pod_count: 0,
            draining: false,
            active: true,
        },
    ));
    assert!(effects.decisions.is_empty());

    // Request lease — should be granted immediately.
    let effects = sched.process(SchedulerCoreInput::RequestLease {
        namespace_id: ns("test"),
        pod_id: PodId::test(1),
        proto_resume_artifact: None,
    });
    assert_eq!(effects.decisions.len(), 1);
    assert!(matches!(
        &effects.decisions[0],
        SchedulerDecision::Grant { worker_id, .. } if *worker_id == GlobalWorkerId::from(1)
    ));
}

#[test]
fn pend_when_no_workers() {
    let mut sched = SchedulerCore::new();

    let effects = sched.process(SchedulerCoreInput::RequestLease {
        namespace_id: ns("test"),
        pod_id: PodId::test(1),
        proto_resume_artifact: None,
    });
    assert!(effects.decisions.is_empty(), "should pend when no workers");
}

#[test]
fn retry_pending_on_worker_update() {
    let mut sched = SchedulerCore::new();

    // Request lease with no workers — pends.
    sched.process(SchedulerCoreInput::RequestLease {
        namespace_id: ns("test"),
        pod_id: PodId::test(1),
        proto_resume_artifact: None,
    });

    // Add worker — should grant the pending request.
    let effects = sched.process(SchedulerCoreInput::WorkerUpdate(
        GlobalWorkerId::from(1),
        WorkerCandidate {
            worker_id: GlobalWorkerId::from(1),
            max_pressure_band: PressureBand::Normal,
            pod_count: 0,
            draining: false,
            active: true,
        },
    ));
    assert_eq!(effects.decisions.len(), 1);
    assert!(matches!(&effects.decisions[0], SchedulerDecision::Grant { .. }));
}

#[test]
fn drop_request_revokes_granted() {
    let mut sched = SchedulerCore::new();

    sched.process(SchedulerCoreInput::WorkerUpdate(
        GlobalWorkerId::from(1),
        WorkerCandidate {
            worker_id: GlobalWorkerId::from(1),
            max_pressure_band: PressureBand::Normal,
            pod_count: 0,
            draining: false,
            active: true,
        },
    ));

    sched.process(SchedulerCoreInput::RequestLease {
        namespace_id: ns("test"),
        pod_id: PodId::test(1),
        proto_resume_artifact: None,
    });

    let effects = sched.process(SchedulerCoreInput::DropRequest {
        namespace_id: ns("test"),
        pod_id: PodId::test(1),
    });
    assert_eq!(effects.decisions.len(), 1);
    assert!(matches!(&effects.decisions[0], SchedulerDecision::Revoke { .. }));
}
