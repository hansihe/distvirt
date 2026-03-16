use std::time::Duration;

use crate::harness::*;
use crate::harness::mock_worker::MockWorkerConfig;
use distvirt_worker_protocol::WorkerCommand;

/// Two pool workers. Activate on one worker, suspend (artifact placed on that worker).
/// Re-activate. Assert ResumePod goes to the artifact-holding worker (not the other).
#[test]
fn test_resume_pinned_to_artifact_worker() {
    let mut h = TestHarness::new();
    let _w1 = h.add_worker_with(MockWorkerConfig::with_pool());
    let _w2 = h.add_worker_with(MockWorkerConfig::with_pool());

    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout));
    h.converge();
    h.assert_workload_dormant("ns", "web");

    // Activate → Running (lands on one of the two workers).
    h.activate_service("ns", "web-svc");
    let running_worker = h.workload_global_worker_id("ns", "web").expect("expected worker_id");

    // Idle → suspend.
    h.deactivate_service("ns", "web-svc");
    h.advance_past_idle_timeout("ns", "web-svc");
    h.assert_workload_suspended("ns", "web");

    // Re-activate → should resume on the same worker (artifact pinning).
    h.activate_service("ns", "web-svc");

    let resume_worker = h.workload_global_worker_id("ns", "web").expect("expected worker_id");
    assert_eq!(
        resume_worker, running_worker,
        "resume should be pinned to the artifact-holding worker, not the other"
    );

    // Verify ResumePod was issued (not LaunchPod).
    let resume_count = h.worker_command_count(&running_worker, |c| matches!(c, WorkerCommand::ResumePod { .. }));
    assert!(resume_count >= 1, "expected ResumePod for snapshot resume");
}

/// One pool worker. Activate, suspend (artifact placed on worker).
/// Disconnect worker. Add a new pool worker.
/// Re-activate → should cold LaunchPod (not ResumePod) since artifact was lost.
#[test]
fn test_artifact_lost_on_worker_disconnect_cold_launch() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool());

    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout));
    h.converge();
    h.assert_workload_dormant("ns", "web");

    // Activate → Running → idle → suspend
    h.run_activation_suspend_cycle("ns", "web-svc", "web");

    // Disconnect the worker — artifact is lost.
    h.disconnect_worker(&w1);
    h.converge();

    // Add a fresh pool worker.
    let w2 = h.add_worker_with(MockWorkerConfig::with_pool());
    h.converge();

    // Re-activate on the new worker.
    h.activate_service("ns", "web-svc");

    let new_worker = h.workload_global_worker_id("ns", "web").expect("expected worker_id");
    assert_eq!(new_worker, w2, "workload should be on the new worker");

    // Verify LaunchPod was used (not ResumePod) — cold launch since artifact is gone.
    let launch_count = h.worker_command_count(&w2, |c| matches!(c, WorkerCommand::LaunchPod { .. }));
    assert!(launch_count >= 1, "expected LaunchPod for cold launch after artifact loss");
    h.assert_worker_command_count(&w2, "ResumePod", 0, |c| matches!(c, WorkerCommand::ResumePod { .. }));
}
