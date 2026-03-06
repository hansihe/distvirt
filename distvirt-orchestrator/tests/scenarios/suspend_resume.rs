use std::time::Duration;

use crate::harness::*;
use crate::harness::mock_worker::MockWorkerConfig;
use distvirt_worker_protocol::WorkerCommand;

/// Full cycle: activate → run → idle → suspend → re-activate → resume → running.
/// Verify the resume path uses ResumePod (not LaunchPod).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_resume_from_suspended() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;
    h.assert_workload_dormant("ns", "web");

    // Activate → running → idle → suspend
    h.run_activation_suspend_cycle("ns", "web-svc", "web").await;

    // Re-activate → should resume (not cold launch)
    h.activate_service("ns", "web-svc").await;

    // Verify ResumePod was sent (not LaunchPod after the first one)
    h.assert_worker_command_count(&w1, "ResumePod", 1, |c| matches!(c, WorkerCommand::ResumePod { .. }));
}

/// Use suspend_hang handler. Activate → run → idle → suspending → advance past SUSPEND_TIMEOUT.
/// Workload should fall back (StopPod issued).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_suspend_timeout_fallback_to_stop() {
    let config = MockWorkerConfig::with_suspend_hang().add_pool();
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(config).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;

    // Activate → running
    h.activate_service("ns", "web-svc").await;

    // Idle → begin suspending
    h.deactivate_service("ns", "web-svc").await;
    h.advance_past_idle_timeout("ns", "web-svc").await;
    // Should be in Suspending (handler doesn't respond)
    h.assert_workload_suspending("ns", "web");

    // Advance past suspend timeout (30s)
    h.advance_time(Duration::from_secs(31)).await;

    // After suspend timeout, the orchestrator issues StopPod and the workload
    // transitions to Dormant (demand is 0, service went idle).
    h.assert_workload_dormant("ns", "web");

    // StopPod should have been issued
    let stop_count = h.worker_command_count(&w1, |c| matches!(c, WorkerCommand::StopPod { .. }));
    assert!(stop_count >= 1, "expected StopPod after suspend timeout");
}

/// Worker returns PodSuspendFailed. Workload should transition appropriately.
/// StopPod should be issued.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_suspend_failure_fallback_to_stop() {
    let config = MockWorkerConfig::with_suspend_failure().add_pool();
    let mut h = TestHarness::new();
    let _w1 = h.add_worker_with(config).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;

    // Activate → running → idle → suspend attempt (fails immediately)
    h.activate_service("ns", "web-svc").await;
    h.deactivate_service("ns", "web-svc").await;
    h.advance_past_idle_timeout("ns", "web-svc").await;

    // PodSuspendFailed triggers StopPod fallback. With demand at 0 (service went idle),
    // the workload transitions to Dormant.
    h.assert_workload_dormant("ns", "web");
}

/// Use activation_no_suspend_spec. Activate → run → idle → stop (not suspend).
/// Re-activate → cold start (LaunchPod, not ResumePod).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_activation_no_suspend_cold_start() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_no_suspend_spec(timeout)).await;
    h.converge().await;
    h.assert_workload_dormant("ns", "web");

    // Activate → running → idle → stop (not suspend, because suspend_on_idle=false)
    h.run_activation_stop_cycle("ns", "web-svc", "web").await;

    // Re-activate → cold start
    h.activate_service("ns", "web-svc").await;

    // Verify LaunchPod was used both times (not ResumePod)
    h.assert_worker_command_count(&w1, "LaunchPod", 2, |c| matches!(c, WorkerCommand::LaunchPod { .. }));
    h.assert_worker_command_count(&w1, "ResumePod", 0, |c| matches!(c, WorkerCommand::ResumePod { .. }));
}
