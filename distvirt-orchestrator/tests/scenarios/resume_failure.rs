use std::time::Duration;

use distvirt_worker_protocol::WorkerCommand;

use crate::harness::mock_worker::MockWorkerConfig;
use crate::harness::*;

/// Test: ResumePod fails, orchestrator should fall back to cold launch.
#[tokio::test(start_paused = true)]
async fn test_resume_failure_falls_back_to_cold_launch() {
    let mut h = TestHarness::new();

    // Use a worker with a pool that fails on ResumePod but succeeds on LaunchPod.
    let w1 = h.add_worker_with(MockWorkerConfig::with_resume_failure()).await;
    h.converge().await;

    // Create activation namespace.
    let spec = activation_spec(Duration::from_secs(30));
    h.create_namespace("ns1", spec).await;
    h.converge().await;
    h.assert_namespace_status("ns1", distvirt_orchestrator::types::NamespaceStatus::Active);

    // Workload starts dormant (activation-based).
    h.assert_workload_dormant("ns1", "web");
    h.assert_service_idle("ns1", "web-svc");

    // Activate → running → idle → suspended
    h.run_activation_suspend_cycle("ns1", "web-svc", "web").await;
    h.assert_service_idle("ns1", "web-svc");

    // Re-activate via ServiceActivation — resume will fail.
    let svc_ip = h.service_ip("ns1", "web-svc");
    h.worker(&w1).send_event(distvirt_worker_protocol::WorkerEvent::ServiceActivation {
        namespace_id: "ns1".into(),
        service_id: distvirt_worker_protocol::ServiceId::from("web-svc"),
        dst_ip: svc_ip,
    });
    h.converge().await;

    // Reconciliation-based readiness syncing ensures demand is preserved
    // through retry. The workload should enter RetryBackoff, then after backoff,
    // relaunch via cold LaunchPod.
    h.assert_workload_retry_backoff("ns1", "web");
    h.assert_service_need_backend("ns1", "web-svc");

    // Advance past the backoff timer (1s for first retry) → cold launch.
    h.advance_time(Duration::from_secs(2)).await;

    // Workload should be running again after cold launch.
    h.assert_workload_running("ns1", "web");

    // Verify command counts
    h.assert_worker_command_count(&w1, "ResumePod", 1, |c| matches!(c, WorkerCommand::ResumePod { .. }));
    let launch_count = h.worker_command_count(&w1, |c| matches!(c, WorkerCommand::LaunchPod { .. }));
    assert!(launch_count >= 2, "expected at least 2 LaunchPod commands (initial + cold restart), got {}", launch_count);
}
