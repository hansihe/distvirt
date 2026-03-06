use std::time::Duration;

use crate::harness::*;
use crate::harness::mock_worker::MockWorkerConfig;
use distvirt_orchestrator::types::*;
use distvirt_worker_protocol::WorkerCommand;

/// Pod is Launching on worker. Worker disconnects.
/// Workload should go WaitingForCapacity.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_worker_disconnect_during_launch() {
    let config = MockWorkerConfig::with_launch_hang();
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(config).await;
    h.create_namespace("ns", always_on_spec()).await;
    h.converge().await;
    h.assert_workload_launching("ns", "echo");

    // Disconnect the worker hosting the launching pod
    h.disconnect_worker(&w1);
    h.converge().await;

    // Workload should be WaitingForCapacity (no other worker available)
    h.assert_workload_waiting_for_capacity("ns", "echo");
}

/// Pod is Suspending. Worker disconnects. Artifact is lost.
/// Workload should handle gracefully.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_worker_disconnect_during_suspend() {
    let config = MockWorkerConfig::with_suspend_hang().add_pool();
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(config).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;

    // Activate → running → idle → begin suspending
    h.activate_service("ns", "web-svc").await;
    h.deactivate_service("ns", "web-svc").await;
    h.advance_past_idle_timeout("ns", "web-svc").await;
    h.assert_workload_suspending("ns", "web");

    // Disconnect worker during suspend
    h.disconnect_worker(&w1);
    h.converge().await;

    // Demand is 0 (service went idle), no workers available.
    h.assert_workload_dormant("ns", "web");
}

/// Pod is Resuming from Suspended. Worker disconnects. Artifact may be lost.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_worker_disconnect_during_resume() {
    // Need a handler that hangs on ResumePod but handles SuspendPod normally
    let config = MockWorkerConfig {
        handler: Some(Box::new(|cmd| match cmd {
            distvirt_worker_protocol::WorkerCommand::ResumePod { .. } => Some(vec![]),
            _ => None,
        })),
        capabilities: MockWorkerConfig::with_pool().capabilities,
    };
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(config).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;

    // Activate → running → idle → suspend
    h.run_activation_suspend_cycle("ns", "web-svc", "web").await;

    // Re-activate → resuming (hangs)
    // Low-level: must send event directly to trigger resume on the specific worker with hang handler
    let svc_ip = h.service_ip("ns", "web-svc");
    h.worker(&w1).send_event(distvirt_worker_protocol::WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: distvirt_worker_protocol::ServiceId::from("web-svc"),
        dst_ip: svc_ip,
    });
    h.converge().await;
    h.assert_workload_resuming("ns", "web");

    // Disconnect worker during resume
    h.disconnect_worker(&w1);
    h.converge().await;

    // Demand > 0 (service was re-activated), but no workers available.
    h.assert_workload_waiting_for_capacity("ns", "web");

    // Add a new worker — workload should cold-start (LaunchPod, not ResumePod)
    let w2 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    h.converge().await;
    h.assert_workload_running("ns", "web");

    let launch_count = h.worker_command_count(&w2, |c| matches!(c, WorkerCommand::LaunchPod { .. }));
    assert!(launch_count >= 1, "expected LaunchPod (cold start) after artifact loss, got 0");
    h.assert_worker_command_count(&w2, "ResumePod", 0, |c| matches!(c, WorkerCommand::ResumePod { .. }));
}

/// Running workload, all workers disconnect. Workload goes WaitingForCapacity.
/// No panic, no infinite loop.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_all_workers_disconnect() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker().await;
    h.create_namespace("ns", always_on_spec()).await;
    h.converge().await;
    h.assert_workload_running("ns", "echo");

    // Disconnect the only worker
    h.disconnect_worker(&w1);
    h.converge().await;

    h.assert_workload_waiting_for_capacity("ns", "echo");
    h.assert_worker_count(0);
}

/// Worker holds artifacts in placement table. Disconnect.
/// Verify placement entries for that worker are removed.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_worker_disconnect_clears_placements() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;

    // Activate → running → idle → suspended (creates an artifact placement)
    h.run_activation_suspend_cycle("ns", "web-svc", "web").await;

    // Verify there's an artifact and placement exists for w1
    match h.workload_state("ns", "web") {
        WorkloadState::Suspended { .. } => {},
        other => panic!("expected Suspended, got {:?}", other),
    };
    let has_placement = h.orchestrator().placement_table.iter()
        .any(|(_, p)| p.worker_id == w1);
    assert!(has_placement, "expected placement on worker before disconnect");

    // Disconnect worker
    h.disconnect_worker(&w1);
    h.converge().await;

    // Demand is 0 (service went idle), artifact is lost, workload should go Dormant.
    h.assert_workload_dormant("ns", "web");

    // Verify no placements remain for the disconnected worker.
    let remaining = h.orchestrator().placement_table.iter()
        .filter(|(_, p)| p.worker_id == w1)
        .count();
    assert_eq!(remaining, 0, "expected all placements cleared after worker disconnect");
}
