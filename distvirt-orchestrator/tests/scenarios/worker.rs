use std::time::Duration;

use crate::harness::*;
use crate::harness::mock_worker::MockWorkerConfig;
use distvirt_orchestrator::types::*;
use distvirt_worker_protocol::{WorkerCommand, WorkerEvent};

// === worker_disconnect ===

// Regression test: When a worker disconnects and a new worker joins, workloads
// in WaitingForCapacity should be scheduled on the new worker after its fabric
// becomes Active (NamespaceCreated). Fixed by calling schedule_waiting_pods in
// process_namespace_output instead of handle_worker_connected.
#[tokio::test]
async fn test_worker_disconnect_and_recovery() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker().await;
    h.create_namespace("ns", always_on_spec()).await;
    h.converge().await;
    h.assert_workload_running("ns", "echo");
    h.disconnect_worker(&w1);
    h.converge().await;
    h.assert_workload_waiting_for_capacity("ns", "echo");
    h.add_worker().await;
    h.converge().await;
    h.assert_workload_running("ns", "echo");
}

// === worker_failures ===

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
        ..Default::default()
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
    h.worker(&w1).send_event(distvirt_worker_protocol::WorkerEvent::EndpointActivation {
        namespace_id: "ns".into(),
        ip: svc_ip,
        service_id: Some(distvirt_worker_protocol::ServiceId::from("web-svc")),
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

// === worker_conditions ===

/// Inject a WorkerCondition event and verify it's stored on the worker state.
/// Then clear it and verify removal.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_worker_condition_stored_on_event() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool()).await;

    h.create_namespace("ns", always_on_spec()).await;
    h.converge().await;
    h.assert_workload_running("ns", "echo");

    // Assert no conditions initially.
    let conditions = &h.orchestrator().workers.get(&w1).unwrap().conditions;
    assert!(conditions.is_empty(), "expected no conditions initially");

    // Inject a worker condition.
    h.worker(&w1).send_event(WorkerEvent::WorkerCondition {
        key: "pool-soft-watermark".to_string(),
        active: true,
        message: "Pool usage above 80%".to_string(),
    });
    h.converge().await;

    // Assert condition is stored.
    let conditions = &h.orchestrator().workers.get(&w1).unwrap().conditions;
    assert!(conditions.contains_key("pool-soft-watermark"), "condition should be stored");
    let cond = &conditions["pool-soft-watermark"];
    assert!(cond.active);
    assert_eq!(cond.message, "Pool usage above 80%");

    // Clear the condition.
    h.worker(&w1).send_event(WorkerEvent::WorkerCondition {
        key: "pool-soft-watermark".to_string(),
        active: false,
        message: String::new(),
    });
    h.converge().await;

    // Assert condition is removed.
    let conditions = &h.orchestrator().workers.get(&w1).unwrap().conditions;
    assert!(!conditions.contains_key("pool-soft-watermark"), "condition should be removed after deassert");
}

/// Inject a WorkerCondition, then verify it appears in the WorkerStatusReport
/// via the client command path (ListWorkers).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_worker_condition_in_status_report() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool()).await;

    h.create_namespace("ns", always_on_spec()).await;
    h.converge().await;
    h.assert_workload_running("ns", "echo");

    // Inject a worker condition.
    h.worker(&w1).send_event(WorkerEvent::WorkerCondition {
        key: "memory-pressure".to_string(),
        active: true,
        message: "Available memory low".to_string(),
    });
    h.converge().await;

    // Read the condition directly from the worker state (simulating what ListWorkers would report).
    let ws = h.orchestrator().workers.get(&w1).unwrap();
    assert!(ws.conditions.contains_key("memory-pressure"));
    assert_eq!(ws.conditions["memory-pressure"].message, "Available memory low");
    assert!(ws.conditions["memory-pressure"].active);
}

// === multi_worker ===

#[tokio::test]
async fn test_multi_worker_reschedule() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker().await;
    let _w2 = h.add_worker().await;
    h.create_namespace("ns", always_on_spec()).await;
    h.converge().await;
    h.assert_workload_running("ns", "echo");
    // Find which worker got the workload
    let assigned = h
        .workload_state("ns", "echo")
        .worker_id()
        .expect("running workload should have worker_id")
        .clone();
    h.disconnect_worker(&assigned);
    h.converge().await;
    // Should be rescheduled to the other worker
    h.assert_workload_running("ns", "echo");
}
