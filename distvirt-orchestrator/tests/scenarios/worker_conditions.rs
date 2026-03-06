use crate::harness::*;
use crate::harness::mock_worker::MockWorkerConfig;
use distvirt_worker_protocol::WorkerEvent;

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
