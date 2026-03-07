use std::time::Duration;

use crate::harness::*;
use crate::harness::mock_worker::MockWorkerConfig;
use distvirt_orchestrator::types::*;

/// Basic drain: set draining condition, verify it's visible, undrain clears it.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_drain_undrain_condition() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    h.create_namespace("ns", activation_spec(Duration::from_secs(30))).await;
    h.converge().await;

    // Drain the worker.
    h.drain_worker(&w1).await;
    h.assert_worker_draining(&w1);

    // Undrain the worker.
    h.undrain_worker(&w1).await;
    h.assert_worker_not_draining(&w1);
}

/// Draining worker is excluded from scheduling: new activations go to a non-draining worker.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_drain_excludes_from_scheduling() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    let w2 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    h.create_namespace("ns", activation_spec(Duration::from_secs(30))).await;
    h.converge().await;

    // Drain w1.
    h.drain_worker(&w1).await;

    // Activate the service — workload should be scheduled on w2 (not w1).
    h.activate_service("ns", "web-svc").await;
    h.assert_workload_running("ns", "web");

    let wl_state = h.workload_state("ns", "web");
    let worker_id = wl_state.worker_id().expect("workload should have a worker");
    assert_eq!(
        *worker_id, w2,
        "workload should be scheduled on non-draining worker w2, got {:?}", worker_id
    );
}

/// Existing pods on a draining worker continue running — drain doesn't force-stop them.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_drain_does_not_stop_existing_pods() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    h.create_namespace("ns", activation_spec(Duration::from_secs(30))).await;
    h.converge().await;

    // Activate the workload on w1.
    h.activate_service("ns", "web-svc").await;
    h.assert_workload_running("ns", "web");

    // Drain w1 — pod should still be running.
    h.drain_worker(&w1).await;
    h.assert_workload_running("ns", "web");
}

/// After drain, pods deactivate on their normal idle timeout.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_drain_pods_deactivate_on_idle_timeout() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    h.create_namespace("ns", activation_spec(Duration::from_secs(30))).await;
    h.converge().await;

    // Activate the workload.
    h.activate_service("ns", "web-svc").await;
    h.assert_workload_running("ns", "web");

    // Drain the worker.
    h.drain_worker(&w1).await;

    // Signal no more traffic, then advance past idle timeout.
    h.deactivate_service("ns", "web-svc").await;
    h.advance_past_idle_timeout("ns", "web-svc").await;

    // Workload should have suspended (suspend_on_idle=true).
    h.assert_workload_suspended("ns", "web");
}

/// When a draining worker is the only one and all pods drain, scheduling fails gracefully
/// (workload goes to WaitingForCapacity on re-activation).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_drain_single_worker_no_scheduling() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    h.create_namespace("ns", activation_spec(Duration::from_secs(30))).await;
    h.converge().await;

    // Drain the only worker.
    h.drain_worker(&w1).await;

    // Try to activate — workload should be stuck in WaitingForCapacity.
    let svc_ip = h.service_ip("ns", "web-svc");
    h.worker(&w1).send_event(distvirt_worker_protocol::WorkerEvent::EndpointActivation {
        namespace_id: "ns".into(),
        ip: svc_ip,
        service_id: Some(ServiceId::from("web-svc")),
    });
    h.converge().await;
    h.assert_workload_waiting_for_capacity("ns", "web");

    // Undrain — workload should now be schedulable.
    h.undrain_worker(&w1).await;
    // Trigger scheduling by sending a pressure update.
    h.send_pressure_update(&w1, 0.0).await;
    h.assert_workload_running("ns", "web");
}
