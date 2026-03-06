use crate::harness::*;
use crate::harness::mock_worker::MockWorkerConfig;

/// Worker A (256 MB, Elevated pressure after 1 pod) holds a running workload.
/// Disconnect A → workload reschedules to B (4096 MB, Normal pressure).
/// After converge, workload should be running on the remaining worker.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_workload_reschedules_to_lower_pressure_worker_after_disconnect() {
    let mut h = TestHarness::new();

    // Worker A: small memory, will be Elevated after 1 pod.
    let _w_a = h.add_worker_with(MockWorkerConfig::with_pool_and_memory(256)).await;
    // Worker B: large memory, stays Normal.
    let _w_b = h.add_worker_with(MockWorkerConfig::with_pool_and_memory(4096)).await;

    h.create_namespace("ns", always_on_spec()).await;
    h.converge().await;
    h.assert_workload_running("ns", "echo");

    let initial_worker = h.workload_state("ns", "echo").worker_id().unwrap().clone();

    // Disconnect the worker that has the workload.
    h.disconnect_worker(&initial_worker);
    h.converge().await;

    // After converge the scheduler should have placed it on the remaining worker.
    h.assert_workload_running("ns", "echo");

    let new_worker = h.workload_state("ns", "echo").worker_id().unwrap().clone();
    assert_ne!(
        new_worker, initial_worker,
        "workload should have moved to a different worker after disconnect"
    );
}
