use crate::harness::*;

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
