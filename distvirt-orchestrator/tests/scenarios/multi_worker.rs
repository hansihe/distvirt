use crate::harness::*;

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
