use crate::harness::*;

// BUG: When a new worker joins an already-Active namespace, `NamespaceCreated`
// (namespace/events.rs line ~57) only emits registry/route sync to the new worker.
// It does NOT re-emit `pod_requests` for workloads in `WaitingForCapacity`, and
// `schedule_waiting_pods` (called during `handle_worker_connected`) runs before the
// new worker's fabric transitions to Active, so `select_worker_for_pod` finds no
// eligible worker.
//
// Fix: after NamespaceCreated transitions fabric to Active on an already-Active
// namespace, emit `PodRequest` for each WaitingForCapacity workload so the
// orchestrator's `process_namespace_output` can schedule them.
#[tokio::test]
#[should_panic(expected = "expected Running, got WaitingForCapacity")]
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
