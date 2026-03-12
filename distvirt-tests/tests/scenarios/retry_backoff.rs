use std::time::Duration;

use distvirt_orchestrator::types::*;
use distvirt_worker::vmm::guest_sim::ContainerBehavior;
use distvirt_worker::vmm::test_vmm::TestVmm;

use crate::harness::TestCluster;
use crate::harness::spec_builders::always_on_spec;

/// Worker with ExitImmediately(1) causes rapid exit. Tests the retry loop.
///
/// ExitImmediately(1) produces PodRunning then PodExited(1). Since PodRunning
/// resets consecutive_failures, failures never accumulate past 1. This tests
/// the "pod crashes after starting" retry loop (pod keeps getting relaunched).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_pod_exit_retry_loop_e2e() {
    let mut cluster = TestCluster::new();
    let _w1 = cluster
        .add_worker_with(ContainerBehavior::ExitImmediately(1))
        .await;

    cluster.create_namespace("ns", always_on_spec()).await;
    cluster.converge().await;

    // Pod launches, runs briefly, exits(1) -> RetryBackoff.
    cluster.assert_workload_retry_backoff("ns", "echo");

    // Advance time to let the retry fire -> pod relaunches, exits again.
    cluster.advance_time(Duration::from_secs(2)).await;

    // The workload should be back in RetryBackoff (not stuck in Failed or Dormant),
    // verifying the retry cycle continues.
    cluster.assert_workload_retry_backoff("ns", "echo");
}

/// VM fails to start (guest dies before Ready) twice, then succeeds on the third attempt.
/// Tests the fail_before_ready simulation primitive and retry-to-success path.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_pod_launch_failure_recovery_e2e() {
    let mut cluster = TestCluster::new();
    let (vmm, _counter) = TestVmm::with_fail_then_run(2);
    let _w1 = cluster.add_worker_with_vmm(vmm).await;
    let mut events = cluster.subscribe_events("ns");

    cluster.create_namespace("ns", always_on_spec()).await;
    cluster.converge().await;

    // 1st attempt fails → RetryBackoff.
    cluster.assert_workload_retry_backoff("ns", "echo");

    // Advance past first backoff → 2nd attempt also fails → RetryBackoff.
    cluster.advance_time(Duration::from_secs(2)).await;
    cluster.assert_workload_retry_backoff("ns", "echo");

    // Advance past second backoff → 3rd attempt succeeds → wait for PodRunning event.
    tokio::time::advance(Duration::from_secs(4)).await;
    cluster.wait_for_event(&mut events, |e| matches!(e,
        SmNamespaceEvent::Workload { workload_id, event: SmWorkloadEvent::PodRunning { .. } }
        if workload_id.0 == "echo"
    )).await;
    cluster.assert_workload_running("ns", "echo");
}
