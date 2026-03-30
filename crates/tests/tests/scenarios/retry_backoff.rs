use std::time::Duration;

use distvirt_orchestrator::types::WorkloadStatus;
use distvirt_worker::vmm::test_vmm::TestVmm;

use crate::harness::TestCluster;
use crate::harness::spec_builders::always_on_spec;

/// Pod crashes after starting with exit code 1. Tests the retry loop.
///
/// Uses the container registry to trigger exits dynamically via pod handles,
/// exercising the real guest-init supervisor path.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_pod_exit_retry_loop_e2e() {
    let mut cluster = TestCluster::new();
    let _w1 = cluster.add_worker().await;

    cluster.create_namespace("ns", always_on_spec()).await;
    cluster.converge().await;

    // Pod is now running. Trigger exit(1) via the handle.
    cluster.assert_workload_running("ns", "echo").await;
    let handle = cluster.pod_handle("ns", "echo").await;
    handle.trigger_exit("main", 1).await;
    // After trigger_exit, the pod supervisor drains output and performs graceful
    // shutdown (with timeouts), then sends PodExited. wait_workload_status
    // advances time in 1s steps to drive through these timeouts.
    cluster
        .wait_workload_status("ns", "echo", WorkloadStatus::RetryBackoff)
        .await;

    // Advance past the retry backoff (5s configured in test harness) -> pod relaunches.
    cluster.advance_time(Duration::from_secs(6)).await;
    cluster.assert_workload_running("ns", "echo").await;

    // Trigger exit again on the relaunched pod.
    let handle = cluster.pod_handle("ns", "echo").await;
    handle.trigger_exit("main", 1).await;
    cluster
        .wait_workload_status("ns", "echo", WorkloadStatus::RetryBackoff)
        .await;
}

/// VM fails to start (guest dies before Ready) twice, then succeeds on the third attempt.
/// Tests the fail_before_ready simulation primitive and retry-to-success path.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_pod_launch_failure_recovery_e2e() {
    let mut cluster = TestCluster::new();
    let (vmm, _counter) = TestVmm::with_fail_then_run(2);
    let _w1 = cluster.add_worker_with_vmm(vmm).await;

    cluster.create_namespace("ns", always_on_spec()).await;
    cluster.converge().await;

    // 1st attempt fails → RetryBackoff.
    cluster.assert_workload_retry_backoff("ns", "echo").await;

    // Advance past first backoff → 2nd attempt also fails → RetryBackoff.
    cluster.advance_time(Duration::from_secs(2)).await;
    cluster.assert_workload_retry_backoff("ns", "echo").await;

    // Advance past second backoff → 3rd attempt succeeds.
    cluster.advance_time(Duration::from_secs(4)).await;
    cluster.assert_workload_running("ns", "echo").await;
}
