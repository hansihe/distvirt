use std::time::Duration;

use distvirt_worker::vmm::guest_sim::ContainerBehavior;
use distvirt_worker::vmm::test_vmm::TestVmm;

use crate::harness::TestCluster;
use crate::harness::spec_builders::{always_on_spec, no_activation_spec};

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_always_on_pod_lifecycle() {
    let mut cluster = TestCluster::new();
    let _w1 = cluster.add_worker().await;

    cluster.create_namespace("ns", always_on_spec()).await;
    cluster.converge().await;
    cluster.assert_workload_running("ns", "echo").await;

    cluster.delete_namespace("ns").await;
    cluster.converge().await;
    cluster.assert_namespace_absent("ns").await;
}

/// Workload with respects_demand=false, no services, no activation.
/// Should auto-start without any demand signal.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_no_activation_workload_starts() {
    let mut cluster = TestCluster::new();
    let _w1 = cluster.add_worker().await;

    cluster.create_namespace("ns", no_activation_spec()).await;
    cluster.converge().await;
    cluster.assert_workload_running("ns", "web").await;
}

/// When a no-activation workload's pod fails to launch, it should enter
/// retry_backoff — NOT stay dormant.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_no_activation_launch_failure_retries() {
    let mut cluster = TestCluster::new();
    let (vmm, _counter) = TestVmm::with_fail_then_run(2);
    let _w1 = cluster.add_worker_with_vmm(vmm).await;

    cluster.create_namespace("ns", no_activation_spec()).await;
    cluster.converge().await;

    // 1st attempt fails → should be in retry_backoff, NOT dormant.
    cluster.assert_workload_retry_backoff("ns", "web").await;

    // Advance past first backoff → 2nd attempt also fails → retry_backoff.
    cluster.advance_time(Duration::from_secs(2)).await;
    cluster.assert_workload_retry_backoff("ns", "web").await;

    // Advance past second backoff → 3rd attempt succeeds.
    cluster.advance_time(Duration::from_secs(4)).await;
    cluster.assert_workload_running("ns", "web").await;
}
