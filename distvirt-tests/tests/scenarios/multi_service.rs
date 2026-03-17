use std::time::Duration;

use crate::harness::TestCluster;
use crate::harness::spec_builders::multi_service_activation_spec;

/// Two services on one workload — activating either starts the workload,
/// both must deactivate for idle.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_multi_service_shared_workload() {
    let mut cluster = TestCluster::new();
    let w1 = cluster.add_worker().await;

    cluster
        .create_namespace("ns", multi_service_activation_spec(Duration::from_secs(30)))
        .await;
    cluster.converge().await;
    cluster.assert_workload_dormant("ns", "shared");

    // Activate via svc-a.
    cluster.send_activation_traffic("ns", "svc-a").await;
    cluster.assert_workload_running("ns", "shared");
    cluster.assert_service_active("ns", "svc-a");

    // Deactivate svc-a. svc-b was never activated, so idle countdown should start.
    cluster.deactivate_service("ns", "svc-a", &w1).await;

    // Now activate svc-b — workload should stay running.
    cluster.send_activation_traffic("ns", "svc-b").await;
    cluster.assert_workload_running("ns", "shared");
    cluster.assert_service_active("ns", "svc-b");

    // Deactivate svc-b — now both services have no demand, idle countdown starts.
    cluster.deactivate_service("ns", "svc-b", &w1).await;

    // Advance past idle timeout.
    cluster.advance_time(Duration::from_secs(31)).await;
    cluster.wait_workload_suspended("ns", "shared").await;
}

/// When a second service activates while workload is already Running,
/// it should get WorkloadReady immediately.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_second_service_joins_running_workload() {
    let mut cluster = TestCluster::new();
    let _w1 = cluster.add_worker().await;

    cluster
        .create_namespace("ns", multi_service_activation_spec(Duration::from_secs(30)))
        .await;
    cluster.converge().await;

    // Activate via svc-a.
    cluster.send_activation_traffic("ns", "svc-a").await;
    cluster.assert_workload_running("ns", "shared");
    cluster.assert_service_active("ns", "svc-a");

    // Activate via svc-b — workload already running.
    cluster.send_activation_traffic("ns", "svc-b").await;
    cluster.assert_workload_running("ns", "shared");
    cluster.assert_service_active("ns", "svc-b");
    cluster.assert_service_active("ns", "svc-a");
}
