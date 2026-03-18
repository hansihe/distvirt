use std::time::Duration;

use crate::harness::TestCluster;
use crate::harness::spec_builders::activation_spec;

/// Full activation cycle: real traffic triggers activation, injected event
/// deactivates, idle timeout suspends.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_activation_lifecycle_e2e() {
    let mut cluster = TestCluster::new();
    let w1 = cluster.add_worker().await;

    cluster
        .create_namespace("ns", activation_spec(Duration::from_secs(30)))
        .await;
    cluster.converge().await;
    cluster.assert_workload_dormant("ns", "web").await;

    // TCP SYN via internet_tx -> fabric -> EndpointActivation -> orchestrator launches pod.
    cluster.send_activation_traffic("ns", "web-svc").await;
    cluster.assert_workload_running("ns", "web").await;
    cluster.assert_service_active("ns", "web-svc").await;

    // Inject ServiceBackendNeed::None (WASM activator not available in tests).
    cluster.deactivate_service("ns", "web-svc", &w1).await;

    // Advance past idle timeout -> workload should suspend.
    cluster.advance_past_idle_timeout("ns", "web-svc").await;
    cluster.wait_workload_suspended("ns", "web").await;
    cluster.assert_workload_suspended("ns", "web").await;
    cluster.assert_service_idle("ns", "web-svc").await;
}
