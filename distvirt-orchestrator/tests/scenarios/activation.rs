use std::time::Duration;

use crate::harness::*;
use crate::harness::mock_worker::MockWorkerConfig;

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_activation_idle_cycle() {
    let mut h = TestHarness::new();
    let _w1 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;
    h.assert_workload_dormant("ns", "web");
    h.assert_service_idle("ns", "web-svc");

    // Activate → running
    h.activate_service("ns", "web-svc").await;

    // Signal no more traffic
    h.deactivate_service("ns", "web-svc").await;

    // Advance past idle timeout
    h.advance_past_idle_timeout("ns", "web-svc").await;
    h.assert_workload_suspended("ns", "web");
    h.assert_service_idle("ns", "web-svc");
}
