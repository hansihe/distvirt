use std::net::Ipv4Addr;
use std::time::Duration;

use distvirt_orchestrator::types::NamespaceId;
use distvirt_worker_protocol::WorkerEvent;

use crate::harness::TestCluster;
use crate::harness::spec_builders::activation_spec;

/// A route-miss EndpointActivation (service_id: None) with the workload's pod IP
/// should activate a dormant workload.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_route_miss_activates_dormant_workload_e2e() {
    let mut cluster = TestCluster::new();
    let w1 = cluster.add_worker().await;

    cluster
        .create_namespace("ns", activation_spec(Duration::from_secs(30)))
        .await;
    cluster.converge().await;
    cluster.assert_workload_dormant("ns", "web");

    // Inject EndpointActivation with pod IP (not service IP), service_id: None.
    cluster.shell.inject_worker_event(
        w1.clone(),
        WorkerEvent::EndpointActivation {
            namespace_id: NamespaceId::from("ns"),
            ip: Ipv4Addr::new(172, 16, 0, 10),
            service_id: None,
        },
    );
    cluster.converge().await;

    cluster.assert_workload_running("ns", "web");
}

/// A route-miss EndpointActivation should also wake a suspended workload.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_route_miss_activates_suspended_workload_e2e() {
    let mut cluster = TestCluster::new();
    let w1 = cluster.add_worker().await;

    cluster
        .create_namespace("ns", activation_spec(Duration::from_secs(30)))
        .await;
    cluster.converge().await;
    cluster.assert_workload_dormant("ns", "web");

    // Full activation -> suspend cycle.
    cluster.send_activation_traffic("ns", "web-svc").await;
    cluster.assert_workload_running("ns", "web");

    cluster.deactivate_service("ns", "web-svc", &w1).await;
    cluster.advance_past_idle_timeout("ns", "web-svc").await;
    cluster.wait_workload_suspended("ns", "web").await;

    // Now inject a route-miss activation with pod IP.
    cluster.shell.inject_worker_event(
        w1.clone(),
        WorkerEvent::EndpointActivation {
            namespace_id: NamespaceId::from("ns"),
            ip: Ipv4Addr::new(172, 16, 0, 10),
            service_id: None,
        },
    );
    cluster.converge().await;

    cluster.assert_workload_running("ns", "web");
}
