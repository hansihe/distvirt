use std::time::Duration;

use crate::harness::TestCluster;
use crate::harness::spec_builders::activation_spec;

/// Verifies that EndpointFlowStatus events for service endpoints are correctly
/// routed to the backing workload via the service_id field.
///
/// Previously, EndpointFlowStatus carried only the service IP, but the
/// orchestrator matched against pod IPs — so the event was silently ignored.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_endpoint_flow_status_via_service_id() {
    use distvirt_orchestrator::types::*;
    use distvirt_worker_protocol::WorkerEvent;

    let mut cluster = TestCluster::new();
    let w1 = cluster.add_worker().await;

    cluster
        .create_namespace("ns", activation_spec(Duration::from_secs(30)))
        .await;
    cluster.converge().await;
    cluster.assert_workload_dormant("ns", "web");

    // Activate the workload via traffic.
    cluster.send_activation_traffic("ns", "web-svc").await;
    cluster.assert_workload_running("ns", "web");

    // Inject EndpointFlowStatus with the service IP and service_id.
    // The orchestrator should resolve the workload via the service_id.
    let service_ip = cluster.service_ip("ns", "web-svc");
    cluster.shell.inject_worker_event(
        w1.clone(),
        WorkerEvent::EndpointFlowStatus {
            namespace_id: NamespaceId::from("ns"),
            ip: service_ip,
            service_id: Some(ServiceId::from("web-svc")),
            has_active_flows: false,
        },
    );
    cluster.converge().await;

    // The workload should have processed the flow status change.
    let ns = cluster.namespace("ns");
    let wl = ns.workloads.get(&WorkloadId("web".to_string())).unwrap();
    assert!(!wl.has_active_flows, "has_active_flows should be false after EndpointFlowStatus with service_id");
}
