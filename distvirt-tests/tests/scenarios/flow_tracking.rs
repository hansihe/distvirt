use std::time::Duration;

use distvirt_orchestrator::types::*;
use distvirt_worker_protocol::WorkerEvent;

use crate::harness::TestCluster;
use crate::harness::spec_builders::activation_spec;

/// Active flows should prevent a workload from being suspended even after
/// the service is deactivated. EndpointFlowStatus with has_active_flows=true
/// keeps the workload alive past the idle timeout.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_active_flows_prevent_suspend() {
    let mut cluster = TestCluster::new();
    let w1 = cluster.add_worker().await;

    cluster
        .create_namespace("ns", activation_spec(Duration::from_secs(30)))
        .await;
    cluster.converge().await;
    cluster.assert_workload_dormant("ns", "web");

    // Activate via traffic.
    cluster.send_activation_traffic("ns", "web-svc").await;
    cluster.assert_workload_running("ns", "web");

    // Report active flows on the service endpoint.
    let service_ip = cluster.service_ip("ns", "web-svc");
    cluster.shell.inject_worker_event(
        w1.clone(),
        WorkerEvent::EndpointFlowStatus {
            namespace_id: NamespaceId::from("ns"),
            ip: service_ip,
            service_id: Some(ServiceId::from("web-svc")),
            has_active_flows: true,
        },
    );
    cluster.converge().await;

    // Deactivate the service (no more new traffic demand).
    cluster.deactivate_service("ns", "web-svc", &w1).await;

    // Advance well past idle timeout — workload should remain Running
    // because active flows are keeping it alive.
    cluster.advance_time(Duration::from_secs(60)).await;
    cluster.assert_workload_running("ns", "web");
}

/// Clearing active flows after deactivation should allow the idle timeout
/// to proceed and eventually suspend the workload.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_flow_end_triggers_idle_timeout() {
    let mut cluster = TestCluster::new();
    let w1 = cluster.add_worker().await;
    let mut events = cluster.subscribe_events("ns");

    cluster
        .create_namespace("ns", activation_spec(Duration::from_secs(30)))
        .await;
    cluster.converge().await;
    cluster.assert_workload_dormant("ns", "web");

    // Activate via traffic.
    cluster.send_activation_traffic("ns", "web-svc").await;
    cluster.assert_workload_running("ns", "web");

    // Report active flows.
    let service_ip = cluster.service_ip("ns", "web-svc");
    cluster.shell.inject_worker_event(
        w1.clone(),
        WorkerEvent::EndpointFlowStatus {
            namespace_id: NamespaceId::from("ns"),
            ip: service_ip,
            service_id: Some(ServiceId::from("web-svc")),
            has_active_flows: true,
        },
    );
    cluster.converge().await;

    // Deactivate the service.
    cluster.deactivate_service("ns", "web-svc", &w1).await;

    // Advance past idle timeout — should still be running due to flows.
    cluster.advance_time(Duration::from_secs(35)).await;
    cluster.assert_workload_running("ns", "web");

    // Now clear active flows.
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

    // Advance past idle timeout again — now the workload should suspend.
    cluster.advance_past_idle_timeout("ns", "web-svc").await;
    cluster.wait_for_event(&mut events, |e| matches!(e,
        SmNamespaceEvent::Workload { workload_id, event: SmWorkloadEvent::PodSuspended { .. } }
        if workload_id.0 == "web"
    )).await;
    cluster.assert_workload_suspended("ns", "web");
}

/// BROKEN: active_flows is not cleared when the hosting worker disconnects.
/// The orchestrator should treat all flows from a disconnected worker as dead,
/// otherwise stale flow state could keep a workload alive indefinitely after
/// rescheduling to a new worker.
#[tokio::test(flavor = "current_thread", start_paused = true)]
#[should_panic(expected = "active_flows should be cleared after worker disconnect")]
async fn test_flow_status_cleared_on_worker_disconnect() {
    eprintln!("BROKEN: active_flows not cleared on worker disconnect — orchestrator needs to clear active_flows for workloads hosted on disconnected workers");
    let mut cluster = TestCluster::new();
    let _w1 = cluster.add_worker().await;
    let _w2 = cluster.add_worker().await;

    cluster
        .create_namespace("ns", activation_spec(Duration::from_secs(30)))
        .await;
    cluster.converge().await;
    cluster.assert_workload_dormant("ns", "web");

    // Activate via traffic.
    cluster.send_activation_traffic("ns", "web-svc").await;
    cluster.assert_workload_running("ns", "web");

    let hosting = cluster.worker_id_for_workload("ns", "web");

    // Report active flows on the hosting worker.
    let service_ip = cluster.service_ip("ns", "web-svc");
    cluster.shell.inject_worker_event(
        hosting.clone(),
        WorkerEvent::EndpointFlowStatus {
            namespace_id: NamespaceId::from("ns"),
            ip: service_ip,
            service_id: Some(ServiceId::from("web-svc")),
            has_active_flows: true,
        },
    );
    cluster.converge().await;

    // Verify the workload is tracked as having active flows.
    let ns = cluster.namespace("ns");
    assert!(
        ns.active_flows.contains(&WorkloadId("web".to_string())),
        "workload should have active flows"
    );

    // Disconnect the hosting worker.
    cluster.disconnect_worker(&hosting).await;

    // After disconnect, the active_flows for this workload should be cleared.
    // The workload will be rescheduled on w2.
    cluster.advance_time(Duration::from_secs(5)).await;

    let ns = cluster.namespace("ns");
    assert!(
        !ns.active_flows.contains(&WorkloadId("web".to_string())),
        "active_flows should be cleared after worker disconnect"
    );
}
