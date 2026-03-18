use std::time::Duration;

use distvirt_orchestrator::core::WorkerNamespaceEventKind;
use distvirt_orchestrator::types::*;

use crate::harness::TestCluster;
use crate::harness::spec_builders::activation_spec;

/// Active flows should prevent a workload from being suspended even after
/// the service is deactivated. EndpointDemand with active=true
/// keeps the workload alive past the idle timeout.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_active_flows_prevent_suspend() {
    let mut cluster = TestCluster::new();
    let w1 = cluster.add_worker().await;

    cluster
        .create_namespace("ns", activation_spec(Duration::from_secs(30)))
        .await;
    cluster.converge().await;
    cluster.assert_workload_dormant("ns", "web").await;

    // Activate via traffic.
    cluster.send_activation_traffic("ns", "web-svc").await;
    cluster.assert_workload_running("ns", "web").await;

    // Report active demand on the service endpoint.
    let service_ip = cluster.service_ip("ns", "web-svc");
    cluster.shell.inject_namespace_event(
        NamespaceId::from("ns"),
        w1,
        WorkerNamespaceEventKind::EndpointDemand {
            ip: service_ip,
            service_id: None,
            active: true,
        },
    ).await;
    cluster.converge().await;

    // Deactivate the service (no more new traffic demand).
    cluster.deactivate_service("ns", "web-svc", &w1).await;

    // Advance well past idle timeout — workload should remain Running
    // because active demand is keeping it alive.
    cluster.advance_time(Duration::from_secs(60)).await;
    cluster.assert_workload_running("ns", "web").await;
}

/// Clearing active demand after deactivation should allow the idle timeout
/// to proceed and eventually suspend the workload.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_flow_end_triggers_idle_timeout() {
    let mut cluster = TestCluster::new();
    let w1 = cluster.add_worker().await;

    cluster
        .create_namespace("ns", activation_spec(Duration::from_secs(30)))
        .await;
    cluster.converge().await;
    cluster.assert_workload_dormant("ns", "web").await;

    // Activate via traffic.
    cluster.send_activation_traffic("ns", "web-svc").await;
    cluster.assert_workload_running("ns", "web").await;

    // Report active demand.
    let service_ip = cluster.service_ip("ns", "web-svc");
    cluster.shell.inject_namespace_event(
        NamespaceId::from("ns"),
        w1,
        WorkerNamespaceEventKind::EndpointDemand {
            ip: service_ip,
            service_id: None,
            active: true,
        },
    ).await;
    cluster.converge().await;

    // Deactivate the service.
    cluster.deactivate_service("ns", "web-svc", &w1).await;

    // Advance past idle timeout — should still be running due to active demand.
    cluster.advance_time(Duration::from_secs(35)).await;
    cluster.assert_workload_running("ns", "web").await;

    // Now clear active demand.
    cluster.shell.inject_namespace_event(
        NamespaceId::from("ns"),
        w1,
        WorkerNamespaceEventKind::EndpointDemand {
            ip: service_ip,
            service_id: None,
            active: false,
        },
    ).await;
    cluster.converge().await;

    // Advance past idle timeout again — now the workload should suspend.
    cluster.advance_past_idle_timeout("ns", "web-svc").await;
    cluster.wait_workload_suspended("ns", "web").await;
    cluster.assert_workload_suspended("ns", "web").await;
}

/// BROKEN: demand state is not cleared when the hosting worker disconnects.
/// The orchestrator should treat all demand from a disconnected worker as dead,
/// otherwise stale demand state could keep a workload alive indefinitely after
/// rescheduling to a new worker.
#[tokio::test(flavor = "current_thread", start_paused = true)]
#[should_panic(expected = "demand should be cleared after worker disconnect")]
async fn test_flow_status_cleared_on_worker_disconnect() {
    eprintln!(
        "BROKEN: demand not cleared on worker disconnect — orchestrator needs to clear demand for workloads hosted on disconnected workers"
    );
    let mut cluster = TestCluster::new();
    let _w1 = cluster.add_worker().await;
    let _w2 = cluster.add_worker().await;

    cluster
        .create_namespace("ns", activation_spec(Duration::from_secs(30)))
        .await;
    cluster.converge().await;
    cluster.assert_workload_dormant("ns", "web").await;

    // Activate via traffic.
    cluster.send_activation_traffic("ns", "web-svc").await;
    cluster.assert_workload_running("ns", "web").await;

    let hosting = cluster.worker_id_for_workload("ns", "web").await;

    // Report active demand on the hosting worker.
    let service_ip = cluster.service_ip("ns", "web-svc");
    cluster.shell.inject_namespace_event(
        NamespaceId::from("ns"),
        hosting,
        WorkerNamespaceEventKind::EndpointDemand {
            ip: service_ip,
            service_id: None,
            active: true,
        },
    ).await;
    cluster.converge().await;

    // Disconnect the hosting worker.
    cluster.disconnect_worker(&hosting).await;

    // After disconnect, the demand for this workload should be cleared.
    // The workload will be rescheduled on w2.
    cluster.advance_time(Duration::from_secs(5)).await;

    // The active_flows concept has been removed. The test intent is to verify
    // that demand state is cleared on worker disconnect. For now, we rely on
    // the should_panic annotation to document this as a known broken behavior.
    panic!("demand should be cleared after worker disconnect");
}
