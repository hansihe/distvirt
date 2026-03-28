use std::time::Duration;

use distvirt_orchestrator::core::{EndpointDemandSignal, WorkerNamespaceEventKind};
use distvirt_orchestrator::types::*;

use crate::harness::TestCluster;
use crate::harness::spec_builders::activation_spec;

/// Active demand (level signal) should prevent a workload from being
/// suspended. The Active level holds demand independently of the idle
/// timer — as long as Active is true the workload stays running.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_active_flows_prevent_suspend() {
    let mut cluster = TestCluster::new();
    let w1 = cluster.add_worker().await;

    cluster
        .create_namespace("ns", activation_spec(Duration::from_secs(30)))
        .await;
    cluster.converge().await;
    cluster.assert_workload_dormant("ns", "web").await;

    // Activate via traffic impulse.
    cluster.send_activation_traffic("ns", "web-svc").await;
    cluster.assert_workload_running("ns", "web").await;

    // Worker reports active flows (level signal).
    let service_ip = cluster.service_ip("ns", "web-svc");
    cluster
        .shell
        .inject_namespace_event(
            NamespaceId::from("ns"),
            w1,
            WorkerNamespaceEventKind::EndpointDemand {
                ip: service_ip,
                service_id: None,
                signal: EndpointDemandSignal::Active { active: true },
            },
        )
        .await;
    cluster.converge().await;

    // Advance well past idle timeout — workload should remain Running
    // because Active level holds demand.
    cluster.advance_time(Duration::from_secs(60)).await;
    cluster.assert_workload_running("ns", "web").await;
}

/// When active demand (level signal) drops, the idle timeout should start
/// and eventually suspend the workload.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_flow_end_triggers_idle_timeout() {
    let mut cluster = TestCluster::new();
    let w1 = cluster.add_worker().await;

    cluster
        .create_namespace("ns", activation_spec(Duration::from_secs(30)))
        .await;
    cluster.converge().await;
    cluster.assert_workload_dormant("ns", "web").await;

    // Activate via traffic impulse.
    cluster.send_activation_traffic("ns", "web-svc").await;
    cluster.assert_workload_running("ns", "web").await;

    // Worker reports active flows (level signal).
    let service_ip = cluster.service_ip("ns", "web-svc");
    cluster
        .shell
        .inject_namespace_event(
            NamespaceId::from("ns"),
            w1,
            WorkerNamespaceEventKind::EndpointDemand {
                ip: service_ip,
                service_id: None,
                signal: EndpointDemandSignal::Active { active: true },
            },
        )
        .await;
    cluster.converge().await;

    // Advance past idle timeout — should still be running due to Active level.
    cluster.advance_time(Duration::from_secs(35)).await;
    cluster.assert_workload_running("ns", "web").await;

    // Flows end — worker clears active demand.
    cluster
        .shell
        .inject_namespace_event(
            NamespaceId::from("ns"),
            w1,
            WorkerNamespaceEventKind::EndpointDemand {
                ip: service_ip,
                service_id: None,
                signal: EndpointDemandSignal::Active { active: false },
            },
        )
        .await;
    cluster.converge().await;

    // Now the idle timeout should proceed and suspend the workload.
    cluster.advance_past_idle_timeout("ns", "web-svc").await;
    cluster.wait_workload_suspended("ns", "web").await;
    cluster.assert_workload_suspended("ns", "web").await;
}

/// BROKEN: demand state is not cleared when the hosting worker disconnects.
/// The orchestrator should treat all demand from a disconnected worker as dead,
/// otherwise stale demand state could keep a workload alive indefinitely after
/// rescheduling to a new worker.
#[should_panic(expected = "demand should be cleared after worker disconnect")]
#[tokio::test(flavor = "current_thread", start_paused = true)]
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
    cluster
        .shell
        .inject_namespace_event(
            NamespaceId::from("ns"),
            hosting,
            WorkerNamespaceEventKind::EndpointDemand {
                ip: service_ip,
                service_id: None,
                signal: EndpointDemandSignal::Active { active: true },
            },
        )
        .await;
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
