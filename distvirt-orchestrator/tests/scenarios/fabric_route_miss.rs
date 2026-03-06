use std::net::Ipv4Addr;
use std::time::Duration;

use crate::harness::mock_worker::MockWorkerConfig;
use crate::harness::*;
use distvirt_worker_protocol::{BackendNeed, ServiceId, WorkerCommand, WorkerEvent};

/// FabricRouteMiss on a Dormant workload should activate it (LaunchPod).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_route_miss_activates_dormant_workload() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker().await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;
    h.assert_workload_dormant("ns", "web");

    // Low-level: FabricRouteMiss targets the pod IP (not service IP), must use explicit IP
    h.worker(&w1).send_event(WorkerEvent::FabricRouteMiss {
        namespace_id: "ns".into(),
        dst_ip: Ipv4Addr::new(172, 16, 0, 10),
    });
    h.converge().await;

    h.assert_workload_running("ns", "web");
    h.assert_worker_received_command_matching(&w1, "LaunchPod", |cmd| {
        matches!(cmd, WorkerCommand::LaunchPod { .. })
    });
}

/// FabricRouteMiss on a Suspended workload should resume it (ResumePod).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_route_miss_activates_suspended_workload() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;
    h.assert_workload_dormant("ns", "web");

    // Activate via service → run → idle → suspend
    h.activate_service("ns", "web-svc").await;
    h.deactivate_service("ns", "web-svc").await;
    h.advance_past_idle_timeout("ns", "web-svc").await;
    h.assert_workload_suspended("ns", "web");

    // Low-level: FabricRouteMiss targets the pod IP (not service IP)
    h.worker(&w1).send_event(WorkerEvent::FabricRouteMiss {
        namespace_id: "ns".into(),
        dst_ip: Ipv4Addr::new(172, 16, 0, 10),
    });
    h.converge().await;

    h.assert_workload_running("ns", "web");
    h.assert_worker_received_command_matching(&w1, "ResumePod", |cmd| {
        matches!(cmd, WorkerCommand::ResumePod { .. })
    });
}

/// FabricRouteMiss on an already-running workload should be a no-op.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_route_miss_ignored_when_already_running() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker().await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;

    // Activate via service first
    h.activate_service("ns", "web-svc").await;

    // Low-level: command window slicing to verify no new commands after route miss
    let cmds_before = h.worker(&w1).commands().len();

    h.worker(&w1).send_event(WorkerEvent::FabricRouteMiss {
        namespace_id: "ns".into(),
        dst_ip: Ipv4Addr::new(172, 16, 0, 10),
    });
    h.converge().await;

    h.assert_workload_running("ns", "web");
    let cmds_after = h.worker(&w1).commands();
    let new_launches = cmds_after[cmds_before..]
        .iter()
        .filter(|c| matches!(c, WorkerCommand::LaunchPod { .. }))
        .count();
    assert_eq!(
        new_launches, 0,
        "no new LaunchPod should be issued when already running"
    );
}

/// FabricRouteMiss for an IP that doesn't match any workload should be ignored.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_route_miss_ignored_for_unknown_ip() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker().await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;
    h.assert_workload_dormant("ns", "web");

    // Low-level: testing with an IP that doesn't match any workload
    h.worker(&w1).send_event(WorkerEvent::FabricRouteMiss {
        namespace_id: "ns".into(),
        dst_ip: Ipv4Addr::new(172, 16, 0, 99),
    });
    h.converge().await;

    h.assert_workload_dormant("ns", "web");
}

/// BUG: `route_miss_wake` flag is never cleared, causing a demand leak.
#[tokio::test(flavor = "current_thread", start_paused = true)]
#[should_panic]
async fn test_route_miss_demand_leak() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;
    h.assert_workload_dormant("ns", "web");

    // Low-level: testing exact demand leak behavior with route miss + service activation interaction
    // Step 1: FabricRouteMiss activates the workload.
    h.worker(&w1).send_event(WorkerEvent::FabricRouteMiss {
        namespace_id: "ns".into(),
        dst_ip: Ipv4Addr::new(172, 16, 0, 10),
    });
    h.converge().await;
    h.assert_workload_running("ns", "web");

    // Step 2: ServiceActivation arrives (real traffic hits the service IP).
    let svc_ip = h.service_ip("ns", "web-svc");
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        dst_ip: svc_ip,
    });
    h.converge().await;
    h.assert_workload_running("ns", "web");
    h.assert_service_active("ns", "web-svc");

    // Step 3: Signal no more traffic → start idle timer.
    h.worker(&w1).send_event(WorkerEvent::ServiceBackendNeed {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        need: BackendNeed::None,
    });
    h.converge().await;

    // Step 4: Advance past idle timeout.
    h.advance_past_idle_timeout("ns", "web-svc").await;
    h.assert_service_idle("ns", "web-svc");
    h.assert_workload_suspended("ns", "web");
}
