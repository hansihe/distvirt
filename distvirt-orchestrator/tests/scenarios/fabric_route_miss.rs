use std::net::Ipv4Addr;
use std::time::Duration;

use crate::harness::*;
use crate::harness::mock_worker::MockWorkerConfig;
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

    // Send FabricRouteMiss targeting the workload's pod IP
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
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 100),
    });
    h.converge().await;
    h.assert_workload_running("ns", "web");

    h.worker(&w1).send_event(WorkerEvent::ServiceBackendNeed {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        need: BackendNeed::None,
    });
    h.converge().await;
    h.advance_time(timeout + Duration::from_secs(1)).await;
    h.assert_workload_suspended("ns", "web");

    // Now send FabricRouteMiss while suspended
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
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 100),
    });
    h.converge().await;
    h.assert_workload_running("ns", "web");

    let cmds_before = h.worker(&w1).commands().len();

    // Send FabricRouteMiss while already running
    h.worker(&w1).send_event(WorkerEvent::FabricRouteMiss {
        namespace_id: "ns".into(),
        dst_ip: Ipv4Addr::new(172, 16, 0, 10),
    });
    h.converge().await;

    h.assert_workload_running("ns", "web");
    // No new LaunchPod should have been issued
    let cmds_after = h.worker(&w1).commands();
    let new_launches = cmds_after[cmds_before..]
        .iter()
        .filter(|c| matches!(c, WorkerCommand::LaunchPod { .. }))
        .count();
    assert_eq!(new_launches, 0, "no new LaunchPod should be issued when already running");
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

    // Send FabricRouteMiss for an IP that doesn't match any workload
    h.worker(&w1).send_event(WorkerEvent::FabricRouteMiss {
        namespace_id: "ns".into(),
        dst_ip: Ipv4Addr::new(172, 16, 0, 99),
    });
    h.converge().await;

    // Workload should remain dormant
    h.assert_workload_dormant("ns", "web");
}

/// BUG DOCUMENTATION (partially fixed): FabricRouteMiss sends DemandUp directly to
/// the workload SM with no corresponding DemandDown source.
///
/// **Fixed**: Late-joiner WorkloadReady now notifies services when DemandUp arrives
/// on an already-Running workload, so the service correctly transitions to Active.
///
/// **Remaining bug**: Orphaned demand from FabricRouteMiss. demand_count gets
/// permanently elevated by 1, so even when all services go idle and fire DemandDown,
/// the workload never reaches demand_count=0 and thus never suspends/goes dormant.
/// Correct behavior: workload should suspend when all services are idle, regardless
/// of FabricRouteMiss history.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_route_miss_demand_leak() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;
    h.assert_workload_dormant("ns", "web");

    // Step 1: FabricRouteMiss activates the workload.
    // This sends DemandUp to the workload SM. demand_count = 1.
    // There is no mechanism to ever send a corresponding DemandDown.
    h.worker(&w1).send_event(WorkerEvent::FabricRouteMiss {
        namespace_id: "ns".into(),
        dst_ip: Ipv4Addr::new(172, 16, 0, 10),
    });
    h.converge().await;
    h.assert_workload_running("ns", "web");

    // Step 2: ServiceActivation arrives (e.g. real traffic hits the service IP).
    // Service SM goes Idle → NeedBackend → sends another DemandUp. demand_count = 2.
    // Fixed: Late-joiner WorkloadReady notifies the service, so it transitions to Active.
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 100),
    });
    h.converge().await;
    h.assert_workload_running("ns", "web");
    // Fixed: service is now Active (previously stuck in NeedBackend).
    h.assert_service_active("ns", "web-svc");

    // Step 3: Advance well past the idle timeout.
    // The service is Active with backend_need=Active, no idle timer fires
    // (would need BackendNeed::None to start idle timer).
    h.advance_time(timeout * 3).await;

    // BUG (remaining): workload is still Running because demand_count == 2
    // (one orphaned from FabricRouteMiss, one from the service).
    // Correct behavior: workload should be Suspended after all services are idle.
    h.assert_workload_running("ns", "web");
}
