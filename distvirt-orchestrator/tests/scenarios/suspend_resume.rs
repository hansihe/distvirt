use std::net::Ipv4Addr;
use std::time::Duration;

use crate::harness::*;
use crate::harness::mock_worker::MockWorkerConfig;
use distvirt_worker_protocol::{BackendNeed, ServiceId, WorkerCommand, WorkerEvent};

/// Full cycle: activate → run → idle → suspend → re-activate → resume → running.
/// Verify the resume path uses ResumePod (not LaunchPod).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_resume_from_suspended() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;
    h.assert_workload_dormant("ns", "web");

    // Activate
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 100),
    });
    h.converge().await;
    h.assert_workload_running("ns", "web");

    // Idle → suspend
    h.worker(&w1).send_event(WorkerEvent::ServiceBackendNeed {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        need: BackendNeed::None,
    });
    h.converge().await;
    h.advance_time(timeout + Duration::from_secs(1)).await;
    h.assert_workload_suspended("ns", "web");

    // Re-activate → should resume (not cold launch)
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 100),
    });
    h.converge().await;
    h.assert_workload_running("ns", "web");

    // Verify ResumePod was sent (not LaunchPod after the first one)
    let cmds = h.worker(&w1).commands();
    let resume_count = cmds.iter().filter(|c| matches!(c, WorkerCommand::ResumePod { .. })).count();
    assert!(resume_count >= 1, "expected at least one ResumePod command, got {}", resume_count);
}

/// Use suspend_hang handler. Activate → run → idle → suspending → advance past SUSPEND_TIMEOUT.
/// Workload should fall back (StopPod issued).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_suspend_timeout_fallback_to_stop() {
    let config = MockWorkerConfig::with_suspend_hang().add_pool();
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(config).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;

    // Activate → running
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 100),
    });
    h.converge().await;
    h.assert_workload_running("ns", "web");

    // Idle → begin suspending
    h.worker(&w1).send_event(WorkerEvent::ServiceBackendNeed {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        need: BackendNeed::None,
    });
    h.converge().await;
    h.advance_time(timeout + Duration::from_secs(1)).await;
    // Should be in Suspending (handler doesn't respond)
    h.assert_workload_suspending("ns", "web");

    // Advance past suspend timeout (30s)
    h.advance_time(Duration::from_secs(31)).await;

    // After suspend timeout, the orchestrator issues StopPod and the workload
    // transitions to Dormant (demand is 0, service went idle).
    h.assert_workload_dormant("ns", "web");

    // StopPod should have been issued
    let cmds = h.worker(&w1).commands();
    let stop_count = cmds.iter().filter(|c| matches!(c, WorkerCommand::StopPod { .. })).count();
    assert!(stop_count >= 1, "expected StopPod after suspend timeout");
}

/// Worker returns PodSuspendFailed. Workload should transition appropriately.
/// StopPod should be issued.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_suspend_failure_fallback_to_stop() {
    let config = MockWorkerConfig::with_suspend_failure().add_pool();
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(config).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;

    // Activate → running
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 100),
    });
    h.converge().await;
    h.assert_workload_running("ns", "web");

    // Idle → suspend attempt (fails immediately)
    h.worker(&w1).send_event(WorkerEvent::ServiceBackendNeed {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        need: BackendNeed::None,
    });
    h.converge().await;
    h.advance_time(timeout + Duration::from_secs(1)).await;

    // PodSuspendFailed triggers StopPod fallback. With demand at 0 (service went idle),
    // the workload transitions to Dormant.
    h.assert_workload_dormant("ns", "web");
}

/// Use activation_no_suspend_spec. Activate → run → idle → stop (not suspend).
/// Re-activate → cold start (LaunchPod, not ResumePod).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_activation_no_suspend_cold_start() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_no_suspend_spec(timeout)).await;
    h.converge().await;
    h.assert_workload_dormant("ns", "web");

    // Activate
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 100),
    });
    h.converge().await;
    h.assert_workload_running("ns", "web");

    // Idle → stop (not suspend, because suspend_on_idle=false)
    h.worker(&w1).send_event(WorkerEvent::ServiceBackendNeed {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        need: BackendNeed::None,
    });
    h.converge().await;
    h.advance_time(timeout + Duration::from_secs(1)).await;

    // Should be Dormant (not Suspended)
    h.assert_workload_dormant("ns", "web");

    // Re-activate → cold start
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 100),
    });
    h.converge().await;
    h.assert_workload_running("ns", "web");

    // Verify LaunchPod was used both times (not ResumePod)
    let cmds = h.worker(&w1).commands();
    let launch_count = cmds.iter().filter(|c| matches!(c, WorkerCommand::LaunchPod { .. })).count();
    let resume_count = cmds.iter().filter(|c| matches!(c, WorkerCommand::ResumePod { .. })).count();
    assert_eq!(launch_count, 2, "expected 2 LaunchPod commands for cold starts");
    assert_eq!(resume_count, 0, "expected 0 ResumePod commands");
}
