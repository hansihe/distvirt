use std::net::Ipv4Addr;
use std::time::Duration;

use crate::harness::*;
use crate::harness::mock_worker::MockWorkerConfig;
use distvirt_worker_protocol::{BackendNeed, ServiceId, WorkerCommand, WorkerEvent};

/// Two pool workers. Activate on one worker, suspend (artifact placed on that worker).
/// Re-activate. Assert ResumePod goes to the artifact-holding worker (not the other).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_resume_pinned_to_artifact_worker() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    let _w2 = h.add_worker_with(MockWorkerConfig::with_pool()).await;

    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;
    h.assert_workload_dormant("ns", "web");

    // Activate → Running (lands on one of the two workers).
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 100),
    });
    h.converge().await;
    h.assert_workload_running("ns", "web");

    let running_worker = h.workload_state("ns", "web").worker_id().unwrap().clone();

    // Idle → suspend.
    h.worker(&running_worker).send_event(WorkerEvent::ServiceBackendNeed {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        need: BackendNeed::None,
    });
    h.converge().await;
    h.advance_time(timeout + Duration::from_secs(1)).await;
    h.assert_workload_suspended("ns", "web");

    // Re-activate → should resume on the same worker (artifact pinning).
    h.worker(&running_worker).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 100),
    });
    h.converge().await;
    h.assert_workload_running("ns", "web");

    let resume_worker = h.workload_state("ns", "web").worker_id().unwrap().clone();
    assert_eq!(
        resume_worker, running_worker,
        "resume should be pinned to the artifact-holding worker, not the other"
    );

    // Verify ResumePod was issued (not LaunchPod).
    let cmds = h.worker(&running_worker).commands();
    let resume_count = cmds.iter().filter(|c| matches!(c, WorkerCommand::ResumePod { .. })).count();
    assert!(resume_count >= 1, "expected ResumePod for snapshot resume");
}

/// One pool worker. Activate, suspend (artifact placed on worker).
/// Disconnect worker. Add a new pool worker.
/// Re-activate → should cold LaunchPod (not ResumePod) since artifact was lost.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_artifact_lost_on_worker_disconnect_cold_launch() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool()).await;

    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;
    h.assert_workload_dormant("ns", "web");

    // Activate → Running.
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 100),
    });
    h.converge().await;
    h.assert_workload_running("ns", "web");

    // Idle → suspend.
    h.worker(&w1).send_event(WorkerEvent::ServiceBackendNeed {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        need: BackendNeed::None,
    });
    h.converge().await;
    h.advance_time(timeout + Duration::from_secs(1)).await;
    h.assert_workload_suspended("ns", "web");

    // Disconnect the worker — artifact is lost.
    h.disconnect_worker(&w1);
    h.converge().await;

    // Workload should fall back to WaitingForCapacity (artifact gone, demand from always-on? No —
    // this is an activation spec with idle timeout, demand was 0 after idle. But we need demand
    // to trigger a re-launch). The workload goes to Dormant since demand is 0.
    // So we need to re-activate on the new worker.

    // Add a fresh pool worker.
    let w2 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    h.converge().await;

    // Re-activate on the new worker.
    h.worker(&w2).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 100),
    });
    h.converge().await;
    h.assert_workload_running("ns", "web");

    let new_worker = h.workload_state("ns", "web").worker_id().unwrap().clone();
    assert_eq!(new_worker, w2, "workload should be on the new worker");

    // Verify LaunchPod was used (not ResumePod) — cold launch since artifact is gone.
    let cmds = h.worker(&w2).commands();
    let launch_count = cmds.iter().filter(|c| matches!(c, WorkerCommand::LaunchPod { .. })).count();
    let resume_count = cmds.iter().filter(|c| matches!(c, WorkerCommand::ResumePod { .. })).count();
    assert!(launch_count >= 1, "expected LaunchPod for cold launch after artifact loss");
    assert_eq!(resume_count, 0, "expected no ResumePod — artifact was on the disconnected worker");
}
