use std::net::Ipv4Addr;
use std::time::Duration;

use crate::harness::*;
use crate::harness::mock_worker::MockWorkerConfig;
use distvirt_orchestrator::types::*;
use distvirt_worker_protocol::{BackendNeed, ServiceId, WorkerCommand, WorkerEvent};

/// Workload is Suspending (handler suppresses SuspendPod response).
/// Inject ServiceActivation (DemandUp). Then inject PodSuspended.
/// Workload SHOULD go directly to Resuming (not through Dormant).
///
/// BUG: In workload.rs PodSuspended handler, when pending=Demand, the workload's
/// state was set to Dormant by `mem::replace` and is never updated to Suspended.
/// The ResumeRequest output is emitted, but `handle_resume_pod` checks for
/// `WorkloadState::Suspended` → finds Dormant → returns early. The workload gets
/// stuck in Dormant with demand > 0.
///
/// Fix: in the `PendingIntent::Demand` and `PendingIntent::None` (with demand > 0)
/// branches of PodSuspended, set `self.state = WorkloadState::Suspended { artifact_id }`
/// before emitting `ResumeRequest`.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_demand_during_suspend_immediate_resume() {
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
    h.assert_workload_suspending("ns", "web");

    // Capture the pod_id and artifact_id from suspending state
    let (pod_id, artifact_id) = match h.workload_state("ns", "web") {
        WorkloadState::Suspending { pod_id, artifact_id, .. } => (pod_id.clone(), artifact_id.clone()),
        other => panic!("expected Suspending, got {:?}", other),
    };

    // Inject demand (ServiceActivation) while suspending
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 100),
    });
    h.converge().await;

    // Still suspending but with Demand pending
    h.assert_workload_suspending("ns", "web");

    // Inject artifact events + PodSuspended
    h.worker(&w1).send_event(WorkerEvent::ArtifactWriteStarted {
        namespace_id: "ns".into(),
        artifact_id: artifact_id.clone(),
        pool_id: "local".into(),
    });
    h.worker(&w1).send_event(WorkerEvent::ArtifactWriteCommitted {
        namespace_id: "ns".into(),
        artifact_id: artifact_id.clone(),
        pool_id: "local".into(),
        size_bytes: 1024,
    });
    h.worker(&w1).send_event(WorkerEvent::PodSuspended {
        namespace_id: "ns".into(),
        pod_id,
        artifact_id: artifact_id.clone(),
        artifact_size_bytes: 1024,
        pool_id: "local".into(),
    });
    h.converge().await;

    // Fixed: Workload correctly transitions through Suspended → Resuming → Running.
    let state = h.workload_state("ns", "web");
    assert!(
        matches!(state, WorkloadState::Resuming { .. } | WorkloadState::Running { .. }),
        "Expected Resuming or Running after demand-during-suspend, got {:?}",
        state
    );
}

/// Start launching (handler suppresses LaunchPod response). Inject DemandDown (ForceDeactivate).
/// Then inject PodRunning. Workload should immediately begin deactivation.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_force_deactivate_during_launch() {
    let config = MockWorkerConfig::with_launch_hang();
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(config).await;
    h.create_namespace("ns", always_on_spec()).await;
    h.converge().await;

    // Should be in Launching (handler doesn't auto-respond)
    h.assert_workload_launching("ns", "echo");

    let pod_id = h.workload_state("ns", "echo").pod_id().unwrap().clone();

    // Delete namespace to force demand down
    h.delete_namespace("ns").await;
    h.converge().await;

    // Inject PodRunning (from the previous launch)
    h.worker(&w1).send_event(WorkerEvent::PodRunning {
        namespace_id: "ns".into(),
        pod_id,
    });
    h.converge().await;

    // Namespace should be gone (destroyed)
    h.assert_namespace_absent("ns");
}

/// Workload is Resuming. Inject DemandUp (second service). Then PodRunning arrives.
/// Workload should be Running (demand is already satisfied — no-op pending).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_demand_up_during_resume() {
    // Use a handler that hangs on ResumePod
    let config = MockWorkerConfig {
        handler: Some(Box::new(|cmd| match cmd {
            WorkerCommand::ResumePod { .. } => Some(vec![]),
            _ => None,
        })),
        capabilities: MockWorkerConfig::with_pool().capabilities,
    };
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(config).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", multi_service_spec()).await;
    h.converge().await;
    h.assert_workload_dormant("ns", "shared");

    // Activate via svc-a → running (default handler responds to LaunchPod)
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("svc-a"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 100),
    });
    h.converge().await;
    h.assert_workload_running("ns", "shared");

    // Idle svc-a → suspending/suspended
    h.worker(&w1).send_event(WorkerEvent::ServiceBackendNeed {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("svc-a"),
        need: BackendNeed::None,
    });
    h.converge().await;
    h.advance_time(timeout + Duration::from_secs(1)).await;

    // Need the workload to actually become Suspended first (default SuspendPod handler works).
    // But our handler only hangs ResumePod. The default SuspendPod handler should kick in.
    // Actually let me check - the handler returns None for SuspendPod, so default_handle takes over.
    h.assert_workload_suspended("ns", "shared");

    // Activate via svc-a → triggers resume (but handler hangs on ResumePod)
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("svc-a"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 100),
    });
    h.converge().await;
    h.assert_workload_resuming("ns", "shared");

    let pod_id = h.workload_state("ns", "shared").pod_id().unwrap().clone();

    // Now activate svc-b too (second demand while resuming)
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("svc-b"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 101),
    });
    h.converge().await;
    h.assert_workload_resuming("ns", "shared");

    // Inject PodRunning
    h.worker(&w1).send_event(WorkerEvent::PodRunning {
        namespace_id: "ns".into(),
        pod_id,
    });
    h.converge().await;

    // Should be Running with both services active
    h.assert_workload_running("ns", "shared");
}

/// Workload is Launching. Update spec with new image (SpecChanged → PendingIntent::Restart).
/// Then PodRunning arrives. Workload should stop old pod and relaunch with new image.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_spec_change_during_launch() {
    let config = MockWorkerConfig::with_launch_hang();
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(config).await;
    h.create_namespace("ns", always_on_spec()).await;
    h.converge().await;
    h.assert_workload_launching("ns", "echo");

    let pod_id = h.workload_state("ns", "echo").pod_id().unwrap().clone();

    // Update spec with new image
    let mut new_spec = always_on_spec();
    new_spec.workloads.get_mut(&WorkloadId("echo".to_string())).unwrap()
        .containers[0].image_ref = "docker.io/library/alpine:v2".to_string();
    h.update_namespace("ns", new_spec).await;
    h.converge().await;

    // Still launching (pending Restart)
    h.assert_workload_launching("ns", "echo");

    // Inject PodRunning from the old launch
    h.worker(&w1).send_event(WorkerEvent::PodRunning {
        namespace_id: "ns".into(),
        pod_id,
    });
    h.converge().await;

    // Pending was Restart: workload stops old pod, goes Dormant, then
    // transition_on_demand → WaitingForCapacity (demand > 0, always-on).
    // The handler hangs on LaunchPod, so the re-launch stays in WaitingForCapacity
    // or Launching.
    let state = h.workload_state("ns", "echo");
    assert!(
        matches!(state, WorkloadState::WaitingForCapacity | WorkloadState::Launching { .. }),
        "workload should be WaitingForCapacity or Launching after Restart intent, got {:?}",
        state
    );

    // Verify StopPod was issued for the old pod
    let cmds = h.worker(&w1).commands();
    let stop_count = cmds.iter().filter(|c| matches!(c, WorkerCommand::StopPod { .. })).count();
    assert!(stop_count >= 1, "expected StopPod for old pod after PodRunning with Restart pending");
}

/// Workload is Suspending. Update spec with new image. PodSuspended arrives.
/// Workload should NOT resume with old snapshot — should delete artifact and relaunch.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_spec_change_during_suspend() {
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
    h.assert_workload_suspending("ns", "web");

    let (pod_id, artifact_id) = match h.workload_state("ns", "web") {
        WorkloadState::Suspending { pod_id, artifact_id, .. } => (pod_id.clone(), artifact_id.clone()),
        other => panic!("expected Suspending, got {:?}", other),
    };

    // Update spec with new image while suspending
    let mut new_spec = activation_spec(timeout);
    new_spec.workloads.get_mut(&WorkloadId("web".to_string())).unwrap()
        .containers[0].image_ref = "docker.io/library/nginx:v2".to_string();
    h.update_namespace("ns", new_spec).await;
    h.converge().await;

    // Inject artifact events + PodSuspended
    h.worker(&w1).send_event(WorkerEvent::ArtifactWriteStarted {
        namespace_id: "ns".into(),
        artifact_id: artifact_id.clone(),
        pool_id: "local".into(),
    });
    h.worker(&w1).send_event(WorkerEvent::ArtifactWriteCommitted {
        namespace_id: "ns".into(),
        artifact_id: artifact_id.clone(),
        pool_id: "local".into(),
        size_bytes: 1024,
    });
    h.worker(&w1).send_event(WorkerEvent::PodSuspended {
        namespace_id: "ns".into(),
        pod_id,
        artifact_id: artifact_id.clone(),
        artifact_size_bytes: 1024,
        pool_id: "local".into(),
    });
    h.converge().await;

    // Pending was Restart. Workload deletes the stale artifact and transitions based on demand.
    // Demand is 0 (service went idle before suspend), so workload should be Dormant.
    h.assert_workload_dormant("ns", "web");

    // Verify DeleteArtifact was issued
    let cmds = h.worker(&w1).commands();
    let delete_count = cmds.iter().filter(|c| matches!(c, WorkerCommand::DeleteArtifact { .. })).count();
    assert!(delete_count >= 1, "expected DeleteArtifact for old snapshot after spec change");
}
