use std::time::Duration;

use crate::harness::*;
use crate::harness::mock_worker::MockWorkerConfig;
use distvirt_orchestrator::types::*;
use distvirt_worker_protocol::{ServiceId, WorkerCommand, WorkerEvent};

/// Workload is Suspending (handler suppresses SuspendPod response).
/// Inject EndpointActivation (DemandUp). Then inject PodSuspended.
/// Workload SHOULD go directly to Resuming (not through Dormant).
// Low-level: tests mid-state-transition event injection with specific state dependencies
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_demand_during_suspend_immediate_resume() {
    let config = MockWorkerConfig::with_suspend_hang().add_pool();
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(config).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;

    // Activate → running → idle → begin suspending
    h.activate_service("ns", "web-svc").await;
    h.deactivate_service("ns", "web-svc").await;
    h.advance_past_idle_timeout("ns", "web-svc").await;
    h.assert_workload_suspending("ns", "web");

    // Capture the pod_id and artifact_id from suspending state
    let (pod_id, artifact_id) = match h.workload_state("ns", "web") {
        WorkloadState::Suspending { pod_id, artifact_id, .. } => (pod_id.clone(), artifact_id.clone()),
        other => panic!("expected Suspending, got {:?}", other),
    };

    // Low-level: inject demand (EndpointActivation) while suspending
    let svc_ip = h.service_ip("ns", "web-svc");
    h.worker(&w1).send_event(WorkerEvent::EndpointActivation {
        namespace_id: "ns".into(),
        ip: svc_ip,
        service_id: Some(ServiceId::from("web-svc")),
    });
    h.converge().await;

    // Still suspending but with Demand pending
    h.assert_workload_suspending("ns", "web");

    // Low-level: inject artifact events + PodSuspended
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
// Low-level: tests mid-state-transition event injection
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

    // Low-level: inject PodRunning (from the previous launch)
    h.worker(&w1).send_event(WorkerEvent::PodRunning {
        namespace_id: "ns".into(),
        pod_id,
    });
    h.converge().await;

    h.assert_namespace_absent("ns");
}

/// Workload is Resuming. Inject DemandUp (second service). Then PodRunning arrives.
// Low-level: tests mid-state-transition event injection with specific state dependencies
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
    h.activate_service("ns", "svc-a").await;

    // Idle svc-a → suspending/suspended
    h.deactivate_service("ns", "svc-a").await;
    h.advance_time(timeout + Duration::from_secs(1)).await;
    h.assert_workload_suspended("ns", "shared");

    // Low-level: activate via svc-a → triggers resume (but handler hangs on ResumePod)
    let svc_a_ip = h.service_ip("ns", "svc-a");
    h.worker(&w1).send_event(WorkerEvent::EndpointActivation {
        namespace_id: "ns".into(),
        ip: svc_a_ip,
        service_id: Some(ServiceId::from("svc-a")),
    });
    h.converge().await;
    h.assert_workload_resuming("ns", "shared");

    let pod_id = h.workload_state("ns", "shared").pod_id().unwrap().clone();

    // Low-level: activate svc-b too (second demand while resuming)
    let svc_b_ip = h.service_ip("ns", "svc-b");
    h.worker(&w1).send_event(WorkerEvent::EndpointActivation {
        namespace_id: "ns".into(),
        ip: svc_b_ip,
        service_id: Some(ServiceId::from("svc-b")),
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
    h.assert_service_active("ns", "svc-a");
    h.assert_service_active("ns", "svc-b");
}

/// Workload is Launching. Update spec with new image (SpecChanged → PendingIntent::Restart).
// Low-level: tests mid-state-transition event injection
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

    // Low-level: inject PodRunning from the old launch
    h.worker(&w1).send_event(WorkerEvent::PodRunning {
        namespace_id: "ns".into(),
        pod_id,
    });
    h.converge().await;

    h.assert_workload_launching("ns", "echo");

    // Verify StopPod was issued for the old pod
    let stop_count = h.worker_command_count(&w1, |c| matches!(c, WorkerCommand::StopPod { .. }));
    assert!(stop_count >= 1, "expected StopPod for old pod after PodRunning with Restart pending");
}

/// Workload is Suspending. Update spec with new image. PodSuspended arrives.
// Low-level: tests mid-state-transition event injection with specific state dependencies
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_spec_change_during_suspend() {
    let config = MockWorkerConfig::with_suspend_hang().add_pool();
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(config).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;

    // Activate → running → idle → begin suspending
    h.activate_service("ns", "web-svc").await;
    h.deactivate_service("ns", "web-svc").await;
    h.advance_past_idle_timeout("ns", "web-svc").await;
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

    // Low-level: inject artifact events + PodSuspended
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

    h.assert_workload_dormant("ns", "web");

    // Verify DeleteArtifact was issued
    let delete_count = h.worker_command_count(&w1, |c| matches!(c, WorkerCommand::DeleteArtifact { .. }));
    assert!(delete_count >= 1, "expected DeleteArtifact for old snapshot after spec change");
}
