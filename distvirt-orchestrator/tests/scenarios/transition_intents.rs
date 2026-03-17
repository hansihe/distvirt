use std::time::Duration;

use crate::harness::mock_worker::MockWorkerConfig;
use crate::harness::*;
use distvirt_orchestrator::types::*;
use distvirt_worker_protocol::{ServiceId, WorkerCommand, WorkerEvent};

/// Workload is Suspending (handler suppresses SuspendPod response).
/// Inject EndpointActivation (DemandUp). Then inject PodSuspended.
/// Workload SHOULD go directly to Resuming (not through Dormant).
// Low-level: tests mid-state-transition event injection with specific state dependencies
#[test]
fn test_demand_during_suspend_immediate_resume() {
    let config = MockWorkerConfig::with_suspend_hang().add_pool();
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(config);
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout));
    h.converge();

    // Activate → running → idle → begin suspending
    h.activate_service("ns", "web-svc");
    h.deactivate_service("ns", "web-svc");
    h.advance_past_idle_timeout("ns", "web-svc");
    h.assert_workload_suspending("ns", "web");

    // Capture the pod_id and artifact_id from suspending state
    let pod_id = h
        .workload_proto_pod_id("ns", "web")
        .expect("expected pod_id");
    // The artifact_id is assigned by the namespace core's IdMaps when suspend begins.
    // We need to extract it from the worker commands (SuspendPod command has the artifact_id).
    let artifact_id = {
        let cmds = h.worker(&w1).commands();
        cmds.iter()
            .find_map(|cmd| match cmd {
                WorkerCommand::SuspendPod { artifact_id, .. } => Some(artifact_id.clone()),
                _ => None,
            })
            .expect("expected SuspendPod command with artifact_id")
    };

    // Low-level: inject demand (EndpointActivation) while suspending
    let svc_ip = h.service_ip("ns", "web-svc");
    h.worker(&w1).send_event(WorkerEvent::EndpointActivation {
        namespace_id: "ns".into(),
        ip: svc_ip,
        service_id: Some(ServiceId::from("web-svc")),
    });
    h.converge();

    // Still suspending but with Demand pending
    h.assert_workload_suspending("ns", "web");

    // Low-level: inject artifact events + PodSuspended
    h.worker(&w1).send_event(WorkerEvent::ArtifactWriteStarted {
        namespace_id: "ns".into(),
        artifact_id: artifact_id.clone(),
        pool_id: "local".into(),
    });
    h.worker(&w1)
        .send_event(WorkerEvent::ArtifactWriteCommitted {
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
    h.converge();

    // Fixed: Workload correctly transitions through Suspended → Resuming → Running.
    let status = h.workload_status("ns", "web");
    assert!(
        matches!(
            status,
            distvirt_orchestrator::sm_new::WlStatus::Launching
                | distvirt_orchestrator::sm_new::WlStatus::Running
        ),
        "Expected Resuming/Launching or Running after demand-during-suspend, got {:?}",
        status
    );
}

/// Start launching (handler suppresses LaunchPod response). Inject DemandDown (ForceDeactivate).
/// Then inject PodRunning. Workload should immediately begin deactivation.
// Low-level: tests mid-state-transition event injection
#[test]
fn test_force_deactivate_during_launch() {
    let config = MockWorkerConfig::with_launch_hang();
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(config);
    h.create_namespace("ns", always_on_spec());
    h.converge();

    // Should be in Launching (handler doesn't auto-respond)
    h.assert_workload_launching("ns", "echo");

    let pod_id = h
        .workload_proto_pod_id("ns", "echo")
        .expect("expected pod_id");

    // Delete namespace to force demand down
    h.delete_namespace("ns");
    h.converge();

    // Low-level: inject PodRunning (from the previous launch)
    h.worker(&w1).send_event(WorkerEvent::PodRunning {
        namespace_id: "ns".into(),
        pod_id,
    });
    h.converge();

    h.assert_namespace_absent("ns");
}

/// Workload is Resuming. Inject DemandUp (second service). Then PodRunning arrives.
// Low-level: tests mid-state-transition event injection with specific state dependencies
#[test]
fn test_demand_up_during_resume() {
    // Use a handler that hangs on ResumePod
    let config = MockWorkerConfig {
        handler: Some(Box::new(|cmd| match cmd {
            WorkerCommand::ResumePod { .. } => Some(vec![]),
            _ => None,
        })),
        capabilities: MockWorkerConfig::with_pool().capabilities,
        ..Default::default()
    };
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(config);
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", multi_service_spec());
    h.converge();
    h.assert_workload_dormant("ns", "shared");

    // Activate via svc-a → running (default handler responds to LaunchPod)
    h.activate_service("ns", "svc-a");

    // Idle svc-a → suspending/suspended
    h.deactivate_service("ns", "svc-a");
    h.advance_time(timeout + Duration::from_secs(1));
    h.assert_workload_suspended("ns", "shared");

    // Low-level: activate via svc-a → triggers resume (but handler hangs on ResumePod)
    let svc_a_ip = h.service_ip("ns", "svc-a");
    h.worker(&w1).send_event(WorkerEvent::EndpointActivation {
        namespace_id: "ns".into(),
        ip: svc_a_ip,
        service_id: Some(ServiceId::from("svc-a")),
    });
    h.converge();
    h.assert_workload_resuming("ns", "shared");

    let pod_id = h
        .workload_proto_pod_id("ns", "shared")
        .expect("expected pod_id");

    // Low-level: activate svc-b too (second demand while resuming)
    let svc_b_ip = h.service_ip("ns", "svc-b");
    h.worker(&w1).send_event(WorkerEvent::EndpointActivation {
        namespace_id: "ns".into(),
        ip: svc_b_ip,
        service_id: Some(ServiceId::from("svc-b")),
    });
    h.converge();
    h.assert_workload_resuming("ns", "shared");

    // Inject PodRunning
    h.worker(&w1).send_event(WorkerEvent::PodRunning {
        namespace_id: "ns".into(),
        pod_id,
    });
    h.converge();

    // Should be Running with both services active
    h.assert_workload_running("ns", "shared");
    h.assert_service_active("ns", "svc-a");
    h.assert_service_active("ns", "svc-b");
}

/// Workload is Launching. Update spec with new image (SpecChanged → PendingIntent::Restart).
// Low-level: tests mid-state-transition event injection
#[test]
fn test_spec_change_during_launch() {
    let config = MockWorkerConfig::with_launch_hang();
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(config);
    h.create_namespace("ns", always_on_spec());
    h.converge();
    h.assert_workload_launching("ns", "echo");

    let pod_id = h
        .workload_proto_pod_id("ns", "echo")
        .expect("expected pod_id");

    // Update spec with new image
    let mut new_spec = always_on_spec();
    new_spec
        .workloads
        .get_mut(&WorkloadId("echo".to_string()))
        .unwrap()
        .containers[0]
        .image_ref = "docker.io/library/alpine:v2".to_string();
    h.update_namespace("ns", new_spec);
    h.converge();

    // Still launching (pending Restart)
    h.assert_workload_launching("ns", "echo");

    // Low-level: inject PodRunning from the old launch
    h.worker(&w1).send_event(WorkerEvent::PodRunning {
        namespace_id: "ns".into(),
        pod_id,
    });
    h.converge();

    h.assert_workload_launching("ns", "echo");

    // Verify StopPod was issued for the old pod
    let stop_count = h.worker_command_count(&w1, |c| matches!(c, WorkerCommand::StopPod { .. }));
    assert!(
        stop_count >= 1,
        "expected StopPod for old pod after PodRunning with Restart pending"
    );
}

/// Workload is Suspending. Update spec with new image. PodSuspended arrives.
// Low-level: tests mid-state-transition event injection with specific state dependencies
#[test]
fn test_spec_change_during_suspend() {
    let config = MockWorkerConfig::with_suspend_hang().add_pool();
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(config);
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout));
    h.converge();

    // Activate → running → idle → begin suspending
    h.activate_service("ns", "web-svc");
    h.deactivate_service("ns", "web-svc");
    h.advance_past_idle_timeout("ns", "web-svc");
    h.assert_workload_suspending("ns", "web");

    let pod_id = h
        .workload_proto_pod_id("ns", "web")
        .expect("expected pod_id");
    let artifact_id = {
        let cmds = h.worker(&w1).commands();
        cmds.iter()
            .find_map(|cmd| match cmd {
                WorkerCommand::SuspendPod { artifact_id, .. } => Some(artifact_id.clone()),
                _ => None,
            })
            .expect("expected SuspendPod command with artifact_id")
    };

    // Update spec with new image while suspending
    let mut new_spec = activation_spec(timeout);
    new_spec
        .workloads
        .get_mut(&WorkloadId("web".to_string()))
        .unwrap()
        .containers[0]
        .image_ref = "docker.io/library/nginx:v2".to_string();
    h.update_namespace("ns", new_spec);
    h.converge();

    // Low-level: inject artifact events + PodSuspended
    h.worker(&w1).send_event(WorkerEvent::ArtifactWriteStarted {
        namespace_id: "ns".into(),
        artifact_id: artifact_id.clone(),
        pool_id: "local".into(),
    });
    h.worker(&w1)
        .send_event(WorkerEvent::ArtifactWriteCommitted {
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
    h.converge();

    h.assert_workload_dormant("ns", "web");

    // Verify DeleteArtifact was issued
    let delete_count =
        h.worker_command_count(&w1, |c| matches!(c, WorkerCommand::DeleteArtifact { .. }));
    assert!(
        delete_count >= 1,
        "expected DeleteArtifact for old snapshot after spec change"
    );
}
