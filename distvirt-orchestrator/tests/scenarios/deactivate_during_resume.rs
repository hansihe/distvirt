use std::net::Ipv4Addr;
use std::time::Duration;

use crate::harness::*;
use crate::harness::mock_worker::MockWorkerConfig;
use distvirt_orchestrator::types::*;
use distvirt_worker_protocol::{BackendNeed, ServiceId, WorkerEvent};

/// When a namespace is deleted while a workload is mid-resume (Resuming state),
/// the ForceDeactivate sets PendingIntent::Deactivate. When the resume completes
/// (PodRunning), the workload should stop rather than entering Running.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_delete_during_resume() {
    let mut h = TestHarness::new();

    // Use a handler that hangs on ResumePod AND DestroyNamespace (no response)
    // so we can observe the intermediate Destroying state.
    let config = MockWorkerConfig {
        handler: Some(Box::new(|cmd| match cmd {
            distvirt_worker_protocol::WorkerCommand::ResumePod { .. } => Some(vec![]),
            distvirt_worker_protocol::WorkerCommand::DestroyNamespace { .. } => Some(vec![]),
            _ => None,
        })),
        ..MockWorkerConfig::with_pool()
    };
    let w1 = h.add_worker_with(config).await;

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

    // Signal idle → starts idle timer.
    h.worker(&w1).send_event(WorkerEvent::ServiceBackendNeed {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        need: BackendNeed::None,
    });
    h.converge().await;

    // Advance past idle timeout → workload suspends.
    h.advance_time(timeout + Duration::from_secs(1)).await;
    h.assert_workload_suspended("ns", "web");

    // Re-activate → should start resume (which will hang).
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 100),
    });
    h.converge().await;
    h.assert_workload_resuming("ns", "web");

    // Get the pod_id before deleting namespace.
    let pod_id = match h.workload_state("ns", "web") {
        WorkloadState::Resuming { pod_id, .. } => pod_id.clone(),
        other => panic!("expected Resuming, got {:?}", other),
    };

    // Delete the namespace while mid-resume — sets PendingIntent::Deactivate.
    h.delete_namespace("ns").await;
    h.converge().await;

    // Namespace should be in Destroying state, not yet removed
    // (waiting for worker confirmation).
    h.assert_namespace_status("ns", NamespaceStatus::Destroying);

    // Complete the resume by injecting PodRunning.
    h.worker(&w1).send_event(WorkerEvent::PodRunning {
        namespace_id: "ns".into(),
        pod_id,
    });
    h.converge().await;

    // The workload should have been stopped (Deactivate intent)
    // and the namespace should proceed with destruction.
    // DestroyNamespace hangs, so namespace stays in Destroying.
    h.assert_namespace_status("ns", NamespaceStatus::Destroying);

    // Now inject NamespaceDestroyed to complete the destroy cycle.
    h.worker(&w1).send_event(WorkerEvent::NamespaceDestroyed {
        namespace_id: "ns".into(),
    });
    h.converge().await;

    // Namespace should now be fully removed.
    h.assert_namespace_absent("ns");
}

/// Spec change (image update) during resume sets PendingIntent::Restart.
/// When resume completes, the old pod is stopped and a new one launches
/// with the updated image.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_spec_change_during_resume() {
    let mut h = TestHarness::new();

    // Use a handler that hangs on ResumePod (no response) but handles everything else.
    let config = MockWorkerConfig {
        handler: Some(Box::new(|cmd| match cmd {
            distvirt_worker_protocol::WorkerCommand::ResumePod { .. } => Some(vec![]),
            _ => None,
        })),
        ..MockWorkerConfig::with_pool()
    };
    let w1 = h.add_worker_with(config).await;

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

    // Signal idle → suspend.
    h.worker(&w1).send_event(WorkerEvent::ServiceBackendNeed {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        need: BackendNeed::None,
    });
    h.converge().await;
    h.advance_time(timeout + Duration::from_secs(1)).await;
    h.assert_workload_suspended("ns", "web");

    // Re-activate → Resuming (hangs).
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 100),
    });
    h.converge().await;
    h.assert_workload_resuming("ns", "web");

    let pod_id = match h.workload_state("ns", "web") {
        WorkloadState::Resuming { pod_id, .. } => pod_id.clone(),
        other => panic!("expected Resuming, got {:?}", other),
    };

    // Update spec with new image while resuming.
    let mut new_spec = activation_spec(timeout);
    new_spec
        .workloads
        .get_mut(&WorkloadId("web".to_string()))
        .unwrap()
        .containers[0]
        .image_ref = "docker.io/library/nginx:v2".to_string();
    h.update_namespace("ns", new_spec).await;
    h.converge().await;

    // Still resuming (handler hangs).
    h.assert_workload_resuming("ns", "web");

    // Complete the resume.
    h.worker(&w1).send_event(WorkerEvent::PodRunning {
        namespace_id: "ns".into(),
        pod_id: pod_id.clone(),
    });
    h.converge().await;

    // The Restart intent should have stopped the old pod and relaunched.
    // Since the service still has demand, the workload should be Running
    // with a new pod_id (old pod was stopped, new one launched).
    h.assert_workload_running("ns", "web");
    let new_pod_id = h.workload_state("ns", "web").pod_id().unwrap();
    assert_ne!(
        *new_pod_id, pod_id,
        "pod should have been replaced (stopped + relaunched) due to Restart intent"
    );

    // Verify StopPod was issued for the old pod.
    let commands = h.worker(&w1).commands();
    let stop_count = commands
        .iter()
        .filter(|cmd| matches!(cmd, distvirt_worker_protocol::WorkerCommand::StopPod { pod_id: pid, .. } if *pid == pod_id))
        .count();
    assert!(
        stop_count >= 1,
        "should have issued StopPod for the old pod after Restart intent"
    );
}
