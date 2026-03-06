use std::time::Duration;

use crate::harness::*;
use crate::harness::mock_worker::MockWorkerConfig;
use distvirt_orchestrator::types::*;
use distvirt_worker_protocol::WorkerEvent;

/// When a namespace is deleted while a workload is mid-resume (Resuming state),
/// the ForceDeactivate sets PendingIntent::Deactivate. When the resume completes
/// (PodRunning), the workload should stop rather than entering Running.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_delete_during_resume() {
    let mut h = TestHarness::new();

    // Use a handler that hangs on ResumePod AND DestroyNamespace (no response)
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

    // Activate → Running → idle → suspend
    h.run_activation_suspend_cycle("ns", "web-svc", "web").await;

    // Re-activate → should start resume (which will hang).
    // Low-level: need to trigger resume without asserting Running (it hangs)
    let svc_ip = h.service_ip("ns", "web-svc");
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: distvirt_worker_protocol::ServiceId::from("web-svc"),
        dst_ip: svc_ip,
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

    h.assert_namespace_status("ns", NamespaceStatus::Destroying);

    // Complete the resume by injecting PodRunning.
    h.worker(&w1).send_event(WorkerEvent::PodRunning {
        namespace_id: "ns".into(),
        pod_id,
    });
    h.converge().await;

    h.assert_namespace_status("ns", NamespaceStatus::Destroying);

    // Inject NamespaceDestroyed to complete the destroy cycle.
    h.worker(&w1).send_event(WorkerEvent::NamespaceDestroyed {
        namespace_id: "ns".into(),
    });
    h.converge().await;

    h.assert_namespace_absent("ns");
}

/// Spec change (image update) during resume sets PendingIntent::Restart.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_spec_change_during_resume() {
    let mut h = TestHarness::new();

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

    // Activate → Running → idle → suspend
    h.run_activation_suspend_cycle("ns", "web-svc", "web").await;

    // Re-activate → Resuming (hangs).
    // Low-level: need to trigger resume without asserting Running (it hangs)
    let svc_ip = h.service_ip("ns", "web-svc");
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: distvirt_worker_protocol::ServiceId::from("web-svc"),
        dst_ip: svc_ip,
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
