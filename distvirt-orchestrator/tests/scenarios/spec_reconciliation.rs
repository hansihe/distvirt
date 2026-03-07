use std::net::Ipv4Addr;
use std::time::Duration;

use crate::harness::*;
use crate::harness::mock_worker::MockWorkerConfig;
use distvirt_orchestrator::types::*;
use distvirt_worker_protocol::{ServiceId, WorkerCommand};

/// Running workload. Update spec with different image. Workload stops and relaunches.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_image_change_restarts_running_workload() {
    let mut h = TestHarness::new();
    let _w1 = h.add_worker().await;
    h.create_namespace("ns", always_on_spec()).await;
    h.converge().await;
    h.assert_workload_running("ns", "echo");

    let old_pod_id = h.workload_state("ns", "echo").pod_id().unwrap().clone();

    // Update spec with new image
    let mut new_spec = always_on_spec();
    new_spec.workloads.get_mut(&WorkloadId("echo".to_string())).unwrap()
        .containers[0].image_ref = "docker.io/library/alpine:v2".to_string();
    h.update_namespace("ns", new_spec).await;
    h.converge().await;

    // Should be running again with a new pod
    h.assert_workload_running("ns", "echo");
    let new_pod_id = h.workload_state("ns", "echo").pod_id().unwrap().clone();
    assert_ne!(old_pod_id, new_pod_id, "pod should have been replaced with new image");
}

/// Suspended workload. Update spec with new image. Old artifact deleted.
/// On next activation, cold start with new image (not resume).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_image_change_on_suspended_workload() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;

    // Activate → running → idle → suspended
    h.run_activation_suspend_cycle("ns", "web-svc", "web").await;

    // Update spec with new image
    let mut new_spec = activation_spec(timeout);
    new_spec.workloads.get_mut(&WorkloadId("web".to_string())).unwrap()
        .containers[0].image_ref = "docker.io/library/nginx:v2".to_string();
    h.update_namespace("ns", new_spec).await;
    h.converge().await;

    // Artifact should be deleted, workload should be Dormant (no demand)
    h.assert_workload_dormant("ns", "web");

    // Verify DeleteArtifact was issued
    let delete_count = h.worker_command_count(&w1, |c| matches!(c, WorkerCommand::DeleteArtifact { .. }));
    assert!(delete_count >= 1, "expected DeleteArtifact for old snapshot");

    // Re-activate → cold start (LaunchPod, not ResumePod)
    h.activate_service("ns", "web-svc").await;

    // Verify LaunchPod was used (count: 2 = first launch + this launch)
    h.assert_worker_command_count(&w1, "LaunchPod", 2, |c| matches!(c, WorkerCommand::LaunchPod { .. }));
    h.assert_worker_command_count(&w1, "ResumePod", 0, |c| matches!(c, WorkerCommand::ResumePod { .. }));
}

/// Namespace running. Add a new workload via spec update. New workload should start.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_add_workload_to_existing_namespace() {
    let mut h = TestHarness::new();
    h.add_worker().await;
    h.create_namespace("ns", always_on_spec()).await;
    h.converge().await;
    h.assert_workload_running("ns", "echo");

    // Add a second workload
    let mut new_spec = always_on_spec();
    let wl_b = WorkloadId("echo-b".to_string());
    new_spec.workloads.insert(
        wl_b.clone(),
        WorkloadSpec {
            containers: vec![container_spec("docker.io/library/alpine:latest")],
            network: pod_network(11),
            suspend_on_idle: false,
            resources: None,
        },
    );
    new_spec.services.insert(
        ServiceId::from("svc-b"),
        ServiceSpec {
            workload_id: wl_b,
            ip: Ipv4Addr::new(172, 16, 0, 101),
            policy: distvirt_worker_protocol::ServicePolicy {
                buffer_frames: 100,
                timeout_ms: 5000,
                activator: None,
            },
            activation: None,
        },
    );
    h.update_namespace("ns", new_spec).await;
    h.converge().await;

    // Both workloads should be running
    h.assert_workload_running("ns", "echo");
    h.assert_workload_running("ns", "echo-b");
}

/// Running workload removed from spec. Pod stopped, workload cleaned up.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_remove_workload_from_namespace() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker().await;
    h.create_namespace("ns", always_on_two_workloads_spec()).await;
    h.converge().await;
    h.assert_workload_running("ns", "echo-a");
    h.assert_workload_running("ns", "echo-b");

    // Remove echo-b: create spec with only echo-a
    let mut new_spec = always_on_two_workloads_spec();
    new_spec.workloads.remove(&WorkloadId("echo-b".to_string()));
    new_spec.services.remove(&ServiceId::from("svc-b"));
    h.update_namespace("ns", new_spec).await;
    h.converge().await;

    // echo-a should still be running, echo-b should be gone
    h.assert_workload_running("ns", "echo-a");
    let ns = h.namespace("ns");
    assert!(
        !ns.workloads.contains_key(&WorkloadId("echo-b".to_string())),
        "removed workload 'echo-b' should not exist"
    );

    // Verify StopPod was issued for echo-b
    let stop_count = h.worker_command_count(&w1, |c| matches!(c, WorkerCommand::StopPod { .. }));
    assert!(stop_count >= 1, "expected StopPod for removed workload");
}

/// Running workload with suspend_on_idle: true. Change to false.
/// On next idle cycle, workload should stop (not suspend).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_suspend_on_idle_flag_change() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;

    // Activate → running
    h.activate_service("ns", "web-svc").await;

    // Verify suspend_on_idle is currently true
    let ns = h.namespace("ns");
    let wl = ns.workloads.get(&WorkloadId("web".to_string())).unwrap();
    assert!(wl.suspend_on_idle, "should start with suspend_on_idle=true");

    // Update spec: change suspend_on_idle to false
    let mut new_spec = activation_spec(timeout);
    new_spec.workloads.get_mut(&WorkloadId("web".to_string())).unwrap()
        .suspend_on_idle = false;
    h.update_namespace("ns", new_spec).await;
    h.converge().await;

    // Verify suspend_on_idle was updated
    let ns = h.namespace("ns");
    let wl = ns.workloads.get(&WorkloadId("web".to_string())).unwrap();
    assert!(!wl.suspend_on_idle, "should now have suspend_on_idle=false");

    // Idle → should stop (not suspend)
    h.deactivate_service("ns", "web-svc").await;
    h.advance_past_idle_timeout("ns", "web-svc").await;

    // Should be Dormant (stopped), not Suspended
    h.assert_workload_dormant("ns", "web");

    // Verify StopPod was issued (not SuspendPod)
    h.assert_worker_command_count(&w1, "SuspendPod", 0, |c| matches!(c, WorkerCommand::SuspendPod { .. }));
    let stop_count = h.worker_command_count(&w1, |c| matches!(c, WorkerCommand::StopPod { .. }));
    assert!(stop_count >= 1, "should have issued StopPod");
}
