use std::time::Duration;

use distvirt_orchestrator::types::*;

use crate::harness::TestCluster;
use crate::harness::spec_builders::{activation_spec, always_on_spec};

/// Delete a namespace while its pod is still launching (before it reaches Running).
/// The namespace should be cleaned up without leaving dangling state.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_delete_during_launch() {
    let mut cluster = TestCluster::new();
    let _w1 = cluster.add_worker().await;

    // Create namespace but don't fully converge — just enough to start scheduling.
    cluster.create_namespace("ns", always_on_spec()).await;

    // Partially converge: drain + a few yields, but not full quiescence.
    // This gives the orchestrator time to schedule but the pod may still be launching.
    cluster.shell.drain().await;
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }
    cluster.shell.step().await;

    // Delete while potentially still launching.
    cluster.delete_namespace("ns").await;
    cluster.converge().await;

    cluster.assert_namespace_absent("ns");
}

/// Send activation traffic while a pod is mid-suspend. The incoming demand
/// should override the suspend intent and the workload should resume/stay running.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_traffic_during_suspend() {
    let mut cluster = TestCluster::new();
    let w1 = cluster.add_worker().await;

    cluster
        .create_namespace("ns", activation_spec(Duration::from_secs(30)))
        .await;
    cluster.converge().await;
    cluster.assert_workload_dormant("ns", "web");

    // Activate via traffic -> Running.
    cluster.send_activation_traffic("ns", "web-svc").await;
    cluster.assert_workload_running("ns", "web");

    // Deactivate service to start idle timer.
    cluster.deactivate_service("ns", "web-svc", &w1).await;

    // Advance past idle timeout to trigger suspend.
    cluster.advance_past_idle_timeout("ns", "web-svc").await;

    // The workload should be suspending or suspended at this point.
    // Now send new activation traffic — this should override the suspend.
    cluster.send_activation_traffic("ns", "web-svc").await;

    // Give the system time to process the re-activation.
    cluster.advance_time(Duration::from_secs(5)).await;

    // The workload should end up Running (either stayed running or was re-activated).
    cluster.assert_workload_running("ns", "web");
}

/// Multiple rapid activate/deactivate cycles in quick succession.
/// The final state should be consistent with the last action taken.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_rapid_activate_deactivate_cycles() {
    let mut cluster = TestCluster::new();
    let w1 = cluster.add_worker().await;

    cluster
        .create_namespace("ns", activation_spec(Duration::from_secs(30)))
        .await;
    cluster.converge().await;
    cluster.assert_workload_dormant("ns", "web");

    // Rapid fire: activate, deactivate, activate, deactivate, activate.
    // Don't converge between each — let them pile up.
    cluster.send_activation_traffic("ns", "web-svc").await;
    cluster.deactivate_service("ns", "web-svc", &w1).await;
    cluster.send_activation_traffic("ns", "web-svc").await;
    cluster.deactivate_service("ns", "web-svc", &w1).await;
    cluster.send_activation_traffic("ns", "web-svc").await;

    // Final converge — last action was activate, so workload should be Running.
    cluster.converge().await;
    cluster.assert_workload_running("ns", "web");
    cluster.assert_service_active("ns", "web-svc");
}

/// Spec update arrives while pod is in the process of suspending.
/// If the image changed, the snapshot should be invalidated and a cold-start
/// should happen on next activation instead of a resume.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_spec_update_during_suspend() {
    use crate::harness::spec_builders::container_spec;

    let mut cluster = TestCluster::new();
    let w1 = cluster.add_worker().await;

    cluster
        .create_namespace("ns", activation_spec(Duration::from_secs(30)))
        .await;
    cluster.converge().await;
    cluster.assert_workload_dormant("ns", "web");

    // Activate -> Running.
    cluster.send_activation_traffic("ns", "web-svc").await;
    cluster.assert_workload_running("ns", "web");

    // Deactivate to start idle timer.
    cluster.deactivate_service("ns", "web-svc", &w1).await;

    // Advance past idle timeout to trigger suspend.
    cluster.advance_past_idle_timeout("ns", "web-svc").await;

    // While suspending/suspended, change the image spec.
    let mut new_spec = activation_spec(Duration::from_secs(30));
    new_spec
        .workloads
        .get_mut(&WorkloadId("web".to_string()))
        .unwrap()
        .containers = vec![container_spec("docker.io/library/alpine:3.19")];

    cluster.update_namespace("ns", new_spec).await;
    cluster.advance_time(Duration::from_secs(2)).await;

    // Snapshot should be invalidated. No demand → Dormant.
    cluster.assert_workload_dormant("ns", "web");

    // Re-activate — should cold-start (new pod_id), not resume from snapshot.
    cluster.send_activation_traffic("ns", "web-svc").await;
    cluster.assert_workload_running("ns", "web");
}
