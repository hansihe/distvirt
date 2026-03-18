use std::time::Duration;

use distvirt_worker::vmm::guest_sim::ContainerBehavior;
use distvirt_worker::vmm::test_vmm::TestVmm;

use crate::harness::TestCluster;
use crate::harness::spec_builders::activation_spec;

/// After suspend, re-activation triggers ResumePod (not LaunchPod).
/// Successful completion from Suspended validates the resume path.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_suspend_then_resume_e2e() {
    let mut cluster = TestCluster::new();
    let w1 = cluster.add_worker().await;

    cluster
        .create_namespace("ns", activation_spec(Duration::from_secs(30)))
        .await;
    cluster.converge().await;
    cluster.assert_workload_dormant("ns", "web").await;

    // First activation cycle: activate -> deactivate -> suspend.
    cluster.send_activation_traffic("ns", "web-svc").await;
    cluster.assert_workload_running("ns", "web").await;

    cluster.deactivate_service("ns", "web-svc", &w1).await;
    cluster.advance_past_idle_timeout("ns", "web-svc").await;
    cluster.wait_workload_suspended("ns", "web").await;
    cluster.assert_workload_suspended("ns", "web").await;
    cluster.assert_service_idle("ns", "web-svc").await;

    // Second activation: should resume from snapshot (ResumePod path).
    cluster.send_activation_traffic("ns", "web-svc").await;
    cluster.assert_workload_running("ns", "web").await;
    cluster.assert_service_active("ns", "web-svc").await;
}

/// When the guest hangs on PrepareSuspend, the worker's 10s SUSPEND_TIMEOUT fires,
/// emitting PodSuspendFailed. The orchestrator should fall back: workload goes Dormant.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_suspend_timeout_fallback_to_stop_e2e() {
    let mut cluster = TestCluster::new();
    let vmm = TestVmm::with_suspend_hang(ContainerBehavior::RunUntilSignaled);
    let w1 = cluster.add_worker_with_vmm(vmm).await;

    cluster
        .create_namespace("ns", activation_spec(Duration::from_secs(30)))
        .await;
    cluster.converge().await;
    cluster.assert_workload_dormant("ns", "web").await;

    // Activate.
    cluster.send_activation_traffic("ns", "web-svc").await;
    cluster.assert_workload_running("ns", "web").await;

    // Deactivate, advance past idle timeout → triggers suspend attempt.
    cluster.deactivate_service("ns", "web-svc", &w1).await;
    cluster.advance_past_idle_timeout("ns", "web-svc").await;

    // Guest hangs on PrepareSuspend. Advance past the worker's 10s SUSPEND_TIMEOUT.
    cluster.advance_time(Duration::from_secs(11)).await;

    // PodSuspendFailed → orchestrator transitions workload to Dormant.
    cluster.assert_workload_dormant("ns", "web").await;
}

/// After suspend, re-activation resumes on the same worker that holds the artifact.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_resume_pinned_to_artifact_worker_e2e() {
    let mut cluster = TestCluster::new();
    let _w1 = cluster.add_worker().await;
    let _w2 = cluster.add_worker().await;

    cluster
        .create_namespace("ns", activation_spec(Duration::from_secs(30)))
        .await;
    cluster.converge().await;
    cluster.assert_workload_dormant("ns", "web").await;

    // Activate → running. Record hosting worker.
    cluster.send_activation_traffic("ns", "web-svc").await;
    cluster.assert_workload_running("ns", "web").await;
    let hosting_worker = cluster.worker_id_for_workload("ns", "web").await;

    // Deactivate → suspend.
    cluster
        .deactivate_service("ns", "web-svc", &hosting_worker)
        .await;
    cluster.advance_past_idle_timeout("ns", "web-svc").await;
    cluster.wait_workload_suspended("ns", "web").await;

    // Re-activate → should resume on the same worker (artifact pinning).
    cluster.send_activation_traffic("ns", "web-svc").await;

    // The resume path may need extra convergence for the snapshot restore I/O.
    // Check if already running, otherwise wait for it.
    let state = cluster.workload_status_str("ns", "web").await;
    if state != "running" {
        cluster.assert_workload_running("ns", "web").await;
    }
    cluster.assert_workload_running("ns", "web").await;
    let resume_worker = cluster.worker_id_for_workload("ns", "web").await;
    assert_eq!(
        hosting_worker, resume_worker,
        "resumed workload should be pinned to the artifact-holding worker"
    );
}
