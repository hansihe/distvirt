use crate::harness::TestCluster;
use crate::harness::spec_builders::always_on_spec;
use distvirt_orchestrator::types::WorkloadStatus;

/// Single worker hosts a workload, then dies. With no workers available the
/// workload should become displaced (launching / waiting-for-capacity). When a
/// new worker appears the orchestrator must automatically schedule the workload
/// onto it.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_worker_failover_reschedule_on_new_worker() {
    let mut cluster = TestCluster::new();
    let w1 = cluster.add_worker().await;

    cluster.create_namespace("ns", always_on_spec()).await;
    cluster.converge().await;
    cluster.assert_workload_running("ns", "echo").await;

    let hosting = cluster.worker_id_for_workload("ns", "echo").await;
    assert_eq!(hosting, w1);

    // Kill the only worker.
    cluster.disconnect_worker(&w1).await;
    cluster.converge().await;

    // No workers remain — workload should be waiting for capacity.
    cluster
        .assert_workload_waiting_for_capacity("ns", "echo")
        .await;

    // A new worker comes up after a little while.
    let w2 = cluster.add_worker().await;
    cluster
        .wait_workload_status("ns", "echo", WorkloadStatus::Running)
        .await;

    // Workload should now be running on the new worker.
    cluster.assert_workload_running("ns", "echo").await;
    let new_hosting = cluster.worker_id_for_workload("ns", "echo").await;
    assert_eq!(
        new_hosting, w2,
        "workload should have been rescheduled to the new worker"
    );
}

/// Regression test for deferred grant leak: when the scheduler grants a pod to
/// a worker that dies before completing namespace setup (NamespaceCreated), the
/// deferred grant is dropped but the scheduler still thinks the pod is granted.
/// A subsequent worker should still pick up the workload.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_deferred_grant_cleanup_on_ephemeral_worker() {
    let mut cluster = TestCluster::new();
    let w1 = cluster.add_worker().await;

    cluster.create_namespace("ns", always_on_spec()).await;
    cluster.converge().await;
    cluster.assert_workload_running("ns", "echo").await;

    // Kill the only worker — pod becomes displaced, schedule request pending.
    cluster.disconnect_worker(&w1).await;
    cluster.converge().await;
    cluster
        .assert_workload_waiting_for_capacity("ns", "echo")
        .await;

    // An ephemeral worker connects (handshake only, never processes commands).
    // The scheduler grants the pending pod to it, but the namespace defers the
    // grant because the worker never sends NamespaceCreated.
    let w_ephemeral = cluster.add_ephemeral_worker().await;

    // Kill the ephemeral worker before it completes namespace setup.
    cluster.disconnect_worker(&w_ephemeral).await;

    // The deferred grant is dropped. The scheduler must also clean up its
    // granted entry so the pod can be rescheduled.
    // A real worker now appears.
    let w3 = cluster.add_worker().await;
    cluster
        .wait_workload_status("ns", "echo", WorkloadStatus::Running)
        .await;

    cluster.assert_workload_running("ns", "echo").await;
    let hosting = cluster.worker_id_for_workload("ns", "echo").await;
    assert_eq!(
        hosting, w3,
        "workload should have been rescheduled to the third worker"
    );
}
