use crate::harness::TestCluster;
use crate::harness::spec_builders::always_on_spec;

/// Drain a worker -> new pods go elsewhere. Undrain -> worker schedulable again.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_drain_excludes_from_scheduling() {
    let mut cluster = TestCluster::new();
    let w1 = cluster.add_worker().await;
    let _w2 = cluster.add_worker().await;

    // Drain w1 before creating any namespace.
    cluster.drain_worker(&w1).await;

    cluster.create_namespace("ns", always_on_spec()).await;
    cluster.converge().await;
    cluster.assert_workload_running("ns", "echo");

    // Workload should be on w2 (not the drained w1).
    let hosting = cluster.worker_id_for_workload("ns", "echo");
    assert_ne!(hosting, w1, "workload should not be scheduled on drained worker");

    // Undrain w1.
    cluster.undrain_worker(&w1).await;

    // Delete and recreate — scheduling should now consider both workers.
    cluster.delete_namespace("ns").await;
    cluster.converge().await;
    cluster.create_namespace("ns2", always_on_spec()).await;
    cluster.converge().await;
    cluster.assert_workload_running("ns2", "echo");
    // We can't predict which worker it lands on, but it should be running.
}

/// Existing pods aren't stopped when a worker is drained.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_drain_existing_pods_continue_running() {
    let mut cluster = TestCluster::new();
    let w1 = cluster.add_worker().await;

    cluster.create_namespace("ns", always_on_spec()).await;
    cluster.converge().await;
    cluster.assert_workload_running("ns", "echo");
    assert_eq!(cluster.worker_id_for_workload("ns", "echo"), w1);

    // Drain w1 after pod is running.
    cluster.drain_worker(&w1).await;

    // Pod should still be running.
    cluster.assert_workload_running("ns", "echo");
}
