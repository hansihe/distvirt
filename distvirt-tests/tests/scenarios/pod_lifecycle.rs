use crate::harness::TestCluster;
use crate::harness::spec_builders::always_on_spec;

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_always_on_pod_lifecycle() {
    let mut cluster = TestCluster::new();
    let _w1 = cluster.add_worker().await;

    cluster.create_namespace("ns", always_on_spec()).await;
    cluster.converge().await;
    cluster.assert_workload_running("ns", "echo").await;

    cluster.delete_namespace("ns").await;
    cluster.converge().await;
    cluster.assert_namespace_absent("ns").await;
}
