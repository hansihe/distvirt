use crate::harness::TestCluster;
use crate::harness::spec_builders::always_on_spec;

/// Pod runs on one worker. Disconnect it. Orchestrator reschedules to the remaining worker.
///
/// Previously failed because `SimGatewayProvider` used a flat `HashMap<NamespaceId, Sender>`.
/// When both workers created the same namespace, the second `register()` overwrote the
/// first worker's `internet_tx`, closing its `ChannelEgress` channel. This caused the
/// first worker's gateway to exit, sending `NamespaceFailed` and removing it from
/// `ns.workers`. Fixed by making the registry store a `Vec` of senders per namespace.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_multi_worker_reschedule_on_disconnect_e2e() {
    let mut cluster = TestCluster::new();
    let w1 = cluster.add_worker().await;
    let w2 = cluster.add_worker().await;

    cluster.create_namespace("ns", always_on_spec()).await;
    cluster.converge().await;
    cluster.assert_workload_running("ns", "echo").await;

    // Identify which worker is hosting the pod.
    let hosting_worker = cluster.worker_id_for_workload("ns", "echo").await;

    let other_worker = if hosting_worker == w1 {
        w2.clone()
    } else {
        w1.clone()
    };

    // Disconnect the hosting worker.
    cluster.disconnect_worker(&hosting_worker).await;
    cluster.converge().await;

    // The orchestrator should reschedule to the remaining worker.
    cluster.assert_workload_running("ns", "echo").await;
    let new_worker = cluster.worker_id_for_workload("ns", "echo").await;
    assert_eq!(
        new_worker, other_worker,
        "workload should have been rescheduled to the other worker"
    );
}
