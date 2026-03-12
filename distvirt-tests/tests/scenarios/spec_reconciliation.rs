use std::time::Duration;

use distvirt_orchestrator::types::*;

use crate::harness::TestCluster;
use crate::harness::spec_builders::{
    activation_spec, always_on_spec, container_spec, two_workload_spec,
};

/// Update namespace spec with new image -> pod restarts.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_image_change_restarts_running_pod() {
    let mut cluster = TestCluster::new();
    let _w1 = cluster.add_worker().await;

    cluster.create_namespace("ns", always_on_spec()).await;
    cluster.converge().await;
    cluster.assert_workload_running("ns", "echo");

    let old_pod_id = cluster
        .workload_state("ns", "echo")
        .pod_id()
        .expect("should have pod_id")
        .clone();

    // Modify spec: change container image.
    let mut new_spec = always_on_spec();
    new_spec
        .workloads
        .get_mut(&WorkloadId("echo".to_string()))
        .unwrap()
        .containers = vec![container_spec("docker.io/library/alpine:3.19")];

    cluster.update_namespace("ns", new_spec).await;
    cluster.converge().await;
    cluster.assert_workload_running("ns", "echo");

    let new_pod_id = cluster
        .workload_state("ns", "echo")
        .pod_id()
        .expect("should have pod_id")
        .clone();

    assert_ne!(
        old_pod_id, new_pod_id,
        "image change should restart pod with a new pod_id"
    );
}

/// Add a new workload to a running namespace.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_add_workload_to_existing_namespace() {
    let mut cluster = TestCluster::new();
    let _w1 = cluster.add_worker().await;

    cluster.create_namespace("ns", always_on_spec()).await;
    cluster.converge().await;
    cluster.assert_workload_running("ns", "echo");

    // Update to two-workload spec. This adds "echo-a" and "echo-b" while removing "echo".
    cluster.update_namespace("ns", two_workload_spec()).await;
    cluster.converge().await;

    // Both new workloads should be running.
    cluster.assert_workload_running("ns", "echo-a");
    cluster.assert_workload_running("ns", "echo-b");
}

/// Remove a workload from a running namespace.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_remove_workload_from_namespace() {
    let mut cluster = TestCluster::new();
    let _w1 = cluster.add_worker().await;

    cluster
        .create_namespace("ns", two_workload_spec())
        .await;
    cluster.converge().await;
    cluster.assert_workload_running("ns", "echo-a");
    cluster.assert_workload_running("ns", "echo-b");

    // Update to single-workload spec (just always_on_spec which has "echo").
    cluster.update_namespace("ns", always_on_spec()).await;
    cluster.converge().await;

    // "echo" should be running, "echo-a" and "echo-b" should be gone.
    cluster.assert_workload_running("ns", "echo");
    let ns = cluster.namespace("ns");
    assert!(
        !ns.workloads.contains_key(&WorkloadId("echo-a".to_string())),
        "echo-a should be removed"
    );
    assert!(
        !ns.workloads.contains_key(&WorkloadId("echo-b".to_string())),
        "echo-b should be removed"
    );
}

/// Changing image while workload is suspended should invalidate the snapshot.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_image_change_on_suspended_workload() {
    let mut cluster = TestCluster::new();
    let w1 = cluster.add_worker().await;
    let mut events = cluster.subscribe_events("ns-img");

    cluster
        .create_namespace("ns-img", activation_spec(Duration::from_secs(30)))
        .await;
    cluster.converge().await;
    cluster.assert_workload_dormant("ns-img", "web");

    // Activate -> Running -> deactivate -> suspend.
    cluster.send_activation_traffic("ns-img", "web-svc").await;
    cluster.assert_workload_running("ns-img", "web");

    cluster.deactivate_service("ns-img", "web-svc", &w1).await;
    cluster.advance_past_idle_timeout("ns-img", "web-svc").await;
    cluster.wait_for_event(&mut events, |e| matches!(e,
        SmNamespaceEvent::Workload { workload_id, event: SmWorkloadEvent::PodSuspended { .. } }
        if workload_id.0 == "web"
    )).await;
    cluster.assert_workload_suspended("ns-img", "web");

    // Modify spec: change container image.
    let mut new_spec = activation_spec(Duration::from_secs(30));
    new_spec
        .workloads
        .get_mut(&WorkloadId("web".to_string()))
        .unwrap()
        .containers = vec![container_spec("docker.io/library/alpine:3.19")];

    cluster.update_namespace("ns-img", new_spec).await;
    // Extra converge to let spec reconciliation propagate.
    cluster.converge().await;
    cluster.advance_time(Duration::from_secs(2)).await;

    // Snapshot invalidated, no demand → should go back to Dormant.
    cluster.assert_workload_dormant("ns-img", "web");

    // Re-activate and verify it reaches Running with the new image.
    cluster.send_activation_traffic("ns-img", "web-svc").await;
    cluster.assert_workload_running("ns-img", "web");
}
