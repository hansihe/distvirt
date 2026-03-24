use std::time::Duration;

use distvirt_orchestrator::types::*;
use distvirt_worker_protocol::{ConfigDataFile, VolumeSpec, VolumeType};

use crate::harness::TestCluster;
use crate::harness::spec_builders::{
    activation_with_volumes_spec, config_data_spec, container_spec, empty_dir_spec,
    mixed_volumes_spec,
};

/// Workload with an empty_dir volume starts and runs normally.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_empty_dir_volume_lifecycle() {
    let mut cluster = TestCluster::new();
    let _w1 = cluster.add_worker().await;

    cluster.create_namespace("ns", empty_dir_spec()).await;
    cluster.converge().await;
    cluster.assert_workload_running("ns", "app").await;

    cluster.delete_namespace("ns").await;
    cluster.converge().await;
    cluster.assert_namespace_absent("ns").await;
}

/// Workload with a config_data volume starts and runs normally.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_config_data_volume_lifecycle() {
    let mut cluster = TestCluster::new();
    let _w1 = cluster.add_worker().await;

    cluster.create_namespace("ns", config_data_spec()).await;
    cluster.converge().await;
    cluster.assert_workload_running("ns", "app").await;

    cluster.delete_namespace("ns").await;
    cluster.converge().await;
    cluster.assert_namespace_absent("ns").await;
}

/// Workload with both empty_dir and config_data volumes starts and runs.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_mixed_volumes_lifecycle() {
    let mut cluster = TestCluster::new();
    let _w1 = cluster.add_worker().await;

    cluster.create_namespace("ns", mixed_volumes_spec()).await;
    cluster.converge().await;
    cluster.assert_workload_running("ns", "app").await;

    cluster.delete_namespace("ns").await;
    cluster.converge().await;
    cluster.assert_namespace_absent("ns").await;
}

/// Changing volume spec triggers pod restart (spec reconciliation).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_volume_change_restarts_pod() {
    let mut cluster = TestCluster::new();
    let _w1 = cluster.add_worker().await;

    cluster.create_namespace("ns", empty_dir_spec()).await;
    cluster.converge().await;
    cluster.assert_workload_running("ns", "app").await;

    let old_pod_id = cluster
        .namespace_status("ns")
        .await
        .workloads
        .get(&WorkloadName("app".to_string()))
        .expect("workload 'app' not found")
        .pod_id
        .clone()
        .expect("should have pod_id");

    // Change volume size.
    let mut new_spec = empty_dir_spec();
    new_spec
        .workloads
        .get_mut(&WorkloadName("app".to_string()))
        .unwrap()
        .volumes = vec![VolumeSpec {
        name: "scratch".to_string(),
        volume_type: VolumeType::EmptyDir { size_mb: 128 },
    }];

    cluster.update_namespace("ns", new_spec).await;
    cluster.converge().await;
    cluster.assert_workload_running("ns", "app").await;

    let new_pod_id = cluster
        .namespace_status("ns")
        .await
        .workloads
        .get(&WorkloadName("app".to_string()))
        .expect("workload 'app' not found")
        .pod_id
        .clone()
        .expect("should have pod_id");

    assert_ne!(
        old_pod_id, new_pod_id,
        "volume spec change should restart pod with a new pod_id"
    );
}

/// Adding a volume to an existing workload triggers pod restart.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_add_volume_to_running_workload() {
    let mut cluster = TestCluster::new();
    let _w1 = cluster.add_worker().await;

    // Start with no volumes (use empty_dir_spec but strip volumes).
    let mut no_vol_spec = empty_dir_spec();
    let wl = no_vol_spec
        .workloads
        .get_mut(&WorkloadName("app".to_string()))
        .unwrap();
    wl.volumes = vec![];
    wl.containers = vec![container_spec("docker.io/library/alpine:latest")];

    cluster.create_namespace("ns", no_vol_spec).await;
    cluster.converge().await;
    cluster.assert_workload_running("ns", "app").await;

    let old_pod_id = cluster
        .namespace_status("ns")
        .await
        .workloads
        .get(&WorkloadName("app".to_string()))
        .expect("workload 'app' not found")
        .pod_id
        .clone()
        .expect("should have pod_id");

    // Now update to add a volume.
    cluster.update_namespace("ns", empty_dir_spec()).await;
    cluster.converge().await;
    cluster.assert_workload_running("ns", "app").await;

    let new_pod_id = cluster
        .namespace_status("ns")
        .await
        .workloads
        .get(&WorkloadName("app".to_string()))
        .expect("workload 'app' not found")
        .pod_id
        .clone()
        .expect("should have pod_id");

    assert_ne!(
        old_pod_id, new_pod_id,
        "adding a volume should restart pod with a new pod_id"
    );
}

/// Changing config_data file contents triggers pod restart.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_config_data_content_change_restarts_pod() {
    let mut cluster = TestCluster::new();
    let _w1 = cluster.add_worker().await;

    cluster.create_namespace("ns", config_data_spec()).await;
    cluster.converge().await;
    cluster.assert_workload_running("ns", "app").await;

    let old_pod_id = cluster
        .namespace_status("ns")
        .await
        .workloads
        .get(&WorkloadName("app".to_string()))
        .expect("workload 'app' not found")
        .pod_id
        .clone()
        .expect("should have pod_id");

    // Change config file content.
    let mut new_spec = config_data_spec();
    new_spec
        .workloads
        .get_mut(&WorkloadName("app".to_string()))
        .unwrap()
        .volumes = vec![VolumeSpec {
        name: "cfg".to_string(),
        volume_type: VolumeType::ConfigData {
            files: vec![ConfigDataFile {
                path: "app.toml".to_string(),
                content: "[server]\nport = 9090\n".to_string(),
                mode: 0o644,
            }],
        },
    }];

    cluster.update_namespace("ns", new_spec).await;
    cluster.converge().await;
    cluster.assert_workload_running("ns", "app").await;

    let new_pod_id = cluster
        .namespace_status("ns")
        .await
        .workloads
        .get(&WorkloadName("app".to_string()))
        .expect("workload 'app' not found")
        .pod_id
        .clone()
        .expect("should have pod_id");

    assert_ne!(
        old_pod_id, new_pod_id,
        "config data content change should restart pod with a new pod_id"
    );
}

/// Activation-based workload with volumes: dormant -> activate -> suspend -> resume.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_activation_with_volumes_suspend_resume() {
    let mut cluster = TestCluster::new();
    let w1 = cluster.add_worker().await;

    let idle = Duration::from_secs(30);
    cluster
        .create_namespace("ns", activation_with_volumes_spec(idle))
        .await;
    cluster.converge().await;
    cluster.assert_workload_dormant("ns", "web").await;

    // Activate via traffic.
    cluster.send_activation_traffic("ns", "web-svc").await;
    cluster.assert_workload_running("ns", "web").await;

    // Deactivate and wait for suspend.
    cluster.deactivate_service("ns", "web-svc", &w1).await;
    cluster.advance_past_idle_timeout("ns", "web-svc").await;
    cluster.wait_workload_suspended("ns", "web").await;
    cluster.assert_workload_suspended("ns", "web").await;

    // Re-activate: should resume from snapshot.
    cluster.send_activation_traffic("ns", "web-svc").await;
    cluster.assert_workload_running("ns", "web").await;
}
