//! Tests that exercise the client crate's API against the in-process orchestrator.
//!
//! These tests use `distvirt_client::operations` (apply, sync, down, get_status)
//! through the same gRPC service layer a real client would, but over an in-process
//! transport instead of TCP.

use std::collections::HashMap;

use distvirt_client::operations;
use distvirt_client_protocol as proto;

use crate::harness::TestCluster;

/// Build a simple always-on workload spec in proto format.
fn always_on_proto_spec() -> proto::NamespaceSpec {
    let mut workloads = HashMap::new();
    workloads.insert(
        "echo".to_string(),
        proto::WorkloadSpec {
            network: Some(proto::PodNetworkConfig {
                ip: String::new(),
                mac: String::new(),
            }),
            containers: vec![proto::ContainerSpec {
                name: "main".to_string(),
                image: "docker.io/library/alpine:latest".to_string(),
                config: Some(proto::ContainerConfig {
                    command: vec!["/bin/echo".to_string()],
                    has_command: true,
                    args: vec!["hello".to_string()],
                    has_args: true,
                    ..Default::default()
                }),
            }],
            respects_demand: false,
            suspend_on_idle: false,
            ..Default::default()
        },
    );

    let mut services = HashMap::new();
    services.insert(
        "echo-svc".to_string(),
        proto::ServiceSpec {
            workload_id: "echo".to_string(),
            network: Some(proto::ServiceNetworkConfig {
                ip: String::new(),
                mac: String::new(),
            }),
            ..Default::default()
        },
    );

    proto::NamespaceSpec {
        network: Some(proto::NetworkConfig {
            subnet: "172.16.0.0/24".to_string(),
        }),
        workloads,
        services,
    }
}

/// Build a two-workload spec in proto format.
fn two_workload_proto_spec() -> proto::NamespaceSpec {
    let mut workloads = HashMap::new();
    for name in &["alpha", "beta"] {
        workloads.insert(
            name.to_string(),
            proto::WorkloadSpec {
                network: Some(proto::PodNetworkConfig {
                    ip: String::new(),
                    mac: String::new(),
                }),
                containers: vec![proto::ContainerSpec {
                    name: "main".to_string(),
                    image: "docker.io/library/alpine:latest".to_string(),
                    config: Some(proto::ContainerConfig {
                        command: vec!["/bin/echo".to_string()],
                        has_command: true,
                        args: vec!["hello".to_string()],
                        has_args: true,
                        ..Default::default()
                    }),
                }],
                respects_demand: false,
                suspend_on_idle: false,
                ..Default::default()
            },
        );
    }

    proto::NamespaceSpec {
        network: Some(proto::NetworkConfig {
            subnet: "172.16.0.0/24".to_string(),
        }),
        workloads,
        services: HashMap::new(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Apply a namespace via the client, verify it converges to running,
/// then delete it via the client.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_client_apply_and_delete() {
    let mut cluster = TestCluster::new();
    let _w1 = cluster.add_worker().await;

    let mut client = cluster.client();
    let spec = always_on_proto_spec();

    // Apply creates the namespace.
    let outcome = operations::apply(&mut client, "ns", &spec).await.unwrap();
    assert!(matches!(outcome, operations::ApplyOutcome::Created));

    cluster.converge().await;
    cluster.assert_workload_running("ns", "echo").await;

    // Apply again should patch (idempotent).
    let outcome = operations::apply(&mut client, "ns", &spec).await.unwrap();
    assert!(matches!(outcome, operations::ApplyOutcome::Patched));

    // Delete via client.
    operations::down(&mut client, "ns").await.unwrap();
    cluster.converge().await;
    cluster.assert_namespace_absent("ns").await;
}

/// Sync a namespace via the client, verify it converges to running.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_client_sync() {
    let mut cluster = TestCluster::new();
    let _w1 = cluster.add_worker().await;

    let mut client = cluster.client();
    let spec = always_on_proto_spec();

    let outcome = operations::sync(&mut client, "ns", &spec).await.unwrap();
    assert!(matches!(outcome, operations::SyncOutcome::Created));

    cluster.converge().await;
    cluster.assert_workload_running("ns", "echo").await;

    // Sync again should update (not create).
    let outcome = operations::sync(&mut client, "ns", &spec).await.unwrap();
    assert!(matches!(outcome, operations::SyncOutcome::Synced));
}

/// Get namespace status via the client and verify workload states.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_client_get_status() {
    let mut cluster = TestCluster::new();
    let _w1 = cluster.add_worker().await;

    let mut client = cluster.client();
    let spec = always_on_proto_spec();

    operations::apply(&mut client, "ns", &spec).await.unwrap();
    cluster.converge().await;

    let status = operations::get_status(&mut client, "ns").await.unwrap();
    assert_eq!(status.namespace_id, "ns");

    // Workload should be running.
    let wl = status.workloads.get("echo").expect("workload 'echo' not found");
    let state = wl.state.as_ref().expect("missing workload state");
    assert!(
        state.state.as_ref().is_some_and(|s| matches!(s, proto::workload_state::State::Running(_))),
        "expected running, got {:?}",
        state
    );
}

/// Apply with multiple workloads, verify both converge.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_client_multi_workload() {
    let mut cluster = TestCluster::new();
    let _w1 = cluster.add_worker().await;

    let mut client = cluster.client();
    let spec = two_workload_proto_spec();

    operations::apply(&mut client, "ns", &spec).await.unwrap();
    cluster.converge().await;

    let status = operations::get_status(&mut client, "ns").await.unwrap();
    for name in &["alpha", "beta"] {
        let wl = status.workloads.get(*name).unwrap_or_else(|| panic!("workload '{}' not found", name));
        let state = wl.state.as_ref().expect("missing state");
        assert!(
            state.state.as_ref().is_some_and(|s| matches!(s, proto::workload_state::State::Running(_))),
            "workload '{}': expected running, got {:?}",
            name,
            state
        );
    }
}

/// Verify that get_status for a non-existent namespace returns an error.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_client_status_not_found() {
    let cluster = TestCluster::new();
    let mut client = cluster.client();

    let result = operations::get_status(&mut client, "nonexistent").await;
    assert!(result.is_err(), "expected error for nonexistent namespace");
}
