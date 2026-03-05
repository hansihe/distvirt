use std::net::Ipv4Addr;

use distvirt_worker_protocol::{
    ContainerConfig, ContainerSpec, NetworkConfig, PodNetworkConfig, PoolId, PoolInfo,
    WorkerCommand, WorkerEvent,
};

use super::common::*;

/// Test cross-worker artifact transfer via TCP streaming.
///
/// Worker A suspends a pod into its local pool, then a TransferArtifact command
/// streams the artifact over TCP to worker B's pool. Worker B resumes the pod
/// from the transferred artifact.
#[tokio::test]
async fn test_cross_worker_artifact_transfer() -> anyhow::Result<()> {
    if !should_run() {
        return Ok(());
    }

    let _ = env_logger::try_init();

    // Each worker gets its own separate pool directory (not shared).
    let pool_dir_a = tempfile::tempdir()?;
    let pool_dir_b = tempfile::tempdir()?;

    let pool_id_a = PoolId::from("pool-a");
    let pool_id_b = PoolId::from("pool-b");

    let pools_a = vec![PoolInfo {
        pool_id: pool_id_a.clone(),
        path: pool_dir_a.path().to_string_lossy().into_owned(),
        capacity_bytes: 0,
        available_bytes: 0,
    }];

    let pools_b = vec![PoolInfo {
        pool_id: pool_id_b.clone(),
        path: pool_dir_b.path().to_string_lossy().into_owned(),
        capacity_bytes: 0,
        available_bytes: 0,
    }];

    // Start both workers, capturing transfer_listen_port from WorkerReady.
    let ws_a = setup_with_pools_full("worker-a", pools_a).await?;
    let ws_b = setup_with_pools_full("worker-b", pools_b).await?;

    let mut conn_a = ws_a.conn;
    let handle_a = ws_a.handle;
    let mut conn_b = ws_b.conn;
    let handle_b = ws_b.handle;

    let transfer_port_b = ws_b
        .transfer_listen_port
        .expect("worker-b should have a transfer listen port");
    eprintln!("e2e: worker-b transfer listen port: {}", transfer_port_b);

    let network = NetworkConfig {
        subnet: Ipv4Addr::new(10, 0, 0, 0),
        gateway: Ipv4Addr::new(10, 0, 0, 1),
        prefix_len: 24,
        segment_id: None,
    };

    let pod_network = PodNetworkConfig {
        ip: Ipv4Addr::new(10, 0, 0, 2),
        mac: [0x02, 0x00, 0x00, 0x00, 0x00, 0x01],
        gateway: Ipv4Addr::new(10, 0, 0, 1),
        netmask: "255.255.255.0".into(),
    };

    // --- Create namespace on worker A ---
    conn_a
        .send_command(&WorkerCommand::CreateNamespace {
            namespace_id: "ns-xfer".into(),
            network: network.clone(),
        })
        .await?;

    recv_until(&mut conn_a, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::NamespaceCreated { .. })
    })
    .await?;

    // --- Launch a long-running pod on worker A ---
    conn_a
        .send_command(&WorkerCommand::LaunchPod {
            namespace_id: "ns-xfer".into(),
            pod_id: "pod-xfer".into(),
            network: pod_network.clone(),
            containers: vec![ContainerSpec {
                container_id: "ctr-xfer".into(),
                image_ref: "docker.io/library/alpine:latest".into(),
                config: ContainerConfig {
                    entrypoint: vec!["/bin/sleep".into()],
                    args: vec!["3600".into()],
                    env: vec![],
                    working_dir: None,
                    uid: None,
                    gid: None,
                    hostname: None,
                    capture_output: false,
                    stdin: false,
                },
            }],
        })
        .await?;

    recv_until(&mut conn_a, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodRunning { pod_id, .. } if pod_id == "pod-xfer")
    })
    .await?;
    eprintln!("e2e: pod-xfer running on worker-a");

    // --- Suspend pod on worker A into pool-a ---
    conn_a
        .send_command(&WorkerCommand::SuspendPod {
            namespace_id: "ns-xfer".into(),
            pod_id: "pod-xfer".into(),
            artifact_id: "snap-xfer".into(),
            pool_id: pool_id_a.clone(),
        })
        .await?;

    // Wait for two-phase write events.
    recv_until(&mut conn_a, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::ArtifactWriteStarted { .. })
    })
    .await?;
    eprintln!("e2e: received ArtifactWriteStarted on worker-a");

    recv_until(&mut conn_a, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::ArtifactWriteCommitted { .. })
    })
    .await?;
    eprintln!("e2e: received ArtifactWriteCommitted on worker-a");

    let event = recv_until(&mut conn_a, EVENT_TIMEOUT, |e| {
        matches!(
            e,
            WorkerEvent::PodSuspended { .. } | WorkerEvent::PodSuspendFailed { .. }
        )
    })
    .await?;

    if let WorkerEvent::PodSuspendFailed { error, .. } = &event {
        anyhow::bail!("suspend on worker-a failed: {}", error);
    }

    match &event {
        WorkerEvent::PodSuspended {
            artifact_size_bytes,
            ..
        } => {
            assert!(
                *artifact_size_bytes > 0,
                "snapshot should have non-zero size, got {}",
                artifact_size_bytes
            );
            eprintln!(
                "e2e: pod suspended on worker-a, artifact_size={}",
                artifact_size_bytes
            );
        }
        other => panic!("expected PodSuspended, got {:?}", other),
    }

    // --- Transfer artifact from worker-a (pool-a) to worker-b (pool-b) via TCP ---
    let dest_endpoint = format!("127.0.0.1:{}", transfer_port_b);
    conn_a
        .send_command(&WorkerCommand::TransferArtifact {
            transfer_id: 42,
            source_artifact_id: "snap-xfer".into(),
            source_pool_id: pool_id_a.clone(),
            dest_artifact_id: "snap-xfer-copy".into(),
            dest_pool_id: pool_id_b.clone(),
            dest_endpoint: Some(dest_endpoint),
        })
        .await?;
    eprintln!("e2e: sent TransferArtifact to worker-a");

    // The destination worker (B) receives the transfer and emits ArtifactTransferReceived.
    let event = recv_until(&mut conn_b, EVENT_TIMEOUT, |e| {
        matches!(
            e,
            WorkerEvent::ArtifactTransferReceived { .. } | WorkerEvent::TransferFailed { .. }
        )
    })
    .await?;

    match &event {
        WorkerEvent::ArtifactTransferReceived {
            transfer_id,
            dest_artifact_id,
            dest_pool_id,
            size_bytes,
            ..
        } => {
            assert_eq!(*transfer_id, 42, "transfer_id mismatch");
            assert_eq!(dest_artifact_id, "snap-xfer-copy");
            assert_eq!(dest_pool_id, "pool-b");
            assert!(*size_bytes > 0, "transferred artifact should have non-zero size");
            eprintln!(
                "e2e: artifact transfer received on worker-b, size={}",
                size_bytes
            );
        }
        WorkerEvent::TransferFailed { error, .. } => {
            anyhow::bail!("artifact transfer failed: {}", error);
        }
        other => panic!("unexpected event: {:?}", other),
    }

    // --- Create namespace on worker B and resume from the transferred artifact ---
    conn_b
        .send_command(&WorkerCommand::CreateNamespace {
            namespace_id: "ns-xfer".into(),
            network: network.clone(),
        })
        .await?;

    recv_until(&mut conn_b, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::NamespaceCreated { .. })
    })
    .await?;

    conn_b
        .send_command(&WorkerCommand::ResumePod {
            namespace_id: "ns-xfer".into(),
            pod_id: "pod-xfer-resumed".into(),
            artifact_id: "snap-xfer-copy".into(),
            network: pod_network,
            pool_id: pool_id_b.clone(),
        })
        .await?;

    recv_until(&mut conn_b, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodRunning { pod_id, .. } if pod_id == "pod-xfer-resumed")
    })
    .await?;
    eprintln!("e2e: pod-xfer-resumed running on worker-b after cross-worker transfer");

    // --- Stop pod on worker B ---
    conn_b
        .send_command(&WorkerCommand::StopPod {
            namespace_id: "ns-xfer".into(),
            pod_id: "pod-xfer-resumed".into(),
            graceful: true,
        })
        .await?;

    recv_until(&mut conn_b, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodExited { pod_id, .. } if pod_id == "pod-xfer-resumed")
    })
    .await?;
    eprintln!("e2e: pod-xfer-resumed stopped on worker-b");

    // --- Shutdown both workers ---
    shutdown_worker(&mut conn_a, handle_a).await?;
    shutdown_worker(&mut conn_b, handle_b).await?;

    eprintln!("e2e: cross-worker artifact transfer test succeeded!");

    Ok(())
}
