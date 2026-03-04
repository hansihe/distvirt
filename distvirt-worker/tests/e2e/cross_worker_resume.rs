use std::net::Ipv4Addr;

use distvirt_worker_protocol::{
    ContainerConfig, ContainerSpec, NetworkConfig, PodNetworkConfig, PoolId, WorkerCommand,
    WorkerEvent,
};

use super::common::*;

/// Test cross-worker suspend/resume via a shared snapshot pool.
///
/// Worker A suspends a pod into a shared pool directory, then worker B resumes
/// the pod from that same shared pool. This validates the "live migration via
/// shared pool" scenario from the storage-pools design doc.
#[tokio::test]
async fn test_cross_worker_shared_pool_resume() -> anyhow::Result<()> {
    if !should_run() {
        return Ok(());
    }

    let _ = env_logger::try_init();

    // Create a shared tmpdir that both workers can access
    let shared_dir = tempfile::tempdir()?;
    let shared_pool_id = PoolId::from("shared");

    let extra_pools = vec![(shared_pool_id.clone(), shared_dir.path().to_path_buf())];

    // --- Start two workers, both with the shared pool ---
    let (mut conn_a, handle_a) =
        setup_with_pools("worker-a", extra_pools.clone()).await?;
    let (mut conn_b, handle_b) =
        setup_with_pools("worker-b", extra_pools).await?;

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
            namespace_id: "ns-shared".into(),
            network: network.clone(),
        })
        .await?;

    recv_until(&mut conn_a, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::NamespaceCreated { .. })
    })
    .await?;

    // --- Launch pod on worker A ---
    conn_a
        .send_command(&WorkerCommand::LaunchPod {
            namespace_id: "ns-shared".into(),
            pod_id: "pod-migrate".into(),
            network: pod_network.clone(),
            containers: vec![ContainerSpec {
                container_id: "ctr-migrate".into(),
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
        matches!(e, WorkerEvent::PodRunning { pod_id, .. } if pod_id == "pod-migrate")
    })
    .await?;
    eprintln!("e2e: pod-migrate running on worker-a");

    // --- Suspend pod on worker A into the shared pool ---
    conn_a
        .send_command(&WorkerCommand::SuspendPod {
            namespace_id: "ns-shared".into(),
            pod_id: "pod-migrate".into(),
            snapshot_id: "snap-shared".into(),
            pool_id: shared_pool_id.clone(),
        })
        .await?;

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
            snapshot_size_bytes, ..
        } => {
            assert!(
                *snapshot_size_bytes > 0,
                "snapshot should have non-zero size, got {}",
                snapshot_size_bytes
            );
            eprintln!(
                "e2e: pod suspended on worker-a, snapshot_size={}",
                snapshot_size_bytes
            );
        }
        other => panic!("expected PodSuspended, got {:?}", other),
    }

    // --- Create namespace on worker B ---
    conn_b
        .send_command(&WorkerCommand::CreateNamespace {
            namespace_id: "ns-shared".into(),
            network: network.clone(),
        })
        .await?;

    recv_until(&mut conn_b, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::NamespaceCreated { .. })
    })
    .await?;

    // --- Resume pod on worker B from the shared pool ---
    conn_b
        .send_command(&WorkerCommand::ResumePod {
            namespace_id: "ns-shared".into(),
            pod_id: "pod-migrated".into(),
            snapshot_id: "snap-shared".into(),
            network: pod_network,
            pool_id: shared_pool_id.clone(),
        })
        .await?;

    recv_until(&mut conn_b, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodRunning { pod_id, .. } if pod_id == "pod-migrated")
    })
    .await?;
    eprintln!("e2e: pod-migrated running on worker-b after cross-worker resume");

    // --- Stop pod on worker B ---
    conn_b
        .send_command(&WorkerCommand::StopPod {
            namespace_id: "ns-shared".into(),
            pod_id: "pod-migrated".into(),
            graceful: true,
        })
        .await?;

    recv_until(&mut conn_b, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodExited { pod_id, .. } if pod_id == "pod-migrated")
    })
    .await?;
    eprintln!("e2e: pod-migrated stopped on worker-b");

    // --- Shutdown both workers ---
    shutdown_worker(&mut conn_a, handle_a).await?;
    shutdown_worker(&mut conn_b, handle_b).await?;

    eprintln!("e2e: cross-worker shared pool suspend/resume succeeded!");

    Ok(())
}
