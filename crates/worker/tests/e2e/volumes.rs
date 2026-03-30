use distvirt_worker_protocol::{
    ContainerConfig, ContainerSpec, NamespaceId, PodId, VolumeMountSpec, VolumeSpec, VolumeType,
    WorkerCommand, WorkerEvent, WorkerId,
};

use super::common::*;

/// Launch a pod with an empty_dir volume mounted into the container, write a
/// file to it from inside the guest, and verify the output.
#[tokio::test]
async fn test_empty_dir_volume() -> anyhow::Result<()> {
    if !should_run() {
        return Ok(());
    }

    let (mut conn, worker_handle) = setup().await?;

    conn.send_command(&WorkerCommand::CreateNamespace {
        namespace_id: NamespaceId::new("ns-vol", 0),
        network: test_network_config(),
    })
    .await?;

    let event = recv_event_timeout(&mut conn, EVENT_TIMEOUT).await?;
    assert!(
        matches!(&event, WorkerEvent::NamespaceCreated { namespace_id } if namespace_id.name() == "ns-vol"),
        "expected NamespaceCreated, got {:?}",
        event
    );

    register_pod_endpoint(
        &mut conn,
        "ns-vol",
        &test_pod_network_config(),
        WorkerId(1),
    )
    .await?;

    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: NamespaceId::new("ns-vol", 0),
        pod_id: PodId(1),
        network: test_pod_network_config(),
        containers: vec![ContainerSpec {
            container_id: "ctr-vol".into(),
            image_ref: "docker.io/library/distvirt-test-containers:latest".into(),
            config: ContainerConfig {
                command: Some(vec!["/bin/test-containers".into()]),
                args: Some(vec![
                    "volume-check".into(),
                    "--path".into(),
                    "/mnt/data/test.txt".into(),
                    "--data".into(),
                    "hello-from-volume".into(),
                ]),
                env: vec![],
                working_dir: None,
                user: None,
                hostname: None,
                capture_output: true,
                stdin: false,
                volume_mounts: vec![VolumeMountSpec {
                    name: "data".into(),
                    mount_path: "/mnt/data".into(),
                }],
            },
        }],
        resources: None,
        volumes: vec![VolumeSpec {
            name: "data".into(),
            volume_type: VolumeType::EmptyDir { size_mb: 64 },
        }],
    })
    .await?;

    // Wait for PodRunning
    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodRunning { .. })
    })
    .await?;

    // Read log stream — should contain the written data
    let log_str = drain_log_stream(&mut conn).await?;
    assert!(
        log_str.contains("volume-check: hello-from-volume"),
        "expected 'volume-check: hello-from-volume' in log output, got: {:?}",
        log_str
    );

    // Wait for PodExited with success
    let event = recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodExited { .. })
    })
    .await?;
    assert!(
        matches!(&event, WorkerEvent::PodExited { exit_code, .. } if *exit_code == 0),
        "expected exit_code 0, got {:?}",
        event
    );

    shutdown_worker(&mut conn, worker_handle).await?;

    Ok(())
}
