use std::time::Duration;

use distvirt_worker_protocol::{ContainerConfig, ContainerSpec, PodId, WorkerCommand, WorkerEvent, WorkerId};

use super::common::*;

#[tokio::test]
async fn test_launch_pod_echo() -> anyhow::Result<()> {
    if !should_run() {
        return Ok(());
    }

    let (mut conn, worker_handle) = setup().await?;

    // Create namespace
    conn.send_command(&WorkerCommand::CreateNamespace {
        namespace_id: "ns-echo".into(),
        network: test_network_config(),
    })
    .await?;

    let event = recv_event_timeout(&mut conn, EVENT_TIMEOUT).await?;
    assert!(
        matches!(&event, WorkerEvent::NamespaceCreated { namespace_id } if namespace_id == "ns-echo"),
        "expected NamespaceCreated, got {:?}",
        event
    );

    // Register pod endpoint before launch
    register_pod_endpoint(
        &mut conn,
        "ns-echo",
        &test_pod_network_config(),
        WorkerId(1),
    )
    .await?;

    // Launch pod
    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-echo".into(),
        pod_id: PodId(1),
        network: test_pod_network_config(),
        containers: vec![ContainerSpec {
            container_id: "ctr-echo".into(),
            image_ref: "docker.io/library/distvirt-test-containers:latest".into(),
            config: ContainerConfig {
                command: Some(vec!["/bin/test-containers".into()]),
                args: Some(vec!["echo".into(), "hello".into(), "world".into()]),
                env: vec![],
                working_dir: None,
                user: None,
                hostname: None,
                capture_output: true,
                stdin: false,
                volume_mounts: vec![],
            },
        }],
        resources: None,
        volumes: vec![],
    })
    .await?;

    // Wait for PodRunning
    let event = recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodRunning { .. })
    })
    .await?;
    assert!(
        matches!(&event, WorkerEvent::PodRunning { namespace_id, pod_id }
            if namespace_id == "ns-echo" && *pod_id == PodId(1)),
        "unexpected event: {:?}",
        event
    );

    // Read log stream for "hello world"
    let log_str = drain_log_stream(&mut conn).await?;
    assert!(
        log_str.contains("hello world"),
        "expected 'hello world' in log output, got: {:?}",
        log_str
    );

    // Wait for PodExited
    let event = recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodExited { .. })
    })
    .await?;
    assert!(
        matches!(&event, WorkerEvent::PodExited { exit_code, .. } if *exit_code == 0),
        "expected exit_code 0, got {:?}",
        event
    );

    // Verify clean worker shutdown (validates VM process actually exited).
    shutdown_worker(&mut conn, worker_handle).await?;

    Ok(())
}

#[tokio::test]
async fn test_pod_exit_code() -> anyhow::Result<()> {
    if !should_run() {
        return Ok(());
    }

    let (mut conn, worker_handle) = setup().await?;

    conn.send_command(&WorkerCommand::CreateNamespace {
        namespace_id: "ns-exit".into(),
        network: test_network_config(),
    })
    .await?;

    recv_event_timeout(&mut conn, EVENT_TIMEOUT).await?;

    register_pod_endpoint(
        &mut conn,
        "ns-exit",
        &test_pod_network_config(),
        WorkerId(1),
    )
    .await?;

    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-exit".into(),
        pod_id: PodId(1),
        network: test_pod_network_config(),
        containers: vec![ContainerSpec {
            container_id: "ctr-exit".into(),
            image_ref: "docker.io/library/distvirt-test-containers:latest".into(),
            config: ContainerConfig {
                command: Some(vec!["/bin/test-containers".into()]),
                args: Some(vec!["exit-code".into(), "--code".into(), "42".into()]),
                env: vec![],
                working_dir: None,
                user: None,
                hostname: None,
                capture_output: true,
                stdin: false,
                volume_mounts: vec![],
            },
        }],
        resources: None,
        volumes: vec![],
    })
    .await?;

    // Wait for PodRunning then PodExited
    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodRunning { .. })
    })
    .await?;

    let _log_output = drain_log_stream(&mut conn).await?;

    let event = recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodExited { .. })
    })
    .await?;

    assert!(
        matches!(&event, WorkerEvent::PodExited { exit_code, .. } if *exit_code == 42),
        "expected exit_code 42, got {:?}",
        event
    );

    // Verify clean worker shutdown (validates VM process actually exited).
    shutdown_worker(&mut conn, worker_handle).await?;

    Ok(())
}

#[tokio::test]
async fn test_stop_pod() -> anyhow::Result<()> {
    if !should_run() {
        return Ok(());
    }

    let (mut conn, worker_handle) = setup().await?;

    conn.send_command(&WorkerCommand::CreateNamespace {
        namespace_id: "ns-stop".into(),
        network: test_network_config(),
    })
    .await?;

    recv_event_timeout(&mut conn, EVENT_TIMEOUT).await?;

    register_pod_endpoint(
        &mut conn,
        "ns-stop",
        &test_pod_network_config(),
        WorkerId(1),
    )
    .await?;

    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-stop".into(),
        pod_id: PodId(1),
        network: test_pod_network_config(),
        containers: vec![ContainerSpec {
            container_id: "ctr-stop".into(),
            image_ref: "docker.io/library/distvirt-test-containers:latest".into(),
            config: ContainerConfig {
                command: Some(vec!["/bin/test-containers".into()]),
                args: Some(vec!["sleep".into()]),
                env: vec![],
                working_dir: None,
                user: None,
                hostname: None,
                capture_output: true,
                stdin: false,
                volume_mounts: vec![],
            },
        }],
        resources: None,
        volumes: vec![],
    })
    .await?;

    // Wait for PodRunning
    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodRunning { .. })
    })
    .await?;

    // Send graceful stop
    conn.send_command(&WorkerCommand::StopPod {
        namespace_id: "ns-stop".into(),
        pod_id: PodId(1),
        graceful: true,
    })
    .await?;

    // Wait for PodExited
    let event = recv_until(&mut conn, Duration::from_secs(30), |e| {
        matches!(e, WorkerEvent::PodExited { .. })
    })
    .await?;

    assert!(
        matches!(&event, WorkerEvent::PodExited { namespace_id, pod_id, .. }
            if namespace_id == "ns-stop" && *pod_id == PodId(1)),
        "unexpected event: {:?}",
        event
    );

    // Verify clean worker shutdown (validates VM process actually exited).
    shutdown_worker(&mut conn, worker_handle).await?;

    Ok(())
}

#[tokio::test]
async fn test_force_stop_pod() -> anyhow::Result<()> {
    if !should_run() {
        return Ok(());
    }

    let (mut conn, worker_handle) = setup().await?;

    conn.send_command(&WorkerCommand::CreateNamespace {
        namespace_id: "ns-force".into(),
        network: test_network_config(),
    })
    .await?;

    recv_event_timeout(&mut conn, EVENT_TIMEOUT).await?;

    register_pod_endpoint(
        &mut conn,
        "ns-force",
        &test_pod_network_config(),
        WorkerId(1),
    )
    .await?;

    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-force".into(),
        pod_id: PodId(1),
        network: test_pod_network_config(),
        containers: vec![ContainerSpec {
            container_id: "ctr-force".into(),
            image_ref: "docker.io/library/distvirt-test-containers:latest".into(),
            config: ContainerConfig {
                command: Some(vec!["/bin/test-containers".into()]),
                args: Some(vec!["sleep".into()]),
                env: vec![],
                working_dir: None,
                user: None,
                hostname: None,
                capture_output: true,
                stdin: false,
                volume_mounts: vec![],
            },
        }],
        resources: None,
        volumes: vec![],
    })
    .await?;

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodRunning { .. })
    })
    .await?;

    // Force stop (non-graceful) — aborts the supervisor immediately
    conn.send_command(&WorkerCommand::StopPod {
        namespace_id: "ns-force".into(),
        pod_id: PodId(1),
        graceful: false,
    })
    .await?;

    // Worker shutdown should still complete cleanly even after force stop
    shutdown_worker(&mut conn, worker_handle).await?;

    Ok(())
}

#[tokio::test]
async fn test_destroy_namespace() -> anyhow::Result<()> {
    if !should_run() {
        return Ok(());
    }

    let (mut conn, worker_handle) = setup().await?;

    conn.send_command(&WorkerCommand::CreateNamespace {
        namespace_id: "ns-destroy".into(),
        network: test_network_config(),
    })
    .await?;

    recv_event_timeout(&mut conn, EVENT_TIMEOUT).await?;

    // Launch a long-running pod inside the namespace
    register_pod_endpoint(
        &mut conn,
        "ns-destroy",
        &test_pod_network_config(),
        WorkerId(1),
    )
    .await?;

    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-destroy".into(),
        pod_id: PodId(1),
        network: test_pod_network_config(),
        containers: vec![ContainerSpec {
            container_id: "ctr-destroy".into(),
            image_ref: "docker.io/library/distvirt-test-containers:latest".into(),
            config: ContainerConfig {
                command: Some(vec!["/bin/test-containers".into()]),
                args: Some(vec!["sleep".into()]),
                env: vec![],
                working_dir: None,
                user: None,
                hostname: None,
                capture_output: true,
                stdin: false,
                volume_mounts: vec![],
            },
        }],
        resources: None,
        volumes: vec![],
    })
    .await?;

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodRunning { .. })
    })
    .await?;

    // Destroy the namespace — should tear down the running pod
    conn.send_command(&WorkerCommand::DestroyNamespace {
        namespace_id: "ns-destroy".into(),
    })
    .await?;

    // We should get a PodExited event from the cancelled pod
    let event = recv_until(&mut conn, Duration::from_secs(30), |e| {
        matches!(e, WorkerEvent::PodExited { .. })
    })
    .await?;

    assert!(
        matches!(&event, WorkerEvent::PodExited { namespace_id, pod_id, .. }
            if namespace_id == "ns-destroy" && *pod_id == PodId(1)),
        "unexpected event: {:?}",
        event
    );

    shutdown_worker(&mut conn, worker_handle).await?;

    Ok(())
}

#[tokio::test]
async fn test_pod_launch_failure() -> anyhow::Result<()> {
    if !should_run() {
        return Ok(());
    }

    let (mut conn, worker_handle) = setup().await?;

    conn.send_command(&WorkerCommand::CreateNamespace {
        namespace_id: "ns-fail".into(),
        network: test_network_config(),
    })
    .await?;

    recv_event_timeout(&mut conn, EVENT_TIMEOUT).await?;

    // Launch with a non-existent image — should trigger PodFailed
    register_pod_endpoint(
        &mut conn,
        "ns-fail",
        &test_pod_network_config(),
        WorkerId(1),
    )
    .await?;

    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-fail".into(),
        pod_id: PodId(1),
        network: test_pod_network_config(),
        containers: vec![ContainerSpec {
            container_id: "ctr-fail".into(),
            image_ref: "docker.io/library/this-image-does-not-exist:99.99.99".into(),
            config: ContainerConfig {
                command: Some(vec!["/bin/true".into()]),
                args: Some(vec![]),
                env: vec![],
                working_dir: None,
                user: None,
                hostname: None,
                capture_output: false,
                stdin: false,
                volume_mounts: vec![],
            },
        }],
        resources: None,
        volumes: vec![],
    })
    .await?;

    let event = recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodFailed { .. })
    })
    .await?;

    assert!(
        matches!(&event, WorkerEvent::PodFailed { namespace_id, pod_id, error }
            if namespace_id == "ns-fail" && *pod_id == PodId(1) && !error.is_empty()),
        "unexpected event: {:?}",
        event
    );

    shutdown_worker(&mut conn, worker_handle).await?;

    Ok(())
}

#[tokio::test]
async fn test_env_and_working_dir() -> anyhow::Result<()> {
    if !should_run() {
        return Ok(());
    }

    let (mut conn, worker_handle) = setup().await?;

    conn.send_command(&WorkerCommand::CreateNamespace {
        namespace_id: "ns-env".into(),
        network: test_network_config(),
    })
    .await?;

    recv_event_timeout(&mut conn, EVENT_TIMEOUT).await?;

    // Launch a pod that prints an env var and the working directory
    register_pod_endpoint(
        &mut conn,
        "ns-env",
        &test_pod_network_config(),
        WorkerId(1),
    )
    .await?;

    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-env".into(),
        pod_id: PodId(1),
        network: test_pod_network_config(),
        containers: vec![ContainerSpec {
            container_id: "ctr-env".into(),
            image_ref: "docker.io/library/distvirt-test-containers:latest".into(),
            config: ContainerConfig {
                command: Some(vec!["/bin/test-containers".into()]),
                args: Some(vec![
                    "env-check".into(),
                    "--var".into(),
                    "MY_VAR".into(),
                    "--pwd".into(),
                ]),
                env: vec!["MY_VAR=hello_from_env".into()],
                working_dir: Some("/tmp".into()),
                user: None,
                hostname: None,
                capture_output: true,
                stdin: false,
                volume_mounts: vec![],
            },
        }],
        resources: None,
        volumes: vec![],
    })
    .await?;

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodRunning { .. })
    })
    .await?;

    // Read log stream
    let log_str = drain_log_stream(&mut conn).await?;
    assert!(
        log_str.contains("MY_VAR=hello_from_env"),
        "expected env var in output, got: {:?}",
        log_str
    );
    assert!(
        log_str.contains("/tmp"),
        "expected /tmp as working dir in output, got: {:?}",
        log_str
    );

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodExited { .. })
    })
    .await?;

    shutdown_worker(&mut conn, worker_handle).await?;

    Ok(())
}
