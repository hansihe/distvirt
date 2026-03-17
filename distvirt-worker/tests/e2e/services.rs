use std::net::Ipv4Addr;
use std::time::Duration;

use futures_lite::io::AsyncReadExt;

use distvirt_worker_protocol::{
    ActivatorConfig, BackendNeed, ContainerConfig, ContainerSpec, EndpointKind, EndpointPodBackend,
    EndpointSpec, RegistryEntry, ServicePolicy, WorkerCommand, WorkerEvent,
};

use super::common::*;

#[tokio::test]
async fn test_registry_sync() -> anyhow::Result<()> {
    if !should_run() {
        return Ok(());
    }

    let (mut conn, worker_handle) = setup().await?;

    conn.send_command(&WorkerCommand::CreateNamespace {
        namespace_id: "ns-dns".into(),
        network: test_network_config(),
    })
    .await?;

    recv_event_timeout(&mut conn, EVENT_TIMEOUT).await?;

    // Sync a DNS registry entry
    conn.send_command(&WorkerCommand::RegistrySync {
        namespace_id: "ns-dns".into(),
        entries: vec![RegistryEntry {
            name: "myservice".into(),
            ip: Ipv4Addr::new(10, 0, 0, 99),
        }],
    })
    .await?;

    // Launch a pod that resolves the name via the gateway DNS
    register_pod_endpoint(
        &mut conn,
        "ns-dns",
        &test_pod_network_config(),
        "test-worker",
    )
    .await?;

    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-dns".into(),
        pod_id: "pod-dns".into(),
        network: test_pod_network_config(),
        containers: vec![ContainerSpec {
            container_id: "ctr-dns".into(),
            image_ref: "docker.io/library/distvirt-test-containers:latest".into(),
            config: ContainerConfig {
                entrypoint: vec!["/bin/test-containers".into()],
                args: vec!["dns-lookup".into(), "--host".into(), "myservice".into()],
                env: vec![],
                working_dir: None,
                uid: None,
                gid: None,
                hostname: None,
                capture_output: true,
                stdin: false,
            },
        }],
        resources: None,
    })
    .await?;

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodRunning { .. })
    })
    .await?;

    // Read log stream — verify the DNS resolution returned our registered IP
    let log_str = drain_log_stream(&mut conn).await?;
    assert!(
        log_str.contains("10.0.0.99"),
        "expected DNS to resolve myservice to 10.0.0.99, got: {:?}",
        log_str
    );

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodExited { .. })
    })
    .await?;

    shutdown_worker(&mut conn, worker_handle).await?;

    Ok(())
}

#[tokio::test]
async fn test_tcp_activator_activation() -> anyhow::Result<()> {
    if !should_run() {
        return Ok(());
    }

    let (mut conn, worker_handle) = setup_with_activators().await?;

    // Create namespace
    conn.send_command(&WorkerCommand::CreateNamespace {
        namespace_id: "ns-tcp-act".into(),
        network: test_network_config(),
    })
    .await?;

    let event = recv_event_timeout(&mut conn, EVENT_TIMEOUT).await?;
    assert!(
        matches!(&event, WorkerEvent::NamespaceCreated { namespace_id } if namespace_id == "ns-tcp-act"),
        "expected NamespaceCreated, got {:?}",
        event
    );

    // Create a service with TCP activator via EndpointSync
    conn.send_command(&WorkerCommand::EndpointSync {
        namespace_id: "ns-tcp-act".into(),
        endpoints: vec![EndpointSpec {
            ip: Ipv4Addr::new(10, 0, 0, 99),
            kind: EndpointKind::Service {
                service_id: "svc-tcp".into(),
                policy: ServicePolicy {
                    buffer_frames: 64,
                    timeout_ms: 30000,
                    activator: Some(ActivatorConfig::Tcp {
                        ports: None,
                        tcp_only: true,
                        max_flows: 1024,
                    }),
                },
                backend: None,
            },
        }],
    })
    .await?;

    // Launch a pod that sends a TCP SYN to the service IP
    register_pod_endpoint(
        &mut conn,
        "ns-tcp-act",
        &test_pod_network_config(),
        "test-worker",
    )
    .await?;

    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-tcp-act".into(),
        pod_id: "pod-tcp-act".into(),
        network: test_pod_network_config(),
        containers: vec![ContainerSpec {
            container_id: "ctr-tcp-act".into(),
            image_ref: "docker.io/library/distvirt-test-containers:latest".into(),
            config: ContainerConfig {
                entrypoint: vec!["/bin/test-containers".into()],
                args: vec![
                    "send".into(),
                    "--host".into(),
                    "10.0.0.99".into(),
                    "--port".into(),
                    "80".into(),
                    "--data".into(),
                    "trigger\n".into(),
                    "--timeout".into(),
                    "5".into(),
                ],
                env: vec![],
                working_dir: None,
                uid: None,
                gid: None,
                hostname: None,
                capture_output: true,
                stdin: false,
            },
        }],
        resources: None,
    })
    .await?;

    // Wait for the TCP activator to signal backend need.
    // Note: ServiceBackendNeed may arrive before PodRunning because the SYN
    // is sent as soon as the guest network is up, so we must wait for it first.
    let event = recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(
            e,
            WorkerEvent::ServiceBackendNeed {
                need: BackendNeed::Traffic,
                ..
            }
        )
    })
    .await?;

    assert!(
        matches!(&event, WorkerEvent::ServiceBackendNeed { namespace_id, service_id, need: BackendNeed::Traffic }
            if namespace_id == "ns-tcp-act" && service_id == "svc-tcp"),
        "unexpected event: {:?}",
        event
    );

    // Wait for pod exit (test-containers send --timeout 5 will time out)
    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodExited { .. })
    })
    .await?;

    shutdown_worker(&mut conn, worker_handle).await?;

    Ok(())
}

#[tokio::test]
async fn test_service_backend_ready_forward() -> anyhow::Result<()> {
    if !should_run() {
        return Ok(());
    }

    let (mut conn, worker_handle) = setup().await?;

    // Create namespace
    conn.send_command(&WorkerCommand::CreateNamespace {
        namespace_id: "ns-svc-fwd".into(),
        network: test_network_config(),
    })
    .await?;

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::NamespaceCreated { .. })
    })
    .await?;

    // Create service at 10.0.0.99 (no activator) via EndpointSync
    conn.send_command(&WorkerCommand::EndpointSync {
        namespace_id: "ns-svc-fwd".into(),
        endpoints: vec![EndpointSpec {
            ip: Ipv4Addr::new(10, 0, 0, 99),
            kind: EndpointKind::Service {
                service_id: "svc-fwd".into(),
                policy: ServicePolicy {
                    buffer_frames: 64,
                    timeout_ms: 30000,
                    activator: None,
                },
                backend: None,
            },
        }],
    })
    .await?;

    // Launch backend pod that listens on port 80 at the service VIP.
    register_pod_endpoint(
        &mut conn,
        "ns-svc-fwd",
        &test_pod_network_config(),
        "test-worker",
    )
    .await?;

    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-svc-fwd".into(),
        pod_id: "pod-backend".into(),
        network: test_pod_network_config(),
        containers: vec![ContainerSpec {
            container_id: "ctr-backend".into(),
            image_ref: "docker.io/library/distvirt-test-containers:latest".into(),
            config: ContainerConfig {
                entrypoint: vec!["/bin/test-containers".into()],
                args: vec![
                    "recv".into(),
                    "--port".into(),
                    "80".into(),
                    "--expected".into(),
                    "hello-service\n".into(),
                    "--response".into(),
                    "ok".into(),
                    "--timeout".into(),
                    "30".into(),
                ],
                env: vec![],
                working_dir: None,
                uid: None,
                gid: None,
                hostname: None,
                capture_output: true,
                stdin: false,
            },
        }],
        resources: None,
    })
    .await?;

    // Wait for backend pod running
    recv_until(
        &mut conn,
        EVENT_TIMEOUT,
        |e| matches!(e, WorkerEvent::PodRunning { pod_id, .. } if pod_id == "pod-backend"),
    )
    .await?;

    // Accept the log stream for the backend pod
    let (_header, mut log_stream) = tokio::time::timeout(EVENT_TIMEOUT, conn.accept_log_stream())
        .await
        .map_err(|_| anyhow::anyhow!("timed out waiting for log stream"))??;

    // Set the backend and mark service ready via EndpointUpdate
    conn.send_command(&WorkerCommand::EndpointUpdate {
        namespace_id: "ns-svc-fwd".into(),
        upserted: vec![EndpointSpec {
            ip: Ipv4Addr::new(10, 0, 0, 99),
            kind: EndpointKind::Service {
                service_id: "svc-fwd".into(),
                policy: ServicePolicy {
                    buffer_frames: 64,
                    timeout_ms: 30000,
                    activator: None,
                },
                backend: Some(EndpointPodBackend {
                    pod_ip: Ipv4Addr::new(10, 0, 0, 2),
                    placement: None,
                    ready: true,
                }),
            },
        }],
        removed_ips: vec![],
    })
    .await?;

    // Launch client pod that sends data to the service VIP
    register_pod_endpoint(
        &mut conn,
        "ns-svc-fwd",
        &test_pod_network_config_2(),
        "test-worker",
    )
    .await?;

    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-svc-fwd".into(),
        pod_id: "pod-client".into(),
        network: test_pod_network_config_2(),
        containers: vec![ContainerSpec {
            container_id: "ctr-client".into(),
            image_ref: "docker.io/library/distvirt-test-containers:latest".into(),
            config: ContainerConfig {
                entrypoint: vec!["/bin/test-containers".into()],
                args: vec![
                    "send".into(),
                    "--host".into(),
                    "10.0.0.99".into(),
                    "--port".into(),
                    "80".into(),
                    "--data".into(),
                    "hello-service\n".into(),
                    "--timeout".into(),
                    "30".into(),
                ],
                env: vec![],
                working_dir: None,
                uid: None,
                gid: None,
                hostname: None,
                capture_output: true,
                stdin: false,
            },
        }],
        resources: None,
    })
    .await?;

    // Wait for backend pod to exit — test-containers recv exits 0 on data match
    let event = recv_until(
        &mut conn,
        EVENT_TIMEOUT,
        |e| matches!(e, WorkerEvent::PodExited { pod_id, .. } if pod_id == "pod-backend"),
    )
    .await?;

    // Drain the backend log stream (for debug output)
    let mut log_data = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let mut buf = [0u8; 4096];
            match log_stream.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => log_data.extend_from_slice(&buf[..n]),
                Err(_) => break,
            }
        }
    })
    .await;
    let log_str = String::from_utf8_lossy(&log_data);
    eprintln!("backend log output: {:?}", log_str);

    // Verify backend exited successfully (exit code 0 means data matched)
    assert!(
        matches!(&event, WorkerEvent::PodExited { exit_code: 0, .. }),
        "expected backend to exit with code 0 (data match), got: {:?}",
        event
    );

    shutdown_worker(&mut conn, worker_handle).await?;

    Ok(())
}

#[tokio::test]
async fn test_service_backend_buffer_and_flush() -> anyhow::Result<()> {
    if !should_run() {
        return Ok(());
    }

    let (mut conn, worker_handle) = setup_with_activators().await?;

    // Create namespace
    conn.send_command(&WorkerCommand::CreateNamespace {
        namespace_id: "ns-svc-buf".into(),
        network: test_network_config(),
    })
    .await?;

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::NamespaceCreated { .. })
    })
    .await?;

    // Create service with TCP activator via EndpointSync
    conn.send_command(&WorkerCommand::EndpointSync {
        namespace_id: "ns-svc-buf".into(),
        endpoints: vec![EndpointSpec {
            ip: Ipv4Addr::new(10, 0, 0, 99),
            kind: EndpointKind::Service {
                service_id: "svc-buf".into(),
                policy: ServicePolicy {
                    buffer_frames: 64,
                    timeout_ms: 30000,
                    activator: Some(ActivatorConfig::Tcp {
                        ports: None,
                        tcp_only: true,
                        max_flows: 1024,
                    }),
                },
                backend: None,
            },
        }],
    })
    .await?;

    // Launch backend pod that listens on port 80 at the service VIP.
    register_pod_endpoint(
        &mut conn,
        "ns-svc-buf",
        &test_pod_network_config(),
        "test-worker",
    )
    .await?;

    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-svc-buf".into(),
        pod_id: "pod-backend".into(),
        network: test_pod_network_config(),
        containers: vec![ContainerSpec {
            container_id: "ctr-backend".into(),
            image_ref: "docker.io/library/distvirt-test-containers:latest".into(),
            config: ContainerConfig {
                entrypoint: vec!["/bin/test-containers".into()],
                args: vec![
                    "recv".into(),
                    "--port".into(),
                    "80".into(),
                    "--expected".into(),
                    "hello-buffered\n".into(),
                    "--response".into(),
                    "ok".into(),
                    "--timeout".into(),
                    "30".into(),
                ],
                env: vec![],
                working_dir: None,
                uid: None,
                gid: None,
                hostname: None,
                capture_output: true,
                stdin: false,
            },
        }],
        resources: None,
    })
    .await?;

    // Wait for backend pod running
    recv_until(
        &mut conn,
        EVENT_TIMEOUT,
        |e| matches!(e, WorkerEvent::PodRunning { pod_id, .. } if pod_id == "pod-backend"),
    )
    .await?;

    // Accept backend log stream
    let (_header, mut log_stream) = tokio::time::timeout(EVENT_TIMEOUT, conn.accept_log_stream())
        .await
        .map_err(|_| anyhow::anyhow!("timed out waiting for log stream"))??;

    // Launch client pod BEFORE setting backend — traffic will be buffered
    register_pod_endpoint(
        &mut conn,
        "ns-svc-buf",
        &test_pod_network_config_2(),
        "test-worker",
    )
    .await?;

    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-svc-buf".into(),
        pod_id: "pod-client".into(),
        network: test_pod_network_config_2(),
        containers: vec![ContainerSpec {
            container_id: "ctr-client".into(),
            image_ref: "docker.io/library/distvirt-test-containers:latest".into(),
            config: ContainerConfig {
                entrypoint: vec!["/bin/test-containers".into()],
                args: vec![
                    "send".into(),
                    "--host".into(),
                    "10.0.0.99".into(),
                    "--port".into(),
                    "80".into(),
                    "--data".into(),
                    "hello-buffered\n".into(),
                    "--timeout".into(),
                    "30".into(),
                ],
                env: vec![],
                working_dir: None,
                uid: None,
                gid: None,
                hostname: None,
                capture_output: true,
                stdin: false,
            },
        }],
        resources: None,
    })
    .await?;

    // Wait for the TCP activator to signal backend need (SYN was buffered)
    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(
            e,
            WorkerEvent::ServiceBackendNeed {
                need: BackendNeed::Traffic,
                ..
            }
        )
    })
    .await?;

    // Now set the backend and mark ready — buffered SYN should be flushed
    conn.send_command(&WorkerCommand::EndpointUpdate {
        namespace_id: "ns-svc-buf".into(),
        upserted: vec![EndpointSpec {
            ip: Ipv4Addr::new(10, 0, 0, 99),
            kind: EndpointKind::Service {
                service_id: "svc-buf".into(),
                policy: ServicePolicy {
                    buffer_frames: 64,
                    timeout_ms: 30000,
                    activator: Some(ActivatorConfig::Tcp {
                        ports: None,
                        tcp_only: true,
                        max_flows: 1024,
                    }),
                },
                backend: Some(EndpointPodBackend {
                    pod_ip: Ipv4Addr::new(10, 0, 0, 2),
                    placement: None,
                    ready: true,
                }),
            },
        }],
        removed_ips: vec![],
    })
    .await?;

    // Wait for backend pod to exit — test-containers recv exits 0 on data match
    let event = recv_until(
        &mut conn,
        EVENT_TIMEOUT,
        |e| matches!(e, WorkerEvent::PodExited { pod_id, .. } if pod_id == "pod-backend"),
    )
    .await?;

    // Drain the backend log stream (for debug output)
    let mut log_data = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let mut buf = [0u8; 4096];
            match log_stream.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => log_data.extend_from_slice(&buf[..n]),
                Err(_) => break,
            }
        }
    })
    .await;
    let log_str = String::from_utf8_lossy(&log_data);
    eprintln!("backend log output: {:?}", log_str);

    // Verify backend exited successfully (exit code 0 means data matched)
    assert!(
        matches!(&event, WorkerEvent::PodExited { exit_code: 0, .. }),
        "expected backend to exit with code 0 (data match), got: {:?}",
        event
    );

    shutdown_worker(&mut conn, worker_handle).await?;

    Ok(())
}

#[tokio::test]
async fn test_destroy_service() -> anyhow::Result<()> {
    if !should_run() {
        return Ok(());
    }

    let (mut conn, worker_handle) = setup().await?;

    // Create namespace
    conn.send_command(&WorkerCommand::CreateNamespace {
        namespace_id: "ns-svc-destroy".into(),
        network: test_network_config(),
    })
    .await?;

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::NamespaceCreated { .. })
    })
    .await?;

    // Create service via EndpointSync
    conn.send_command(&WorkerCommand::EndpointSync {
        namespace_id: "ns-svc-destroy".into(),
        endpoints: vec![EndpointSpec {
            ip: Ipv4Addr::new(10, 0, 0, 99),
            kind: EndpointKind::Service {
                service_id: "svc-destroy".into(),
                policy: ServicePolicy {
                    buffer_frames: 64,
                    timeout_ms: 30000,
                    activator: None,
                },
                backend: None,
            },
        }],
    })
    .await?;

    // Destroy the service via EndpointUpdate with removed_ips
    conn.send_command(&WorkerCommand::EndpointUpdate {
        namespace_id: "ns-svc-destroy".into(),
        upserted: vec![],
        removed_ips: vec![Ipv4Addr::new(10, 0, 0, 99)],
    })
    .await?;

    // Verify clean shutdown (no panics from service teardown)
    shutdown_worker(&mut conn, worker_handle).await?;

    Ok(())
}
