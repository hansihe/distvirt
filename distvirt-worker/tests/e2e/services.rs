use std::net::Ipv4Addr;
use std::time::Duration;

use futures_lite::io::AsyncReadExt;

use distvirt_worker_protocol::{
    ActivatorConfig, BackendNeed, ContainerConfig, ContainerSpec, RegistryEntry, ServiceBackend,
    ServicePolicy, WorkerCommand, WorkerEvent,
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
    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-dns".into(),
        pod_id: "pod-dns".into(),
        network: test_pod_network_config(),
        containers: vec![ContainerSpec {
            container_id: "ctr-dns".into(),
            image_ref: "docker.io/library/alpine:latest".into(),
            config: ContainerConfig {
                entrypoint: vec!["/bin/sh".into()],
                args: vec!["-c".into(), "nslookup myservice 2>&1 || true".into()],
                env: vec![],
                working_dir: None,
                uid: None,
                gid: None,
                hostname: None,
                capture_output: true,
                stdin: false,
            },
        }],
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

    // Create a service with TCP activator
    conn.send_command(&WorkerCommand::CreateService {
        namespace_id: "ns-tcp-act".into(),
        service_id: "svc-tcp".into(),
        ip: Ipv4Addr::new(10, 0, 0, 99),
        policy: ServicePolicy {
            buffer_frames: 64,
            timeout_ms: 30000,
            activator: Some(ActivatorConfig::Tcp {
                ports: None,
                tcp_only: true,
                max_flows: 1024,
            }),
        },
    })
    .await?;

    // Launch a pod that sends a TCP SYN to the service IP
    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-tcp-act".into(),
        pod_id: "pod-tcp-act".into(),
        network: test_pod_network_config(),
        containers: vec![ContainerSpec {
            container_id: "ctr-tcp-act".into(),
            image_ref: "docker.io/library/alpine:latest".into(),
            config: ContainerConfig {
                entrypoint: vec!["/bin/sh".into()],
                args: vec!["-c".into(), "nc -w 1 10.0.0.99 80 || true".into()],
                env: vec![],
                working_dir: None,
                uid: None,
                gid: None,
                hostname: None,
                capture_output: true,
                stdin: false,
            },
        }],
    })
    .await?;

    // Wait for PodRunning
    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodRunning { .. })
    })
    .await?;

    // Wait for the TCP activator to signal backend need
    let event = recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::ServiceBackendNeed { need: BackendNeed::Traffic, .. })
    })
    .await?;

    assert!(
        matches!(&event, WorkerEvent::ServiceBackendNeed { namespace_id, service_id, need: BackendNeed::Traffic }
            if namespace_id == "ns-tcp-act" && service_id == "svc-tcp"),
        "unexpected event: {:?}",
        event
    );

    // Wait for pod exit (nc -w 1 will time out after 1 second)
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

    // Create service at 10.0.0.99 (no activator)
    conn.send_command(&WorkerCommand::CreateService {
        namespace_id: "ns-svc-fwd".into(),
        service_id: "svc-fwd".into(),
        ip: Ipv4Addr::new(10, 0, 0, 99),
        policy: ServicePolicy {
            buffer_frames: 64,
            timeout_ms: 30000,
            activator: None,
        },
    })
    .await?;

    // Launch backend pod that listens on port 80 at the service VIP.
    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-svc-fwd".into(),
        pod_id: "pod-backend".into(),
        network: test_pod_network_config(),
        containers: vec![ContainerSpec {
            container_id: "ctr-backend".into(),
            image_ref: "docker.io/library/alpine:latest".into(),
            config: ContainerConfig {
                entrypoint: vec!["/bin/sh".into()],
                args: vec![
                    "-c".into(),
                    "nc -l -w 5 -p 80".into(),
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
    })
    .await?;

    // Wait for backend pod running
    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodRunning { pod_id, .. } if pod_id == "pod-backend")
    })
    .await?;

    // Accept the log stream for the backend pod
    let (_header, mut log_stream) = tokio::time::timeout(
        EVENT_TIMEOUT,
        conn.accept_log_stream(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("timed out waiting for log stream"))??;

    // Set the backend and mark service ready
    conn.send_command(&WorkerCommand::UpdateServiceBackend {
        namespace_id: "ns-svc-fwd".into(),
        service_id: "svc-fwd".into(),
        backend: Some(ServiceBackend {
            pod_ip: Ipv4Addr::new(10, 0, 0, 2),
        }),
    })
    .await?;

    conn.send_command(&WorkerCommand::ServiceReady {
        namespace_id: "ns-svc-fwd".into(),
        service_id: "svc-fwd".into(),
    })
    .await?;

    // Launch client pod that sends data to the service VIP
    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-svc-fwd".into(),
        pod_id: "pod-client".into(),
        network: test_pod_network_config_2(),
        containers: vec![ContainerSpec {
            container_id: "ctr-client".into(),
            image_ref: "docker.io/library/alpine:latest".into(),
            config: ContainerConfig {
                entrypoint: vec!["/bin/sh".into()],
                args: vec![
                    "-c".into(),
                    "echo hello-service | nc -w 5 10.0.0.99 80".into(),
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
    })
    .await?;

    // Wait for backend pod to exit (nc -l exits after first connection)
    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodExited { pod_id, .. } if pod_id == "pod-backend")
    })
    .await?;

    // Read backend logs — should contain the data sent by client
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
    assert!(
        log_str.contains("hello-service"),
        "expected 'hello-service' in backend log output, got: {:?}",
        log_str
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

    // Create service with TCP activator
    conn.send_command(&WorkerCommand::CreateService {
        namespace_id: "ns-svc-buf".into(),
        service_id: "svc-buf".into(),
        ip: Ipv4Addr::new(10, 0, 0, 99),
        policy: ServicePolicy {
            buffer_frames: 64,
            timeout_ms: 30000,
            activator: Some(ActivatorConfig::Tcp {
                ports: None,
                tcp_only: true,
                max_flows: 1024,
            }),
        },
    })
    .await?;

    // Launch backend pod that listens on port 80 at the service VIP.
    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-svc-buf".into(),
        pod_id: "pod-backend".into(),
        network: test_pod_network_config(),
        containers: vec![ContainerSpec {
            container_id: "ctr-backend".into(),
            image_ref: "docker.io/library/alpine:latest".into(),
            config: ContainerConfig {
                entrypoint: vec!["/bin/sh".into()],
                args: vec![
                    "-c".into(),
                    "nc -l -w 5 -p 80".into(),
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
    })
    .await?;

    // Wait for backend pod running
    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodRunning { pod_id, .. } if pod_id == "pod-backend")
    })
    .await?;

    // Accept backend log stream
    let (_header, mut log_stream) = tokio::time::timeout(
        EVENT_TIMEOUT,
        conn.accept_log_stream(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("timed out waiting for log stream"))??;

    // Launch client pod BEFORE setting backend — traffic will be buffered
    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-svc-buf".into(),
        pod_id: "pod-client".into(),
        network: test_pod_network_config_2(),
        containers: vec![ContainerSpec {
            container_id: "ctr-client".into(),
            image_ref: "docker.io/library/alpine:latest".into(),
            config: ContainerConfig {
                entrypoint: vec!["/bin/sh".into()],
                args: vec![
                    "-c".into(),
                    "echo hello-buffered | nc -w 5 10.0.0.99 80".into(),
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
    })
    .await?;

    // Wait for the TCP activator to signal backend need (SYN was buffered)
    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::ServiceBackendNeed { need: BackendNeed::Traffic, .. })
    })
    .await?;

    // Now set the backend and mark ready — buffered SYN should be flushed
    conn.send_command(&WorkerCommand::UpdateServiceBackend {
        namespace_id: "ns-svc-buf".into(),
        service_id: "svc-buf".into(),
        backend: Some(ServiceBackend {
            pod_ip: Ipv4Addr::new(10, 0, 0, 2),
        }),
    })
    .await?;

    conn.send_command(&WorkerCommand::ServiceReady {
        namespace_id: "ns-svc-buf".into(),
        service_id: "svc-buf".into(),
    })
    .await?;

    // Wait for backend pod to exit (nc -l exits after first connection)
    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodExited { pod_id, .. } if pod_id == "pod-backend")
    })
    .await?;

    // Read backend logs — should contain data sent by client
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
    assert!(
        log_str.contains("hello-buffered"),
        "expected 'hello-buffered' in backend log output, got: {:?}",
        log_str
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

    // Create service
    conn.send_command(&WorkerCommand::CreateService {
        namespace_id: "ns-svc-destroy".into(),
        service_id: "svc-destroy".into(),
        ip: Ipv4Addr::new(10, 0, 0, 99),
        policy: ServicePolicy {
            buffer_frames: 64,
            timeout_ms: 30000,
            activator: None,
        },
    })
    .await?;

    // Destroy the service
    conn.send_command(&WorkerCommand::DestroyService {
        namespace_id: "ns-svc-destroy".into(),
        service_id: "svc-destroy".into(),
    })
    .await?;

    // Verify clean shutdown (no panics from service teardown)
    shutdown_worker(&mut conn, worker_handle).await?;

    Ok(())
}
