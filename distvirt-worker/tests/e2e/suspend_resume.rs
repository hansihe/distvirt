use std::net::Ipv4Addr;

use distvirt_worker_protocol::{
    ActivatorConfig, BackendNeed, ContainerConfig, ContainerSpec, ServiceBackend, ServicePolicy,
    WorkerCommand, WorkerEvent,
};

use super::common::*;

#[tokio::test]
async fn test_suspend_resume_pod() -> anyhow::Result<()> {
    if !should_run() {
        return Ok(());
    }

    let (mut conn, worker_handle) = setup().await?;

    // Create namespace
    conn.send_command(&WorkerCommand::CreateNamespace {
        namespace_id: "ns-suspend".into(),
        network: test_network_config(),
    })
    .await?;

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::NamespaceCreated { .. })
    })
    .await?;

    // Launch a long-running pod
    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-suspend".into(),
        pod_id: "pod-suspend".into(),
        network: test_pod_network_config(),
        containers: vec![ContainerSpec {
            container_id: "ctr-suspend".into(),
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

    // Wait for PodRunning
    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodRunning { pod_id, .. } if pod_id == "pod-suspend")
    })
    .await?;
    eprintln!("e2e: pod-suspend is running, sending SuspendPod");

    // Suspend the pod
    conn.send_command(&WorkerCommand::SuspendPod {
        namespace_id: "ns-suspend".into(),
        pod_id: "pod-suspend".into(),
        artifact_id: "snap-1".into(),
        pool_id: "local-default".into(),
    })
    .await?;

    // Wait for PodSuspended (or PodSuspendFailed)
    let event = recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(
            e,
            WorkerEvent::PodSuspended { .. } | WorkerEvent::PodSuspendFailed { .. }
        )
    })
    .await?;

    if let WorkerEvent::PodSuspendFailed { error, .. } = &event {
        anyhow::bail!("suspend failed: {}", error);
    }

    match &event {
        WorkerEvent::PodSuspended {
            namespace_id,
            pod_id,
            artifact_id,
            artifact_size_bytes,
            ..
        } => {
            assert_eq!(namespace_id, "ns-suspend");
            assert_eq!(pod_id, "pod-suspend");
            assert_eq!(artifact_id, "snap-1");
            assert!(
                *artifact_size_bytes > 0,
                "snapshot should have non-zero size, got {}",
                artifact_size_bytes
            );
            eprintln!(
                "e2e: pod suspended, artifact_size={}",
                artifact_size_bytes
            );
        }
        other => panic!("expected PodSuspended, got {:?}", other),
    }

    // Resume the pod from the snapshot
    conn.send_command(&WorkerCommand::ResumePod {
        namespace_id: "ns-suspend".into(),
        pod_id: "pod-resumed".into(),
        artifact_id: "snap-1".into(),
        network: test_pod_network_config(),
        pool_id: "local-default".into(),
    })
    .await?;

    // Wait for PodRunning again
    let event = recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodRunning { pod_id, .. } if pod_id == "pod-resumed")
    })
    .await?;
    eprintln!("e2e: pod-resumed is running: {:?}", event);

    // Stop the resumed pod to clean up
    conn.send_command(&WorkerCommand::StopPod {
        namespace_id: "ns-suspend".into(),
        pod_id: "pod-resumed".into(),
        graceful: true,
    })
    .await?;

    // Wait for PodExited
    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodExited { pod_id, .. } if pod_id == "pod-resumed")
    })
    .await?;

    // Clean up snapshot
    conn.send_command(&WorkerCommand::DeleteArtifact {
        artifact_id: "snap-1".into(),
        pool_id: "local-default".into(),
    })
    .await?;

    shutdown_worker(&mut conn, worker_handle).await?;

    Ok(())
}

/// Test that a pod can receive TCP traffic over the network after suspend/resume.
///
/// Flow:
/// 1. Launch a server pod running a persistent TCP listener
/// 2. Set up a service pointing at the server pod
/// 3. Launch a client pod, verify it receives a response (pre-suspend check)
/// 4. Suspend the server pod
/// 5. Resume the server pod from the snapshot
/// 6. Re-point the service at the resumed pod
/// 7. Launch another client pod, verify it receives a response (post-resume check)
#[tokio::test]
async fn test_suspend_resume_network() -> anyhow::Result<()> {
    if !should_run() {
        return Ok(());
    }

    let (mut conn, worker_handle) = setup().await?;

    // Create namespace
    conn.send_command(&WorkerCommand::CreateNamespace {
        namespace_id: "ns-susp-net".into(),
        network: test_network_config(),
    })
    .await?;

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::NamespaceCreated { .. })
    })
    .await?;

    // Create service at 10.0.0.99 (no activator, simple forwarding)
    conn.send_command(&WorkerCommand::CreateService {
        namespace_id: "ns-susp-net".into(),
        service_id: "svc-susp".into(),
        ip: Ipv4Addr::new(10, 0, 0, 99),
        policy: ServicePolicy {
            buffer_frames: 64,
            timeout_ms: 5000,
            activator: None,
        },
    })
    .await?;

    // Launch server pod with a persistent TCP listener (responds "pong" per connection)
    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-susp-net".into(),
        pod_id: "pod-server".into(),
        network: test_pod_network_config(),
        containers: vec![ContainerSpec {
            container_id: "ctr-server".into(),
            image_ref: "docker.io/library/alpine:latest".into(),
            config: ContainerConfig {
                entrypoint: vec!["/bin/sh".into()],
                args: vec![
                    "-c".into(),
                    "while true; do echo pong | nc -l -p 80; done".into(),
                ],
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

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodRunning { pod_id, .. } if pod_id == "pod-server")
    })
    .await?;

    // Point service at server pod and mark ready
    conn.send_command(&WorkerCommand::UpdateServiceBackend {
        namespace_id: "ns-susp-net".into(),
        service_id: "svc-susp".into(),
        backend: Some(ServiceBackend {
            pod_ip: Ipv4Addr::new(10, 0, 0, 2),
        }),
    })
    .await?;

    conn.send_command(&WorkerCommand::ServiceReady {
        namespace_id: "ns-susp-net".into(),
        service_id: "svc-susp".into(),
    })
    .await?;

    // --- Pre-suspend: verify TCP connectivity ---
    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-susp-net".into(),
        pod_id: "pod-client-pre".into(),
        network: test_pod_network_config_2(),
        containers: vec![ContainerSpec {
            container_id: "ctr-client-pre".into(),
            image_ref: "docker.io/library/alpine:latest".into(),
            config: ContainerConfig {
                entrypoint: vec!["/bin/sh".into()],
                args: vec!["-c".into(), "nc -w 1 10.0.0.99 80".into()],
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
        matches!(e, WorkerEvent::PodRunning { pod_id, .. } if pod_id == "pod-client-pre")
    })
    .await?;

    let log_str = drain_log_stream(&mut conn).await?;
    assert!(
        log_str.contains("pong"),
        "expected 'pong' in pre-suspend client output, got: {:?}",
        log_str
    );

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodExited { pod_id, .. } if pod_id == "pod-client-pre")
    })
    .await?;
    eprintln!("e2e: pre-suspend connectivity verified");

    // --- Suspend the server pod ---
    conn.send_command(&WorkerCommand::SuspendPod {
        namespace_id: "ns-susp-net".into(),
        pod_id: "pod-server".into(),
        artifact_id: "snap-net".into(),
        pool_id: "local-default".into(),
    })
    .await?;

    let event = recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(
            e,
            WorkerEvent::PodSuspended { .. } | WorkerEvent::PodSuspendFailed { .. }
        )
    })
    .await?;

    if let WorkerEvent::PodSuspendFailed { error, .. } = &event {
        anyhow::bail!("suspend failed: {}", error);
    }
    eprintln!("e2e: server pod suspended");

    // --- Resume the server pod ---
    conn.send_command(&WorkerCommand::ResumePod {
        namespace_id: "ns-susp-net".into(),
        pod_id: "pod-server-resumed".into(),
        artifact_id: "snap-net".into(),
        network: test_pod_network_config(),
        pool_id: "local-default".into(),
    })
    .await?;

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodRunning { pod_id, .. } if pod_id == "pod-server-resumed")
    })
    .await?;
    eprintln!("e2e: server pod resumed");

    // Re-point service at resumed pod (same IP/MAC) and mark ready
    conn.send_command(&WorkerCommand::UpdateServiceBackend {
        namespace_id: "ns-susp-net".into(),
        service_id: "svc-susp".into(),
        backend: Some(ServiceBackend {
            pod_ip: Ipv4Addr::new(10, 0, 0, 2),
        }),
    })
    .await?;

    conn.send_command(&WorkerCommand::ServiceReady {
        namespace_id: "ns-susp-net".into(),
        service_id: "svc-susp".into(),
    })
    .await?;

    // --- Post-resume: verify TCP connectivity ---
    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-susp-net".into(),
        pod_id: "pod-client-post".into(),
        network: test_pod_network_config_2(),
        containers: vec![ContainerSpec {
            container_id: "ctr-client-post".into(),
            image_ref: "docker.io/library/alpine:latest".into(),
            config: ContainerConfig {
                entrypoint: vec!["/bin/sh".into()],
                args: vec!["-c".into(), "nc -w 1 10.0.0.99 80".into()],
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
        matches!(e, WorkerEvent::PodRunning { pod_id, .. } if pod_id == "pod-client-post")
    })
    .await?;

    let log_str = drain_log_stream(&mut conn).await?;
    assert!(
        log_str.contains("pong"),
        "expected 'pong' in post-resume client output, got: {:?}",
        log_str
    );
    eprintln!("e2e: post-resume connectivity verified");

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodExited { pod_id, .. } if pod_id == "pod-client-post")
    })
    .await?;

    // Clean up
    conn.send_command(&WorkerCommand::StopPod {
        namespace_id: "ns-susp-net".into(),
        pod_id: "pod-server-resumed".into(),
        graceful: true,
    })
    .await?;

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodExited { pod_id, .. } if pod_id == "pod-server-resumed")
    })
    .await?;

    conn.send_command(&WorkerCommand::DeleteArtifact {
        artifact_id: "snap-net".into(),
        pool_id: "local-default".into(),
    })
    .await?;

    shutdown_worker(&mut conn, worker_handle).await?;

    Ok(())
}

/// Test the full activation-on-demand flow with suspend/resume:
/// a client sends traffic while the server pod is suspended, the TCP activator
/// buffers the SYN and signals BackendNeed, the orchestrator resumes the pod,
/// and the buffered traffic is flushed so the client gets its response.
///
/// Flow:
/// 1. Launch server pod, set up service with TCP activator, verify connectivity
/// 2. Suspend the server pod
/// 3. Clear the service backend (no backend available)
/// 4. Launch a client pod that sends traffic to the service VIP
/// 5. TCP activator buffers the SYN and emits ServiceBackendNeed::Traffic
/// 6. Resume the server pod from the snapshot
/// 7. Re-point service backend at resumed pod, mark ready (flushes buffered SYN)
/// 8. Verify the client receives the server's response
#[tokio::test]
async fn test_suspend_resume_activation() -> anyhow::Result<()> {
    if !should_run() {
        return Ok(());
    }

    let (mut conn, worker_handle) = setup_with_activators().await?;

    // Create namespace
    conn.send_command(&WorkerCommand::CreateNamespace {
        namespace_id: "ns-act-resume".into(),
        network: test_network_config(),
    })
    .await?;

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::NamespaceCreated { .. })
    })
    .await?;

    // Create service with TCP activator at 10.0.0.99
    conn.send_command(&WorkerCommand::CreateService {
        namespace_id: "ns-act-resume".into(),
        service_id: "svc-act".into(),
        ip: Ipv4Addr::new(10, 0, 0, 99),
        policy: ServicePolicy {
            buffer_frames: 64,
            timeout_ms: 10000,
            activator: Some(ActivatorConfig::Tcp {
                ports: None,
                tcp_only: true,
                max_flows: 1024,
            }),
        },
    })
    .await?;

    // Launch server pod with a persistent TCP listener
    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-act-resume".into(),
        pod_id: "pod-server".into(),
        network: test_pod_network_config(),
        containers: vec![ContainerSpec {
            container_id: "ctr-server".into(),
            image_ref: "docker.io/library/alpine:latest".into(),
            config: ContainerConfig {
                entrypoint: vec!["/bin/sh".into()],
                args: vec![
                    "-c".into(),
                    "while true; do echo pong | nc -l -p 80; done".into(),
                ],
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

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodRunning { pod_id, .. } if pod_id == "pod-server")
    })
    .await?;

    // Point service at server pod and mark ready
    conn.send_command(&WorkerCommand::UpdateServiceBackend {
        namespace_id: "ns-act-resume".into(),
        service_id: "svc-act".into(),
        backend: Some(ServiceBackend {
            pod_ip: Ipv4Addr::new(10, 0, 0, 2),
        }),
    })
    .await?;

    conn.send_command(&WorkerCommand::ServiceReady {
        namespace_id: "ns-act-resume".into(),
        service_id: "svc-act".into(),
    })
    .await?;

    // Pre-suspend sanity check: verify connectivity
    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-act-resume".into(),
        pod_id: "pod-pre-check".into(),
        network: test_pod_network_config_2(),
        containers: vec![ContainerSpec {
            container_id: "ctr-pre-check".into(),
            image_ref: "docker.io/library/alpine:latest".into(),
            config: ContainerConfig {
                entrypoint: vec!["/bin/sh".into()],
                args: vec!["-c".into(), "nc -w 1 10.0.0.99 80".into()],
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
        matches!(e, WorkerEvent::PodRunning { pod_id, .. } if pod_id == "pod-pre-check")
    })
    .await?;

    let log_str = drain_log_stream(&mut conn).await?;
    assert!(
        log_str.contains("pong"),
        "expected 'pong' in pre-suspend check, got: {:?}",
        log_str
    );

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodExited { pod_id, .. } if pod_id == "pod-pre-check")
    })
    .await?;
    eprintln!("e2e: pre-suspend connectivity verified");

    // --- Suspend the server pod ---
    conn.send_command(&WorkerCommand::SuspendPod {
        namespace_id: "ns-act-resume".into(),
        pod_id: "pod-server".into(),
        artifact_id: "snap-act".into(),
        pool_id: "local-default".into(),
    })
    .await?;

    let event = recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(
            e,
            WorkerEvent::PodSuspended { .. } | WorkerEvent::PodSuspendFailed { .. }
        )
    })
    .await?;

    if let WorkerEvent::PodSuspendFailed { error, .. } = &event {
        anyhow::bail!("suspend failed: {}", error);
    }
    eprintln!("e2e: server pod suspended");

    // Clear the service backend — pod is suspended, no backend available.
    // The activator will now buffer incoming traffic and signal BackendNeed.
    conn.send_command(&WorkerCommand::UpdateServiceBackend {
        namespace_id: "ns-act-resume".into(),
        service_id: "svc-act".into(),
        backend: None,
    })
    .await?;

    // Launch client that connects to the service VIP while no backend exists.
    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-act-resume".into(),
        pod_id: "pod-client".into(),
        network: test_pod_network_config_2(),
        containers: vec![ContainerSpec {
            container_id: "ctr-client".into(),
            image_ref: "docker.io/library/alpine:latest".into(),
            config: ContainerConfig {
                entrypoint: vec!["/bin/sh".into()],
                args: vec!["-c".into(), "nc -w 3 10.0.0.99 80".into()],
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

    // Wait for the TCP activator to detect the SYN and signal activation
    let event = recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::ServiceBackendNeed { need: BackendNeed::Traffic, .. })
    })
    .await?;

    assert!(
        matches!(&event, WorkerEvent::ServiceBackendNeed { namespace_id, service_id, need: BackendNeed::Traffic }
            if namespace_id == "ns-act-resume" && service_id == "svc-act"),
        "unexpected event: {:?}",
        event
    );
    eprintln!("e2e: activator signaled BackendNeed::Traffic, resuming server pod");

    // Resume the server pod from the snapshot
    conn.send_command(&WorkerCommand::ResumePod {
        namespace_id: "ns-act-resume".into(),
        pod_id: "pod-server-resumed".into(),
        artifact_id: "snap-act".into(),
        network: test_pod_network_config(),
        pool_id: "local-default".into(),
    })
    .await?;

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodRunning { pod_id, .. } if pod_id == "pod-server-resumed")
    })
    .await?;
    eprintln!("e2e: server pod resumed");

    // Point service at resumed pod and mark ready — flushes the buffered SYN
    conn.send_command(&WorkerCommand::UpdateServiceBackend {
        namespace_id: "ns-act-resume".into(),
        service_id: "svc-act".into(),
        backend: Some(ServiceBackend {
            pod_ip: Ipv4Addr::new(10, 0, 0, 2),
        }),
    })
    .await?;

    conn.send_command(&WorkerCommand::ServiceReady {
        namespace_id: "ns-act-resume".into(),
        service_id: "svc-act".into(),
    })
    .await?;

    // The buffered SYN is flushed, the TCP handshake completes, and the server
    // responds with "pong". Verify the client received it.
    let log_str = drain_log_stream(&mut conn).await?;
    assert!(
        log_str.contains("pong"),
        "expected 'pong' in post-activation client output, got: {:?}",
        log_str
    );
    eprintln!("e2e: client received response after activation + resume");

    // Wait for client to exit
    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodExited { pod_id, .. } if pod_id == "pod-client")
    })
    .await?;

    // Clean up
    conn.send_command(&WorkerCommand::StopPod {
        namespace_id: "ns-act-resume".into(),
        pod_id: "pod-server-resumed".into(),
        graceful: true,
    })
    .await?;

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodExited { pod_id, .. } if pod_id == "pod-server-resumed")
    })
    .await?;

    conn.send_command(&WorkerCommand::DeleteArtifact {
        artifact_id: "snap-act".into(),
        pool_id: "local-default".into(),
    })
    .await?;

    shutdown_worker(&mut conn, worker_handle).await?;

    Ok(())
}
