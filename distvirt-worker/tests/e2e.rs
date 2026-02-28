//! End-to-end integration tests for distvirt-worker.
//!
//! These tests launch real Firecracker VMs and require:
//! - Root privileges
//! - `firecracker` binary (or `FIRECRACKER_BIN` env var)
//! - Running containerd (or `CONTAINERD_SOCKET` env var)
//! - Built kernel at `../guest-image/result-kernel/bzImage`
//! - Built rootfs at `../guest-image/result-rootfs`
//!
//! Gate with: `DISTVIRT_E2E=1 cargo test --package distvirt-worker --test e2e`

use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::time::Duration;

use futures_lite::io::AsyncReadExt;

use distvirt_worker_protocol::{
    ContainerConfig, ContainerSpec, NetworkConfig, OrchestratorConnection, PodNetworkConfig,
    RegistryEntry, WorkerCommand, WorkerConnection, WorkerEvent,
};

/// Returns `true` if the E2E env var is set, otherwise prints a skip message.
fn should_run() -> bool {
    if std::env::var("DISTVIRT_E2E").is_ok() {
        true
    } else {
        eprintln!("DISTVIRT_E2E not set, skipping integration test");
        false
    }
}

/// Spawn a worker on one half of a duplex and return the orchestrator connection
/// along with the worker task handle.
async fn setup() -> anyhow::Result<(OrchestratorConnection, tokio::task::JoinHandle<anyhow::Result<()>>)> {
    let _ = env_logger::try_init();

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let kernel = manifest_dir.join("../guest-image/result-kernel/vmlinux");
    let rootfs = manifest_dir.join("../guest-image/result-rootfs");

    assert!(kernel.exists(), "kernel not found at {}", kernel.display());
    assert!(rootfs.exists(), "rootfs not found at {}", rootfs.display());

    let firecracker_bin = std::env::var("FIRECRACKER_BIN").unwrap_or_else(|_| "firecracker".into());
    let vmm = distvirt_worker::vmm::firecracker::Firecracker::new(firecracker_bin);

    let containerd_socket = std::env::var("CONTAINERD_SOCKET")
        .unwrap_or_else(|_| "/run/containerd/containerd.sock".into());
    let image_provider = distvirt_worker::image_provider::containerd_overlayfs::ContainerdOverlayfsProvider {
        socket: containerd_socket,
        namespace: "default".into(),
    };

    let (orch_half, worker_half) = tokio::io::duplex(64 * 1024);

    let worker_handle = tokio::spawn(async move {
        let conn = WorkerConnection::accept(worker_half).await.unwrap();
        let worker = distvirt_worker::worker::Worker::new(kernel, rootfs, vmm, image_provider, None);
        worker.run(conn).await
    });

    let conn = OrchestratorConnection::connect(orch_half).await?;
    Ok((conn, worker_handle))
}

/// Timeout for the worker to shut down after receiving a Shutdown command.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

/// Send Shutdown and assert the worker task exits cleanly.
///
/// This validates that the entire shutdown path works — including VM process exit.
/// If firecracker doesn't terminate (e.g. guest didn't reboot properly), this will fail.
///
/// We don't try to receive the ShuttingDown event because the worker may close the
/// connection before we can read it (shutdown_all can complete very quickly when all
/// pods have already exited).
async fn shutdown_worker(
    conn: &mut OrchestratorConnection,
    worker_handle: tokio::task::JoinHandle<anyhow::Result<()>>,
) -> anyhow::Result<()> {
    conn.send_command(&WorkerCommand::Shutdown).await?;

    match tokio::time::timeout(SHUTDOWN_TIMEOUT, worker_handle).await {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(e))) => panic!("worker exited with error: {:#}", e),
        Ok(Err(e)) => panic!("worker task panicked: {}", e),
        Err(_) => panic!("worker did not shut down within {:?} — VM process likely stuck", SHUTDOWN_TIMEOUT),
    }
}

fn test_network_config() -> NetworkConfig {
    NetworkConfig {
        subnet: Ipv4Addr::new(10, 0, 0, 0),
        gateway: Ipv4Addr::new(10, 0, 0, 1),
        prefix_len: 24,
    }
}

fn test_pod_network_config() -> PodNetworkConfig {
    PodNetworkConfig {
        ip: Ipv4Addr::new(10, 0, 0, 2),
        mac: [0x02, 0x00, 0x00, 0x00, 0x00, 0x01],
        gateway: Ipv4Addr::new(10, 0, 0, 1),
        netmask: "255.255.255.0".into(),
    }
}

/// Helper: receive the next WorkerEvent with a timeout.
async fn recv_event_timeout(
    conn: &mut OrchestratorConnection,
    timeout: Duration,
) -> anyhow::Result<WorkerEvent> {
    tokio::time::timeout(timeout, conn.recv_event())
        .await
        .map_err(|_| anyhow::anyhow!("timed out waiting for event"))?
}

/// Helper: drain events until we find one matching the predicate, with a deadline.
async fn recv_until<F: Fn(&WorkerEvent) -> bool>(
    conn: &mut OrchestratorConnection,
    timeout: Duration,
    pred: F,
) -> anyhow::Result<WorkerEvent> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline - tokio::time::Instant::now();
        let event = recv_event_timeout(conn, remaining).await?;
        if pred(&event) {
            return Ok(event);
        }
        eprintln!("(skipping event: {:?})", event);
    }
}

const EVENT_TIMEOUT: Duration = Duration::from_secs(120);

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

    // Launch pod
    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-echo".into(),
        pod_id: "pod-echo".into(),
        network: test_pod_network_config(),
        containers: vec![ContainerSpec {
            container_id: "ctr-echo".into(),
            image_ref: "docker.io/library/alpine:latest".into(),
            config: ContainerConfig {
                entrypoint: "/bin/echo".into(),
                args: vec!["hello".into(), "world".into()],
                env: vec![],
                working_dir: None,
                uid: None,
                gid: None,
                hostname: None,
                capture_output: true,
            },
        }],
    })
    .await?;

    // Wait for PodRunning
    let event = recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodRunning { .. })
    })
    .await?;
    assert!(
        matches!(&event, WorkerEvent::PodRunning { namespace_id, pod_id }
            if namespace_id == "ns-echo" && pod_id == "pod-echo"),
        "unexpected event: {:?}",
        event
    );

    // Read log stream for "hello world"
    let (header, mut log_stream) = tokio::time::timeout(
        EVENT_TIMEOUT,
        conn.accept_log_stream(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("timed out waiting for log stream"))??;

    assert_eq!(header.pod_id, "pod-echo");
    assert_eq!(header.container_id, "ctr-echo");

    // Read all log data
    let mut log_data = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(30), async {
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
    eprintln!("captured log output: {:?}", log_str);
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

    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-exit".into(),
        pod_id: "pod-exit".into(),
        network: test_pod_network_config(),
        containers: vec![ContainerSpec {
            container_id: "ctr-exit".into(),
            image_ref: "docker.io/library/alpine:latest".into(),
            config: ContainerConfig {
                entrypoint: "/bin/sh".into(),
                args: vec!["-c".into(), "exit 42".into()],
                env: vec![],
                working_dir: None,
                uid: None,
                gid: None,
                hostname: None,
                capture_output: false,
            },
        }],
    })
    .await?;

    // Wait for PodRunning then PodExited
    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodRunning { .. })
    })
    .await?;

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

    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-stop".into(),
        pod_id: "pod-stop".into(),
        network: test_pod_network_config(),
        containers: vec![ContainerSpec {
            container_id: "ctr-stop".into(),
            image_ref: "docker.io/library/alpine:latest".into(),
            config: ContainerConfig {
                entrypoint: "/bin/sleep".into(),
                args: vec!["3600".into()],
                env: vec![],
                working_dir: None,
                uid: None,
                gid: None,
                hostname: None,
                capture_output: false,
            },
        }],
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
        pod_id: "pod-stop".into(),
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
            if namespace_id == "ns-stop" && pod_id == "pod-stop"),
        "unexpected event: {:?}",
        event
    );

    // Verify clean worker shutdown (validates VM process actually exited).
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
    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-destroy".into(),
        pod_id: "pod-destroy".into(),
        network: test_pod_network_config(),
        containers: vec![ContainerSpec {
            container_id: "ctr-destroy".into(),
            image_ref: "docker.io/library/alpine:latest".into(),
            config: ContainerConfig {
                entrypoint: "/bin/sleep".into(),
                args: vec!["3600".into()],
                env: vec![],
                working_dir: None,
                uid: None,
                gid: None,
                hostname: None,
                capture_output: false,
            },
        }],
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
            if namespace_id == "ns-destroy" && pod_id == "pod-destroy"),
        "unexpected event: {:?}",
        event
    );

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

    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-force".into(),
        pod_id: "pod-force".into(),
        network: test_pod_network_config(),
        containers: vec![ContainerSpec {
            container_id: "ctr-force".into(),
            image_ref: "docker.io/library/alpine:latest".into(),
            config: ContainerConfig {
                entrypoint: "/bin/sleep".into(),
                args: vec!["3600".into()],
                env: vec![],
                working_dir: None,
                uid: None,
                gid: None,
                hostname: None,
                capture_output: false,
            },
        }],
    })
    .await?;

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodRunning { .. })
    })
    .await?;

    // Force stop (non-graceful) — aborts the supervisor immediately
    conn.send_command(&WorkerCommand::StopPod {
        namespace_id: "ns-force".into(),
        pod_id: "pod-force".into(),
        graceful: false,
    })
    .await?;

    // Worker shutdown should still complete cleanly even after force stop
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
    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-fail".into(),
        pod_id: "pod-fail".into(),
        network: test_pod_network_config(),
        containers: vec![ContainerSpec {
            container_id: "ctr-fail".into(),
            image_ref: "docker.io/library/this-image-does-not-exist:99.99.99".into(),
            config: ContainerConfig {
                entrypoint: "/bin/true".into(),
                args: vec![],
                env: vec![],
                working_dir: None,
                uid: None,
                gid: None,
                hostname: None,
                capture_output: false,
            },
        }],
    })
    .await?;

    let event = recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodFailed { .. })
    })
    .await?;

    assert!(
        matches!(&event, WorkerEvent::PodFailed { namespace_id, pod_id, error }
            if namespace_id == "ns-fail" && pod_id == "pod-fail" && !error.is_empty()),
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
    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-env".into(),
        pod_id: "pod-env".into(),
        network: test_pod_network_config(),
        containers: vec![ContainerSpec {
            container_id: "ctr-env".into(),
            image_ref: "docker.io/library/alpine:latest".into(),
            config: ContainerConfig {
                entrypoint: "/bin/sh".into(),
                args: vec!["-c".into(), "echo MY_VAR=$MY_VAR && pwd".into()],
                env: vec!["MY_VAR=hello_from_env".into()],
                working_dir: Some("/tmp".into()),
                uid: None,
                gid: None,
                hostname: None,
                capture_output: true,
            },
        }],
    })
    .await?;

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodRunning { .. })
    })
    .await?;

    // Read log stream
    let (_header, mut log_stream) = tokio::time::timeout(
        EVENT_TIMEOUT,
        conn.accept_log_stream(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("timed out waiting for log stream"))??;

    let mut log_data = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(30), async {
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
    eprintln!("captured log output: {:?}", log_str);
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
                entrypoint: "/bin/sh".into(),
                args: vec!["-c".into(), "nslookup myservice 2>&1 || true".into()],
                env: vec![],
                working_dir: None,
                uid: None,
                gid: None,
                hostname: None,
                capture_output: true,
            },
        }],
    })
    .await?;

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodRunning { .. })
    })
    .await?;

    // Read log stream — verify the DNS resolution returned our registered IP
    let (_header, mut log_stream) = tokio::time::timeout(
        EVENT_TIMEOUT,
        conn.accept_log_stream(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("timed out waiting for log stream"))??;

    let mut log_data = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(30), async {
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
    eprintln!("captured log output: {:?}", log_str);
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

