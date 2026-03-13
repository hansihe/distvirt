use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::time::Duration;

use futures_lite::io::AsyncReadExt;

use distvirt_worker_protocol::{
    EndpointKind, EndpointPlacement, EndpointSpec, NetworkConfig, OrchestratorConnection,
    PodNetworkConfig, PoolInfo, WorkerAccepted, WorkerCommand, WorkerConnection, WorkerEvent,
    WorkerId,
};

/// Returns `true` if the E2E env var is set, otherwise prints a skip message.
pub fn should_run() -> bool {
    if std::env::var("DISTVIRT_E2E").is_ok() {
        true
    } else {
        eprintln!("DISTVIRT_E2E not set, skipping integration test");
        false
    }
}

/// Result of setting up a worker, including connection and optional metadata.
pub struct WorkerSetup {
    pub conn: OrchestratorConnection,
    pub handle: tokio::task::JoinHandle<anyhow::Result<()>>,
    pub transfer_listen_port: Option<u16>,
}

/// Spawn a worker on one half of a duplex and return the orchestrator connection
/// along with the worker task handle.
pub async fn setup() -> anyhow::Result<(OrchestratorConnection, tokio::task::JoinHandle<anyhow::Result<()>>)> {
    let ws = setup_full(None, None, vec![]).await?;
    Ok((ws.conn, ws.handle))
}

/// Like `setup()`, but with a WASM component directory for activator support.
pub async fn setup_with_activators() -> anyhow::Result<(OrchestratorConnection, tokio::task::JoinHandle<anyhow::Result<()>>)> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let component_dir = manifest_dir.join("../activators/target/components");
    assert!(
        component_dir.exists(),
        "WASM component directory not found at {}. Run activators/build.sh first.",
        component_dir.display()
    );
    let ws = setup_full(Some(component_dir), None, vec![]).await?;
    Ok((ws.conn, ws.handle))
}

/// Like `setup()`, but with pools pushed to the worker via handshake and a custom worker ID.
pub async fn setup_with_pools(
    worker_id: &str,
    pushed_pools: Vec<PoolInfo>,
) -> anyhow::Result<(OrchestratorConnection, tokio::task::JoinHandle<anyhow::Result<()>>)> {
    let ws = setup_full(None, Some(worker_id), pushed_pools).await?;
    Ok((ws.conn, ws.handle))
}

/// Like `setup_with_pools()`, but also returns the worker's transfer listen port.
pub async fn setup_with_pools_full(
    worker_id: &str,
    pushed_pools: Vec<PoolInfo>,
) -> anyhow::Result<WorkerSetup> {
    setup_full(None, Some(worker_id), pushed_pools).await
}

async fn setup_full(
    component_dir: Option<PathBuf>,
    worker_id: Option<&str>,
    pushed_pools: Vec<PoolInfo>,
) -> anyhow::Result<WorkerSetup> {
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
        docker_config: None,
    };

    let (orch_half, worker_half) = tokio::io::duplex(64 * 1024);

    let worker_handle = tokio::spawn(async move {
        let conn = WorkerConnection::accept(worker_half).await.unwrap();
        let worker = distvirt_worker::worker::Worker::<_, _, _, distvirt_worker::TokioFs>::new(kernel, rootfs, vmm, image_provider, component_dir, String::new(), distvirt_worker::TunGatewayProvider);
        worker.run(conn, "test-secret".to_string()).await
    });

    let wid = worker_id.unwrap_or("test-worker");
    let mut conn = OrchestratorConnection::connect(orch_half).await?;

    // Perform handshake
    let hello = conn.recv_hello().await?;
    eprintln!("e2e: worker '{}' capabilities: {:?}", wid, hello.capabilities);

    conn.send_accepted(&WorkerAccepted {
        worker_id: WorkerId::from(wid),
        adapters: vec![],
        tunnel_encrypted: true,
        pools: pushed_pools,
    })
    .await?;

    let ready = conn.recv_ready().await?;
    eprintln!("e2e: worker '{}' handshake complete (transfer_port: {:?})", wid, ready.transfer_listen_port);

    Ok(WorkerSetup {
        conn,
        handle: worker_handle,
        transfer_listen_port: ready.transfer_listen_port,
    })
}

/// Accept a log stream and drain it to stderr for debugging, returning the
/// collected output as a string. Useful for tests that want to print container
/// output without necessarily asserting on it.
pub async fn drain_log_stream(conn: &mut OrchestratorConnection) -> anyhow::Result<String> {
    let (header, mut log_stream) = tokio::time::timeout(
        EVENT_TIMEOUT,
        conn.accept_log_stream(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("timed out waiting for log stream"))??;

    let prefix = format!("[{}/{}]", header.pod_id, header.container_id);
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

    let log_str = String::from_utf8_lossy(&log_data).into_owned();
    for line in log_str.lines() {
        eprintln!("{} {}", prefix, line);
    }
    Ok(log_str)
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
pub async fn shutdown_worker(
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

/// Abort the worker task immediately without graceful shutdown.
///
/// Use this in tests that don't need to validate the shutdown path and want
/// fast teardown (e.g. when pods are running long-lived processes like `sleep`).
pub async fn force_shutdown_worker(
    worker_handle: tokio::task::JoinHandle<anyhow::Result<()>>,
) {
    worker_handle.abort();
    let _ = worker_handle.await;
}

pub fn test_network_config() -> NetworkConfig {
    NetworkConfig {
        subnet: Ipv4Addr::new(10, 0, 0, 0),
        gateway: Ipv4Addr::new(10, 0, 0, 1),
        prefix_len: 24,
        segment_id: None,
    }
}

pub fn test_pod_network_config() -> PodNetworkConfig {
    PodNetworkConfig {
        ip: Ipv4Addr::new(10, 0, 0, 2),
        mac: [0x02, 0x00, 0x00, 0x00, 0x00, 0x01],
        gateway: Ipv4Addr::new(10, 0, 0, 1),
        netmask: "255.255.255.0".into(),
    }
}

pub fn test_pod_network_config_2() -> PodNetworkConfig {
    PodNetworkConfig {
        ip: Ipv4Addr::new(10, 0, 0, 3),
        mac: [0x02, 0x00, 0x00, 0x00, 0x00, 0x02],
        gateway: Ipv4Addr::new(10, 0, 0, 1),
        netmask: "255.255.255.0".into(),
    }
}

/// Helper: receive the next WorkerEvent with a timeout.
pub async fn recv_event_timeout(
    conn: &mut OrchestratorConnection,
    timeout: Duration,
) -> anyhow::Result<WorkerEvent> {
    tokio::time::timeout(timeout, conn.recv_event())
        .await
        .map_err(|_| anyhow::anyhow!("timed out waiting for event"))?
}

/// Helper: drain events until we find one matching the predicate, with a deadline.
pub async fn recv_until<F: Fn(&WorkerEvent) -> bool>(
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

pub const EVENT_TIMEOUT: Duration = Duration::from_secs(120);

/// Register a pod endpoint in the fabric before launching the pod.
///
/// In production, the orchestrator sends EndpointUpdate before LaunchPod so the
/// fabric's endpoint table already has an entry when `add_tap_port` calls
/// `attach_port`. Without this, `attach_port` races and logs spurious errors.
pub async fn register_pod_endpoint(
    conn: &mut OrchestratorConnection,
    namespace_id: &str,
    pod_network: &PodNetworkConfig,
    worker_id: &str,
) -> anyhow::Result<()> {
    conn.send_command(&WorkerCommand::EndpointUpdate {
        namespace_id: namespace_id.into(),
        upserted: vec![EndpointSpec {
            ip: pod_network.ip,
            kind: EndpointKind::Pod {
                placement: Some(EndpointPlacement {
                    worker_id: WorkerId::from(worker_id),
                }),
            },
        }],
        removed_ips: vec![],
    })
    .await?;
    Ok(())
}
