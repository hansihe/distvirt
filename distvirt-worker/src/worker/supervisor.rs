use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use distvirt_worker_protocol::{
    ContainerSpec, LogStreamHeader, LogStreamOpener, NamespaceId, PodId, PodNetworkConfig,
    PoolId, SnapshotId, WorkerEvent,
};
use futures_lite::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::fabric::{Fabric, FabricPort};
use crate::image_provider::ImageProvider;
use crate::io_session::IoEvent;
use crate::managed_vm::ManagedVm;
use crate::oci;
use crate::task_handle::TaskHandle;
use crate::vmm::{NetConfig, SnapshotArtifacts, VmConfig, VmInstance, Vmm};

/// Timeout for graceful guest shutdown before force-killing.
pub(crate) const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// Outer timeout for awaiting a pod supervisor after cancellation.
pub(crate) const STOP_POD_TIMEOUT: Duration = Duration::from_secs(15);

/// Request to suspend a running pod.
pub(crate) struct SuspendRequest {
    pub(crate) snapshot_id: SnapshotId,
    pub(crate) snapshot_dir: PathBuf,
    pub(crate) pool_id: PoolId,
    pub(crate) reply: oneshot::Sender<Result<SnapshotArtifacts, String>>,
}

/// Per-pod state: cancellation token, supervisor task handle, and suspend channel.
pub(crate) struct PodState {
    pub(crate) cancel: CancellationToken,
    pub(crate) supervisor: TaskHandle<()>,
    pub(crate) suspend_tx: mpsc::Sender<SuspendRequest>,
}

/// Send an event to the worker main loop, or log and return if the worker is shutting down.
pub(crate) async fn send_event(tx: &mpsc::Sender<WorkerEvent>, event: WorkerEvent) {
    if tx.send(event).await.is_err() {
        log::warn!("failed to send event, worker already shut down");
    }
}

/// Top-level pod supervisor: launches the pod and monitors it.
///
/// On launch failure, sends `PodFailed` and returns.
/// On success, sends `PodRunning` then delegates to `pod_monitor`.
pub(crate) async fn pod_supervisor<V: Vmm + 'static, P: ImageProvider + 'static>(
    vmm: Arc<V>,
    image_provider: Arc<P>,
    fabric: Arc<Fabric<FabricPort>>,
    kernel_path: PathBuf,
    rootfs_image_path: PathBuf,
    log_opener: LogStreamOpener,
    cancel: CancellationToken,
    event_tx: mpsc::Sender<WorkerEvent>,
    namespace_id: NamespaceId,
    pod_id: PodId,
    network: PodNetworkConfig,
    containers: Vec<ContainerSpec>,
    suspend_rx: mpsc::Receiver<SuspendRequest>,
) {
    match pod_launch(
        &*vmm,
        &*image_provider,
        &fabric,
        &kernel_path,
        &rootfs_image_path,
        &log_opener,
        &event_tx,
        &namespace_id,
        &pod_id,
        network,
        containers,
        &cancel,
    )
    .await
    {
        Ok((vm, yamux_driver, io_session, port_task)) => {
            // Emit PodRunning event.
            send_event(
                &event_tx,
                WorkerEvent::PodRunning {
                    namespace_id: namespace_id.clone(),
                    pod_id: pod_id.clone(),
                },
            )
            .await;
            pod_monitor(vm, yamux_driver, io_session, port_task, cancel, event_tx, namespace_id, pod_id, suspend_rx).await;
        }
        Err(e) => {
            if cancel.is_cancelled() {
                log::info!("pod '{}': launch cancelled", pod_id);
                send_event(
                    &event_tx,
                    WorkerEvent::PodExited {
                        namespace_id,
                        pod_id: pod_id.clone(),
                        exit_code: -1,
                    },
                )
                .await;
            } else {
                log::error!("pod '{}': launch failed: {:#}", pod_id, e);
                send_event(
                    &event_tx,
                    WorkerEvent::PodFailed {
                        namespace_id,
                        pod_id: pod_id.clone(),
                        error: format!("{:#}", e),
                    },
                )
                .await;
            }
        }
    }
}

/// Top-level resume supervisor: restores a pod from a snapshot and monitors it.
///
/// Similar to `pod_supervisor` but calls `vmm.restore()` instead of launching fresh.
pub(crate) async fn pod_resume_supervisor<V: Vmm + 'static>(
    vmm: Arc<V>,
    fabric: Arc<Fabric<FabricPort>>,
    cancel: CancellationToken,
    event_tx: mpsc::Sender<WorkerEvent>,
    namespace_id: NamespaceId,
    pod_id: PodId,
    network: PodNetworkConfig,
    snapshot: SnapshotArtifacts,
    suspend_rx: mpsc::Receiver<SuspendRequest>,
) {
    match pod_restore(
        &*vmm,
        &fabric,
        &event_tx,
        &namespace_id,
        &pod_id,
        network,
        snapshot,
        &cancel,
    )
    .await
    {
        Ok((vm, yamux_driver, port_task)) => {
            send_event(
                &event_tx,
                WorkerEvent::PodRunning {
                    namespace_id: namespace_id.clone(),
                    pod_id: pod_id.clone(),
                },
            )
            .await;
            pod_monitor(vm, yamux_driver, None, port_task, cancel, event_tx, namespace_id, pod_id, suspend_rx).await;
        }
        Err(e) => {
            if cancel.is_cancelled() {
                log::info!("pod '{}': resume cancelled", pod_id);
                send_event(
                    &event_tx,
                    WorkerEvent::PodExited {
                        namespace_id,
                        pod_id: pod_id.clone(),
                        exit_code: -1,
                    },
                )
                .await;
            } else {
                log::error!("pod '{}': resume failed: {:#}", pod_id, e);
                send_event(
                    &event_tx,
                    WorkerEvent::PodFailed {
                        namespace_id,
                        pod_id: pod_id.clone(),
                        error: format!("{:#}", e),
                    },
                )
                .await;
            }
        }
    }
}

/// Perform all fallible restore: restore VM from snapshot, vsock connect,
/// add TAP to fabric. Containers are already running in the restored VM.
async fn pod_restore<V: Vmm + 'static>(
    vmm: &V,
    fabric: &Fabric<FabricPort>,
    _event_tx: &mpsc::Sender<WorkerEvent>,
    _namespace_id: &NamespaceId,
    pod_id: &PodId,
    network: PodNetworkConfig,
    snapshot: SnapshotArtifacts,
    cancel: &CancellationToken,
) -> anyhow::Result<(
    ManagedVm<V::Instance>,
    TaskHandle<anyhow::Result<()>>,
    Option<TaskHandle<()>>,
)> {
    let net_config = NetConfig {
        guest_ip: network.ip.to_string(),
        netmask: network.netmask.clone(),
        gateway: network.gateway.to_string(),
        guest_mac: network.mac,
    };

    let mut instance = tokio::select! {
        result = vmm.restore(&snapshot, Some(&net_config)) => {
            result.context("restore VM from snapshot")?
        }
        _ = cancel.cancelled() => {
            anyhow::bail!("cancelled during VM restore");
        }
    };
    log::info!("worker: pod '{}' VM restored from snapshot", pod_id);

    let port_task = if let Some(tap) = instance.take_tap() {
        let tap_name = tap.name.clone();
        let (_port_id, task) = fabric
            .add_tap_port(tap, network.ip, network.mac)
            .map_err(|e| anyhow::anyhow!("fabric add_port for {}: {}", tap_name, e))?;
        log::info!("worker: pod '{}' TAP {} added to fabric", pod_id, tap_name);
        Some(task)
    } else {
        None
    };

    let (vm, yamux_driver) = tokio::select! {
        result = ManagedVm::connect(instance) => { result? }
        _ = cancel.cancelled() => {
            anyhow::bail!("cancelled during VM connect after restore");
        }
    };

    Ok((vm, yamux_driver, port_task))
}

/// Perform all fallible pod setup: image prep, VM launch, vsock connect,
/// network config, container start, log stream setup.
async fn pod_launch<V: Vmm + 'static, P: ImageProvider + 'static>(
    vmm: &V,
    image_provider: &P,
    fabric: &Fabric<FabricPort>,
    kernel_path: &PathBuf,
    rootfs_image_path: &PathBuf,
    log_opener: &LogStreamOpener,
    event_tx: &mpsc::Sender<WorkerEvent>,
    namespace_id: &NamespaceId,
    pod_id: &PodId,
    network: PodNetworkConfig,
    containers: Vec<ContainerSpec>,
    cancel: &CancellationToken,
) -> anyhow::Result<(
    ManagedVm<V::Instance>,
    TaskHandle<anyhow::Result<()>>,
    Option<(crate::io_session::IoSession, yamux::Stream)>,
    Option<TaskHandle<()>>,
)> {
    let container = containers
        .into_iter()
        .next()
        .context("pod must have at least one container")?;

    let artifact = tokio::select! {
        result = image_provider.prepare(&container.image_ref) => {
            result.context("preparing image")?
        }
        _ = cancel.cancelled() => {
            anyhow::bail!("cancelled during image prepare");
        }
    };

    let config = if let Some(ref oci_config) = artifact.oci_config {
        oci::merge_config(oci_config, &container.config)?
    } else {
        container.config
    };

    let net_config = NetConfig {
        guest_ip: network.ip.to_string(),
        netmask: network.netmask.clone(),
        gateway: network.gateway.to_string(),
        guest_mac: network.mac,
    };

    let vm_config = VmConfig {
        kernel_path: kernel_path.clone(),
        rootfs_image_path: rootfs_image_path.clone(),
        container_image_path: artifact.image_path.clone(),
        vcpu_count: 1,
        mem_size_mib: 128,
        net: Some(net_config.clone()),
        serial_console: true,
    };

    let mut instance = tokio::select! {
        result = vmm.launch(&vm_config) => {
            result.context("launch VM")?
        }
        _ = cancel.cancelled() => {
            anyhow::bail!("cancelled during VM launch");
        }
    };
    log::info!("worker: pod '{}' VM launched", pod_id);

    let port_task = if let Some(tap) = instance.take_tap() {
        let tap_name = tap.name.clone();
        let (_port_id, task) = fabric
            .add_tap_port(tap, network.ip, network.mac)
            .map_err(|e| anyhow::anyhow!("fabric add_port for {}: {}", tap_name, e))?;
        log::info!("worker: pod '{}' TAP {} added to fabric", pod_id, tap_name);
        Some(task)
    } else {
        None
    };

    let (mut vm, yamux_driver) = tokio::select! {
        result = ManagedVm::connect(instance) => { result? }
        _ = cancel.cancelled() => {
            // instance is moved into connect(); on cancel, connect() is dropped,
            // which drops instance → FirecrackerInstance::drop sends SIGKILL.
            anyhow::bail!("cancelled during VM connect");
        }
    };

    vm.configure_network("eth0", &net_config).await?;

    let dns_servers = vec![network.gateway.to_string()];

    let container_id = &container.container_id;
    vm.add_container(container_id, "/dev/vdb", &dns_servers)
        .await?;

    vm.start_container(container_id, &config).await?;

    // Set up log streaming via yamux log streams.
    let io_session = if config.capture_output {
        match vm.accept_output_stream().await {
            Ok((_cid, session)) => {
                let header = LogStreamHeader {
                    namespace_id: namespace_id.clone(),
                    pod_id: pod_id.clone(),
                    container_id: container_id.to_string(),
                };
                match log_opener.open_log_stream(&header).await {
                    Ok(log_stream) => Some((session, log_stream)),
                    Err(e) => {
                        log::error!("pod '{}': failed to open log stream: {:#}", pod_id, e);
                        send_event(
                            event_tx,
                            WorkerEvent::PodLogStreamError {
                                namespace_id: namespace_id.clone(),
                                pod_id: pod_id.clone(),
                                container_id: container_id.to_string(),
                                phase: "open_stream".to_string(),
                                error: format!("{:#}", e),
                            },
                        )
                        .await;
                        None
                    }
                }
            }
            Err(e) => {
                log::error!("pod '{}': failed to accept output stream: {:#}", pod_id, e);
                send_event(
                    event_tx,
                    WorkerEvent::PodLogStreamError {
                        namespace_id: namespace_id.clone(),
                        pod_id: pod_id.clone(),
                        container_id: container_id.to_string(),
                        phase: "connect".to_string(),
                        error: format!("{:#}", e),
                    },
                )
                .await;
                None
            }
        }
    } else {
        None
    };

    Ok((vm, yamux_driver, io_session, port_task))
}

/// Timeout for suspend handshake with guest.
const SUSPEND_TIMEOUT: Duration = Duration::from_secs(10);

/// Pod monitor: watches a running pod's sub-tasks and handles cleanup.
///
/// This owns the `ManagedVm` and coordinates between container exit,
/// yamux driver health, log streaming, suspend requests, and cancellation.
async fn pod_monitor<I: VmInstance>(
    mut vm: ManagedVm<I>,
    mut yamux_driver: TaskHandle<anyhow::Result<()>>,
    io_session: Option<(crate::io_session::IoSession, yamux::Stream)>,
    port_task: Option<TaskHandle<()>>,
    cancel: CancellationToken,
    event_tx: mpsc::Sender<WorkerEvent>,
    namespace_id: NamespaceId,
    pod_id: PodId,
    mut suspend_rx: mpsc::Receiver<SuspendRequest>,
) {
    // Spawn log streaming as a non-fatal sub-task.
    // Uses TaskHandle so it's automatically aborted when monitor exits.
    let _log_task = io_session.map(|(mut session, mut log_stream)| {
        let log_pod_id = pod_id.clone();
        TaskHandle::spawn(async move {
            loop {
                match session.next_event().await {
                    Ok(IoEvent::Stdout(data)) | Ok(IoEvent::Stderr(data)) => {
                        if log_stream.write_all(&data).await.is_err() {
                            break;
                        }
                    }
                    Ok(IoEvent::Eof) => break,
                    Err(e) => {
                        log::warn!("pod '{}' log stream error: {:#}", log_pod_id, e);
                        break;
                    }
                }
            }
            let _ = log_stream.close().await;
        })
    });

    // Create a future that completes when the port task exits, or pends forever if there is none.
    let mut port_task = port_task;
    let mut port_task_fut = std::pin::pin!(async {
        match port_task.as_mut() {
            Some(task) => { let _ = task.await; }
            None => std::future::pending::<()>().await,
        }
    });

    let event = tokio::select! {
        // Normal path: container exits.
        result = vm.wait_container_exit() => {
            match result {
                Ok((_container_id, exit_code)) => {
                    // Gracefully shut down the VM.
                    match tokio::time::timeout(GRACEFUL_SHUTDOWN_TIMEOUT, vm.graceful_shutdown(Duration::from_secs(8))).await {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => {
                            log::warn!("pod '{}': shutdown error: {:#}, force killing", pod_id, e);
                            let _ = vm.force_kill().await;
                        }
                        Err(_) => {
                            log::warn!("pod '{}': shutdown timed out, force killing", pod_id);
                            let _ = vm.force_kill().await;
                        }
                    }
                    WorkerEvent::PodExited {
                        namespace_id: namespace_id.clone(),
                        pod_id: pod_id.clone(),
                        exit_code,
                    }
                }
                Err(e) => {
                    log::error!("pod '{}': wait_container_exit error: {:#}", pod_id, e);
                    let _ = vm.force_kill().await;
                    WorkerEvent::PodFailed {
                        namespace_id: namespace_id.clone(),
                        pod_id: pod_id.clone(),
                        error: format!("{:#}", e),
                    }
                }
            }
        }

        // Fatal: yamux driver died unexpectedly.
        result = &mut yamux_driver => {
            let error = match result {
                Ok(Ok(())) => "yamux driver exited unexpectedly".to_string(),
                Ok(Err(e)) => format!("yamux driver error: {:#}", e),
                Err(e) => format!("yamux driver task panicked: {}", e),
            };
            log::error!("pod '{}': {}", pod_id, error);
            let _ = vm.force_kill().await;
            WorkerEvent::PodFailed {
                namespace_id: namespace_id.clone(),
                pod_id: pod_id.clone(),
                error,
            }
        }

        // Fatal: port read task died (TAP error, etc.).
        _ = &mut port_task_fut => {
            log::error!("pod '{}': port task exited, network dead — force killing VM", pod_id);
            let _ = vm.force_kill().await;
            WorkerEvent::PodFailed {
                namespace_id: namespace_id.clone(),
                pod_id: pod_id.clone(),
                error: "port task exited unexpectedly".to_string(),
            }
        }

        // Suspend request: snapshot the VM and exit.
        Some(req) = suspend_rx.recv() => {
            log::info!("pod '{}': suspend requested, snapshot_id={}", pod_id, req.snapshot_id);
            match vm.suspend(&req.snapshot_dir, SUSPEND_TIMEOUT).await {
                Ok(artifacts) => {
                    // Calculate snapshot size.
                    let snapshot_size_bytes = dir_size(&req.snapshot_dir).await.unwrap_or(0);
                    let _ = req.reply.send(Ok(artifacts));
                    send_event(
                        &event_tx,
                        WorkerEvent::PodSuspended {
                            namespace_id: namespace_id.clone(),
                            pod_id: pod_id.clone(),
                            snapshot_id: req.snapshot_id,
                            snapshot_size_bytes,
                            pool_id: req.pool_id,
                        },
                    )
                    .await;
                    return; // VM is dead after suspend, exit monitor.
                }
                Err(e) => {
                    let err_msg = format!("{:#}", e);
                    log::error!("pod '{}': suspend failed: {}", pod_id, err_msg);
                    let _ = req.reply.send(Err(err_msg.clone()));
                    let _ = vm.force_kill().await;
                    WorkerEvent::PodSuspendFailed {
                        namespace_id: namespace_id.clone(),
                        pod_id: pod_id.clone(),
                        error: err_msg,
                    }
                }
            }
        }

        // Cancellation: graceful shutdown requested.
        _ = cancel.cancelled() => {
            log::info!("pod '{}': cancellation received, shutting down gracefully", pod_id);
            match tokio::time::timeout(GRACEFUL_SHUTDOWN_TIMEOUT, vm.graceful_shutdown(Duration::from_secs(8))).await {
                Ok(Ok(())) => {
                    log::info!("pod '{}': graceful shutdown complete", pod_id);
                }
                Ok(Err(e)) => {
                    log::warn!("pod '{}': graceful shutdown error: {:#}, force killing", pod_id, e);
                    let _ = vm.force_kill().await;
                }
                Err(_) => {
                    log::warn!("pod '{}': graceful shutdown timed out, force killing", pod_id);
                    let _ = vm.force_kill().await;
                }
            }
            WorkerEvent::PodExited {
                namespace_id: namespace_id.clone(),
                pod_id: pod_id.clone(),
                exit_code: -1,
            }
        }
    };

    // _log_task is dropped here, automatically aborting via TaskHandle.

    // Send the event back to the main loop.
    send_event(&event_tx, event).await;
}

/// Calculate the total size of files in a directory.
async fn dir_size(path: &std::path::Path) -> anyhow::Result<u64> {
    let mut total = 0u64;
    let mut entries = tokio::fs::read_dir(path).await?;
    while let Some(entry) = entries.next_entry().await? {
        let meta = entry.metadata().await?;
        if meta.is_file() {
            total += meta.len();
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;
    use std::path::PathBuf;

    use distvirt_worker_protocol::{
        ContainerConfig, ContainerSpec, PodNetworkConfig,
    };
    use tokio::net::UnixStream;

    use crate::fabric::{Fabric, FabricPort};
    use crate::image_provider::{ImageProvider, PreparedArtifact};
    use crate::tap::TapDevice;
    use crate::vmm::{VmConfig, VmInstance, Vmm};

    // -----------------------------------------------------------------------
    // Stubs & Mocks
    // -----------------------------------------------------------------------

    struct StubVmm;

    impl Vmm for StubVmm {
        type Instance = StubVmInstance;
        async fn launch(&self, _config: &VmConfig) -> anyhow::Result<StubVmInstance> {
            panic!("StubVmm::launch should not be called");
        }
    }

    struct StubVmInstance;

    impl VmInstance for StubVmInstance {
        async fn connect_vsock(&self, _port: u32) -> anyhow::Result<UnixStream> {
            panic!("StubVmInstance::connect_vsock called");
        }
        fn tap(&self) -> Option<&TapDevice> {
            None
        }
        fn take_tap(&mut self) -> Option<TapDevice> {
            None
        }
        async fn wait(&mut self) -> anyhow::Result<()> {
            std::future::pending().await
        }
        async fn kill(&mut self) -> anyhow::Result<()> {
            Ok(())
        }
    }

    struct MockVmm {
        /// If Some, launch() returns this error.
        launch_error: Option<String>,
        /// The mock VM's vsock socket (worker side).
        vm_socket: tokio::sync::Mutex<Option<UnixStream>>,
    }

    struct MockVmInstance {
        vsock_socket: tokio::sync::Mutex<Option<UnixStream>>,
        killed: tokio::sync::Mutex<bool>,
    }

    impl Vmm for MockVmm {
        type Instance = MockVmInstance;
        async fn launch(&self, _config: &VmConfig) -> anyhow::Result<MockVmInstance> {
            if let Some(ref err) = self.launch_error {
                return Err(anyhow::anyhow!("{}", err));
            }
            let socket = self
                .vm_socket
                .lock()
                .await
                .take()
                .expect("MockVmm: socket already taken");
            Ok(MockVmInstance {
                vsock_socket: tokio::sync::Mutex::new(Some(socket)),
                killed: tokio::sync::Mutex::new(false),
            })
        }
    }

    impl VmInstance for MockVmInstance {
        async fn connect_vsock(&self, _port: u32) -> anyhow::Result<UnixStream> {
            self.vsock_socket
                .lock()
                .await
                .take()
                .ok_or_else(|| anyhow::anyhow!("MockVmInstance: vsock already connected"))
        }
        fn tap(&self) -> Option<&TapDevice> {
            None
        }
        fn take_tap(&mut self) -> Option<TapDevice> {
            None
        }
        async fn wait(&mut self) -> anyhow::Result<()> {
            std::future::pending().await
        }
        async fn kill(&mut self) -> anyhow::Result<()> {
            *self.killed.lock().await = true;
            Ok(())
        }
    }

    struct FailingImageProvider {
        error_msg: String,
    }

    impl ImageProvider for FailingImageProvider {
        async fn prepare(&self, _image_ref: &str) -> anyhow::Result<PreparedArtifact> {
            Err(anyhow::anyhow!("{}", self.error_msg))
        }
    }

    struct MockImageProvider;

    impl ImageProvider for MockImageProvider {
        async fn prepare(&self, _image_ref: &str) -> anyhow::Result<PreparedArtifact> {
            Ok(PreparedArtifact::new(
                PathBuf::from("/fake/image.ext4"),
                None, // no OCI config
                (),   // no cleanup
            ))
        }
    }

    fn make_pod_network() -> PodNetworkConfig {
        PodNetworkConfig {
            ip: Ipv4Addr::new(172, 16, 0, 10),
            mac: [0x02, 0, 0, 0, 0, 0x10],
            gateway: Ipv4Addr::new(172, 16, 0, 1),
            netmask: "255.255.255.0".to_string(),
        }
    }

    fn make_containers() -> Vec<ContainerSpec> {
        vec![ContainerSpec {
            container_id: "main".to_string(),
            image_ref: "test-image:latest".to_string(),
            config: ContainerConfig {
                entrypoint: vec!["/bin/echo".to_string()],
                args: vec!["hello".to_string()],
                env: vec![],
                working_dir: None,
                uid: None,
                gid: None,
                hostname: None,
                capture_output: false,
                stdin: false,
            },
        }]
    }

    fn make_log_opener() -> LogStreamOpener {
        LogStreamOpener::disconnected()
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn image_provider_failure_sends_pod_failed() {
        let (bg_event_tx, mut bg_event_rx) = mpsc::channel(256);
        let image_provider = Arc::new(FailingImageProvider {
            error_msg: "image not found".to_string(),
        });
        let vmm = Arc::new(StubVmm);
        let fabric = Arc::new(Fabric::<FabricPort>::new(Ipv4Addr::new(172, 16, 0, 0), 16));
        let cancel = CancellationToken::new();

        let log_opener = make_log_opener();

        let ns_id = NamespaceId::from("ns1");
        let pod_id = PodId::from("pod1");

        // Run pod_supervisor directly.
        tokio::spawn({
            let ns_id = ns_id.clone();
            let pod_id = pod_id.clone();
            let cancel = cancel.clone();
            async move {
                let (_suspend_tx, suspend_rx) = mpsc::channel(1);
                pod_supervisor(
                    vmm,
                    image_provider,
                    fabric,
                    PathBuf::from("/fake/kernel"),
                    PathBuf::from("/fake/rootfs"),
                    log_opener,
                    cancel,
                    bg_event_tx,
                    ns_id,
                    pod_id,
                    make_pod_network(),
                    make_containers(),
                    suspend_rx,
                )
                .await;
            }
        });

        // Should receive PodFailed.
        let event = tokio::time::timeout(Duration::from_secs(5), bg_event_rx.recv())
            .await
            .expect("timeout waiting for event")
            .expect("channel closed");

        match event {
            WorkerEvent::PodFailed {
                namespace_id,
                pod_id,
                error,
            } => {
                assert_eq!(namespace_id, "ns1");
                assert_eq!(pod_id, "pod1");
                assert!(
                    error.contains("image not found"),
                    "error should mention image failure: {}",
                    error
                );
            }
            other => panic!("expected PodFailed, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn vm_launch_failure_sends_pod_failed() {
        let (worker_socket, _guest_socket) = UnixStream::pair().unwrap();
        let vmm = Arc::new(MockVmm {
            launch_error: Some("VM exploded".to_string()),
            vm_socket: tokio::sync::Mutex::new(Some(worker_socket)),
        });
        let image_provider = Arc::new(MockImageProvider);
        let fabric = Arc::new(Fabric::<FabricPort>::new(Ipv4Addr::new(172, 16, 0, 0), 16));
        let cancel = CancellationToken::new();

        let log_opener = make_log_opener();
        let (bg_event_tx, mut bg_event_rx) = mpsc::channel(256);

        let ns_id = NamespaceId::from("ns1");
        let pod_id = PodId::from("pod1");

        tokio::spawn({
            let ns_id = ns_id.clone();
            let pod_id = pod_id.clone();
            let cancel = cancel.clone();
            async move {
                let (_suspend_tx, suspend_rx) = mpsc::channel(1);
                pod_supervisor(
                    vmm,
                    image_provider,
                    fabric,
                    PathBuf::from("/fake/kernel"),
                    PathBuf::from("/fake/rootfs"),
                    log_opener,
                    cancel,
                    bg_event_tx,
                    ns_id,
                    pod_id,
                    make_pod_network(),
                    make_containers(),
                    suspend_rx,
                )
                .await;
            }
        });

        let event = tokio::time::timeout(Duration::from_secs(5), bg_event_rx.recv())
            .await
            .expect("timeout waiting for event")
            .expect("channel closed");

        match event {
            WorkerEvent::PodFailed { error, .. } => {
                assert!(
                    error.contains("VM exploded"),
                    "error should mention VM failure: {}",
                    error
                );
            }
            other => panic!("expected PodFailed, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn cancel_during_image_prepare_sends_pod_exited() {
        // Use a slow image provider that waits forever.
        struct HangingImageProvider;
        impl ImageProvider for HangingImageProvider {
            async fn prepare(&self, _image_ref: &str) -> anyhow::Result<PreparedArtifact> {
                std::future::pending().await
            }
        }

        let vmm = Arc::new(StubVmm);
        let image_provider = Arc::new(HangingImageProvider);
        let fabric = Arc::new(Fabric::<FabricPort>::new(Ipv4Addr::new(172, 16, 0, 0), 16));
        let cancel = CancellationToken::new();

        let log_opener = make_log_opener();
        let (bg_event_tx, mut bg_event_rx) = mpsc::channel(256);

        let cancel_clone = cancel.clone();
        tokio::spawn(async move {
            let (_suspend_tx, suspend_rx) = mpsc::channel(1);
            pod_supervisor(
                vmm,
                image_provider,
                fabric,
                PathBuf::from("/fake/kernel"),
                PathBuf::from("/fake/rootfs"),
                log_opener,
                cancel_clone,
                bg_event_tx,
                NamespaceId::from("ns1"),
                PodId::from("pod1"),
                make_pod_network(),
                make_containers(),
                suspend_rx,
            )
            .await;
        });

        // Cancel after a short delay.
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();

        let event = tokio::time::timeout(Duration::from_secs(5), bg_event_rx.recv())
            .await
            .expect("timeout")
            .expect("channel closed");

        match event {
            WorkerEvent::PodExited { exit_code, .. } => {
                assert_eq!(exit_code, -1, "cancelled pod should exit with -1");
            }
            other => panic!("expected PodExited(-1), got {:?}", other),
        }
    }
}
