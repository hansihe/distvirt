use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::Context;
use distvirt_worker_protocol::{
    ContainerSpec, LogStreamHeader, LogStreamOpener, PodNetworkConfig, RegistryEntry,
    WorkerCommand, WorkerConnection, WorkerEvent,
};
use futures_lite::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::task_handle::TaskHandle;

use crate::fabric::{DnsRegistry, Fabric, FabricGateway};
use crate::image_provider::ImageProvider;
use crate::io_session::IoEvent;
use crate::managed_vm::{ImageOverrides, ManagedVm, merge_config};
use crate::vmm::{NetConfig, VmConfig, VmInstance, Vmm};

/// Timeout for graceful guest shutdown before force-killing.
const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// Outer timeout for awaiting a pod supervisor after cancellation.
const STOP_POD_TIMEOUT: Duration = Duration::from_secs(15);

/// Errors that should kill the entire worker.
/// INTENTIONALLY no From<anyhow::Error> — forces explicit construction.
#[derive(Debug)]
enum FatalError {
    ConnectionLost(anyhow::Error),
    InternalInvariant(String),
}

impl fmt::Display for FatalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FatalError::ConnectionLost(e) => write!(f, "connection lost: {:#}", e),
            FatalError::InternalInvariant(msg) => {
                write!(f, "internal invariant violated: {}", msg)
            }
        }
    }
}

/// Per-pod state: cancellation token and supervisor task handle.
struct PodState {
    cancel: CancellationToken,
    supervisor: TaskHandle<()>,
}

/// Per-namespace state: fabric, gateway, registry, pods, and cancellation token.
struct NamespaceState {
    fabric: Arc<tokio::sync::Mutex<Fabric>>,
    _gateway_task: TaskHandle<()>,
    registry: DnsRegistry,
    pods: HashMap<String, PodState>,
    token: CancellationToken,
}

/// The worker: sits between the orchestrator and the raw VM/fabric primitives.
///
/// Receives `WorkerCommand`s via a `WorkerConnection`, sends `WorkerEvent`s back,
/// and opens yamux log streams for container output.
pub struct Worker<V: Vmm + 'static, P: ImageProvider + 'static> {
    kernel_path: PathBuf,
    rootfs_image_path: PathBuf,
    namespaces: HashMap<String, NamespaceState>,
    vmm: Arc<V>,
    image_provider: Arc<P>,
    /// Channel for background tasks to send events back to the main loop.
    bg_event_tx: mpsc::Sender<WorkerEvent>,
    bg_event_rx: mpsc::Receiver<WorkerEvent>,
    /// Root cancellation token for the entire worker.
    worker_token: CancellationToken,
}

impl<V: Vmm + 'static, P: ImageProvider + 'static> Worker<V, P> {
    pub fn new(
        kernel_path: PathBuf,
        rootfs_image_path: PathBuf,
        vmm: V,
        image_provider: P,
    ) -> Self {
        let (bg_event_tx, bg_event_rx) = mpsc::channel(256);
        Worker {
            kernel_path,
            rootfs_image_path,
            namespaces: HashMap::new(),
            vmm: Arc::new(vmm),
            image_provider: Arc::new(image_provider),
            bg_event_tx,
            bg_event_rx,
            worker_token: CancellationToken::new(),
        }
    }

    /// Run the worker main loop: receive commands, dispatch them,
    /// and forward background events to the orchestrator.
    pub async fn run(mut self, mut conn: WorkerConnection) -> anyhow::Result<()> {
        let log_opener = conn.log_stream_opener();

        let result = loop {
            tokio::select! {
                cmd_result = conn.recv_command() => {
                    match cmd_result {
                        Ok(WorkerCommand::Shutdown) => {
                            log::info!("worker: received Shutdown command");
                            let _ = conn.send_event(&WorkerEvent::ShuttingDown).await;
                            break Ok(());
                        }
                        Ok(cmd) => {
                            if let Err(e) = self.handle_command(cmd, &log_opener).await {
                                log::error!("worker: fatal error: {}", e);
                                break Err(anyhow::anyhow!("{}", e));
                            }
                        }
                        Err(e) => {
                            log::error!("worker: connection lost: {:#}", e);
                            break Err(e);
                        }
                    }
                }
                Some(event) = self.bg_event_rx.recv() => {
                    if let Err(e) = conn.send_event(&event).await {
                        log::error!("worker: failed to send event: {:#}", e);
                        break Err(e);
                    }
                }
            }
        };

        // Shutdown all pods on any exit path.
        self.shutdown_all().await;
        result
    }

    /// Cancel all tokens and await all pod supervisors.
    async fn shutdown_all(&mut self) {
        log::info!("worker: shutting down all namespaces and pods");
        self.worker_token.cancel();

        for (ns_id, ns) in self.namespaces.drain() {
            for (pod_id, pod) in ns.pods {
                log::info!("worker: awaiting pod '{}' in namespace '{}'", pod_id, ns_id);
                match tokio::time::timeout(STOP_POD_TIMEOUT, pod.supervisor).await {
                    Ok(Ok(())) => { /* clean exit */ }
                    Ok(Err(join_error)) => {
                        log::error!(
                            "worker: pod '{}' supervisor panicked: {}",
                            pod_id,
                            join_error
                        );
                    }
                    Err(_) => {
                        log::warn!(
                            "worker: pod '{}' supervisor timed out, aborting",
                            pod_id
                        );
                        // pod.supervisor (TaskHandle) drops here, automatically aborting.
                    }
                }
            }
            // ns._gateway_task (TaskHandle) drops here, automatically aborting.
        }
    }

    /// Handle a single command.
    async fn handle_command(
        &mut self,
        cmd: WorkerCommand,
        log_opener: &LogStreamOpener,
    ) -> Result<(), FatalError> {
        match cmd {
            WorkerCommand::CreateNamespace {
                namespace_id,
                network: _network,
            } => self.handle_create_namespace(namespace_id).await,
            WorkerCommand::DestroyNamespace { namespace_id } => {
                self.handle_destroy_namespace(&namespace_id).await
            }
            WorkerCommand::RegistrySync {
                namespace_id,
                entries,
            } => self.handle_registry_sync(&namespace_id, entries),
            WorkerCommand::LaunchPod {
                namespace_id,
                pod_id,
                network,
                containers,
            } => {
                self.handle_launch_pod(&namespace_id, pod_id, network, containers, log_opener)
                    .await
            }
            WorkerCommand::StopPod {
                namespace_id,
                pod_id,
                graceful,
            } => self.handle_stop_pod(&namespace_id, &pod_id, graceful).await,
            WorkerCommand::Shutdown => {
                // Handled in the main loop; should not reach here.
                unreachable!("Shutdown handled in run()")
            }
        }
    }

    async fn handle_create_namespace(
        &mut self,
        namespace_id: String,
    ) -> Result<(), FatalError> {
        let mut fabric = Fabric::new();

        let registry: DnsRegistry = Arc::new(RwLock::new(HashMap::new()));

        let (gateway, egress_tx, ingress_rx) = FabricGateway::new(Arc::clone(&registry))
            .map_err(|e| {
                FatalError::InternalInvariant(format!("create fabric gateway: {:#}", e))
            })?;
        fabric.set_gateway(egress_tx, ingress_rx);

        let ns_token = self.worker_token.child_token();
        let ns_cancel = ns_token.clone();
        let gateway_ns_id = namespace_id.clone();

        let gateway_task = TaskHandle::spawn(async move {
            gateway.run().await;
            log::error!(
                "namespace '{}': gateway exited, cancelling all pods",
                gateway_ns_id
            );
            ns_cancel.cancel();
        });

        log::info!(
            "worker: created namespace '{}' with fabric + gateway",
            namespace_id
        );

        let ns = NamespaceState {
            fabric: Arc::new(tokio::sync::Mutex::new(fabric)),
            _gateway_task: gateway_task,
            registry,
            pods: HashMap::new(),
            token: ns_token,
        };

        self.namespaces.insert(namespace_id.clone(), ns);

        // Send via background event channel — handlers never touch conn directly.
        let _ = self
            .bg_event_tx
            .send(WorkerEvent::NamespaceCreated { namespace_id })
            .await;
        Ok(())
    }

    async fn handle_destroy_namespace(&mut self, namespace_id: &str) -> Result<(), FatalError> {
        if let Some(ns) = self.namespaces.remove(namespace_id) {
            // Cancel the namespace token, cascading to all pods.
            ns.token.cancel();

            // Await all pod supervisors with timeout.
            for (pod_id, pod) in ns.pods {
                log::info!(
                    "worker: awaiting pod '{}' for namespace '{}' destruction",
                    pod_id,
                    namespace_id
                );
                match tokio::time::timeout(STOP_POD_TIMEOUT, pod.supervisor).await {
                    Ok(Ok(())) => { /* clean exit */ }
                    Ok(Err(join_error)) => {
                        log::error!(
                            "worker: pod '{}' supervisor panicked: {}",
                            pod_id,
                            join_error
                        );
                    }
                    Err(_) => {
                        log::warn!(
                            "worker: pod '{}' supervisor timed out during namespace destroy, aborting",
                            pod_id
                        );
                        // pod.supervisor (TaskHandle) drops here, automatically aborting.
                    }
                }
            }
            // ns._gateway_task (TaskHandle) drops here, automatically aborting.
            log::info!("worker: destroyed namespace '{}'", namespace_id);
        }
        Ok(())
    }

    fn handle_registry_sync(
        &mut self,
        namespace_id: &str,
        entries: Vec<RegistryEntry>,
    ) -> Result<(), FatalError> {
        let ns = self.namespaces.get_mut(namespace_id).ok_or_else(|| {
            FatalError::InternalInvariant(format!("namespace '{}' not found", namespace_id))
        })?;

        let mut map = ns.registry.write().map_err(|e| {
            FatalError::InternalInvariant(format!("registry lock poisoned: {}", e))
        })?;
        map.clear();
        for entry in entries {
            map.insert(entry.name, entry.ip);
        }

        log::info!("worker: synced registry for namespace '{}'", namespace_id);
        Ok(())
    }

    async fn handle_launch_pod(
        &mut self,
        namespace_id: &str,
        pod_id: String,
        network: PodNetworkConfig,
        containers: Vec<ContainerSpec>,
        log_opener: &LogStreamOpener,
    ) -> Result<(), FatalError> {
        let ns = self.namespaces.get_mut(namespace_id).ok_or_else(|| {
            FatalError::InternalInvariant(format!("namespace '{}' not found", namespace_id))
        })?;

        let pod_cancel = ns.token.child_token();
        let event_tx = self.bg_event_tx.clone();
        let vmm = Arc::clone(&self.vmm);
        let image_provider = Arc::clone(&self.image_provider);
        let fabric = Arc::clone(&ns.fabric);
        let kernel_path = self.kernel_path.clone();
        let rootfs_image_path = self.rootfs_image_path.clone();
        let log_opener = log_opener.clone();
        let ns_id = namespace_id.to_string();
        let pid = pod_id.clone();
        let cancel_clone = pod_cancel.clone();

        let supervisor = TaskHandle::spawn(async move {
            pod_supervisor(
                vmm,
                image_provider,
                fabric,
                kernel_path,
                rootfs_image_path,
                log_opener,
                cancel_clone,
                event_tx,
                ns_id,
                pid,
                network,
                containers,
            )
            .await;
        });

        ns.pods.insert(
            pod_id,
            PodState {
                cancel: pod_cancel,
                supervisor,
            },
        );

        Ok(())
    }

    async fn handle_stop_pod(
        &mut self,
        namespace_id: &str,
        pod_id: &str,
        graceful: bool,
    ) -> Result<(), FatalError> {
        let ns = self.namespaces.get_mut(namespace_id).ok_or_else(|| {
            FatalError::InternalInvariant(format!("namespace '{}' not found", namespace_id))
        })?;

        if let Some(pod) = ns.pods.remove(pod_id) {
            if graceful {
                // Cancel the pod's token to trigger graceful shutdown in supervisor.
                pod.cancel.cancel();
                // Await supervisor with an outer timeout.
                match tokio::time::timeout(STOP_POD_TIMEOUT, pod.supervisor).await {
                    Ok(Ok(())) => {
                        log::info!(
                            "worker: gracefully stopped pod '{}' in namespace '{}'",
                            pod_id,
                            namespace_id
                        );
                    }
                    Ok(Err(join_error)) => {
                        log::error!(
                            "worker: pod '{}' supervisor panicked: {}",
                            pod_id,
                            join_error
                        );
                    }
                    Err(_) => {
                        log::warn!(
                            "worker: pod '{}' graceful stop timed out, aborting",
                            pod_id
                        );
                        // pod.supervisor (TaskHandle) drops here, automatically aborting.
                    }
                }
            } else {
                // Non-graceful: abort the supervisor immediately via drop.
                drop(pod.supervisor);
                log::info!(
                    "worker: forcibly stopped pod '{}' in namespace '{}'",
                    pod_id,
                    namespace_id
                );
            }
        }

        Ok(())
    }
}

/// Top-level pod supervisor: launches the pod and monitors it.
///
/// On launch failure, sends `PodFailed` and returns.
/// On success, sends `PodRunning` then delegates to `pod_monitor`.
async fn pod_supervisor<V: Vmm + 'static, P: ImageProvider + 'static>(
    vmm: Arc<V>,
    image_provider: Arc<P>,
    fabric: Arc<tokio::sync::Mutex<Fabric>>,
    kernel_path: PathBuf,
    rootfs_image_path: PathBuf,
    log_opener: LogStreamOpener,
    cancel: CancellationToken,
    event_tx: mpsc::Sender<WorkerEvent>,
    namespace_id: String,
    pod_id: String,
    network: PodNetworkConfig,
    containers: Vec<ContainerSpec>,
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
    )
    .await
    {
        Ok((vm, yamux_driver, io_session)) => {
            // Emit PodRunning event.
            if event_tx
                .send(WorkerEvent::PodRunning {
                    namespace_id: namespace_id.clone(),
                    pod_id: pod_id.clone(),
                })
                .await
                .is_err()
            {
                log::warn!(
                    "pod '{}': failed to send PodRunning, worker already shut down",
                    pod_id
                );
                return;
            }
            pod_monitor(vm, yamux_driver, io_session, cancel, event_tx, namespace_id, pod_id).await;
        }
        Err(e) => {
            log::error!("pod '{}': launch failed: {:#}", pod_id, e);
            let _ = event_tx
                .send(WorkerEvent::PodFailed {
                    namespace_id,
                    pod_id: pod_id.clone(),
                    error: format!("{:#}", e),
                })
                .await;
        }
    }
}

/// Perform all fallible pod setup: image prep, VM launch, vsock connect,
/// network config, container start, log stream setup.
async fn pod_launch<V: Vmm + 'static, P: ImageProvider + 'static>(
    vmm: &V,
    image_provider: &P,
    fabric: &tokio::sync::Mutex<Fabric>,
    kernel_path: &PathBuf,
    rootfs_image_path: &PathBuf,
    log_opener: &LogStreamOpener,
    event_tx: &mpsc::Sender<WorkerEvent>,
    namespace_id: &str,
    pod_id: &str,
    network: PodNetworkConfig,
    containers: Vec<ContainerSpec>,
) -> anyhow::Result<(
    ManagedVm<V::Instance>,
    TaskHandle<anyhow::Result<()>>,
    Option<(crate::io_session::IoSession, yamux::Stream)>,
)> {
    let container = containers
        .into_iter()
        .next()
        .context("pod must have at least one container")?;

    let artifact = image_provider
        .prepare(&container.image_ref)
        .await
        .context("preparing image")?;

    let capture_output = container.config.capture_output;
    let config = if let Some(ref oci_config) = artifact.oci_config {
        let overrides = ImageOverrides {
            entrypoint: if container.config.entrypoint.is_empty() {
                None
            } else {
                Some(container.config.entrypoint.clone())
            },
            args: container.config.args.clone(),
            env: container.config.env.clone(),
            working_dir: container.config.working_dir.clone(),
            uid: container.config.uid,
            gid: container.config.gid,
            hostname: container.config.hostname.clone(),
        };
        let mut cfg = merge_config(oci_config, &overrides)?;
        cfg.capture_output = capture_output;
        cfg
    } else {
        container.config
    };

    let net_config = NetConfig {
        guest_ip: network.ip.to_string(),
        netmask: network.netmask.clone(),
        gateway: network.gateway.to_string(),
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

    let mut instance = vmm.launch(&vm_config).await.context("launch VM")?;
    log::info!("worker: pod '{}' VM launched", pod_id);

    if let Some(tap) = instance.take_tap() {
        let tap_name = tap.name.clone();
        fabric
            .lock()
            .await
            .add_port(tap)
            .map_err(|e| anyhow::anyhow!("fabric add_port for {}: {}", tap_name, e))?;
        log::info!("worker: pod '{}' TAP {} added to fabric", pod_id, tap_name);
    }

    let (mut vm, yamux_driver) = ManagedVm::connect(instance).await?;

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
                    namespace_id: namespace_id.to_string(),
                    pod_id: pod_id.to_string(),
                    container_id: container_id.to_string(),
                };
                match log_opener.open_log_stream(&header).await {
                    Ok(log_stream) => Some((session, log_stream)),
                    Err(e) => {
                        log::error!("pod '{}': failed to open log stream: {:#}", pod_id, e);
                        let _ = event_tx
                            .send(WorkerEvent::PodLogStreamError {
                                namespace_id: namespace_id.to_string(),
                                pod_id: pod_id.to_string(),
                                container_id: container_id.to_string(),
                                phase: "open_stream".to_string(),
                                error: format!("{:#}", e),
                            })
                            .await;
                        None
                    }
                }
            }
            Err(e) => {
                log::error!("pod '{}': failed to accept output stream: {:#}", pod_id, e);
                let _ = event_tx
                    .send(WorkerEvent::PodLogStreamError {
                        namespace_id: namespace_id.to_string(),
                        pod_id: pod_id.to_string(),
                        container_id: container_id.to_string(),
                        phase: "connect".to_string(),
                        error: format!("{:#}", e),
                    })
                    .await;
                None
            }
        }
    } else {
        None
    };

    Ok((vm, yamux_driver, io_session))
}

/// Pod monitor: watches a running pod's sub-tasks and handles cleanup.
///
/// This owns the `ManagedVm` and coordinates between container exit,
/// yamux driver health, log streaming, and cancellation.
async fn pod_monitor<I: VmInstance>(
    mut vm: ManagedVm<I>,
    mut yamux_driver: TaskHandle<anyhow::Result<()>>,
    io_session: Option<(crate::io_session::IoSession, yamux::Stream)>,
    cancel: CancellationToken,
    event_tx: mpsc::Sender<WorkerEvent>,
    namespace_id: String,
    pod_id: String,
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

    let event = tokio::select! {
        // Normal path: container exits.
        result = vm.wait_container_exit() => {
            match result {
                Ok((_container_id, exit_code)) => {
                    // Gracefully shut down the VM.
                    match tokio::time::timeout(GRACEFUL_SHUTDOWN_TIMEOUT, vm.shutdown()).await {
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

        // Cancellation: graceful shutdown requested.
        _ = cancel.cancelled() => {
            log::info!("pod '{}': cancellation received, shutting down gracefully", pod_id);
            match tokio::time::timeout(GRACEFUL_SHUTDOWN_TIMEOUT, vm.shutdown()).await {
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
    if event_tx.send(event).await.is_err() {
        log::warn!("pod '{}': failed to send event, worker already shut down", pod_id);
    }
}
