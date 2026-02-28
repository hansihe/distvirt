use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::Context;
use distvirt_worker_protocol::{
    ContainerSpec, LogStreamHeader, LogStreamOpener, NetworkConfig, PodNetworkConfig,
    RegistryEntry, WorkerCommand, WorkerConnection, WorkerEvent,
};
use futures_lite::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::task_handle::TaskHandle;

use crate::fabric::{DnsRegistry, Fabric, FabricEvent, FabricGateway, RouteTable, ServiceTable};
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
    InternalInvariant(String),
}

impl fmt::Display for FatalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
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
///
/// If we add more namespace-level tasks beyond the gateway (e.g. health checks,
/// metrics collection), consider extracting a `NamespaceSupervisor` to formalize
/// the one-for-all supervision pattern instead of growing this struct.
struct NamespaceState {
    fabric: Arc<tokio::sync::Mutex<Fabric>>,
    route_table: Arc<std::sync::Mutex<RouteTable>>,
    service_table: Arc<std::sync::Mutex<ServiceTable>>,
    _gateway_task: TaskHandle<()>,
    _event_bridge_task: TaskHandle<()>,
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
    ///
    /// The 256-element buffer provides intentional backpressure: if the main
    /// loop falls behind, senders block rather than silently dropping events.
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
                    // Clean up completed pods from namespace state.
                    match &event {
                        WorkerEvent::PodExited { namespace_id, pod_id, .. }
                        | WorkerEvent::PodFailed { namespace_id, pod_id, .. } => {
                            self.remove_finished_pod(namespace_id, pod_id);
                        }
                        _ => {}
                    }
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
                network,
            } => self.handle_create_namespace(namespace_id, network).await,
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
            WorkerCommand::FabricRouteSync {
                namespace_id,
                routes,
            } => self.handle_fabric_route_sync(&namespace_id, routes),
            WorkerCommand::FabricRouteUpdate {
                namespace_id,
                added,
                removed_ips,
            } => self.handle_fabric_route_update(&namespace_id, added, removed_ips),
            WorkerCommand::CreateService {
                namespace_id,
                service_id,
                ip,
                mac,
                policy,
            } => self.handle_create_service(&namespace_id, service_id, ip, mac, policy),
            WorkerCommand::UpdateServiceBackend {
                namespace_id,
                service_id,
                backend,
            } => self.handle_update_service_backend(&namespace_id, &service_id, backend),
            WorkerCommand::ServiceReady {
                namespace_id,
                service_id,
            } => self.handle_service_ready(&namespace_id, &service_id).await,
            WorkerCommand::DestroyService {
                namespace_id,
                service_id,
            } => self.handle_destroy_service(&namespace_id, &service_id),
            WorkerCommand::Shutdown => {
                // Handled in the main loop; should not reach here.
                unreachable!("Shutdown handled in run()")
            }
        }
    }

    async fn handle_create_namespace(
        &mut self,
        namespace_id: String,
        network: NetworkConfig,
    ) -> Result<(), FatalError> {
        let mut fabric = Fabric::new();

        let registry: DnsRegistry = Arc::new(RwLock::new(HashMap::new()));

        // Set up fabric event channel for route miss reporting.
        let (fabric_event_tx, mut fabric_event_rx) = mpsc::channel::<FabricEvent>(64);
        fabric.set_event_channel(fabric_event_tx);

        let route_table = fabric.route_table();
        let service_table = fabric.service_table();

        let pod_gateway_ip = network.gateway.octets();
        let (gateway, egress_tx, ingress_rx) =
            FabricGateway::new(Arc::clone(&registry), pod_gateway_ip, network.prefix_len)
                .map_err(|e| {
                    FatalError::InternalInvariant(format!("create fabric gateway: {:#}", e))
                })?;
        fabric.set_gateway(egress_tx, ingress_rx);

        let ns_token = self.worker_token.child_token();
        let ns_cancel = ns_token.clone();
        let gateway_ns_id = namespace_id.clone();

        let gateway_event_tx = self.bg_event_tx.clone();
        let gateway_task = TaskHandle::spawn(async move {
            gateway.run().await;
            log::error!(
                "namespace '{}': gateway exited, cancelling all pods",
                gateway_ns_id
            );
            send_event(
                &gateway_event_tx,
                WorkerEvent::NamespaceFailed {
                    namespace_id: gateway_ns_id.clone(),
                    error: "gateway exited unexpectedly".to_string(),
                },
            )
            .await;
            ns_cancel.cancel();
        });

        // Bridge task: map FabricEvents to WorkerEvents.
        let bridge_event_tx = self.bg_event_tx.clone();
        let bridge_ns_id = namespace_id.clone();
        let event_bridge_task = TaskHandle::spawn(async move {
            while let Some(event) = fabric_event_rx.recv().await {
                match event {
                    FabricEvent::RouteMiss { dst_ip, dst_mac } => {
                        let _ = bridge_event_tx
                            .try_send(WorkerEvent::FabricRouteMiss {
                                namespace_id: bridge_ns_id.clone(),
                                dst_ip,
                                dst_mac,
                            });
                    }
                    FabricEvent::ServiceActivation { service_id, dst_ip } => {
                        let _ = bridge_event_tx
                            .try_send(WorkerEvent::ServiceActivation {
                                namespace_id: bridge_ns_id.clone(),
                                service_id,
                                dst_ip,
                            });
                    }
                }
            }
        });

        log::info!(
            "worker: created namespace '{}' with fabric + gateway",
            namespace_id
        );

        let ns = NamespaceState {
            fabric: Arc::new(tokio::sync::Mutex::new(fabric)),
            route_table,
            service_table,
            _gateway_task: gateway_task,
            _event_bridge_task: event_bridge_task,
            registry,
            pods: HashMap::new(),
            token: ns_token,
        };

        self.namespaces.insert(namespace_id.clone(), ns);

        // Send via background event channel — handlers never touch conn directly.
        send_event(
            &self.bg_event_tx,
            WorkerEvent::NamespaceCreated { namespace_id },
        )
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

    fn handle_fabric_route_sync(
        &mut self,
        namespace_id: &str,
        routes: Vec<distvirt_worker_protocol::FabricRouteEntry>,
    ) -> Result<(), FatalError> {
        let ns = self.namespaces.get_mut(namespace_id).ok_or_else(|| {
            FatalError::InternalInvariant(format!("namespace '{}' not found", namespace_id))
        })?;

        let mut rt = ns.route_table.lock().map_err(|e| {
            FatalError::InternalInvariant(format!("route table lock poisoned: {}", e))
        })?;
        rt.sync(routes);

        log::info!(
            "worker: synced fabric routes for namespace '{}'",
            namespace_id
        );
        Ok(())
    }

    fn handle_fabric_route_update(
        &mut self,
        namespace_id: &str,
        added: Vec<distvirt_worker_protocol::FabricRouteEntry>,
        removed_ips: Vec<std::net::Ipv4Addr>,
    ) -> Result<(), FatalError> {
        let ns = self.namespaces.get_mut(namespace_id).ok_or_else(|| {
            FatalError::InternalInvariant(format!("namespace '{}' not found", namespace_id))
        })?;

        let mut rt = ns.route_table.lock().map_err(|e| {
            FatalError::InternalInvariant(format!("route table lock poisoned: {}", e))
        })?;
        rt.update(added, removed_ips);

        log::info!(
            "worker: updated fabric routes for namespace '{}'",
            namespace_id
        );
        Ok(())
    }

    fn handle_create_service(
        &mut self,
        namespace_id: &str,
        service_id: String,
        ip: std::net::Ipv4Addr,
        mac: [u8; 6],
        policy: distvirt_worker_protocol::ServicePolicy,
    ) -> Result<(), FatalError> {
        let ns = self.namespaces.get_mut(namespace_id).ok_or_else(|| {
            FatalError::InternalInvariant(format!("namespace '{}' not found", namespace_id))
        })?;

        let mut st = ns.service_table.lock().map_err(|e| {
            FatalError::InternalInvariant(format!("service table lock poisoned: {}", e))
        })?;
        st.create(service_id.clone(), ip, mac, policy);

        log::info!(
            "worker: created service '{}' with ip {} in namespace '{}'",
            service_id, ip, namespace_id
        );
        Ok(())
    }

    fn handle_update_service_backend(
        &mut self,
        namespace_id: &str,
        service_id: &str,
        backend: Option<distvirt_worker_protocol::ServiceBackend>,
    ) -> Result<(), FatalError> {
        let ns = self.namespaces.get_mut(namespace_id).ok_or_else(|| {
            FatalError::InternalInvariant(format!("namespace '{}' not found", namespace_id))
        })?;

        let mut st = ns.service_table.lock().map_err(|e| {
            FatalError::InternalInvariant(format!("service table lock poisoned: {}", e))
        })?;
        let backend_tuple = backend.map(|b| (b.pod_ip, b.pod_mac));
        st.update_backend(service_id, backend_tuple);

        log::info!(
            "worker: updated service backend '{}' in namespace '{}'",
            service_id, namespace_id
        );
        Ok(())
    }

    async fn handle_service_ready(
        &mut self,
        namespace_id: &str,
        service_id: &str,
    ) -> Result<(), FatalError> {
        let ns = self.namespaces.get_mut(namespace_id).ok_or_else(|| {
            FatalError::InternalInvariant(format!("namespace '{}' not found", namespace_id))
        })?;

        let flush_data = {
            let mut st = ns.service_table.lock().map_err(|e| {
                FatalError::InternalInvariant(format!("service table lock poisoned: {}", e))
            })?;
            st.mark_ready(service_id)
        };

        if let Some((frames, backend_mac)) = flush_data {
            if !frames.is_empty() {
                let fabric = ns.fabric.lock().await;
                fabric.flush_service_frames(frames, backend_mac);
            }
        }

        log::info!(
            "worker: service '{}' marked ready in namespace '{}'",
            service_id, namespace_id
        );
        Ok(())
    }

    fn handle_destroy_service(
        &mut self,
        namespace_id: &str,
        service_id: &str,
    ) -> Result<(), FatalError> {
        let ns = self.namespaces.get_mut(namespace_id).ok_or_else(|| {
            FatalError::InternalInvariant(format!("namespace '{}' not found", namespace_id))
        })?;

        let mut st = ns.service_table.lock().map_err(|e| {
            FatalError::InternalInvariant(format!("service table lock poisoned: {}", e))
        })?;
        st.destroy(service_id);

        log::info!(
            "worker: destroyed service '{}' in namespace '{}'",
            service_id, namespace_id
        );
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

    /// Remove a finished pod from its namespace. The TaskHandle is dropped
    /// (harmless since the supervisor already exited).
    fn remove_finished_pod(&mut self, namespace_id: &str, pod_id: &str) {
        if let Some(ns) = self.namespaces.get_mut(namespace_id) {
            ns.pods.remove(pod_id);
        }
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

/// Send an event to the worker main loop, or log and return if the worker is shutting down.
async fn send_event(tx: &mpsc::Sender<WorkerEvent>, event: WorkerEvent) {
    if tx.send(event).await.is_err() {
        log::warn!("failed to send event, worker already shut down");
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
            pod_monitor(vm, yamux_driver, io_session, port_task, cancel, event_tx, namespace_id, pod_id).await;
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
            .lock()
            .await
            .add_port_with_ip(tap, network.ip)
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

    let dns_servers = vec![crate::fabric::GATEWAY_IP_STR.to_string()];

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
                        send_event(
                            event_tx,
                            WorkerEvent::PodLogStreamError {
                                namespace_id: namespace_id.to_string(),
                                pod_id: pod_id.to_string(),
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
                        namespace_id: namespace_id.to_string(),
                        pod_id: pod_id.to_string(),
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

/// Pod monitor: watches a running pod's sub-tasks and handles cleanup.
///
/// This owns the `ManagedVm` and coordinates between container exit,
/// yamux driver health, log streaming, and cancellation.
async fn pod_monitor<I: VmInstance>(
    mut vm: ManagedVm<I>,
    mut yamux_driver: TaskHandle<anyhow::Result<()>>,
    io_session: Option<(crate::io_session::IoSession, yamux::Stream)>,
    port_task: Option<TaskHandle<()>>,
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
    send_event(&event_tx, event).await;
}
