use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::Context;
use distvirt_activator::{ActivatorInstance, ActivatorRuntime, StreamManager, StreamManagerConfig};
use distvirt_worker_protocol::{
    ActivatorConfig, ContainerSpec, LogStreamHeader, LogStreamOpener, NamespaceId, NetworkConfig,
    PodId, PodNetworkConfig, RegistryEntry, ServiceId, WorkerCapabilities, WorkerCommand,
    WorkerConnection, WorkerEvent, WorkerHello,
};
use futures_lite::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::task_handle::TaskHandle;

use crate::adapter::{AdapterManager, AdapterPortHandle};
use crate::fabric::{Fabric, FabricContextInner, FabricEvent, FabricPort};
use crate::gateway::{DnsRegistry, FabricGateway};
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
    fabric: Arc<tokio::sync::Mutex<Fabric<FabricPort>>>,
    tables: Arc<FabricContextInner<FabricPort>>,
    _gateway_task: TaskHandle<()>,
    _event_bridge_task: TaskHandle<()>,
    _adapter_tasks: Vec<TaskHandle<()>>,
    _adapter_ports: Vec<AdapterPortHandle>,
    registry: DnsRegistry,
    pods: HashMap<PodId, PodState>,
    token: CancellationToken,
}

/// The worker: sits between the orchestrator and the raw VM/fabric primitives.
///
/// Receives `WorkerCommand`s via a `WorkerConnection`, sends `WorkerEvent`s back,
/// and opens yamux log streams for container output.
pub struct Worker<V: Vmm + 'static, P: ImageProvider + 'static> {
    kernel_path: PathBuf,
    rootfs_image_path: PathBuf,
    namespaces: HashMap<NamespaceId, NamespaceState>,
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
    /// Activator WASM runtime (None if no component directory or loading failed).
    activator_runtime: Option<ActivatorRuntime>,
    /// Ingress adapter manager, initialized from handshake config.
    adapter_manager: AdapterManager,
    /// Public endpoint where this worker is reachable by other workers.
    public_endpoint: String,
}

impl<V: Vmm + 'static, P: ImageProvider + 'static> Worker<V, P> {
    pub fn new(
        kernel_path: PathBuf,
        rootfs_image_path: PathBuf,
        vmm: V,
        image_provider: P,
        component_dir: Option<PathBuf>,
        public_endpoint: String,
    ) -> Self {
        let (bg_event_tx, bg_event_rx) = mpsc::channel(256);
        let activator_runtime = component_dir.and_then(|dir| {
            match ActivatorRuntime::new(&dir) {
                Ok(rt) => Some(rt),
                Err(e) => {
                    log::warn!("activator runtime init failed: {:#}, activators disabled", e);
                    None
                }
            }
        });
        Worker {
            kernel_path,
            rootfs_image_path,
            namespaces: HashMap::new(),
            vmm: Arc::new(vmm),
            image_provider: Arc::new(image_provider),
            bg_event_tx,
            bg_event_rx,
            worker_token: CancellationToken::new(),
            activator_runtime,
            adapter_manager: AdapterManager::empty(),
            public_endpoint,
        }
    }

    /// Run the worker main loop: receive commands, dispatch them,
    /// and forward background events to the orchestrator.
    /// Detect local capabilities for the WorkerHello handshake.
    fn detect_capabilities(&self) -> WorkerCapabilities {
        let has_kvm = std::path::Path::new("/dev/kvm").exists();
        let containerd_socket = std::env::var("CONTAINERD_SOCKET")
            .unwrap_or_else(|_| "/run/containerd/containerd.sock".into());
        let has_containerd = std::path::Path::new(&containerd_socket).exists();
        WorkerCapabilities {
            has_kvm,
            has_containerd,
            available_adapters: vec!["wireguard".to_string()],
            max_pods: 10,
            available_memory_mb: 1024,
            public_endpoint: self.public_endpoint.clone(),
        }
    }

    pub async fn run(mut self, mut conn: WorkerConnection) -> anyhow::Result<()> {
        // --- Handshake ---
        let capabilities = self.detect_capabilities();
        log::info!("worker: capabilities: {:?}", capabilities);

        conn.send_hello(&WorkerHello {
            auth_token: String::new(),
            capabilities,
        })
        .await
        .context("handshake: send WorkerHello")?;

        let accepted = conn
            .recv_accepted()
            .await
            .context("handshake: recv WorkerAccepted")?;
        log::info!("worker: accepted as worker_id={}", accepted.worker_id);
        self.adapter_manager = AdapterManager::new(&accepted.adapters).await;

        conn.send_ready()
            .await
            .context("handshake: send WorkerReady")?;
        log::info!("worker: handshake complete, entering command loop");

        // --- Command loop ---
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
            WorkerCommand::RegistryUpdate {
                namespace_id,
                added,
                removed,
            } => self.handle_registry_update(&namespace_id, added, removed),
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
            } => self.handle_create_service(&namespace_id, &service_id, ip, mac, policy),
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
            WorkerCommand::AddWireGuardPeer {
                namespace_id,
                peer_public_key,
                peer_ip,
                preshared_key,
            } => {
                if let Some(wg) = self.adapter_manager.wireguard() {
                    if let Err(e) = wg
                        .add_peer(namespace_id.as_ref(), peer_public_key, peer_ip, preshared_key)
                        .await
                    {
                        log::error!("wireguard: add_peer failed: {:#}", e);
                    }
                } else {
                    log::warn!("wireguard: AddWireGuardPeer command but no WireGuard adapter configured");
                }
                Ok(())
            }
            WorkerCommand::RemoveWireGuardPeer { peer_public_key } => {
                if let Some(wg) = self.adapter_manager.wireguard() {
                    if let Err(e) = wg.remove_peer(&peer_public_key).await {
                        log::error!("wireguard: remove_peer failed: {:#}", e);
                    }
                } else {
                    log::warn!("wireguard: RemoveWireGuardPeer command but no WireGuard adapter configured");
                }
                Ok(())
            }
            WorkerCommand::Shutdown => {
                // Handled in the main loop; should not reach here.
                unreachable!("Shutdown handled in run()")
            }
        }
    }

    async fn handle_create_namespace(
        &mut self,
        namespace_id: NamespaceId,
        network: NetworkConfig,
    ) -> Result<(), FatalError> {
        let mut fabric = Fabric::<FabricPort>::new();

        let registry: DnsRegistry = Arc::new(RwLock::new(HashMap::new()));

        // Set up fabric event channel for route miss reporting.
        let (fabric_event_tx, mut fabric_event_rx) = mpsc::channel::<FabricEvent>(64);
        fabric.set_event_channel(fabric_event_tx);

        let tables = fabric.tables();

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
                        if let Err(e) = bridge_event_tx
                            .try_send(WorkerEvent::FabricRouteMiss {
                                namespace_id: bridge_ns_id.clone(),
                                dst_ip,
                                dst_mac,
                            })
                        {
                            log::warn!("worker: dropped FabricRouteMiss event: {}", e);
                        }
                    }
                    FabricEvent::ServiceActivation { service_id, dst_ip } => {
                        if let Err(e) = bridge_event_tx
                            .try_send(WorkerEvent::ServiceActivation {
                                namespace_id: bridge_ns_id.clone(),
                                service_id: ServiceId::from(service_id),
                                dst_ip,
                            })
                        {
                            log::warn!("worker: dropped ServiceActivation event: {}", e);
                        }
                    }
                    FabricEvent::ServiceBackendNeed { service_id, dst_ip: _, need } => {
                        if let Err(e) = bridge_event_tx
                            .send(WorkerEvent::ServiceBackendNeed {
                                namespace_id: bridge_ns_id.clone(),
                                service_id: ServiceId::from(service_id),
                                need,
                            }).await
                        {
                            log::warn!("worker: failed to send ServiceBackendNeed event: {}", e);
                        }
                    }
                }
            }
        });

        // Create adapter virtual ports and plug them into the fabric.
        let adapter_ports_result = self
            .adapter_manager
            .create_namespace_ports(namespace_id.as_ref());
        let mut adapter_handles = Vec::new();
        let mut adapter_tasks = Vec::new();
        for (channel_port, handle) in adapter_ports_result {
            let (_port_id, task) = fabric.add_port_raw(FabricPort::Virtual(channel_port));
            adapter_handles.push(handle);
            adapter_tasks.push(task);
        }

        log::info!(
            "worker: created namespace '{}' with fabric + gateway",
            namespace_id
        );

        let ns = NamespaceState {
            fabric: Arc::new(tokio::sync::Mutex::new(fabric)),
            tables,
            _gateway_task: gateway_task,
            _event_bridge_task: event_bridge_task,
            _adapter_tasks: adapter_tasks,
            _adapter_ports: adapter_handles,
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

    async fn handle_destroy_namespace(&mut self, namespace_id: &NamespaceId) -> Result<(), FatalError> {
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

            send_event(
                &self.bg_event_tx,
                WorkerEvent::NamespaceDestroyed {
                    namespace_id: namespace_id.clone(),
                },
            )
            .await;
        }
        Ok(())
    }

    fn handle_registry_sync(
        &mut self,
        namespace_id: &NamespaceId,
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

    fn handle_registry_update(
        &mut self,
        namespace_id: &NamespaceId,
        added: Vec<RegistryEntry>,
        removed: Vec<String>,
    ) -> Result<(), FatalError> {
        let ns = self.namespaces.get_mut(namespace_id).ok_or_else(|| {
            FatalError::InternalInvariant(format!("namespace '{}' not found", namespace_id))
        })?;

        let mut map = ns.registry.write().map_err(|e| {
            FatalError::InternalInvariant(format!("registry lock poisoned: {}", e))
        })?;
        for name in &removed {
            map.remove(name);
        }
        for entry in added {
            map.insert(entry.name, entry.ip);
        }

        log::info!(
            "worker: updated registry for namespace '{}' ({} removed)",
            namespace_id,
            removed.len()
        );
        Ok(())
    }

    fn handle_fabric_route_sync(
        &mut self,
        namespace_id: &NamespaceId,
        routes: Vec<distvirt_worker_protocol::FabricRouteEntry>,
    ) -> Result<(), FatalError> {
        let ns = self.namespaces.get_mut(namespace_id).ok_or_else(|| {
            FatalError::InternalInvariant(format!("namespace '{}' not found", namespace_id))
        })?;

        let mut rt = ns.tables.route_table.lock().map_err(|e| {
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
        namespace_id: &NamespaceId,
        added: Vec<distvirt_worker_protocol::FabricRouteEntry>,
        removed_ips: Vec<std::net::Ipv4Addr>,
    ) -> Result<(), FatalError> {
        let ns = self.namespaces.get_mut(namespace_id).ok_or_else(|| {
            FatalError::InternalInvariant(format!("namespace '{}' not found", namespace_id))
        })?;

        let mut rt = ns.tables.route_table.lock().map_err(|e| {
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
        namespace_id: &NamespaceId,
        service_id: &ServiceId,
        ip: std::net::Ipv4Addr,
        mac: [u8; 6],
        policy: distvirt_worker_protocol::ServicePolicy,
    ) -> Result<(), FatalError> {
        let ns = self.namespaces.get_mut(namespace_id).ok_or_else(|| {
            FatalError::InternalInvariant(format!("namespace '{}' not found", namespace_id))
        })?;

        let activator = if policy.activator.is_some() {
            if let Some(ref runtime) = self.activator_runtime {
                let component_name = match &policy.activator {
                    Some(ActivatorConfig::Tcp { .. }) => "tcp",
                    Some(ActivatorConfig::Http2 { .. }) => "http2",
                    None => unreachable!(),
                };
                match runtime.get_component(component_name) {
                    Some(component) => {
                        match ActivatorInstance::new(runtime.engine(), component) {
                            Ok(instance) => Some(instance),
                            Err(e) => {
                                log::error!("failed to instantiate activator: {:#}", e);
                                None
                            }
                        }
                    }
                    None => {
                        log::warn!("activator component '{}' not found", component_name);
                        None
                    }
                }
            } else {
                log::warn!("activator requested but runtime not available");
                None
            }
        } else {
            None
        };

        // Create StreamManager for L4 mode (Http2 activator).
        let stream_manager = match &policy.activator {
            Some(ActivatorConfig::Http2 { .. }) => {
                Some(StreamManager::new(StreamManagerConfig {
                    service_ip: ip,
                    service_mac: mac,
                    listen_ports: vec![80],
                    ..StreamManagerConfig::default()
                }))
            }
            _ => None,
        };

        let mut st = ns.tables.service_table.lock().map_err(|e| {
            FatalError::InternalInvariant(format!("service table lock poisoned: {}", e))
        })?;
        st.create(service_id.0.clone(), ip, mac, policy, activator, stream_manager);

        log::info!(
            "worker: created service '{}' with ip {} in namespace '{}'",
            service_id, ip, namespace_id
        );
        Ok(())
    }

    fn handle_update_service_backend(
        &mut self,
        namespace_id: &NamespaceId,
        service_id: &ServiceId,
        backend: Option<distvirt_worker_protocol::ServiceBackend>,
    ) -> Result<(), FatalError> {
        let ns = self.namespaces.get_mut(namespace_id).ok_or_else(|| {
            FatalError::InternalInvariant(format!("namespace '{}' not found", namespace_id))
        })?;

        let mut st = ns.tables.service_table.lock().map_err(|e| {
            FatalError::InternalInvariant(format!("service table lock poisoned: {}", e))
        })?;
        let backend_tuple = backend.map(|b| (b.pod_ip, b.pod_mac));
        st.update_backend(service_id.as_ref(), backend_tuple);

        log::info!(
            "worker: updated service backend '{}' in namespace '{}'",
            service_id, namespace_id
        );
        Ok(())
    }

    async fn handle_service_ready(
        &mut self,
        namespace_id: &NamespaceId,
        service_id: &ServiceId,
    ) -> Result<(), FatalError> {
        let ns = self.namespaces.get_mut(namespace_id).ok_or_else(|| {
            FatalError::InternalInvariant(format!("namespace '{}' not found", namespace_id))
        })?;

        let flush_data = {
            let mut st = ns.tables.service_table.lock().map_err(|e| {
                FatalError::InternalInvariant(format!("service table lock poisoned: {}", e))
            })?;
            st.mark_ready(service_id.as_ref())
        };

        if let Some(result) = flush_data {
            use crate::fabric::service::{MarkReadyResult, ServiceAction};
            match &result {
                MarkReadyResult::Passthrough { frames, actions, backend_mac, .. } => {
                    log::info!(
                        "worker: service '{}' mark_ready returned Passthrough: {} buffered frames, {} actions, backend_mac={:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
                        service_id, frames.len(), actions.len(),
                        backend_mac[0], backend_mac[1], backend_mac[2],
                        backend_mac[3], backend_mac[4], backend_mac[5],
                    );
                }
                MarkReadyResult::L4(action) => {
                    log::info!(
                        "worker: service '{}' mark_ready returned L4: {:?}",
                        service_id, std::mem::discriminant(action)
                    );
                }
            }
            let fabric = ns.fabric.lock().await;
            match result {
                MarkReadyResult::Passthrough { frames, backend_mac, backend_ip, service_ip, service_mac, actions } => {
                    if !frames.is_empty() {
                        fabric.flush_service_frames(frames, backend_mac, backend_ip, service_ip, service_mac);
                    }
                    fabric.dispatch_actions(&actions, service_id.as_ref()).await;
                }
                MarkReadyResult::L4(ServiceAction::L4Result { actions, frames, .. }) => {
                    fabric.send_l4_frames(frames);
                    fabric.dispatch_actions(&actions, service_id.as_ref()).await;
                }
                _ => {}
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
        namespace_id: &NamespaceId,
        service_id: &ServiceId,
    ) -> Result<(), FatalError> {
        let ns = self.namespaces.get_mut(namespace_id).ok_or_else(|| {
            FatalError::InternalInvariant(format!("namespace '{}' not found", namespace_id))
        })?;

        let mut st = ns.tables.service_table.lock().map_err(|e| {
            FatalError::InternalInvariant(format!("service table lock poisoned: {}", e))
        })?;
        st.destroy(service_id.as_ref());

        log::info!(
            "worker: destroyed service '{}' in namespace '{}'",
            service_id, namespace_id
        );
        Ok(())
    }

    async fn handle_launch_pod(
        &mut self,
        namespace_id: &NamespaceId,
        pod_id: PodId,
        network: PodNetworkConfig,
        containers: Vec<ContainerSpec>,
        log_opener: &LogStreamOpener,
    ) -> Result<(), FatalError> {
        let ns = self.namespaces.get_mut(namespace_id).ok_or_else(|| {
            FatalError::InternalInvariant(format!("namespace '{}' not found", namespace_id))
        })?;

        // Register the pod's IP→MAC mapping with the WireGuard adapter so it
        // can resolve destination MACs when injecting frames into the fabric.
        if let Some(wg) = self.adapter_manager.wireguard() {
            wg.register_pod_mac(namespace_id.as_ref(), network.ip, network.mac).await;
        }

        let pod_cancel = ns.token.child_token();
        let event_tx = self.bg_event_tx.clone();
        let vmm = Arc::clone(&self.vmm);
        let image_provider = Arc::clone(&self.image_provider);
        let fabric = Arc::clone(&ns.fabric);
        let kernel_path = self.kernel_path.clone();
        let rootfs_image_path = self.rootfs_image_path.clone();
        let log_opener = log_opener.clone();
        let ns_id = namespace_id.clone();
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
    fn remove_finished_pod(&mut self, namespace_id: &NamespaceId, pod_id: &PodId) {
        if let Some(ns) = self.namespaces.get_mut(namespace_id) {
            ns.pods.remove(pod_id);
        }
    }

    async fn handle_stop_pod(
        &mut self,
        namespace_id: &NamespaceId,
        pod_id: &PodId,
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
    fabric: Arc<tokio::sync::Mutex<Fabric<FabricPort>>>,
    kernel_path: PathBuf,
    rootfs_image_path: PathBuf,
    log_opener: LogStreamOpener,
    cancel: CancellationToken,
    event_tx: mpsc::Sender<WorkerEvent>,
    namespace_id: NamespaceId,
    pod_id: PodId,
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
    fabric: &tokio::sync::Mutex<Fabric<FabricPort>>,
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
            .lock()
            .await
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
    namespace_id: NamespaceId,
    pod_id: PodId,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    use distvirt_worker_protocol::{
        BufferPolicy, ContainerConfig, ContainerSpec, FabricRouteEntry, PodNetworkConfig,
        RegistryEntry, RouteDestination, ServiceBackend, ServicePolicy,
    };
    use tokio::net::UnixStream;

    use crate::fabric::{Fabric, FabricPort};
    use crate::image_provider::{ImageProvider, PreparedArtifact};
    use crate::tap::TapDevice;
    use crate::vmm::{VmConfig, VmInstance, Vmm};

    // -----------------------------------------------------------------------
    // Stubs (panic if called — for tests that don't launch pods)
    // -----------------------------------------------------------------------

    struct StubVmm;

    impl Vmm for StubVmm {
        type Instance = StubVmInstance;
        async fn launch(&self, _config: &VmConfig) -> anyhow::Result<StubVmInstance> {
            panic!("StubVmm::launch should not be called in state management tests");
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

    struct StubImageProvider;

    impl ImageProvider for StubImageProvider {
        async fn prepare(&self, _image_ref: &str) -> anyhow::Result<PreparedArtifact> {
            panic!("StubImageProvider::prepare should not be called in state management tests");
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn make_worker() -> Worker<StubVmm, StubImageProvider> {
        Worker::new(
            PathBuf::from("/fake/kernel"),
            PathBuf::from("/fake/rootfs"),
            StubVmm,
            StubImageProvider,
            None, // no activator component dir
            String::new(),
        )
    }

    /// Inject a NamespaceState directly into the worker, bypassing
    /// handle_create_namespace (which requires root for TUN/gateway).
    fn inject_namespace(worker: &mut Worker<StubVmm, StubImageProvider>, ns_id: &str) {
        let fabric = Fabric::<FabricPort>::new();
        let tables = fabric.tables();

        // Fabric event channel — receiver intentionally dropped (events go nowhere).
        let (fabric_event_tx, _rx) = mpsc::channel::<FabricEvent>(64);
        // We don't call set_event_channel since we don't need events in these tests.
        let _ = fabric_event_tx;

        let registry: DnsRegistry = Arc::new(RwLock::new(HashMap::new()));

        let ns_token = worker.worker_token.child_token();

        // Dummy task handles that pend forever.
        let gateway_task = TaskHandle::spawn(std::future::pending::<()>());
        let event_bridge_task = TaskHandle::spawn(std::future::pending::<()>());

        let ns = NamespaceState {
            fabric: Arc::new(tokio::sync::Mutex::new(fabric)),
            tables,
            _gateway_task: gateway_task,
            _event_bridge_task: event_bridge_task,
            _adapter_tasks: Vec::new(),
            _adapter_ports: Vec::new(),
            registry,
            pods: HashMap::new(),
            token: ns_token,
        };

        worker
            .namespaces
            .insert(NamespaceId::from(ns_id), ns);
    }

    fn make_log_opener() -> LogStreamOpener {
        LogStreamOpener::disconnected()
    }

    // -----------------------------------------------------------------------
    // Registry tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn registry_sync_populates_entries() {
        let mut w = make_worker();
        inject_namespace(&mut w, "ns1");

        let entries = vec![
            RegistryEntry {
                name: "api".to_string(),
                ip: Ipv4Addr::new(172, 16, 0, 10),
            },
            RegistryEntry {
                name: "db".to_string(),
                ip: Ipv4Addr::new(172, 16, 0, 11),
            },
        ];

        w.handle_registry_sync(&NamespaceId::from("ns1"), entries)
            .unwrap();

        let ns = w.namespaces.get(&NamespaceId::from("ns1")).unwrap();
        let map = ns.registry.read().unwrap();
        assert_eq!(map.len(), 2);
        assert_eq!(map.get("api"), Some(&Ipv4Addr::new(172, 16, 0, 10)));
        assert_eq!(map.get("db"), Some(&Ipv4Addr::new(172, 16, 0, 11)));
    }

    #[tokio::test]
    async fn registry_sync_replaces_on_resync() {
        let mut w = make_worker();
        inject_namespace(&mut w, "ns1");

        let entries1 = vec![RegistryEntry {
            name: "api".to_string(),
            ip: Ipv4Addr::new(172, 16, 0, 10),
        }];
        w.handle_registry_sync(&NamespaceId::from("ns1"), entries1)
            .unwrap();

        // Re-sync with different entries.
        let entries2 = vec![RegistryEntry {
            name: "web".to_string(),
            ip: Ipv4Addr::new(172, 16, 0, 20),
        }];
        w.handle_registry_sync(&NamespaceId::from("ns1"), entries2)
            .unwrap();

        let ns = w.namespaces.get(&NamespaceId::from("ns1")).unwrap();
        let map = ns.registry.read().unwrap();
        assert_eq!(map.len(), 1);
        assert!(map.get("api").is_none());
        assert_eq!(map.get("web"), Some(&Ipv4Addr::new(172, 16, 0, 20)));
    }

    #[tokio::test]
    async fn registry_sync_errors_on_missing_namespace() {
        let mut w = make_worker();
        let result = w.handle_registry_sync(&NamespaceId::from("nonexistent"), vec![]);
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // Fabric route tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn fabric_route_sync_populates_routes() {
        let mut w = make_worker();
        inject_namespace(&mut w, "ns1");

        let routes = vec![FabricRouteEntry {
            ip: Ipv4Addr::new(172, 16, 0, 10),
            mac: [0x02, 0, 0, 0, 0, 0x10],
            destination: RouteDestination::Placeholder {
                buffer_policy: BufferPolicy {
                    buffer_frames: 10,
                    timeout_ms: 5000,
                },
            },
        }];

        w.handle_fabric_route_sync(&NamespaceId::from("ns1"), routes)
            .unwrap();

        // Verify via lookup_and_buffer (requires &mut).
        let ns = w.namespaces.get(&NamespaceId::from("ns1")).unwrap();
        let mut rt = ns.tables.route_table.lock().unwrap();
        let (action, _) = rt.lookup_and_buffer(Ipv4Addr::new(172, 16, 0, 10), &[0xDE, 0xAD]);
        assert!(
            !matches!(action, crate::fabric::route::RouteAction::NoRoute),
            "expected a route entry, got NoRoute"
        );
    }

    #[tokio::test]
    async fn fabric_route_sync_errors_on_missing_namespace() {
        let mut w = make_worker();
        let result = w.handle_fabric_route_sync(&NamespaceId::from("nope"), vec![]);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn fabric_route_update_adds_and_removes() {
        let mut w = make_worker();
        inject_namespace(&mut w, "ns1");
        let ns_id = NamespaceId::from("ns1");

        // Sync initial route.
        let routes = vec![FabricRouteEntry {
            ip: Ipv4Addr::new(172, 16, 0, 10),
            mac: [0x02, 0, 0, 0, 0, 0x10],
            destination: RouteDestination::Placeholder {
                buffer_policy: BufferPolicy {
                    buffer_frames: 5,
                    timeout_ms: 5000,
                },
            },
        }];
        w.handle_fabric_route_sync(&ns_id, routes).unwrap();

        // Update: add a new route, remove the old one.
        let added = vec![FabricRouteEntry {
            ip: Ipv4Addr::new(172, 16, 0, 20),
            mac: [0x02, 0, 0, 0, 0, 0x20],
            destination: RouteDestination::RemoteWorker {
                worker_id: "w2".to_string().into(),
            },
        }];
        w.handle_fabric_route_update(
            &ns_id,
            added,
            vec![Ipv4Addr::new(172, 16, 0, 10)],
        )
        .unwrap();
    }

    #[tokio::test]
    async fn fabric_route_update_errors_on_missing_namespace() {
        let mut w = make_worker();
        let result =
            w.handle_fabric_route_update(&NamespaceId::from("nope"), vec![], vec![]);
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // Service lifecycle tests
    // -----------------------------------------------------------------------

    fn default_policy() -> ServicePolicy {
        ServicePolicy {
            buffer_frames: 10,
            timeout_ms: 5000,
            activator: None,
        }
    }

    #[tokio::test]
    async fn service_create_and_destroy() {
        let mut w = make_worker();
        inject_namespace(&mut w, "ns1");
        let ns_id = NamespaceId::from("ns1");
        let svc_id = ServiceId::from("svc1");

        w.handle_create_service(
            &ns_id,
            &svc_id,
            Ipv4Addr::new(172, 16, 0, 100),
            [0x02, 0, 0, 0, 0, 0xAA],
            default_policy(),
        )
        .unwrap();

        // Verify service exists by checking the service table.
        {
            let ns = w.namespaces.get(&ns_id).unwrap();
            let st = ns.tables.service_table.lock().unwrap();
            assert_eq!(
                st.get_ip_by_id("svc1"),
                Some(Ipv4Addr::new(172, 16, 0, 100))
            );
        }

        // Destroy.
        w.handle_destroy_service(&ns_id, &svc_id).unwrap();

        {
            let ns = w.namespaces.get(&ns_id).unwrap();
            let st = ns.tables.service_table.lock().unwrap();
            assert_eq!(st.get_ip_by_id("svc1"), None);
        }
    }

    #[tokio::test]
    async fn service_create_errors_on_missing_namespace() {
        let mut w = make_worker();
        let result = w.handle_create_service(
            &NamespaceId::from("nope"),
            &ServiceId::from("svc1"),
            Ipv4Addr::new(172, 16, 0, 100),
            [0x02, 0, 0, 0, 0, 0xAA],
            default_policy(),
        );
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn service_update_backend() {
        let mut w = make_worker();
        inject_namespace(&mut w, "ns1");
        let ns_id = NamespaceId::from("ns1");
        let svc_id = ServiceId::from("svc1");

        w.handle_create_service(
            &ns_id,
            &svc_id,
            Ipv4Addr::new(172, 16, 0, 100),
            [0x02, 0, 0, 0, 0, 0xAA],
            default_policy(),
        )
        .unwrap();

        // Assign backend.
        w.handle_update_service_backend(
            &ns_id,
            &svc_id,
            Some(ServiceBackend {
                pod_ip: Ipv4Addr::new(172, 16, 0, 10),
                pod_mac: [0x02, 0, 0, 0, 0, 0x10],
            }),
        )
        .unwrap();

        // Remove backend.
        w.handle_update_service_backend(&ns_id, &svc_id, None)
            .unwrap();
    }

    #[tokio::test]
    async fn service_update_backend_errors_on_missing_namespace() {
        let mut w = make_worker();
        let result = w.handle_update_service_backend(
            &NamespaceId::from("nope"),
            &ServiceId::from("svc1"),
            None,
        );
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn service_ready_errors_on_missing_namespace() {
        let mut w = make_worker();
        let result = w
            .handle_service_ready(&NamespaceId::from("nope"), &ServiceId::from("svc1"))
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn service_ready_on_existing_service() {
        let mut w = make_worker();
        inject_namespace(&mut w, "ns1");
        let ns_id = NamespaceId::from("ns1");
        let svc_id = ServiceId::from("svc1");

        w.handle_create_service(
            &ns_id,
            &svc_id,
            Ipv4Addr::new(172, 16, 0, 100),
            [0x02, 0, 0, 0, 0, 0xAA],
            default_policy(),
        )
        .unwrap();

        w.handle_update_service_backend(
            &ns_id,
            &svc_id,
            Some(ServiceBackend {
                pod_ip: Ipv4Addr::new(172, 16, 0, 10),
                pod_mac: [0x02, 0, 0, 0, 0, 0x10],
            }),
        )
        .unwrap();

        // ServiceReady should succeed (no buffered frames, so no flush).
        w.handle_service_ready(&ns_id, &svc_id).await.unwrap();
    }

    #[tokio::test]
    async fn service_destroy_errors_on_missing_namespace() {
        let mut w = make_worker();
        let result = w.handle_destroy_service(
            &NamespaceId::from("nope"),
            &ServiceId::from("svc1"),
        );
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // Namespace destruction tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn destroy_namespace_removes_state() {
        let mut w = make_worker();
        inject_namespace(&mut w, "ns1");
        assert!(w.namespaces.contains_key(&NamespaceId::from("ns1")));

        w.handle_destroy_namespace(&NamespaceId::from("ns1"))
            .await
            .unwrap();
        assert!(!w.namespaces.contains_key(&NamespaceId::from("ns1")));
    }

    #[tokio::test]
    async fn destroy_namespace_noop_for_nonexistent() {
        let mut w = make_worker();
        // Should not error.
        w.handle_destroy_namespace(&NamespaceId::from("nope"))
            .await
            .unwrap();
    }

    // -----------------------------------------------------------------------
    // Stop pod tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn stop_pod_errors_on_missing_namespace() {
        let mut w = make_worker();
        let result = w
            .handle_stop_pod(&NamespaceId::from("nope"), &PodId::from("pod1"), true)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn stop_pod_noop_for_missing_pod() {
        let mut w = make_worker();
        inject_namespace(&mut w, "ns1");

        // Stopping a pod that doesn't exist should succeed (no-op).
        w.handle_stop_pod(&NamespaceId::from("ns1"), &PodId::from("pod1"), true)
            .await
            .unwrap();
    }

    // -----------------------------------------------------------------------
    // Shutdown all tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn shutdown_all_drains_namespaces() {
        let mut w = make_worker();
        inject_namespace(&mut w, "ns1");
        inject_namespace(&mut w, "ns2");
        assert_eq!(w.namespaces.len(), 2);

        w.shutdown_all().await;
        assert!(w.namespaces.is_empty());
        assert!(w.worker_token.is_cancelled());
    }

    // -----------------------------------------------------------------------
    // Full command dispatch tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn handle_command_dispatches_registry_sync() {
        let mut w = make_worker();
        inject_namespace(&mut w, "ns1");

        let log_opener = make_log_opener();

        let cmd = WorkerCommand::RegistrySync {
            namespace_id: NamespaceId::from("ns1"),
            entries: vec![RegistryEntry {
                name: "api".to_string(),
                ip: Ipv4Addr::new(172, 16, 0, 10),
            }],
        };

        w.handle_command(cmd, &log_opener).await.unwrap();

        let ns = w.namespaces.get(&NamespaceId::from("ns1")).unwrap();
        let map = ns.registry.read().unwrap();
        assert_eq!(map.get("api"), Some(&Ipv4Addr::new(172, 16, 0, 10)));
    }

    #[tokio::test]
    async fn handle_command_dispatches_destroy_namespace() {
        let mut w = make_worker();
        inject_namespace(&mut w, "ns1");

        let log_opener = make_log_opener();

        let cmd = WorkerCommand::DestroyNamespace {
            namespace_id: NamespaceId::from("ns1"),
        };

        w.handle_command(cmd, &log_opener).await.unwrap();
        assert!(!w.namespaces.contains_key(&NamespaceId::from("ns1")));
    }

    // -----------------------------------------------------------------------
    // Pod lifecycle tests (mock VM)
    // -----------------------------------------------------------------------

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
            // Wait forever (or until killed).
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
                entrypoint: "/bin/echo".to_string(),
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

    #[tokio::test]
    async fn image_provider_failure_sends_pod_failed() {
        let (bg_event_tx, mut bg_event_rx) = mpsc::channel(256);
        let image_provider = Arc::new(FailingImageProvider {
            error_msg: "image not found".to_string(),
        });
        let vmm = Arc::new(StubVmm);
        let fabric = Arc::new(tokio::sync::Mutex::new(Fabric::<FabricPort>::new()));
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
        let fabric = Arc::new(tokio::sync::Mutex::new(Fabric::<FabricPort>::new()));
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
        let fabric = Arc::new(tokio::sync::Mutex::new(Fabric::<FabricPort>::new()));
        let cancel = CancellationToken::new();

        let log_opener = make_log_opener();
        let (bg_event_tx, mut bg_event_rx) = mpsc::channel(256);

        let cancel_clone = cancel.clone();
        tokio::spawn(async move {
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

    #[tokio::test]
    async fn handle_launch_pod_registers_pod_state() {
        // Verify that handle_launch_pod creates PodState in the namespace.
        let (worker_socket, _guest_socket) = UnixStream::pair().unwrap();
        let vmm = MockVmm {
            launch_error: None,
            vm_socket: tokio::sync::Mutex::new(Some(worker_socket)),
        };
        // Use FailingImageProvider so the supervisor fails quickly —
        // we just want to verify the PodState was registered.
        let mut w = Worker::new(
            PathBuf::from("/fake/kernel"),
            PathBuf::from("/fake/rootfs"),
            vmm,
            FailingImageProvider {
                error_msg: "intentional".to_string(),
            },
            None,
            String::new(),
        );

        // Inject namespace manually.
        {
            let fabric = Fabric::<FabricPort>::new();
            let tables = fabric.tables();
            let ns_token = w.worker_token.child_token();
            let ns = NamespaceState {
                fabric: Arc::new(tokio::sync::Mutex::new(fabric)),
                tables,
                _gateway_task: TaskHandle::spawn(std::future::pending::<()>()),
                _event_bridge_task: TaskHandle::spawn(std::future::pending::<()>()),
                _adapter_tasks: Vec::new(),
                _adapter_ports: Vec::new(),
                registry: Arc::new(RwLock::new(HashMap::new())),
                pods: HashMap::new(),
                token: ns_token,
            };
            w.namespaces.insert(NamespaceId::from("ns1"), ns);
        }

        let log_opener = make_log_opener();

        w.handle_launch_pod(
            &NamespaceId::from("ns1"),
            PodId::from("pod1"),
            make_pod_network(),
            make_containers(),
            &log_opener,
        )
        .await
        .unwrap();

        // Pod should be registered.
        let ns = w.namespaces.get(&NamespaceId::from("ns1")).unwrap();
        assert!(ns.pods.contains_key(&PodId::from("pod1")));
    }
}
