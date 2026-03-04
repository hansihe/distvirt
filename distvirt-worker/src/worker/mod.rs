pub(crate) mod namespace;
pub(crate) mod supervisor;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use distvirt_activator::ActivatorRuntime;
use distvirt_worker_protocol::{
    ContainerSpec, LogStreamOpener, NamespaceId, NetworkConfig,
    PodId, PodNetworkConfig, SnapshotId, WorkerCapabilities, WorkerCommand,
    WorkerConnection, WorkerEvent, WorkerHello,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::adapter::AdapterManager;
use crate::image_provider::ImageProvider;
use namespace::{FatalError, NamespaceState};
use supervisor::{PodState, SuspendRequest, pod_supervisor, pod_resume_supervisor, send_event, STOP_POD_TIMEOUT};
use crate::task_handle::TaskHandle;
use crate::vmm::Vmm;

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
    /// Base directory for VM snapshots.
    snapshot_base_dir: PathBuf,
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
        let snapshot_base_dir = std::env::temp_dir().join(format!("distvirt-snapshots-{}", std::process::id()));
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
            snapshot_base_dir,
        }
    }

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

    /// Run the worker main loop: receive commands, dispatch them,
    /// and forward background events to the orchestrator.
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
                        | WorkerEvent::PodFailed { namespace_id, pod_id, .. }
                        | WorkerEvent::PodSuspended { namespace_id, pod_id, .. }
                        | WorkerEvent::PodSuspendFailed { namespace_id, pod_id, .. } => {
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
            } => {
                let ns = self.get_namespace_mut(&namespace_id)?;
                ns.registry_sync(&namespace_id, entries)
            }
            WorkerCommand::RegistryUpdate {
                namespace_id,
                added,
                removed,
            } => {
                let ns = self.get_namespace_mut(&namespace_id)?;
                ns.registry_update(&namespace_id, added, removed)
            }
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
            } => {
                let ns = self.get_namespace_mut(&namespace_id)?;
                ns.route_sync(&namespace_id, routes)
            }
            WorkerCommand::FabricRouteUpdate {
                namespace_id,
                added,
                removed_ips,
            } => {
                let ns = self.get_namespace_mut(&namespace_id)?;
                ns.route_update(&namespace_id, added, removed_ips)
            }
            WorkerCommand::CreateService {
                namespace_id,
                service_id,
                ip,
                mac,
                policy,
            } => {
                // Borrow separate fields to avoid conflicting mutable/immutable borrows on self.
                let activator_runtime = self.activator_runtime.as_ref();
                let ns = self.namespaces.get_mut(&namespace_id).ok_or_else(|| {
                    FatalError::InternalInvariant(format!("namespace '{}' not found", namespace_id))
                })?;
                ns.create_service(&namespace_id, &service_id, ip, mac, policy, activator_runtime)
            }
            WorkerCommand::UpdateServiceBackend {
                namespace_id,
                service_id,
                backend,
            } => {
                let ns = self.get_namespace_mut(&namespace_id)?;
                ns.update_service_backend(&namespace_id, &service_id, backend)
            }
            WorkerCommand::ServiceReady {
                namespace_id,
                service_id,
            } => {
                let ns = self.get_namespace_mut(&namespace_id)?;
                ns.service_ready(&namespace_id, &service_id).await
            }
            WorkerCommand::DestroyService {
                namespace_id,
                service_id,
            } => {
                let ns = self.get_namespace_mut(&namespace_id)?;
                ns.destroy_service(&namespace_id, &service_id)
            }
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
            WorkerCommand::SuspendPod {
                namespace_id,
                pod_id,
                snapshot_id,
            } => {
                self.handle_suspend_pod(&namespace_id, &pod_id, snapshot_id)
                    .await
            }
            WorkerCommand::ResumePod {
                namespace_id,
                pod_id,
                snapshot_id,
                network,
            } => {
                self.handle_resume_pod(&namespace_id, pod_id, snapshot_id, network)
                    .await
            }
            WorkerCommand::DeleteSnapshot { snapshot_id } => {
                self.handle_delete_snapshot(&snapshot_id).await
            }
            WorkerCommand::Shutdown => {
                // Handled in the main loop; should not reach here.
                unreachable!("Shutdown handled in run()")
            }
        }
    }

    /// Look up a namespace by ID, returning FatalError if not found.
    fn get_namespace_mut(&mut self, namespace_id: &NamespaceId) -> Result<&mut NamespaceState, FatalError> {
        self.namespaces.get_mut(namespace_id).ok_or_else(|| {
            FatalError::InternalInvariant(format!("namespace '{}' not found", namespace_id))
        })
    }

    async fn handle_create_namespace(
        &mut self,
        namespace_id: NamespaceId,
        network: NetworkConfig,
    ) -> Result<(), FatalError> {
        let (ns, event) = NamespaceState::new(
            &self.worker_token,
            &self.bg_event_tx,
            &self.adapter_manager,
            &namespace_id,
            network,
        )?;

        self.namespaces.insert(namespace_id, ns);

        // Send via background event channel — handlers never touch conn directly.
        send_event(&self.bg_event_tx, event).await;
        Ok(())
    }

    async fn handle_destroy_namespace(&mut self, namespace_id: &NamespaceId) -> Result<(), FatalError> {
        if let Some(ns) = self.namespaces.remove(namespace_id) {
            ns.destroy(&self.bg_event_tx, namespace_id).await;
        }
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

        let (suspend_tx, suspend_rx) = mpsc::channel(1);

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
                suspend_rx,
            )
            .await;
        });

        ns.pods.insert(
            pod_id,
            PodState {
                cancel: pod_cancel,
                supervisor,
                suspend_tx,
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
        let ns = self.get_namespace_mut(namespace_id)?;

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
                // Non-graceful: cancel and give the supervisor a short window to
                // kill the VM and clean up (TAP destruction needs the process gone).
                pod.cancel.cancel();
                match tokio::time::timeout(STOP_POD_TIMEOUT, pod.supervisor).await {
                    Ok(Ok(())) => {}
                    Ok(Err(join_error)) => {
                        log::error!(
                            "worker: pod '{}' supervisor panicked: {}",
                            pod_id,
                            join_error
                        );
                    }
                    Err(_) => {
                        // Timed out — supervisor task is dropped here, aborting it.
                        log::warn!(
                            "worker: pod '{}' force stop timed out, aborting",
                            pod_id
                        );
                    }
                }
                log::info!(
                    "worker: forcibly stopped pod '{}' in namespace '{}'",
                    pod_id,
                    namespace_id
                );
            }
        }

        Ok(())
    }

    async fn handle_suspend_pod(
        &mut self,
        namespace_id: &NamespaceId,
        pod_id: &PodId,
        snapshot_id: SnapshotId,
    ) -> Result<(), FatalError> {
        let snapshot_dir = self.snapshot_base_dir.join(snapshot_id.as_ref());

        let suspend_tx = {
            let ns = self.get_namespace_mut(namespace_id)?;
            match ns.pods.get(pod_id) {
                Some(pod) => pod.suspend_tx.clone(),
                None => {
                    send_event(
                        &self.bg_event_tx,
                        WorkerEvent::PodSuspendFailed {
                            namespace_id: namespace_id.clone(),
                            pod_id: pod_id.clone(),
                            error: format!("pod '{}' not found", pod_id),
                        },
                    )
                    .await;
                    return Ok(());
                }
            }
        };

        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();

        let req = SuspendRequest {
            snapshot_id: snapshot_id.clone(),
            snapshot_dir,
            reply: reply_tx,
        };

        if suspend_tx.send(req).await.is_err() {
            log::error!("suspend_pod: pod '{}' supervisor already exited", pod_id);
            send_event(
                &self.bg_event_tx,
                WorkerEvent::PodSuspendFailed {
                    namespace_id: namespace_id.clone(),
                    pod_id: pod_id.clone(),
                    error: "pod supervisor already exited".to_string(),
                },
            )
            .await;
            return Ok(());
        }

        // Wait for the suspend to complete (the monitor handles the actual work).
        match reply_rx.await {
            Ok(Ok(_artifacts)) => {
                log::info!("suspend_pod: pod '{}' suspended successfully", pod_id);
            }
            Ok(Err(e)) => {
                log::error!("suspend_pod: pod '{}' suspend failed: {}", pod_id, e);
            }
            Err(_) => {
                log::error!("suspend_pod: pod '{}' reply channel dropped", pod_id);
            }
        }

        Ok(())
    }

    async fn handle_resume_pod(
        &mut self,
        namespace_id: &NamespaceId,
        pod_id: PodId,
        snapshot_id: SnapshotId,
        network: PodNetworkConfig,
    ) -> Result<(), FatalError> {
        let ns = self.namespaces.get_mut(namespace_id).ok_or_else(|| {
            FatalError::InternalInvariant(format!("namespace '{}' not found", namespace_id))
        })?;

        // Load snapshot metadata.
        let snapshot_dir = self.snapshot_base_dir.join(snapshot_id.as_ref());
        let metadata_path = snapshot_dir.join("metadata.json");
        let metadata_bytes = tokio::fs::read(&metadata_path).await.map_err(|e| {
            FatalError::InternalInvariant(format!(
                "failed to read snapshot metadata at {}: {}",
                metadata_path.display(),
                e
            ))
        })?;
        let metadata: crate::vmm::SnapshotMetadata =
            serde_json::from_slice(&metadata_bytes).map_err(|e| {
                FatalError::InternalInvariant(format!("invalid snapshot metadata: {}", e))
            })?;

        let snapshot = crate::vmm::SnapshotArtifacts {
            snapshot_dir,
            metadata,
        };

        // Register the pod's IP→MAC mapping with the WireGuard adapter.
        if let Some(wg) = self.adapter_manager.wireguard() {
            wg.register_pod_mac(namespace_id.as_ref(), network.ip, network.mac).await;
        }

        let pod_cancel = ns.token.child_token();
        let event_tx = self.bg_event_tx.clone();
        let vmm = Arc::clone(&self.vmm);
        let fabric = Arc::clone(&ns.fabric);
        let ns_id = namespace_id.clone();
        let pid = pod_id.clone();
        let cancel_clone = pod_cancel.clone();

        let (suspend_tx, suspend_rx) = mpsc::channel(1);

        let supervisor = TaskHandle::spawn(async move {
            pod_resume_supervisor(
                vmm,
                fabric,
                cancel_clone,
                event_tx,
                ns_id,
                pid,
                network,
                snapshot,
                suspend_rx,
            )
            .await;
        });

        ns.pods.insert(
            pod_id,
            PodState {
                cancel: pod_cancel,
                supervisor,
                suspend_tx,
            },
        );

        Ok(())
    }

    async fn handle_delete_snapshot(
        &self,
        snapshot_id: &SnapshotId,
    ) -> Result<(), FatalError> {
        let snapshot_dir = self.snapshot_base_dir.join(snapshot_id.as_ref());
        if snapshot_dir.exists() {
            if let Err(e) = tokio::fs::remove_dir_all(&snapshot_dir).await {
                log::error!(
                    "delete_snapshot: failed to remove {}: {}",
                    snapshot_dir.display(),
                    e
                );
            } else {
                log::info!("delete_snapshot: removed {}", snapshot_dir.display());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::net::Ipv4Addr;
    use std::sync::RwLock;

    use distvirt_worker_protocol::{
        ContainerConfig, ContainerSpec, PodNetworkConfig,
        RegistryEntry,
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

        let ns_token = worker.worker_token.child_token();

        // Dummy task handles that pend forever.
        let gateway_task = TaskHandle::spawn(std::future::pending::<()>());
        let event_bridge_task = TaskHandle::spawn(std::future::pending::<()>());

        let ns = NamespaceState::new_for_test(
            Arc::new(fabric),
            tables,
            gateway_task,
            event_bridge_task,
            Vec::new(),
            Vec::new(),
            Arc::new(RwLock::new(HashMap::new())),
            HashMap::new(),
            ns_token,
        );

        worker
            .namespaces
            .insert(NamespaceId::from(ns_id), ns);
    }

    fn make_log_opener() -> LogStreamOpener {
        LogStreamOpener::disconnected()
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

    struct FailingImageProvider {
        error_msg: String,
    }

    impl ImageProvider for FailingImageProvider {
        async fn prepare(&self, _image_ref: &str) -> anyhow::Result<PreparedArtifact> {
            Err(anyhow::anyhow!("{}", self.error_msg))
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
            let ns = NamespaceState::new_for_test(
                Arc::new(fabric),
                tables,
                TaskHandle::spawn(std::future::pending::<()>()),
                TaskHandle::spawn(std::future::pending::<()>()),
                Vec::new(),
                Vec::new(),
                Arc::new(RwLock::new(HashMap::new())),
                HashMap::new(),
                ns_token,
            );
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
