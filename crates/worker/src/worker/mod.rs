pub(crate) mod artifact_transfer;
pub(crate) mod namespace;
pub(crate) mod resources;
pub(crate) mod supervisor;
pub(crate) mod tunnel_manager;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use distvirt_activator::ActivatorRuntime;
use distvirt_worker_protocol::{
    ArtifactId, ContainerSpec, LogStreamOpener, NamespaceId, NetworkConfig, PodId,
    PodNetworkConfig, PoolId, PoolInfo, PsiMetrics, WorkerCapabilities, WorkerCommand,
    WorkerConnection, WorkerEvent, WorkerHello, WorkerId, WorkerReady,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use distvirt_common::ActivityTracker;

use crate::adapter::AdapterManager;
use crate::fabric::gateway::GatewayProvider;
use crate::fs::Fs;
use crate::image_provider::ImageProvider;
use crate::resource_monitor::{HostResourceMonitor, ResourceMonitor};
use crate::task_handle::TaskHandle;
use crate::vmm::Vmm;
use namespace::{FatalError, NamespaceState};
use resources::*;
use supervisor::{
    FORCE_STOP_TIMEOUT, PodState, STOP_POD_TIMEOUT, SuspendRequest, pod_resume_supervisor,
    pod_supervisor, send_event,
};
use tunnel_manager::TunnelManager;

/// The worker: sits between the orchestrator and the raw VM/fabric primitives.
///
/// Receives `WorkerCommand`s via a `WorkerConnection`, sends `WorkerEvent`s back,
/// and opens yamux log streams for container output.
pub struct Worker<
    V: Vmm + 'static,
    P: ImageProvider + 'static,
    G: GatewayProvider + 'static,
    F: Fs = crate::fs::TokioFs,
    R: ResourceMonitor = HostResourceMonitor,
> {
    kernel_path: PathBuf,
    rootfs_image_path: PathBuf,
    namespaces: HashMap<NamespaceId, NamespaceState>,
    vmm: Arc<V>,
    image_provider: Arc<P>,
    _fs: std::marker::PhantomData<F>,
    _resource_monitor: std::marker::PhantomData<R>,
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
    /// Pool registry: maps pool IDs to their root directories.
    pools: HashMap<PoolId, PathBuf>,
    /// Tunnel manager for inter-worker fabric forwarding.
    tunnel_manager: Option<TunnelManager>,
    /// Assigned worker ID from handshake (set after WorkerAccepted).
    worker_id: Option<WorkerId>,
    /// Gateway provider for creating egress ports per namespace.
    gateway_provider: G,
    /// Activity tracker for convergence detection in tests.
    activity: Arc<ActivityTracker>,
}

impl<
    V: Vmm + 'static,
    P: ImageProvider + 'static,
    G: GatewayProvider + 'static,
    F: Fs,
    R: ResourceMonitor,
> Worker<V, P, G, F, R>
{
    pub fn new(
        kernel_path: PathBuf,
        rootfs_image_path: PathBuf,
        vmm: V,
        image_provider: P,
        component_dir: Option<PathBuf>,
        public_endpoint: String,
        gateway_provider: G,
        activity: Arc<ActivityTracker>,
    ) -> Self {
        let (bg_event_tx, bg_event_rx) = mpsc::channel(256);
        let activator_runtime = component_dir.and_then(|dir| match ActivatorRuntime::new(&dir) {
            Ok(rt) => Some(rt),
            Err(e) => {
                log::warn!(
                    "activator runtime init failed: {:#}, activators disabled",
                    e
                );
                None
            }
        });
        // Include a per-instance counter so parallel tests in the same process
        // don't share snapshot directories and stomp each other's artifacts.
        use std::sync::atomic::{AtomicU64, Ordering};
        static INSTANCE_COUNTER: AtomicU64 = AtomicU64::new(0);
        let instance_id = INSTANCE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let default_pool_path = std::env::temp_dir().join(format!(
            "distvirt-snapshots-{}-{}",
            std::process::id(),
            instance_id,
        ));
        let mut pools = HashMap::new();
        let pool_id = PoolId::from("local-default".to_string());
        pools.insert(pool_id, default_pool_path);
        Worker {
            kernel_path,
            rootfs_image_path,
            namespaces: HashMap::new(),
            vmm: Arc::new(vmm),
            image_provider: Arc::new(image_provider),
            _fs: std::marker::PhantomData,
            _resource_monitor: std::marker::PhantomData,
            bg_event_tx,
            bg_event_rx,
            worker_token: CancellationToken::new(),
            activator_runtime,
            adapter_manager: AdapterManager::empty(),
            public_endpoint,
            pools,
            tunnel_manager: None,
            worker_id: None,
            gateway_provider,
            activity,
        }
    }

    /// Look up a pool's root directory by ID.
    fn pool_path(&self, pool_id: &PoolId) -> Option<&PathBuf> {
        self.pools.get(pool_id)
    }

    /// Detect local capabilities for the WorkerHello handshake.
    fn detect_capabilities(&self) -> WorkerCapabilities {
        let has_kvm = std::path::Path::new("/dev/kvm").exists();
        let containerd_socket = std::env::var("CONTAINERD_SOCKET")
            .unwrap_or_else(|_| "/run/containerd/containerd.sock".into());
        let has_containerd = std::path::Path::new(&containerd_socket).exists();

        let pools: Vec<distvirt_worker_protocol::PoolInfo> = self
            .pools
            .iter()
            .map(|(pool_id, path)| {
                let (capacity_bytes, available_bytes) = pool_disk_stats(path);
                distvirt_worker_protocol::PoolInfo {
                    pool_id: pool_id.clone(),
                    path: path.to_string_lossy().into_owned(),
                    capacity_bytes,
                    available_bytes,
                }
            })
            .collect();

        WorkerCapabilities {
            has_kvm,
            has_containerd,
            available_adapters: vec!["wireguard".to_string()],
            max_pods: 10,
            available_memory_mb: detect_host_memory_mb(),
            public_endpoint: self.public_endpoint.clone(),
            pools,
        }
    }

    /// Run the worker main loop: receive commands, dispatch them,
    /// and forward background events to the orchestrator.
    pub async fn run(
        mut self,
        mut conn: WorkerConnection,
        worker_secret: String,
    ) -> anyhow::Result<()> {
        // --- Handshake ---
        let capabilities = self.detect_capabilities();
        log::info!("worker: capabilities: {:?}", capabilities);

        conn.send_hello(&WorkerHello {
            auth_token: worker_secret,
            capabilities,
        })
        .await
        .context("handshake: send WorkerHello")?;

        let accepted = conn
            .recv_accepted()
            .await
            .context("handshake: recv WorkerAccepted")?;
        log::info!("worker: accepted as worker_id={}", accepted.worker_id);
        self.worker_id = Some(accepted.worker_id.clone());

        // Process pools pushed by the orchestrator.
        for pool in &accepted.pools {
            let path = std::path::PathBuf::from(&pool.path);
            if !path.exists() {
                log::warn!(
                    "worker: pushed pool '{}' path {} does not exist, skipping",
                    pool.pool_id,
                    pool.path
                );
                continue;
            }
            log::info!(
                "worker: registering pushed pool '{}' at {}",
                pool.pool_id,
                pool.path
            );
            self.pools.insert(pool.pool_id.clone(), path);
        }

        self.adapter_manager = AdapterManager::new(&accepted.adapters).await;

        // Initialize tunnel manager after handshake so we know whether
        // the orchestrator wants encrypted tunnels.
        match TunnelManager::new("0.0.0.0:0".parse().unwrap(), accepted.tunnel_encrypted).await {
            Ok(tm) => {
                log::info!(
                    "worker: tunnel manager listening on port {:?} (encrypted={})",
                    tm.listen_port(),
                    accepted.tunnel_encrypted,
                );
                self.tunnel_manager = Some(tm);
            }
            Err(e) => {
                log::warn!(
                    "worker: failed to init tunnel manager: {}, tunnels disabled",
                    e
                );
            }
        }

        // Start artifact transfer listener.
        let transfer_listen_port =
            match artifact_transfer::start_transfer_listener("0.0.0.0:0").await {
                Ok((listener, port)) => {
                    log::info!("worker: artifact transfer listener on port {}", port);
                    let pools = self.pools.clone();
                    let tx = self.bg_event_tx.clone();
                    tokio::spawn(artifact_transfer::transfer_accept_loop(listener, pools, tx));
                    Some(port)
                }
                Err(e) => {
                    log::warn!(
                        "worker: failed to start transfer listener: {}, transfers disabled",
                        e
                    );
                    None
                }
            };

        conn.send_ready(&WorkerReady {
            tunnel_listen_port: self.tunnel_manager.as_ref().and_then(|tm| tm.listen_port()),
            tunnel_public_key: self.tunnel_manager.as_ref().and_then(|tm| tm.public_key()),
            transfer_listen_port,
            wireguard_listen_port: self.adapter_manager.wireguard_listen_port(),
            wireguard_public_key: self.adapter_manager.wireguard_public_key(),
        })
        .await
        .context("handshake: send WorkerReady")?;
        log::info!("worker: handshake complete, entering command loop");

        // --- Command loop ---
        //
        // Split the connection into separate reader/writer halves to avoid
        // cancellation-safety issues. `recv_command` uses `read_exact` under
        // the hood, which is NOT cancellation-safe — if a `tokio::select!`
        // branch wins while `read_exact` has partially consumed bytes from
        // the yamux stream, those bytes are lost and the framing is corrupted.
        //
        // By moving reads into a dedicated task, `recv_command` is never
        // cancelled mid-read. The main loop only touches mpsc channels,
        // which are cancellation-safe.
        let (reader, writer, log_opener, _conn_driver) = conn.into_split();

        // Reader task: continuously receives commands and forwards them.
        // Owned by a TaskHandle so it's aborted if the main loop exits.
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<WorkerCommand>(64);
        let _reader_task = TaskHandle::spawn(async move {
            let mut reader = reader;
            loop {
                match reader.recv_command().await {
                    Ok(cmd) => {
                        if cmd_tx.send(cmd).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        log::error!("worker: connection read error: {:#}", e);
                        break;
                    }
                }
            }
        });

        // Writer task: receives events from a channel and sends them on the wire.
        // Owned by a TaskHandle so it's aborted if the main loop exits.
        let (event_tx, event_rx) = mpsc::channel::<WorkerEvent>(64);
        let _writer_task = TaskHandle::spawn(async move {
            let mut writer = writer;
            let mut event_rx = event_rx;
            while let Some(event) = event_rx.recv().await {
                if let Err(e) = writer.send_event(&event).await {
                    log::error!("worker: connection write error: {:#}", e);
                    break;
                }
            }
        });

        let mut capacity_interval = tokio::time::interval(std::time::Duration::from_secs(30));
        capacity_interval.tick().await; // consume the immediate first tick
        let mut last_pools: Vec<PoolInfo> = Vec::new();

        // PSI pressure reporting (10s interval, Linux only).
        let psi_available = R::read_psi().await.is_some();
        if !psi_available {
            log::info!("worker: PSI not available, pressure will use static accounting only");
        }
        let mut psi_interval = tokio::time::interval(std::time::Duration::from_secs(10));
        psi_interval.tick().await; // consume the immediate first tick
        let mut last_psi: Option<(PsiMetrics, PsiMetrics, PsiMetrics)> = None;

        let result = 'result: loop {
            tokio::select! {
                cmd_result = cmd_rx.recv() => {
                    match cmd_result {
                        Some(WorkerCommand::Shutdown) => {
                            log::info!("worker: received Shutdown command");
                            let _ = event_tx.send(WorkerEvent::ShuttingDown).await;
                            break Ok(());
                        }
                        Some(cmd) => {
                            if let Err(e) = self.handle_command(cmd, &log_opener).await {
                                log::error!("worker: fatal error: {}", e);
                                break Err(anyhow::anyhow!("{}", e));
                            }
                        }
                        None => {
                            log::error!("worker: connection lost");
                            break Err(anyhow::anyhow!("connection lost: reader task exited"));
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
                    if event_tx.send(event).await.is_err() {
                        log::error!("worker: failed to send event: writer task exited");
                        break Err(anyhow::anyhow!("connection lost: writer task exited"));
                    }
                }
                _ = capacity_interval.tick() => {
                    let pools: Vec<PoolInfo> = self
                        .pools
                        .iter()
                        .map(|(pool_id, path)| {
                            let (capacity_bytes, available_bytes) = pool_disk_stats(path);
                            PoolInfo {
                                pool_id: pool_id.clone(),
                                path: path.to_string_lossy().into_owned(),
                                capacity_bytes,
                                available_bytes,
                            }
                        })
                        .collect();

                    // Only send if capacity changed meaningfully (>1% delta on any pool).
                    let changed = pools.len() != last_pools.len() || pools.iter().zip(last_pools.iter()).any(|(new, old)| {
                        if new.pool_id != old.pool_id || new.capacity_bytes != old.capacity_bytes {
                            return true;
                        }
                        let threshold = old.capacity_bytes / 100; // 1%
                        let diff = if new.available_bytes > old.available_bytes {
                            new.available_bytes - old.available_bytes
                        } else {
                            old.available_bytes - new.available_bytes
                        };
                        diff > threshold
                    });

                    if changed {
                        // Check watermark thresholds and emit/deassert conditions.
                        for pool in &pools {
                            if pool.capacity_bytes == 0 {
                                continue;
                            }
                            let used_pct = ((pool.capacity_bytes - pool.available_bytes) as f64
                                / pool.capacity_bytes as f64
                                * 100.0) as u64;

                            let soft_key = format!("storage/pool/{}/pressure-soft", pool.pool_id);
                            let hard_key = format!("storage/pool/{}/pressure-hard", pool.pool_id);

                            // Hard threshold: 95%
                            let hard_active = used_pct >= 95;
                            if event_tx.send(WorkerEvent::WorkerCondition {
                                key: hard_key,
                                active: hard_active,
                                message: if hard_active {
                                    format!("pool {} at {}% capacity", pool.pool_id, used_pct)
                                } else {
                                    String::new()
                                },
                            }).await.is_err() {
                                log::error!("worker: failed to send condition event: writer task exited");
                                break 'result Err(anyhow::anyhow!("connection lost: writer task exited"));
                            }

                            // Soft threshold: 85%
                            let soft_active = used_pct >= 85;
                            if event_tx.send(WorkerEvent::WorkerCondition {
                                key: soft_key,
                                active: soft_active,
                                message: if soft_active {
                                    format!("pool {} at {}% capacity", pool.pool_id, used_pct)
                                } else {
                                    String::new()
                                },
                            }).await.is_err() {
                                log::error!("worker: failed to send condition event: writer task exited");
                                break 'result Err(anyhow::anyhow!("connection lost: writer task exited"));
                            }
                        }

                        if event_tx.send(WorkerEvent::PoolCapacityUpdate {
                            pools: pools.clone(),
                        }).await.is_err() {
                            log::error!("worker: failed to send capacity update: writer task exited");
                            break 'result Err(anyhow::anyhow!("connection lost: writer task exited"));
                        }
                        last_pools = pools;
                    }
                }
                _ = psi_interval.tick(), if psi_available => {
                    if let Some(psi) = R::read_psi().await {
                        let should_send = match &last_psi {
                            Some(old) => psi_changed_significantly(old, &psi),
                            None => true,
                        };
                        if should_send {
                            if event_tx.send(WorkerEvent::PressureUpdate {
                                cpu: psi.0.clone(),
                                memory: psi.1.clone(),
                                io: psi.2.clone(),
                            }).await.is_err() {
                                log::error!("worker: failed to send pressure update: writer task exited");
                                break 'result Err(anyhow::anyhow!("connection lost: writer task exited"));
                            }
                            last_psi = Some(psi);
                        }
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
                        log::warn!("worker: pod '{}' supervisor timed out, aborting", pod_id);
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
                resources,
                volumes,
            } => {
                self.handle_launch_pod(
                    &namespace_id,
                    pod_id,
                    network,
                    containers,
                    resources,
                    volumes,
                    log_opener,
                )
                .await
            }
            WorkerCommand::StopPod {
                namespace_id,
                pod_id,
                graceful,
            } => self.handle_stop_pod(&namespace_id, &pod_id, graceful).await,

            WorkerCommand::AddWireGuardPeer {
                namespace_id,
                peer_public_key,
                peer_ip,
                preshared_key,
            } => {
                if let Some(wg) = self.adapter_manager.wireguard() {
                    if let Err(e) = wg
                        .add_peer(
                            namespace_id.as_ref(),
                            peer_public_key,
                            peer_ip,
                            preshared_key,
                        )
                        .await
                    {
                        log::error!("wireguard: add_peer failed: {:#}", e);
                    }
                } else {
                    log::warn!(
                        "wireguard: AddWireGuardPeer command but no WireGuard adapter configured"
                    );
                }
                Ok(())
            }
            WorkerCommand::RemoveWireGuardPeer { peer_public_key } => {
                if let Some(wg) = self.adapter_manager.wireguard() {
                    if let Err(e) = wg.remove_peer(&peer_public_key).await {
                        log::error!("wireguard: remove_peer failed: {:#}", e);
                    }
                } else {
                    log::warn!(
                        "wireguard: RemoveWireGuardPeer command but no WireGuard adapter configured"
                    );
                }
                Ok(())
            }
            WorkerCommand::SuspendPod {
                namespace_id,
                pod_id,
                artifact_id,
                pool_id,
            } => {
                self.handle_suspend_pod(&namespace_id, &pod_id, artifact_id, pool_id)
                    .await
            }
            WorkerCommand::ResumePod {
                namespace_id,
                pod_id,
                artifact_id,
                network,
                pool_id,
            } => {
                self.handle_resume_pod(&namespace_id, pod_id, artifact_id, network, pool_id)
                    .await
            }
            WorkerCommand::DeleteArtifact {
                artifact_id,
                pool_id,
            } => self.handle_delete_artifact(&artifact_id, &pool_id).await,
            WorkerCommand::TransferArtifact {
                transfer_id,
                source_artifact_id,
                source_pool_id,
                dest_artifact_id,
                dest_pool_id,
                dest_endpoint,
            } => {
                self.handle_transfer_artifact(
                    transfer_id,
                    source_artifact_id,
                    source_pool_id,
                    dest_artifact_id,
                    dest_pool_id,
                    dest_endpoint,
                )
                .await
            }
            WorkerCommand::WorkerRegistrySync { workers } => {
                log::info!("received worker registry sync with {} peers", workers.len());
                if let Some(ref mut tm) = self.tunnel_manager {
                    tm.handle_registry_sync(workers);
                }
                Ok(())
            }
            WorkerCommand::EndpointSync {
                namespace_id,
                endpoints,
            } => {
                log::info!(
                    "worker: received EndpointSync for ns='{}' with {} endpoints: [{}]",
                    namespace_id,
                    endpoints.len(),
                    endpoints.iter().map(|e| format!("{}", e.ip)).collect::<Vec<_>>().join(", ")
                );
                let worker_id = self.worker_id.clone().ok_or_else(|| {
                    FatalError::InternalInvariant(
                        "worker_id not set (handshake not completed)".into(),
                    )
                })?;
                // Borrow separate fields to avoid conflicting mutable/immutable borrows on self.
                let activator_runtime = self.activator_runtime.as_ref();
                let ns = self.namespaces.get_mut(&namespace_id).ok_or_else(|| {
                    FatalError::InternalInvariant(format!("namespace '{}' not found", namespace_id))
                })?;
                let pending_events =
                    ns.endpoint_sync(&namespace_id, endpoints, worker_id, activator_runtime)?;
                for event in pending_events {
                    let _ = self.bg_event_tx.try_send(event);
                }
                Ok(())
            }
            WorkerCommand::EndpointUpdate {
                namespace_id,
                upserted,
                removed_ips,
            } => {
                log::info!(
                    "worker: received EndpointUpdate for ns='{}' upserted=[{}] removed=[{}]",
                    namespace_id,
                    upserted.iter().map(|e| format!("{}", e.ip)).collect::<Vec<_>>().join(", "),
                    removed_ips.iter().map(|ip| format!("{}", ip)).collect::<Vec<_>>().join(", ")
                );
                let worker_id = self.worker_id.clone().ok_or_else(|| {
                    FatalError::InternalInvariant(
                        "worker_id not set (handshake not completed)".into(),
                    )
                })?;
                let activator_runtime = self.activator_runtime.as_ref();
                let ns = self.namespaces.get_mut(&namespace_id).ok_or_else(|| {
                    FatalError::InternalInvariant(format!("namespace '{}' not found", namespace_id))
                })?;
                let pending_events = ns.endpoint_update(
                    &namespace_id,
                    upserted,
                    removed_ips,
                    worker_id,
                    activator_runtime,
                )?;
                for event in pending_events {
                    let _ = self.bg_event_tx.try_send(event);
                }
                Ok(())
            }
            WorkerCommand::Shutdown => {
                // Handled in the main loop; should not reach here.
                unreachable!("Shutdown handled in run()")
            }
        }
    }

    /// Look up a namespace by ID, returning FatalError if not found.
    fn get_namespace_mut(
        &mut self,
        namespace_id: &NamespaceId,
    ) -> Result<&mut NamespaceState, FatalError> {
        self.namespaces.get_mut(namespace_id).ok_or_else(|| {
            FatalError::InternalInvariant(format!("namespace '{}' not found", namespace_id))
        })
    }

    async fn handle_create_namespace(
        &mut self,
        namespace_id: NamespaceId,
        network: NetworkConfig,
    ) -> Result<(), FatalError> {
        let segment_id = network.segment_id;

        let (ns, event) = NamespaceState::new(
            &self.worker_token,
            &self.bg_event_tx,
            &self.adapter_manager,
            &self.gateway_provider,
            &namespace_id,
            network,
        )
        .await?;

        // Notify tunnel manager if this namespace has a segment_id.
        if let Some(seg) = segment_id {
            if let Some(tm) = &mut self.tunnel_manager {
                tm.on_namespace_created(&namespace_id, seg, &ns.fabric);
            }
        }

        self.namespaces.insert(namespace_id, ns);

        // Send via background event channel — handlers never touch conn directly.
        send_event(&self.bg_event_tx, event).await;
        Ok(())
    }

    async fn handle_destroy_namespace(
        &mut self,
        namespace_id: &NamespaceId,
    ) -> Result<(), FatalError> {
        if let Some(ns) = self.namespaces.remove(namespace_id) {
            // Notify tunnel manager before dropping the namespace.
            if let Some(seg) = ns.segment_id {
                if let Some(tm) = &mut self.tunnel_manager {
                    tm.on_namespace_destroyed(seg);
                }
            }
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
        resources: Option<distvirt_worker_protocol::ResourceRequirements>,
        volumes: Vec<distvirt_worker_protocol::VolumeSpec>,
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
        let ns_id = namespace_id.clone();
        let pid = pod_id.clone();
        let cancel_clone = pod_cancel.clone();
        let activity = Arc::clone(&self.activity);

        let (suspend_tx, suspend_rx) = mpsc::channel(1);

        let supervisor = TaskHandle::spawn(async move {
            pod_supervisor::<V, P, F>(
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
                resources,
                volumes,
                suspend_rx,
                activity,
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

        if let Some(mut pod) = ns.pods.remove(pod_id) {
            if graceful {
                // Cancel the pod's token to trigger graceful shutdown in supervisor.
                // The supervisor will SIGTERM containers, wait for exit, then shut
                // down the VM cleanly.
                pod.cancel.cancel();
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
                        log::warn!("worker: pod '{}' graceful stop timed out, aborting", pod_id);
                        // pod.supervisor (TaskHandle) drops here, automatically aborting.
                    }
                }
            } else {
                // Non-graceful: abort the supervisor immediately. This kills the
                // VM process via Drop (SIGKILL) without attempting graceful
                // container shutdown.
                pod.supervisor.abort();
                // Brief window for the abort to propagate and process cleanup
                // (e.g. TAP device teardown needs the process gone first).
                match tokio::time::timeout(FORCE_STOP_TIMEOUT, &mut pod.supervisor).await {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) => {} // JoinError from abort — expected
                    Err(_) => {
                        log::warn!("worker: pod '{}' force stop cleanup timed out", pod_id);
                        // TaskHandle drops here, ensuring abort.
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
        artifact_id: ArtifactId,
        pool_id: PoolId,
    ) -> Result<(), FatalError> {
        let pool_base = match self.pool_path(&pool_id) {
            Some(p) => p.clone(),
            None => {
                send_event(
                    &self.bg_event_tx,
                    WorkerEvent::PodSuspendFailed {
                        namespace_id: namespace_id.clone(),
                        pod_id: pod_id.clone(),
                        error: format!("unknown pool '{}'", pool_id),
                    },
                )
                .await;
                return Ok(());
            }
        };
        let snapshot_dir = pool_base.join(artifact_id.as_ref());

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
            artifact_id: artifact_id.clone(),
            snapshot_dir,
            pool_id,
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
        artifact_id: ArtifactId,
        network: PodNetworkConfig,
        pool_id: PoolId,
    ) -> Result<(), FatalError> {
        let pool_base = match self.pool_path(&pool_id) {
            Some(p) => p.clone(),
            None => {
                send_event(
                    &self.bg_event_tx,
                    WorkerEvent::PodFailed {
                        namespace_id: namespace_id.clone(),
                        pod_id: pod_id.clone(),
                        error: format!("unknown pool '{}'", pool_id),
                    },
                )
                .await;
                return Ok(());
            }
        };

        let ns = self.namespaces.get_mut(namespace_id).ok_or_else(|| {
            FatalError::InternalInvariant(format!("namespace '{}' not found", namespace_id))
        })?;

        // Load snapshot metadata.
        let snapshot_dir = pool_base.join(artifact_id.as_ref());
        let metadata_path = snapshot_dir.join("metadata.json");
        let metadata_bytes = match F::read(&metadata_path).await {
            Ok(bytes) => bytes,
            Err(e) => {
                send_event(
                    &self.bg_event_tx,
                    WorkerEvent::PodFailed {
                        namespace_id: namespace_id.clone(),
                        pod_id: pod_id.clone(),
                        error: format!(
                            "failed to read snapshot metadata at {}: {}",
                            metadata_path.display(),
                            e
                        ),
                    },
                )
                .await;
                return Ok(());
            }
        };
        let metadata: crate::vmm::SnapshotMetadata = match serde_json::from_slice(&metadata_bytes) {
            Ok(m) => m,
            Err(e) => {
                send_event(
                    &self.bg_event_tx,
                    WorkerEvent::PodFailed {
                        namespace_id: namespace_id.clone(),
                        pod_id: pod_id.clone(),
                        error: format!("invalid snapshot metadata: {}", e),
                    },
                )
                .await;
                return Ok(());
            }
        };

        let snapshot = crate::vmm::SnapshotArtifacts {
            snapshot_dir,
            metadata,
        };

        let pod_cancel = ns.token.child_token();
        let event_tx = self.bg_event_tx.clone();
        let vmm = Arc::clone(&self.vmm);
        let image_provider = Arc::clone(&self.image_provider);
        let fabric = Arc::clone(&ns.fabric);
        let ns_id = namespace_id.clone();
        let pid = pod_id.clone();
        let cancel_clone = pod_cancel.clone();
        let activity = Arc::clone(&self.activity);

        let (suspend_tx, suspend_rx) = mpsc::channel(1);

        let supervisor = TaskHandle::spawn(async move {
            pod_resume_supervisor::<V, P, F>(
                vmm,
                image_provider,
                fabric,
                cancel_clone,
                event_tx,
                ns_id,
                pid,
                network,
                snapshot,
                suspend_rx,
                activity,
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

    async fn handle_transfer_artifact(
        &mut self,
        transfer_id: u64,
        source_artifact_id: ArtifactId,
        source_pool_id: PoolId,
        dest_artifact_id: ArtifactId,
        dest_pool_id: PoolId,
        dest_endpoint: Option<String>,
    ) -> Result<(), FatalError> {
        let source_base = match self.pool_path(&source_pool_id) {
            Some(p) => p.clone(),
            None => {
                send_event(
                    &self.bg_event_tx,
                    WorkerEvent::TransferFailed {
                        transfer_id,
                        source_artifact_id,
                        source_pool_id,
                        dest_artifact_id,
                        dest_pool_id,
                        error: "unknown source pool".to_string(),
                    },
                )
                .await;
                return Ok(());
            }
        };
        let source_dir = source_base.join(source_artifact_id.as_ref());

        if let Some(endpoint) = dest_endpoint {
            // Remote transfer: spawn background task to stream artifact via TCP.
            let tx = self.bg_event_tx.clone();
            let sa = source_artifact_id.clone();
            let sp = source_pool_id.clone();
            let da = dest_artifact_id.clone();
            let dp = dest_pool_id.clone();
            tokio::spawn(async move {
                log::info!(
                    "artifact transfer: sending transfer_id={} {}:{} -> {} ({}:{})",
                    transfer_id,
                    sp,
                    sa,
                    endpoint,
                    dp,
                    da,
                );
                if let Err(e) = artifact_transfer::send_artifact(
                    &endpoint,
                    transfer_id,
                    &sa,
                    &sp,
                    &da,
                    &dp,
                    &source_dir,
                )
                .await
                {
                    log::error!("artifact transfer: send failed: {:#}", e);
                    send_event(
                        &tx,
                        WorkerEvent::TransferFailed {
                            transfer_id,
                            source_artifact_id: sa,
                            source_pool_id: sp,
                            dest_artifact_id: da,
                            dest_pool_id: dp,
                            error: format!("{:#}", e),
                        },
                    )
                    .await;
                }
                // No success event from source — dest emits ArtifactTransferReceived.
            });
        } else {
            // Local copy: resolve dest pool, copy files.
            let dest_base = match self.pool_path(&dest_pool_id) {
                Some(p) => p.clone(),
                None => {
                    send_event(
                        &self.bg_event_tx,
                        WorkerEvent::TransferFailed {
                            transfer_id,
                            source_artifact_id,
                            source_pool_id,
                            dest_artifact_id,
                            dest_pool_id,
                            error: "unknown dest pool".to_string(),
                        },
                    )
                    .await;
                    return Ok(());
                }
            };
            let dest_dir = dest_base.join(dest_artifact_id.as_ref());
            let tx = self.bg_event_tx.clone();
            let sa = source_artifact_id.clone();
            let sp = source_pool_id.clone();
            let da = dest_artifact_id.clone();
            let dp = dest_pool_id.clone();
            tokio::spawn(async move {
                match artifact_transfer::local_pool_copy(&source_dir, &dest_dir).await {
                    Ok(size_bytes) => {
                        log::info!(
                            "artifact transfer: local copy transfer_id={} done, {} bytes",
                            transfer_id,
                            size_bytes,
                        );
                        send_event(
                            &tx,
                            WorkerEvent::ArtifactTransferReceived {
                                transfer_id,
                                source_artifact_id: sa,
                                source_pool_id: sp,
                                dest_artifact_id: da,
                                dest_pool_id: dp,
                                size_bytes,
                            },
                        )
                        .await;
                    }
                    Err(e) => {
                        log::error!("artifact transfer: local copy failed: {:#}", e);
                        send_event(
                            &tx,
                            WorkerEvent::TransferFailed {
                                transfer_id,
                                source_artifact_id: sa,
                                source_pool_id: sp,
                                dest_artifact_id: da,
                                dest_pool_id: dp,
                                error: format!("{:#}", e),
                            },
                        )
                        .await;
                    }
                }
            });
        }

        Ok(())
    }

    async fn handle_delete_artifact(
        &self,
        artifact_id: &ArtifactId,
        pool_id: &PoolId,
    ) -> Result<(), FatalError> {
        let pool_base = match self.pool_path(pool_id) {
            Some(p) => p,
            None => {
                log::warn!("delete_artifact: unknown pool '{}', ignoring", pool_id);
                return Ok(());
            }
        };
        let snapshot_dir = pool_base.join(artifact_id.as_ref());
        if snapshot_dir.exists() {
            if let Err(e) = F::remove_dir_all(&snapshot_dir).await {
                log::error!(
                    "delete_artifact: failed to remove {}: {}",
                    snapshot_dir.display(),
                    e
                );
            } else {
                log::info!("delete_artifact: removed {}", snapshot_dir.display());
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
        ContainerConfig, ContainerSpec, PodNetworkConfig, RegistryEntry,
    };
    use tokio::net::UnixStream;

    use crate::fabric::{Fabric, FabricPort};
    use crate::image_provider::{ImageProvider, PreparedArtifact};
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
        fn take_fabric_port(&mut self) -> Option<FabricPort> {
            None
        }
        async fn wait(&mut self) -> anyhow::Result<std::process::ExitStatus> {
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

    fn make_worker()
    -> Worker<StubVmm, StubImageProvider, crate::sim_traffic::SimGatewayProvider, crate::fs::SyncFs>
    {
        Worker::<_, _, _, crate::fs::SyncFs>::new(
            PathBuf::from("/fake/kernel"),
            PathBuf::from("/fake/rootfs"),
            StubVmm,
            StubImageProvider,
            None, // no activator component dir
            String::new(),
            crate::sim_traffic::SimGatewayProvider::new(),
            Arc::new(distvirt_common::ActivityTracker::new()),
        )
    }

    /// Inject a NamespaceState directly into the worker, bypassing
    /// handle_create_namespace (which requires root for TUN/gateway).
    fn inject_namespace(
        worker: &mut Worker<
            StubVmm,
            StubImageProvider,
            crate::sim_traffic::SimGatewayProvider,
            crate::fs::SyncFs,
        >,
        ns_id: &str,
    ) {
        let fabric = Fabric::<FabricPort>::new(Ipv4Addr::new(172, 16, 0, 0), 16);
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

        worker.namespaces.insert(NamespaceId::from(ns_id), ns);
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
                command: Some(vec!["/bin/echo".to_string()]),
                args: Some(vec!["hello".to_string()]),
                env: vec![],
                working_dir: None,
                user: None,
                hostname: None,
                capture_output: false,
                stdin: false,
                volume_mounts: vec![],
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
            .handle_stop_pod(&NamespaceId::from("nope"), &PodId(11), true)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn stop_pod_noop_for_missing_pod() {
        let mut w = make_worker();
        inject_namespace(&mut w, "ns1");

        // Stopping a pod that doesn't exist should succeed (no-op).
        w.handle_stop_pod(&NamespaceId::from("ns1"), &PodId(11), true)
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
        fn take_fabric_port(&mut self) -> Option<FabricPort> {
            None
        }
        async fn wait(&mut self) -> anyhow::Result<std::process::ExitStatus> {
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
        let mut w = Worker::<_, _, _, crate::fs::SyncFs>::new(
            PathBuf::from("/fake/kernel"),
            PathBuf::from("/fake/rootfs"),
            vmm,
            FailingImageProvider {
                error_msg: "intentional".to_string(),
            },
            None,
            String::new(),
            crate::sim_traffic::SimGatewayProvider::new(),
            Arc::new(distvirt_common::ActivityTracker::new()),
        );

        // Inject namespace manually.
        {
            let fabric = Fabric::<FabricPort>::new(Ipv4Addr::new(172, 16, 0, 0), 16);
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
            PodId(11),
            make_pod_network(),
            make_containers(),
            None,
            vec![],
            &log_opener,
        )
        .await
        .unwrap();

        // Pod should be registered.
        let ns = w.namespaces.get(&NamespaceId::from("ns1")).unwrap();
        assert!(ns.pods.contains_key(&PodId(11)));
    }
}
