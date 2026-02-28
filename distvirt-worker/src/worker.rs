use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use anyhow::Context;
use distvirt_worker_protocol::{
    ContainerSpec, LogStreamHeader, PodNetworkConfig, RegistryEntry,
    WorkerCommand, WorkerConnection, WorkerEvent,
};
use futures_lite::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::fabric::{DnsRegistry, Fabric, FabricGateway};
use crate::image_provider::ImageProvider;
use crate::io_session::IoEvent;
use crate::managed_vm::{ImageOverrides, ManagedVm, merge_config};
use crate::vmm::{NetConfig, VmConfig, VmInstance, Vmm};

/// Per-pod state: the VM, background tasks, container tracking.
struct PodState {
    _exit_task: JoinHandle<()>,
}

/// Per-namespace state: fabric, gateway, registry, and pods.
struct NamespaceState {
    fabric: Fabric,
    _gateway_task: JoinHandle<()>,
    registry: DnsRegistry,
    pods: HashMap<String, PodState>,
}

/// The worker: sits between the orchestrator and the raw VM/fabric primitives.
///
/// Receives `WorkerCommand`s via a `WorkerConnection`, sends `WorkerEvent`s back,
/// and opens yamux log streams for container output.
pub struct Worker<V: Vmm, P: ImageProvider> {
    kernel_path: PathBuf,
    rootfs_image_path: PathBuf,
    namespaces: HashMap<String, NamespaceState>,
    vmm: V,
    image_provider: P,
    /// Channel for background tasks to send events back to the main loop.
    bg_event_tx: mpsc::Sender<WorkerEvent>,
    bg_event_rx: mpsc::Receiver<WorkerEvent>,
}

impl<V: Vmm, P: ImageProvider> Worker<V, P> {
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
            vmm,
            image_provider,
            bg_event_tx,
            bg_event_rx,
        }
    }

    /// Run the worker main loop: receive commands, dispatch them,
    /// and forward background events to the orchestrator.
    pub async fn run(mut self, mut conn: WorkerConnection) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                cmd_result = conn.recv_command() => {
                    let cmd = cmd_result?;
                    self.handle_command(cmd, &mut conn).await?;
                }
                Some(event) = self.bg_event_rx.recv() => {
                    conn.send_event(&event).await?;
                }
            }
        }
    }

    /// Handle a single command.
    async fn handle_command(
        &mut self,
        cmd: WorkerCommand,
        conn: &mut WorkerConnection,
    ) -> anyhow::Result<()> {
        match cmd {
            WorkerCommand::CreateNamespace {
                namespace_id,
                network: _network,
            } => {
                self.handle_create_namespace(namespace_id, conn).await
            }
            WorkerCommand::DestroyNamespace { namespace_id } => {
                self.handle_destroy_namespace(&namespace_id).await
            }
            WorkerCommand::RegistrySync {
                namespace_id,
                entries,
            } => {
                self.handle_registry_sync(&namespace_id, entries)
            }
            WorkerCommand::LaunchPod {
                namespace_id,
                pod_id,
                network,
                containers,
            } => {
                self.handle_launch_pod(
                    &namespace_id,
                    pod_id,
                    network,
                    containers,
                    conn,
                )
                .await
            }
            WorkerCommand::StopPod {
                namespace_id,
                pod_id,
                graceful: _,
            } => {
                self.handle_stop_pod(&namespace_id, &pod_id).await
            }
        }
    }

    async fn handle_create_namespace(
        &mut self,
        namespace_id: String,
        conn: &mut WorkerConnection,
    ) -> anyhow::Result<()> {
        let mut fabric = Fabric::new();

        let registry: DnsRegistry = Arc::new(RwLock::new(HashMap::new()));

        let (gateway, egress_tx, ingress_rx) =
            FabricGateway::new(Arc::clone(&registry)).context("create fabric gateway")?;
        fabric.set_gateway(egress_tx, ingress_rx);
        let gateway_task = tokio::spawn(gateway.run());

        log::info!("worker: created namespace '{}' with fabric + gateway", namespace_id);

        let ns = NamespaceState {
            fabric,
            _gateway_task: gateway_task,
            registry,
            pods: HashMap::new(),
        };

        self.namespaces.insert(namespace_id.clone(), ns);

        // Send directly on the connection (we're in the main loop context).
        conn.send_event(&WorkerEvent::NamespaceCreated { namespace_id }).await?;
        Ok(())
    }

    async fn handle_destroy_namespace(&mut self, namespace_id: &str) -> anyhow::Result<()> {
        if let Some(ns) = self.namespaces.remove(namespace_id) {
            ns._gateway_task.abort();
            drop(ns);
            log::info!("worker: destroyed namespace '{}'", namespace_id);
        }
        Ok(())
    }

    fn handle_registry_sync(
        &mut self,
        namespace_id: &str,
        entries: Vec<RegistryEntry>,
    ) -> anyhow::Result<()> {
        let ns = self
            .namespaces
            .get_mut(namespace_id)
            .context("namespace not found")?;

        let mut map = ns.registry.write().map_err(|e| anyhow::anyhow!("registry lock poisoned: {}", e))?;
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
        conn: &mut WorkerConnection,
    ) -> anyhow::Result<()> {
        let container = containers
            .into_iter()
            .next()
            .context("pod must have at least one container")?;

        let artifact = self.image_provider
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
            kernel_path: self.kernel_path.clone(),
            rootfs_image_path: self.rootfs_image_path.clone(),
            container_image_path: artifact.image_path.clone(),
            vcpu_count: 1,
            mem_size_mib: 128,
            net: Some(net_config.clone()),
            serial_console: true,
        };

        let mut instance = self.vmm.launch(&vm_config).await.context("launch VM")?;
        log::info!("worker: pod '{}' VM launched", pod_id);

        let ns = self
            .namespaces
            .get_mut(namespace_id)
            .context("namespace not found")?;

        if let Some(tap) = instance.take_tap() {
            let tap_name = tap.name.clone();
            ns.fabric
                .add_port(tap)
                .map_err(|e| anyhow::anyhow!("fabric add_port for {}: {}", tap_name, e))?;
            log::info!("worker: pod '{}' TAP {} added to fabric", pod_id, tap_name);
        }

        let mut vm = ManagedVm::connect(instance).await?;

        vm.configure_network("eth0", &net_config).await?;

        let dns_servers = vec![network.gateway.to_string()];

        let container_id = &container.container_id;
        vm.add_container(container_id, "/dev/vdb", &dns_servers)
            .await?;

        vm.start_container(container_id, &config).await?;

        let ns_id = namespace_id.to_string();
        let pid = pod_id.clone();
        let cid = container_id.to_string();

        // Set up log streaming via yamux log streams.
        let log_opener = conn.log_stream_opener();
        let io_session = if config.capture_output {
            match vm.accept_output_stream().await {
                Ok((_cid, session)) => {
                    let header = LogStreamHeader {
                        namespace_id: ns_id.clone(),
                        pod_id: pid.clone(),
                        container_id: cid.clone(),
                    };
                    match log_opener.open_log_stream(&header).await {
                        Ok(log_stream) => Some((session, log_stream)),
                        Err(e) => {
                            log::error!("pod '{}': failed to open log stream: {:#}", pid, e);
                            conn.send_event(&WorkerEvent::PodLogStreamError {
                                namespace_id: ns_id.clone(),
                                pod_id: pid.clone(),
                                container_id: cid.clone(),
                                phase: "open_stream".to_string(),
                                error: format!("{:#}", e),
                            }).await?;
                            None
                        }
                    }
                }
                Err(e) => {
                    log::error!("pod '{}': failed to accept output stream: {:#}", pid, e);
                    conn.send_event(&WorkerEvent::PodLogStreamError {
                        namespace_id: ns_id.clone(),
                        pod_id: pid.clone(),
                        container_id: cid.clone(),
                        phase: "connect".to_string(),
                        error: format!("{:#}", e),
                    }).await?;
                    None
                }
            }
        } else {
            None
        };

        // Emit PodRunning event.
        conn.send_event(&WorkerEvent::PodRunning {
            namespace_id: ns_id.clone(),
            pod_id: pid.clone(),
        }).await?;

        // Background tasks use the internal channel to send events.
        let event_tx = self.bg_event_tx.clone();

        let exit_task = tokio::spawn(async move {
            // Stream logs via the yamux log stream.
            if let Some((mut session, mut log_stream)) = io_session {
                tokio::spawn(async move {
                    loop {
                        match session.next_event().await {
                            Ok(IoEvent::Stdout(data)) | Ok(IoEvent::Stderr(data)) => {
                                if log_stream.write_all(&data).await.is_err() {
                                    break;
                                }
                            }
                            Ok(IoEvent::Eof) => break,
                            Err(e) => {
                                log::warn!("pod log stream error: {:#}", e);
                                break;
                            }
                        }
                    }
                    let _ = log_stream.close().await;
                });
            }

            // Wait for the container to exit.
            let result = vm.wait_container_exit().await;
            match result {
                Ok((_container_id, exit_code)) => {
                    let _ = vm.shutdown().await;
                    let _ = event_tx
                        .send(WorkerEvent::PodExited {
                            namespace_id: ns_id,
                            pod_id: pid,
                            exit_code,
                        })
                        .await;
                }
                Err(e) => {
                    let _ = event_tx
                        .send(WorkerEvent::PodFailed {
                            namespace_id: ns_id,
                            pod_id: pid,
                            error: format!("{:#}", e),
                        })
                        .await;
                }
            }
        });

        ns.pods.insert(pod_id, PodState { _exit_task: exit_task });

        Ok(())
    }

    async fn handle_stop_pod(
        &mut self,
        namespace_id: &str,
        pod_id: &str,
    ) -> anyhow::Result<()> {
        let ns = self
            .namespaces
            .get_mut(namespace_id)
            .context("namespace not found")?;

        if let Some(pod) = ns.pods.remove(pod_id) {
            pod._exit_task.abort();
            log::info!("worker: stopped pod '{}' in namespace '{}'", pod_id, namespace_id);
        }

        Ok(())
    }
}
