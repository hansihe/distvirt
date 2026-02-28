use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use anyhow::Context;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::fabric::{DnsRegistry, Fabric, FabricGateway};
use crate::image_provider::ImageProvider;
use crate::orchestrate::{ImageOverrides, ManagedVm, merge_config};
use crate::protocol::{
    ContainerSpec, OutputStream, PodNetworkConfig, RegistryEntry, WorkerCommand, WorkerEvent,
};
use crate::vmm::{NetConfig, VmConfig, VmInstance, Vmm};

/// Per-pod state: the VM, background tasks, container tracking.
struct PodState {
    /// Background task that waits for the container to exit and emits PodExited/PodFailed.
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
/// Receives `WorkerCommand`s, emits `WorkerEvent`s via the event channel.
/// Manages `NamespaceState` internally.
pub struct Worker<V: Vmm, P: ImageProvider> {
    kernel_path: PathBuf,
    rootfs_image_path: PathBuf,
    namespaces: HashMap<String, NamespaceState>,
    event_tx: mpsc::Sender<WorkerEvent>,
    vmm: V,
    image_provider: P,
}

impl<V: Vmm, P: ImageProvider> Worker<V, P> {
    pub fn new(
        kernel_path: PathBuf,
        rootfs_image_path: PathBuf,
        event_tx: mpsc::Sender<WorkerEvent>,
        vmm: V,
        image_provider: P,
    ) -> Self {
        Worker {
            kernel_path,
            rootfs_image_path,
            namespaces: HashMap::new(),
            event_tx,
            vmm,
            image_provider,
        }
    }

    /// Handle a single command. This is the main dispatch point.
    pub async fn handle_command(
        &mut self,
        cmd: WorkerCommand,
    ) -> anyhow::Result<()> {
        match cmd {
            WorkerCommand::CreateNamespace {
                namespace_id,
                network: _network,
            } => {
                self.handle_create_namespace(namespace_id).await
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

    // -----------------------------------------------------------------------
    // Step 3: Command handlers (refactored from orchestrate::run)
    // -----------------------------------------------------------------------

    async fn handle_create_namespace(
        &mut self,
        namespace_id: String,
    ) -> anyhow::Result<()> {
        // Create the L2 fabric switch.
        let mut fabric = Fabric::new();

        // Create the shared DNS registry.
        let registry: DnsRegistry = Arc::new(RwLock::new(HashMap::new()));

        // Create the gateway and wire it to the fabric.
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

        let _ = self.event_tx.send(WorkerEvent::NamespaceCreated { namespace_id }).await;
        Ok(())
    }

    async fn handle_destroy_namespace(&mut self, namespace_id: &str) -> anyhow::Result<()> {
        if let Some(ns) = self.namespaces.remove(namespace_id) {
            // Dropping the fabric aborts port tasks; dropping gateway_task aborts gateway.
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

        // Replace the entire registry contents.
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
    ) -> anyhow::Result<()> {
        // For each container, prepare its image.
        // Currently we support one container per pod (one VM = one container).
        let container = containers
            .into_iter()
            .next()
            .context("pod must have at least one container")?;

        let artifact = self.image_provider
            .prepare(&container.image_ref)
            .await
            .context("preparing image")?;

        // Resolve the final container config: merge OCI config with spec overrides.
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

        // Build VM config.
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

        // Launch the VM.
        let mut instance = self.vmm.launch(&vm_config).await.context("launch VM")?;
        log::info!("worker: pod '{}' VM launched", pod_id);

        let ns = self
            .namespaces
            .get_mut(namespace_id)
            .context("namespace not found")?;

        // Take the TAP device and add it to the namespace's fabric.
        if let Some(tap) = instance.take_tap() {
            let tap_name = tap.name.clone();
            ns.fabric
                .add_port(tap)
                .map_err(|e| anyhow::anyhow!("fabric add_port for {}: {}", tap_name, e))?;
            log::info!("worker: pod '{}' TAP {} added to fabric", pod_id, tap_name);
        }

        // Connect to guest and wait for ready.
        let mut vm = ManagedVm::connect(instance).await?;

        // Configure network.
        vm.configure_network("eth0", &net_config).await?;

        // Build DNS server list from the gateway IP.
        let dns_servers = vec![network.gateway.to_string()];

        // Add and start the container.
        let container_id = &container.container_id;
        vm.add_container(container_id, "/dev/vdb", &dns_servers)
            .await?;

        vm.start_container(container_id, &config).await?;

        // Set up log streaming.
        let event_tx = self.event_tx.clone();
        let ns_id = namespace_id.to_string();
        let pid = pod_id.clone();
        let cid = container_id.to_string();

        let io_session = if config.capture_output {
            match vm.accept_output_stream().await {
                Ok((_cid, session)) => Some(session),
                Err(e) => {
                    log::error!("pod '{}': failed to accept output stream: {:#}", pid, e);
                    let _ = event_tx
                        .send(WorkerEvent::PodLogStreamError {
                            namespace_id: ns_id.clone(),
                            pod_id: pid.clone(),
                            container_id: cid.clone(),
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

        // Emit PodRunning event.
        let _ = event_tx
            .send(WorkerEvent::PodRunning {
                namespace_id: ns_id.clone(),
                pod_id: pid.clone(),
            })
            .await;

        // Spawn background tasks for log streaming and exit waiting.
        let exit_task = tokio::spawn(async move {
            // Stream logs in a separate task if we have an IO session.
            if let Some(mut session) = io_session {
                let event_tx2 = event_tx.clone();
                let ns_id2 = ns_id.clone();
                let pid2 = pid.clone();
                let cid2 = cid.clone();
                tokio::spawn(async move {
                    loop {
                        match session.next_event().await {
                            Ok(crate::io_session::IoEvent::Stdout(data)) => {
                                let _ = event_tx2
                                    .send(WorkerEvent::PodOutput {
                                        namespace_id: ns_id2.clone(),
                                        pod_id: pid2.clone(),
                                        container_id: cid2.clone(),
                                        stream: OutputStream::Stdout,
                                        data,
                                    })
                                    .await;
                            }
                            Ok(crate::io_session::IoEvent::Stderr(data)) => {
                                let _ = event_tx2
                                    .send(WorkerEvent::PodOutput {
                                        namespace_id: ns_id2.clone(),
                                        pod_id: pid2.clone(),
                                        container_id: cid2.clone(),
                                        stream: OutputStream::Stderr,
                                        data,
                                    })
                                    .await;
                            }
                            Ok(crate::io_session::IoEvent::Eof) => break,
                            Err(e) => {
                                log::warn!("pod '{}' log stream error: {:#}", pid2, e);
                                let _ = event_tx2
                                    .send(WorkerEvent::PodLogStreamError {
                                        namespace_id: ns_id2.clone(),
                                        pod_id: pid2.clone(),
                                        container_id: cid2.clone(),
                                        phase: "streaming".to_string(),
                                        error: format!("{:#}", e),
                                    })
                                    .await;
                                break;
                            }
                        }
                    }
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
