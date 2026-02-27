use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use anyhow::Context;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::fabric::dns::DnsRegistry;
use crate::fabric::gateway::FabricGateway;
use crate::fabric::Fabric;
use crate::image_provider::ImageProvider;
use crate::orchestrate::{ContainerConfig, ImageOverrides, ManagedVm, merge_config};
use crate::vmm::{NetConfig, VmConfig, VmInstance, Vmm};

// ---------------------------------------------------------------------------
// Step 1: WorkerCommand and WorkerEvent types
// ---------------------------------------------------------------------------

/// Network configuration for a namespace.
pub struct NetworkConfig {
    pub subnet: Ipv4Addr,
    pub gateway: Ipv4Addr,
    pub prefix_len: u8,
}

/// Network configuration for a single pod within a namespace.
pub struct PodNetworkConfig {
    pub ip: Ipv4Addr,
    pub mac: [u8; 6],
    pub gateway: Ipv4Addr,
    pub netmask: String,
}

/// Specification for a container within a pod.
pub struct ContainerSpec {
    pub container_id: String,
    pub image_ref: String,
    pub config: ContainerConfig,
}

/// A service registry entry (name -> IP).
pub struct RegistryEntry {
    pub name: String,
    pub ip: Ipv4Addr,
}

/// Output stream identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputStream {
    Stdout,
    Stderr,
}

/// Commands sent from the orchestrator to the worker.
pub enum WorkerCommand {
    CreateNamespace {
        namespace_id: String,
        network: NetworkConfig,
    },
    DestroyNamespace {
        namespace_id: String,
    },
    RegistrySync {
        namespace_id: String,
        entries: Vec<RegistryEntry>,
    },
    LaunchPod {
        namespace_id: String,
        pod_id: String,
        network: PodNetworkConfig,
        containers: Vec<ContainerSpec>,
    },
    StopPod {
        namespace_id: String,
        pod_id: String,
        graceful: bool,
    },
}

/// Events emitted by the worker back to the orchestrator.
#[derive(Debug)]
pub enum WorkerEvent {
    NamespaceCreated {
        namespace_id: String,
    },
    PodRunning {
        namespace_id: String,
        pod_id: String,
    },
    PodExited {
        namespace_id: String,
        pod_id: String,
        exit_code: i32,
    },
    PodFailed {
        namespace_id: String,
        pod_id: String,
        error: String,
    },
    PodOutput {
        namespace_id: String,
        pod_id: String,
        container_id: String,
        stream: OutputStream,
        data: Vec<u8>,
    },
}

// ---------------------------------------------------------------------------
// Step 2: Worker struct and internal state
// ---------------------------------------------------------------------------

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
pub struct Worker {
    kernel_path: PathBuf,
    rootfs_image_path: PathBuf,
    namespaces: HashMap<String, NamespaceState>,
    event_tx: mpsc::Sender<WorkerEvent>,
}

impl Worker {
    pub fn new(
        kernel_path: PathBuf,
        rootfs_image_path: PathBuf,
        event_tx: mpsc::Sender<WorkerEvent>,
    ) -> Self {
        Worker {
            kernel_path,
            rootfs_image_path,
            namespaces: HashMap::new(),
            event_tx,
        }
    }

    /// Handle a single command. This is the main dispatch point.
    pub async fn handle_command(
        &mut self,
        cmd: WorkerCommand,
        vmm: &(impl Vmm + ?Sized),
        image_provider: &(impl ImageProvider + ?Sized),
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
                    vmm,
                    image_provider,
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
        vmm: &(impl Vmm + ?Sized),
        image_provider: &(impl ImageProvider + ?Sized),
        namespace_id: &str,
        pod_id: String,
        network: PodNetworkConfig,
        containers: Vec<ContainerSpec>,
    ) -> anyhow::Result<()> {
        let ns = self
            .namespaces
            .get_mut(namespace_id)
            .context("namespace not found")?;

        // For each container, prepare its image.
        // Currently we support one container per pod (one VM = one container).
        let container = containers
            .into_iter()
            .next()
            .context("pod must have at least one container")?;

        let artifact = image_provider
            .prepare(&container.image_ref)
            .await
            .context("preparing image")?;

        // Resolve the final container config: merge OCI config with spec overrides.
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
            merge_config(oci_config, &overrides)?
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
        };

        // Launch the VM.
        let mut instance = vmm.launch(&vm_config).await.context("launch VM")?;
        log::info!("worker: pod '{}' VM launched", pod_id);

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
        let io_session = vm.stream_logs(container_id).await.ok();

        // Emit PodRunning event.
        let event_tx = self.event_tx.clone();
        let ns_id = namespace_id.to_string();
        let pid = pod_id.clone();

        let _ = event_tx
            .send(WorkerEvent::PodRunning {
                namespace_id: ns_id.clone(),
                pod_id: pid.clone(),
            })
            .await;

        // Spawn background tasks for log streaming and exit waiting.
        let cid = container_id.to_string();
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

// ---------------------------------------------------------------------------
// Step 5: Local orchestrator loop
// ---------------------------------------------------------------------------

/// Configuration for the local orchestrator.
pub struct LocalOrchestratorConfig {
    pub kernel_path: PathBuf,
    pub rootfs_image_path: PathBuf,
}

/// Run a deployment locally using an in-process worker.
///
/// This is the "embedded orchestrator" from the worker protocol doc's Local Mode.
/// It takes a `Deployment`, plans it, then sequences commands to the worker:
/// 1. CreateNamespace
/// 2. RegistrySync with all service name→IP mappings
/// 3. LaunchPod for each service (in dependency order)
/// 4. Stream PodOutput events to the LogCollector
/// 5. Wait for all pods to exit
pub async fn run_deployment(
    config: &LocalOrchestratorConfig,
    deployment: &crate::deployment::Deployment,
    vmm: &(impl Vmm + ?Sized),
    image_provider: &(impl ImageProvider + ?Sized),
) -> anyhow::Result<()> {
    let plan = crate::deployment::plan(deployment)?;
    let namespace_id = deployment.name.clone();

    // Create worker with event channel.
    let (event_tx, mut event_rx) = mpsc::channel::<WorkerEvent>(256);
    let mut worker = Worker::new(
        config.kernel_path.clone(),
        config.rootfs_image_path.clone(),
        event_tx,
    );

    // 1. Create namespace.
    worker
        .handle_command(
            WorkerCommand::CreateNamespace {
                namespace_id: namespace_id.clone(),
                network: NetworkConfig {
                    subnet: Ipv4Addr::new(172, 16, 0, 0),
                    gateway: Ipv4Addr::new(172, 16, 0, 1),
                    prefix_len: 24,
                },
            },
            vmm,
            image_provider,
        )
        .await
        .context("create namespace")?;

    // 2. Registry sync: build service name → IP mappings from the plan.
    let registry_entries: Vec<RegistryEntry> = plan
        .services
        .iter()
        .map(|s| RegistryEntry {
            name: s.name.clone(),
            ip: s.ip,
        })
        .collect();

    worker
        .handle_command(
            WorkerCommand::RegistrySync {
                namespace_id: namespace_id.clone(),
                entries: registry_entries,
            },
            vmm,
            image_provider,
        )
        .await
        .context("registry sync")?;

    // 3. Launch pods for each service in dependency order.
    let total_pods = plan.services.len();
    for planned in &plan.services {
        let service_spec = deployment
            .services
            .get(&planned.name)
            .context("planned service not in deployment")?;

        // Build ContainerConfig from the service spec.
        let container_config = ContainerConfig {
            entrypoint: service_spec
                .entrypoint
                .as_ref()
                .and_then(|ep| ep.first().cloned())
                .unwrap_or_default(),
            args: service_spec
                .entrypoint
                .as_ref()
                .map(|ep| ep.get(1..).unwrap_or_default().to_vec())
                .unwrap_or_default()
                .into_iter()
                .chain(
                    service_spec
                        .command
                        .as_ref()
                        .cloned()
                        .unwrap_or_default(),
                )
                .collect(),
            env: service_spec
                .environment
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect(),
            working_dir: service_spec.working_dir.clone(),
            uid: None,
            gid: None,
            hostname: service_spec.hostname.clone(),
            capture_output: true,
        };

        let container_spec = ContainerSpec {
            container_id: planned.name.clone(),
            image_ref: service_spec.image.clone(),
            config: container_config,
        };

        worker
            .handle_command(
                WorkerCommand::LaunchPod {
                    namespace_id: namespace_id.clone(),
                    pod_id: planned.name.clone(),
                    network: PodNetworkConfig {
                        ip: planned.ip,
                        mac: planned.mac,
                        gateway: Ipv4Addr::new(172, 16, 0, 1),
                        netmask: "255.255.255.0".to_string(),
                    },
                    containers: vec![container_spec],
                },
                vmm,
                image_provider,
            )
            .await
            .with_context(|| format!("launch pod '{}'", planned.name))?;
    }

    // 4. Event loop: stream output and wait for pods to exit.
    let mut exited_count = 0;
    let mut _last_exit_code = 0;

    while let Some(event) = event_rx.recv().await {
        match event {
            WorkerEvent::NamespaceCreated { namespace_id } => {
                log::info!("namespace '{}' created", namespace_id);
            }
            WorkerEvent::PodRunning {
                namespace_id: _,
                pod_id,
            } => {
                log::info!("pod '{}' is running", pod_id);
            }
            WorkerEvent::PodOutput {
                namespace_id: _,
                pod_id,
                container_id: _,
                stream,
                data,
            } => {
                let stream_name = match stream {
                    OutputStream::Stdout => "stdout",
                    OutputStream::Stderr => "stderr",
                };
                // Print with service prefix.
                if let Ok(text) = std::str::from_utf8(&data) {
                    for line in text.lines() {
                        println!("{} | {}", pod_id, line);
                    }
                } else {
                    log::debug!(
                        "pod '{}' {}: {} bytes (binary)",
                        pod_id,
                        stream_name,
                        data.len()
                    );
                }
            }
            WorkerEvent::PodExited {
                namespace_id: _,
                pod_id,
                exit_code,
            } => {
                log::info!("pod '{}' exited with code {}", pod_id, exit_code);
                _last_exit_code = exit_code;
                exited_count += 1;
                if exited_count >= total_pods {
                    break;
                }
            }
            WorkerEvent::PodFailed {
                namespace_id: _,
                pod_id,
                error,
            } => {
                log::error!("pod '{}' failed: {}", pod_id, error);
                exited_count += 1;
                if exited_count >= total_pods {
                    break;
                }
            }
        }
    }

    // 5. Clean up: destroy namespace.
    worker
        .handle_command(
            WorkerCommand::DestroyNamespace {
                namespace_id: namespace_id.clone(),
            },
            vmm,
            image_provider,
        )
        .await
        .context("destroy namespace")?;

    Ok(())
}
