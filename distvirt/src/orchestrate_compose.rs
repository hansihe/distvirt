use std::net::Ipv4Addr;

use anyhow::Context;
use tokio::sync::mpsc;

use crate::deployment::{Deployment, ServiceSpec};
use crate::image_provider::ImageProvider;
use crate::orchestrate::ContainerConfig;
use crate::protocol::{
    ContainerSpec, NetworkConfig, OutputStream, PodNetworkConfig, RegistryEntry, WorkerCommand,
    WorkerEvent,
};
use crate::vmm::Vmm;
use crate::worker::Worker;

/// Build a [`ContainerConfig`] from a compose [`ServiceSpec`].
fn build_container_config(spec: &ServiceSpec) -> ContainerConfig {
    ContainerConfig {
        entrypoint: spec
            .entrypoint
            .as_ref()
            .and_then(|ep| ep.first().cloned())
            .unwrap_or_default(),
        args: spec
            .entrypoint
            .as_ref()
            .map(|ep| ep.get(1..).unwrap_or_default().to_vec())
            .unwrap_or_default()
            .into_iter()
            .chain(
                spec.command
                    .as_ref()
                    .cloned()
                    .unwrap_or_default(),
            )
            .collect(),
        env: spec
            .environment
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect(),
        working_dir: spec.working_dir.clone(),
        uid: None,
        gid: None,
        hostname: spec.hostname.clone(),
        capture_output: true,
    }
}

/// Run a compose deployment end-to-end using the worker protocol.
///
/// Creates a namespace, syncs the DNS registry, launches all pods in dependency
/// order, streams output, waits for all pods to exit, and cleans up the namespace.
pub async fn run_compose<V: Vmm, P: ImageProvider>(
    deployment: &Deployment,
    worker: &mut Worker<V, P>,
    event_rx: &mut mpsc::Receiver<WorkerEvent>,
) -> anyhow::Result<()> {
    let plan = crate::deployment::plan(deployment).context("planning deployment")?;
    let namespace_id = deployment.name.clone();

    // 1. Create namespace.
    worker
        .handle_command(WorkerCommand::CreateNamespace {
            namespace_id: namespace_id.clone(),
            network: NetworkConfig {
                subnet: Ipv4Addr::new(172, 16, 0, 0),
                gateway: Ipv4Addr::new(172, 16, 0, 1),
                prefix_len: 24,
            },
        })
        .await
        .context("create namespace")?;

    // 2. Registry sync.
    let registry_entries: Vec<RegistryEntry> = plan
        .services
        .iter()
        .map(|s| RegistryEntry {
            name: s.name.clone(),
            ip: s.ip,
        })
        .collect();

    worker
        .handle_command(WorkerCommand::RegistrySync {
            namespace_id: namespace_id.clone(),
            entries: registry_entries,
        })
        .await
        .context("registry sync")?;

    // 3. Launch pods for each service in dependency order.
    let total_pods = plan.services.len();
    for planned in &plan.services {
        let service_spec = deployment
            .services
            .get(&planned.name)
            .context("planned service not in deployment")?;

        let container_config = build_container_config(service_spec);
        let container_spec = ContainerSpec {
            container_id: planned.name.clone(),
            image_ref: service_spec.image.clone(),
            config: container_config,
        };

        worker
            .handle_command(WorkerCommand::LaunchPod {
                namespace_id: namespace_id.clone(),
                pod_id: planned.name.clone(),
                network: PodNetworkConfig {
                    ip: planned.ip,
                    mac: planned.mac,
                    gateway: Ipv4Addr::new(172, 16, 0, 1),
                    netmask: "255.255.255.0".to_string(),
                },
                containers: vec![container_spec],
            })
            .await
            .with_context(|| format!("launch pod '{}'", planned.name))?;
    }

    // 4. Event loop: stream output and wait for pods to exit.
    let mut exited_count = 0;

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
            WorkerEvent::PodLogStreamError {
                namespace_id: _,
                pod_id,
                container_id: _,
                phase,
                error,
            } => {
                eprintln!("{} | log stream error ({}): {}", pod_id, phase, error);
            }
        }
    }

    // 5. Clean up: destroy namespace.
    worker
        .handle_command(WorkerCommand::DestroyNamespace {
            namespace_id: namespace_id.clone(),
        })
        .await
        .context("destroy namespace")?;

    Ok(())
}
