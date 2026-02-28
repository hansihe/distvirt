use anyhow::Context;
use distvirt_worker_protocol::codec::recv_msg;
use distvirt_worker_protocol::{
    ContainerConfig, ContainerSpec, LogStreamHeader, NetworkConfig, OrchestratorConnection,
    PodNetworkConfig, RegistryEntry, ServiceBackend, ServicePolicy, WorkerCommand, WorkerEvent,
};
use futures_lite::io::AsyncReadExt;
use tokio::sync::mpsc;

use crate::deployment::{DEFAULT_GATEWAY, DEFAULT_NETMASK, DEFAULT_PREFIX_LEN, DEFAULT_SUBNET};
use crate::types::{Deployment, ServiceSpec};

/// Build a [`ContainerConfig`] from a compose [`ServiceSpec`].
///
/// Follows Docker semantics for entrypoint/command:
/// - `entrypoint` alone: used as the full command line
/// - `command` alone: used as the full command line (image entrypoint would
///   normally prepend, but that's resolved at the worker/image level)
/// - both: entrypoint is the executable, command supplies the arguments
fn build_container_config(spec: &ServiceSpec) -> ContainerConfig {
    let (entrypoint, args) = match (&spec.entrypoint, &spec.command) {
        (Some(ep), Some(cmd)) => {
            // Both specified: entrypoint[0] is the binary, rest of entrypoint + command are args.
            let binary = ep.first().cloned().unwrap_or_default();
            let mut args: Vec<String> = ep.get(1..).unwrap_or_default().to_vec();
            args.extend(cmd.iter().cloned());
            (binary, args)
        }
        (Some(ep), None) => {
            // Only entrypoint: first element is binary, rest are args.
            let binary = ep.first().cloned().unwrap_or_default();
            let args = ep.get(1..).unwrap_or_default().to_vec();
            (binary, args)
        }
        (None, Some(cmd)) => {
            // Only command: first element is binary, rest are args.
            let binary = cmd.first().cloned().unwrap_or_default();
            let args = cmd.get(1..).unwrap_or_default().to_vec();
            (binary, args)
        }
        (None, None) => {
            // Neither specified: rely on image defaults (empty here).
            (String::new(), Vec::new())
        }
    };

    ContainerConfig {
        entrypoint,
        args,
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

/// A log line received from a container log stream.
pub struct LogLine {
    pub pod_id: String,
    pub data: Vec<u8>,
}

/// Run a compose deployment end-to-end using the worker protocol over a connection.
///
/// Creates a namespace, syncs the DNS registry, launches all pods in dependency
/// order, streams output, waits for all pods to exit, and cleans up the namespace.
pub async fn run_compose(
    deployment: &Deployment,
    conn: &mut OrchestratorConnection,
) -> anyhow::Result<()> {
    let plan = crate::deployment::plan(deployment).context("planning deployment")?;
    let namespace_id = deployment.name.clone();

    // Take the log stream receiver and spawn a background log acceptor.
    let mut log_rx = conn.take_log_stream_receiver();
    let (log_line_tx, mut log_line_rx) = mpsc::channel::<LogLine>(256);

    tokio::spawn(async move {
        while let Some(mut stream) = log_rx.recv().await {
            let header: Result<LogStreamHeader, _> = recv_msg(&mut stream).await;
            match header {
                Ok(header) => {
                    let tx = log_line_tx.clone();
                    let pod_id = header.pod_id.clone();
                    tokio::spawn(async move {
                        let mut buf = vec![0u8; 8192];
                        loop {
                            match stream.read(&mut buf).await {
                                Ok(0) => break,
                                Ok(n) => {
                                    let _ = tx
                                        .send(LogLine {
                                            pod_id: pod_id.clone(),
                                            data: buf[..n].to_vec(),
                                        })
                                        .await;
                                }
                                Err(_) => break,
                            }
                        }
                    });
                }
                Err(e) => {
                    log::warn!("failed to read log stream header: {:#}", e);
                }
            }
        }
    });

    // 1. Create namespace.
    conn.send_command(&WorkerCommand::CreateNamespace {
        namespace_id: namespace_id.clone(),
        network: NetworkConfig {
            subnet: DEFAULT_SUBNET,
            gateway: DEFAULT_GATEWAY,
            prefix_len: DEFAULT_PREFIX_LEN,
        },
    })
    .await
    .context("send create namespace")?;

    // Wait for NamespaceCreated event.
    let event = conn.recv_event().await.context("recv namespace created")?;
    match event {
        WorkerEvent::NamespaceCreated { .. } => {
            log::info!("namespace '{}' created", namespace_id);
        }
        other => {
            anyhow::bail!("expected NamespaceCreated, got {:?}", other);
        }
    }

    // 2. Registry sync — DNS names resolve to service IPs.
    let registry_entries: Vec<RegistryEntry> = plan
        .services
        .iter()
        .map(|s| RegistryEntry {
            name: s.name.clone(),
            ip: s.service_ip,
        })
        .collect();

    conn.send_command(&WorkerCommand::RegistrySync {
        namespace_id: namespace_id.clone(),
        entries: registry_entries,
    })
    .await
    .context("send registry sync")?;

    // 2b. Create fabric-level service entities for each planned service.
    for planned in &plan.services {
        conn.send_command(&WorkerCommand::CreateService {
            namespace_id: namespace_id.clone(),
            service_id: planned.name.clone(),
            ip: planned.service_ip,
            mac: planned.service_mac,
            policy: ServicePolicy {
                buffer_frames: 64,
                timeout_ms: 30000,
                activator: None,
            },
        })
        .await
        .with_context(|| format!("send create service '{}'", planned.name))?;
    }

    // 3. Launch pods for each service in dependency order.
    let total_pods = plan.services.len();
    for planned in &plan.services {
        let service_spec = deployment
            .services
            .get(&planned.name)
            .context("planned service not in deployment")?;

        if !service_spec.ports.is_empty() {
            log::warn!(
                "service '{}': port mappings are not yet implemented, ignoring {} mapping(s)",
                planned.name,
                service_spec.ports.len()
            );
        }

        let container_config = build_container_config(service_spec);
        let container_spec = ContainerSpec {
            container_id: planned.name.clone(),
            image_ref: service_spec.image.clone(),
            config: container_config,
        };

        conn.send_command(&WorkerCommand::LaunchPod {
            namespace_id: namespace_id.clone(),
            pod_id: planned.name.clone(),
            network: PodNetworkConfig {
                ip: planned.pod_ip,
                mac: planned.pod_mac,
                gateway: DEFAULT_GATEWAY,
                netmask: DEFAULT_NETMASK.to_string(),
            },
            containers: vec![container_spec],
        })
        .await
        .with_context(|| format!("send launch pod '{}'", planned.name))?;
    }

    // Build a lookup from pod_id to planned service for service readiness on PodRunning.
    let planned_by_name: std::collections::HashMap<&str, &crate::deployment::PlannedService> = plan
        .services
        .iter()
        .map(|s| (s.name.as_str(), s))
        .collect();

    // 4. Event loop: receive events and log lines concurrently.
    let mut exited_count = 0;
    let mut failed_pods: Vec<String> = Vec::new();
    let mut namespace_error: Option<String> = None;

    loop {
        tokio::select! {
            event_result = conn.recv_event() => {
                let event = event_result.context("recv event")?;
                match event {
                    WorkerEvent::NamespaceCreated { namespace_id } => {
                        log::info!("namespace '{}' created", namespace_id);
                    }
                    WorkerEvent::PodRunning {
                        namespace_id: _,
                        pod_id,
                    } => {
                        log::info!("pod '{}' is running", pod_id);
                        // Wire up the service backend and mark ready.
                        if let Some(planned) = planned_by_name.get(pod_id.as_str()) {
                            conn.send_command(&WorkerCommand::UpdateServiceBackend {
                                namespace_id: namespace_id.clone(),
                                service_id: pod_id.clone(),
                                backend: Some(ServiceBackend {
                                    pod_ip: planned.pod_ip,
                                    pod_mac: planned.pod_mac,
                                }),
                            })
                            .await
                            .with_context(|| format!("send update service backend '{}'", pod_id))?;

                            conn.send_command(&WorkerCommand::ServiceReady {
                                namespace_id: namespace_id.clone(),
                                service_id: pod_id.clone(),
                            })
                            .await
                            .with_context(|| format!("send service ready '{}'", pod_id))?;
                        }
                    }
                    WorkerEvent::PodExited {
                        namespace_id: _,
                        pod_id,
                        exit_code,
                    } => {
                        if exit_code == 0 {
                            log::info!("pod '{}' exited successfully", pod_id);
                        } else {
                            log::error!("pod '{}' exited with code {}", pod_id, exit_code);
                            failed_pods.push(format!("{} (exit code {})", pod_id, exit_code));
                        }
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
                        failed_pods.push(format!("{} ({})", pod_id, error));
                        exited_count += 1;
                        if exited_count >= total_pods {
                            break;
                        }
                    }
                    WorkerEvent::NamespaceFailed {
                        namespace_id: _,
                        error,
                    } => {
                        log::error!("namespace '{}' failed: {}", namespace_id, error);
                        namespace_error = Some(error);
                        break;
                    }
                    WorkerEvent::ShuttingDown => {
                        log::info!("worker is shutting down");
                        break;
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
                    WorkerEvent::FabricRouteMiss { namespace_id: _, dst_ip, dst_mac: _ } => {
                        log::debug!("fabric route miss for {}", dst_ip);
                    }
                    WorkerEvent::ServiceActivation { namespace_id: _, service_id, dst_ip } => {
                        log::debug!("service activation for '{}' ({})", service_id, dst_ip);
                    }
                    WorkerEvent::ServiceBackendNeed { namespace_id: _, service_id, need } => {
                        log::debug!("service backend need for '{}': {:?}", service_id, need);
                    }
                }
            }
            Some(log_line) = log_line_rx.recv() => {
                if let Ok(text) = std::str::from_utf8(&log_line.data) {
                    for line in text.lines() {
                        println!("{} | {}", log_line.pod_id, line);
                    }
                } else {
                    log::debug!(
                        "pod '{}': {} bytes (binary)",
                        log_line.pod_id,
                        log_line.data.len()
                    );
                }
            }
        }
    }

    // 5. Clean up: destroy namespace.
    conn.send_command(&WorkerCommand::DestroyNamespace {
        namespace_id: namespace_id.clone(),
    })
    .await
    .context("send destroy namespace")?;

    // 6. Report results.
    if let Some(error) = namespace_error {
        anyhow::bail!("namespace '{}' failed: {}", namespace_id, error);
    }
    if !failed_pods.is_empty() {
        anyhow::bail!(
            "{} pod(s) failed: {}",
            failed_pods.len(),
            failed_pods.join(", ")
        );
    }

    Ok(())
}
