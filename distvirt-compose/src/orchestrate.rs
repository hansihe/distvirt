use anyhow::Context;
use distvirt_worker_protocol::codec::recv_log_header;
use distvirt_worker_protocol::{
    ContainerConfig, ContainerSpec, EndpointKind, EndpointPodBackend, EndpointSpec,
    LogStreamHeader, NamespaceId, NetworkConfig, OrchestratorConnection, PodNetworkConfig,
    RegistryEntry, ServiceId, ServicePolicy, WorkerCommand, WorkerEvent,
};
use futures_lite::io::AsyncReadExt;
use tokio::sync::mpsc;

use crate::deployment::{DEFAULT_GATEWAY, DEFAULT_NETMASK, DEFAULT_PREFIX_LEN, DEFAULT_SUBNET};
use crate::types::{Deployment, ServiceSpec};

/// Build a [`ContainerConfig`] from a compose [`ServiceSpec`].
///
/// Passes through entrypoint and command as overrides for the worker's OCI
/// merge logic. No splitting is done here — the worker resolves entrypoint/cmd
/// against the image config.
fn build_container_config(spec: &ServiceSpec) -> ContainerConfig {
    let entrypoint = spec.entrypoint.clone().unwrap_or_default();
    let args = spec.command.clone().unwrap_or_default();

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
        stdin: false,
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
    let namespace_id: NamespaceId = deployment.name.clone().into();

    // Take the log stream receiver and spawn a background log acceptor.
    let mut log_rx = conn.take_log_stream_receiver();
    let (log_line_tx, mut log_line_rx) = mpsc::channel::<LogLine>(256);

    tokio::spawn(async move {
        while let Some(mut stream) = log_rx.recv().await {
            let header: Result<LogStreamHeader, _> = recv_log_header(&mut stream).await;
            match header {
                Ok(header) => {
                    let tx = log_line_tx.clone();
                    let pod_id = header.pod_id.to_string();
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
            segment_id: None,
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

    // 2b. Sync endpoint table with service entries for each planned service.
    {
        let endpoints: Vec<EndpointSpec> = plan
            .services
            .iter()
            .map(|s| EndpointSpec {
                ip: s.service_ip,
                kind: EndpointKind::Service {
                    service_id: ServiceId::from(s.name.as_str()),
                    policy: ServicePolicy {
                        buffer_frames: 64,
                        timeout_ms: 30000,
                        activator: None,
                    },
                    backend: None,
                },
            })
            .collect();
        conn.send_command(&WorkerCommand::EndpointSync {
            namespace_id: namespace_id.clone(),
            endpoints,
        })
        .await
        .context("send endpoint sync")?;
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
            pod_id: planned.name.clone().into(),
            network: PodNetworkConfig {
                ip: planned.pod_ip,
                mac: [0x06, 0x00, planned.pod_ip.octets()[0], planned.pod_ip.octets()[1], planned.pod_ip.octets()[2], planned.pod_ip.octets()[3]],
                gateway: DEFAULT_GATEWAY,
                netmask: DEFAULT_NETMASK.to_string(),
            },
            containers: vec![container_spec],
            resources: None,
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
                        // Wire up the service backend via endpoint update.
                        if let Some(planned) = planned_by_name.get(pod_id.as_ref()) {
                            conn.send_command(&WorkerCommand::EndpointUpdate {
                                namespace_id: namespace_id.clone(),
                                upserted: vec![EndpointSpec {
                                    ip: planned.service_ip,
                                    kind: EndpointKind::Service {
                                        service_id: ServiceId::from(pod_id.as_ref()),
                                        policy: ServicePolicy {
                                            buffer_frames: 64,
                                            timeout_ms: 30000,
                                            activator: None,
                                        },
                                        backend: Some(EndpointPodBackend {
                                            pod_ip: planned.pod_ip,
                                            placement: None,
                                            ready: true,
                                        }),
                                    },
                                }],
                                removed_ips: vec![],
                            })
                            .await
                            .with_context(|| format!("send endpoint update for '{}'", pod_id))?;
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
                    WorkerEvent::EndpointActivation { namespace_id: _, ip, service_id } => {
                        log::debug!("endpoint activation for {} (service: {:?})", ip, service_id);
                    }
                    WorkerEvent::EndpointFlowStatus { namespace_id: _, ip, service_id, has_active_flows } => {
                        log::debug!("endpoint flow status for {} (service: {:?}): active={}", ip, service_id, has_active_flows);
                    }
                    WorkerEvent::ServiceBackendNeed { namespace_id: _, service_id, need } => {
                        log::debug!("service backend need for '{}': {:?}", service_id, need);
                    }
                    WorkerEvent::NamespaceDestroyed { namespace_id: _ } => {
                        log::info!("namespace destroyed");
                    }
                    WorkerEvent::PodSuspended { pod_id, artifact_id, .. } => {
                        log::info!("pod '{}' suspended (artifact: {})", pod_id, artifact_id);
                    }
                    WorkerEvent::PodSuspendFailed { pod_id, error, .. } => {
                        log::error!("pod '{}' suspend failed: {}", pod_id, error);
                    }
                    WorkerEvent::TunnelStatus { peer_worker_id, status } => {
                        log::debug!("tunnel status for '{}': {:?}", peer_worker_id, status);
                    }
                    WorkerEvent::WorkerCondition { key, active, message } => {
                        if active {
                            log::info!("worker condition asserted: {} — {}", key, message);
                        } else {
                            log::info!("worker condition deasserted: {}", key);
                        }
                    }
                    WorkerEvent::PoolCapacityUpdate { pools } => {
                        log::debug!("pool capacity update: {} pool(s)", pools.len());
                    }
                    WorkerEvent::ArtifactWriteStarted { artifact_id, .. } => {
                        log::debug!("artifact write started: {}", artifact_id);
                    }
                    WorkerEvent::ArtifactWriteCommitted { artifact_id, size_bytes, .. } => {
                        log::debug!("artifact write committed: {} ({} bytes)", artifact_id, size_bytes);
                    }
                    WorkerEvent::ArtifactTransferReceived { transfer_id, dest_artifact_id, size_bytes, .. } => {
                        log::debug!("artifact transfer received: transfer_id={} artifact={} ({} bytes)", transfer_id, dest_artifact_id, size_bytes);
                    }
                    WorkerEvent::TransferFailed { transfer_id, error, .. } => {
                        log::error!("artifact transfer failed: transfer_id={} error={}", transfer_id, error);
                    }
                    WorkerEvent::PressureUpdate { .. } => {
                        log::debug!("pressure update received");
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
