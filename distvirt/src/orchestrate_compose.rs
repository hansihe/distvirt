use std::net::Ipv4Addr;

use anyhow::Context;
use distvirt_worker_protocol::codec::recv_msg;
use distvirt_worker_protocol::{
    ContainerConfig, ContainerSpec, LogStreamHeader, NetworkConfig, OrchestratorConnection,
    PodNetworkConfig, RegistryEntry, WorkerCommand, WorkerEvent,
};
use futures_lite::io::AsyncReadExt;
use tokio::sync::mpsc;

use crate::deployment::{Deployment, ServiceSpec};

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
            subnet: Ipv4Addr::new(172, 16, 0, 0),
            gateway: Ipv4Addr::new(172, 16, 0, 1),
            prefix_len: 24,
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

    // 2. Registry sync.
    let registry_entries: Vec<RegistryEntry> = plan
        .services
        .iter()
        .map(|s| RegistryEntry {
            name: s.name.clone(),
            ip: s.ip,
        })
        .collect();

    conn.send_command(&WorkerCommand::RegistrySync {
        namespace_id: namespace_id.clone(),
        entries: registry_entries,
    })
    .await
    .context("send registry sync")?;

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

        conn.send_command(&WorkerCommand::LaunchPod {
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
        .with_context(|| format!("send launch pod '{}'", planned.name))?;
    }

    // 4. Event loop: receive events and log lines concurrently.
    let mut exited_count = 0;

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

    Ok(())
}
