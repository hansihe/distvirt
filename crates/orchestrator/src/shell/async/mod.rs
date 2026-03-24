//! Async production shell wrapping `OrchestratorCore` + `NamespaceUnit`s.
//!
//! The shell owns both the orchestrator and all namespace units, routing
//! messages between them in a delivery loop (same structure as the sync shell).
//!
//! Timers are per-namespace — the shell queries all namespaces for their
//! `next_deadline()` and sleeps until the earliest one.

mod worker_reader;
mod worker_writer;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use distvirt_common::ActivityTracker;
use distvirt_worker_protocol::OrchestratorConnection;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::adapter::timer::TimerConfig;
use crate::core::namespace::NamespaceUnit;
use crate::core::orchestrator::OrchestratorCore;
use crate::core::types::{
    CreateNamespaceInfo, NamespaceOutput, OrchestratorInputNew, OrchestratorOutput,
    OrchestratorToNamespace, SchedulerCoreInput, WorkerConnectedInfo, WorkerStateCoreEvent,
};
use crate::core::orchestrator::worker_state::WorkerTunnelInfo;
use crate::core::{ClientCommand, ClientError, ConnectResult, GlobalWorkerId, WorkerWriterHandle};
use crate::event_bus::EventBusHandle;
use crate::id_registry::IdRegistryMap;
use crate::log_bus::LogBusHandle;
use crate::types::NamespaceId;

// =============================================================================
// Shell event
// =============================================================================

enum ShellEvent {
    Command(ShellCommand),
    /// Namespace-scoped event from a worker reader — route to namespace directly.
    NamespaceEvent {
        namespace_id: NamespaceId,
        event: OrchestratorToNamespace,
    },
    /// Worker state event from a worker reader.
    WorkerStateEvent(WorkerStateCoreEvent),
    /// Scheduler event (artifact placement) from a worker reader.
    SchedulerInput(SchedulerCoreInput),
    /// Worker reader exited.
    WorkerDisconnected { worker_id: GlobalWorkerId },
}

enum ShellCommand {
    WorkerConnection {
        conn: OrchestratorConnection,
    },
    CreateNamespace {
        namespace_id: NamespaceId,
        network: distvirt_worker_protocol::NetworkConfig,
        response: oneshot::Sender<Result<(), ClientError>>,
    },
    DestroyNamespace {
        namespace_id: NamespaceId,
        response: oneshot::Sender<Result<(), ClientError>>,
    },
    UpdateNamespace {
        namespace_id: NamespaceId,
        spec: crate::types::NamespaceSpecInput,
        response: oneshot::Sender<Result<crate::types::IpAllocResult, ClientError>>,
    },
    PatchNamespace {
        namespace_id: NamespaceId,
        patch: crate::types::NamespacePatchInput,
        response: oneshot::Sender<Result<crate::types::IpAllocResult, ClientError>>,
    },
    GetNamespaceStatus {
        namespace_id: NamespaceId,
        response: oneshot::Sender<Result<crate::types::NamespaceStatusReport, ClientError>>,
    },
    ListNamespaces {
        response: oneshot::Sender<Vec<crate::types::NamespaceStatusReport>>,
    },
    ConnectNetwork {
        namespace_id: NamespaceId,
        client_public_key: [u8; 32],
        response: oneshot::Sender<Result<ConnectResult, ClientError>>,
    },
    DisconnectNetwork {
        namespace_id: NamespaceId,
        client_public_key: [u8; 32],
        response: oneshot::Sender<Result<(), ClientError>>,
    },
    ListWorkers {
        response: oneshot::Sender<Vec<crate::core::orchestrator::worker_state::WorkerQueryInfo>>,
    },
    GetWorker {
        worker_id: GlobalWorkerId,
        response: oneshot::Sender<Result<crate::core::orchestrator::worker_state::WorkerQueryInfo, ClientError>>,
    },
    ListPods {
        namespace_id: NamespaceId,
        response: oneshot::Sender<Result<Vec<crate::types::PodStatusReport>, ClientError>>,
    },
    InjectNamespaceEvent {
        namespace_id: NamespaceId,
        worker_id: GlobalWorkerId,
        event: crate::core::WorkerNamespaceEventKind,
        response: oneshot::Sender<()>,
    },
    InjectWorkerStateEvent {
        event: WorkerStateCoreEvent,
        response: oneshot::Sender<()>,
    },
}

// =============================================================================
// Shell handle
// =============================================================================

#[derive(Clone)]
pub struct ShellHandle {
    tx: mpsc::Sender<ShellEvent>,
    activity: Arc<ActivityTracker>,
}

impl ShellHandle {
    pub fn worker_connection(&self, conn: OrchestratorConnection) {
        let _ = self
            .tx
            .try_send(ShellEvent::Command(ShellCommand::WorkerConnection { conn }));
    }

    pub async fn create_namespace(
        &self,
        namespace_id: NamespaceId,
        network: distvirt_worker_protocol::NetworkConfig,
    ) -> Result<(), ClientError> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(ShellEvent::Command(ShellCommand::CreateNamespace {
                namespace_id,
                network,
                response: tx,
            }))
            .await;
        rx.await.unwrap_or(Err(ClientError::ShellGone))
    }

    pub async fn destroy_namespace(&self, namespace_id: NamespaceId) -> Result<(), ClientError> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(ShellEvent::Command(ShellCommand::DestroyNamespace {
                namespace_id,
                response: tx,
            }))
            .await;
        rx.await.unwrap_or(Err(ClientError::ShellGone))
    }

    pub async fn update_namespace(
        &self,
        namespace_id: NamespaceId,
        spec: crate::types::NamespaceSpecInput,
    ) -> Result<crate::types::IpAllocResult, ClientError> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(ShellEvent::Command(ShellCommand::UpdateNamespace {
                namespace_id,
                spec,
                response: tx,
            }))
            .await;
        rx.await.unwrap_or(Err(ClientError::ShellGone))
    }

    pub async fn patch_namespace(
        &self,
        namespace_id: NamespaceId,
        patch: crate::types::NamespacePatchInput,
    ) -> Result<crate::types::IpAllocResult, ClientError> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(ShellEvent::Command(ShellCommand::PatchNamespace {
                namespace_id,
                patch,
                response: tx,
            }))
            .await;
        rx.await.unwrap_or(Err(ClientError::ShellGone))
    }

    pub async fn get_namespace_status(
        &self,
        namespace_id: NamespaceId,
    ) -> Result<crate::types::NamespaceStatusReport, ClientError> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(ShellEvent::Command(ShellCommand::GetNamespaceStatus {
                namespace_id,
                response: tx,
            }))
            .await;
        rx.await.unwrap_or(Err(ClientError::ShellGone))
    }

    pub async fn list_namespaces(&self) -> Result<Vec<crate::types::NamespaceStatusReport>, ClientError> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(ShellEvent::Command(ShellCommand::ListNamespaces {
                response: tx,
            }))
            .await;
        rx.await.map_err(|_| ClientError::ShellGone)
    }

    pub async fn connect_network(
        &self,
        namespace_id: NamespaceId,
        client_public_key: [u8; 32],
    ) -> Result<ConnectResult, ClientError> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(ShellEvent::Command(ShellCommand::ConnectNetwork {
                namespace_id,
                client_public_key,
                response: tx,
            }))
            .await;
        rx.await.unwrap_or(Err(ClientError::ShellGone))
    }

    pub async fn disconnect_network(
        &self,
        namespace_id: NamespaceId,
        client_public_key: [u8; 32],
    ) -> Result<(), ClientError> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(ShellEvent::Command(ShellCommand::DisconnectNetwork {
                namespace_id,
                client_public_key,
                response: tx,
            }))
            .await;
        rx.await.unwrap_or(Err(ClientError::ShellGone))
    }

    pub async fn list_workers(&self) -> Result<Vec<crate::core::orchestrator::worker_state::WorkerQueryInfo>, ClientError> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(ShellEvent::Command(ShellCommand::ListWorkers {
                response: tx,
            }))
            .await;
        rx.await.map_err(|_| ClientError::ShellGone)
    }

    pub async fn get_worker(&self, worker_id: GlobalWorkerId) -> Result<crate::core::orchestrator::worker_state::WorkerQueryInfo, ClientError> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(ShellEvent::Command(ShellCommand::GetWorker {
                worker_id,
                response: tx,
            }))
            .await;
        rx.await.unwrap_or(Err(ClientError::ShellGone))
    }

    pub async fn list_pods(&self, namespace_id: NamespaceId) -> Result<Vec<crate::types::PodStatusReport>, ClientError> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(ShellEvent::Command(ShellCommand::ListPods {
                namespace_id,
                response: tx,
            }))
            .await;
        rx.await.unwrap_or(Err(ClientError::ShellGone))
    }

    pub fn activity_count(&self) -> u64 {
        self.activity.activity_count()
    }

    pub fn activity_tracker(&self) -> &Arc<ActivityTracker> {
        &self.activity
    }

    pub async fn inject_namespace_event(
        &self,
        namespace_id: NamespaceId,
        worker_id: GlobalWorkerId,
        event: crate::core::WorkerNamespaceEventKind,
    ) {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(ShellEvent::Command(ShellCommand::InjectNamespaceEvent {
                namespace_id,
                worker_id,
                event,
                response: tx,
            }))
            .await;
        let _ = rx.await;
    }

    pub async fn inject_worker_state_event(
        &self,
        event: crate::core::types::WorkerStateCoreEvent,
    ) {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(ShellEvent::Command(ShellCommand::InjectWorkerStateEvent {
                event,
                response: tx,
            }))
            .await;
        let _ = rx.await;
    }
}

// =============================================================================
// Per-worker slot
// =============================================================================

struct WorkerSlot {
    writer: WorkerWriterHandle,
    #[allow(dead_code)]
    reader_handle: JoinHandle<()>,
    #[allow(dead_code)]
    writer_handle: JoinHandle<()>,
    #[allow(dead_code)]
    driver: distvirt_worker_protocol::DriverHandle,
}

// =============================================================================
// Shell
// =============================================================================

struct Shell {
    orchestrator: OrchestratorCore,
    namespaces: HashMap<NamespaceId, NamespaceUnit>,

    next_worker_id: u64,
    workers: HashMap<GlobalWorkerId, WorkerSlot>,

    start: Instant,
    rx: mpsc::Receiver<ShellEvent>,
    self_tx: mpsc::Sender<ShellEvent>,

    worker_secret: String,
    tunnel_encrypted: bool,
    wireguard_listen_port: u16,

    activity: Arc<ActivityTracker>,
    log_bus: LogBusHandle,
    event_bus: EventBusHandle,
    id_registry_map: crate::id_registry::IdRegistryMap,
}

impl Shell {
    fn now(&self) -> Duration {
        self.start.elapsed()
    }

    /// Compute the earliest deadline across all namespace timer wheels.
    fn next_deadline(&self) -> Option<Duration> {
        self.namespaces
            .values()
            .filter_map(|ns| ns.next_deadline())
            .min()
    }

    async fn run(mut self) {
        loop {
            let timer_sleep = match self.next_deadline() {
                Some(deadline) => tokio::time::sleep_until(self.start + deadline),
                None => tokio::time::sleep(Duration::MAX),
            };
            let has_timer = self.next_deadline().is_some();

            tokio::select! {
                event = self.rx.recv() => {
                    let Some(event) = event else { break };
                    self.handle_event(event).await;
                    self.activity.tick();
                }
                _ = timer_sleep, if has_timer => {
                    // Timer expired.
                }
            }

            // After either branch, fire any expired timers on all namespaces.
            let now = self.now();
            let ns_ids: Vec<_> = self.namespaces.keys().cloned().collect();
            for ns_id in ns_ids {
                if let Some(ns) = self.namespaces.get_mut(&ns_id) {
                    let ns_output = ns.advance_to(now);
                    self.route_namespace_output(&ns_id, ns_output).await;
                }
            }
            // Drain any pending orchestrator messages from timer processing.
            self.drain_pending().await;
            self.activity.tick();
        }
    }

    /// Drain pending orchestrator inputs (from namespace outputs) until quiescent.
    /// In the current single-task model this is a no-op since we process
    /// namespace outputs immediately. But it's here for correctness if
    /// route_namespace_output queues orchestrator inputs.
    async fn drain_pending(&mut self) {
        // Currently, route_namespace_output processes orchestrator messages inline.
        // This is a placeholder for the future multi-task model.
    }

    async fn handle_event(&mut self, event: ShellEvent) {
        let now = self.now();
        match event {
            ShellEvent::Command(cmd) => match cmd {
                ShellCommand::WorkerConnection { conn } => {
                    if let Err(e) = self.handle_worker_connection(conn).await {
                        eprintln!("worker connection error: {}", e);
                    }
                }
                ShellCommand::CreateNamespace {
                    namespace_id,
                    network,
                    response,
                } => {
                    let (result, output) = self.orchestrator.create_namespace(CreateNamespaceInfo {
                        namespace_id: namespace_id.clone(),
                        network,
                    });
                    self.route_orchestrator_output(output).await;

                    let client_result = match result {
                        Ok(creation_info) => {
                            let ns = NamespaceUnit::new(
                                namespace_id.clone(),
                                creation_info.timer_config,
                                &creation_info.network,
                                creation_info.id_registry,
                            );
                            self.namespaces.insert(namespace_id.clone(), ns);

                            // Send WorkerConnected to the new namespace for each connected worker.
                            for summary in creation_info.connected_workers {
                                if let Some(ns) = self.namespaces.get_mut(&namespace_id) {
                                    let ns_output = ns.process(
                                        OrchestratorToNamespace::WorkerConnected {
                                            worker_id: summary.worker_id,
                                            proto_worker_id: summary.proto_worker_id,
                                            info: crate::sm::WorkerInfo {
                                                capacity: summary.max_pods,
                                                default_pool: summary.default_pool,
                                            },
                                        },
                                        now,
                                    );
                                    self.route_namespace_output(&namespace_id, ns_output).await;
                                }
                            }
                            Ok(())
                        }
                        Err(e) => Err(e),
                    };
                    let _ = response.send(client_result);
                }
                ShellCommand::DestroyNamespace {
                    namespace_id,
                    response,
                } => {
                    self.namespaces.remove(&namespace_id);
                    let (result, output) = self.orchestrator.destroy_namespace(&namespace_id);
                    self.route_orchestrator_output(output).await;
                    self.log_bus.remove_namespace(&namespace_id);
                    self.event_bus.remove_namespace(&namespace_id);
                    let _ = response.send(result);
                }
                ShellCommand::UpdateNamespace {
                    namespace_id,
                    spec,
                    response,
                } => {
                    let result = match self.namespaces.get_mut(&namespace_id) {
                        Some(ns) => match ns.apply_full_spec(spec, now) {
                            Ok((ns_output, alloc)) => {
                                self.route_namespace_output(&namespace_id, ns_output).await;
                                Ok(alloc)
                            }
                            Err(e) => Err(e),
                        },
                        None => Err(ClientError::NamespaceNotFound),
                    };
                    let _ = response.send(result);
                }
                ShellCommand::PatchNamespace {
                    namespace_id,
                    patch,
                    response,
                } => {
                    let result = match self.namespaces.get_mut(&namespace_id) {
                        Some(ns) => match ns.apply_patch(patch, now) {
                            Ok((ns_output, alloc)) => {
                                self.route_namespace_output(&namespace_id, ns_output).await;
                                Ok(alloc)
                            }
                            Err(e) => Err(e),
                        },
                        None => Err(ClientError::NamespaceNotFound),
                    };
                    let _ = response.send(result);
                }
                ShellCommand::GetNamespaceStatus {
                    namespace_id,
                    response,
                } => {
                    let result = self.namespaces.get(&namespace_id)
                        .map(|ns| ns.status_report())
                        .ok_or(ClientError::NamespaceNotFound);
                    let _ = response.send(result);
                }
                ShellCommand::ListNamespaces { response } => {
                    let result: Vec<_> = self.namespaces.values().map(|ns| ns.status_report()).collect();
                    let _ = response.send(result);
                }
                ShellCommand::ConnectNetwork {
                    namespace_id,
                    client_public_key,
                    response,
                } => {
                    let result = self.handle_connect_network(&namespace_id, client_public_key, now).await;
                    let _ = response.send(result);
                }
                ShellCommand::DisconnectNetwork {
                    namespace_id,
                    client_public_key,
                    response,
                } => {
                    let result = if let Some(ns) = self.namespaces.get_mut(&namespace_id) {
                        let ns_output = ns.process(
                            OrchestratorToNamespace::ClientCommand(ClientCommand::Disconnect {
                                client_public_key,
                            }),
                            now,
                        );
                        self.route_namespace_output(&namespace_id, ns_output).await;
                        Ok(())
                    } else {
                        Err(ClientError::NamespaceNotFound)
                    };
                    let _ = response.send(result);
                }
                ShellCommand::ListWorkers { response } => {
                    let result = self.orchestrator.list_workers();
                    let _ = response.send(result);
                }
                ShellCommand::GetWorker {
                    worker_id,
                    response,
                } => {
                    let result = self.orchestrator.get_worker(worker_id);
                    let _ = response.send(result);
                }
                ShellCommand::ListPods {
                    namespace_id,
                    response,
                } => {
                    let result = self.namespaces.get(&namespace_id)
                        .map(|ns| {
                            let report = ns.status_report();
                            report.pods.into_values().collect()
                        })
                        .ok_or(ClientError::NamespaceNotFound);
                    let _ = response.send(result);
                }
                ShellCommand::InjectNamespaceEvent {
                    namespace_id,
                    worker_id,
                    event,
                    response,
                } => {
                    if let Some(ns) = self.namespaces.get_mut(&namespace_id) {
                        let ns_output = ns.process(
                            OrchestratorToNamespace::WorkerEvent(crate::core::WorkerNamespaceEvent {
                                worker_id,
                                event,
                            }),
                            now,
                        );
                        self.route_namespace_output(&namespace_id, ns_output).await;
                    }
                    let _ = response.send(());
                }
                ShellCommand::InjectWorkerStateEvent { event, response } => {
                    let output = self.orchestrator.process(OrchestratorInputNew::WorkerStateEvent(event));
                    self.route_orchestrator_output(output).await;
                    let _ = response.send(());
                }
            },
            ShellEvent::NamespaceEvent {
                namespace_id,
                event,
            } => {
                if let Some(ns) = self.namespaces.get_mut(&namespace_id) {
                    let ns_output = ns.process(event, now);
                    self.route_namespace_output(&namespace_id, ns_output).await;
                }
            }
            ShellEvent::WorkerStateEvent(event) => {
                let output = self.orchestrator.process(OrchestratorInputNew::WorkerStateEvent(event));
                self.route_orchestrator_output(output).await;
            }
            ShellEvent::SchedulerInput(input) => {
                let output = self.orchestrator.process(OrchestratorInputNew::SchedulerEvent(input));
                self.route_orchestrator_output(output).await;
            }
            ShellEvent::WorkerDisconnected { worker_id } => {
                let output = self.orchestrator.worker_disconnected(worker_id, now);
                self.route_orchestrator_output(output).await;
                // Process namespace messages from worker disconnect.
                self.drain_ns_messages().await;
                self.workers.remove(&worker_id);
            }
        }
    }

    async fn handle_connect_network(
        &mut self,
        namespace_id: &NamespaceId,
        client_public_key: [u8; 32],
        now: Duration,
    ) -> Result<ConnectResult, ClientError> {
        if !self.namespaces.contains_key(namespace_id) {
            return Err(ClientError::NamespaceNotFound);
        }

        let (worker_id, wg_info, public_endpoint) = match self.orchestrator.find_wireguard_worker() {
            Some((wid, wg, ep)) => (wid, wg.clone(), ep.to_string()),
            None => return Err(ClientError::NoTunnelWorker),
        };

        let ns = self.namespaces.get_mut(namespace_id).unwrap();
        let ns_output = ns.process(
            OrchestratorToNamespace::ClientCommand(ClientCommand::Connect {
                client_public_key,
                worker_id,
            }),
            now,
        );
        self.route_namespace_output(namespace_id, ns_output).await;

        let ns = self.namespaces.get(namespace_id).unwrap();
        let wg_peers = ns.wg_peers();
        let client_ip = match wg_peers.peers.get(&client_public_key) {
            Some(info) => info.client_ip,
            None => return Err(ClientError::IpExhausted),
        };

        let subnet_cidr = wg_peers.subnet_cidr();
        let endpoint = format!("{}:{}", public_endpoint, wg_info.listen_port);

        Ok(ConnectResult {
            server_public_key: wg_info.public_key,
            endpoint,
            client_ip,
            subnet: subnet_cidr,
        })
    }

    /// Drain namespace messages from pending orchestrator output.
    async fn drain_ns_messages(&mut self) {
        // In the current inline model, namespace messages are processed
        // immediately in route_orchestrator_output. This is a no-op.
    }

    async fn handle_worker_connection(
        &mut self,
        mut conn: OrchestratorConnection,
    ) -> anyhow::Result<()> {
        let hello = conn.recv_hello().await?;

        if !constant_time_eq(hello.auth_token.as_bytes(), self.worker_secret.as_bytes()) {
            anyhow::bail!("worker authentication failed");
        }

        let global_id = crate::sm::WorkerId(self.next_worker_id);
        self.next_worker_id += 1;
        let proto_worker_id = distvirt_worker_protocol::WorkerId::from(global_id.0);

        conn.send_accepted(&distvirt_worker_protocol::WorkerAccepted {
            worker_id: proto_worker_id.clone(),
            adapters: vec![distvirt_worker_protocol::AdapterConfig::WireGuard {
                listen_port: self.wireguard_listen_port,
            }],
            tunnel_encrypted: self.tunnel_encrypted,
            pools: vec![],
        })
        .await?;

        let ready = conn.recv_ready().await?;

        let tunnel_info = match (ready.tunnel_listen_port, ready.tunnel_public_key) {
            (Some(port), Some(key)) => Some(WorkerTunnelInfo {
                listen_port: port,
                public_key: key,
            }),
            _ => None,
        };

        let wireguard_info = match (ready.wireguard_listen_port, ready.wireguard_public_key) {
            (Some(port), Some(key)) => Some(crate::core::orchestrator::worker_state::WireguardAdapterInfo {
                listen_port: port,
                public_key: key,
            }),
            _ => None,
        };

        let (reader, writer, log_rx, driver) = conn.into_split();
        spawn_log_ingest(log_rx, self.log_bus.clone(), self.id_registry_map.clone());

        let (cmd_tx, cmd_rx) = mpsc::channel::<distvirt_worker_protocol::WorkerCommand>(256);
        let writer_handle = tokio::spawn(worker_writer::run(cmd_rx, writer));
        let writer_hdl = WorkerWriterHandle::new(cmd_tx);

        let reader_handle = worker_reader::spawn(global_id, reader, self.self_tx.clone());

        let now = self.now();
        let output = self.orchestrator.worker_connected(
            WorkerConnectedInfo {
                worker_id: global_id,
                capabilities: hello.capabilities,
                tunnel_info,
                wireguard_info,
                proto_worker_id,
            },
            now,
        );
        self.route_orchestrator_output(output).await;

        self.workers.insert(
            global_id,
            WorkerSlot {
                writer: writer_hdl,
                reader_handle,
                writer_handle,
                driver,
            },
        );

        Ok(())
    }

    /// Route orchestrator output: deliver namespace messages inline, send wire commands.
    async fn route_orchestrator_output(&mut self, output: OrchestratorOutput) {
        let now = self.now();

        // Direct wire commands.
        for direct in output.direct_worker_commands {
            if let Some(slot) = self.workers.get(&direct.worker_id) {
                slot.writer.send(direct.command).await;
            }
        }

        // Global broadcasts.
        for cmd in output.global_broadcasts {
            for slot in self.workers.values() {
                slot.writer.send(cmd.clone()).await;
            }
        }

        // Worker commands (e.g. DeleteArtifact).
        for (worker_id, cmd) in output.worker_commands {
            if let Some(slot) = self.workers.get(&worker_id) {
                slot.writer.send(cmd).await;
            }
        }

        // Deliver namespace messages inline.
        for (ns_id, msg) in output.to_namespaces {
            if let Some(ns) = self.namespaces.get_mut(&ns_id) {
                let ns_output = ns.process(msg, now);
                self.route_namespace_output_inner(&ns_id, ns_output).await;
            }
        }
    }

    /// Route namespace output: send wire commands, process orchestrator messages inline.
    async fn route_namespace_output(&mut self, namespace_id: &NamespaceId, output: NamespaceOutput) {
        self.route_namespace_output_inner(namespace_id, output).await;
    }

    async fn route_namespace_output_inner(&mut self, namespace_id: &NamespaceId, output: NamespaceOutput) {
        let now = self.now();

        // Namespace-scoped broadcasts.
        if !output.broadcast_commands.is_empty() {
            if let Some(ns) = self.namespaces.get(namespace_id) {
                for cmd in &output.broadcast_commands {
                    for worker_id in ns.active_worker_ids() {
                        if let Some(slot) = self.workers.get(&worker_id) {
                            slot.writer.send(cmd.clone()).await;
                        }
                    }
                }
            }
        }

        // Targeted worker commands.
        for (worker_id, cmd) in output.worker_commands {
            if let Some(slot) = self.workers.get(&worker_id) {
                slot.writer.send(cmd).await;
            }
        }

        // Observability events.
        if !output.observability_events.is_empty() {
            self.event_bus
                .publish(namespace_id, output.observability_events);
        }

        // Process orchestrator messages inline.
        for msg in output.to_orchestrator {
            let orch_output = self.orchestrator.process(OrchestratorInputNew::FromNamespace {
                namespace_id: namespace_id.clone(),
                message: msg,
            });
            // Recursively route orchestrator output (scheduler decisions → namespace messages).
            // This is bounded: scheduler decisions don't produce further scheduler messages.
            self.route_orchestrator_output_inner_no_recurse(orch_output, now).await;
        }
    }

    /// Route orchestrator output without recursing into namespace processing for to_namespaces.
    /// Instead, process namespace messages inline.
    async fn route_orchestrator_output_inner_no_recurse(&mut self, output: OrchestratorOutput, now: Duration) {
        // Direct wire commands.
        for direct in output.direct_worker_commands {
            if let Some(slot) = self.workers.get(&direct.worker_id) {
                slot.writer.send(direct.command).await;
            }
        }

        // Global broadcasts.
        for cmd in output.global_broadcasts {
            for slot in self.workers.values() {
                slot.writer.send(cmd.clone()).await;
            }
        }

        // Worker commands.
        for (worker_id, cmd) in output.worker_commands {
            if let Some(slot) = self.workers.get(&worker_id) {
                slot.writer.send(cmd).await;
            }
        }

        // Deliver namespace messages inline.
        for (ns_id, msg) in output.to_namespaces {
            if let Some(ns) = self.namespaces.get_mut(&ns_id) {
                let ns_output = ns.process(msg, now);
                // Only send wire effects, don't recurse into orchestrator messages.
                // Scheduler decisions should not produce further scheduler messages.
                if !ns_output.broadcast_commands.is_empty() {
                    if let Some(ns_ref) = self.namespaces.get(&ns_id) {
                        for cmd in &ns_output.broadcast_commands {
                            for worker_id in ns_ref.active_worker_ids() {
                                if let Some(slot) = self.workers.get(&worker_id) {
                                    slot.writer.send(cmd.clone()).await;
                                }
                            }
                        }
                    }
                }
                for (worker_id, cmd) in ns_output.worker_commands {
                    if let Some(slot) = self.workers.get(&worker_id) {
                        slot.writer.send(cmd).await;
                    }
                }
                if !ns_output.observability_events.is_empty() {
                    self.event_bus.publish(&ns_id, ns_output.observability_events);
                }
                debug_assert!(
                    ns_output.to_orchestrator.is_empty(),
                    "scheduler decisions should not produce new scheduler messages"
                );
            }
        }
    }
}

fn spawn_log_ingest(
    mut log_rx: mpsc::UnboundedReceiver<::yamux::Stream>,
    log_bus: LogBusHandle,
    id_registry_map: crate::id_registry::IdRegistryMap,
) {
    tokio::spawn(async move {
        while let Some(mut stream) = log_rx.recv().await {
            let header: distvirt_worker_protocol::LogStreamHeader =
                match distvirt_worker_protocol::codec::recv_log_header(&mut stream).await {
                    Ok(h) => h,
                    Err(e) => {
                        log::warn!("failed to read log stream header: {:#}", e);
                        continue;
                    }
                };
            let bus = log_bus.clone();

            // Resolve workload name lazily per stream.
            // Convert protocol PodId to router PodId for registry lookup.
            let router_pod_id = crate::sm::PodId(header.pod_id.0);
            let id_registry_map_clone = id_registry_map.clone();
            let mut workload_name: Option<String> = id_registry_map
                .get(&header.namespace_id)
                .and_then(|reg| reg.pod_workload_name(&router_pod_id));

            tokio::spawn(async move {
                loop {
                    match distvirt_worker_protocol::codec::recv_log_frame(&mut stream).await {
                        Ok(None) => break,
                        Ok(Some((seq, payload))) => {
                            // Re-resolve workload name if previously unknown.
                            // The id registry may not have been populated when the
                            // stream first opened (race with sync_dynamic_ids).
                            if workload_name.is_none() {
                                workload_name = id_registry_map_clone
                                    .get(&header.namespace_id)
                                    .and_then(|reg| reg.pod_workload_name(&router_pod_id));
                            }

                            bus.publish(
                                crate::log_bus::LogChunk {
                                    namespace_id: header.namespace_id.clone(),
                                    pod_id: header.pod_id.clone(),
                                    container_id: header.container_id.clone(),
                                    workload_name: workload_name.clone(),
                                    data: payload,
                                    timestamp: std::time::Instant::now(),
                                    seq,
                                },
                                workload_name.clone(),
                            );
                        }
                        Err(e) => {
                            log::debug!("log stream read error: {}", e);
                            break;
                        }
                    }
                }
                // Stream closed — mark topic as retired.
                bus.retire_topic(
                    &header.namespace_id,
                    &header.pod_id,
                    &header.container_id,
                );
            });
        }
    });
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    use subtle::ConstantTimeEq;
    if a.len() != b.len() {
        return false;
    }
    a.ct_eq(b).into()
}

pub fn spawn(
    worker_secret: String,
    timer_config: TimerConfig,
    tunnel_encrypted: bool,
    wireguard_listen_port: u16,
    activity: Arc<ActivityTracker>,
) -> (ShellHandle, LogBusHandle, EventBusHandle, IdRegistryMap, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(256);
    let log_bus = LogBusHandle::new(256 * 1024);
    let event_bus = EventBusHandle::new(1024);
    let id_registry_map = IdRegistryMap::new();

    let shell = Shell {
        orchestrator: OrchestratorCore::new(timer_config, id_registry_map.clone()),
        namespaces: HashMap::new(),
        next_worker_id: 0,
        workers: HashMap::new(),
        start: Instant::now(),
        rx,
        self_tx: tx.clone(),
        worker_secret,
        tunnel_encrypted,
        wireguard_listen_port,
        activity: activity.clone(),
        log_bus: log_bus.clone(),
        event_bus: event_bus.clone(),
        id_registry_map: id_registry_map.clone(),
    };

    let handle = tokio::spawn(shell.run());
    (ShellHandle { tx, activity }, log_bus, event_bus, id_registry_map, handle)
}
