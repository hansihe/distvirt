//! Async production shell wrapping `OrchestratorCore`.
//!
//! This module owns an `OrchestratorCore`, handles all I/O (channels, timers,
//! worker connections), and executes effects produced by the core.
//! The shell contains **no logic** — only boilerplate.
//!
//! Timers are driven via the core's `TimerWheel` — the shell queries
//! `next_deadline()` and sleeps until that instant, then calls `advance_to()`.

mod worker_reader;
mod worker_writer;

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use distvirt_worker_protocol::OrchestratorConnection;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::adapter::timer::TimerConfig;
use crate::core::orchestrator::OrchestratorCore;
use crate::core::types::{
    CreateNamespaceInfo, NamespaceCoreEvent, OrchestratorEffects, OrchestratorInput,
    SchedulerCoreInput, WorkerConnectedInfo, WorkerStateCoreEvent,
};
use crate::core::worker_state::WorkerTunnelInfo;
use crate::core::{ClientError, ConnectResult, GlobalWorkerId, WorkerWriterHandle};
use crate::types::{NamespaceId, NamespaceSpec};

// =============================================================================
// Shell event — everything the shell can receive
// =============================================================================

enum ShellEvent {
    /// External command (worker connection, namespace create/destroy).
    Command(ShellCommand),
    /// Namespace-scoped event from a worker reader.
    NamespaceEvent {
        namespace_id: NamespaceId,
        event: NamespaceCoreEvent,
    },
    /// Worker state event from a worker reader.
    WorkerStateEvent(WorkerStateCoreEvent),
    /// Scheduler event (artifact placement) from a worker reader.
    SchedulerInput(SchedulerCoreInput),
    /// Worker reader exited.
    WorkerDisconnected { worker_id: GlobalWorkerId },
}

/// Commands sent to the shell from external callers.
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
        spec: NamespaceSpec,
        response: oneshot::Sender<Result<(), ClientError>>,
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
        response: oneshot::Sender<Vec<crate::core::worker_state::WorkerQueryInfo>>,
    },
    GetWorker {
        worker_id: GlobalWorkerId,
        response: oneshot::Sender<Result<crate::core::worker_state::WorkerQueryInfo, ClientError>>,
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
    activity: Arc<AtomicU64>,
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
        spec: NamespaceSpec,
    ) -> Result<(), ClientError> {
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

    pub async fn list_workers(&self) -> Result<Vec<crate::core::worker_state::WorkerQueryInfo>, ClientError> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(ShellEvent::Command(ShellCommand::ListWorkers {
                response: tx,
            }))
            .await;
        rx.await.map_err(|_| ClientError::ShellGone)
    }

    pub async fn get_worker(&self, worker_id: GlobalWorkerId) -> Result<crate::core::worker_state::WorkerQueryInfo, ClientError> {
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
        self.activity.load(std::sync::atomic::Ordering::Relaxed)
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
// Per-worker slot (async-only state)
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

    next_worker_id: u64,

    workers: HashMap<GlobalWorkerId, WorkerSlot>,

    /// Boot instant — used to convert between logical Duration and real Instant.
    start: Instant,

    rx: mpsc::Receiver<ShellEvent>,
    self_tx: mpsc::Sender<ShellEvent>,

    worker_secret: String,

    activity: Arc<AtomicU64>,
}

impl Shell {
    /// Current logical time (Duration since shell boot).
    fn now(&self) -> Duration {
        self.start.elapsed()
    }

    async fn run(mut self) {
        loop {
            // Compute the next timer deadline as a real Instant.
            let timer_sleep = match self.orchestrator.next_deadline() {
                Some(deadline) => tokio::time::sleep_until(self.start + deadline),
                None => tokio::time::sleep(Duration::MAX),
            };
            let has_timer = self.orchestrator.next_deadline().is_some();

            tokio::select! {
                event = self.rx.recv() => {
                    let Some(event) = event else { break };
                    self.handle_event(event).await;
                    self.activity.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                _ = timer_sleep, if has_timer => {
                    // Timer expired — advance_to will fire it and process effects.
                }
            }

            // After either branch, fire any expired timers.
            let now = self.now();
            let effects = self.orchestrator.advance_to(now);
            self.execute_effects(effects).await;
            self.activity.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
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
                    let (result, effects) = self.orchestrator.create_namespace(
                        CreateNamespaceInfo {
                            namespace_id,
                            network,
                        },
                        now,
                    );
                    self.execute_effects(effects).await;
                    let _ = response.send(result);
                }
                ShellCommand::DestroyNamespace {
                    namespace_id,
                    response,
                } => {
                    let (result, effects) = self.orchestrator.destroy_namespace(&namespace_id);
                    self.execute_effects(effects).await;
                    let _ = response.send(result);
                }
                ShellCommand::UpdateNamespace {
                    namespace_id,
                    spec,
                    response,
                } => {
                    let (result, effects) = self.orchestrator.update_namespace(&namespace_id, spec, now);
                    self.execute_effects(effects).await;
                    let _ = response.send(result);
                }
                ShellCommand::GetNamespaceStatus {
                    namespace_id,
                    response,
                } => {
                    let result = self.orchestrator.get_namespace_status(&namespace_id);
                    let _ = response.send(result);
                }
                ShellCommand::ListNamespaces { response } => {
                    let result = self.orchestrator.list_namespaces();
                    let _ = response.send(result);
                }
                ShellCommand::ConnectNetwork {
                    namespace_id,
                    client_public_key,
                    response,
                } => {
                    let (result, effects) = self.orchestrator.connect_network(
                        &namespace_id,
                        client_public_key,
                        now,
                    );
                    self.execute_effects(effects).await;
                    let _ = response.send(result);
                }
                ShellCommand::DisconnectNetwork {
                    namespace_id,
                    client_public_key,
                    response,
                } => {
                    let (result, effects) = self.orchestrator.disconnect_network(
                        &namespace_id,
                        client_public_key,
                        now,
                    );
                    self.execute_effects(effects).await;
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
                    let result = self.orchestrator.list_pods(&namespace_id);
                    let _ = response.send(result);
                }
                ShellCommand::InjectNamespaceEvent {
                    namespace_id,
                    worker_id,
                    event,
                    response,
                } => {
                    let effects = self.orchestrator.process(
                        OrchestratorInput::NamespaceEvent {
                            namespace_id,
                            event: NamespaceCoreEvent::WorkerEvent(crate::core::WorkerNamespaceEvent {
                                worker_id,
                                event,
                            }),
                        },
                        now,
                    );
                    self.execute_effects(effects).await;
                    let _ = response.send(());
                }
                ShellCommand::InjectWorkerStateEvent { event, response } => {
                    let effects = self
                        .orchestrator
                        .process(OrchestratorInput::WorkerStateEvent(event), now);
                    self.execute_effects(effects).await;
                    let _ = response.send(());
                }
            },
            ShellEvent::NamespaceEvent {
                namespace_id,
                event,
            } => {
                let effects = self.orchestrator.process(
                    OrchestratorInput::NamespaceEvent {
                        namespace_id,
                        event,
                    },
                    now,
                );
                self.execute_effects(effects).await;
            }
            ShellEvent::WorkerStateEvent(event) => {
                let effects = self
                    .orchestrator
                    .process(OrchestratorInput::WorkerStateEvent(event), now);
                self.execute_effects(effects).await;
            }
            ShellEvent::SchedulerInput(input) => {
                let effects = self
                    .orchestrator
                    .process(OrchestratorInput::SchedulerEvent(input), now);
                self.execute_effects(effects).await;
            }
            ShellEvent::WorkerDisconnected { worker_id } => {
                let effects = self.orchestrator.worker_disconnected(worker_id, now);
                self.execute_effects(effects).await;
                self.workers.remove(&worker_id);
            }
        }
    }

    /// Handshake a new worker connection. The only place with real I/O logic,
    /// but it's purely protocol plumbing — no domain decisions.
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
        let proto_worker_id =
            distvirt_worker_protocol::WorkerId::from(global_id.0);

        conn.send_accepted(&distvirt_worker_protocol::WorkerAccepted {
            worker_id: proto_worker_id.clone(),
            adapters: vec![],
            tunnel_encrypted: false,
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

        // Split connection into reader/writer.
        let (reader, writer, _log_rx, driver) = conn.into_split();

        let (cmd_tx, cmd_rx) = mpsc::channel::<distvirt_worker_protocol::WorkerCommand>(256);
        let writer_handle = tokio::spawn(worker_writer::run(cmd_rx, writer));
        let writer_hdl = WorkerWriterHandle::new(cmd_tx);

        let reader_handle = worker_reader::spawn(global_id, reader, self.self_tx.clone());

        let now = self.now();

        // Tell the orchestrator about the new worker (handles all fan-out).
        let effects = self.orchestrator.worker_connected(
            WorkerConnectedInfo {
                worker_id: global_id,
                capabilities: hello.capabilities,
                tunnel_info,
                proto_worker_id,
            },
            now,
        );
        self.execute_effects(effects).await;

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

    /// Execute effects produced by the sync orchestrator.
    /// Pure boilerplate: send commands on the wire.
    async fn execute_effects(&mut self, effects: OrchestratorEffects) {
        // Direct wire commands (e.g. CreateNamespace sent before fabric is ready).
        for direct in effects.direct_worker_commands {
            if let Some(slot) = self.workers.get(&direct.worker_id) {
                slot.writer.send(direct.command).await;
            }
        }

        // Worker commands (routed through namespace logic).
        for (worker_id, cmd) in effects.worker_commands {
            if let Some(slot) = self.workers.get(&worker_id) {
                slot.writer.send(cmd).await;
            }
        }

        // Namespace-scoped broadcasts.
        for (namespace_id, cmd) in effects.broadcast_commands {
            if let Some(ns) = self.orchestrator.namespace(&namespace_id) {
                for worker_id in ns.active_worker_ids() {
                    if let Some(slot) = self.workers.get(&worker_id) {
                        slot.writer.send(cmd.clone()).await;
                    }
                }
            }
        }

        // Global broadcasts.
        for cmd in effects.global_broadcasts {
            for slot in self.workers.values() {
                slot.writer.send(cmd.clone()).await;
            }
        }
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    use subtle::ConstantTimeEq;
    if a.len() != b.len() {
        return false;
    }
    a.ct_eq(b).into()
}

/// Spawn the new async shell. Returns (handle, join_handle).
pub fn spawn(
    worker_secret: String,
    timer_config: TimerConfig,
) -> (ShellHandle, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(256);
    let activity = Arc::new(AtomicU64::new(0));

    let shell = Shell {
        orchestrator: OrchestratorCore::new(timer_config),
        next_worker_id: 0,
        workers: HashMap::new(),
        start: Instant::now(),
        rx,
        self_tx: tx.clone(),
        worker_secret,
        activity: activity.clone(),
    };

    let handle = tokio::spawn(shell.run());
    (ShellHandle { tx, activity }, handle)
}
