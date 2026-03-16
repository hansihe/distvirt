//! Async production shell wrapping `OrchestratorCore`.
//!
//! This module owns an `OrchestratorCore`, handles all I/O (channels, timers,
//! worker connections), and executes effects produced by the core.
//! The shell contains **no logic** — only boilerplate.

pub(crate) mod worker_reader;
pub(crate) mod worker_writer;

use std::collections::HashMap;

use distvirt_worker_protocol::OrchestratorConnection;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::adapter::timer::{TimerAction, TimerConfig, TimerIdentity};
use crate::core::types::{
    CreateNamespaceInfo, NamespaceCoreEvent, OrchestratorEffects, OrchestratorInput,
    SchedulerCoreInput, WorkerConnectedInfo, WorkerStateCoreEvent,
};
use crate::core::orchestrator::OrchestratorCore;
use crate::task::{GlobalWorkerId, WorkerWriterHandle};
use crate::types::NamespaceId;

// =============================================================================
// Shell event — everything the shell can receive
// =============================================================================

pub(crate) enum ShellEvent {
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
    /// A timer fired.
    TimerFired {
        namespace_id: NamespaceId,
        identity: TimerIdentity,
        generation: u64,
    },
    /// Worker reader exited.
    WorkerDisconnected { worker_id: GlobalWorkerId },
}

/// Commands sent to the shell from external callers.
pub(crate) enum ShellCommand {
    WorkerConnection { conn: OrchestratorConnection },
    CreateNamespace {
        namespace_id: NamespaceId,
        network: distvirt_worker_protocol::NetworkConfig,
    },
    DestroyNamespace { namespace_id: NamespaceId },
}

// =============================================================================
// Shell handle
// =============================================================================

#[derive(Clone)]
pub(crate) struct ShellHandle {
    tx: mpsc::Sender<ShellEvent>,
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
    ) {
        let _ = self
            .tx
            .send(ShellEvent::Command(ShellCommand::CreateNamespace {
                namespace_id,
                network,
            }))
            .await;
    }

    pub async fn destroy_namespace(&self, namespace_id: NamespaceId) {
        let _ = self
            .tx
            .send(ShellEvent::Command(ShellCommand::DestroyNamespace {
                namespace_id,
            }))
            .await;
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

    /// Timer handles: (namespace_id, identity) → (generation, JoinHandle)
    timer_handles: HashMap<(NamespaceId, TimerIdentity), (u64, JoinHandle<()>)>,

    rx: mpsc::Receiver<ShellEvent>,
    self_tx: mpsc::Sender<ShellEvent>,

    worker_secret: String,
}

impl Shell {
    async fn run(mut self) {
        while let Some(event) = self.rx.recv().await {
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
                    } => {
                        let effects =
                            self.orchestrator.create_namespace(CreateNamespaceInfo {
                                namespace_id,
                                network,
                            });
                        self.execute_effects(effects).await;
                    }
                    ShellCommand::DestroyNamespace { namespace_id } => {
                        let effects = self.orchestrator.destroy_namespace(&namespace_id);
                        self.execute_effects(effects).await;
                        // Cancel all timers for this namespace.
                        let timer_keys: Vec<_> = self
                            .timer_handles
                            .keys()
                            .filter(|(ns_id, _)| *ns_id == namespace_id)
                            .cloned()
                            .collect();
                        for key in timer_keys {
                            if let Some((_, handle)) = self.timer_handles.remove(&key) {
                                handle.abort();
                            }
                        }
                    }
                },
                ShellEvent::NamespaceEvent {
                    namespace_id,
                    event,
                } => {
                    let effects =
                        self.orchestrator.process(OrchestratorInput::NamespaceEvent {
                            namespace_id,
                            event,
                        });
                    self.execute_effects(effects).await;
                }
                ShellEvent::WorkerStateEvent(event) => {
                    let effects = self
                        .orchestrator
                        .process(OrchestratorInput::WorkerStateEvent(event));
                    self.execute_effects(effects).await;
                }
                ShellEvent::SchedulerInput(input) => {
                    let effects = self
                        .orchestrator
                        .process(OrchestratorInput::SchedulerEvent(input));
                    self.execute_effects(effects).await;
                }
                ShellEvent::TimerFired {
                    namespace_id,
                    identity,
                    generation,
                } => {
                    let key = (namespace_id.clone(), identity.clone());
                    let valid = self
                        .timer_handles
                        .get(&key)
                        .map(|(g, _)| *g == generation)
                        .unwrap_or(false);
                    if valid {
                        self.timer_handles.remove(&key);
                        let effects =
                            self.orchestrator.process(OrchestratorInput::NamespaceEvent {
                                namespace_id,
                                event: NamespaceCoreEvent::TimerFired {
                                    identity,
                                    generation,
                                },
                            });
                        self.execute_effects(effects).await;
                    }
                }
                ShellEvent::WorkerDisconnected { worker_id } => {
                    let effects = self.orchestrator.worker_disconnected(worker_id);
                    self.execute_effects(effects).await;
                    self.workers.remove(&worker_id);
                }
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

        if !constant_time_eq(
            hello.auth_token.as_bytes(),
            self.worker_secret.as_bytes(),
        ) {
            anyhow::bail!("worker authentication failed");
        }

        let global_id = GlobalWorkerId(self.next_worker_id);
        self.next_worker_id += 1;
        let proto_worker_id =
            distvirt_worker_protocol::WorkerId::from(format!("w-{}", global_id.0));

        conn.send_accepted(&distvirt_worker_protocol::WorkerAccepted {
            worker_id: proto_worker_id.clone(),
            adapters: vec![],
            tunnel_encrypted: false,
            pools: vec![],
        })
        .await?;

        let ready = conn.recv_ready().await?;

        let tunnel_info = match (ready.tunnel_listen_port, ready.tunnel_public_key) {
            (Some(port), Some(key)) => Some(crate::task::worker_state::WorkerTunnelInfo {
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

        let reader_handle =
            worker_reader::spawn(global_id, reader, self.self_tx.clone());

        // Tell the orchestrator about the new worker (handles all fan-out).
        let effects = self.orchestrator.worker_connected(WorkerConnectedInfo {
            worker_id: global_id,
            capabilities: hello.capabilities,
            tunnel_info,
            proto_worker_id,
        });
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
    /// Pure boilerplate: send commands, spawn/cancel timers.
    async fn execute_effects(&mut self, effects: OrchestratorEffects) {
        // Timer actions.
        for (namespace_id, timer_actions) in effects.timer_actions {
            for action in timer_actions {
                match action {
                    TimerAction::Start {
                        identity,
                        generation,
                        duration,
                    } => {
                        let key = (namespace_id.clone(), identity.clone());
                        if let Some((_, handle)) = self.timer_handles.remove(&key) {
                            handle.abort();
                        }
                        let self_tx = self.self_tx.clone();
                        let ns_id = namespace_id.clone();
                        let ident = identity.clone();
                        let handle = tokio::spawn(async move {
                            tokio::time::sleep(duration).await;
                            let _ = self_tx
                                .send(ShellEvent::TimerFired {
                                    namespace_id: ns_id,
                                    identity: ident,
                                    generation,
                                })
                                .await;
                        });
                        self.timer_handles.insert(key, (generation, handle));
                    }
                    TimerAction::Cancel { identity } => {
                        let key = (namespace_id.clone(), identity);
                        if let Some((_, handle)) = self.timer_handles.remove(&key) {
                            handle.abort();
                        }
                    }
                }
            }
        }

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
                for &worker_id in ns.active_workers() {
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
pub(crate) fn spawn(
    worker_secret: String,
    timer_config: TimerConfig,
) -> (ShellHandle, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(256);

    let shell = Shell {
        orchestrator: OrchestratorCore::new(timer_config),
        next_worker_id: 0,
        workers: HashMap::new(),
        timer_handles: HashMap::new(),
        rx,
        self_tx: tx.clone(),
        worker_secret,
    };

    let handle = tokio::spawn(shell.run());
    (ShellHandle { tx }, handle)
}
