//! Async production shell wrapping `OrchestratorCore`.
//!
//! This module owns an `OrchestratorCore`, handles all I/O (channels, timers,
//! worker connections), and executes effects produced by the core.
//! The shell contains **no logic** — only boilerplate.
//!
//! Timers are driven via the core's `TimerWheel` — the shell queries
//! `next_deadline()` and sleeps until that instant, then calls `advance_to()`.

pub(crate) mod worker_reader;
pub(crate) mod worker_writer;

use std::collections::HashMap;
use std::time::Duration;

use distvirt_worker_protocol::OrchestratorConnection;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::adapter::timer::TimerConfig;
use crate::core::orchestrator::OrchestratorCore;
use crate::core::types::{
    CreateNamespaceInfo, NamespaceCoreEvent, OrchestratorEffects, OrchestratorInput,
    SchedulerCoreInput, WorkerConnectedInfo, WorkerStateCoreEvent,
};
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
    /// Worker reader exited.
    WorkerDisconnected { worker_id: GlobalWorkerId },
}

/// Commands sent to the shell from external callers.
pub(crate) enum ShellCommand {
    WorkerConnection {
        conn: OrchestratorConnection,
    },
    CreateNamespace {
        namespace_id: NamespaceId,
        network: distvirt_worker_protocol::NetworkConfig,
    },
    DestroyNamespace {
        namespace_id: NamespaceId,
    },
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

    /// Boot instant — used to convert between logical Duration and real Instant.
    start: Instant,

    rx: mpsc::Receiver<ShellEvent>,
    self_tx: mpsc::Sender<ShellEvent>,

    worker_secret: String,
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
                }
                _ = timer_sleep, if has_timer => {
                    // Timer expired — advance_to will fire it and process effects.
                }
            }

            // After either branch, fire any expired timers.
            let now = self.now();
            let effects = self.orchestrator.advance_to(now);
            self.execute_effects(effects).await;
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
                } => {
                    let effects = self.orchestrator.create_namespace(
                        CreateNamespaceInfo {
                            namespace_id,
                            network,
                        },
                        now,
                    );
                    self.execute_effects(effects).await;
                }
                ShellCommand::DestroyNamespace { namespace_id } => {
                    let effects = self.orchestrator.destroy_namespace(&namespace_id);
                    self.execute_effects(effects).await;
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
        start: Instant::now(),
        rx,
        self_tx: tx.clone(),
        worker_secret,
    };

    let handle = tokio::spawn(shell.run());
    (ShellHandle { tx }, handle)
}
