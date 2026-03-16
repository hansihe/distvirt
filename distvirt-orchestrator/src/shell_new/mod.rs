//! Async production shell wrapping `SyncOrchestrator`.
//!
//! This module is the async counterpart to `core/`. It owns a `SyncOrchestrator`,
//! handles all I/O (channels, timers, worker connections), and executes effects
//! produced by the sync core. The shell contains **no logic** — only boilerplate.

pub(crate) mod worker_reader;
pub(crate) mod worker_writer;

use std::collections::{BTreeSet, HashMap, HashSet};

use distvirt_worker_protocol::OrchestratorConnection;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::adapter::timer::{TimerAction, TimerConfig, TimerIdentity};
use crate::core::types::{
    NamespaceCoreEvent, OrchestratorEffects, OrchestratorInput, SchedulerCoreInput,
    WorkerStateCoreEvent,
};
use crate::core::SyncOrchestrator;
use crate::sm_new::WorkerInfo;
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
    WorkerDisconnected {
        worker_id: GlobalWorkerId,
    },
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

/// Clonable handle for sending commands to the shell.
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
// Per-worker slot
// =============================================================================

struct WorkerSlot {
    proto_worker_id: distvirt_worker_protocol::WorkerId,
    writer: WorkerWriterHandle,
    #[allow(dead_code)]
    capabilities: distvirt_worker_protocol::WorkerCapabilities,
    #[allow(dead_code)]
    reader_handle: JoinHandle<()>,
    #[allow(dead_code)]
    writer_handle: JoinHandle<()>,
    #[allow(dead_code)]
    driver: distvirt_worker_protocol::DriverHandle,
}

// =============================================================================
// Per-namespace slot
// =============================================================================

struct NamespaceSlot {
    segment_id: u16,
    network: distvirt_worker_protocol::NetworkConfig,
}

// =============================================================================
// Shell
// =============================================================================

struct Shell {
    orchestrator: SyncOrchestrator,

    next_worker_id: u64,
    next_segment_id: u16,
    active_segment_ids: BTreeSet<u16>,

    workers: HashMap<GlobalWorkerId, WorkerSlot>,
    namespaces: HashMap<NamespaceId, NamespaceSlot>,

    /// Timer handles: (namespace_id, identity) → (generation, JoinHandle)
    timer_handles: HashMap<(NamespaceId, TimerIdentity), (u64, JoinHandle<()>)>,

    rx: mpsc::Receiver<ShellEvent>,
    self_tx: mpsc::Sender<ShellEvent>,

    worker_secret: String,
}

impl Shell {
    fn alloc_segment_id(&mut self) -> u16 {
        loop {
            let id = self.next_segment_id;
            self.next_segment_id = self.next_segment_id.wrapping_add(1);
            if id == 0 {
                continue;
            }
            if !self.active_segment_ids.contains(&id) {
                self.active_segment_ids.insert(id);
                return id;
            }
        }
    }

    fn free_segment_id(&mut self, id: u16) {
        self.active_segment_ids.remove(&id);
    }

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
                        self.handle_create_namespace(namespace_id, network).await;
                    }
                    ShellCommand::DestroyNamespace { namespace_id } => {
                        self.handle_destroy_namespace(namespace_id).await;
                    }
                },
                ShellEvent::NamespaceEvent {
                    namespace_id,
                    event,
                } => {
                    let effects = self.orchestrator.process(OrchestratorInput::NamespaceEvent {
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
                    // Artifact events go directly to the scheduler via the orchestrator.
                    // We wrap them as a worker state event since they affect the scheduler.
                    // Actually, these go directly to the scheduler core, but we need to route
                    // them through the SyncOrchestrator. For now, handle them as a special case.
                    // Since SchedulerCoreInput maps to SchedulerCore::process, we need a path.
                    // The cleanest approach: extend OrchestratorInput with a SchedulerEvent variant.
                    // For now, we handle artifact events in-line.
                    let _ = input; // TODO: route artifact events through orchestrator
                }
                ShellEvent::TimerFired {
                    namespace_id,
                    identity,
                    generation,
                } => {
                    // Generation check: only forward if the generation matches.
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
                    self.handle_worker_disconnected(worker_id).await;
                }
            }
        }
    }

    async fn handle_worker_connection(
        &mut self,
        mut conn: OrchestratorConnection,
    ) -> anyhow::Result<()> {
        // Handshake.
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

        // Split connection.
        let (reader, writer, _log_rx, driver) = conn.into_split();

        // Writer task.
        let (cmd_tx, cmd_rx) = mpsc::channel::<distvirt_worker_protocol::WorkerCommand>(256);
        let writer_handle = tokio::spawn(worker_writer::run(cmd_rx, writer));
        let writer_hdl = WorkerWriterHandle::new(cmd_tx);

        // Reader task.
        let namespace_ids: HashSet<NamespaceId> = self.namespaces.keys().cloned().collect();
        let reader_handle =
            worker_reader::spawn(global_id, reader, self.self_tx.clone(), namespace_ids);

        // Notify orchestrator of worker connection (via worker state).
        let effects = self
            .orchestrator
            .process(OrchestratorInput::WorkerStateEvent(
                WorkerStateCoreEvent::Connected {
                    worker_id: global_id,
                    capabilities: hello.capabilities.clone(),
                    tunnel_info,
                    proto_worker_id: proto_worker_id.clone(),
                },
            ));
        self.execute_effects(effects).await;

        // For each active namespace, send CreateNamespace and WorkerConnected.
        let ns_info: Vec<_> = self
            .namespaces
            .iter()
            .map(|(ns_id, ns_slot)| (ns_id.clone(), ns_slot.network.clone()))
            .collect();
        for (ns_id, network) in ns_info {
            writer_hdl
                .send(distvirt_worker_protocol::WorkerCommand::CreateNamespace {
                    namespace_id: ns_id.clone(),
                    network,
                })
                .await;

            let effects = self
                .orchestrator
                .process(OrchestratorInput::NamespaceEvent {
                    namespace_id: ns_id.clone(),
                    event: NamespaceCoreEvent::WorkerConnected {
                        worker_id: global_id,
                        proto_worker_id: proto_worker_id.clone(),
                        info: WorkerInfo {
                            capacity: hello.capabilities.max_pods,
                        },
                    },
                });
            self.execute_effects(effects).await;

            // Notify worker state of namespace assignment.
            let effects = self
                .orchestrator
                .process(OrchestratorInput::WorkerStateEvent(
                    WorkerStateCoreEvent::NamespaceAssigned {
                        worker_id: global_id,
                        namespace_id: ns_id,
                    },
                ));
            self.execute_effects(effects).await;
        }

        self.workers.insert(
            global_id,
            WorkerSlot {
                proto_worker_id,
                writer: writer_hdl,
                capabilities: hello.capabilities,
                reader_handle,
                writer_handle,
                driver,
            },
        );

        Ok(())
    }

    async fn handle_create_namespace(
        &mut self,
        namespace_id: NamespaceId,
        network: distvirt_worker_protocol::NetworkConfig,
    ) {
        if self.namespaces.contains_key(&namespace_id) {
            return;
        }

        let segment_id = self.alloc_segment_id();
        let mut network = network;
        network.segment_id = Some(segment_id);

        // Register segment with worker state.
        let effects = self
            .orchestrator
            .process(OrchestratorInput::WorkerStateEvent(
                WorkerStateCoreEvent::RegisterNamespaceSegment {
                    namespace_id: namespace_id.clone(),
                    segment_id,
                },
            ));
        self.execute_effects(effects).await;

        // Create namespace in orchestrator core.
        let effects = self.orchestrator.process(OrchestratorInput::CreateNamespace {
            namespace_id: namespace_id.clone(),
        });
        self.execute_effects(effects).await;

        // For each connected worker, send CreateNamespace and WorkerConnected.
        let worker_info: Vec<_> = self
            .workers
            .iter()
            .map(|(&wid, slot)| (wid, slot.proto_worker_id.clone(), slot.capabilities.max_pods))
            .collect();
        for (worker_global_id, proto_wid, max_pods) in &worker_info {
            if let Some(slot) = self.workers.get(worker_global_id) {
                slot.writer
                    .send(distvirt_worker_protocol::WorkerCommand::CreateNamespace {
                        namespace_id: namespace_id.clone(),
                        network: network.clone(),
                    })
                    .await;
            }

            let effects = self
                .orchestrator
                .process(OrchestratorInput::NamespaceEvent {
                    namespace_id: namespace_id.clone(),
                    event: NamespaceCoreEvent::WorkerConnected {
                        worker_id: *worker_global_id,
                        proto_worker_id: proto_wid.clone(),
                        info: WorkerInfo {
                            capacity: *max_pods,
                        },
                    },
                });
            self.execute_effects(effects).await;

            let effects = self
                .orchestrator
                .process(OrchestratorInput::WorkerStateEvent(
                    WorkerStateCoreEvent::NamespaceAssigned {
                        worker_id: *worker_global_id,
                        namespace_id: namespace_id.clone(),
                    },
                ));
            self.execute_effects(effects).await;
        }

        self.namespaces.insert(
            namespace_id,
            NamespaceSlot {
                segment_id,
                network,
            },
        );
    }

    async fn handle_destroy_namespace(&mut self, namespace_id: NamespaceId) {
        if let Some(slot) = self.namespaces.remove(&namespace_id) {
            self.free_segment_id(slot.segment_id);

            // Notify worker state of namespace unassignment for all workers.
            let worker_ids: Vec<_> = self.workers.keys().copied().collect();
            for worker_global_id in worker_ids {
                let effects = self
                    .orchestrator
                    .process(OrchestratorInput::WorkerStateEvent(
                        WorkerStateCoreEvent::NamespaceUnassigned {
                            worker_id: worker_global_id,
                            namespace_id: namespace_id.clone(),
                        },
                    ));
                self.execute_effects(effects).await;
            }

            // Unregister segment.
            let effects = self
                .orchestrator
                .process(OrchestratorInput::WorkerStateEvent(
                    WorkerStateCoreEvent::UnregisterNamespaceSegment {
                        namespace_id: namespace_id.clone(),
                    },
                ));
            self.execute_effects(effects).await;

            // Destroy namespace in orchestrator core.
            let effects = self
                .orchestrator
                .process(OrchestratorInput::DestroyNamespace {
                    namespace_id: namespace_id.clone(),
                });
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
    }

    async fn handle_worker_disconnected(&mut self, worker_id: GlobalWorkerId) {
        // Notify all namespaces of worker disconnection.
        for ns_id in self.namespaces.keys().cloned().collect::<Vec<_>>() {
            let effects = self
                .orchestrator
                .process(OrchestratorInput::NamespaceEvent {
                    namespace_id: ns_id.clone(),
                    event: NamespaceCoreEvent::WorkerDisconnected { worker_id },
                });
            self.execute_effects(effects).await;

            let effects = self
                .orchestrator
                .process(OrchestratorInput::WorkerStateEvent(
                    WorkerStateCoreEvent::NamespaceUnassigned {
                        worker_id,
                        namespace_id: ns_id,
                    },
                ));
            self.execute_effects(effects).await;
        }

        // Notify worker state of disconnection.
        let effects = self
            .orchestrator
            .process(OrchestratorInput::WorkerStateEvent(
                WorkerStateCoreEvent::Disconnected { worker_id },
            ));
        self.execute_effects(effects).await;

        self.workers.remove(&worker_id);
    }

    /// Execute effects produced by the sync orchestrator.
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

        // Worker commands.
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
        orchestrator: SyncOrchestrator::new(timer_config),
        next_worker_id: 0,
        next_segment_id: 1,
        active_segment_ids: BTreeSet::new(),
        workers: HashMap::new(),
        namespaces: HashMap::new(),
        timer_handles: HashMap::new(),
        rx,
        self_tx: tx.clone(),
        worker_secret,
    };

    let handle = tokio::spawn(shell.run());
    (ShellHandle { tx }, handle)
}
