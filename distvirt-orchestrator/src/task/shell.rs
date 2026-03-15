use std::collections::{BTreeSet, HashMap};

use distvirt_worker_protocol::OrchestratorConnection;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::adapter::timer::TimerConfig;
use crate::sm_new::WorkerInfo;
use crate::types::NamespaceId;

use super::{
    GlobalWorkerId, NamespaceEvent, ReaderControl, SchedulerInput,
    WorkerStateEvent, WorkerWriterHandle,
};

/// Commands sent to the shell.
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

/// Clonable handle for sending commands to the shell.
#[derive(Clone)]
pub(crate) struct ShellHandle {
    tx: mpsc::Sender<ShellCommand>,
}

impl ShellHandle {
    pub fn worker_connection(&self, conn: OrchestratorConnection) {
        let _ = self.tx.try_send(ShellCommand::WorkerConnection { conn });
    }

    pub async fn create_namespace(&self, namespace_id: NamespaceId, network: distvirt_worker_protocol::NetworkConfig) {
        let _ = self
            .tx
            .send(ShellCommand::CreateNamespace { namespace_id, network })
            .await;
    }

    pub async fn destroy_namespace(&self, namespace_id: NamespaceId) {
        let _ = self
            .tx
            .send(ShellCommand::DestroyNamespace { namespace_id })
            .await;
    }
}

struct WorkerSlot {
    proto_worker_id: distvirt_worker_protocol::WorkerId,
    writer: WorkerWriterHandle,
    reader_ctrl: mpsc::Sender<ReaderControl>,
    #[allow(dead_code)]
    capabilities: distvirt_worker_protocol::WorkerCapabilities,
    #[allow(dead_code)]
    reader_handle: JoinHandle<()>,
    #[allow(dead_code)]
    writer_handle: JoinHandle<()>,
    // Hold driver handle to keep the yamux connection alive.
    #[allow(dead_code)]
    driver: distvirt_worker_protocol::DriverHandle,
}

struct NamespaceSlot {
    event_tx: mpsc::Sender<NamespaceEvent>,
    segment_id: u16,
    network: distvirt_worker_protocol::NetworkConfig,
    #[allow(dead_code)]
    task_handle: JoinHandle<()>,
}

struct Shell {
    next_worker_id: u64,
    next_segment_id: u16,
    active_segment_ids: BTreeSet<u16>,

    // Global infrastructure
    scheduler_tx: mpsc::Sender<SchedulerInput>,
    state_tracker_tx: mpsc::Sender<WorkerStateEvent>,

    // Connected workers
    workers: HashMap<GlobalWorkerId, WorkerSlot>,

    // Active namespaces
    namespaces: HashMap<NamespaceId, NamespaceSlot>,

    // Input
    rx: mpsc::Receiver<ShellCommand>,

    // Config
    worker_secret: String,
    timer_config: TimerConfig,
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
        while let Some(cmd) = self.rx.recv().await {
            match cmd {
                ShellCommand::WorkerConnection { conn } => {
                    if let Err(e) = self.handle_worker_connection(conn).await {
                        eprintln!("worker connection error: {}", e);
                    }
                }
                ShellCommand::CreateNamespace { namespace_id, network } => {
                    self.handle_create_namespace(namespace_id, network).await;
                }
                ShellCommand::DestroyNamespace { namespace_id } => {
                    self.handle_destroy_namespace(namespace_id).await;
                }
            }
        }
    }

    async fn handle_worker_connection(
        &mut self,
        mut conn: OrchestratorConnection,
    ) -> anyhow::Result<()> {
        // Step 1: Handshake
        let hello = conn.recv_hello().await?;

        // Validate auth token.
        if !constant_time_eq(
            hello.auth_token.as_bytes(),
            self.worker_secret.as_bytes(),
        ) {
            anyhow::bail!("worker authentication failed");
        }

        // Assign global ID and protocol ID.
        let global_id = GlobalWorkerId(self.next_worker_id);
        self.next_worker_id += 1;
        let proto_worker_id =
            distvirt_worker_protocol::WorkerId::from(format!("w-{}", global_id.0));

        // Send accepted.
        conn.send_accepted(&distvirt_worker_protocol::WorkerAccepted {
            worker_id: proto_worker_id.clone(),
            adapters: vec![],
            tunnel_encrypted: false,
            pools: vec![],
        })
        .await?;

        let ready = conn.recv_ready().await?;

        // Extract tunnel info from WorkerReady (set after handshake).
        let tunnel_info = match (ready.tunnel_listen_port, ready.tunnel_public_key) {
            (Some(port), Some(key)) => Some(super::worker_state::WorkerTunnelInfo {
                listen_port: port,
                public_key: key,
            }),
            _ => None,
        };

        // Step 2: Split connection
        let (reader, writer, _log_rx, driver) = conn.into_split();

        // Step 3: Create writer channel and spawn writer task
        let (cmd_tx, cmd_rx) = mpsc::channel::<distvirt_worker_protocol::WorkerCommand>(256);
        let writer_handle = tokio::spawn(super::worker_writer::run(cmd_rx, writer));
        let writer_hdl = WorkerWriterHandle::new(cmd_tx);

        // Step 4: Spawn reader task
        let (reader_ctrl, reader_handle) = super::worker_reader::spawn(
            global_id,
            reader,
            self.state_tracker_tx.clone(),
            self.scheduler_tx.clone(),
        );

        // Step 5: Notify state tracker of connection
        let _ = self
            .state_tracker_tx
            .send(WorkerStateEvent::Connected {
                worker_id: global_id,
                capabilities: hello.capabilities.clone(),
                tunnel_info: tunnel_info.clone(),
                proto_worker_id: proto_worker_id.clone(),
                writer: writer_hdl.clone(),
            })
            .await;

        // Step 6: For each active namespace, add route, send CreateNamespace, and send WorkerConnected
        for (ns_id, ns_slot) in &self.namespaces {
            let _ = reader_ctrl
                .send(ReaderControl::AddNamespaceRoute {
                    namespace_id: ns_id.clone(),
                    tx: ns_slot.event_tx.clone(),
                })
                .await;

            // Send CreateNamespace to the new worker for this namespace.
            writer_hdl.send(distvirt_worker_protocol::WorkerCommand::CreateNamespace {
                namespace_id: ns_id.clone(),
                network: ns_slot.network.clone(),
            }).await;

            let _ = ns_slot
                .event_tx
                .send(NamespaceEvent::WorkerConnected {
                    worker_id: global_id,
                    proto_worker_id: proto_worker_id.clone(),
                    info: WorkerInfo {
                        capacity: hello.capabilities.max_pods,
                    },
                    writer: writer_hdl.clone(),
                })
                .await;

            // Notify state tracker of namespace assignment (for worker registry segments).
            let _ = self
                .state_tracker_tx
                .send(WorkerStateEvent::NamespaceAssigned {
                    worker_id: global_id,
                    namespace_id: ns_id.clone(),
                })
                .await;
        }

        self.workers.insert(
            global_id,
            WorkerSlot {
                proto_worker_id,
                writer: writer_hdl,
                reader_ctrl,
                capabilities: hello.capabilities,
                reader_handle,
                writer_handle,
                driver,
            },
        );

        Ok(())
    }

    async fn handle_create_namespace(&mut self, namespace_id: NamespaceId, network: distvirt_worker_protocol::NetworkConfig) {
        if self.namespaces.contains_key(&namespace_id) {
            return;
        }

        // Allocate segment ID for this namespace.
        let segment_id = self.alloc_segment_id();
        let mut network = network;
        network.segment_id = Some(segment_id);

        // Register the segment with the state tracker.
        let _ = self
            .state_tracker_tx
            .send(WorkerStateEvent::RegisterNamespaceSegment {
                namespace_id: namespace_id.clone(),
                segment_id,
            })
            .await;

        // Spawn namespace task.
        let (event_tx, task_handle) = super::namespace::spawn(
            namespace_id.clone(),
            self.scheduler_tx.clone(),
            self.timer_config.clone(),
        );

        // Add routes in all connected workers and send WorkerConnected events.
        for (&worker_global_id, slot) in &self.workers {
            let _ = slot
                .reader_ctrl
                .send(ReaderControl::AddNamespaceRoute {
                    namespace_id: namespace_id.clone(),
                    tx: event_tx.clone(),
                })
                .await;

            // Send CreateNamespace to the worker so it sets up the fabric.
            slot.writer.send(distvirt_worker_protocol::WorkerCommand::CreateNamespace {
                namespace_id: namespace_id.clone(),
                network: network.clone(),
            }).await;

            let _ = event_tx
                .send(NamespaceEvent::WorkerConnected {
                    worker_id: worker_global_id,
                    proto_worker_id: slot.proto_worker_id.clone(),
                    info: WorkerInfo {
                        capacity: slot.capabilities.max_pods,
                    },
                    writer: slot.writer.clone(),
                })
                .await;

            // Notify state tracker of namespace assignment.
            let _ = self
                .state_tracker_tx
                .send(WorkerStateEvent::NamespaceAssigned {
                    worker_id: worker_global_id,
                    namespace_id: namespace_id.clone(),
                })
                .await;
        }

        self.namespaces.insert(
            namespace_id,
            NamespaceSlot {
                event_tx,
                segment_id,
                network,
                task_handle,
            },
        );
    }

    async fn handle_destroy_namespace(&mut self, namespace_id: NamespaceId) {
        if let Some(slot) = self.namespaces.remove(&namespace_id) {
            // Free the segment ID for reuse.
            self.free_segment_id(slot.segment_id);

            // Remove routes from all worker readers and notify state tracker.
            for (&worker_global_id, worker_slot) in &self.workers {
                let _ = worker_slot
                    .reader_ctrl
                    .send(ReaderControl::RemoveNamespaceRoute {
                        namespace_id: namespace_id.clone(),
                    })
                    .await;

                let _ = self
                    .state_tracker_tx
                    .send(WorkerStateEvent::NamespaceUnassigned {
                        worker_id: worker_global_id,
                        namespace_id: namespace_id.clone(),
                    })
                    .await;
            }

            // Unregister the segment.
            let _ = self
                .state_tracker_tx
                .send(WorkerStateEvent::UnregisterNamespaceSegment {
                    namespace_id: namespace_id.clone(),
                })
                .await;

            // Drop the event_tx, which will cause the namespace task to exit.
            drop(slot.event_tx);
            // The task_handle will be dropped, which is fine — the task will exit
            // when its event channel closes.
        }
    }
}

/// Constant-time byte comparison to prevent timing attacks on auth tokens.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    use subtle::ConstantTimeEq;
    if a.len() != b.len() {
        return false;
    }
    a.ct_eq(b).into()
}

/// Spawn the shell. Returns (handle, join_handle).
pub(crate) fn spawn(
    scheduler_tx: mpsc::Sender<SchedulerInput>,
    state_tracker_tx: mpsc::Sender<WorkerStateEvent>,
    worker_secret: String,
    timer_config: TimerConfig,
) -> (ShellHandle, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(64);

    let shell = Shell {
        next_worker_id: 0,
        next_segment_id: 1, // segment 0 is reserved
        active_segment_ids: BTreeSet::new(),
        scheduler_tx,
        state_tracker_tx,
        workers: HashMap::new(),
        namespaces: HashMap::new(),
        rx,
        worker_secret,
        timer_config,
    };

    let handle = tokio::spawn(shell.run());
    (ShellHandle { tx }, handle)
}
