use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

use futures_lite::io::AsyncReadExt;
use tokio::sync::{mpsc, oneshot};
use x25519_dalek::{PublicKey, StaticSecret};

use distvirt_worker_protocol::{OrchestratorConnection, OrchestratorWriter};

use crate::orchestrator::Orchestrator;
use crate::types::*;

/// Maximum number of log chunks buffered per workload.
const LOG_BUFFER_CAP: usize = 500;

#[derive(Clone)]
pub struct LogChunkData {
    pub namespace_id: NamespaceId,
    pub workload_id: WorkloadId,
    pub data: Vec<u8>,
}

struct LogSubscriber {
    namespace_id: NamespaceId,
    workload_id: Option<WorkloadId>, // None = all workloads in namespace
    tx: mpsc::Sender<LogChunkData>,
}

#[derive(Clone)]
pub struct EventData {
    pub namespace_id: NamespaceId,
    pub event: SmNamespaceEvent,
}

struct EventSubscriber {
    namespace_id: NamespaceId,
    workload_ids: HashSet<WorkloadId>,
    service_ids: HashSet<ServiceId>,
    tx: mpsc::Sender<EventData>,
}

pub struct OrchestratorShell {
    orchestrator: Orchestrator,
    workers: HashMap<WorkerId, WorkerHandle>,
    clients: HashMap<ClientId, ClientSender>,
    msg_tx: mpsc::UnboundedSender<ShellMsg>,
    msg_rx: mpsc::UnboundedReceiver<ShellMsg>,
    timer_handles: HashMap<TimerKey, tokio::task::JoinHandle<()>>,
    timer_ns: HashMap<TimerKey, NamespaceId>,
    next_worker_id: u64,
    wg_listen_port: u16,
    tunnel_encrypted: bool,
    worker_pool_configs: Vec<distvirt_worker_protocol::PoolInfo>,
    log_subscribers: Vec<LogSubscriber>,
    log_buffers: HashMap<(NamespaceId, WorkloadId), VecDeque<LogChunkData>>,
    event_subscribers: Vec<EventSubscriber>,
}

struct WorkerHandle {
    writer: OrchestratorWriter,
    _reader_task: tokio::task::JoinHandle<()>,
}

struct ClientSender {
    pending: BTreeMap<u64, oneshot::Sender<ClientEvent>>,
}

enum ShellMsg {
    WorkerEvent {
        worker_id: WorkerId,
        event: distvirt_worker_protocol::WorkerEvent,
    },
    WorkerDisconnected {
        worker_id: WorkerId,
    },
    TimerFired {
        timer_key: TimerKey,
    },
    ClientCommand {
        client_id: ClientId,
        request_id: u64,
        command: ClientCommand,
        response_tx: oneshot::Sender<ClientEvent>,
    },
    ClientConnected {
        client_id: ClientId,
    },
    ClientDisconnected {
        client_id: ClientId,
    },
    WorkerConnection {
        conn: OrchestratorConnection,
    },
    LogData {
        namespace_id: NamespaceId,
        workload_id: WorkloadId,
        data: Vec<u8>,
    },
    SubscribeLogs {
        namespace_id: NamespaceId,
        workload_id: Option<WorkloadId>,
        log_tx: mpsc::Sender<LogChunkData>,
    },
    ResolvePod {
        namespace_id: NamespaceId,
        pod_id: PodId,
        reply: oneshot::Sender<Option<WorkloadId>>,
    },
    SubscribeEvents {
        namespace_id: NamespaceId,
        workload_ids: HashSet<WorkloadId>,
        service_ids: HashSet<ServiceId>,
        event_tx: mpsc::Sender<EventData>,
    },
}

/// Cloneable handle for sending commands to the shell from gRPC handlers.
#[derive(Clone)]
pub struct ShellHandle {
    msg_tx: mpsc::UnboundedSender<ShellMsg>,
    next_client_id: &'static AtomicU64,
    next_request_id: &'static AtomicU64,
}

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

impl ShellHandle {
    /// Allocate a new client ID and register it with the shell.
    pub fn connect_client(&self) -> ClientId {
        let id = ClientId(self.next_client_id.fetch_add(1, Ordering::Relaxed));
        let _ = self.msg_tx.send(ShellMsg::ClientConnected {
            client_id: id.clone(),
        });
        id
    }

    /// Disconnect a client from the shell.
    pub fn disconnect_client(&self, client_id: ClientId) {
        let _ = self.msg_tx.send(ShellMsg::ClientDisconnected { client_id });
    }

    /// Submit a new worker connection to be handled by the shell's run loop.
    pub fn submit_worker_connection(&self, conn: OrchestratorConnection) {
        let _ = self.msg_tx.send(ShellMsg::WorkerConnection { conn });
    }

    /// Subscribe to log output for a namespace (optionally filtered to a workload).
    /// Returns a receiver that yields log chunks (buffered history first, then live).
    pub fn subscribe_logs(
        &self,
        namespace_id: NamespaceId,
        workload_id: Option<WorkloadId>,
    ) -> mpsc::Receiver<LogChunkData> {
        let (tx, rx) = mpsc::channel(256);
        let _ = self.msg_tx.send(ShellMsg::SubscribeLogs {
            namespace_id,
            workload_id,
            log_tx: tx,
        });
        rx
    }

    /// Subscribe to namespace events (optionally filtered to workloads or services).
    /// Returns a receiver that yields events as they occur.
    pub fn subscribe_events(
        &self,
        namespace_id: NamespaceId,
        workload_ids: HashSet<WorkloadId>,
        service_ids: HashSet<ServiceId>,
    ) -> mpsc::Receiver<EventData> {
        let (tx, rx) = mpsc::channel(256);
        let _ = self.msg_tx.send(ShellMsg::SubscribeEvents {
            namespace_id,
            workload_ids,
            service_ids,
            event_tx: tx,
        });
        rx
    }

    /// Send a command and wait for the response.
    pub async fn send_command(
        &self,
        client_id: ClientId,
        command: ClientCommand,
    ) -> Result<ClientEvent, anyhow::Error> {
        let (tx, rx) = oneshot::channel();
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        self.msg_tx
            .send(ShellMsg::ClientCommand {
                client_id,
                request_id,
                command,
                response_tx: tx,
            })
            .map_err(|_| anyhow::anyhow!("shell closed"))?;
        rx.await.map_err(|_| anyhow::anyhow!("shell dropped response channel"))
    }
}

impl OrchestratorShell {
    pub fn new(wg_listen_port: u16, tunnel_encrypted: bool, worker_pool_configs: Vec<distvirt_worker_protocol::PoolInfo>) -> Self {
        let (msg_tx, msg_rx) = mpsc::unbounded_channel();
        OrchestratorShell {
            orchestrator: Orchestrator::new(),
            workers: HashMap::new(),
            clients: HashMap::new(),
            msg_tx,
            msg_rx,
            timer_handles: HashMap::new(),
            timer_ns: HashMap::new(),
            next_worker_id: 1,
            wg_listen_port,
            tunnel_encrypted,
            worker_pool_configs,
            log_subscribers: Vec::new(),
            log_buffers: HashMap::new(),
            event_subscribers: Vec::new(),
        }
    }

    /// Create a cloneable handle for sending commands from gRPC handlers.
    pub fn handle(&self) -> ShellHandle {
        ShellHandle {
            msg_tx: self.msg_tx.clone(),
            next_client_id: &NEXT_CLIENT_ID,
            next_request_id: &NEXT_REQUEST_ID,
        }
    }

    /// Add a worker connection. Performs the three-step handshake (WorkerHello →
    /// WorkerAccepted → WorkerReady), then spawns a reader task that pushes
    /// events into the message channel. Returns the assigned worker ID.
    pub async fn add_worker(
        &mut self,
        mut conn: OrchestratorConnection,
    ) -> anyhow::Result<WorkerId> {
        // Allocate worker ID.
        let id = self.next_worker_id;
        self.next_worker_id += 1;
        let worker_id = WorkerId::from(format!("w-{}", id));

        // Handshake: recv hello, send accepted, recv ready.
        let hello = conn.recv_hello().await?;

        // Build adapter configs. If worker supports WireGuard, generate a keypair.
        let mut adapters = vec![];
        let mut wg_config = None;

        if hello.capabilities.available_adapters.iter().any(|a| a == "wireguard") {
            let private_key = StaticSecret::random_from_rng(rand::thread_rng());
            let public_key = PublicKey::from(&private_key);
            let listen_port = self.wg_listen_port;

            adapters.push(distvirt_worker_protocol::AdapterConfig::WireGuard {
                listen_port,
                private_key: private_key.to_bytes().to_vec(),
            });

            wg_config = Some(WorkerWgConfig {
                listen_port,
                public_key: public_key.to_bytes(),
            });
        }

        // Auth validation placeholder — always accept.
        conn.send_accepted(&distvirt_worker_protocol::WorkerAccepted {
            worker_id: worker_id.clone(),
            adapters,
            tunnel_encrypted: self.tunnel_encrypted,
            pools: self.worker_pool_configs.clone(),
        })
        .await?;
        let ready = conn.recv_ready().await?;

        // Map protocol capabilities to orchestrator capabilities.
        // Merge pushed pools into the worker's self-reported pools so the
        // orchestrator's view matches the worker's actual pool set.
        let mut pools = hello.capabilities.pools.clone();
        for pushed in &self.worker_pool_configs {
            if !pools.iter().any(|p| p.pool_id == pushed.pool_id) {
                pools.push(pushed.clone());
            }
        }
        let capabilities = WorkerCapabilities {
            max_pods: hello.capabilities.max_pods,
            available_memory_mb: hello.capabilities.available_memory_mb,
            public_endpoint: hello.capabilities.public_endpoint.clone(),
            pools,
        };

        // Extract tunnel config from WorkerReady (set after handshake so the
        // worker knows whether to enable encryption).
        let tunnel_config = match (
            ready.tunnel_listen_port,
            ready.tunnel_public_key,
        ) {
            (Some(port), Some(key)) => Some(WorkerTunnelConfig {
                listen_port: port,
                public_key: key,
            }),
            _ => None,
        };

        let tx = self.msg_tx.clone();
        let wid = worker_id.clone();

        // Split the connection so the reader task only reads (no cancellation-safety issues)
        // and the shell sends commands directly via the write half.
        let (mut reader, writer, mut log_streams) = conn.into_split();

        let reader_task = tokio::spawn(async move {
            loop {
                match reader.recv_event().await {
                    Ok(event) => {
                        if tx
                            .send(ShellMsg::WorkerEvent {
                                worker_id: wid.clone(),
                                event,
                            })
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(_) => {
                        let _ = tx.send(ShellMsg::WorkerDisconnected {
                            worker_id: wid.clone(),
                        });
                        break;
                    }
                }
            }
        });

        // Spawn a task that accepts incoming log streams from this worker.
        let log_tx = self.msg_tx.clone();
        tokio::spawn(async move {
            while let Some(mut stream) = log_streams.recv().await {
                // Read the log stream header to learn which pod this is for.
                let header = match distvirt_worker_protocol::codec::recv_log_header(&mut stream).await {
                    Ok(h) => h,
                    Err(e) => {
                        log::warn!("failed to read log stream header: {}", e);
                        continue;
                    }
                };

                // Resolve pod_id → workload_id via the shell.
                let (reply_tx, reply_rx) = oneshot::channel();
                if log_tx
                    .send(ShellMsg::ResolvePod {
                        namespace_id: header.namespace_id.clone(),
                        pod_id: header.pod_id.clone(),
                        reply: reply_tx,
                    })
                    .is_err()
                {
                    break;
                }

                let workload_id = match reply_rx.await {
                    Ok(Some(wid)) => wid,
                    Ok(None) => {
                        log::warn!(
                            "log stream for unknown pod {}/{}, ignoring",
                            header.namespace_id.0,
                            header.pod_id.0
                        );
                        continue;
                    }
                    Err(_) => break,
                };

                // Spawn a per-stream reader that forwards bytes as LogData messages.
                let ns_id = header.namespace_id.clone();
                let wl_id = workload_id.clone();
                let stream_tx = log_tx.clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 4096];
                    loop {
                        match stream.read(&mut buf).await {
                            Ok(0) => break,
                            Ok(n) => {
                                if stream_tx
                                    .send(ShellMsg::LogData {
                                        namespace_id: ns_id.clone(),
                                        workload_id: wl_id.clone(),
                                        data: buf[..n].to_vec(),
                                    })
                                    .is_err()
                                {
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                });
            }
        });

        self.workers.insert(
            worker_id.clone(),
            WorkerHandle {
                writer,
                _reader_task: reader_task,
            },
        );

        // Feed WorkerConnected to the orchestrator SM.
        let output = self.orchestrator.step(OrchestratorInput::WorkerConnected {
            worker_id: worker_id.clone(),
            capabilities,
            wg_config,
            tunnel_config,
        });
        self.process_output(output).await;

        Ok(worker_id)
    }

    /// Feed a client command into the orchestrator.
    pub async fn client_command(&mut self, client_id: ClientId, command: ClientCommand) {
        let output = self.orchestrator.step(OrchestratorInput::ClientCommand {
            client_id,
            command,
        });
        self.process_output(output).await;
    }

    /// Access the orchestrator state (for assertions in tests).
    pub fn orchestrator(&self) -> &Orchestrator {
        &self.orchestrator
    }

    /// Process one pending message. Returns false if no more messages.
    pub async fn step(&mut self) -> bool {
        match self.msg_rx.try_recv() {
            Ok(msg) => {
                self.handle_msg(msg).await;
                true
            }
            Err(_) => false,
        }
    }

    /// Drain all pending messages.
    pub async fn drain(&mut self) {
        while self.step().await {}
    }

    /// Run until all workers disconnect.
    pub async fn run(&mut self) -> anyhow::Result<()> {
        while let Some(msg) = self.msg_rx.recv().await {
            self.handle_msg(msg).await;
        }
        Ok(())
    }

    async fn handle_msg(&mut self, msg: ShellMsg) {
        match msg {
            ShellMsg::ClientConnected { client_id } => {
                self.clients.insert(
                    client_id.clone(),
                    ClientSender {
                        pending: BTreeMap::new(),
                    },
                );
                let output = self.orchestrator.step(OrchestratorInput::ClientConnected {
                    client_id,
                });
                self.process_output(output).await;
                return;
            }
            ShellMsg::ClientDisconnected { client_id } => {
                self.clients.remove(&client_id);
                let output = self.orchestrator.step(OrchestratorInput::ClientDisconnected {
                    client_id,
                });
                self.process_output(output).await;
                return;
            }
            ShellMsg::ClientCommand {
                client_id,
                request_id,
                command,
                response_tx,
            } => {
                // Register the pending response before stepping.
                if let Some(client) = self.clients.get_mut(&client_id) {
                    client.pending.insert(request_id, response_tx);
                }
                let output = self.orchestrator.step(OrchestratorInput::ClientCommand {
                    client_id: client_id.clone(),
                    command,
                });
                self.process_output(output).await;
                // Route any client events produced for this client to the pending sender.
                // (already handled in process_output)
                return;
            }
            ShellMsg::WorkerConnection { conn } => {
                match self.add_worker(conn).await {
                    Ok(worker_id) => log::info!("worker connected: {}", worker_id.0),
                    Err(e) => log::error!("worker handshake failed: {}", e),
                }
                return;
            }
            ShellMsg::LogData {
                namespace_id,
                workload_id,
                data,
            } => {
                let chunk = LogChunkData {
                    namespace_id: namespace_id.clone(),
                    workload_id: workload_id.clone(),
                    data,
                };

                // Buffer the chunk.
                let buf = self
                    .log_buffers
                    .entry((namespace_id.clone(), workload_id.clone()))
                    .or_insert_with(VecDeque::new);
                buf.push_back(chunk.clone());
                if buf.len() > LOG_BUFFER_CAP {
                    buf.pop_front();
                }

                // Distribute to matching live subscribers, removing closed ones.
                self.log_subscribers.retain(|sub| {
                    if sub.namespace_id != namespace_id {
                        return true;
                    }
                    if let Some(ref wl) = sub.workload_id {
                        if *wl != workload_id {
                            return true;
                        }
                    }
                    // Skip full channels, remove closed ones.
                    match sub.tx.try_send(chunk.clone()) {
                        Ok(()) => true,
                        Err(mpsc::error::TrySendError::Full(_)) => true,
                        Err(mpsc::error::TrySendError::Closed(_)) => false,
                    }
                });
                return;
            }
            ShellMsg::SubscribeLogs {
                namespace_id,
                workload_id,
                log_tx,
            } => {
                // Replay buffered history to the new subscriber.
                for ((ns, wl), buf) in &self.log_buffers {
                    if *ns != namespace_id {
                        continue;
                    }
                    if let Some(ref filter_wl) = workload_id {
                        if *wl != *filter_wl {
                            continue;
                        }
                    }
                    for chunk in buf {
                        if log_tx.try_send(chunk.clone()).is_err() {
                            break;
                        }
                    }
                }

                // Add to live subscriber list.
                self.log_subscribers.push(LogSubscriber {
                    namespace_id,
                    workload_id,
                    tx: log_tx,
                });
                return;
            }
            ShellMsg::ResolvePod {
                namespace_id,
                pod_id,
                reply,
            } => {
                let workload_id = self
                    .orchestrator
                    .namespaces
                    .get(&namespace_id)
                    .and_then(|ns| ns.pod_map.get(&pod_id))
                    .map(|info| info.workload_id.clone());
                let _ = reply.send(workload_id);
                return;
            }
            ShellMsg::SubscribeEvents {
                namespace_id,
                workload_ids,
                service_ids,
                event_tx,
            } => {
                self.event_subscribers.push(EventSubscriber {
                    namespace_id,
                    workload_ids,
                    service_ids,
                    tx: event_tx,
                });
                return;
            }
            _ => {}
        }

        let input = match msg {
            ShellMsg::WorkerEvent { worker_id, event } => {
                // Handle worker-scoped conditions directly (not routed to namespace SM).
                // Handle pool capacity updates directly (worker-scoped, not routed to namespace SM).
                if let distvirt_worker_protocol::WorkerEvent::PoolCapacityUpdate {
                    ref pools,
                } = event
                {
                    if let Some(ws) = self.orchestrator.workers.get_mut(&worker_id) {
                        log::debug!(
                            "worker {} pool capacity update: {} pool(s)",
                            worker_id.0,
                            pools.len()
                        );
                        for new_pool in pools {
                            if let Some(existing) = ws.capabilities.pools.iter_mut().find(|p| p.pool_id == new_pool.pool_id) {
                                existing.capacity_bytes = new_pool.capacity_bytes;
                                existing.available_bytes = new_pool.available_bytes;
                            }
                        }
                    }
                    return;
                }

                // Handle worker-scoped conditions directly (not routed to namespace SM).
                if let distvirt_worker_protocol::WorkerEvent::WorkerCondition {
                    ref key,
                    active,
                    ref message,
                } = event
                {
                    if let Some(ws) = self.orchestrator.workers.get_mut(&worker_id) {
                        if active {
                            log::info!(
                                "worker {} condition asserted: {} — {}",
                                worker_id.0, key, message
                            );
                            ws.conditions.insert(
                                key.clone(),
                                WorkerCondition {
                                    active: true,
                                    message: message.clone(),
                                },
                            );
                        } else {
                            log::info!("worker {} condition deasserted: {}", worker_id.0, key);
                            ws.conditions.remove(key);
                        }
                    }
                    return;
                }
                self.convert_worker_event(worker_id, event)
            }
            ShellMsg::WorkerDisconnected { worker_id } => {
                self.workers.remove(&worker_id);
                Some(OrchestratorInput::WorkerDisconnected { worker_id })
            }
            ShellMsg::TimerFired { timer_key } => {
                self.timer_handles.remove(&timer_key);
                if let Some(ns_id) = self.timer_ns.remove(&timer_key) {
                    Some(OrchestratorInput::NamespaceInput {
                        namespace_id: ns_id,
                        input: NamespaceInput::TimerFired { timer_key },
                    })
                } else {
                    None
                }
            }
            // Already handled above.
            ShellMsg::ClientConnected { .. }
            | ShellMsg::ClientDisconnected { .. }
            | ShellMsg::ClientCommand { .. }
            | ShellMsg::WorkerConnection { .. }
            | ShellMsg::LogData { .. }
            | ShellMsg::SubscribeLogs { .. }
            | ShellMsg::ResolvePod { .. }
            | ShellMsg::SubscribeEvents { .. } => unreachable!(),
        };

        if let Some(input) = input {
            let output = self.orchestrator.step(input);
            self.process_output(output).await;
        }
    }

    fn convert_worker_event(
        &self,
        worker_id: WorkerId,
        event: distvirt_worker_protocol::WorkerEvent,
    ) -> Option<OrchestratorInput> {
        use distvirt_worker_protocol::WorkerEvent as ProtoEvent;
        match event {
            ProtoEvent::NamespaceCreated { namespace_id } => {
                Some(OrchestratorInput::NamespaceInput {
                    namespace_id,
                    input: NamespaceInput::WorkerEvent {
                        worker_id,
                        event: WorkerEvent::NamespaceCreated,
                    },
                })
            }
            ProtoEvent::PodRunning {
                namespace_id,
                pod_id,
            } => Some(OrchestratorInput::NamespaceInput {
                namespace_id,
                input: NamespaceInput::WorkerEvent {
                    worker_id,
                    event: WorkerEvent::PodRunning { pod_id },
                },
            }),
            ProtoEvent::PodExited {
                namespace_id,
                pod_id,
                exit_code,
            } => Some(OrchestratorInput::NamespaceInput {
                namespace_id,
                input: NamespaceInput::WorkerEvent {
                    worker_id,
                    event: WorkerEvent::PodExited { pod_id, exit_code },
                },
            }),
            ProtoEvent::PodFailed {
                namespace_id,
                pod_id,
                error,
            } => Some(OrchestratorInput::NamespaceInput {
                namespace_id,
                input: NamespaceInput::WorkerEvent {
                    worker_id,
                    event: WorkerEvent::PodFailed { pod_id, error },
                },
            }),
            ProtoEvent::NamespaceFailed {
                namespace_id,
                error,
            } => Some(OrchestratorInput::NamespaceInput {
                namespace_id,
                input: NamespaceInput::WorkerEvent {
                    worker_id,
                    event: WorkerEvent::NamespaceFailed { error },
                },
            }),
            ProtoEvent::ServiceActivation {
                namespace_id,
                service_id,
                ..
            } => Some(OrchestratorInput::NamespaceInput {
                namespace_id,
                input: NamespaceInput::WorkerEvent {
                    worker_id,
                    event: WorkerEvent::ServiceActivation { service_id },
                },
            }),
            ProtoEvent::ServiceBackendNeed {
                namespace_id,
                service_id,
                need,
            } => Some(OrchestratorInput::NamespaceInput {
                namespace_id,
                input: NamespaceInput::WorkerEvent {
                    worker_id,
                    event: WorkerEvent::ServiceBackendNeed { service_id, need },
                },
            }),
            ProtoEvent::NamespaceDestroyed { namespace_id } => {
                Some(OrchestratorInput::NamespaceInput {
                    namespace_id,
                    input: NamespaceInput::WorkerEvent {
                        worker_id,
                        event: WorkerEvent::NamespaceDestroyed,
                    },
                })
            }
            ProtoEvent::PodSuspended {
                namespace_id,
                pod_id,
                artifact_id,
                pool_id,
                ..
            } => Some(OrchestratorInput::NamespaceInput {
                namespace_id,
                input: NamespaceInput::WorkerEvent {
                    worker_id,
                    event: WorkerEvent::PodSuspended { pod_id, artifact_id, pool_id },
                },
            }),
            ProtoEvent::PodSuspendFailed {
                namespace_id,
                pod_id,
                error,
            } => Some(OrchestratorInput::NamespaceInput {
                namespace_id,
                input: NamespaceInput::WorkerEvent {
                    worker_id,
                    event: WorkerEvent::PodSuspendFailed { pod_id, error },
                },
            }),
            ProtoEvent::FabricRouteMiss {
                namespace_id,
                dst_ip,
            } => Some(OrchestratorInput::NamespaceInput {
                namespace_id,
                input: NamespaceInput::WorkerEvent {
                    worker_id,
                    event: WorkerEvent::FabricRouteMiss { dst_ip },
                },
            }),
            ProtoEvent::TunnelStatus { .. } => {
                // Informational only for now.
                log::debug!("tunnel status event from worker {}", worker_id.0);
                None
            }
            // WorkerCondition and PoolCapacityUpdate are handled directly in handle_msg (needs &mut self).
            ProtoEvent::WorkerCondition { .. } => unreachable!(),
            ProtoEvent::PoolCapacityUpdate { .. } => unreachable!(),
            // Wire-only variants — not routed to orchestrator SM.
            ProtoEvent::ShuttingDown => None,
            ProtoEvent::PodLogStreamError { .. } => None,
        }
    }

    async fn process_output(&mut self, output: OrchestratorOutput) {
        // Track timer -> namespace mappings from namespace_outputs.
        for (ns_id, ns_out) in &output.namespace_outputs {
            for (timer_key, _) in &ns_out.timers_set {
                self.timer_ns.insert(timer_key.clone(), ns_id.clone());
            }
        }

        // Route client events to pending response senders.
        for (client_id, event) in output.client_events {
            if let Some(client) = self.clients.get_mut(&client_id) {
                // Send to the first pending sender (FIFO — commands are serialized per client).
                if let Some((&req_id, _)) = client.pending.iter().next() {
                    if let Some(tx) = client.pending.remove(&req_id) {
                        let _ = tx.send(event);
                    }
                }
            }
        }

        // Also route client events from namespace outputs.
        for (_ns_id, ns_out) in &output.namespace_outputs {
            for (client_id, event) in &ns_out.client_events {
                if let Some(client) = self.clients.get_mut(client_id) {
                    if let Some((&req_id, _)) = client.pending.iter().next() {
                        if let Some(tx) = client.pending.remove(&req_id) {
                            let _ = tx.send(event.clone());
                        }
                    }
                }
            }
        }

        // Send commands to workers.
        for (worker_id, cmd) in &output.worker_commands {
            if let Some(handle) = self.workers.get_mut(worker_id) {
                let _ = handle.writer.send_command(cmd).await;
            }
        }

        // Set timers.
        for (timer_key, duration) in &output.timers_set {
            let tx = self.msg_tx.clone();
            let key = timer_key.clone();
            let dur = *duration;
            let handle = tokio::spawn(async move {
                tokio::time::sleep(dur).await;
                let _ = tx.send(ShellMsg::TimerFired { timer_key: key });
            });
            self.timer_handles.insert(timer_key.clone(), handle);
        }

        // Cancel timers.
        for timer_key in &output.timers_cancel {
            if let Some(handle) = self.timer_handles.remove(timer_key) {
                handle.abort();
            }
            self.timer_ns.remove(timer_key);
        }

        // Distribute namespace events to matching subscribers.
        for (ns_id, ns_out) in &output.namespace_outputs {
            for sm_event in &ns_out.events {
                // Extract workload_id and service_id for filtering.
                let (event_wl_id, event_svc_id) = match sm_event {
                    SmNamespaceEvent::Workload { workload_id, .. } => {
                        (Some(workload_id), None)
                    }
                    SmNamespaceEvent::Service {
                        workload_id,
                        service_id,
                        ..
                    } => (Some(workload_id), Some(service_id)),
                };

                let event_data = EventData {
                    namespace_id: ns_id.clone(),
                    event: sm_event.clone(),
                };

                self.event_subscribers.retain(|sub| {
                    if sub.namespace_id != *ns_id {
                        return true;
                    }
                    // Apply workload filter (empty = no filter).
                    if !sub.workload_ids.is_empty() {
                        if event_wl_id.map_or(true, |wl| !sub.workload_ids.contains(wl)) {
                            return true; // Keep subscriber, just doesn't match this event.
                        }
                    }
                    // Apply service filter (empty = no filter).
                    if !sub.service_ids.is_empty() {
                        if event_svc_id.map_or(true, |svc| !sub.service_ids.contains(svc)) {
                            return true;
                        }
                    }
                    match sub.tx.try_send(event_data.clone()) {
                        Ok(()) => true,
                        Err(mpsc::error::TrySendError::Full(_)) => true,
                        Err(mpsc::error::TrySendError::Closed(_)) => false,
                    }
                });
            }
        }
    }
}
