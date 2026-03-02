use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::{mpsc, oneshot};

use distvirt_worker_protocol::{OrchestratorConnection, OrchestratorWriter};

use crate::orchestrator::Orchestrator;
use crate::types::*;

pub struct OrchestratorShell {
    orchestrator: Orchestrator,
    workers: HashMap<WorkerId, WorkerHandle>,
    clients: HashMap<ClientId, ClientSender>,
    msg_tx: mpsc::UnboundedSender<ShellMsg>,
    msg_rx: mpsc::UnboundedReceiver<ShellMsg>,
    timer_handles: HashMap<TimerKey, tokio::task::JoinHandle<()>>,
    timer_ns: HashMap<TimerKey, NamespaceId>,
}

struct WorkerHandle {
    writer: OrchestratorWriter,
    _reader_task: tokio::task::JoinHandle<()>,
}

struct ClientSender {
    pending: HashMap<u64, oneshot::Sender<ClientEvent>>,
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
    pub fn new() -> Self {
        let (msg_tx, msg_rx) = mpsc::unbounded_channel();
        OrchestratorShell {
            orchestrator: Orchestrator::new(),
            workers: HashMap::new(),
            clients: HashMap::new(),
            msg_tx,
            msg_rx,
            timer_handles: HashMap::new(),
            timer_ns: HashMap::new(),
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

    /// Add a worker connection. Spawns a reader task that pushes events into the message channel.
    pub async fn add_worker(
        &mut self,
        worker_id: WorkerId,
        capabilities: WorkerCapabilities,
        conn: OrchestratorConnection,
    ) {
        let tx = self.msg_tx.clone();
        let wid = worker_id.clone();

        // Split the connection so the reader task only reads (no cancellation-safety issues)
        // and the shell sends commands directly via the write half.
        let (mut reader, writer, _log_streams) = conn.into_split();

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

        self.workers.insert(
            worker_id.clone(),
            WorkerHandle {
                writer,
                _reader_task: reader_task,
            },
        );

        // Feed WorkerConnected to the orchestrator SM.
        let output = self.orchestrator.step(OrchestratorInput::WorkerConnected {
            worker_id,
            capabilities,
        });
        self.process_output(output).await;
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
                        pending: HashMap::new(),
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
            _ => {}
        }

        let input = match msg {
            ShellMsg::WorkerEvent { worker_id, event } => {
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
            | ShellMsg::ClientCommand { .. } => unreachable!(),
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
            // Wire-only variants — not routed to orchestrator SM.
            ProtoEvent::ShuttingDown => None,
            ProtoEvent::PodLogStreamError { .. } => None,
            ProtoEvent::FabricRouteMiss { .. } => None,
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
    }
}
