use std::collections::HashMap;

use tokio::sync::mpsc;

use distvirt_worker_protocol::{OrchestratorConnection, OrchestratorWriter};

use crate::orchestrator::Orchestrator;
use crate::types::*;

pub struct OrchestratorShell {
    orchestrator: Orchestrator,
    workers: HashMap<WorkerId, WorkerHandle>,
    msg_tx: mpsc::UnboundedSender<ShellMsg>,
    msg_rx: mpsc::UnboundedReceiver<ShellMsg>,
    timer_handles: HashMap<TimerKey, tokio::task::JoinHandle<()>>,
    timer_ns: HashMap<TimerKey, NamespaceId>,
}

struct WorkerHandle {
    writer: OrchestratorWriter,
    _reader_task: tokio::task::JoinHandle<()>,
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
}

impl OrchestratorShell {
    pub fn new() -> Self {
        let (msg_tx, msg_rx) = mpsc::unbounded_channel();
        OrchestratorShell {
            orchestrator: Orchestrator::new(),
            workers: HashMap::new(),
            msg_tx,
            msg_rx,
            timer_handles: HashMap::new(),
            timer_ns: HashMap::new(),
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
