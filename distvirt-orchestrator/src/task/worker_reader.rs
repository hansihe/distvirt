use std::collections::HashMap;

use distvirt_worker_protocol::OrchestratorReader;
use distvirt_worker_protocol::WorkerEvent;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::types::NamespaceId;

use super::{
    ArtifactPlacementEvent, GlobalWorkerId, NamespaceEvent, ReaderControl, SchedulerInput,
    WorkerNamespaceEvent, WorkerNamespaceEventKind, WorkerStateEvent,
};

struct WorkerReaderTask {
    global_worker_id: GlobalWorkerId,
    reader: OrchestratorReader,
    namespace_routes: HashMap<NamespaceId, mpsc::Sender<NamespaceEvent>>,
    state_tracker_tx: mpsc::Sender<WorkerStateEvent>,
    scheduler_tx: mpsc::Sender<SchedulerInput>,
    ctrl_rx: mpsc::Receiver<ReaderControl>,
}

impl WorkerReaderTask {
    async fn run(mut self) {
        loop {
            tokio::select! {
                ctrl = self.ctrl_rx.recv() => {
                    match ctrl {
                        Some(ReaderControl::AddNamespaceRoute { namespace_id, tx }) => {
                            self.namespace_routes.insert(namespace_id, tx);
                        }
                        Some(ReaderControl::RemoveNamespaceRoute { namespace_id }) => {
                            self.namespace_routes.remove(&namespace_id);
                        }
                        None => break,
                    }
                }
                event = self.reader.recv_event() => {
                    match event {
                        Ok(event) => self.handle_event(event).await,
                        Err(e) => {
                            eprintln!("worker reader error for {:?}: {}", self.global_worker_id, e);
                            break;
                        }
                    }
                }
            }
        }

        // On exit: notify state tracker and all namespaces of disconnection.
        let _ = self
            .state_tracker_tx
            .send(WorkerStateEvent::Disconnected {
                worker_id: self.global_worker_id,
            })
            .await;

        for (_ns_id, tx) in &self.namespace_routes {
            let _ = tx
                .send(NamespaceEvent::WorkerDisconnected {
                    worker_id: self.global_worker_id,
                })
                .await;
        }
    }

    async fn handle_event(&self, event: WorkerEvent) {
        match event {
            // Namespace-scoped pod events: route with protocol string IDs.
            WorkerEvent::PodRunning { namespace_id, pod_id } => {
                self.send_to_namespace(&namespace_id, WorkerNamespaceEventKind::PodRunning {
                    pod_id,
                }).await;
            }
            WorkerEvent::PodExited { namespace_id, pod_id, exit_code } => {
                self.send_to_namespace(&namespace_id, WorkerNamespaceEventKind::PodExited {
                    pod_id,
                    exit_code,
                }).await;
            }
            WorkerEvent::PodFailed { namespace_id, pod_id, .. } => {
                self.send_to_namespace(&namespace_id, WorkerNamespaceEventKind::PodFailed {
                    pod_id,
                }).await;
            }
            WorkerEvent::PodSuspended { namespace_id, pod_id, artifact_id, .. } => {
                self.send_to_namespace(&namespace_id, WorkerNamespaceEventKind::PodSuspended {
                    pod_id,
                    artifact_id,
                }).await;
            }
            WorkerEvent::PodSuspendFailed { namespace_id, pod_id, .. } => {
                self.send_to_namespace(&namespace_id, WorkerNamespaceEventKind::PodSuspendFailed {
                    pod_id,
                }).await;
            }
            WorkerEvent::ServiceBackendNeed { namespace_id, service_id, need } => {
                self.send_to_namespace(&namespace_id, WorkerNamespaceEventKind::ServiceBackendNeed {
                    service_id,
                    need,
                }).await;
            }

            // Namespace lifecycle events.
            WorkerEvent::NamespaceCreated { namespace_id } => {
                self.send_to_namespace(&namespace_id, WorkerNamespaceEventKind::NamespaceCreated).await;
            }
            WorkerEvent::NamespaceFailed { namespace_id, error } => {
                self.send_to_namespace(&namespace_id, WorkerNamespaceEventKind::NamespaceFailed {
                    error,
                }).await;
            }
            WorkerEvent::NamespaceDestroyed { namespace_id } => {
                let _ = namespace_id;
            }

            // Endpoint events — route to namespace task.
            WorkerEvent::EndpointActivation { namespace_id, ip, service_id } => {
                self.send_to_namespace(&namespace_id, WorkerNamespaceEventKind::EndpointActivation {
                    ip,
                    service_id,
                }).await;
            }
            WorkerEvent::EndpointFlowStatus { namespace_id, ip, service_id, has_active_flows } => {
                self.send_to_namespace(&namespace_id, WorkerNamespaceEventKind::EndpointFlowStatus {
                    ip,
                    service_id,
                    has_active_flows,
                }).await;
            }

            // Artifact events → scheduler (for placement tracking).
            WorkerEvent::ArtifactWriteStarted { artifact_id, pool_id, .. } => {
                let _ = self
                    .scheduler_tx
                    .send(SchedulerInput::ArtifactEvent {
                        worker_id: self.global_worker_id,
                        event: ArtifactPlacementEvent::WriteStarted { artifact_id, pool_id },
                    })
                    .await;
            }
            WorkerEvent::ArtifactWriteCommitted { artifact_id, pool_id, size_bytes, .. } => {
                let _ = self
                    .scheduler_tx
                    .send(SchedulerInput::ArtifactEvent {
                        worker_id: self.global_worker_id,
                        event: ArtifactPlacementEvent::WriteCommitted { artifact_id, pool_id, size_bytes },
                    })
                    .await;
            }

            // Global events: send to state tracker.
            WorkerEvent::PressureUpdate { cpu, memory, io } => {
                let _ = self
                    .state_tracker_tx
                    .send(WorkerStateEvent::PressureUpdate {
                        worker_id: self.global_worker_id,
                        cpu,
                        memory,
                        io,
                    })
                    .await;
            }
            WorkerEvent::PoolCapacityUpdate { pools } => {
                let _ = self
                    .state_tracker_tx
                    .send(WorkerStateEvent::PoolCapacityUpdate {
                        worker_id: self.global_worker_id,
                        pools,
                    })
                    .await;
            }
            WorkerEvent::WorkerCondition {
                key,
                active,
                message,
            } => {
                let _ = self
                    .state_tracker_tx
                    .send(WorkerStateEvent::ConditionUpdate {
                        worker_id: self.global_worker_id,
                        key,
                        active,
                        message,
                    })
                    .await;
            }

            WorkerEvent::ArtifactTransferReceived { dest_artifact_id, dest_pool_id, size_bytes, .. } => {
                let _ = self
                    .scheduler_tx
                    .send(SchedulerInput::ArtifactEvent {
                        worker_id: self.global_worker_id,
                        event: ArtifactPlacementEvent::TransferReceived {
                            artifact_id: dest_artifact_id,
                            pool_id: dest_pool_id,
                            size_bytes,
                        },
                    })
                    .await;
            }
            WorkerEvent::TransferFailed { source_artifact_id, .. } => {
                let _ = self
                    .scheduler_tx
                    .send(SchedulerInput::ArtifactEvent {
                        worker_id: self.global_worker_id,
                        event: ArtifactPlacementEvent::TransferFailed {
                            artifact_id: source_artifact_id,
                        },
                    })
                    .await;
            }

            // Events we don't handle yet.
            WorkerEvent::ShuttingDown
            | WorkerEvent::TunnelStatus { .. }
            | WorkerEvent::PodLogStreamError { .. } => {}
        }
    }

    /// Send a namespace-scoped event to the appropriate namespace task.
    async fn send_to_namespace(
        &self,
        namespace_id: &distvirt_worker_protocol::NamespaceId,
        event_kind: WorkerNamespaceEventKind,
    ) {
        // Convert protocol NamespaceId to our NamespaceId type
        let ns_id = NamespaceId::from(namespace_id.as_ref());
        if let Some(tx) = self.namespace_routes.get(&ns_id) {
            let _ = tx
                .send(NamespaceEvent::WorkerEvent(WorkerNamespaceEvent {
                    worker_id: self.global_worker_id,
                    event: event_kind,
                }))
                .await;
        }
    }
}

/// Spawn a worker reader task. Returns (control_channel, join_handle).
pub(crate) fn spawn(
    global_worker_id: GlobalWorkerId,
    reader: OrchestratorReader,
    state_tracker_tx: mpsc::Sender<WorkerStateEvent>,
    scheduler_tx: mpsc::Sender<SchedulerInput>,
) -> (mpsc::Sender<ReaderControl>, JoinHandle<()>) {
    let (ctrl_tx, ctrl_rx) = mpsc::channel(64);

    let task = WorkerReaderTask {
        global_worker_id,
        reader,
        namespace_routes: HashMap::new(),
        state_tracker_tx,
        scheduler_tx,
        ctrl_rx,
    };

    let handle = tokio::spawn(task.run());
    (ctrl_tx, handle)
}
