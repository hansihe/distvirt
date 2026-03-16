//! Per-connection wire decoder for the new async shell.
//!
//! This is structurally identical to `task/worker_reader.rs` but sends events
//! into the shell's unified channel rather than to separate namespace tasks.

use std::collections::HashSet;

use distvirt_worker_protocol::OrchestratorReader;
use distvirt_worker_protocol::WorkerEvent;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::core::types::{
    ArtifactPlacementEvent, SchedulerCoreInput, WorkerNamespaceEventKind, WorkerStateCoreEvent,
};
use crate::task::{GlobalWorkerId, WorkerNamespaceEvent};
use crate::types::NamespaceId;

use super::ShellEvent;

/// Spawn a worker reader task that decodes wire events and sends them
/// to the shell's event channel as `ShellEvent` variants.
pub(crate) fn spawn(
    global_worker_id: GlobalWorkerId,
    reader: OrchestratorReader,
    shell_tx: mpsc::Sender<ShellEvent>,
    namespace_ids: HashSet<NamespaceId>,
) -> JoinHandle<()> {
    let task = WorkerReaderTask {
        global_worker_id,
        reader,
        namespace_ids,
        shell_tx,
    };
    tokio::spawn(task.run())
}

struct WorkerReaderTask {
    global_worker_id: GlobalWorkerId,
    reader: OrchestratorReader,
    namespace_ids: HashSet<NamespaceId>,
    shell_tx: mpsc::Sender<ShellEvent>,
}

impl WorkerReaderTask {
    async fn run(mut self) {
        loop {
            match self.reader.recv_event().await {
                Ok(event) => self.handle_event(event).await,
                Err(e) => {
                    eprintln!(
                        "worker reader error for {:?}: {}",
                        self.global_worker_id, e
                    );
                    break;
                }
            }
        }

        // On exit: notify shell of disconnection.
        let _ = self
            .shell_tx
            .send(ShellEvent::WorkerDisconnected {
                worker_id: self.global_worker_id,
            })
            .await;
    }

    async fn handle_event(&self, event: WorkerEvent) {
        match event {
            // Namespace-scoped pod events.
            WorkerEvent::PodRunning { namespace_id, pod_id } => {
                self.send_ns_event(&namespace_id, WorkerNamespaceEventKind::PodRunning { pod_id })
                    .await;
            }
            WorkerEvent::PodExited {
                namespace_id,
                pod_id,
                exit_code,
            } => {
                self.send_ns_event(
                    &namespace_id,
                    WorkerNamespaceEventKind::PodExited { pod_id, exit_code },
                )
                .await;
            }
            WorkerEvent::PodFailed {
                namespace_id,
                pod_id,
                ..
            } => {
                self.send_ns_event(&namespace_id, WorkerNamespaceEventKind::PodFailed { pod_id })
                    .await;
            }
            WorkerEvent::PodSuspended {
                namespace_id,
                pod_id,
                artifact_id,
                ..
            } => {
                self.send_ns_event(
                    &namespace_id,
                    WorkerNamespaceEventKind::PodSuspended {
                        pod_id,
                        artifact_id,
                    },
                )
                .await;
            }
            WorkerEvent::PodSuspendFailed {
                namespace_id,
                pod_id,
                ..
            } => {
                self.send_ns_event(
                    &namespace_id,
                    WorkerNamespaceEventKind::PodSuspendFailed { pod_id },
                )
                .await;
            }
            WorkerEvent::ServiceBackendNeed {
                namespace_id,
                service_id,
                need,
            } => {
                self.send_ns_event(
                    &namespace_id,
                    WorkerNamespaceEventKind::ServiceBackendNeed { service_id, need },
                )
                .await;
            }

            // Namespace lifecycle events.
            WorkerEvent::NamespaceCreated { namespace_id } => {
                self.send_ns_event(&namespace_id, WorkerNamespaceEventKind::NamespaceCreated)
                    .await;
            }
            WorkerEvent::NamespaceFailed {
                namespace_id,
                error,
            } => {
                self.send_ns_event(
                    &namespace_id,
                    WorkerNamespaceEventKind::NamespaceFailed { error },
                )
                .await;
            }
            WorkerEvent::NamespaceDestroyed { .. } => {}

            // Endpoint events.
            WorkerEvent::EndpointActivation {
                namespace_id,
                ip,
                service_id,
            } => {
                self.send_ns_event(
                    &namespace_id,
                    WorkerNamespaceEventKind::EndpointActivation { ip, service_id },
                )
                .await;
            }
            WorkerEvent::EndpointFlowStatus {
                namespace_id,
                ip,
                service_id,
                has_active_flows,
            } => {
                self.send_ns_event(
                    &namespace_id,
                    WorkerNamespaceEventKind::EndpointFlowStatus {
                        ip,
                        service_id,
                        has_active_flows,
                    },
                )
                .await;
            }

            // Artifact events → scheduler.
            WorkerEvent::ArtifactWriteStarted {
                artifact_id,
                pool_id,
                ..
            } => {
                let _ = self
                    .shell_tx
                    .send(ShellEvent::SchedulerInput(SchedulerCoreInput::ArtifactEvent {
                        worker_id: self.global_worker_id,
                        event: ArtifactPlacementEvent::WriteStarted {
                            artifact_id,
                            pool_id,
                        },
                    }))
                    .await;
            }
            WorkerEvent::ArtifactWriteCommitted {
                artifact_id,
                pool_id,
                size_bytes,
                ..
            } => {
                let _ = self
                    .shell_tx
                    .send(ShellEvent::SchedulerInput(SchedulerCoreInput::ArtifactEvent {
                        worker_id: self.global_worker_id,
                        event: ArtifactPlacementEvent::WriteCommitted {
                            artifact_id,
                            pool_id,
                            size_bytes,
                        },
                    }))
                    .await;
            }
            WorkerEvent::ArtifactTransferReceived {
                dest_artifact_id,
                dest_pool_id,
                size_bytes,
                ..
            } => {
                let _ = self
                    .shell_tx
                    .send(ShellEvent::SchedulerInput(SchedulerCoreInput::ArtifactEvent {
                        worker_id: self.global_worker_id,
                        event: ArtifactPlacementEvent::TransferReceived {
                            artifact_id: dest_artifact_id,
                            pool_id: dest_pool_id,
                            size_bytes,
                        },
                    }))
                    .await;
            }
            WorkerEvent::TransferFailed {
                source_artifact_id, ..
            } => {
                let _ = self
                    .shell_tx
                    .send(ShellEvent::SchedulerInput(SchedulerCoreInput::ArtifactEvent {
                        worker_id: self.global_worker_id,
                        event: ArtifactPlacementEvent::TransferFailed {
                            artifact_id: source_artifact_id,
                        },
                    }))
                    .await;
            }

            // Global worker state events.
            WorkerEvent::PressureUpdate { cpu, memory, io } => {
                let _ = self
                    .shell_tx
                    .send(ShellEvent::WorkerStateEvent(
                        WorkerStateCoreEvent::PressureUpdate {
                            worker_id: self.global_worker_id,
                            cpu,
                            memory,
                            io,
                        },
                    ))
                    .await;
            }
            WorkerEvent::PoolCapacityUpdate { pools } => {
                let _ = self
                    .shell_tx
                    .send(ShellEvent::WorkerStateEvent(
                        WorkerStateCoreEvent::PoolCapacityUpdate {
                            worker_id: self.global_worker_id,
                            pools,
                        },
                    ))
                    .await;
            }
            WorkerEvent::WorkerCondition {
                key,
                active,
                message,
            } => {
                let _ = self
                    .shell_tx
                    .send(ShellEvent::WorkerStateEvent(
                        WorkerStateCoreEvent::ConditionUpdate {
                            worker_id: self.global_worker_id,
                            key,
                            active,
                            message,
                        },
                    ))
                    .await;
            }

            WorkerEvent::ShuttingDown
            | WorkerEvent::TunnelStatus { .. }
            | WorkerEvent::PodLogStreamError { .. } => {}
        }
    }

    async fn send_ns_event(
        &self,
        namespace_id: &distvirt_worker_protocol::NamespaceId,
        event_kind: WorkerNamespaceEventKind,
    ) {
        let ns_id = NamespaceId::from(namespace_id.as_ref());
        if self.namespace_ids.contains(&ns_id) {
            let _ = self
                .shell_tx
                .send(ShellEvent::NamespaceEvent {
                    namespace_id: ns_id,
                    event: crate::core::types::NamespaceCoreEvent::WorkerEvent(
                        WorkerNamespaceEvent {
                            worker_id: self.global_worker_id,
                            event: event_kind,
                        },
                    ),
                })
                .await;
        }
    }
}
