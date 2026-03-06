use crate::types::*;

use super::OrchestratorShell;

impl OrchestratorShell {
    pub(super) fn convert_worker_event(
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
            ProtoEvent::ArtifactWriteStarted {
                namespace_id,
                artifact_id,
                pool_id,
            } => Some(OrchestratorInput::NamespaceInput {
                namespace_id,
                input: NamespaceInput::WorkerEvent {
                    worker_id,
                    event: WorkerEvent::ArtifactWriteStarted { artifact_id, pool_id },
                },
            }),
            ProtoEvent::ArtifactWriteCommitted {
                namespace_id,
                artifact_id,
                pool_id,
                size_bytes,
            } => Some(OrchestratorInput::NamespaceInput {
                namespace_id,
                input: NamespaceInput::WorkerEvent {
                    worker_id,
                    event: WorkerEvent::ArtifactWriteCommitted { artifact_id, pool_id, size_bytes },
                },
            }),
            ProtoEvent::TunnelStatus { .. } => {
                // Informational only for now.
                log::debug!("tunnel status event from worker {}", worker_id.0);
                None
            }
            // WorkerCondition, PoolCapacityUpdate, PressureUpdate, and transfer events are handled directly in handle_msg (needs &mut self).
            ProtoEvent::WorkerCondition { .. } => unreachable!(),
            ProtoEvent::PoolCapacityUpdate { .. } => unreachable!(),
            ProtoEvent::PressureUpdate { .. } => unreachable!(),
            ProtoEvent::ArtifactTransferReceived { .. } => unreachable!(),
            ProtoEvent::TransferFailed { .. } => unreachable!(),
            // Wire-only variants — not routed to orchestrator SM.
            ProtoEvent::ShuttingDown => None,
            ProtoEvent::PodLogStreamError { .. } => None,
        }
    }
}
