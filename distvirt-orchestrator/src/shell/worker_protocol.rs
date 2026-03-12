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
            ProtoEvent::PressureUpdate { cpu, memory, io } => {
                log::debug!(
                    "worker {} pressure update: cpu={:.1} mem={:.1} io={:.1}",
                    worker_id.0, cpu.some_avg10, memory.some_avg10, io.some_avg10,
                );
                Some(OrchestratorInput::WorkerPressureUpdate { worker_id, cpu, memory, io })
            }
            ProtoEvent::PoolCapacityUpdate { pools } => {
                log::debug!(
                    "worker {} pool capacity update: {} pool(s)",
                    worker_id.0, pools.len(),
                );
                Some(OrchestratorInput::WorkerPoolCapacityUpdate { worker_id, pools })
            }
            ProtoEvent::ArtifactTransferReceived {
                transfer_id,
                dest_artifact_id,
                dest_pool_id,
                size_bytes,
                ..
            } => {
                log::info!(
                    "worker {} artifact transfer received: transfer_id={} artifact={} pool={} size={}",
                    worker_id.0, transfer_id, dest_artifact_id, dest_pool_id, size_bytes,
                );
                Some(OrchestratorInput::WorkerArtifactTransferReceived {
                    worker_id, transfer_id, dest_artifact_id, dest_pool_id, size_bytes,
                })
            }
            ProtoEvent::TransferFailed {
                transfer_id,
                source_artifact_id,
                dest_artifact_id,
                error,
                ..
            } => {
                log::error!(
                    "worker {} artifact transfer failed: transfer_id={} src={} dest={} error={}",
                    worker_id.0, transfer_id, source_artifact_id, dest_artifact_id, error,
                );
                Some(OrchestratorInput::WorkerTransferFailed {
                    worker_id, transfer_id, source_artifact_id, dest_artifact_id, error,
                })
            }
            ProtoEvent::WorkerCondition { key, active, message } => {
                if active {
                    log::info!("worker {} condition asserted: {} — {}", worker_id.0, key, message);
                } else {
                    log::info!("worker {} condition deasserted: {}", worker_id.0, key);
                }
                Some(OrchestratorInput::WorkerConditionUpdate { worker_id, key, active, message })
            }
            // Unified endpoint events — routed directly to domain event handlers.
            ProtoEvent::EndpointActivation { namespace_id, ip, service_id } => {
                Some(OrchestratorInput::NamespaceInput {
                    namespace_id,
                    input: NamespaceInput::WorkerEvent {
                        worker_id,
                        event: WorkerEvent::EndpointActivation { ip, service_id },
                    },
                })
            }
            ProtoEvent::EndpointFlowStatus { namespace_id, ip, service_id, has_active_flows } => {
                Some(OrchestratorInput::NamespaceInput {
                    namespace_id,
                    input: NamespaceInput::WorkerEvent {
                        worker_id,
                        event: WorkerEvent::EndpointFlowStatus { ip, service_id, has_active_flows },
                    },
                })
            }
            // Wire-only variants — not routed to orchestrator SM.
            ProtoEvent::ShuttingDown => None,
            ProtoEvent::PodLogStreamError { .. } => None,
        }
    }
}
