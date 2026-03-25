//! Pure classification of wire-level `WorkerEvent` into orchestrator destinations.
//!
//! The async reader calls `classify()`, gets back a destination enum, and
//! blindly sends it to the shell channel. No domain logic in the reader.

use distvirt_worker_protocol::WorkerEvent;

use crate::{
    core::{
        ArtifactPlacementEvent, EndpointDemandSignal, GlobalWorkerId, WorkerNamespaceEvent,
        WorkerNamespaceEventKind,
    },
    types::NamespaceId,
};

use super::types::{OrchestratorToNamespace, SchedulerCoreInput, WorkerStateCoreEvent};

/// Where a wire-level WorkerEvent should be routed.
pub(crate) enum ClassifiedWorkerEvent {
    /// Namespace-scoped event — route to the namespace directly.
    Namespace {
        namespace_id: NamespaceId,
        event: OrchestratorToNamespace,
    },
    /// Global worker state event — route through OrchestratorCore as WorkerStateEvent.
    WorkerState(WorkerStateCoreEvent),
    /// Scheduler-bound event (artifact placement) — route as SchedulerEvent.
    Scheduler(SchedulerCoreInput),
    /// Event that the orchestrator doesn't handle.
    Ignored,
}

/// Classify a wire-level `WorkerEvent` into its orchestrator destination.
///
/// Pure function — no I/O, no state. The reader calls this and forwards the
/// result to the shell's event channel.
pub(crate) fn classify(worker_id: GlobalWorkerId, event: WorkerEvent) -> ClassifiedWorkerEvent {
    match event {
        // =====================================================================
        // Namespace-scoped events
        // =====================================================================
        WorkerEvent::PodRunning {
            namespace_id,
            pod_id,
        } => ns_event(
            &namespace_id,
            worker_id,
            WorkerNamespaceEventKind::PodRunning { pod_id },
        ),
        WorkerEvent::PodExited {
            namespace_id,
            pod_id,
            exit_code,
        } => ns_event(
            &namespace_id,
            worker_id,
            WorkerNamespaceEventKind::PodExited { pod_id, exit_code },
        ),
        WorkerEvent::PodFailed {
            namespace_id,
            pod_id,
            error,
        } => ns_event(
            &namespace_id,
            worker_id,
            WorkerNamespaceEventKind::PodFailed { pod_id, error },
        ),
        WorkerEvent::PodSuspended {
            namespace_id,
            pod_id,
            artifact_id,
            ..
        } => ns_event(
            &namespace_id,
            worker_id,
            WorkerNamespaceEventKind::PodSuspended {
                pod_id,
                artifact_id,
            },
        ),
        WorkerEvent::PodSuspendFailed {
            namespace_id,
            pod_id,
            ..
        } => ns_event(
            &namespace_id,
            worker_id,
            WorkerNamespaceEventKind::PodSuspendFailed { pod_id },
        ),
        // Namespace lifecycle
        WorkerEvent::NamespaceCreated { namespace_id } => ns_event(
            &namespace_id,
            worker_id,
            WorkerNamespaceEventKind::NamespaceCreated,
        ),
        WorkerEvent::NamespaceFailed {
            namespace_id,
            error,
        } => ns_event(
            &namespace_id,
            worker_id,
            WorkerNamespaceEventKind::NamespaceFailed { error },
        ),
        WorkerEvent::NamespaceDestroyed { .. } => ClassifiedWorkerEvent::Ignored,

        // Endpoint events — both map to a unified EndpointDemand variant.
        WorkerEvent::EndpointDemandTraffic {
            namespace_id,
            ip,
            service_id,
        } => ns_event(
            &namespace_id,
            worker_id,
            WorkerNamespaceEventKind::EndpointDemand {
                ip,
                service_id,
                signal: EndpointDemandSignal::Traffic,
            },
        ),
        WorkerEvent::EndpointDemandActive {
            namespace_id,
            ip,
            service_id,
            active,
        } => ns_event(
            &namespace_id,
            worker_id,
            WorkerNamespaceEventKind::EndpointDemand {
                ip,
                service_id,
                signal: EndpointDemandSignal::Active { active },
            },
        ),

        // =====================================================================
        // Artifact events → scheduler
        // =====================================================================
        WorkerEvent::ArtifactWriteStarted {
            artifact_id,
            pool_id,
            ..
        } => ClassifiedWorkerEvent::Scheduler(SchedulerCoreInput::ArtifactEvent {
            worker_id,
            event: ArtifactPlacementEvent::WriteStarted {
                artifact_id,
                pool_id,
            },
        }),
        WorkerEvent::ArtifactWriteCommitted {
            artifact_id,
            pool_id,
            size_bytes,
            ..
        } => ClassifiedWorkerEvent::Scheduler(SchedulerCoreInput::ArtifactEvent {
            worker_id,
            event: ArtifactPlacementEvent::WriteCommitted {
                artifact_id,
                pool_id,
                size_bytes,
            },
        }),
        WorkerEvent::ArtifactTransferReceived {
            dest_artifact_id,
            dest_pool_id,
            size_bytes,
            ..
        } => ClassifiedWorkerEvent::Scheduler(SchedulerCoreInput::ArtifactEvent {
            worker_id,
            event: ArtifactPlacementEvent::TransferReceived {
                artifact_id: dest_artifact_id,
                pool_id: dest_pool_id,
                size_bytes,
            },
        }),
        WorkerEvent::TransferFailed {
            source_artifact_id, ..
        } => ClassifiedWorkerEvent::Scheduler(SchedulerCoreInput::ArtifactEvent {
            worker_id,
            event: ArtifactPlacementEvent::TransferFailed {
                artifact_id: source_artifact_id,
            },
        }),

        // =====================================================================
        // Global worker state events
        // =====================================================================
        WorkerEvent::PressureUpdate { cpu, memory, io } => {
            ClassifiedWorkerEvent::WorkerState(WorkerStateCoreEvent::PressureUpdate {
                worker_id,
                cpu,
                memory,
                io,
            })
        }
        WorkerEvent::PoolCapacityUpdate { pools } => {
            ClassifiedWorkerEvent::WorkerState(WorkerStateCoreEvent::PoolCapacityUpdate {
                worker_id,
                pools,
            })
        }
        WorkerEvent::WorkerCondition {
            key,
            active,
            message,
        } => ClassifiedWorkerEvent::WorkerState(WorkerStateCoreEvent::ConditionUpdate {
            worker_id,
            key,
            active,
            message,
        }),

        // =====================================================================
        // Memory observability events
        // =====================================================================
        WorkerEvent::PodMemoryConstrained {
            namespace_id,
            pod_id,
            reason,
        } => ns_event(
            &namespace_id,
            worker_id,
            WorkerNamespaceEventKind::PodMemoryConstrained { pod_id, reason },
        ),
        WorkerEvent::PodMemoryConstraintCleared {
            namespace_id,
            pod_id,
        } => ns_event(
            &namespace_id,
            worker_id,
            WorkerNamespaceEventKind::PodMemoryConstraintCleared { pod_id },
        ),
        WorkerEvent::PodOomKill {
            namespace_id,
            pod_id,
            count,
        } => ns_event(
            &namespace_id,
            worker_id,
            WorkerNamespaceEventKind::PodOomKill { pod_id, count },
        ),

        // =====================================================================
        // Ignored
        // =====================================================================
        WorkerEvent::ShuttingDown
        | WorkerEvent::TunnelStatus { .. }
        | WorkerEvent::PodLogStreamError { .. } => ClassifiedWorkerEvent::Ignored,
    }
}

/// Helper: wrap a namespace-scoped event kind into a ClassifiedWorkerEvent.
fn ns_event(
    proto_namespace_id: &distvirt_worker_protocol::NamespaceId,
    worker_id: GlobalWorkerId,
    kind: WorkerNamespaceEventKind,
) -> ClassifiedWorkerEvent {
    ClassifiedWorkerEvent::Namespace {
        namespace_id: NamespaceId::from(proto_namespace_id.as_ref()),
        event: OrchestratorToNamespace::WorkerEvent(WorkerNamespaceEvent {
            worker_id,
            event: kind,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pod_running_classifies_as_namespace() {
        let event = WorkerEvent::PodRunning {
            namespace_id: distvirt_worker_protocol::NamespaceId::from("ns1"),
            pod_id: distvirt_worker_protocol::PodId::from(1u64),
        };
        match classify(GlobalWorkerId::from(1), event) {
            ClassifiedWorkerEvent::Namespace { namespace_id, .. } => {
                assert_eq!(namespace_id, NamespaceId::from("ns1"));
            }
            _ => panic!("expected Namespace"),
        }
    }

    #[test]
    fn pressure_update_classifies_as_worker_state() {
        let event = WorkerEvent::PressureUpdate {
            cpu: Default::default(),
            memory: Default::default(),
            io: Default::default(),
        };
        assert!(matches!(
            classify(GlobalWorkerId::from(1), event),
            ClassifiedWorkerEvent::WorkerState(WorkerStateCoreEvent::PressureUpdate { .. })
        ));
    }

    #[test]
    fn artifact_write_classifies_as_scheduler() {
        let event = WorkerEvent::ArtifactWriteStarted {
            namespace_id: distvirt_worker_protocol::NamespaceId::from("ns1"),
            artifact_id: distvirt_worker_protocol::ArtifactId::from("1"),
            pool_id: distvirt_worker_protocol::PoolId::from("p1"),
        };
        assert!(matches!(
            classify(GlobalWorkerId::from(1), event),
            ClassifiedWorkerEvent::Scheduler(SchedulerCoreInput::ArtifactEvent { .. })
        ));
    }

    #[test]
    fn shutting_down_classifies_as_ignored() {
        assert!(matches!(
            classify(GlobalWorkerId::from(1), WorkerEvent::ShuttingDown),
            ClassifiedWorkerEvent::Ignored
        ));
    }
}
