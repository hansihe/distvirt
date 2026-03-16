//! Shared types for the pure orchestrator core.
//!
//! These types define the inputs and outputs of the core state machines,
//! free of any async or channel dependencies.

use crate::adapter::timer::{TimerAction, TimerIdentity};
use crate::sm_new::PodId;
use crate::task::GlobalWorkerId;
use crate::types::NamespaceId;

// Re-export types that are shared between task/ and core/.
pub(crate) use crate::task::{
    ArtifactPlacementEvent, ClientCommand, SchedulerDecision, WorkerNamespaceEvent,
    WorkerNamespaceEventKind,
};

// =============================================================================
// Namespace core input/output
// =============================================================================

/// Input events for NamespaceCore (no channel handles).
pub(crate) enum NamespaceCoreEvent {
    WorkerEvent(WorkerNamespaceEvent),
    SchedulerDecision(SchedulerDecision),
    TimerFired {
        identity: TimerIdentity,
        generation: u64,
    },
    WorkerConnected {
        worker_id: GlobalWorkerId,
        proto_worker_id: distvirt_worker_protocol::WorkerId,
        info: crate::sm_new::WorkerInfo,
    },
    WorkerDisconnected {
        worker_id: GlobalWorkerId,
    },
    ClientCommand(ClientCommand),
}

/// Output effects from NamespaceCore after processing an event.
#[derive(Default)]
pub(crate) struct NamespaceEffects {
    pub timer_actions: Vec<TimerAction>,
    pub scheduler_messages: Vec<SchedulerMessage>,
    pub worker_commands: Vec<(GlobalWorkerId, distvirt_worker_protocol::WorkerCommand)>,
    /// Commands to broadcast to all active workers in this namespace.
    pub broadcast_commands: Vec<distvirt_worker_protocol::WorkerCommand>,
}

impl NamespaceEffects {
    pub fn is_empty(&self) -> bool {
        self.timer_actions.is_empty()
            && self.scheduler_messages.is_empty()
            && self.worker_commands.is_empty()
            && self.broadcast_commands.is_empty()
    }
}

/// Messages from namespace core to the scheduler.
pub(crate) enum SchedulerMessage {
    RequestLease {
        namespace_id: NamespaceId,
        pod_id: PodId,
        proto_resume_artifact: Option<distvirt_worker_protocol::ArtifactId>,
    },
    DropRequest {
        namespace_id: NamespaceId,
        pod_id: PodId,
    },
}

// =============================================================================
// Scheduler core input/output
// =============================================================================

/// Input events for SchedulerCore (no channel handles).
pub(crate) enum SchedulerCoreInput {
    RequestLease {
        namespace_id: NamespaceId,
        pod_id: PodId,
        proto_resume_artifact: Option<distvirt_worker_protocol::ArtifactId>,
    },
    DropRequest {
        namespace_id: NamespaceId,
        pod_id: PodId,
    },
    WorkerUpdate(GlobalWorkerId, crate::task::scheduler::WorkerCandidate),
    WorkerRemoved(GlobalWorkerId),
    ArtifactEvent {
        worker_id: GlobalWorkerId,
        event: ArtifactPlacementEvent,
    },
}

// =============================================================================
// Worker state core input/output
// =============================================================================

/// Input events for WorkerStateCore (no channel handles, no writer).
pub(crate) enum WorkerStateCoreEvent {
    PressureUpdate {
        worker_id: GlobalWorkerId,
        cpu: distvirt_worker_protocol::PsiMetrics,
        memory: distvirt_worker_protocol::PsiMetrics,
        io: distvirt_worker_protocol::PsiMetrics,
    },
    PoolCapacityUpdate {
        worker_id: GlobalWorkerId,
        pools: Vec<distvirt_worker_protocol::PoolInfo>,
    },
    ConditionUpdate {
        worker_id: GlobalWorkerId,
        key: String,
        active: bool,
        message: String,
    },
    Connected {
        worker_id: GlobalWorkerId,
        capabilities: distvirt_worker_protocol::WorkerCapabilities,
        tunnel_info: Option<crate::task::worker_state::WorkerTunnelInfo>,
        proto_worker_id: distvirt_worker_protocol::WorkerId,
    },
    Disconnected {
        worker_id: GlobalWorkerId,
    },
    NamespaceAssigned {
        worker_id: GlobalWorkerId,
        namespace_id: NamespaceId,
    },
    NamespaceUnassigned {
        worker_id: GlobalWorkerId,
        namespace_id: NamespaceId,
    },
    RegisterNamespaceSegment {
        namespace_id: NamespaceId,
        segment_id: u16,
    },
    UnregisterNamespaceSegment {
        namespace_id: NamespaceId,
    },
}

/// Output effects from WorkerStateCore.
#[derive(Default)]
pub(crate) struct WorkerStateEffects {
    /// Updates to forward to the scheduler.
    pub scheduler_updates: Vec<SchedulerCoreInput>,
    /// If the worker registry changed, broadcast this command to all workers.
    pub worker_registry_broadcast: Option<distvirt_worker_protocol::WorkerCommand>,
}

// =============================================================================
// Orchestrator-level input/output
// =============================================================================

/// Top-level input to SyncOrchestrator.
pub(crate) enum OrchestratorInput {
    NamespaceEvent {
        namespace_id: NamespaceId,
        event: NamespaceCoreEvent,
    },
    WorkerStateEvent(WorkerStateCoreEvent),
    CreateNamespace {
        namespace_id: NamespaceId,
    },
    DestroyNamespace {
        namespace_id: NamespaceId,
    },
}

/// Top-level output from SyncOrchestrator.
#[derive(Default)]
pub(crate) struct OrchestratorEffects {
    /// Timer actions scoped to a namespace.
    pub timer_actions: Vec<(NamespaceId, Vec<TimerAction>)>,
    /// Commands targeted at specific workers.
    pub worker_commands: Vec<(GlobalWorkerId, distvirt_worker_protocol::WorkerCommand)>,
    /// Commands to broadcast to all workers in a specific namespace.
    pub broadcast_commands: Vec<(NamespaceId, distvirt_worker_protocol::WorkerCommand)>,
    /// Commands to broadcast to all connected workers globally (e.g. worker registry sync).
    pub global_broadcasts: Vec<distvirt_worker_protocol::WorkerCommand>,
}
