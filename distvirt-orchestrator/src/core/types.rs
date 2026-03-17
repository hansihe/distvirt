//! Shared types for the pure orchestrator core.
//!
//! These types define the inputs and outputs of the core state machines,
//! free of any async or channel dependencies.

use crate::adapter::timer::{TimerAction, TimerIdentity};
use crate::core::scheduler::WorkerCandidate;
use crate::core::worker_state::WorkerTunnelInfo;
use crate::core::{
    ArtifactPlacementEvent, ClientCommand, GlobalWorkerId, SchedulerDecision, WorkerNamespaceEvent,
};
use crate::sm::PodId;
use crate::types::NamespaceId;

// =============================================================================
// Namespace core input/output
// =============================================================================

/// Input events for NamespaceCore (no channel handles).
pub enum NamespaceCoreEvent {
    WorkerEvent(WorkerNamespaceEvent),
    SchedulerDecision(SchedulerDecision),
    TimerFired {
        identity: TimerIdentity,
        generation: u64,
    },
    WorkerConnected {
        worker_id: GlobalWorkerId,
        proto_worker_id: distvirt_worker_protocol::WorkerId,
        info: crate::sm::WorkerInfo,
    },
    WorkerDisconnected {
        worker_id: GlobalWorkerId,
    },
    ClientCommand(ClientCommand),
}

/// Output effects from NamespaceCore after processing an event.
#[derive(Default)]
pub struct NamespaceEffects {
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
pub enum SchedulerMessage {
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
pub enum SchedulerCoreInput {
    RequestLease {
        namespace_id: NamespaceId,
        pod_id: PodId,
        proto_resume_artifact: Option<distvirt_worker_protocol::ArtifactId>,
    },
    DropRequest {
        namespace_id: NamespaceId,
        pod_id: PodId,
    },
    WorkerUpdate(GlobalWorkerId, WorkerCandidate),
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
pub enum WorkerStateCoreEvent {
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
        tunnel_info: Option<WorkerTunnelInfo>,
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
    PodCountChange {
        worker_id: GlobalWorkerId,
        delta: i32,
    },
}

/// Output effects from WorkerStateCore.
#[derive(Default)]
pub struct WorkerStateEffects {
    /// Updates to forward to the scheduler.
    pub scheduler_updates: Vec<SchedulerCoreInput>,
    /// If the worker registry changed, broadcast this command to all workers.
    pub worker_registry_broadcast: Option<distvirt_worker_protocol::WorkerCommand>,
}

// =============================================================================
// Orchestrator-level input/output
// =============================================================================

/// Top-level input to OrchestratorCore.
pub enum OrchestratorInput {
    NamespaceEvent {
        namespace_id: NamespaceId,
        event: NamespaceCoreEvent,
    },
    WorkerStateEvent(WorkerStateCoreEvent),
    /// Direct scheduler input (e.g. artifact placement events from workers).
    SchedulerEvent(SchedulerCoreInput),
    CreateNamespace {
        namespace_id: NamespaceId,
    },
    DestroyNamespace {
        namespace_id: NamespaceId,
    },
}

/// Information needed to register a new worker with the orchestrator.
/// Produced by the async shell after handshake, consumed by OrchestratorCore.
pub struct WorkerConnectedInfo {
    pub worker_id: GlobalWorkerId,
    pub capabilities: distvirt_worker_protocol::WorkerCapabilities,
    pub tunnel_info: Option<WorkerTunnelInfo>,
    pub proto_worker_id: distvirt_worker_protocol::WorkerId,
}

/// Information needed to create a namespace in the orchestrator.
pub struct CreateNamespaceInfo {
    pub namespace_id: NamespaceId,
    pub network: distvirt_worker_protocol::NetworkConfig,
}

/// Worker command that the shell must send directly on the wire
/// (not routed through a namespace, e.g. CreateNamespace wire command).
pub struct DirectWorkerCommand {
    pub worker_id: GlobalWorkerId,
    pub command: distvirt_worker_protocol::WorkerCommand,
}

/// Top-level output from OrchestratorCore.
///
/// Timer actions are **not** included here — they are absorbed internally
/// by the core's `TimerWheel`. Shells drive time via `advance_to()` /
/// `next_deadline()` instead.
#[derive(Default)]
pub struct OrchestratorEffects {
    /// Commands targeted at specific workers (routed through namespace logic).
    pub worker_commands: Vec<(GlobalWorkerId, distvirt_worker_protocol::WorkerCommand)>,
    /// Commands to broadcast to all workers in a specific namespace.
    pub broadcast_commands: Vec<(NamespaceId, distvirt_worker_protocol::WorkerCommand)>,
    /// Commands to broadcast to all connected workers globally (e.g. worker registry sync).
    pub global_broadcasts: Vec<distvirt_worker_protocol::WorkerCommand>,
    /// Direct wire commands the shell must send (e.g. CreateNamespace).
    /// These bypass namespace logic — the shell just sends them on the writer.
    pub direct_worker_commands: Vec<DirectWorkerCommand>,
}

impl OrchestratorEffects {
    /// Merge another set of effects into this one.
    pub fn merge(&mut self, other: OrchestratorEffects) {
        self.worker_commands.extend(other.worker_commands);
        self.broadcast_commands.extend(other.broadcast_commands);
        self.global_broadcasts.extend(other.global_broadcasts);
        self.direct_worker_commands
            .extend(other.direct_worker_commands);
    }
}
