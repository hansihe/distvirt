//! Shared types for the pure orchestrator core.
//!
//! These types define the inputs and outputs of the core state machines,
//! free of any async or channel dependencies.

use crate::adapter::observability::ObservabilityEvent;
use crate::adapter::timer::TimerAction;
use crate::core::orchestrator::scheduler::WorkerCandidate;
use crate::core::orchestrator::worker_state::{WorkerTunnelInfo, WireguardAdapterInfo};
use crate::core::{
    ArtifactPlacementEvent, ClientCommand, GlobalWorkerId,
    SchedulerDecision, WorkerNamespaceEvent,
};
use crate::sm::{ArtifactPortId, PodId};
use crate::types::NamespaceId;

// =============================================================================
// Namespace core input/output
// =============================================================================

/// Output effects from Namespace after processing an event.
#[derive(Default)]
pub struct NamespaceEffects {
    pub timer_actions: Vec<TimerAction>,
    pub scheduler_messages: Vec<SchedulerMessage>,
    pub worker_commands: Vec<(GlobalWorkerId, distvirt_worker_protocol::WorkerCommand)>,
    /// Commands to broadcast to all active workers in this namespace.
    pub broadcast_commands: Vec<distvirt_worker_protocol::WorkerCommand>,
    pub observability_events: Vec<ObservabilityEvent>,
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
    ArtifactReferenced {
        namespace_id: NamespaceId,
        proto_artifact_id: distvirt_worker_protocol::ArtifactId,
    },
    ArtifactReleased {
        namespace_id: NamespaceId,
        proto_artifact_id: distvirt_worker_protocol::ArtifactId,
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
    /// A workload has set an edge to an artifact port (wants to keep it alive).
    ArtifactReferenced {
        proto_artifact_id: distvirt_worker_protocol::ArtifactId,
        namespace_id: NamespaceId,
    },
    /// A workload has removed its edge to an artifact port (no longer needs it).
    ArtifactReleased {
        proto_artifact_id: distvirt_worker_protocol::ArtifactId,
        namespace_id: NamespaceId,
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
        wireguard_info: Option<WireguardAdapterInfo>,
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

/// Information needed to register a new worker with the orchestrator.
/// Produced by the async shell after handshake, consumed by OrchestratorCore.
pub struct WorkerConnectedInfo {
    pub worker_id: GlobalWorkerId,
    pub capabilities: distvirt_worker_protocol::WorkerCapabilities,
    pub tunnel_info: Option<WorkerTunnelInfo>,
    pub wireguard_info: Option<WireguardAdapterInfo>,
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

// =============================================================================
// Split-core types (orchestrator ↔ namespace communication)
// =============================================================================

/// Orchestrator → Namespace message.
pub enum OrchestratorToNamespace {
    WorkerConnected {
        worker_id: GlobalWorkerId,
        proto_worker_id: distvirt_worker_protocol::WorkerId,
        info: crate::sm::WorkerInfo,
    },
    WorkerDisconnected {
        worker_id: GlobalWorkerId,
    },
    SchedulerDecision(SchedulerDecision),
    ArtifactInvalidated {
        artifact_port_id: ArtifactPortId,
    },
    WorkerEvent(WorkerNamespaceEvent),
    ClientCommand(ClientCommand),
}

/// Namespace → Orchestrator message.
pub enum NamespaceToOrchestrator {
    SchedulerMessage(SchedulerMessage),
}

/// Output from `NamespaceUnit::process()` and `advance_to()`.
#[derive(Default)]
pub struct NamespaceOutput {
    pub to_orchestrator: Vec<NamespaceToOrchestrator>,
    pub worker_commands: Vec<(GlobalWorkerId, distvirt_worker_protocol::WorkerCommand)>,
    /// Commands to broadcast to all active workers in this namespace.
    /// The shell resolves which workers are active via `active_worker_ids()`.
    pub broadcast_commands: Vec<distvirt_worker_protocol::WorkerCommand>,
    pub observability_events: Vec<ObservabilityEvent>,
}

impl NamespaceOutput {
    /// Merge another output into this one.
    pub fn merge(&mut self, other: NamespaceOutput) {
        self.to_orchestrator.extend(other.to_orchestrator);
        self.worker_commands.extend(other.worker_commands);
        self.broadcast_commands.extend(other.broadcast_commands);
        self.observability_events.extend(other.observability_events);
    }
}

/// Output from `OrchestratorCore::process()`.
#[derive(Default)]
pub struct OrchestratorOutput {
    pub to_namespaces: Vec<(NamespaceId, OrchestratorToNamespace)>,
    pub worker_commands: Vec<(GlobalWorkerId, distvirt_worker_protocol::WorkerCommand)>,
    pub direct_worker_commands: Vec<DirectWorkerCommand>,
    pub global_broadcasts: Vec<distvirt_worker_protocol::WorkerCommand>,
}

impl OrchestratorOutput {
    pub fn merge(&mut self, other: OrchestratorOutput) {
        self.to_namespaces.extend(other.to_namespaces);
        self.worker_commands.extend(other.worker_commands);
        self.direct_worker_commands.extend(other.direct_worker_commands);
        self.global_broadcasts.extend(other.global_broadcasts);
    }
}

/// New top-level input to OrchestratorCore (split version).
pub enum OrchestratorInputNew {
    WorkerStateEvent(WorkerStateCoreEvent),
    SchedulerEvent(SchedulerCoreInput),
    FromNamespace {
        namespace_id: NamespaceId,
        message: NamespaceToOrchestrator,
    },
}

/// Information returned by `create_namespace` for the shell to construct a `NamespaceUnit`.
pub struct NamespaceCreationInfo {
    pub network: distvirt_worker_protocol::NetworkConfig,
    pub id_registry: crate::id_registry::IdRegistry,
    pub timer_config: crate::adapter::timer::TimerConfig,
    pub connected_workers: Vec<ConnectedWorkerSummary>,
}

/// Summary of a connected worker, for namespace creation fan-out.
pub struct ConnectedWorkerSummary {
    pub worker_id: GlobalWorkerId,
    pub proto_worker_id: distvirt_worker_protocol::WorkerId,
    pub max_pods: u32,
    pub default_pool: Option<distvirt_worker_protocol::PoolId>,
}
