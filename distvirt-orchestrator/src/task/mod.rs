use tokio::sync::mpsc;

use crate::adapter::timer::TimerIdentity;
use crate::sm_new::{PodId, WorkerInfo};
use crate::types::{NamespaceId, NamespaceSpec};

pub(crate) mod namespace;
pub(crate) mod scheduler;
pub(crate) mod shell;
pub(crate) mod worker_reader;
pub(crate) mod worker_state;
pub(crate) mod worker_writer;

// =============================================================================
// Global worker identity
// =============================================================================

/// Numeric worker ID assigned by the shell on connect.
/// Used by scheduler, state tracker, and inter-task messages.
/// Distinct from protocol `WorkerId(String)` and router `sm_new::WorkerId(u64)`.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct GlobalWorkerId(pub u64);

#[cfg(test)]
impl GlobalWorkerId {
    pub(crate) fn test(id: u64) -> Self {
        GlobalWorkerId(id)
    }
}

// =============================================================================
// Scheduler interface
// =============================================================================

/// Sent by namespace tasks to the global scheduler.
pub(crate) enum SchedulerInput {
    /// Register a namespace's reply channel with the scheduler.
    RegisterNamespace {
        namespace_id: NamespaceId,
        reply_tx: mpsc::Sender<SchedulerDecision>,
    },
    /// Unregister a namespace from the scheduler.
    UnregisterNamespace {
        namespace_id: NamespaceId,
    },
    RequestLease {
        namespace_id: NamespaceId,
        pod_id: PodId,
        /// Protocol artifact ID for resume affinity (converted at namespace boundary).
        /// None for cold-boot pods.
        proto_resume_artifact: Option<distvirt_worker_protocol::ArtifactId>,
    },
    DropRequest {
        namespace_id: NamespaceId,
        pod_id: PodId,
    },
    /// Worker state changed (from WorkerStateTracker).
    WorkerUpdate(GlobalWorkerId, scheduler::WorkerCandidate),
    /// Worker disconnected.
    WorkerRemoved(GlobalWorkerId),
    /// Artifact placement event from a worker.
    ArtifactEvent {
        worker_id: GlobalWorkerId,
        event: ArtifactPlacementEvent,
    },
}

/// Artifact placement events reported by workers.
pub enum ArtifactPlacementEvent {
    WriteStarted {
        artifact_id: distvirt_worker_protocol::ArtifactId,
        pool_id: distvirt_worker_protocol::PoolId,
    },
    WriteCommitted {
        artifact_id: distvirt_worker_protocol::ArtifactId,
        pool_id: distvirt_worker_protocol::PoolId,
        size_bytes: u64,
    },
    TransferReceived {
        artifact_id: distvirt_worker_protocol::ArtifactId,
        pool_id: distvirt_worker_protocol::PoolId,
        size_bytes: u64,
    },
    TransferFailed {
        artifact_id: distvirt_worker_protocol::ArtifactId,
    },
}

/// Sent by the global scheduler back to a namespace task.
#[derive(Clone, Debug)]
pub enum SchedulerDecision {
    Grant { namespace_id: NamespaceId, pod_id: PodId, worker_id: GlobalWorkerId },
    Revoke { namespace_id: NamespaceId, pod_id: PodId },
}

// =============================================================================
// Client commands
// =============================================================================

/// Commands from external clients (management API) to a namespace task.
pub enum ClientCommand {
    /// Apply a new namespace spec (creates/updates/removes workloads and services).
    UpdateSpec(NamespaceSpec),
    /// Restart a workload by protocol name.
    AdminRestart { workload_name: String },
    /// Scavenge a workload by protocol name.
    Scavenge { workload_name: String },
    /// Activate or deactivate a service by protocol name.
    ActivateService { service_name: String, active: bool },
}

// =============================================================================
// Namespace task interface
// =============================================================================

/// Everything a namespace task can receive.
pub(crate) enum NamespaceEvent {
    /// Worker protocol event routed by a worker reader task.
    WorkerEvent(WorkerNamespaceEvent),
    /// Scheduler decided on a lease.
    SchedulerDecision(SchedulerDecision),
    /// A tokio timer fired.
    TimerFired {
        identity: TimerIdentity,
        generation: u64,
    },
    /// A worker was added to this namespace.
    WorkerConnected {
        worker_id: GlobalWorkerId,
        proto_worker_id: distvirt_worker_protocol::WorkerId,
        info: WorkerInfo,
        writer: WorkerWriterHandle,
    },
    /// A worker was removed from this namespace.
    WorkerDisconnected { worker_id: GlobalWorkerId },
    /// Client management command.
    ClientCommand(ClientCommand),
}

pub struct WorkerNamespaceEvent {
    pub worker_id: GlobalWorkerId,
    pub event: WorkerNamespaceEventKind,
}

/// Worker-reported namespace-scoped events. Uses **protocol string IDs** so the
/// reader can fill these directly from wire data without ID translation.
/// The namespace task translates protocol IDs → router IDs.
pub enum WorkerNamespaceEventKind {
    PodRunning { pod_id: distvirt_worker_protocol::PodId },
    PodExited { pod_id: distvirt_worker_protocol::PodId, exit_code: i32 },
    PodFailed { pod_id: distvirt_worker_protocol::PodId },
    PodSuspended { pod_id: distvirt_worker_protocol::PodId, artifact_id: distvirt_worker_protocol::ArtifactId },
    PodSuspendFailed { pod_id: distvirt_worker_protocol::PodId },
    ServiceBackendNeed { service_id: distvirt_worker_protocol::ServiceId, need: distvirt_worker_protocol::BackendNeed },
    EndpointActivation { ip: std::net::Ipv4Addr, service_id: Option<distvirt_worker_protocol::ServiceId> },
    EndpointFlowStatus { ip: std::net::Ipv4Addr, service_id: Option<distvirt_worker_protocol::ServiceId>, has_active_flows: bool },
    NamespaceCreated,
    NamespaceFailed { error: String },
}

// =============================================================================
// Worker writer
// =============================================================================

/// Handle for sending commands to a specific worker.
/// Sends fully-formed protocol commands (built by the namespace task).
#[derive(Clone)]
pub(crate) struct WorkerWriterHandle {
    tx: mpsc::Sender<distvirt_worker_protocol::WorkerCommand>,
}

impl WorkerWriterHandle {
    pub fn new(tx: mpsc::Sender<distvirt_worker_protocol::WorkerCommand>) -> Self {
        Self { tx }
    }

    pub async fn send(&self, cmd: distvirt_worker_protocol::WorkerCommand) {
        let _ = self.tx.send(cmd).await;
    }
}

// =============================================================================
// Worker state tracker interface
// =============================================================================

/// Reader → state tracker communication.
pub(crate) enum WorkerStateEvent {
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
        tunnel_info: Option<worker_state::WorkerTunnelInfo>,
        proto_worker_id: distvirt_worker_protocol::WorkerId,
        writer: WorkerWriterHandle,
    },
    Disconnected {
        worker_id: GlobalWorkerId,
    },
    /// Worker was assigned to a namespace (shell notifies tracker for segment tracking).
    NamespaceAssigned {
        worker_id: GlobalWorkerId,
        namespace_id: crate::types::NamespaceId,
    },
    /// Worker was unassigned from a namespace.
    NamespaceUnassigned {
        worker_id: GlobalWorkerId,
        namespace_id: crate::types::NamespaceId,
    },
    /// Register a namespace's segment ID (shell sends on namespace creation).
    RegisterNamespaceSegment {
        namespace_id: crate::types::NamespaceId,
        segment_id: u16,
    },
    /// Unregister a namespace segment (shell sends on namespace destruction).
    UnregisterNamespaceSegment {
        namespace_id: crate::types::NamespaceId,
    },
}

// =============================================================================
// Reader control
// =============================================================================

/// Shell → reader control channel.
pub(crate) enum ReaderControl {
    AddNamespaceRoute {
        namespace_id: NamespaceId,
        tx: mpsc::Sender<NamespaceEvent>,
    },
    RemoveNamespaceRoute {
        namespace_id: NamespaceId,
    },
}
