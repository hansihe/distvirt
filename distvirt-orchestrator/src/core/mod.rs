//! Pure orchestration logic — no async, no channels, no I/O.
//!
//! Individual state machine cores, shared types, and the top-level
//! `OrchestratorCore` that composes them.

use distvirt_worker_protocol::NamespaceId;
use tokio::sync::mpsc;

use crate::{sm::PodId, types::NamespaceSpec};

pub mod namespace;
pub mod namespace_boundary;
pub mod orchestrator;
pub(crate) mod scheduler;
pub mod timer_wheel;
pub mod types;
pub mod worker_event;
pub mod worker_state;

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
    Grant {
        namespace_id: NamespaceId,
        pod_id: PodId,
        worker_id: GlobalWorkerId,
    },
    Revoke {
        namespace_id: NamespaceId,
        pod_id: PodId,
        worker_id: GlobalWorkerId,
    },
}

// =============================================================================
// Namespace task interface
// =============================================================================

pub struct WorkerNamespaceEvent {
    pub worker_id: GlobalWorkerId,
    pub event: WorkerNamespaceEventKind,
}

/// Worker-reported namespace-scoped events. Uses **protocol string IDs** so the
/// reader can fill these directly from wire data without ID translation.
/// The namespace task translates protocol IDs → router IDs.
pub enum WorkerNamespaceEventKind {
    PodRunning {
        pod_id: distvirt_worker_protocol::PodId,
    },
    PodExited {
        pod_id: distvirt_worker_protocol::PodId,
        exit_code: i32,
    },
    PodFailed {
        pod_id: distvirt_worker_protocol::PodId,
    },
    PodSuspended {
        pod_id: distvirt_worker_protocol::PodId,
        artifact_id: distvirt_worker_protocol::ArtifactId,
    },
    PodSuspendFailed {
        pod_id: distvirt_worker_protocol::PodId,
    },
    ServiceBackendNeed {
        service_id: distvirt_worker_protocol::ServiceId,
        need: distvirt_worker_protocol::BackendNeed,
    },
    EndpointActivation {
        ip: std::net::Ipv4Addr,
        service_id: Option<distvirt_worker_protocol::ServiceId>,
    },
    EndpointFlowStatus {
        ip: std::net::Ipv4Addr,
        service_id: Option<distvirt_worker_protocol::ServiceId>,
        has_active_flows: bool,
    },
    NamespaceCreated,
    NamespaceFailed {
        error: String,
    },
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
