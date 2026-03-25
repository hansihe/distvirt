//! Pure orchestration logic — no async, no channels, no I/O.
//!
//! Individual state machine cores, shared types, and the top-level
//! `OrchestratorCore` that composes them.

use distvirt_worker_protocol::NamespaceId;
use tokio::sync::mpsc;

use crate::sm::PodId;

pub mod namespace;
pub mod orchestrator;
pub(crate) mod pressure;
pub mod types;
pub mod worker_event;

// =============================================================================
// Global worker identity
// =============================================================================

/// Numeric worker ID assigned by the shell on connect.
/// Now unified with the router's `sm::WorkerId` — the same value flows
/// through the scheduler, the router, and the wire protocol.
pub type GlobalWorkerId = crate::sm::WorkerId;

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

/// Endpoint demand signal from a worker.
///
/// Both wire event types (EndpointActivation and EndpointDemand) are unified
/// into this enum and routed through `EndpointDemandAdapter`.
pub enum EndpointDemandSignal {
    /// Instantaneous traffic event — something meaningful hit this endpoint.
    /// No persistent state mutation; the adapter/SM decides how to react.
    Traffic,
    /// Level signal from a protocol activator — "activation is now true/false
    /// until I say otherwise."
    Active { active: bool },
}

/// Worker-reported namespace-scoped events. Uses protocol u64 IDs so the
/// reader can fill these directly from wire data.
/// The namespace boundary translates protocol IDs → router IDs (trivial u64 copy).
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
        error: String,
    },
    PodSuspended {
        pod_id: distvirt_worker_protocol::PodId,
        artifact_id: distvirt_worker_protocol::ArtifactId,
    },
    PodSuspendFailed {
        pod_id: distvirt_worker_protocol::PodId,
    },
    EndpointDemand {
        ip: std::net::Ipv4Addr,
        /// Carried for debug assertions only — routing uses IP.
        service_id: Option<distvirt_worker_protocol::ServiceId>,
        signal: EndpointDemandSignal,
    },
    PodMemoryConstrained {
        pod_id: distvirt_worker_protocol::PodId,
        reason: distvirt_worker_protocol::MemoryConstraintReason,
    },
    PodMemoryConstraintCleared {
        pod_id: distvirt_worker_protocol::PodId,
    },
    PodOomKill {
        pod_id: distvirt_worker_protocol::PodId,
        count: u64,
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
///
/// Note: spec operations (update/patch) are NOT routed through this enum.
/// They use dedicated methods on `Namespace`/`NamespaceUnit` that return
/// `Result<(NamespaceEffects, IpAllocResult), ClientError>` for explicit
/// error threading and IP allocation result propagation.
pub enum ClientCommand {
    /// Restart a workload by protocol name.
    AdminRestart { workload_name: String },
    /// Scavenge a workload by protocol name.
    Scavenge { workload_name: String },
    /// Activate or deactivate a service by protocol name.
    ActivateService { service_name: String, active: bool },
    /// Connect a WireGuard peer to the namespace network.
    Connect {
        client_public_key: [u8; 32],
        worker_id: crate::sm::WorkerId,
    },
    /// Disconnect a WireGuard peer from the namespace network.
    Disconnect { client_public_key: [u8; 32] },
}

// =============================================================================
// Client errors
// =============================================================================

/// Result of a successful WireGuard network connect.
#[derive(Debug, Clone)]
pub struct ConnectResult {
    pub server_public_key: [u8; 32],
    pub endpoint: String,
    pub client_ip: std::net::Ipv4Addr,
    pub subnet: String,
}

/// Errors returned by core client-facing operations.
#[derive(Debug, Clone)]
pub enum ClientError {
    NamespaceNotFound,
    NamespaceAlreadyExists,
    /// No worker with tunnel capabilities is available.
    NoTunnelWorker,
    /// WireGuard peer IP pool exhausted.
    IpExhausted,
    /// No worker with the given ID exists.
    WorkerNotFound,
    /// IP allocation failed (zone exhaustion, collision, migration, etc.).
    IpAllocation(crate::core::namespace::ip_alloc::IpAllocError),
    /// The shell event loop has stopped.
    ShellGone,
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::NamespaceNotFound => write!(f, "namespace not found"),
            ClientError::NamespaceAlreadyExists => write!(f, "namespace already exists"),
            ClientError::WorkerNotFound => write!(f, "worker not found"),
            ClientError::NoTunnelWorker => write!(f, "no worker with tunnel capabilities"),
            ClientError::IpExhausted => write!(f, "WireGuard peer IP pool exhausted"),
            ClientError::IpAllocation(e) => write!(f, "IP allocation: {e}"),
            ClientError::ShellGone => write!(f, "orchestrator shell has stopped"),
        }
    }
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
