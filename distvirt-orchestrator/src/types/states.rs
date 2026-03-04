use super::*;

// --- Domain Enums ---

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum NamespaceStatus {
    Creating,
    Active,
    Destroying,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WorkloadState {
    Dormant,
    WaitingForCapacity,
    Launching {
        pod_id: PodId,
        worker_id: WorkerId,
        launch_timeout: TimerKey,
    },
    Running {
        pod_id: PodId,
        worker_id: WorkerId,
    },
    /// Pod is being suspended. SuspendPod sent, waiting for PodSuspended.
    Suspending {
        pod_id: PodId,
        worker_id: WorkerId,
        snapshot_id: SnapshotId,
        suspend_timeout: TimerKey,
    },
    /// Pod is suspended. Snapshot exists on worker.
    Suspended {
        worker_id: WorkerId,
        snapshot_id: SnapshotId,
    },
    /// Pod is being resumed from snapshot. ResumePod sent, waiting for PodRunning.
    Resuming {
        pod_id: PodId,
        worker_id: WorkerId,
        snapshot_id: SnapshotId,
        resume_timeout: TimerKey,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ServiceState {
    Pending,
    Idle,
    NeedBackend,
    Active {
        pod_id: PodId,
        worker_id: WorkerId,
        backend_need: BackendNeed,
        idle_timer: Option<TimerKey>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FabricStatus {
    Creating,
    Active,
    Destroying,
}

// --- State Structs ---

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceWorkerState {
    pub fabric_status: FabricStatus,
    pub pods: std::collections::HashSet<PodId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PodInfo {
    pub workload_id: WorkloadId,
    pub worker_id: WorkerId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerState {
    pub capabilities: WorkerCapabilities,
    pub namespaces: std::collections::HashSet<NamespaceId>,
    pub wg_config: Option<WorkerWgConfig>,
}

impl WorkloadState {
    pub fn as_str(&self) -> &'static str {
        match self {
            WorkloadState::Dormant => "dormant",
            WorkloadState::WaitingForCapacity => "waiting_for_capacity",
            WorkloadState::Launching { .. } => "launching",
            WorkloadState::Running { .. } => "running",
            WorkloadState::Suspending { .. } => "suspending",
            WorkloadState::Suspended { .. } => "suspended",
            WorkloadState::Resuming { .. } => "resuming",
        }
    }

    pub fn pod_id(&self) -> Option<&PodId> {
        match self {
            WorkloadState::Launching { pod_id, .. }
            | WorkloadState::Running { pod_id, .. }
            | WorkloadState::Suspending { pod_id, .. }
            | WorkloadState::Resuming { pod_id, .. } => Some(pod_id),
            _ => None,
        }
    }

    pub fn worker_id(&self) -> Option<&WorkerId> {
        match self {
            WorkloadState::Launching { worker_id, .. }
            | WorkloadState::Running { worker_id, .. }
            | WorkloadState::Suspending { worker_id, .. }
            | WorkloadState::Suspended { worker_id, .. }
            | WorkloadState::Resuming { worker_id, .. } => Some(worker_id),
            _ => None,
        }
    }
}

impl ServiceState {
    pub fn as_str(&self) -> &'static str {
        match self {
            ServiceState::Pending => "pending",
            ServiceState::Idle => "idle",
            ServiceState::NeedBackend => "need_backend",
            ServiceState::Active { .. } => "active",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerCapabilities {
    pub max_pods: u32,
    pub available_memory_mb: u64,
    pub public_endpoint: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerWgConfig {
    pub listen_port: u16,
    pub public_key: [u8; 32],
}
