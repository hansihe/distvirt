use std::collections::HashMap;

use super::*;

// --- Artifact Placement ---

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArtifactStatus {
    Writing,
    Ready,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactPlacement {
    pub pool_id: PoolId,
    pub worker_id: WorkerId,
    pub locked_by: Option<PodId>,
    pub status: ArtifactStatus,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlacementTable {
    placements: HashMap<ArtifactId, ArtifactPlacement>,
}

impl PlacementTable {
    pub fn insert(&mut self, artifact_id: ArtifactId, placement: ArtifactPlacement) {
        self.placements.insert(artifact_id, placement);
    }

    pub fn get(&self, artifact_id: &ArtifactId) -> Option<&ArtifactPlacement> {
        self.placements.get(artifact_id)
    }

    pub fn get_mut(&mut self, artifact_id: &ArtifactId) -> Option<&mut ArtifactPlacement> {
        self.placements.get_mut(artifact_id)
    }

    pub fn remove(&mut self, artifact_id: &ArtifactId) -> Option<ArtifactPlacement> {
        self.placements.remove(artifact_id)
    }

    pub fn lock(&mut self, artifact_id: &ArtifactId, pod_id: &PodId) {
        if let Some(p) = self.placements.get_mut(artifact_id) {
            p.locked_by = Some(pod_id.clone());
        }
    }

    pub fn unlock(&mut self, artifact_id: &ArtifactId) {
        if let Some(p) = self.placements.get_mut(artifact_id) {
            p.locked_by = None;
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&ArtifactId, &ArtifactPlacement)> {
        self.placements.iter()
    }

    pub fn remove_by_worker(&mut self, worker_id: &WorkerId) -> Vec<(ArtifactId, ArtifactPlacement)> {
        let to_remove: Vec<ArtifactId> = self
            .placements
            .iter()
            .filter(|(_, p)| p.worker_id == *worker_id)
            .map(|(id, _)| id.clone())
            .collect();
        to_remove
            .into_iter()
            .filter_map(|id| self.placements.remove(&id).map(|p| (id, p)))
            .collect()
    }
}

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
        artifact_id: ArtifactId,
        suspend_timeout: TimerKey,
    },
    /// Pod is suspended. Artifact tracked in placement table.
    Suspended {
        artifact_id: ArtifactId,
    },
    /// Pod is being resumed from snapshot. ResumePod sent, waiting for PodRunning.
    Resuming {
        pod_id: PodId,
        worker_id: WorkerId,
        artifact_id: ArtifactId,
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
    pub primary_pool_id: Option<PoolId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PodInfo {
    pub workload_id: WorkloadId,
    pub worker_id: WorkerId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerTunnelConfig {
    pub listen_port: u16,
    pub public_key: [u8; 32],
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerCondition {
    pub active: bool,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerState {
    pub capabilities: WorkerCapabilities,
    pub namespaces: std::collections::HashSet<NamespaceId>,
    pub wg_config: Option<WorkerWgConfig>,
    pub tunnel_config: Option<WorkerTunnelConfig>,
    pub conditions: std::collections::HashMap<String, WorkerCondition>,
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
            | WorkloadState::Resuming { worker_id, .. } => Some(worker_id),
            _ => None,
        }
    }

    pub fn artifact_id(&self) -> Option<&ArtifactId> {
        match self {
            WorkloadState::Suspending { artifact_id, .. }
            | WorkloadState::Suspended { artifact_id, .. }
            | WorkloadState::Resuming { artifact_id, .. } => Some(artifact_id),
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
    pub pools: Vec<PoolInfo>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerWgConfig {
    pub listen_port: u16,
    pub public_key: [u8; 32],
}
