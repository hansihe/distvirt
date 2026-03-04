use std::collections::HashMap;

use super::*;

#[derive(Debug, Clone, PartialEq)]
pub enum ClientCommand {
    CreateNamespace { namespace_id: NamespaceId, spec: NamespaceSpec },
    UpdateNamespace { namespace_id: NamespaceId, spec: NamespaceSpec },
    DeleteNamespace { namespace_id: NamespaceId },
    GetNamespaceStatus { namespace_id: NamespaceId },
    ListNamespaces,
    Splice { namespace_id: NamespaceId, workload_id: WorkloadId, worker_id: WorkerId },
    Unsplice { namespace_id: NamespaceId, workload_id: WorkloadId },
    CloneNamespace {
        source_namespace_id: NamespaceId,
        target_namespace_id: NamespaceId,
    },
    ListWorkers,
    GetWorker { worker_id: WorkerId },
    ListPods { namespace_id: NamespaceId },
    StreamLogs { namespace_id: NamespaceId, service_id: Option<ServiceId> },
    Connect { namespace_id: NamespaceId, client_public_key: [u8; 32] },
    Disconnect { namespace_id: NamespaceId, client_public_key: [u8; 32] },
    DeactivateWorkload { namespace_id: NamespaceId, workload_id: WorkloadId },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ClientEvent {
    NamespaceStatus { namespace_id: NamespaceId, status: NamespaceStatusReport },
    NamespaceList { namespaces: Vec<NamespaceStatusReport> },
    WorkerList { workers: Vec<WorkerStatusReport> },
    WorkerStatus { worker: WorkerStatusReport },
    PodList { pods: Vec<PodStatusReport> },
    LogChunk { namespace_id: NamespaceId, service_id: ServiceId, data: Vec<u8> },
    Error { message: String },
    Ok,
    ConnectResult {
        server_public_key: [u8; 32],
        endpoint: String,
        client_ip: String,
        subnet: String,
    },
    DeactivateWorkloadResult { deactivated: bool, reason: String },
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerStatusReport {
    pub worker_id: WorkerId,
    pub max_pods: u32,
    pub available_memory_mb: u64,
    pub active_pods: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PodStatusReport {
    pub pod_id: PodId,
    pub workload_id: WorkloadId,
    pub worker_id: WorkerId,
    pub ip: String,
    pub mac: String,
    pub state: PodStatus,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PodStatus {
    Launching,
    Running,
    Suspending,
    Suspended,
    Resuming,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NamespaceStatusReport {
    pub namespace_id: NamespaceId,
    pub status: NamespaceStatus,
    pub services: HashMap<ServiceId, ServiceStatusReport>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ServiceStatusReport {
    pub service_state: String,
    pub workload_id: WorkloadId,
    pub workload_state: String,
    pub pod_id: Option<PodId>,
    pub worker_id: Option<WorkerId>,
    pub backend_need: Option<BackendNeed>,
    pub activation_enabled: bool,
    pub ip: String,
    pub mac: String,
}
