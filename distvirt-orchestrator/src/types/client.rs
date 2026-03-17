use std::collections::BTreeMap;

use super::*;

#[derive(Debug, Clone, PartialEq)]
pub enum ClientCommand {
    CreateNamespace {
        namespace_id: NamespaceId,
        spec: NamespaceSpec,
    },
    UpdateNamespace {
        namespace_id: NamespaceId,
        spec: NamespaceSpec,
    },
    DeleteNamespace {
        namespace_id: NamespaceId,
    },
    GetNamespaceStatus {
        namespace_id: NamespaceId,
    },
    ListNamespaces,
    Splice {
        namespace_id: NamespaceId,
        workload_id: WorkloadName,
        worker_id: WorkerId,
    },
    Unsplice {
        namespace_id: NamespaceId,
        workload_id: WorkloadName,
    },
    CloneNamespace {
        source_namespace_id: NamespaceId,
        target_namespace_id: NamespaceId,
    },
    ListWorkers,
    GetWorker {
        worker_id: WorkerId,
    },
    ListPods {
        namespace_id: NamespaceId,
    },
    StreamLogs {
        namespace_id: NamespaceId,
        service_id: Option<String>,
    },
    Connect {
        namespace_id: NamespaceId,
        client_public_key: [u8; 32],
    },
    Disconnect {
        namespace_id: NamespaceId,
        client_public_key: [u8; 32],
    },
    DeactivateWorkload {
        namespace_id: NamespaceId,
        workload_id: WorkloadName,
    },
    DrainWorker {
        worker_id: WorkerId,
    },
    UndrainWorker {
        worker_id: WorkerId,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ClientEvent {
    NamespaceStatus {
        namespace_id: NamespaceId,
        status: NamespaceStatusReport,
    },
    NamespaceList {
        namespaces: Vec<NamespaceStatusReport>,
    },
    WorkerList {
        workers: Vec<WorkerStatusReport>,
    },
    WorkerStatus {
        worker: WorkerStatusReport,
    },
    PodList {
        pods: Vec<PodStatusReport>,
    },
    LogChunk {
        namespace_id: NamespaceId,
        service_id: String,
        data: Vec<u8>,
    },
    Error {
        message: String,
    },
    Ok,
    ConnectResult {
        server_public_key: [u8; 32],
        endpoint: String,
        client_ip: String,
        subnet: String,
    },
    DeactivateWorkloadResult {
        deactivated: bool,
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerStatusReport {
    pub worker_id: WorkerId,
    pub max_pods: u32,
    pub available_memory_mb: u64,
    pub active_pods: u32,
    pub conditions: BTreeMap<String, WorkerCondition>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PodStatusReport {
    pub pod_id: PodId,
    pub workload_id: WorkloadName,
    pub worker_id: WorkerId,
    pub ip: String,
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
    pub workloads: BTreeMap<WorkloadName, WorkloadStatusReport>,
    pub services: BTreeMap<String, ServiceStatusReport>,
    pub pods: BTreeMap<PodId, PodStatusReport>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkloadStatusReport {
    pub state: String,
    pub pod_id: Option<PodId>,
    pub conditions: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ServiceStatusReport {
    pub workload_id: WorkloadName,
    pub service_state: String,
    pub backend_need: Option<BackendNeed>,
    pub activation_enabled: bool,
    pub ip: String,
    pub conditions: BTreeMap<String, String>,
}
