use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::time::Duration;

use serde::{Deserialize, Serialize};

// --- Re-exports from protocol ---

pub use distvirt_worker_protocol::{
    ActivatorConfig, BackendNeed, ContainerConfig, ContainerSpec, NamespaceId, NetworkConfig,
    PodId, PodNetworkConfig, RegistryEntry, ServiceBackend, ServiceId, ServicePolicy,
    WorkerCommand, WorkerId,
};

// --- Orchestrator-only ID Newtypes ---

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct WorkloadId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ClientId(pub u64);

// --- Timer Keys ---

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TimerKey {
    IdleTimeout { service_id: ServiceId },
    LaunchTimeout { workload_id: WorkloadId, pod_id: PodId },
}

// --- Orchestrator-Level Input/Output ---

#[derive(Debug, Clone, PartialEq)]
pub enum OrchestratorInput {
    ClientConnected { client_id: ClientId },
    ClientDisconnected { client_id: ClientId },
    ClientCommand { client_id: ClientId, command: ClientCommand },
    WorkerConnected { worker_id: WorkerId, capabilities: WorkerCapabilities },
    WorkerDisconnected { worker_id: WorkerId },
    NamespaceInput { namespace_id: NamespaceId, input: NamespaceInput },
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct OrchestratorOutput {
    pub worker_commands: Vec<(WorkerId, WorkerCommand)>,
    pub client_events: Vec<(ClientId, ClientEvent)>,
    pub timers_set: Vec<(TimerKey, Duration)>,
    pub timers_cancel: Vec<TimerKey>,
    pub namespace_outputs: Vec<(NamespaceId, NamespaceOutput)>,
}

// --- Client Protocol ---

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
    StreamLogs { namespace_id: NamespaceId, service_id: Option<ServiceId> },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ClientEvent {
    NamespaceStatus { namespace_id: NamespaceId, status: NamespaceStatusReport },
    NamespaceList { namespaces: Vec<NamespaceStatusReport> },
    LogChunk { namespace_id: NamespaceId, service_id: ServiceId, data: Vec<u8> },
    Error { message: String },
    Ok,
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
    pub spliced: bool,
}

// --- Namespace-Level Input/Output ---

#[derive(Debug, Clone, PartialEq)]
pub enum NamespaceInput {
    WorkerEvent { worker_id: WorkerId, event: WorkerEvent },
    WorkerLost { worker_id: WorkerId },
    TimerFired { timer_key: TimerKey },
    UpdateSpec { client_id: ClientId, spec: NamespaceSpec },
    Delete { client_id: ClientId },
    GetStatus { client_id: ClientId },
    Splice { client_id: ClientId, workload_id: WorkloadId, worker_id: WorkerId },
    Unsplice { client_id: ClientId, workload_id: WorkloadId },
    StreamLogs { client_id: ClientId, service_id: Option<ServiceId> },
    LaunchPod {
        workload_id: WorkloadId,
        worker_id: WorkerId,
        pod_id: PodId,
    },
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct NamespaceOutput {
    pub worker_commands: Vec<(WorkerId, WorkerCommand)>,
    pub client_events: Vec<(ClientId, ClientEvent)>,
    pub timers_set: Vec<(TimerKey, Duration)>,
    pub timers_cancel: Vec<TimerKey>,
    pub pod_requests: Vec<PodRequest>,
    pub destroyed: bool,
}

// --- Orchestrator-domain WorkerEvent ---
// This is the orchestrator's view of worker events. It omits `namespace_id`
// (the shell/router strips it) and wire-only variants like `ShuttingDown`,
// `PodLogStreamError`, `FabricRouteMiss`.

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WorkerEvent {
    NamespaceCreated,
    NamespaceFailed { error: String },
    NamespaceDestroyed,
    PodRunning { pod_id: PodId },
    PodExited { pod_id: PodId, exit_code: i32 },
    PodFailed { pod_id: PodId, error: String },
    ServiceActivation { service_id: ServiceId },
    ServiceBackendNeed { service_id: ServiceId, need: BackendNeed },
}

// --- Domain Enums ---

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum NamespaceStatus {
    Creating,
    Active,
    Cloning { pending_destroy: bool },
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
        hosting: WorkloadHosting,
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
pub enum WorkloadHosting {
    Normal,
    Spliced { original_worker_id: WorkerId },
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
    pub status: WorkerStatus,
    pub namespaces: std::collections::HashSet<NamespaceId>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WorkerStatus {
    Connected,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerCapabilities {
    pub max_pods: u32,
    pub available_memory_mb: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PodRequest {
    pub workload_id: WorkloadId,
}

// --- Spec Types ---

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceSpec {
    pub network: NetworkConfig,
    pub workloads: HashMap<WorkloadId, WorkloadSpec>,
    pub services: HashMap<ServiceId, ServiceSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkloadSpec {
    pub containers: Vec<ContainerSpec>,
    pub network: PodNetworkConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ServiceSpec {
    pub workload_id: WorkloadId,
    pub ip: Ipv4Addr,
    pub mac: [u8; 6],
    pub policy: ServicePolicy,
    pub activation: Option<ActivationSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ActivationSpec {
    pub idle_timeout: Duration,
}
