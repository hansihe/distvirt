use std::collections::{HashMap, HashSet};
use std::time::Duration;

use serde::{Deserialize, Serialize};

// --- ID Newtypes ---

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NamespaceId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct WorkerId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ServiceId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PodId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ClientId(pub u64);

// --- Timer Keys ---

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TimerKey {
    IdleTimeout { service_id: ServiceId },
    LaunchTimeout { service_id: ServiceId, pod_id: PodId },
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
    Splice { namespace_id: NamespaceId, service_id: ServiceId, worker_id: WorkerId },
    Unsplice { namespace_id: NamespaceId, service_id: ServiceId },
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
    pub state: String,
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
    Splice { client_id: ClientId, service_id: ServiceId, worker_id: WorkerId },
    Unsplice { client_id: ClientId, service_id: ServiceId },
    StreamLogs { client_id: ClientId, service_id: Option<ServiceId> },
    CapacityAvailable,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct NamespaceOutput {
    pub worker_commands: Vec<(WorkerId, WorkerCommand)>,
    pub client_events: Vec<(ClientId, ClientEvent)>,
    pub timers_set: Vec<(TimerKey, Duration)>,
    pub timers_cancel: Vec<TimerKey>,
    pub capacity_requests: Vec<CapacityRequest>,
}

// --- Worker Protocol (Orchestrator-Domain) ---

#[derive(Debug, Clone, PartialEq)]
pub enum WorkerCommand {
    CreateNamespace { namespace_id: NamespaceId },
    DestroyNamespace { namespace_id: NamespaceId },
    CreateService { namespace_id: NamespaceId, service_id: ServiceId, spec: ServiceSpec },
    LaunchPod { namespace_id: NamespaceId, pod_id: PodId, service_id: ServiceId },
    StopPod { namespace_id: NamespaceId, pod_id: PodId },
    UpdateServiceBackend {
        namespace_id: NamespaceId,
        service_id: ServiceId,
        backend: Option<PodId>,
    },
    ServiceReady { namespace_id: NamespaceId, service_id: ServiceId },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WorkerEvent {
    NamespaceCreated,
    ServiceCreated { service_id: ServiceId },
    ServiceActivation { service_id: ServiceId },
    ServiceBackendNeed { service_id: ServiceId, need: BackendNeed },
    PodRunning { pod_id: PodId },
    PodExited { pod_id: PodId },
    PodFailed { pod_id: PodId, reason: String },
}

// --- Domain Enums ---

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum BackendNeed {
    None,
    Traffic,
    Active,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum NamespaceStatus {
    Creating,
    Active,
    Cloning { pending_destroy: bool },
    Destroying,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ServiceState {
    Pending,
    Idle,
    WaitingForCapacity,
    Launching {
        pod_id: PodId,
        worker_id: WorkerId,
        launch_timeout: TimerKey,
    },
    Active {
        pod_id: PodId,
        worker_id: WorkerId,
        hosting: ServiceHosting,
        backend_need: BackendNeed,
        idle_timer: Option<TimerKey>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ServiceHosting {
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
    pub pods: HashSet<PodId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PodInfo {
    pub service_id: ServiceId,
    pub worker_id: WorkerId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerState {
    pub capabilities: WorkerCapabilities,
    pub status: WorkerStatus,
    pub namespaces: HashSet<NamespaceId>,
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
pub struct CapacityRequest {
    pub service_id: ServiceId,
    pub memory_mb: u64,
}

// --- Spec Types ---

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceSpec {
    pub services: HashMap<ServiceId, ServiceSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ServiceSpec {
    pub image: String,
    pub activation: Option<ActivationSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ActivationSpec {
    pub idle_timeout: Duration,
}
