use std::time::Duration;

use super::*;

#[derive(Debug, Clone, PartialEq)]
pub enum NamespaceInput {
    WorkerEvent {
        worker_id: WorkerId,
        event: WorkerEvent,
    },
    WorkerLost {
        worker_id: WorkerId,
    },
    TimerFired {
        timer_key: TimerKey,
    },
    UpdateSpec {
        client_id: ClientId,
        spec: NamespaceSpec,
    },
    Delete {
        client_id: ClientId,
    },
    GetStatus {
        client_id: ClientId,
    },
    Splice {
        client_id: ClientId,
        workload_id: WorkloadName,
        worker_id: WorkerId,
    },
    Unsplice {
        client_id: ClientId,
        workload_id: WorkloadName,
    },
    StreamLogs {
        client_id: ClientId,
        service_id: Option<String>,
    },
    LaunchPod {
        workload_id: WorkloadName,
        worker_id: WorkerId,
        pod_id: PodId,
    },
    ResumePod {
        workload_id: WorkloadName,
        worker_id: WorkerId,
        pod_id: PodId,
        artifact_id: ArtifactId,
    },
    Connect {
        client_id: ClientId,
        client_public_key: [u8; 32],
        worker_wg_public_key: [u8; 32],
        worker_endpoint: String,
    },
    Disconnect {
        client_id: ClientId,
        client_public_key: [u8; 32],
    },
    DeactivateWorkload {
        client_id: ClientId,
        workload_id: WorkloadName,
    },
    PreemptWorkload {
        workload_id: WorkloadName,
    },
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct NamespaceOutput {
    pub worker_commands: Vec<(WorkerId, WorkerCommand)>,
    pub client_events: Vec<(ClientId, ClientEvent)>,
    pub timers_set: Vec<(TimerKey, Duration)>,
    pub timers_cancel: Vec<TimerKey>,
    pub pod_requests: Vec<PodRequest>,
    pub resume_requests: Vec<ResumeRequest>,
    pub destroyed: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResumeRequest {
    pub workload_id: WorkloadName,
    pub artifact_id: ArtifactId,
}

// --- Orchestrator-domain WorkerEvent ---
// This is the orchestrator's view of worker events. It omits `namespace_id`
// (the shell/router strips it) and wire-only variants like `ShuttingDown`,
// `PodLogStreamError`.

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WorkerEvent {
    NamespaceCreated,
    NamespaceFailed {
        error: String,
    },
    NamespaceDestroyed,
    PodRunning {
        pod_id: PodId,
    },
    PodExited {
        pod_id: PodId,
        exit_code: i32,
    },
    PodFailed {
        pod_id: PodId,
        error: String,
    },
    PodSuspended {
        pod_id: PodId,
        artifact_id: ArtifactId,
        pool_id: PoolId,
    },
    PodSuspendFailed {
        pod_id: PodId,
        error: String,
    },
    ArtifactWriteStarted {
        artifact_id: ArtifactId,
        pool_id: PoolId,
    },
    ArtifactWriteCommitted {
        artifact_id: ArtifactId,
        pool_id: PoolId,
        size_bytes: u64,
    },
    EndpointActivation {
        ip: std::net::Ipv4Addr,
        service_id: Option<String>,
    },
    EndpointDemand {
        ip: std::net::Ipv4Addr,
        service_id: Option<String>,
        active: bool,
    },
}
