use std::time::Duration;

use super::*;

// --- Namespace Events (emitted during state transitions) ---

#[derive(Debug, Clone, PartialEq)]
pub enum SmNamespaceEvent {
    Workload {
        workload_id: WorkloadId,
        event: SmWorkloadEvent,
    },
    Service {
        service_id: String,
        workload_id: WorkloadId,
        event: SmServiceEvent,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum SmWorkloadEvent {
    DemandChanged {
        demanding_services: u32,
    },
    PodLaunching {
        pod_id: PodId,
        worker_id: WorkerId,
    },
    PodRunning {
        pod_id: PodId,
        worker_id: WorkerId,
    },
    PodStopped {
        exit_code: i32,
    },
    PodFailed {
        reason: String,
    },
    PodSuspending {
        pod_id: PodId,
        worker_id: WorkerId,
    },
    PodSuspended {
        worker_id: WorkerId,
        artifact_id: ArtifactId,
    },
    PodSuspendFailed {
        reason: String,
    },
    PodResuming {
        pod_id: PodId,
        worker_id: WorkerId,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum SmServiceEvent {
    Activated { trigger: ServiceActivationTrigger },
    BackendReady,
    IdleTimerStarted { timeout: Duration },
    IdleTimerCancelled { reason: IdleTimerCancelReason },
    IdleTimeoutFired,
    Deactivated { reason: ServiceDeactivationReason },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceActivationTrigger {
    Traffic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdleTimerCancelReason {
    NewTraffic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceDeactivationReason {
    IdleTimeout,
    ForceDeactivate,
}
