use distvirt_sm_router::{trace, Aggregator, ListAggregator, SmHandler};

mod service;
mod workload;
mod pod;

use service::*;
use workload::*;
use pod::*;

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub(crate) struct ServiceId(u64);

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub(crate) struct WorkloadId(u64);

/// Readiness info broadcast from workload to services.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ReadyInfo {
    pub(crate) pod_id: PodId,
    pub(crate) worker_id: WorkerId,
}

/// Pod status reported from pod SM back to workload.
#[derive(Clone, Debug, PartialEq, Default)]
pub(crate) enum PodStatus {
    #[default]
    Pending,
    Running,
    Suspending,
    /// Terminal: pod successfully suspended, artifact available for resume.
    Suspended { artifact_id: ArtifactId },
    /// Terminal: pod exited gracefully (exit code 0). Not counted as failure.
    Finished,
    /// Terminal: pod failed (non-zero exit, error, worker loss, timeout, abandoned).
    Failed,
}

impl PodStatus {
    fn is_terminal(&self) -> bool {
        matches!(
            self,
            PodStatus::Suspended { .. } | PodStatus::Failed | PodStatus::Finished
        )
    }
}

/// Intent signal from workload to pod via ownership edge.
#[derive(Clone, Debug, PartialEq, Default)]
pub(crate) enum PodIntent {
    #[default]
    None,
    /// Workload wants this pod running.
    Want,
    /// Workload wants this pod to suspend (preserve state).
    Suspend,
}

/// Identifier for a suspend/resume artifact (snapshot).
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub(crate) struct ArtifactId(u64);

/// Workload spec delivered by management port.
#[derive(Clone, Debug, PartialEq, Default)]
pub(crate) struct WorkloadSpec {
    pub(crate) image: String,
}

/// Service spec delivered by management port.
#[derive(Clone, Debug, PartialEq, Default)]
pub(crate) struct ServiceSpec {
    pub(crate) workload: WorkloadId,
    pub(crate) has_activation: bool,
}

/// Worker info produced by the worker port.
#[derive(Clone, Debug, PartialEq, Default)]
pub(crate) struct WorkerInfo {
    pub(crate) capacity: u32,
}

/// Admin command event payload.
#[derive(Clone, Debug, PartialEq)]
#[allow(dead_code)]
pub(crate) enum AdminCmd {
    Restart,
    /// Safe capacity reclamation: deactivate immediately if idle (no demand),
    /// noop if actively demanded. Can be sent broadly to many workloads.
    ///
    /// Corresponds to the old ForceDeactivate — but with weaker semantics:
    /// ForceDeactivate overrode demand, Scavenge respects it.
    Scavenge,
}

/// Observable workload lifecycle status.
#[derive(Clone, Debug, PartialEq, Default)]
pub(crate) enum WlStatus {
    /// No demand, no pod, no spec — nothing happening.
    #[default]
    Dormant,
    /// Has demand but no spec — can't create a pod yet.
    WaitingForSpec,
    /// Pod created but not yet Running.
    Launching,
    /// Pod is Running and serving.
    Running,
    /// Pod is gracefully suspending (snapshot in progress).
    Suspending,
    /// Pod suspended, artifact saved. Will resume fast on next activation.
    Suspended,
    /// Pod failed, waiting for retry backoff timer.
    RetryBackoff,
    /// Max retries exhausted, terminal failure.
    Failed,
}

/// Observable service lifecycle status.
#[derive(Clone, Debug, PartialEq, Default)]
pub(crate) enum SvcStatus {
    /// Has activation, currently idle (no demand signal).
    #[default]
    Idle,
    /// Wants a backend — demand is set but workload not ready.
    NeedBackend,
    /// Active with a ready backend.
    Active,
}

/// Timer key enum for workload-specific timers.
#[derive(Clone, Debug, PartialEq, Default)]
pub(crate) enum WorkloadTimerKey {
    #[default]
    RetryBackoff,
}

/// Backend need level reported by workers to services.
/// Priority: Active > Traffic > None.
#[derive(Clone, Debug, PartialEq, Default)]
pub(crate) enum BackendNeed {
    #[default]
    None,
    Traffic,
    Active,
}

/// Timer key enum for service-specific timers.
#[derive(Clone, Debug, PartialEq, Default)]
pub(crate) enum ServiceTimerKey {
    #[default]
    IdleTimeout,
}

/// Timer request: service declares which timers it wants active.
#[derive(Clone, Debug, PartialEq, Default)]
pub(crate) struct ServiceTimerRequest {
    key: ServiceTimerKey,
    pub(crate) generation: u64,
}

/// Timer key enum for pod-specific timers.
#[derive(Clone, Debug, PartialEq, Default)]
pub(crate) enum PodTimerKey {
    #[default]
    LaunchTimeout,
    SuspendTimeout,
}

/// Timer request: pod declares which timers it wants active.
#[derive(Clone, Debug, PartialEq, Default)]
pub(crate) struct PodTimerRequest {
    key: PodTimerKey,
    pub(crate) generation: u64,
}

// ============================================================================
// Aggregators
// ============================================================================

/// Timer request: workload declares which timers it wants active.
#[derive(Clone, Debug, PartialEq, Default)]
pub(crate) struct TimerRequest {
    pub(crate) key: WorkloadTimerKey,
    pub(crate) generation: u64,
}

/// Counts services with demand=true, also collects all service IDs.
#[derive(Default)]
pub(crate) struct DemandAggregator;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DemandInfo {
    pub(crate) demand_count: u32,
    pub(crate) service_ids: Vec<ServiceId>,
}

impl Aggregator for DemandAggregator {
    type Input = (ServiceId, bool);
    type Output = DemandInfo;

    fn aggregate(&self, inputs: &[(ServiceId, bool)]) -> DemandInfo {
        let mut demand_count = 0;
        let mut service_ids = Vec::new();
        for (id, demand) in inputs {
            service_ids.push(*id);
            if *demand {
                demand_count += 1;
            }
        }
        DemandInfo {
            demand_count,
            service_ids,
        }
    }
}

/// Aggregates the workload spec, preserving the management port ID.
/// Expects at most one spec source.
#[derive(Default)]
pub(crate) struct SpecAggregator;

impl Aggregator for SpecAggregator {
    type Input = (ManagementId, WorkloadSpec);
    type Output = Option<(ManagementId, WorkloadSpec)>;

    fn aggregate(&self, inputs: &[(ManagementId, WorkloadSpec)]) -> Option<(ManagementId, WorkloadSpec)> {
        inputs.first().cloned()
    }
}

/// Aggregates incoming WorkloadToPod edges to extract the owner workload ID
/// and intent (Want/Suspend/None). Expects at most one owner.
#[derive(Default)]
pub(crate) struct OwnerAggregator;

impl Aggregator for OwnerAggregator {
    type Input = (WorkloadId, PodIntent);
    type Output = Option<(WorkloadId, PodIntent)>;

    fn aggregate(&self, inputs: &[(WorkloadId, PodIntent)]) -> Option<(WorkloadId, PodIntent)> {
        inputs.first().cloned()
    }
}

/// Aggregates BackendNeed from multiple workers. Returns the "hottest" need.
/// Priority: Active > Traffic > None.
#[derive(Default)]
pub(crate) struct BackendNeedAggregator;

impl Aggregator for BackendNeedAggregator {
    type Input = (WorkerId, BackendNeed);
    type Output = BackendNeed;

    fn aggregate(&self, inputs: &[(WorkerId, BackendNeed)]) -> BackendNeed {
        let mut result = BackendNeed::None;
        for (_, need) in inputs {
            match need {
                BackendNeed::Active => return BackendNeed::Active,
                BackendNeed::Traffic => result = BackendNeed::Traffic,
                BackendNeed::None => {}
            }
        }
        result
    }
}

/// Aggregates worker assignment for a pod. A pod expects at most one worker.
/// Preserves the WorkerId (unlike ListAggregator which drops IDs).
#[derive(Default)]
pub(crate) struct WorkerAssignmentAggregator;

impl Aggregator for WorkerAssignmentAggregator {
    type Input = (WorkerId, WorkerInfo);
    type Output = Option<(WorkerId, WorkerInfo)>;

    fn aggregate(&self, inputs: &[(WorkerId, WorkerInfo)]) -> Option<(WorkerId, WorkerInfo)> {
        inputs.first().cloned()
    }
}

/// Aggregates the service spec, preserving the management port ID.
/// Expects at most one spec source.
#[derive(Default)]
pub(crate) struct SvcSpecAggregator;

impl Aggregator for SvcSpecAggregator {
    type Input = (ManagementId, ServiceSpec);
    type Output = Option<(ManagementId, ServiceSpec)>;

    fn aggregate(
        &self,
        inputs: &[(ManagementId, ServiceSpec)],
    ) -> Option<(ManagementId, ServiceSpec)> {
        inputs.first().cloned()
    }
}

// ============================================================================
// Router topology declaration
// ============================================================================

distvirt_sm_router::router! {
    state_machines {
        Service(ServiceId, ServiceSm),
        Workload(WorkloadId, WorkloadSm),
        Pod(auto, PodSm),
    }
    ports {
        Worker(auto),
        Management(auto),
        Timer(auto),
    }
    signals {
        Service::Demand(bool),
        Service::SvcWantedTimers(Vec<ServiceTimerRequest>),
        Service::SvcStatusSignal(SvcStatus),
        Service::IdleTimerActiveSignal(bool),
        Workload::Readiness(Option<ReadyInfo>),
        Workload::PodIntent(PodIntent),
        Workload::WantedTimers(Vec<TimerRequest>),
        Workload::WlStatusSignal(WlStatus),
        Workload::ConsecutiveFailuresSignal(u32),
        Workload::SpecStaleSignal(bool),
        Pod::Status(PodStatus),
        Pod::Worker(Option<WorkerId>),
        Pod::WantedPodTimers(Vec<PodTimerRequest>),
        Worker::Info(WorkerInfo),
        Worker::BackendNeed(BackendNeed),
        Management::WlSpec(WorkloadSpec),
        Management::SvcSpec(ServiceSpec),
    }
    edges {
        ServiceToWorkload: Service -> Workload,
        WorkloadToService: Workload -> Service,
        WorkloadToPod: Workload -> Pod,
        PodToWorkload: Pod -> Workload,
        WorkerToPod: Worker -> Pod,
        WorkerToService: Worker -> Service,
        ManagementToWorkload: Management -> Workload,
        ManagementToService: Management -> Service,
        WorkloadToTimer: Workload -> Timer,
        ServiceToTimer: Service -> Timer,
        PodToTimer: Pod -> Timer,
    }
    events {
        AdminCommand(AdminCmd): Management -> Workload,
        ActivateService(bool): Management -> Service,
        NotifyPodStatus(PodStatus): Worker -> Pod,
        NotifyPodSuspended(ArtifactId): Worker -> Pod,
        WorkloadTimerFired(WorkloadTimerKey): Timer -> Workload,
        ServiceTimerFired(ServiceTimerKey): Timer -> Service,
        PodTimerFired(PodTimerKey): Timer -> Pod,
    }
    invariants {
        // Worker info should always have positive capacity
        Worker::Info(value.capacity > 0),
    }
    inputs {
        Workload::DemandInput {
            sources: [(ServiceToWorkload, Service::Demand)],
            aggregator: DemandAggregator,
        },
        Workload::SpecInput {
            sources: [(ManagementToWorkload, Management::WlSpec)],
            aggregator: SpecAggregator,
        },
        Workload::PodStatusInput {
            sources: [(PodToWorkload, Pod::Status)],
            aggregator: ListAggregator<PodId, PodStatus>,
        },
        Workload::PodWorkerInput {
            sources: [(PodToWorkload, Pod::Worker)],
            aggregator: ListAggregator<PodId, Option<WorkerId>>,
        },
        Service::ReadinessInput {
            sources: [(WorkloadToService, Workload::Readiness)],
            aggregator: ListAggregator<WorkloadId, Option<ReadyInfo>>,
        },
        Service::SvcSpecInput {
            sources: [(ManagementToService, Management::SvcSpec)],
            aggregator: SvcSpecAggregator,
        },
        Service::BackendNeedInput {
            sources: [(WorkerToService, Worker::BackendNeed)],
            aggregator: BackendNeedAggregator,
        },
        Pod::WorkerInput {
            sources: [(WorkerToPod, Worker::Info)],
            aggregator: WorkerAssignmentAggregator,
        },
        Pod::OwnerInput {
            sources: [(WorkloadToPod, Workload::PodIntent)],
            aggregator: OwnerAggregator,
        },
        Timer::WorkloadTimersInput {
            sources: [(WorkloadToTimer, Workload::WantedTimers)],
            aggregator: ListAggregator<WorkloadId, Vec<TimerRequest>>,
        },
        Timer::ServiceTimersInput {
            sources: [(ServiceToTimer, Service::SvcWantedTimers)],
            aggregator: ListAggregator<ServiceId, Vec<ServiceTimerRequest>>,
        },
        Timer::PodTimersInput {
            sources: [(PodToTimer, Pod::WantedPodTimers)],
            aggregator: ListAggregator<PodId, Vec<PodTimerRequest>>,
        },
    }
}

#[cfg(test)]
mod tests;
