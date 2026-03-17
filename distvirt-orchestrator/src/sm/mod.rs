use distvirt_sm_router::{Aggregator, IncrementalAggregator, ListAggregator, SmHandler};

use crate::adapter::pod_assignment::PodAssignmentIncrementalAggregator;
use crate::adapter::schedule_request::ScheduleRequestIncrementalAggregator;
use crate::adapter::endpoint::EndpointIncrementalAggregator;
use crate::adapter::timer::{
    PodTimerIncrementalAggregator, ServiceTimerIncrementalAggregator,
    WorkloadTimerIncrementalAggregator,
};

mod pod;
mod service;
mod workload;

pub use pod::*;
pub use service::*;
pub use workload::*;

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct ServiceId(pub u64);

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct WorkloadId(pub u64);

/// Readiness info broadcast from workload to services.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ReadyInfo {
    pub pod_id: PodId,
    pub worker_id: WorkerId,
}

/// Pod status reported from pod SM back to workload.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub enum PodStatus {
    #[default]
    Pending,
    Running,
    Suspending,
    /// Terminal: pod successfully suspended, artifact available for resume.
    Suspended {
        artifact_id: ArtifactId,
    },
    /// Terminal: pod exited gracefully (exit code 0). Not counted as failure.
    Finished,
    /// Terminal: pod failed (non-zero exit, error, timeout, abandoned).
    Failed,
    /// Terminal: pod displaced by infrastructure — worker disconnect or lease revocation.
    Displaced,
}

impl PodStatus {
    fn is_terminal(&self) -> bool {
        matches!(
            self,
            PodStatus::Suspended { .. }
                | PodStatus::Failed
                | PodStatus::Finished
                | PodStatus::Displaced
        )
    }
}

/// Intent signal from workload to pod via ownership edge.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub enum PodIntent {
    #[default]
    None,
    /// Workload wants this pod running.
    Want,
    /// Workload wants this pod to suspend (preserve state).
    Suspend,
}

/// Identifier for a suspend/resume artifact (snapshot).
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct ArtifactId(pub u64);

/// Manual ID for the singleton schedule-request port.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct ScheduleRequestId(pub u64);

/// The singleton schedule-request port ID.
pub const SCHEDULE_REQUEST: ScheduleRequestId = ScheduleRequestId(0);

/// Manual ID for the singleton endpoint port.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct EndpointId(pub u64);

/// The singleton endpoint port ID.
pub const ENDPOINT: EndpointId = EndpointId(0);

/// Manual ID for the singleton timer port.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct TimerId(pub u64);

/// The singleton timer port ID.
pub const TIMER: TimerId = TimerId(0);

/// Delta produced by the incremental schedule-request aggregator.
#[derive(Clone, Debug, PartialEq)]
pub enum ScheduleRequestDelta {
    Request {
        pod_id: PodId,
        request: PodScheduleRequest,
    },
    Drop {
        pod_id: PodId,
    },
}

/// Schedule request emitted by pod to the schedule-request port.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct PodScheduleRequest {
    pub resume_artifact: Option<ArtifactId>,
    /// Set to true when the pod is in Suspending state and needs a SuspendPod command.
    pub suspend: bool,
}

/// Lease info signaled from a per-pod ScheduleLease port.
/// Carries the assigned worker ID.
#[derive(Clone, Debug, PartialEq)]
pub struct LeaseInfo {
    pub worker_id: WorkerId,
}

impl Default for LeaseInfo {
    fn default() -> Self {
        LeaseInfo {
            worker_id: WorkerId(0),
        }
    }
}

/// Workload spec delivered by management port.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct WorkloadSpec {
    pub image: String,
    /// If true, suspend the pod instead of destroying it when demand drops.
    /// Enables fast resume from snapshot on re-activation.
    pub suspend_on_idle: bool,
}

/// Service spec delivered by management port.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct ServiceSpec {
    pub workload: WorkloadId,
    pub has_activation: bool,
    /// Per-service idle timeout. Only meaningful when `has_activation` is true.
    pub idle_timeout: std::time::Duration,
}

/// Worker info produced by the worker port.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct WorkerInfo {
    pub capacity: u32,
}

/// Admin command event payload.
#[derive(Clone, Debug, PartialEq)]
#[allow(dead_code)]
pub enum AdminCmd {
    Restart,
    /// Safe capacity reclamation: deactivate immediately if idle (no demand),
    /// noop if actively demanded. Can be sent broadly to many workloads.
    ///
    /// Corresponds to the old ForceDeactivate — but with weaker semantics:
    /// ForceDeactivate overrode demand, Scavenge respects it.
    Scavenge,
}

/// Observable workload lifecycle status.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub enum WlStatus {
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
pub enum SvcStatus {
    /// Has activation, currently idle (no demand signal).
    #[default]
    Idle,
    /// Wants a backend — demand is set but workload not ready.
    NeedBackend,
    /// Active with a ready backend.
    Active,
}

/// Timer key enum for workload-specific timers.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub enum WorkloadTimerKey {
    #[default]
    RetryBackoff,
}

/// Backend need level reported by workers to services.
/// Priority: Active > Traffic > None.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub enum BackendNeed {
    #[default]
    None,
    Traffic,
    Active,
}

/// Timer key enum for service-specific timers.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub enum ServiceTimerKey {
    #[default]
    IdleTimeout,
}

/// Timer request: service declares which timers it wants active.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct ServiceTimerRequest {
    pub key: ServiceTimerKey,
    pub generation: u64,
    pub duration: std::time::Duration,
}

/// Timer key enum for pod-specific timers.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub enum PodTimerKey {
    #[default]
    LaunchTimeout,
    SuspendTimeout,
}

/// Timer request: pod declares which timers it wants active.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct PodTimerRequest {
    pub key: PodTimerKey,
    pub generation: u64,
    pub duration: std::time::Duration,
}

// ============================================================================
// Aggregators
// ============================================================================

/// Timer request: workload declares which timers it wants active.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct TimerRequest {
    pub key: WorkloadTimerKey,
    pub generation: u64,
    pub duration: std::time::Duration,
}

/// Counts services with demand=true, also collects all service IDs.
#[derive(Default)]
pub struct DemandAggregator;

#[derive(Clone, Debug, PartialEq)]
pub struct DemandInfo {
    pub demand_count: u32,
    pub service_ids: Vec<ServiceId>,
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
pub struct SpecAggregator;

impl Aggregator for SpecAggregator {
    type Input = (ManagementId, WorkloadSpec);
    type Output = Option<(ManagementId, WorkloadSpec)>;

    fn aggregate(
        &self,
        inputs: &[(ManagementId, WorkloadSpec)],
    ) -> Option<(ManagementId, WorkloadSpec)> {
        inputs.first().cloned()
    }
}

/// Aggregates incoming WorkloadToPod edges to extract the owner workload ID
/// and intent (Want/Suspend/None). Expects at most one owner.
#[derive(Default)]
pub struct OwnerAggregator;

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
pub struct BackendNeedAggregator;

impl Aggregator for BackendNeedAggregator {
    type Input = (BackendNeedId, BackendNeed);
    type Output = BackendNeed;

    fn aggregate(&self, inputs: &[(BackendNeedId, BackendNeed)]) -> BackendNeed {
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

/// Aggregates lease info for a pod. Expects 0 or 1 lease source.
#[derive(Default)]
pub struct LeaseAggregator;

impl Aggregator for LeaseAggregator {
    type Input = (ScheduleLeaseId, LeaseInfo);
    type Output = Option<LeaseInfo>;

    fn aggregate(&self, inputs: &[(ScheduleLeaseId, LeaseInfo)]) -> Option<LeaseInfo> {
        inputs.first().map(|(_, info)| info.clone())
    }
}

/// Aggregates worker assignment for a pod. A pod expects at most one worker.
/// Preserves the WorkerId (unlike ListAggregator which drops IDs).
#[derive(Default)]
pub struct WorkerAssignmentAggregator;

impl Aggregator for WorkerAssignmentAggregator {
    type Input = (WorkerId, WorkerInfo);
    type Output = Option<(WorkerId, WorkerInfo)>;

    fn aggregate(&self, inputs: &[(WorkerId, WorkerInfo)]) -> Option<(WorkerId, WorkerInfo)> {
        inputs.first().cloned()
    }
}

/// Aggregator that preserves source IDs. Like ListAggregator but keeps (Id, V) pairs.
/// Used by timer inputs so the adapter can map requests back to their source SM.
pub struct IdListAggregator<Id, V>(std::marker::PhantomData<(Id, V)>);

impl<Id, V> Default for IdListAggregator<Id, V> {
    fn default() -> Self {
        IdListAggregator(std::marker::PhantomData)
    }
}

impl<Id: Clone, V: Clone> Aggregator for IdListAggregator<Id, V> {
    type Input = (Id, V);
    type Output = Vec<(Id, V)>;

    fn aggregate(&self, inputs: &[(Id, V)]) -> Vec<(Id, V)> {
        inputs.to_vec()
    }
}

/// Aggregates the service spec, preserving the management port ID.
/// Expects at most one spec source.
#[derive(Default)]
pub struct SvcSpecAggregator;

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
        BackendNeed(auto),
        Management(auto),
        Timer(TimerId),
        ScheduleRequest(ScheduleRequestId),
        ScheduleLease(auto),
        Endpoint(EndpointId),
    }
    signals {
        Service::Demand(bool),
        Service::SvcWantedTimers(Vec<ServiceTimerRequest>),
        Service::SvcStatusSignal(SvcStatus),
        Service::IdleTimerActiveSignal(bool),
        Service::EndpointInfo(Option<ReadyInfo>),
        Workload::Readiness(Option<ReadyInfo>),
        Workload::PodIntent(PodIntent),
        Workload::WantedTimers(Vec<TimerRequest>),
        Workload::WlStatusSignal(WlStatus),
        Workload::ConsecutiveFailuresSignal(u32),
        Workload::SpecStaleSignal(bool),
        Pod::Status(PodStatus),
        Pod::Worker(Option<WorkerId>),
        Pod::WantedPodTimers(Vec<PodTimerRequest>),
        Pod::ScheduleRequest(PodScheduleRequest),
        Worker::Info(WorkerInfo),
        BackendNeed::Level(BackendNeed),
        Management::WlSpec(WorkloadSpec),
        Management::SvcSpec(ServiceSpec),
        ScheduleLease::Lease(LeaseInfo),
    }
    edges {
        ServiceToWorkload: Service -> Workload,
        WorkloadToService: Workload -> Service,
        WorkloadToPod: Workload -> Pod,
        PodToWorkload: Pod -> Workload,
        WorkerToPod: Worker -> Pod,
        BackendNeedToService: BackendNeed -> Service,
        ManagementToWorkload: Management -> Workload,
        ManagementToService: Management -> Service,
        WorkloadToTimer: Workload -> Timer,
        ServiceToTimer: Service -> Timer,
        PodToTimer: Pod -> Timer,
        PodToScheduleRequest: Pod -> ScheduleRequest,
        ScheduleLeaseToPod: ScheduleLease -> Pod,
        PodToWorker: Pod -> Worker,
        ServiceToEndpoint: Service -> Endpoint,
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
            sources: [(BackendNeedToService, BackendNeed::Level)],
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
        Pod::LeaseInput {
            sources: [(ScheduleLeaseToPod, ScheduleLease::Lease)],
            aggregator: LeaseAggregator,
        },
        Worker::AssignedPodsInput {
            sources: [(PodToWorker, Pod::ScheduleRequest)],
            incremental_aggregator: PodAssignmentIncrementalAggregator,
        },
        ScheduleRequest::PodRequestsInput {
            sources: [(PodToScheduleRequest, Pod::ScheduleRequest)],
            incremental_aggregator: ScheduleRequestIncrementalAggregator,
        },
        Timer::WorkloadTimersInput {
            sources: [(WorkloadToTimer, Workload::WantedTimers)],
            incremental_aggregator: WorkloadTimerIncrementalAggregator,
        },
        Timer::ServiceTimersInput {
            sources: [(ServiceToTimer, Service::SvcWantedTimers)],
            incremental_aggregator: ServiceTimerIncrementalAggregator,
        },
        Timer::PodTimersInput {
            sources: [(PodToTimer, Pod::WantedPodTimers)],
            incremental_aggregator: PodTimerIncrementalAggregator,
        },
        Endpoint::ServiceEndpointsInput {
            sources: [(ServiceToEndpoint, Service::EndpointInfo)],
            incremental_aggregator: EndpointIncrementalAggregator,
        },
    }
}

// TODO: temporary, switch back to plain Router when refactor stabilizes
pub type DRouter = Router<distvirt_sm_router::trace::PanicTracer>;

#[cfg(test)]
impl PodId {
    pub(crate) fn test(id: u64) -> Self {
        PodId(id)
    }
}

#[cfg(test)]
mod tests;
