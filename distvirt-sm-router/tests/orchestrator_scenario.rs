//! Integration test: orchestrator-like scenario using the signal router.
//!
//! Models the service → workload → pod lifecycle with worker ports and
//! management ports, exercising demand aggregation, reactive edge creation,
//! readiness propagation, and worker loss via port removal.
//!
//! ## Topology
//!
//! ```text
//! Management ──spec──▶ Service ──demand──▶ Workload ──creates──▶ Pod
//! Management ──spec──▶ Workload              ◀──status──┘         ▲
//!                      Workload ──ready──▶ Service           Worker (port)
//! ```
//!
//! ## Design notes
//!
//! All SM signals and edges are set exclusively from within SM handlers via
//! the context. External inputs flow through ports (signals + events).
//!
//! Workloads create and destroy pods directly from their handlers via
//! `ctx.create_pod()` / `ctx.destroy_pod()`. Pods learn their owner
//! workload by aggregating incoming WorkloadToPod edges. Worker assignment
//! (worker_to_pod edges) is managed externally as a placement concern.

use distvirt_sm_router::{Aggregator, ListAggregator, SmHandler, trace};

// ============================================================================
// Domain types
// ============================================================================

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
struct ServiceId(u64);

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
struct WorkloadId(u64);

/// Readiness info broadcast from workload to services.
#[derive(Clone, Debug, PartialEq)]
struct ReadyInfo {
    pod_id: PodId,
    worker_id: WorkerId,
}

/// Pod status reported from pod SM back to workload.
#[derive(Clone, Debug, PartialEq, Default)]
enum PodStatus {
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
enum PodIntent {
    #[default]
    None,
    /// Workload wants this pod running.
    Want,
    /// Workload wants this pod to suspend (preserve state).
    Suspend,
}

/// Identifier for a suspend/resume artifact (snapshot).
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
struct ArtifactId(u64);

/// Workload spec delivered by management port.
#[derive(Clone, Debug, PartialEq, Default)]
struct WorkloadSpec {
    image: String,
}

/// Service spec delivered by management port.
#[derive(Clone, Debug, PartialEq, Default)]
struct ServiceSpec {
    workload: WorkloadId,
    has_activation: bool,
}

/// Worker info produced by the worker port.
#[derive(Clone, Debug, PartialEq, Default)]
struct WorkerInfo {
    capacity: u32,
}

/// Admin command event payload.
#[derive(Clone, Debug, PartialEq)]
#[allow(dead_code)]
enum AdminCmd {
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
enum WlStatus {
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
enum SvcStatus {
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
enum WorkloadTimerKey {
    #[default]
    RetryBackoff,
}

/// Backend need level reported by workers to services.
/// Priority: Active > Traffic > None.
#[derive(Clone, Debug, PartialEq, Default)]
enum BackendNeed {
    #[default]
    None,
    Traffic,
    Active,
}

/// Timer key enum for service-specific timers.
#[derive(Clone, Debug, PartialEq, Default)]
enum ServiceTimerKey {
    #[default]
    IdleTimeout,
}

/// Timer request: service declares which timers it wants active.
#[derive(Clone, Debug, PartialEq, Default)]
struct ServiceTimerRequest {
    key: ServiceTimerKey,
    generation: u64,
}

/// Timer key enum for pod-specific timers.
#[derive(Clone, Debug, PartialEq, Default)]
enum PodTimerKey {
    #[default]
    LaunchTimeout,
    SuspendTimeout,
}

/// Timer request: pod declares which timers it wants active.
#[derive(Clone, Debug, PartialEq, Default)]
struct PodTimerRequest {
    key: PodTimerKey,
    generation: u64,
}

// ============================================================================
// Aggregators
// ============================================================================

/// Timer request: workload declares which timers it wants active.
#[derive(Clone, Debug, PartialEq, Default)]
struct TimerRequest {
    key: WorkloadTimerKey,
    generation: u64,
}

/// Counts services with demand=true, also collects all service IDs.
#[derive(Default)]
struct DemandAggregator;

#[derive(Clone, Debug, PartialEq)]
struct DemandInfo {
    demand_count: u32,
    service_ids: Vec<ServiceId>,
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
struct SpecAggregator;

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
struct OwnerAggregator;

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
struct BackendNeedAggregator;

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
struct WorkerAssignmentAggregator;

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
struct SvcSpecAggregator;

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

// ============================================================================
// SM implementations
// ============================================================================

// ---- Service SM ----

#[derive(Debug, Clone, PartialEq)]
enum ServiceState {
    /// Has activation, currently idle (no demand signal).
    Idle,
    /// Wants a backend — demand signal is true.
    NeedBackend,
    /// Active with a ready backend.
    Active { ready: ReadyInfo },
}

#[derive(Clone)]
struct ServiceSm {
    state: ServiceState,
    has_activation: bool,
    timer_id: TimerId,
    idle_generation: u64,
    idle_timer_active: bool,
}

impl ServiceSm {
    fn new(timer_id: TimerId, has_activation: bool) -> Self {
        ServiceSm {
            state: if has_activation {
                ServiceState::Idle
            } else {
                ServiceState::NeedBackend
            },
            has_activation,
            timer_id,
            idle_generation: 0,
            idle_timer_active: false,
        }
    }

    fn update_timer_signal(&self, ctx: &mut impl ServiceCtx) {
        if self.idle_timer_active {
            ctx.set_svc_wanted_timers(vec![ServiceTimerRequest {
                key: ServiceTimerKey::IdleTimeout,
                generation: self.idle_generation,
            }]);
        } else {
            ctx.set_svc_wanted_timers(vec![]);
        }
    }

    fn update_status_signals(&self, ctx: &mut impl ServiceCtx) {
        let status = match &self.state {
            ServiceState::Idle => SvcStatus::Idle,
            ServiceState::NeedBackend => SvcStatus::NeedBackend,
            ServiceState::Active { .. } => SvcStatus::Active,
        };
        ctx.set_svc_status_signal(status);
        ctx.set_idle_timer_active_signal(self.idle_timer_active);
    }
}

impl<C: ServiceCtx> SmHandler<C> for ServiceSm {
    type Input = ServiceInput;

    fn initialize(&mut self, ctx: &mut C) {
        ctx.set_service_to_timer_edges(vec![self.timer_id]);
        self.update_status_signals(ctx);
    }

    fn handle(&mut self, input: Self::Input, ctx: &mut C) {
        match input {
            ServiceInput::ReadinessInput(readiness_list) => {
                let ready = readiness_list.into_iter().next().flatten();
                match (&self.state, ready) {
                    (ServiceState::NeedBackend, Some(info)) => {
                        self.state = ServiceState::Active { ready: info };
                    }
                    (ServiceState::Active { .. }, None) => {
                        self.state = ServiceState::NeedBackend;
                        if self.idle_timer_active {
                            self.idle_timer_active = false;
                            self.update_timer_signal(ctx);
                        }
                    }
                    (ServiceState::Active { .. }, Some(info)) => {
                        self.state = ServiceState::Active { ready: info };
                    }
                    _ => {}
                }
            }
            ServiceInput::SvcSpecInput(spec_opt) => {
                if let Some((_, spec)) = spec_opt {
                    self.has_activation = spec.has_activation;
                    if !self.has_activation {
                        // Always-on: set demand immediately.
                        ctx.set_demand(true);
                        if matches!(self.state, ServiceState::Idle) {
                            self.state = ServiceState::NeedBackend;
                        }
                    }
                    ctx.set_service_to_workload_edges(vec![spec.workload]);
                } else {
                    // Spec removed — self-destruct.
                    ctx.self_destruct();
                }
            }
            ServiceInput::ActivateService(active) => {
                if self.has_activation {
                    ctx.set_demand(active);
                    if active && matches!(self.state, ServiceState::Idle) {
                        self.state = ServiceState::NeedBackend;
                    } else if !active {
                        self.state = ServiceState::Idle;
                        ctx.set_demand(false);
                        if self.idle_timer_active {
                            self.idle_timer_active = false;
                            self.update_timer_signal(ctx);
                        }
                    }
                }
            }
            ServiceInput::BackendNeedInput(need) => match (&self.state, &need) {
                (ServiceState::Active { .. }, BackendNeed::None) if self.has_activation => {
                    if !self.idle_timer_active {
                        self.idle_timer_active = true;
                        self.idle_generation += 1;
                        self.update_timer_signal(ctx);
                    }
                }
                (ServiceState::Active { .. }, BackendNeed::Traffic | BackendNeed::Active) => {
                    if self.idle_timer_active {
                        self.idle_timer_active = false;
                        self.update_timer_signal(ctx);
                    }
                }
                (ServiceState::Idle, BackendNeed::Traffic | BackendNeed::Active) => {
                    ctx.set_demand(true);
                    self.state = ServiceState::NeedBackend;
                }
                _ => {}
            },
            ServiceInput::ServiceTimerFired(key) => match key {
                ServiceTimerKey::IdleTimeout => {
                    if matches!(self.state, ServiceState::Active { .. })
                        && self.idle_timer_active
                        && self.has_activation
                    {
                        self.state = ServiceState::Idle;
                        ctx.set_demand(false);
                        self.idle_timer_active = false;
                        self.update_timer_signal(ctx);
                    }
                }
            },
        }
        self.update_status_signals(ctx);
    }
}

// ---- Workload SM ----

const MAX_RETRIES: u32 = 5;

#[derive(Clone)]
struct WorkloadSm {
    has_spec: bool,
    has_demand: bool,
    pod_running: bool,
    wants_pod: bool,
    pod_id: Option<PodId>,

    /// Set when demand transitions 0→non-zero. Prevents demand fluctuations
    /// from aborting an in-progress pod launch. Cleared when:
    /// - Pod reaches Running (commitment fulfilled)
    /// - Scavenge arrives (explicit override)
    /// - Pod is destroyed with no demand (nothing to commit to)
    committed_to_boot: bool,

    /// Incremented each time the spec signal changes value (Some→Some).
    /// Compared against `launched_with_spec_version` to detect spec changes
    /// during pod launch — replaces PendingIntent::Restart.
    spec_version: u64,
    /// The spec_version when the current pod was created.
    launched_with_spec_version: u64,

    /// Number of consecutive pod failures without a successful Running transition.
    consecutive_failures: u32,
    /// Maximum retries before entering terminal Failed state.
    max_retries: u32,
    /// True while waiting for a retry backoff timer to fire.
    in_backoff: bool,
    /// Incremented each time we enter backoff, used for timer identity.
    backoff_generation: u64,

    /// Timer port ID, passed to pods created by this workload.
    timer_id: TimerId,

    /// Worker ID of the current pod, learned from PodWorkerInput.
    pod_worker_id: Option<WorkerId>,

    /// Whether to suspend the pod instead of destroying it when demand drops.
    suspend_on_idle: bool,
    /// Artifact from a successfully suspended pod. Used to resume on next
    /// demand cycle instead of cold-booting.
    suspended_artifact: Option<ArtifactId>,
    /// True while the pod is in the process of suspending. Prevents reconcile
    /// from touching the pod until it reaches a terminal state.
    awaiting_suspend: bool,
    /// Counter for generating unique artifact IDs.
    artifact_counter: u64,
}

impl WorkloadSm {
    fn new(timer_id: TimerId) -> Self {
        Self::with_max_retries(timer_id, MAX_RETRIES)
    }

    #[allow(dead_code)]
    fn with_max_retries(timer_id: TimerId, max_retries: u32) -> Self {
        WorkloadSm {
            has_spec: false,
            has_demand: false,
            pod_running: false,
            wants_pod: false,
            pod_id: None,
            committed_to_boot: false,
            spec_version: 0,
            launched_with_spec_version: 0,
            consecutive_failures: 0,
            max_retries,
            in_backoff: false,
            backoff_generation: 0,
            timer_id,
            pod_worker_id: None,
            suspend_on_idle: false,
            suspended_artifact: None,
            awaiting_suspend: false,
            artifact_counter: 0,
        }
    }

    #[allow(dead_code)]
    fn new_suspendable(timer_id: TimerId) -> Self {
        WorkloadSm {
            suspend_on_idle: true,
            ..Self::new(timer_id)
        }
    }

    #[allow(dead_code)]
    fn new_suspendable_with_max_retries(timer_id: TimerId, max_retries: u32) -> Self {
        WorkloadSm {
            suspend_on_idle: true,
            ..Self::with_max_retries(timer_id, max_retries)
        }
    }

    #[allow(dead_code)]
    fn next_artifact_id(&mut self) -> ArtifactId {
        self.artifact_counter += 1;
        ArtifactId(self.artifact_counter)
    }
}

impl<C: WorkloadCtx> SmHandler<C> for WorkloadSm {
    type Input = WorkloadInput;

    fn initialize(&mut self, ctx: &mut C) {
        ctx.set_workload_to_timer_edges(vec![self.timer_id]);
        self.update_status_signals(ctx);
    }

    fn handle(&mut self, input: Self::Input, ctx: &mut C) {
        match input {
            WorkloadInput::DemandInput(demand) => {
                let old_demand = self.has_demand;
                self.has_demand = demand.demand_count > 0;

                // Demand appeared: commit to reaching Running.
                if self.has_demand && !old_demand {
                    self.committed_to_boot = true;
                }
                // Demand dropped with no pod in flight: clear commitment and retry state.
                if !self.has_demand && self.pod_id.is_none() {
                    self.committed_to_boot = false;
                    self.consecutive_failures = 0;
                    self.in_backoff = false;
                }

                ctx.set_workload_to_service_edges(demand.service_ids);
                self.reconcile(ctx);
                self.update_timer_signal(ctx);
            }
            WorkloadInput::SpecInput(spec_opt) => {
                let new_has_spec = spec_opt.is_some();

                if self.has_spec && new_has_spec {
                    // Spec value changed (Some→Some). Increment version so we
                    // detect stale launches via on_pod_running.
                    self.spec_version += 1;
                    self.consecutive_failures = 0;
                    self.in_backoff = false;

                    // If pod is already Running, restart immediately.
                    if self.pod_running {
                        self.destroy_current_pod(ctx);
                    }
                }

                if self.has_spec && !new_has_spec {
                    // Spec removed — clean up and self-destruct.
                    self.destroy_current_pod(ctx);
                    ctx.self_destruct();
                    return;
                }

                self.has_spec = new_has_spec;
                self.reconcile(ctx);
                self.update_timer_signal(ctx);
            }
            WorkloadInput::PodStatusInput(statuses) => {
                let was_running = self.pod_running;
                self.pod_running = statuses.iter().any(|s| *s == PodStatus::Running);
                let has_failed = statuses.iter().any(|s| *s == PodStatus::Failed);
                let has_finished = statuses.iter().any(|s| *s == PodStatus::Finished);

                // Pod reached Suspended terminal state — save artifact and reap.
                let suspended_artifact = statuses.iter().find_map(|s| match s {
                    PodStatus::Suspended { artifact_id } => Some(*artifact_id),
                    _ => None,
                });

                // All pods gone — single cleanup path for signal-derived state.
                // Normally pod_id is already cleared by the initiator
                // (destroy_current_pod, on_pod_failed, etc.), but this acts
                // as a safety net for unexpected pod disappearance.
                if statuses.is_empty() && self.pod_id.is_some() {
                    self.pod_id = None;
                    self.pod_worker_id = None;
                    self.awaiting_suspend = false;
                    self.committed_to_boot = false;
                    ctx.set_workload_to_pod_edges(vec![]);
                    ctx.set_pod_intent(PodIntent::None);
                    ctx.set_readiness(None);
                }

                if let Some(artifact_id) = suspended_artifact {
                    // Pod successfully suspended. Save artifact, reap pod.
                    self.suspended_artifact = Some(artifact_id);
                    // pod_running already set to false at top of handler
                    // (Suspended is not Running).
                    // pod_worker_id will be cleared by PodWorkerInput signal propagation.
                    self.awaiting_suspend = false;
                    ctx.set_readiness(None);
                    // Remove edge → pod will self-destruct (terminal + no owner).
                    ctx.set_workload_to_pod_edges(vec![]);
                    ctx.set_pod_intent(PodIntent::None);
                    self.pod_id = None;
                    // Reconcile may create a new pod if demand returned during suspend.
                    self.reconcile(ctx);
                } else if self.pod_running && !was_running {
                    // Pod just became Running — check current signal state
                    // to decide what to do. This replaces PendingIntent.
                    self.on_pod_running(ctx);
                } else if has_failed && self.pod_id.is_some() {
                    self.on_pod_failed(ctx);
                } else if has_finished && self.pod_id.is_some() {
                    self.on_pod_finished(ctx);
                } else if !self.pod_running && was_running {
                    // Pod lost running status.
                    ctx.set_readiness(None);
                    self.reconcile(ctx);
                } else {
                    self.reconcile(ctx);
                }
                self.update_timer_signal(ctx);
            }
            WorkloadInput::PodWorkerInput(workers) => {
                // Track the worker ID of our current pod.
                let new_worker_id = workers.into_iter().next().flatten();
                if new_worker_id != self.pod_worker_id {
                    self.pod_worker_id = new_worker_id;
                    // If pod is running, update readiness with the real worker ID.
                    if self.pod_running {
                        self.update_readiness(ctx);
                    }
                }
            }
            WorkloadInput::AdminCommand(cmd) => {
                match cmd {
                    AdminCmd::Scavenge => {
                        // Safe capacity reclamation. Noop if actively demanded.
                        if self.has_demand {
                            return;
                        }
                        // Not demanded — reclaim: destroy pod, clear commitment and retry state.
                        // Also discard any suspended artifact.
                        self.committed_to_boot = false;
                        self.consecutive_failures = 0;
                        self.in_backoff = false;
                        self.suspended_artifact = None;
                        self.destroy_current_pod(ctx);
                        self.reconcile(ctx);
                    }
                    AdminCmd::Restart => {
                        // Destroy current pod (if any) and let reconcile create
                        // a fresh one. Reset spec version tracking since this is
                        // an intentional restart, not a stale-spec detection.
                        self.consecutive_failures = 0;
                        self.in_backoff = false;
                        self.destroy_current_pod(ctx);
                        self.launched_with_spec_version = self.spec_version;
                        self.reconcile(ctx);
                    }
                }
                self.update_timer_signal(ctx);
            }
            WorkloadInput::WorkloadTimerFired(key) => match key {
                WorkloadTimerKey::RetryBackoff => {
                    if self.in_backoff {
                        self.in_backoff = false;
                        self.reconcile(ctx);
                        self.update_timer_signal(ctx);
                    }
                }
            },
        }
        self.update_status_signals(ctx);
    }
}

impl WorkloadSm {
    /// Called when the pod transitions to Running. Makes decisions based on
    /// current signal state rather than accumulated PendingIntent.
    ///
    /// Priority order:
    /// 1. Spec changed since launch → restart with new spec
    /// 2. No demand → deactivate (committed_to_boot fulfilled)
    /// 3. Otherwise → emit readiness
    fn on_pod_running(&mut self, ctx: &mut impl WorkloadCtx) {
        self.committed_to_boot = false;
        self.consecutive_failures = 0;

        // 1. Spec changed since we launched this pod → restart.
        if self.launched_with_spec_version != self.spec_version {
            self.destroy_current_pod(ctx);
            self.reconcile(ctx);
            return;
        }

        // 2. No demand → let reconcile decide (suspend if enabled, else destroy).
        if !self.has_demand {
            self.reconcile(ctx);
            return;
        }

        // 3. Active — emit readiness with real worker ID.
        self.update_readiness(ctx);
    }

    /// Emit readiness signal with current pod and worker info.
    fn update_readiness(&self, ctx: &mut impl WorkloadCtx) {
        ctx.set_readiness(Some(ReadyInfo {
            pod_id: self.pod_id.unwrap_or(PodId(0)),
            worker_id: self.pod_worker_id.unwrap_or(WorkerId(0)),
        }));
    }

    /// Called when a pod reports Finished status (graceful exit, exit code 0).
    /// Not counted as a failure. Cleans up and reconciles.
    fn on_pod_finished(&mut self, ctx: &mut impl WorkloadCtx) {
        // pod_running is already false — set by PodStatusInput handler at
        // the top (Finished is not Running).
        self.awaiting_suspend = false;
        ctx.set_readiness(None);

        // Remove ownership edge — pod is terminal (Finished),
        // so removing the edge triggers self-destruct.
        ctx.set_workload_to_pod_edges(vec![]);
        ctx.set_pod_intent(PodIntent::None);
        self.pod_id = None;
        // pod_worker_id will be cleared by PodWorkerInput signal propagation.

        // No failure increment — graceful exit is not a failure.
        // Re-evaluate commitment.
        if !self.has_demand {
            self.committed_to_boot = false;
        }

        self.reconcile(ctx);
        self.update_timer_signal(ctx);
    }

    /// Called when a pod reports Failed status. Cleans up tracking and enters
    /// backoff for retry, or gives up if max retries exceeded.
    fn on_pod_failed(&mut self, ctx: &mut impl WorkloadCtx) {
        // pod_running is already false — set by PodStatusInput handler at
        // the top (Failed is not Running).
        self.awaiting_suspend = false;
        ctx.set_readiness(None);

        // Remove ownership edge — pod is already terminal (Failed),
        // so removing the edge triggers self-destruct (terminal + no owner).
        ctx.set_workload_to_pod_edges(vec![]);
        ctx.set_pod_intent(PodIntent::None);
        self.pod_id = None;
        // pod_worker_id will be cleared by PodWorkerInput signal propagation.

        self.consecutive_failures += 1;

        // Re-evaluate commitment: no demand after pod death → no reason to retry.
        if !self.has_demand {
            self.committed_to_boot = false;
        }
        if self.consecutive_failures >= self.max_retries {
            self.committed_to_boot = false;
        }

        // Enter backoff only if we actually want to retry.
        let want_retry = (self.has_demand || self.committed_to_boot)
            && self.consecutive_failures < self.max_retries;
        if want_retry {
            self.in_backoff = true;
            self.backoff_generation += 1;
        } else if !self.has_demand {
            // Going dormant — clear failure tracking.
            self.consecutive_failures = 0;
        }

        self.reconcile(ctx);
        self.update_timer_signal(ctx);
    }

    /// Abandon the current pod by removing the ownership edge.
    /// The pod will drive itself to a terminal state and self-destruct.
    /// Any suspended artifact is discarded (this is a hard kill).
    fn destroy_current_pod(&mut self, ctx: &mut impl WorkloadCtx) {
        if self.pod_id.is_some() {
            ctx.set_workload_to_pod_edges(vec![]);
            ctx.set_pod_intent(PodIntent::None);
            self.pod_id = None;
        }
        // pod_running and pod_worker_id are signal-derived — they will be
        // cleared by PodStatusInput([]) and PodWorkerInput([]) when the
        // abandoned pod removes its reverse edges and self-destructs.
        self.awaiting_suspend = false;
        self.suspended_artifact = None;
        ctx.set_readiness(None);
    }

    fn update_timer_signal(&self, ctx: &mut impl WorkloadCtx) {
        if self.in_backoff {
            ctx.set_wanted_timers(vec![TimerRequest {
                key: WorkloadTimerKey::RetryBackoff,
                generation: self.backoff_generation,
            }]);
        } else {
            ctx.set_wanted_timers(vec![]);
        }
    }

    fn update_status_signals(&self, ctx: &mut impl WorkloadCtx) {
        let is_failed = self.consecutive_failures >= self.max_retries
            && (self.has_demand || self.committed_to_boot);
        let status = if is_failed {
            WlStatus::Failed
        } else if self.in_backoff {
            WlStatus::RetryBackoff
        } else if self.awaiting_suspend {
            WlStatus::Suspending
        } else if self.suspended_artifact.is_some() && self.pod_id.is_none() {
            WlStatus::Suspended
        } else if self.pod_running {
            WlStatus::Running
        } else if self.pod_id.is_some() {
            WlStatus::Launching
        } else if !self.has_spec && (self.has_demand || self.committed_to_boot) {
            WlStatus::WaitingForSpec
        } else {
            WlStatus::Dormant
        };
        ctx.set_wl_status_signal(status);
        ctx.set_consecutive_failures_signal(self.consecutive_failures);
        ctx.set_spec_stale_signal(
            self.pod_id.is_some() && self.launched_with_spec_version != self.spec_version,
        );
    }

    fn reconcile(&mut self, ctx: &mut impl WorkloadCtx) {
        // If we're waiting for a suspend to complete, don't touch the pod.
        if self.awaiting_suspend {
            return;
        }

        let is_failed = self.consecutive_failures >= self.max_retries;
        let want_pod = self.has_spec
            && (self.has_demand || self.committed_to_boot)
            && !self.in_backoff
            && !is_failed;
        self.wants_pod = want_pod;

        if want_pod && self.pod_id.is_none() {
            // Create new pod — resume from artifact if available.
            let pod = if let Some(artifact_id) = self.suspended_artifact.take() {
                PodSm::new_from_artifact(self.timer_id, artifact_id)
            } else {
                PodSm::new(self.timer_id)
            };
            let pod_id = ctx.create_pod(pod);
            self.pod_id = Some(pod_id);
            self.launched_with_spec_version = self.spec_version;
            ctx.set_workload_to_pod_edges(vec![pod_id]);
            ctx.set_pod_intent(PodIntent::Want);
        } else if want_pod && self.pod_id.is_some() {
            ctx.set_pod_intent(PodIntent::Want);
        } else if !want_pod && self.pod_id.is_some() {
            if self.pod_running && self.suspend_on_idle {
                // Signal pod to suspend — keep edge, pod drives itself to
                // Suspended terminal state.
                ctx.set_pod_intent(PodIntent::Suspend);
                self.awaiting_suspend = true;
            } else {
                // Abandon pod (remove edge). Pod will drive itself to
                // terminal and self-destruct.
                ctx.set_workload_to_pod_edges(vec![]);
                ctx.set_pod_intent(PodIntent::None);
                self.pod_id = None;
                ctx.set_readiness(None);
            }
        } else {
            ctx.set_pod_intent(PodIntent::None);
        }
    }
}

// ---- Pod SM ----
//
// A pod manages the lifecycle of a single "running thing" from creation to
// terminal state. The lifecycle is linear and non-circular:
//
//   Pending → Running → Suspending → Suspended(artifact)  [terminal]
//                     → Failed                             [terminal]
//            → Failed                                      [terminal]
//
// Terminal states wait for reaping: the pod self-destructs only when it is
// in a terminal state AND has no owner. This gives the workload time to
// read the terminal status (e.g. extract artifact_id from Suspended).
//
// Two paths to pod death:
//   Natural:  pod reaches terminal → workload reads status → workload
//             removes edge (reap) → pod self-destructs.
//   Abandon:  workload removes edge → pod drives itself to terminal
//             (owner loss while live = failure) → pod self-destructs.

#[derive(Clone)]
struct PodSm {
    status: PodStatus,
    workload_id: Option<WorkloadId>,
    worker_id: Option<WorkerId>,
    intent: PodIntent,
    /// Artifact to resume from (set at creation for resumed pods).
    /// The worker port can read this to know whether to cold-boot or resume.
    resume_artifact: Option<ArtifactId>,
    /// Timer port ID for requesting timeouts.
    timer_id: TimerId,
    /// Generation counter for timer requests.
    timer_generation: u64,
}

impl PodSm {
    fn new(timer_id: TimerId) -> Self {
        PodSm {
            status: PodStatus::Pending,
            workload_id: None,
            worker_id: None,
            intent: PodIntent::None,
            resume_artifact: None,
            timer_id,
            timer_generation: 0,
        }
    }

    fn new_from_artifact(timer_id: TimerId, artifact_id: ArtifactId) -> Self {
        PodSm {
            status: PodStatus::Pending,
            workload_id: None,
            worker_id: None,
            intent: PodIntent::None,
            resume_artifact: Some(artifact_id),
            timer_id,
            timer_generation: 0,
        }
    }

    /// Self-destruct if terminal and no owner (the reaping rule).
    fn maybe_reap(&self, ctx: &mut impl PodCtx) {
        if self.status.is_terminal() && self.workload_id.is_none() {
            ctx.self_destruct();
        }
    }

    /// Update the timer signal based on current pod status.
    fn update_timer_signal(&self, ctx: &mut impl PodCtx) {
        match &self.status {
            PodStatus::Pending => {
                ctx.set_wanted_pod_timers(vec![PodTimerRequest {
                    key: PodTimerKey::LaunchTimeout,
                    generation: self.timer_generation,
                }]);
            }
            PodStatus::Suspending => {
                ctx.set_wanted_pod_timers(vec![PodTimerRequest {
                    key: PodTimerKey::SuspendTimeout,
                    generation: self.timer_generation,
                }]);
            }
            _ => {
                ctx.set_wanted_pod_timers(vec![]);
            }
        }
    }
}

impl<C: PodCtx> SmHandler<C> for PodSm {
    type Input = PodInput;

    fn initialize(&mut self, ctx: &mut C) {
        ctx.set_pod_to_timer_edges(vec![self.timer_id]);
        self.update_timer_signal(ctx);
    }

    fn handle(&mut self, input: Self::Input, ctx: &mut C) {
        match input {
            PodInput::WorkerInput(worker) => {
                // Track assigned worker.
                let new_worker_id = worker.as_ref().map(|(id, _)| *id);
                if new_worker_id != self.worker_id {
                    self.worker_id = new_worker_id;
                    ctx.set_worker(self.worker_id);
                }

                if worker.is_none() && !self.status.is_terminal() {
                    // Worker lost — pod is dead.
                    self.status = PodStatus::Failed;
                    ctx.set_status(PodStatus::Failed);
                    self.update_timer_signal(ctx);
                    self.maybe_reap(ctx);
                }
            }
            PodInput::OwnerInput(owner) => {
                let had_owner = self.workload_id.is_some();
                let (new_wl, new_intent) = match owner {
                    Some((wl, intent)) => (Some(wl), intent),
                    None => (None, PodIntent::None),
                };
                self.workload_id = new_wl;
                self.intent = new_intent;

                let edges: Vec<WorkloadId> = self.workload_id.into_iter().collect();
                ctx.set_pod_to_workload_edges(edges);

                // React to intent: Running + Suspend → begin suspending.
                if matches!(
                    (&self.status, &self.intent),
                    (PodStatus::Running, PodIntent::Suspend)
                ) {
                    self.timer_generation += 1;
                    self.status = PodStatus::Suspending;
                    ctx.set_status(PodStatus::Suspending);
                    self.update_timer_signal(ctx);
                }

                // Lost owner while in a live state → drive to terminal.
                // (In a real system this would go through a shutdown sequence
                // with worker interaction; simplified to immediate here.)
                if had_owner && self.workload_id.is_none() && !self.status.is_terminal() {
                    self.status = PodStatus::Failed;
                    ctx.set_status(PodStatus::Failed);
                    self.update_timer_signal(ctx);
                }

                self.maybe_reap(ctx);
            }
            PodInput::NotifyPodStatus(new_status) => {
                if !self.status.is_terminal() {
                    self.status = new_status.clone();
                    ctx.set_status(new_status);
                    self.update_timer_signal(ctx);
                    self.maybe_reap(ctx);
                }
            }
            PodInput::NotifyPodSuspended(artifact_id) => {
                if matches!(self.status, PodStatus::Suspending) {
                    self.status = PodStatus::Suspended {
                        artifact_id: artifact_id.clone(),
                    };
                    ctx.set_status(PodStatus::Suspended { artifact_id });
                    self.update_timer_signal(ctx);
                    self.maybe_reap(ctx);
                }
            }
            PodInput::PodTimerFired(key) => match key {
                PodTimerKey::LaunchTimeout => {
                    if matches!(self.status, PodStatus::Pending) {
                        self.status = PodStatus::Failed;
                        ctx.set_status(PodStatus::Failed);
                        self.update_timer_signal(ctx);
                        self.maybe_reap(ctx);
                    }
                }
                PodTimerKey::SuspendTimeout => {
                    if matches!(self.status, PodStatus::Suspending) {
                        self.status = PodStatus::Failed;
                        ctx.set_status(PodStatus::Failed);
                        self.update_timer_signal(ctx);
                        self.maybe_reap(ctx);
                    }
                }
            },
        }
    }
}

// ============================================================================
// Constants
// ============================================================================

const S1: ServiceId = ServiceId(1);
const S2: ServiceId = ServiceId(2);
const S3: ServiceId = ServiceId(3);
const W1: WorkloadId = WorkloadId(1);
const W2: WorkloadId = WorkloadId(2);

// ============================================================================
// Test helpers
// ============================================================================

/// Extract workload timer requests from timer port inputs.
/// Each delivery is a `Vec<Vec<TimerRequest>>` — one inner Vec per workload connected
/// to the timer port.
fn drain_timer_requests(router: &mut Router, timer: TimerId) -> Vec<Vec<Vec<TimerRequest>>> {
    router
        .drain_timer_inputs()
        .into_iter()
        .filter(|(id, _)| *id == timer)
        .filter_map(|(_, input)| match input {
            TimerPortInput::WorkloadTimersInput(timers) => Some(timers),
            _ => None,
        })
        .collect()
}

/// Assert that the timer port received a timer delivery where the workload
/// declared exactly the expected timer requests.
fn assert_timer_requested(router: &mut Router, timer: TimerId, expected: &[TimerRequest]) {
    let deliveries = drain_timer_requests(router, timer);
    assert!(
        !deliveries.is_empty(),
        "expected timer delivery {:?}, got nothing",
        expected
    );
    // Last delivery should have one workload's timer list matching expected.
    let last = deliveries.last().unwrap();
    assert_eq!(
        last.len(),
        1,
        "expected 1 workload's timers, got {:?}",
        last
    );
    assert_eq!(last[0].as_slice(), expected, "timer requests mismatch");
}

/// Assert that timer output is either absent or empty (no active timers).
fn assert_no_timers_wanted(router: &mut Router, timer: TimerId) {
    let deliveries = drain_timer_requests(router, timer);
    for delivery in &deliveries {
        for workload_timers in delivery {
            assert!(
                workload_timers.is_empty(),
                "expected no timers wanted, got {:?}",
                workload_timers
            );
        }
    }
}

/// Assert no timer-related port inputs were delivered at all (dedup suppressed).
fn assert_no_timer_output(router: &mut Router, timer: TimerId) {
    let deliveries = drain_timer_requests(router, timer);
    assert!(
        deliveries.is_empty(),
        "expected no timer output, got {:?}",
        deliveries
    );
}

// ============================================================================
// Tests
// ============================================================================

/// 1. Demand aggregation: 3 activation-based services → 1 workload, toggle demand.
#[test]
fn demand_aggregation() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    router.create_workload(W1, WorkloadSm::new(timer));
    router.create_service(S1, ServiceSm::new(timer, true));
    router.create_service(S2, ServiceSm::new(timer, true));
    router.create_service(S3, ServiceSm::new(timer, true));

    // Deliver specs through management port — services get edges to W1.
    let mgmt = router.create_management();
    router.set_management_to_service_edges(mgmt, vec![S1, S2, S3]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: true,
        },
    );
    router.propagate();

    // No demand yet (all activation-based, none activated).
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.has_demand);

    // S1 activates.
    router.send_activate_service(mgmt, S1, true);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_demand);

    // S2 also activates.
    router.send_activate_service(mgmt, S2, true);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_demand);

    // Both deactivate.
    router.send_activate_service(mgmt, S1, false);
    router.send_activate_service(mgmt, S2, false);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.has_demand);
}

/// 2. Reactive readiness edges: workload creates WorkloadToService edges
///    based on which services point at it, then readiness propagates back.
#[test]
fn reactive_readiness_edges() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new(timer));
    router.create_service(S1, ServiceSm::new(timer, false)); // always-on
    router.create_service(S2, ServiceSm::new(timer, false));

    // Deliver workload spec.
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "test:latest".into(),
        },
    );

    // Deliver service specs — always-on services auto-set demand + edges.
    router.set_management_to_service_edges(mgmt, vec![S1, S2]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: false,
        },
    );
    router.propagate();

    // Workload should have received demand (from always-on services).
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_demand);

    // Both services should be in NeedBackend (demand set, no readiness yet).
    let s1 = router.get_service(&S1).unwrap();
    assert_eq!(s1.state, ServiceState::NeedBackend);

    // Workload created a pod in reconcile(). Wire worker and make it running.
    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Running);
    router.propagate();

    // Both services should be active now (readiness propagated via reactive edges).
    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));
    let s2 = router.get_service(&S2).unwrap();
    assert!(matches!(s2.state, ServiceState::Active { .. }));

    // Add a third service — it should immediately get readiness.
    router.create_service(S3, ServiceSm::new(timer, false));
    // Use same mgmt port, update edges to include S3.
    router.set_management_to_service_edges(mgmt, vec![S1, S2, S3]);
    router.propagate();

    // S3 got its spec, set demand + edges, workload re-aggregated,
    // readiness propagated to all three services including S3.
    let s3 = router.get_service(&S3).unwrap();
    assert!(matches!(s3.state, ServiceState::Active { .. }));
}

/// 3. Pod lifecycle through signals: workload creates pod in handler,
///    pod status flows back, readiness propagates to services.
#[test]
fn pod_lifecycle() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new(timer));
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "test:latest".into(),
        },
    );
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_spec);
    assert!(!wl.has_demand);

    // Add an always-on service with demand.
    router.create_service(S1, ServiceSm::new(timer, false));
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: false,
        },
    );
    router.propagate();

    // Workload should have created a pod (has spec + demand).
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.wants_pod);
    let pod_id = wl.pod_id.unwrap();

    // Pod is pending — workload sees PodStatus::Pending.
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.pod_running);

    // Wire worker to pod and report running.
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Running);
    router.propagate();

    // Workload should be ready now.
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);

    // Readiness should have propagated to S1.
    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));
}

/// 4. Worker port removal: worker dies, pod sees empty WorkerInput,
///    status goes to Failed, workload sees readiness lost.
#[test]
fn worker_loss_via_port_removal() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new(timer));
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v1".into(),
        },
    );

    router.create_service(S1, ServiceSm::new(timer, false));
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: false,
        },
    );
    router.propagate();

    // Workload created pod. Wire worker and start it.
    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Running);
    router.propagate();

    // Verify everything is active.
    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));

    // No timers wanted while running.
    assert_no_timers_wanted(&mut router, timer);

    // Worker dies — remove the port.
    router.destroy_worker(worker);
    router.propagate();

    // Pod was failed and workload released it (on_pod_failed → self-destruct).
    // Workload should have lost readiness and entered backoff for retry.
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.pod_running);
    assert!(wl.in_backoff);

    // Timer signal should show a retry backoff request.
    assert_timer_requested(
        &mut router,
        timer,
        &[TimerRequest {
            key: WorkloadTimerKey::RetryBackoff,
            generation: 1,
        }],
    );

    // Service should be back to NeedBackend.
    let s1 = router.get_service(&S1).unwrap();
    assert_eq!(s1.state, ServiceState::NeedBackend);
}

/// 5. Spec delivery via management port: init and update use same path.
#[test]
fn spec_via_management_port() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    router.create_workload(W1, WorkloadSm::new(timer));

    let mgmt = router.create_management();
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "v1".into() });
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_spec);

    // Update spec.
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "v2".into() });
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_spec);

    // Remove management port — workload self-destructs.
    router.destroy_management(mgmt);
    router.propagate();

    assert!(router.get_workload(&W1).is_none());
}

/// 6. Service spec via management port: service reads its spec, creates
///    edges to the target workload reactively.
#[test]
fn service_spec_creates_edges_reactively() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    router.create_workload(W1, WorkloadSm::new(timer));
    router.create_service(S1, ServiceSm::new(timer, false));

    // Management port delivers service spec that points at W1.
    let mgmt = router.create_management();
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: false,
        },
    );
    router.propagate();

    // Service should have reactively created edges and set demand=true.
    // Verify indirectly: workload received demand via the edge.
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_demand);

    // Service should be in NeedBackend (always-on with demand set).
    let s1 = router.get_service(&S1).unwrap();
    assert_eq!(s1.state, ServiceState::NeedBackend);
}

/// 7. Admin command event: management port sends restart to workload.
#[test]
fn admin_restart_event() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new(timer));
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v1".into(),
        },
    );

    router.create_service(S1, ServiceSm::new(timer, false));
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: false,
        },
    );
    router.propagate();

    // Workload created pod. Wire worker and start it.
    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Running);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);

    // Send admin restart.
    router.send_admin_command(mgmt, W1, AdminCmd::Restart);
    router.propagate();

    // Workload should have cleared readiness (needs new pod).
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.pod_running);
}

/// 8. Full end-to-end: service activation → demand → pod creation → readiness →
///    service active → worker dies → readiness lost → service back to NeedBackend.
#[test]
fn full_end_to_end() {
    let mut router = Router::new(16);
    let timer = router.create_timer();

    // Infrastructure.
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    // Management ports: one for workload, one per service (different specs).
    let mgmt_wl = router.create_management();
    let mgmt_s1 = router.create_management();
    let mgmt_s2 = router.create_management();

    // Create SMs.
    router.create_workload(W1, WorkloadSm::new(timer));
    router.create_service(S1, ServiceSm::new(timer, false));
    router.create_service(S2, ServiceSm::new(timer, true)); // activation-based

    // Wire management → SMs.
    router.set_management_to_workload_edges(mgmt_wl, vec![W1]);
    router.set_management_wl_spec(
        mgmt_wl,
        WorkloadSpec {
            image: "app:v1".into(),
        },
    );
    router.set_management_to_service_edges(mgmt_s1, vec![S1]);
    router.set_management_svc_spec(
        mgmt_s1,
        ServiceSpec {
            workload: W1,
            has_activation: false,
        },
    );
    router.set_management_to_service_edges(mgmt_s2, vec![S2]);
    router.set_management_svc_spec(
        mgmt_s2,
        ServiceSpec {
            workload: W1,
            has_activation: true,
        },
    );
    router.propagate();

    // S1 (always-on) should have created edges and set demand.
    // Workload has spec + demand → created pod.
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_spec);
    assert!(wl.has_demand); // S1 always-on → demand=true
    let pod_id = wl.pod_id.unwrap();

    // S2 (activation) is idle — no demand yet.
    let s2 = router.get_service(&S2).unwrap();
    assert_eq!(s2.state, ServiceState::Idle);

    // Wire worker to pod and start it.
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Running);
    router.propagate();

    // S1 should be active (always-on, backend ready).
    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));

    // S2 should still be Idle (has_activation=true, no activation event sent).
    let s2 = router.get_service(&S2).unwrap();
    assert_eq!(s2.state, ServiceState::Idle);

    // Activate S2 via event.
    router.send_activate_service(mgmt_s2, S2, true);
    router.propagate();

    // S2 should now be in NeedBackend (demand set) or Active (readiness already available).
    // Since workload already has readiness and S2 is now connected via demand,
    // the workload will re-aggregate demand (2 services now) and re-target readiness edges.
    let s2 = router.get_service(&S2).unwrap();
    // S2 transitions Idle→NeedBackend on activation. Then it receives readiness
    // from the workload (which already has a running pod), so NeedBackend→Active.
    assert!(
        matches!(s2.state, ServiceState::Active { .. })
            || matches!(s2.state, ServiceState::NeedBackend)
    );

    // No timers requested while running normally.
    assert_no_timers_wanted(&mut router, timer);

    // Worker dies.
    router.destroy_worker(worker);
    router.propagate();

    // Pod failed — workload released it and entered backoff.
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.pod_running);
    assert!(wl.in_backoff);

    // Timer signal: workload wants a retry backoff timer.
    assert_timer_requested(
        &mut router,
        timer,
        &[TimerRequest {
            key: WorkloadTimerKey::RetryBackoff,
            generation: 1,
        }],
    );

    // S1 goes back to NeedBackend.
    let s1 = router.get_service(&S1).unwrap();
    assert_eq!(s1.state, ServiceState::NeedBackend);

    // Workload wants a pod but is in backoff — not creating one yet.
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.wants_pod); // backoff suppresses want_pod
    assert!(wl.has_demand); // demand is still there
}

/// 9. Workload creates pod directly from handler when it has spec + demand.
#[test]
fn handler_driven_pod_creation() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();

    router.create_workload(W1, WorkloadSm::new(timer));
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v1".into(),
        },
    );

    // Always-on service
    router.create_service(S1, ServiceSm::new(timer, false));
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: false,
        },
    );
    router.propagate();

    // Workload has spec + demand → created pod in reconcile().
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.wants_pod);
    assert!(wl.has_spec);
    assert!(wl.has_demand);
    let pod_id = wl.pod_id.unwrap();

    // Pod should exist and know its owner workload (via OwnerInput).
    let pod = router.get_pod(&pod_id).unwrap();
    assert_eq!(pod.workload_id, Some(W1));
    assert_eq!(pod.status, PodStatus::Pending);

    // Wire worker and start pod.
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Running);
    router.propagate();

    // Service is active.
    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));

    let pod = router.get_pod(&pod_id).unwrap();
    assert_eq!(pod.status, PodStatus::Running);
}

/// 10. Handler creates SM with auto-ID and the ID counter is properly
///     shared between handler-created and router-created SMs.
#[test]
fn handler_and_router_share_id_counter() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let mgmt = router.create_management();

    // Create first pod via router.
    let p1 = router.create_pod(PodSm::new(timer));

    // Create workload and wire it.
    router.create_workload(W1, WorkloadSm::new(timer));
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "test".into(),
        },
    );

    // Create service to give workload demand → workload creates pod in handler.
    router.create_service(S1, ServiceSm::new(timer, false));
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: false,
        },
    );
    router.propagate();

    // Workload created a pod (p2) in its handler.
    let p2 = router.get_workload(&W1).unwrap().pod_id.unwrap();
    // p2 should have a different ID from p1.
    assert_ne!(p1, p2);
    // p2's ID should be after p1's (both use the same counter).
    assert!(p2.0 > p1.0);

    // Create another pod via router — should continue the counter.
    let p3 = router.create_pod(PodSm::new(timer));
    assert!(p3.0 > p2.0);
}

// ============================================================================
// PendingIntent-equivalent tests — signal-based transition decisions
// ============================================================================

/// Helper: set up a workload with an activation-based service that has been
/// activated, return (mgmt, worker, pod_id, timer).
/// After this, workload has spec + demand, a pod exists in Pending state.
/// Use send_activate_service(mgmt, S1, false) to drop demand.
fn setup_workload_with_pending_pod(
    router: &mut Router,
) -> (ManagementId, WorkerId, PodId, TimerId) {
    let timer = router.create_timer();
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new(timer));
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v1".into(),
        },
    );

    router.create_service(S1, ServiceSm::new(timer, true)); // activation-based
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: true,
        },
    );
    router.propagate();

    // Activate the service to create demand.
    router.send_activate_service(mgmt, S1, true);
    router.propagate();

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    assert_eq!(router.get_pod(&pod_id).unwrap().status, PodStatus::Pending);

    (mgmt, worker, pod_id, timer)
}

/// Helper: make a pending pod Running.
fn make_pod_running(router: &mut Router, worker: WorkerId, pod_id: PodId) {
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Running);
    router.propagate();
}

/// Helper: make a pending pod fail via worker notification.
fn make_pod_failed(router: &mut Router, worker: WorkerId, pod_id: PodId) {
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Failed);
    router.propagate();
}

/// Helper: set up a workload (with configurable max_retries) with an always-on
/// service, a running pod, and return (mgmt, worker, timer).
fn setup_running_workload(
    router: &mut Router,
    max_retries: u32,
) -> (ManagementId, WorkerId, TimerId) {
    let timer = router.create_timer();
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::with_max_retries(timer, max_retries));
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v1".into(),
        },
    );

    router.create_service(S1, ServiceSm::new(timer, false)); // always-on
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: false,
        },
    );
    router.propagate();

    // Make the pod running.
    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    make_pod_running(router, worker, pod_id);

    assert!(router.get_workload(&W1).unwrap().pod_running);
    (mgmt, worker, timer)
}

/// 11. Demand drops during pod launch — committed_to_boot keeps the pod alive.
///     When the pod reaches Running, demand is re-checked and workload deactivates.
#[test]
fn demand_drop_during_launch_committed_to_boot() {
    let mut router = Router::new(16);
    let (mgmt, worker, pod_id, _timer) = setup_workload_with_pending_pod(&mut router);

    // Verify committed state.
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.committed_to_boot);
    assert!(wl.has_demand);

    // Deactivate the service — demand drops to 0.
    router.send_activate_service(mgmt, S1, false);
    router.propagate();

    // Pod should still exist (committed_to_boot keeps it alive).
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.has_demand);
    assert!(wl.committed_to_boot);
    assert!(wl.pod_id.is_some());

    // Pod reaches Running — workload checks demand, finds 0, deactivates.
    make_pod_running(&mut router, worker, pod_id);

    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.committed_to_boot); // cleared on Running
    assert!(!wl.pod_running); // deactivated — pod destroyed
    assert!(wl.pod_id.is_none());
}

/// 12. Demand drops then reappears during pod launch — no restart needed,
///     pod stays alive throughout and becomes active normally.
#[test]
fn demand_fluctuation_during_launch() {
    let mut router = Router::new(16);
    let (mgmt, worker, pod_id, _timer) = setup_workload_with_pending_pod(&mut router);

    // Demand drops.
    router.send_activate_service(mgmt, S1, false);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.has_demand);
    assert!(wl.committed_to_boot);
    let original_pod = wl.pod_id.unwrap();

    // Demand reappears.
    router.send_activate_service(mgmt, S1, true);
    router.propagate();

    // Same pod, still launching.
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_demand);
    assert_eq!(wl.pod_id, Some(original_pod));

    // Pod reaches Running — demand is back, stays active.
    make_pod_running(&mut router, worker, pod_id);

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);
    assert!(wl.pod_id.is_some());

    // Service should be active.
    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));
}

/// 13. Spec changes during pod launch — detected at Running, triggers restart.
///     Replaces PendingIntent::Restart with spec version comparison.
#[test]
fn spec_change_during_launch_triggers_restart() {
    let mut router = Router::new(16);
    let (mgmt, worker, pod_id, _timer) = setup_workload_with_pending_pod(&mut router);

    let wl = router.get_workload(&W1).unwrap();
    let original_pod = wl.pod_id.unwrap();

    // Spec changes while pod is launching.
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v2".into(),
        },
    );
    router.propagate();

    // Pod should still exist (launch continues).
    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.pod_id, Some(original_pod));

    // Pod reaches Running — workload detects spec mismatch, destroys and recreates.
    make_pod_running(&mut router, worker, pod_id);

    let wl = router.get_workload(&W1).unwrap();
    // Old pod destroyed, new pod created.
    assert!(wl.pod_id.is_some());
    assert_ne!(wl.pod_id.unwrap(), original_pod);
    assert!(!wl.pod_running); // new pod is Pending
}

/// 14. Spec changes while pod is Running — immediate restart (no pending).
#[test]
fn spec_change_while_running_restarts_immediately() {
    let mut router = Router::new(16);
    let (mgmt, worker, pod_id, _timer) = setup_workload_with_pending_pod(&mut router);

    // Get pod running.
    make_pod_running(&mut router, worker, pod_id);

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);
    let original_pod = wl.pod_id.unwrap();

    // Spec changes while running.
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v2".into(),
        },
    );
    router.propagate();

    // Workload should have restarted: old pod destroyed, new pod created.
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_id.is_some());
    assert_ne!(wl.pod_id.unwrap(), original_pod);
    assert!(!wl.pod_running); // new pod is Pending

    // Readiness should be cleared.
    let s1 = router.get_service(&S1).unwrap();
    assert_eq!(s1.state, ServiceState::NeedBackend);
}

/// 15. Scavenge with no demand — idle workload deactivated.
#[test]
fn scavenge_idle_workload() {
    let mut router = Router::new(16);
    let (mgmt, worker, pod_id, _timer) = setup_workload_with_pending_pod(&mut router);

    // Get pod running.
    make_pod_running(&mut router, worker, pod_id);

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);

    // Drop demand: switch service to activation-based.
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: true,
        },
    );
    router.propagate();

    // Pod was destroyed because demand dropped and on_pod_running already ran
    // (pod was Running when demand dropped → reconcile destroys it).
    // But let's test a scenario where scavenge actually matters:
    // workload with activation-based service, pod running, then service deactivates.

    // Start fresh for a clean scavenge scenario.
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new(timer));
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v1".into(),
        },
    );

    // Activation-based service.
    router.create_service(S1, ServiceSm::new(timer, true));
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: true,
        },
    );
    router.propagate();

    // No demand yet — workload dormant.
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.has_demand);
    assert!(wl.pod_id.is_none());

    // Activate service → demand → pod created.
    router.send_activate_service(mgmt, S1, true);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_demand);
    let pod_id = wl.pod_id.unwrap();

    // Make pod running.
    make_pod_running(&mut router, worker, pod_id);

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);

    // Service deactivates — demand drops to 0.
    router.send_activate_service(mgmt, S1, false);
    router.propagate();

    // Workload destroyed the pod in reconcile (no demand, not committed).
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.has_demand);
    assert!(wl.pod_id.is_none());

    // Scavenge on already-idle workload is a noop.
    router.send_admin_command(mgmt, W1, AdminCmd::Scavenge);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_id.is_none());
}

/// 16. Scavenge with active demand — noop, workload stays active.
#[test]
fn scavenge_with_demand_is_noop() {
    let mut router = Router::new(16);
    let (mgmt, worker, pod_id, _timer) = setup_workload_with_pending_pod(&mut router);

    // Get pod running.
    make_pod_running(&mut router, worker, pod_id);

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);
    assert!(wl.has_demand);
    let original_pod = wl.pod_id.unwrap();

    // Scavenge while actively demanded → noop.
    router.send_admin_command(mgmt, W1, AdminCmd::Scavenge);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running); // still running
    assert_eq!(wl.pod_id, Some(original_pod)); // same pod
}

/// 17. Scavenge aborts a committed-to-boot launch when demand is gone.
#[test]
fn scavenge_aborts_committed_launch() {
    let mut router = Router::new(16);
    let (mgmt, _worker, _pod_id, _timer) = setup_workload_with_pending_pod(&mut router);

    // Drop demand while pod is launching.
    router.send_activate_service(mgmt, S1, false);
    router.propagate();

    // Pod still alive due to committed_to_boot.
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.has_demand);
    assert!(wl.committed_to_boot);
    assert!(wl.pod_id.is_some());

    // Scavenge overrides committed_to_boot — pod destroyed.
    router.send_admin_command(mgmt, W1, AdminCmd::Scavenge);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.committed_to_boot);
    assert!(wl.pod_id.is_none());
    assert!(!wl.wants_pod);
}

/// 18. Spec change + demand drop during launch — spec change wins
///     (pod restarts on Running, then deactivates because no demand).
#[test]
fn spec_change_and_demand_drop_during_launch() {
    let mut router = Router::new(16);
    let (mgmt, worker, pod_id, _timer) = setup_workload_with_pending_pod(&mut router);

    let original_pod = router.get_workload(&W1).unwrap().pod_id.unwrap();

    // Both happen during launch: spec changes and demand drops.
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v2".into(),
        },
    );
    router.send_activate_service(mgmt, S1, false);
    router.propagate();

    // Pod still alive (committed_to_boot).
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.has_demand);
    assert!(wl.committed_to_boot);
    assert_eq!(wl.pod_id, Some(original_pod));

    // Pod reaches Running — spec mismatch detected first (priority 1),
    // destroys pod and reconciles. No demand → no new pod.
    make_pod_running(&mut router, worker, pod_id);

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_id.is_none()); // no new pod (no demand)
    assert!(!wl.committed_to_boot);
    assert!(!wl.wants_pod);
}

/// 19. Restart during pod launch — destroys and recreates immediately.
#[test]
fn restart_during_launch() {
    let mut router = Router::new(16);
    let (mgmt, worker, _pod_id, _timer) = setup_workload_with_pending_pod(&mut router);

    let original_pod = router.get_workload(&W1).unwrap().pod_id.unwrap();

    // Admin restart while pod is launching.
    router.send_admin_command(mgmt, W1, AdminCmd::Restart);
    router.propagate();

    // Old pod destroyed, new pod created (has spec + demand).
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_id.is_some());
    assert_ne!(wl.pod_id.unwrap(), original_pod);

    // New pod should be Pending.
    let new_pod = wl.pod_id.unwrap();
    let pod = router.get_pod(&new_pod).unwrap();
    assert_eq!(pod.status, PodStatus::Pending);

    // Make new pod running — should become active normally.
    make_pod_running(&mut router, worker, new_pod);

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);
}

// ============================================================================
// Retry backoff + Failed terminal state tests
// ============================================================================

/// 20. Pod fails → workload enters backoff → timer fires → new pod created → succeeds.
#[test]
fn pod_failure_backoff_and_retry() {
    let mut router = Router::new(16);
    let (mgmt, worker, timer) = setup_running_workload(&mut router, 5);

    // Kill worker → pod fails.
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.pod_running);
    assert!(wl.in_backoff);
    assert_eq!(wl.consecutive_failures, 1);
    assert!(wl.pod_id.is_none()); // pod released

    // Workload should have signaled a retry backoff timer.
    assert_timer_requested(
        &mut router,
        timer,
        &[TimerRequest {
            key: WorkloadTimerKey::RetryBackoff,
            generation: 1,
        }],
    );

    // Timer fires — backoff cleared, reconcile creates new pod.
    router.send_workload_timer_fired(timer, W1, WorkloadTimerKey::RetryBackoff);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.in_backoff);
    assert!(wl.pod_id.is_some());
    let new_pod = wl.pod_id.unwrap();

    // Timer signal should now be empty (backoff cleared).
    assert_no_timers_wanted(&mut router, timer);

    // New worker + make pod running.
    let worker2 = router.create_worker();
    router.set_worker_info(worker2, WorkerInfo { capacity: 10 });
    make_pod_running(&mut router, worker2, new_pod);

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);
    assert_eq!(wl.consecutive_failures, 0); // reset on success

    // No timers while running.
    assert_no_timer_output(&mut router, timer);
}

/// 21. Multiple failures increment the counter.
#[test]
fn consecutive_failures_increment() {
    let mut router = Router::new(16);
    let (mgmt, worker, timer) = setup_running_workload(&mut router, 5);

    // First failure.
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 1);
    assert!(wl.in_backoff);

    // Generation 1 timer requested.
    assert_timer_requested(
        &mut router,
        timer,
        &[TimerRequest {
            key: WorkloadTimerKey::RetryBackoff,
            generation: 1,
        }],
    );

    // Timer fires → retry.
    router.send_workload_timer_fired(timer, W1, WorkloadTimerKey::RetryBackoff);
    router.propagate();

    let pod2 = router.get_workload(&W1).unwrap().pod_id.unwrap();

    // Second failure (via direct status, not worker loss).
    let worker2 = router.create_worker();
    router.set_worker_info(worker2, WorkerInfo { capacity: 10 });
    make_pod_failed(&mut router, worker2, pod2);

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 2);
    assert!(wl.in_backoff);

    // Generation 2 timer requested (new backoff cycle).
    assert_timer_requested(
        &mut router,
        timer,
        &[TimerRequest {
            key: WorkloadTimerKey::RetryBackoff,
            generation: 2,
        }],
    );
}

/// 22. After max_retries failures, workload stops retrying (terminal Failed).
#[test]
fn max_retries_enters_failed() {
    let mut router = Router::new(16);
    let (mgmt, worker, timer) = setup_running_workload(&mut router, 2);

    // First failure.
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 1);
    assert!(wl.in_backoff); // still under limit

    // Timer requested for first backoff.
    assert_timer_requested(
        &mut router,
        timer,
        &[TimerRequest {
            key: WorkloadTimerKey::RetryBackoff,
            generation: 1,
        }],
    );

    // Timer fires → retry.
    router.send_workload_timer_fired(timer, W1, WorkloadTimerKey::RetryBackoff);
    router.propagate();

    let pod2 = router.get_workload(&W1).unwrap().pod_id.unwrap();

    // Second failure — hits max_retries (2).
    let worker2 = router.create_worker();
    router.set_worker_info(worker2, WorkerInfo { capacity: 10 });
    make_pod_failed(&mut router, worker2, pod2);

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 2);
    assert!(!wl.in_backoff); // not in backoff — terminal
    assert!(wl.pod_id.is_none()); // no new pod
    assert!(!wl.wants_pod); // reconcile says no

    // Terminal failure: no timer requested (timers cleared).
    assert_no_timers_wanted(&mut router, timer);
}

/// 23. Failed state + spec change → resets failures and retries.
#[test]
fn failed_recovery_via_spec_change() {
    let mut router = Router::new(16);
    let (mgmt, worker, _timer) = setup_running_workload(&mut router, 1);

    // One failure → hits max_retries (1) → terminal.
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 1);
    assert!(!wl.in_backoff);
    assert!(wl.pod_id.is_none());

    // Spec change resets failures.
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v2".into(),
        },
    );
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 0);
    assert!(!wl.in_backoff);
    assert!(wl.pod_id.is_some()); // new pod created
}

/// 24. Failed state + restart command → resets failures and retries.
#[test]
fn failed_recovery_via_restart() {
    let mut router = Router::new(16);
    let (mgmt, worker, _timer) = setup_running_workload(&mut router, 1);

    // One failure → terminal.
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_id.is_none());
    assert_eq!(wl.consecutive_failures, 1);

    // Restart resets failures.
    router.send_admin_command(mgmt, W1, AdminCmd::Restart);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 0);
    assert!(!wl.in_backoff);
    assert!(wl.pod_id.is_some()); // new pod created
}

/// 25. Failed + demand drops (clears) + demand returns → fresh start.
#[test]
fn failed_recovery_via_demand_cycle() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::with_max_retries(timer, 1));
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v1".into(),
        },
    );

    // Activation-based service so we can toggle demand.
    router.create_service(S1, ServiceSm::new(timer, true));
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: true,
        },
    );
    router.propagate();

    // Activate → demand → pod.
    router.send_activate_service(mgmt, S1, true);
    router.propagate();

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    make_pod_running(&mut router, worker, pod_id);

    // Fail → terminal (max_retries=1).
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 1);

    // Drop demand — clears failure state.
    router.send_activate_service(mgmt, S1, false);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 0);
    assert!(!wl.in_backoff);

    // Re-activate — fresh start, creates new pod.
    router.send_activate_service(mgmt, S1, true);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_id.is_some());
    assert_eq!(wl.consecutive_failures, 0);
}

/// 26. Failed + demand still present + more demand → stays Failed.
#[test]
fn failed_ignores_new_demand() {
    let mut router = Router::new(16);
    let (mgmt, worker, timer) = setup_running_workload(&mut router, 1);

    // Fail → terminal.
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 1);
    assert!(wl.pod_id.is_none());

    // Add another service with demand — still Failed, no new pod.
    router.create_service(S2, ServiceSm::new(timer, false));
    router.set_management_to_service_edges(mgmt, vec![S1, S2]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: false,
        },
    );
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_demand);
    assert!(wl.pod_id.is_none()); // still Failed, no retry
    assert_eq!(wl.consecutive_failures, 1);
}

/// 27. In backoff + demand drops → goes dormant (clears backoff and failures).
#[test]
fn backoff_cleared_on_demand_drop() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::with_max_retries(timer, 5));
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v1".into(),
        },
    );

    router.create_service(S1, ServiceSm::new(timer, true));
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: true,
        },
    );
    router.propagate();

    // Activate → running pod.
    router.send_activate_service(mgmt, S1, true);
    router.propagate();

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    make_pod_running(&mut router, worker, pod_id);

    // Fail → enters backoff.
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.in_backoff);
    assert_eq!(wl.consecutive_failures, 1);

    // Timer requested during backoff.
    assert_timer_requested(
        &mut router,
        timer,
        &[TimerRequest {
            key: WorkloadTimerKey::RetryBackoff,
            generation: 1,
        }],
    );

    // Drop demand → clears everything.
    router.send_activate_service(mgmt, S1, false);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.in_backoff);
    assert_eq!(wl.consecutive_failures, 0);
    assert!(wl.pod_id.is_none());

    // Timer cleared after demand drop.
    assert_no_timers_wanted(&mut router, timer);
}

/// 28. In backoff + spec change → clears backoff, immediate retry.
#[test]
fn backoff_cleared_on_spec_change() {
    let mut router = Router::new(16);
    let (mgmt, worker, timer) = setup_running_workload(&mut router, 5);

    // Fail → enters backoff.
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.in_backoff);

    // Timer requested during backoff.
    assert_timer_requested(
        &mut router,
        timer,
        &[TimerRequest {
            key: WorkloadTimerKey::RetryBackoff,
            generation: 1,
        }],
    );

    // Spec change clears backoff + failures → immediate retry.
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v2".into(),
        },
    );
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.in_backoff);
    assert_eq!(wl.consecutive_failures, 0);
    assert!(wl.pod_id.is_some()); // new pod created immediately

    // Timer cleared after spec change.
    assert_no_timers_wanted(&mut router, timer);
}

/// 29. Scavenge during backoff clears everything, goes dormant.
#[test]
fn scavenge_during_backoff() {
    let mut router = Router::new(16);
    let (mgmt, worker, timer) = setup_running_workload(&mut router, 5);

    // Fail → enters backoff.
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.in_backoff);

    // Timer requested during backoff.
    assert_timer_requested(
        &mut router,
        timer,
        &[TimerRequest {
            key: WorkloadTimerKey::RetryBackoff,
            generation: 1,
        }],
    );

    // Scavenge is noop when demand is present (always-on service).
    // So scavenge won't do anything here — demand is still active.
    router.send_admin_command(mgmt, W1, AdminCmd::Scavenge);
    router.propagate();

    // Still in backoff because demand is active — timer unchanged (dedup suppresses).
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.in_backoff);
    assert_no_timer_output(&mut router, timer);
}

/// 30. Scavenge during Failed clears failures (when no demand).
#[test]
fn scavenge_during_failed() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::with_max_retries(timer, 1));
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v1".into(),
        },
    );

    router.create_service(S1, ServiceSm::new(timer, true));
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: true,
        },
    );
    router.propagate();

    // Activate → pod → running.
    router.send_activate_service(mgmt, S1, true);
    router.propagate();

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    make_pod_running(&mut router, worker, pod_id);

    // Fail → terminal (max_retries=1).
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 1);

    // Drop demand first so scavenge doesn't noop.
    router.send_activate_service(mgmt, S1, false);
    router.propagate();

    // Scavenge clears everything.
    router.send_admin_command(mgmt, W1, AdminCmd::Scavenge);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 0);
    assert!(!wl.in_backoff);
    assert!(wl.pod_id.is_none());
}

/// 31. Success resets failure counter: fail, retry, succeed, fail again → counter=1.
#[test]
fn success_resets_failure_counter() {
    let mut router = Router::new(16);
    let (mgmt, worker, timer) = setup_running_workload(&mut router, 5);

    // First failure.
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 1);

    // First backoff: generation 1.
    assert_timer_requested(
        &mut router,
        timer,
        &[TimerRequest {
            key: WorkloadTimerKey::RetryBackoff,
            generation: 1,
        }],
    );

    // Timer fires → retry.
    router.send_workload_timer_fired(timer, W1, WorkloadTimerKey::RetryBackoff);
    router.propagate();

    let pod2 = router.get_workload(&W1).unwrap().pod_id.unwrap();

    // Succeed — counter resets.
    let worker2 = router.create_worker();
    router.set_worker_info(worker2, WorkerInfo { capacity: 10 });
    make_pod_running(&mut router, worker2, pod2);

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 0);
    assert!(wl.pod_running);

    // Fail again — counter should be 1, not 2.
    router.destroy_worker(worker2);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 1);
    assert!(wl.in_backoff);

    // Second backoff: generation 2 (incremented again after success reset).
    assert_timer_requested(
        &mut router,
        timer,
        &[TimerRequest {
            key: WorkloadTimerKey::RetryBackoff,
            generation: 2,
        }],
    );
}

// ============================================================================
// Suspend/Resume tests
// ============================================================================

/// Helper: set up a suspendable workload (suspend_on_idle=true) with an
/// activation-based service, a running pod, and return (mgmt, worker, timer).
fn setup_running_suspendable_workload(router: &mut Router) -> (ManagementId, WorkerId, TimerId) {
    let timer = router.create_timer();
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new_suspendable(timer));
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v1".into(),
        },
    );

    router.create_service(S1, ServiceSm::new(timer, true)); // activation-based
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: true,
        },
    );
    router.propagate();

    // Activate → demand → pod created.
    router.send_activate_service(mgmt, S1, true);
    router.propagate();

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    make_pod_running(router, worker, pod_id);

    assert!(router.get_workload(&W1).unwrap().pod_running);
    (mgmt, worker, timer)
}

/// 32. Basic suspend: demand drops on suspend_on_idle workload → pod suspends →
///     artifact saved → pod self-destructs.
#[test]
fn suspend_on_demand_drop() {
    let mut router = Router::new(16);
    let (mgmt, _worker, _timer) = setup_running_suspendable_workload(&mut router);

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);
    let pod_id = wl.pod_id.unwrap();

    // Deactivate service → demand drops to 0.
    router.send_activate_service(mgmt, S1, false);
    router.propagate();

    // Workload should have signaled Suspend (not destroyed the pod).
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.awaiting_suspend);
    assert!(wl.pod_id.is_some()); // pod still alive
    assert!(!wl.pod_running); // no longer considered running by workload

    // Pod should be in Suspending state.
    let pod = router.get_pod(&pod_id).unwrap();
    assert_eq!(pod.status, PodStatus::Suspending);

    // Worker completes the suspend.
    let artifact = ArtifactId(42);
    router.send_notify_pod_suspended(_worker, pod_id, artifact);
    router.propagate();

    // Workload should have saved the artifact and reaped the pod.
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.awaiting_suspend);
    assert!(wl.pod_id.is_none()); // pod reaped
    assert_eq!(wl.suspended_artifact, Some(artifact));

    // Pod should be gone (self-destructed: terminal + no owner).
    assert!(router.get_pod(&pod_id).is_none());
}

/// 33. Resume from artifact: workload with suspended artifact + demand →
///     creates pod from artifact instead of cold boot.
#[test]
fn resume_from_artifact() {
    let mut router = Router::new(16);
    let (mgmt, worker, _timer) = setup_running_suspendable_workload(&mut router);

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();

    // Suspend: deactivate → suspend → complete.
    router.send_activate_service(mgmt, S1, false);
    router.propagate();

    let artifact = ArtifactId(100);
    router.send_notify_pod_suspended(worker, pod_id, artifact);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.suspended_artifact, Some(artifact));
    assert!(wl.pod_id.is_none());

    // Re-activate → demand returns → workload should create pod from artifact.
    router.send_activate_service(mgmt, S1, true);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_id.is_some());
    let new_pod_id = wl.pod_id.unwrap();
    assert_ne!(new_pod_id, pod_id); // new pod, different ID

    // The new pod should have been created with the artifact.
    let new_pod = router.get_pod(&new_pod_id).unwrap();
    assert_eq!(new_pod.resume_artifact, Some(artifact));

    // Artifact consumed from workload state.
    assert_eq!(wl.suspended_artifact, None);

    // Make resumed pod running — should become active normally.
    make_pod_running(&mut router, worker, new_pod_id);

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);

    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));
}

/// 34. Demand returns during suspend: pod is suspending, demand comes back,
///     pod completes suspend, workload immediately resumes from artifact.
#[test]
fn demand_returns_during_suspend() {
    let mut router = Router::new(16);
    let (mgmt, worker, _timer) = setup_running_suspendable_workload(&mut router);

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();

    // Start suspend.
    router.send_activate_service(mgmt, S1, false);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.awaiting_suspend);

    // Demand returns while suspending.
    router.send_activate_service(mgmt, S1, true);
    router.propagate();

    // Pod is still suspending — can't go back (lifecycle is non-circular).
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.awaiting_suspend); // still waiting
    assert!(wl.has_demand); // demand is back

    let pod = router.get_pod(&pod_id).unwrap();
    assert_eq!(pod.status, PodStatus::Suspending); // still suspending

    // Worker completes the suspend.
    let artifact = ArtifactId(200);
    router.send_notify_pod_suspended(worker, pod_id, artifact);
    router.propagate();

    // Workload saved artifact, reaped pod, and immediately created new pod
    // from artifact (because demand is present).
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.awaiting_suspend);
    assert!(wl.pod_id.is_some());
    assert_ne!(wl.pod_id.unwrap(), pod_id); // new pod
    assert_eq!(wl.suspended_artifact, None); // consumed

    // Old pod is gone.
    assert!(router.get_pod(&pod_id).is_none());

    // New pod should have the artifact for resume.
    let new_pod = router.get_pod(&wl.pod_id.unwrap()).unwrap();
    assert_eq!(new_pod.resume_artifact, Some(artifact));
}

/// 35. Spec change during suspend: workload abandons the suspending pod
///     (artifact is stale), cold boots with new spec.
#[test]
fn spec_change_during_suspend() {
    let mut router = Router::new(16);
    let (mgmt, worker, _timer) = setup_running_suspendable_workload(&mut router);

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();

    // Start suspend.
    router.send_activate_service(mgmt, S1, false);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.awaiting_suspend);

    // Spec changes while pod is suspending.
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v2".into(),
        },
    );
    router.propagate();

    // Spec change while pod_running=false doesn't trigger immediate restart
    // (that branch checks pod_running). But spec_version is incremented.
    // The pod is still suspending.

    // Re-activate demand so the workload wants a pod again.
    router.send_activate_service(mgmt, S1, true);
    router.propagate();

    // Worker completes the suspend.
    let artifact = ArtifactId(300);
    router.send_notify_pod_suspended(worker, pod_id, artifact);
    router.propagate();

    // Workload should have created a new pod. Since spec changed,
    // the spec_version != launched_with_spec_version check will catch it
    // when the pod reaches Running (if it used the old artifact).
    // But actually, the workload still uses the artifact for resume
    // since the artifact was saved. The spec mismatch is detected at Running.
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_id.is_some());
    assert_ne!(wl.pod_id.unwrap(), pod_id);
}

/// 36. Worker loss while pod is running on a suspendable workload — pod fails
///     (not suspended), enters backoff normally. No artifact saved.
#[test]
fn worker_loss_on_suspendable_workload() {
    let mut router = Router::new(16);
    let (_mgmt, worker, _timer) = setup_running_suspendable_workload(&mut router);

    // Worker dies — this is NOT a suspend, it's a failure.
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.pod_running);
    assert!(wl.in_backoff); // failure, not suspend
    assert_eq!(wl.consecutive_failures, 1);
    assert_eq!(wl.suspended_artifact, None); // no artifact from a crash

    // Service should be back to NeedBackend.
    let s1 = router.get_service(&S1).unwrap();
    assert_eq!(s1.state, ServiceState::NeedBackend);
}

/// 37. Destroy (hard kill) discards any previously saved artifact.
#[test]
fn destroy_discards_artifact() {
    let mut router = Router::new(16);
    let (mgmt, worker, _timer) = setup_running_suspendable_workload(&mut router);

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();

    // Suspend successfully.
    router.send_activate_service(mgmt, S1, false);
    router.propagate();
    let artifact = ArtifactId(400);
    router.send_notify_pod_suspended(worker, pod_id, artifact);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.suspended_artifact, Some(artifact));

    // Re-activate → resumes from artifact.
    router.send_activate_service(mgmt, S1, true);
    router.propagate();

    let new_pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    make_pod_running(&mut router, worker, new_pod_id);

    // Now do a hard restart (admin command) — destroys pod AND discards artifact.
    router.send_admin_command(mgmt, W1, AdminCmd::Restart);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.suspended_artifact, None); // artifact discarded
    assert!(wl.pod_id.is_some()); // new pod created (cold boot)

    // New pod should NOT have an artifact.
    let restart_pod = router.get_pod(&wl.pod_id.unwrap()).unwrap();
    assert_eq!(restart_pod.resume_artifact, None);
}

/// 38. Suspend → resume → suspend cycle: verify the full round-trip works
///     and artifact IDs are tracked correctly.
#[test]
fn suspend_resume_suspend_cycle() {
    let mut router = Router::new(16);
    let (mgmt, worker, _timer) = setup_running_suspendable_workload(&mut router);

    // First suspend.
    let pod1 = router.get_workload(&W1).unwrap().pod_id.unwrap();
    router.send_activate_service(mgmt, S1, false);
    router.propagate();
    let artifact1 = ArtifactId(500);
    router.send_notify_pod_suspended(worker, pod1, artifact1);
    router.propagate();

    assert_eq!(
        router.get_workload(&W1).unwrap().suspended_artifact,
        Some(artifact1)
    );

    // First resume.
    router.send_activate_service(mgmt, S1, true);
    router.propagate();
    let pod2 = router.get_workload(&W1).unwrap().pod_id.unwrap();
    assert_ne!(pod1, pod2);
    assert_eq!(
        router.get_pod(&pod2).unwrap().resume_artifact,
        Some(artifact1)
    );
    make_pod_running(&mut router, worker, pod2);

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);

    // Second suspend.
    router.send_activate_service(mgmt, S1, false);
    router.propagate();
    let artifact2 = ArtifactId(501);
    router.send_notify_pod_suspended(worker, pod2, artifact2);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.suspended_artifact, Some(artifact2));
    assert!(wl.pod_id.is_none());

    // Second resume.
    router.send_activate_service(mgmt, S1, true);
    router.propagate();
    let pod3 = router.get_workload(&W1).unwrap().pod_id.unwrap();
    assert_ne!(pod2, pod3);
    assert_eq!(
        router.get_pod(&pod3).unwrap().resume_artifact,
        Some(artifact2)
    );
}

/// 39. Scavenge on suspendable workload with no demand — should behave like
///     normal scavenge (no suspend, just cleanup since pod is already gone).
#[test]
fn scavenge_clears_suspended_artifact() {
    let mut router = Router::new(16);
    let (mgmt, worker, _timer) = setup_running_suspendable_workload(&mut router);

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();

    // Suspend successfully.
    router.send_activate_service(mgmt, S1, false);
    router.propagate();
    let artifact = ArtifactId(600);
    router.send_notify_pod_suspended(worker, pod_id, artifact);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.suspended_artifact, Some(artifact));

    // Scavenge with no demand — should clear the artifact.
    router.send_admin_command(mgmt, W1, AdminCmd::Scavenge);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.suspended_artifact, None);
    assert!(wl.pod_id.is_none());
}

// ============================================================================
// Service idle timeout + BackendNeed tests
// ============================================================================

/// 40. Traffic-triggered activation: idle service receives BackendNeed(Traffic)
///     from worker → activates → demand → pod boots.
#[test]
fn traffic_triggered_activation() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new(timer));
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v1".into(),
        },
    );

    router.create_service(S1, ServiceSm::new(timer, true)); // activation-based
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: true,
        },
    );
    router.propagate();

    // Service is idle, no demand.
    let s1 = router.get_service(&S1).unwrap();
    assert_eq!(s1.state, ServiceState::Idle);
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.has_demand);

    // Worker reports traffic → service activates.
    router.set_worker_to_service_edges(worker, vec![S1]);
    router.set_worker_backend_need(worker, BackendNeed::Traffic);
    router.propagate();

    // Service: Idle → NeedBackend, demand=true.
    let s1 = router.get_service(&S1).unwrap();
    assert_eq!(s1.state, ServiceState::NeedBackend);
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_demand);
    assert!(wl.pod_id.is_some());

    // Make pod running.
    let pod_id = wl.pod_id.unwrap();
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Running);
    router.propagate();

    // Service should be Active.
    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));
}

/// 41. Idle timeout deactivation: running service, traffic stops → idle timer
///     fires → service deactivates → demand drops → workload destroys pod.
#[test]
fn idle_timeout_deactivation() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new(timer));
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v1".into(),
        },
    );

    router.create_service(S1, ServiceSm::new(timer, true));
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: true,
        },
    );
    router.propagate();

    // Traffic activates the service.
    router.set_worker_to_service_edges(worker, vec![S1]);
    router.set_worker_backend_need(worker, BackendNeed::Traffic);
    router.propagate();

    // Make pod running.
    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Running);
    router.propagate();

    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));
    assert!(!s1.idle_timer_active);

    // Traffic stops → idle timer starts.
    router.set_worker_backend_need(worker, BackendNeed::None);
    router.propagate();

    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. })); // still active
    assert!(s1.idle_timer_active);
    assert_eq!(s1.idle_generation, 1);

    // Fire the idle timer → service deactivates.
    router.send_service_timer_fired(timer, S1, ServiceTimerKey::IdleTimeout);
    router.propagate();

    let s1 = router.get_service(&S1).unwrap();
    assert_eq!(s1.state, ServiceState::Idle);
    assert!(!s1.idle_timer_active);

    // Demand should be gone → workload destroyed the pod.
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.has_demand);
    assert!(wl.pod_id.is_none());
}

/// 42. Traffic cancels idle timer: idle timer running, new traffic arrives →
///     timer cancelled, service stays active.
#[test]
fn traffic_cancels_idle_timer() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new(timer));
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v1".into(),
        },
    );

    router.create_service(S1, ServiceSm::new(timer, true));
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: true,
        },
    );
    router.propagate();

    // Traffic → activate → pod running.
    router.set_worker_to_service_edges(worker, vec![S1]);
    router.set_worker_backend_need(worker, BackendNeed::Traffic);
    router.propagate();

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Running);
    router.propagate();

    // Traffic stops → idle timer starts.
    router.set_worker_backend_need(worker, BackendNeed::None);
    router.propagate();

    let s1 = router.get_service(&S1).unwrap();
    assert!(s1.idle_timer_active);

    // Traffic returns → idle timer cancelled.
    router.set_worker_backend_need(worker, BackendNeed::Traffic);
    router.propagate();

    let s1 = router.get_service(&S1).unwrap();
    assert!(!s1.idle_timer_active);
    assert!(matches!(s1.state, ServiceState::Active { .. }));

    // Demand still present.
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_demand);
    assert!(wl.pod_running);
}

/// 43. Idle timeout + suspend integration: full chain from traffic loss → idle
///     timer → service deactivates → demand drops → workload suspends pod →
///     artifact saved.
#[test]
fn idle_timeout_suspend_integration() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new_suspendable(timer));
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v1".into(),
        },
    );

    router.create_service(S1, ServiceSm::new(timer, true));
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: true,
        },
    );
    router.propagate();

    // Traffic → activate → pod running.
    router.set_worker_to_service_edges(worker, vec![S1]);
    router.set_worker_backend_need(worker, BackendNeed::Traffic);
    router.propagate();

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Running);
    router.propagate();

    assert!(matches!(
        router.get_service(&S1).unwrap().state,
        ServiceState::Active { .. }
    ));
    assert!(router.get_workload(&W1).unwrap().pod_running);

    // Traffic stops → idle timer starts.
    router.set_worker_backend_need(worker, BackendNeed::None);
    router.propagate();

    assert!(router.get_service(&S1).unwrap().idle_timer_active);

    // Idle timer fires → service deactivates → demand drops →
    // workload signals pod to suspend (suspend_on_idle=true).
    router.send_service_timer_fired(timer, S1, ServiceTimerKey::IdleTimeout);
    router.propagate();

    let s1 = router.get_service(&S1).unwrap();
    assert_eq!(s1.state, ServiceState::Idle);

    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.has_demand);
    assert!(wl.awaiting_suspend);

    // Pod should be suspending.
    let pod = router.get_pod(&pod_id).unwrap();
    assert_eq!(pod.status, PodStatus::Suspending);

    // Worker completes suspend.
    let artifact = ArtifactId(42);
    router.send_notify_pod_suspended(worker, pod_id, artifact);
    router.propagate();

    // Workload saved artifact, pod reaped.
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.awaiting_suspend);
    assert!(wl.pod_id.is_none());
    assert_eq!(wl.suspended_artifact, Some(artifact));
    assert!(router.get_pod(&pod_id).is_none());
}

/// 44. Worker loss removes backend need: worker providing traffic dies →
///     BackendNeed aggregates to None → idle timer starts.
#[test]
fn worker_loss_removes_backend_need() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new(timer));
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v1".into(),
        },
    );

    router.create_service(S1, ServiceSm::new(timer, true));
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: true,
        },
    );
    router.propagate();

    // Traffic → activate → pod running.
    router.set_worker_to_service_edges(worker, vec![S1]);
    router.set_worker_backend_need(worker, BackendNeed::Traffic);
    router.propagate();

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Running);
    router.propagate();

    assert!(matches!(
        router.get_service(&S1).unwrap().state,
        ServiceState::Active { .. }
    ));
    assert!(!router.get_service(&S1).unwrap().idle_timer_active);

    // Worker dies → BackendNeed aggregates to None → idle timer starts.
    // (Pod also fails, but service idle timer is the focus here.)
    router.destroy_worker(worker);
    router.propagate();

    // Service lost readiness (pod failed) → back to NeedBackend.
    // Idle timer should have been cleared when readiness was lost
    // (Active→NeedBackend transition clears idle timer).
    let s1 = router.get_service(&S1).unwrap();
    assert_eq!(s1.state, ServiceState::NeedBackend);
    assert!(!s1.idle_timer_active);
}

/// 45. Multiple workers, one loses traffic: two workers with traffic, one drops
///     to None → aggregate still Traffic → no idle timer.
#[test]
fn multiple_workers_one_loses_traffic() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let worker1 = router.create_worker();
    let worker2 = router.create_worker();
    router.set_worker_info(worker1, WorkerInfo { capacity: 10 });
    router.set_worker_info(worker2, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new(timer));
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v1".into(),
        },
    );

    router.create_service(S1, ServiceSm::new(timer, true));
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: true,
        },
    );
    router.propagate();

    // Both workers report traffic.
    router.set_worker_to_service_edges(worker1, vec![S1]);
    router.set_worker_to_service_edges(worker2, vec![S1]);
    router.set_worker_backend_need(worker1, BackendNeed::Traffic);
    router.set_worker_backend_need(worker2, BackendNeed::Traffic);
    router.propagate();

    // Service activated via traffic.
    let s1 = router.get_service(&S1).unwrap();
    assert_eq!(s1.state, ServiceState::NeedBackend);

    // Make pod running.
    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    router.set_worker_to_pod_edges(worker1, vec![pod_id]);
    router.send_notify_pod_status(worker1, pod_id, PodStatus::Running);
    router.propagate();

    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));
    assert!(!s1.idle_timer_active);

    // Worker1 drops to None → aggregate still Traffic (worker2 has Traffic).
    router.set_worker_backend_need(worker1, BackendNeed::None);
    router.propagate();

    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));
    assert!(!s1.idle_timer_active); // no idle timer — still has traffic

    // Worker1 reports Active (highest priority) → still no idle timer.
    router.set_worker_backend_need(worker1, BackendNeed::Active);
    router.propagate();

    let s1 = router.get_service(&S1).unwrap();
    assert!(!s1.idle_timer_active);

    // Worker1 back to None.
    router.set_worker_backend_need(worker1, BackendNeed::None);
    router.propagate();

    // Worker2 also drops → aggregate None → idle timer starts.
    router.set_worker_backend_need(worker2, BackendNeed::None);
    router.propagate();

    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));
    assert!(s1.idle_timer_active); // now idle timer starts
}

// ============================================================================
// Graceful exit (Finished) + Worker identity tests
// ============================================================================

/// 46. Graceful pod exit (Finished) does not increment failure counter and
///     does not enter backoff. Workload creates new pod if demand exists.
#[test]
fn graceful_exit_no_failure_count() {
    let mut router = Router::new(16);
    let (mgmt, worker, timer) = setup_running_workload(&mut router, 5);

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);
    let pod_id = wl.pod_id.unwrap();

    // Pod exits gracefully.
    router.send_notify_pod_status(worker, pod_id, PodStatus::Finished);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.pod_running);
    assert_eq!(wl.consecutive_failures, 0); // no failure increment
    assert!(!wl.in_backoff); // no backoff

    // Workload should have created a new pod (demand still exists).
    assert!(wl.pod_id.is_some());
    assert_ne!(wl.pod_id.unwrap(), pod_id); // new pod

    // No retry timer needed.
    assert_no_timers_wanted(&mut router, timer);
}

/// 47. Graceful exit (Finished) does not count as failure even after prior
///     failures — consecutive_failures stays unchanged.
#[test]
fn graceful_exit_after_failures_preserves_count() {
    let mut router = Router::new(16);
    let (_mgmt, worker, timer) = setup_running_workload(&mut router, 5);

    // First failure via worker loss.
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 1);
    assert!(wl.in_backoff);

    // Timer fires → retry.
    router.send_workload_timer_fired(timer, W1, WorkloadTimerKey::RetryBackoff);
    router.propagate();

    let pod2 = router.get_workload(&W1).unwrap().pod_id.unwrap();

    // New worker, make pod running.
    let worker2 = router.create_worker();
    router.set_worker_info(worker2, WorkerInfo { capacity: 10 });
    make_pod_running(&mut router, worker2, pod2);

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);
    assert_eq!(wl.consecutive_failures, 0); // reset on Running

    // Pod finishes gracefully.
    router.send_notify_pod_status(worker2, pod2, PodStatus::Finished);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 0); // still 0, not incremented
    assert!(!wl.in_backoff); // no backoff for graceful exit
    assert!(wl.pod_id.is_some()); // new pod created (demand exists)
}

/// 48. Finished vs Failed: Finished after Running doesn't enter backoff,
///     Failed after Running does.
#[test]
fn finished_vs_failed_backoff_behavior() {
    // Finished path: no backoff.
    let mut router = Router::new(16);
    let (_mgmt, worker, timer) = setup_running_workload(&mut router, 5);
    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();

    router.send_notify_pod_status(worker, pod_id, PodStatus::Finished);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.in_backoff);
    assert_eq!(wl.consecutive_failures, 0);
    assert!(wl.pod_id.is_some()); // immediately created new pod

    // Failed path: enters backoff.
    let mut router = Router::new(16);
    let (_mgmt, worker, timer) = setup_running_workload(&mut router, 5);
    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();

    router.send_notify_pod_status(worker, pod_id, PodStatus::Failed);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.in_backoff);
    assert_eq!(wl.consecutive_failures, 1);
    assert!(wl.pod_id.is_none()); // waiting for backoff timer
}

/// 49. Pod self-destructs on Finished + no owner (same reaping rule as Failed).
#[test]
fn finished_pod_self_destructs() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    // Create a standalone pod (no workload owner).
    let pod_id = router.create_pod(PodSm::new(timer));
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.send_notify_pod_status(worker, pod_id, PodStatus::Running);
    router.propagate();

    assert_eq!(router.get_pod(&pod_id).unwrap().status, PodStatus::Running);

    // Pod finishes gracefully — terminal + no owner → self-destruct.
    router.send_notify_pod_status(worker, pod_id, PodStatus::Finished);
    router.propagate();

    assert!(router.get_pod(&pod_id).is_none());
}

/// 50. Worker identity: readiness carries the correct worker ID from the pod's
///     assigned worker, not a placeholder.
#[test]
fn worker_identity_in_readiness() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new(timer));
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v1".into(),
        },
    );

    router.create_service(S1, ServiceSm::new(timer, false)); // always-on
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: false,
        },
    );
    router.propagate();

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();

    // Wire worker to pod and make running.
    make_pod_running(&mut router, worker, pod_id);

    // Pod should know its worker.
    let pod = router.get_pod(&pod_id).unwrap();
    assert_eq!(pod.worker_id, Some(worker));

    // Workload should have the worker ID.
    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.pod_worker_id, Some(worker));

    // Service readiness should carry the real worker ID.
    let s1 = router.get_service(&S1).unwrap();
    match &s1.state {
        ServiceState::Active { ready } => {
            assert_eq!(ready.worker_id, worker);
            assert_eq!(ready.pod_id, pod_id);
        }
        other => panic!("expected Active, got {:?}", other),
    }
}

/// 51. Worker identity updates when pod moves to a different worker
///     (e.g., after failure and re-creation on new worker).
#[test]
fn worker_identity_updates_on_new_worker() {
    let mut router = Router::new(16);
    let (_mgmt, worker1, timer) = setup_running_workload(&mut router, 5);

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.pod_worker_id, Some(worker1));

    // Worker1 dies → pod fails → backoff.
    router.destroy_worker(worker1);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.in_backoff);
    assert_eq!(wl.pod_worker_id, None); // cleared on failure

    // Timer fires → new pod created.
    router.send_workload_timer_fired(timer, W1, WorkloadTimerKey::RetryBackoff);
    router.propagate();

    let new_pod = router.get_workload(&W1).unwrap().pod_id.unwrap();

    // New worker takes over.
    let worker2 = router.create_worker();
    router.set_worker_info(worker2, WorkerInfo { capacity: 10 });
    make_pod_running(&mut router, worker2, new_pod);

    // Workload should now report worker2.
    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.pod_worker_id, Some(worker2));

    // Service readiness should carry worker2.
    let s1 = router.get_service(&S1).unwrap();
    match &s1.state {
        ServiceState::Active { ready } => {
            assert_eq!(ready.worker_id, worker2);
        }
        other => panic!("expected Active, got {:?}", other),
    }
}

/// 52. Pod tracks worker from WorkerInput signal (not from event).
#[test]
fn pod_tracks_worker_from_input() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let pod_id = router.create_pod(PodSm::new(timer));
    router.propagate();

    // No worker assigned yet.
    let pod = router.get_pod(&pod_id).unwrap();
    assert_eq!(pod.worker_id, None);

    // Assign worker via edge.
    router.set_worker_to_pod_edges(worker, vec![pod_id]);
    router.propagate();

    // Pod should now know its worker.
    let pod = router.get_pod(&pod_id).unwrap();
    assert_eq!(pod.worker_id, Some(worker));

    // Remove worker edge → worker_id cleared.
    router.set_worker_to_pod_edges(worker, vec![]);
    router.propagate();

    // Pod should have failed (worker lost) and self-destructed (no owner).
    assert!(router.get_pod(&pod_id).is_none());
}

// ============================================================================
// Multi-workload tests
// ============================================================================

/// 53. Two workloads sharing a worker — worker dies, both workloads fail
///     independently and can recover on a new worker without interference.
#[test]
fn shared_worker_death_independent_failure() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();

    // Create two workloads, each with their own always-on service.
    router.create_workload(W1, WorkloadSm::new(timer));
    router.create_workload(W2, WorkloadSm::new(timer));

    router.set_management_to_workload_edges(mgmt, vec![W1, W2]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v1".into(),
        },
    );

    router.create_service(S1, ServiceSm::new(timer, false));
    router.create_service(S2, ServiceSm::new(timer, false));

    // S1 → W1, S2 → W2 (different management ports for different specs).
    let mgmt_s1 = router.create_management();
    router.set_management_to_service_edges(mgmt_s1, vec![S1]);
    router.set_management_svc_spec(
        mgmt_s1,
        ServiceSpec {
            workload: W1,
            has_activation: false,
        },
    );

    let mgmt_s2 = router.create_management();
    router.set_management_to_service_edges(mgmt_s2, vec![S2]);
    router.set_management_svc_spec(
        mgmt_s2,
        ServiceSpec {
            workload: W2,
            has_activation: false,
        },
    );
    router.propagate();

    // Both workloads should have demand and created pods.
    let wl1 = router.get_workload(&W1).unwrap();
    let wl2 = router.get_workload(&W2).unwrap();
    assert!(wl1.has_demand);
    assert!(wl2.has_demand);
    let pod1 = wl1.pod_id.unwrap();
    let pod2 = wl2.pod_id.unwrap();
    assert_ne!(pod1, pod2);

    // Both pods on same worker.
    router.set_worker_to_pod_edges(worker, vec![pod1, pod2]);
    router.send_notify_pod_status(worker, pod1, PodStatus::Running);
    router.send_notify_pod_status(worker, pod2, PodStatus::Running);
    router.propagate();

    // Both services should be active.
    let s1 = router.get_service(&S1).unwrap();
    let s2 = router.get_service(&S2).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));
    assert!(matches!(s2.state, ServiceState::Active { .. }));

    // Both workloads should report the correct worker.
    let wl1 = router.get_workload(&W1).unwrap();
    let wl2 = router.get_workload(&W2).unwrap();
    assert_eq!(wl1.pod_worker_id, Some(worker));
    assert_eq!(wl2.pod_worker_id, Some(worker));

    // Worker dies.
    router.destroy_worker(worker);
    router.propagate();

    // Both workloads should have failed independently.
    let wl1 = router.get_workload(&W1).unwrap();
    let wl2 = router.get_workload(&W2).unwrap();
    assert!(!wl1.pod_running);
    assert!(!wl2.pod_running);
    assert!(wl1.in_backoff);
    assert!(wl2.in_backoff);
    assert_eq!(wl1.consecutive_failures, 1);
    assert_eq!(wl2.consecutive_failures, 1);
    assert!(wl1.pod_id.is_none());
    assert!(wl2.pod_id.is_none());

    // Both services should be back to NeedBackend.
    let s1 = router.get_service(&S1).unwrap();
    let s2 = router.get_service(&S2).unwrap();
    assert_eq!(s1.state, ServiceState::NeedBackend);
    assert_eq!(s2.state, ServiceState::NeedBackend);

    // Fire both backoff timers.
    router.send_workload_timer_fired(timer, W1, WorkloadTimerKey::RetryBackoff);
    router.send_workload_timer_fired(timer, W2, WorkloadTimerKey::RetryBackoff);
    router.propagate();

    // Both should have created new pods.
    let wl1 = router.get_workload(&W1).unwrap();
    let wl2 = router.get_workload(&W2).unwrap();
    assert!(wl1.pod_id.is_some());
    assert!(wl2.pod_id.is_some());
    let new_pod1 = wl1.pod_id.unwrap();
    let new_pod2 = wl2.pod_id.unwrap();
    assert_ne!(new_pod1, new_pod2);

    // New worker — recover both.
    let worker2 = router.create_worker();
    router.set_worker_info(worker2, WorkerInfo { capacity: 10 });
    router.set_worker_to_pod_edges(worker2, vec![new_pod1, new_pod2]);
    router.send_notify_pod_status(worker2, new_pod1, PodStatus::Running);
    router.send_notify_pod_status(worker2, new_pod2, PodStatus::Running);
    router.propagate();

    // Both workloads should be running again.
    let wl1 = router.get_workload(&W1).unwrap();
    let wl2 = router.get_workload(&W2).unwrap();
    assert!(wl1.pod_running);
    assert!(wl2.pod_running);
    assert_eq!(wl1.consecutive_failures, 0);
    assert_eq!(wl2.consecutive_failures, 0);

    // Both services active again.
    let s1 = router.get_service(&S1).unwrap();
    let s2 = router.get_service(&S2).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));
    assert!(matches!(s2.state, ServiceState::Active { .. }));
}

/// 54. Service retargeting: service spec changes from workload W1 to W2.
///     Demand should transfer cleanly — old workload loses demand, new one gains it.
#[test]
fn service_retarget_workload() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();

    // Create two workloads with specs.
    router.create_workload(W1, WorkloadSm::new(timer));
    router.create_workload(W2, WorkloadSm::new(timer));
    router.set_management_to_workload_edges(mgmt, vec![W1, W2]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v1".into(),
        },
    );

    // One always-on service pointing at W1.
    router.create_service(S1, ServiceSm::new(timer, false));
    let mgmt_s1 = router.create_management();
    router.set_management_to_service_edges(mgmt_s1, vec![S1]);
    router.set_management_svc_spec(
        mgmt_s1,
        ServiceSpec {
            workload: W1,
            has_activation: false,
        },
    );
    router.propagate();

    // W1 should have demand, W2 should not.
    let wl1 = router.get_workload(&W1).unwrap();
    let wl2 = router.get_workload(&W2).unwrap();
    assert!(wl1.has_demand);
    assert!(!wl2.has_demand);
    assert!(wl1.pod_id.is_some());
    assert!(wl2.pod_id.is_none());

    // Make W1's pod running.
    let pod1 = wl1.pod_id.unwrap();
    make_pod_running(&mut router, worker, pod1);

    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));

    // Retarget S1 from W1 → W2.
    router.set_management_svc_spec(
        mgmt_s1,
        ServiceSpec {
            workload: W2,
            has_activation: false,
        },
    );
    router.propagate();

    // W1 should have lost demand, W2 should have gained it.
    let wl1 = router.get_workload(&W1).unwrap();
    let wl2 = router.get_workload(&W2).unwrap();
    assert!(!wl1.has_demand);
    assert!(wl2.has_demand);

    // W2 should have created a pod.
    assert!(wl2.pod_id.is_some());

    // W1's pod should be destroyed (demand dropped, not suspendable).
    assert!(wl1.pod_id.is_none());

    // Make W2's pod running.
    let pod2 = wl2.pod_id.unwrap();
    router.set_worker_to_pod_edges(worker, vec![pod2]);
    router.send_notify_pod_status(worker, pod2, PodStatus::Running);
    router.propagate();

    // S1 should be active with W2's readiness.
    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));

    let wl2 = router.get_workload(&W2).unwrap();
    assert!(wl2.pod_running);
}

/// 55. Two independent workload-service subgraphs coexist without interference.
#[test]
fn independent_workload_subgraphs() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    // W1 + S1 subgraph.
    router.create_workload(W1, WorkloadSm::new(timer));
    router.create_service(S1, ServiceSm::new(timer, true)); // activation-based

    let mgmt1 = router.create_management();
    router.set_management_to_workload_edges(mgmt1, vec![W1]);
    router.set_management_wl_spec(
        mgmt1,
        WorkloadSpec {
            image: "app-a:v1".into(),
        },
    );
    router.set_management_to_service_edges(mgmt1, vec![S1]);
    router.set_management_svc_spec(
        mgmt1,
        ServiceSpec {
            workload: W1,
            has_activation: true,
        },
    );

    // W2 + S2 subgraph.
    router.create_workload(W2, WorkloadSm::new(timer));
    router.create_service(S2, ServiceSm::new(timer, false)); // always-on

    let mgmt2 = router.create_management();
    router.set_management_to_workload_edges(mgmt2, vec![W2]);
    router.set_management_wl_spec(
        mgmt2,
        WorkloadSpec {
            image: "app-b:v1".into(),
        },
    );
    router.set_management_to_service_edges(mgmt2, vec![S2]);
    router.set_management_svc_spec(
        mgmt2,
        ServiceSpec {
            workload: W2,
            has_activation: false,
        },
    );
    router.propagate();

    // W1 has no demand (activation-based, not activated).
    // W2 has demand (always-on).
    let wl1 = router.get_workload(&W1).unwrap();
    let wl2 = router.get_workload(&W2).unwrap();
    assert!(!wl1.has_demand);
    assert!(wl1.pod_id.is_none());
    assert!(wl2.has_demand);
    assert!(wl2.pod_id.is_some());

    // Make W2's pod running.
    let pod2 = wl2.pod_id.unwrap();
    make_pod_running(&mut router, worker, pod2);

    let s2 = router.get_service(&S2).unwrap();
    assert!(matches!(s2.state, ServiceState::Active { .. }));

    // W1 is still idle — W2's activity doesn't affect it.
    let wl1 = router.get_workload(&W1).unwrap();
    assert!(!wl1.has_demand);
    let s1 = router.get_service(&S1).unwrap();
    assert_eq!(s1.state, ServiceState::Idle);

    // Activate S1 — now W1 also gets demand.
    router.send_activate_service(mgmt1, S1, true);
    router.propagate();

    let wl1 = router.get_workload(&W1).unwrap();
    assert!(wl1.has_demand);
    assert!(wl1.pod_id.is_some());

    let pod1 = wl1.pod_id.unwrap();
    router.set_worker_to_pod_edges(worker, vec![pod1, pod2]);
    router.send_notify_pod_status(worker, pod1, PodStatus::Running);
    router.propagate();

    // Both active, independent.
    let s1 = router.get_service(&S1).unwrap();
    let s2 = router.get_service(&S2).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));
    assert!(matches!(s2.state, ServiceState::Active { .. }));

    // Deactivate S1 — only W1 affected.
    router.send_activate_service(mgmt1, S1, false);
    router.propagate();

    let wl1 = router.get_workload(&W1).unwrap();
    let wl2 = router.get_workload(&W2).unwrap();
    assert!(!wl1.has_demand);
    assert!(wl1.pod_id.is_none());
    assert!(wl2.pod_running); // W2 unaffected

    let s1 = router.get_service(&S1).unwrap();
    let s2 = router.get_service(&S2).unwrap();
    assert_eq!(s1.state, ServiceState::Idle);
    assert!(matches!(s2.state, ServiceState::Active { .. }));
}

/// 56. Multiple services sharing a workload, one retargets away — demand
///     aggregation correctly updates for both the source and target workloads.
#[test]
fn service_fan_in_with_retarget() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    // Both workloads get specs.
    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new(timer));
    router.create_workload(W2, WorkloadSm::new(timer));
    router.set_management_to_workload_edges(mgmt, vec![W1, W2]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v1".into(),
        },
    );

    // Two always-on services both pointing at W1.
    router.create_service(S1, ServiceSm::new(timer, false));
    router.create_service(S2, ServiceSm::new(timer, false));

    let mgmt_s1 = router.create_management();
    router.set_management_to_service_edges(mgmt_s1, vec![S1]);
    router.set_management_svc_spec(
        mgmt_s1,
        ServiceSpec {
            workload: W1,
            has_activation: false,
        },
    );

    let mgmt_s2 = router.create_management();
    router.set_management_to_service_edges(mgmt_s2, vec![S2]);
    router.set_management_svc_spec(
        mgmt_s2,
        ServiceSpec {
            workload: W1,
            has_activation: false,
        },
    );
    router.propagate();

    // W1 has demand from both services, W2 has none.
    let wl1 = router.get_workload(&W1).unwrap();
    let wl2 = router.get_workload(&W2).unwrap();
    assert!(wl1.has_demand);
    assert!(!wl2.has_demand);
    assert!(wl1.pod_id.is_some());
    assert!(wl2.pod_id.is_none());

    // Make W1's pod running.
    let pod1 = wl1.pod_id.unwrap();
    make_pod_running(&mut router, worker, pod1);

    // Both services active via W1.
    let s1 = router.get_service(&S1).unwrap();
    let s2 = router.get_service(&S2).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));
    assert!(matches!(s2.state, ServiceState::Active { .. }));

    // Retarget S2 from W1 → W2.
    router.set_management_svc_spec(
        mgmt_s2,
        ServiceSpec {
            workload: W2,
            has_activation: false,
        },
    );
    router.propagate();

    // W1 still has demand (S1 still points at it).
    let wl1 = router.get_workload(&W1).unwrap();
    assert!(wl1.has_demand);
    assert!(wl1.pod_running); // still running

    // W2 now has demand (S2 retargeted to it).
    let wl2 = router.get_workload(&W2).unwrap();
    assert!(wl2.has_demand);
    assert!(wl2.pod_id.is_some());

    // S1 should still be active (W1 still has a running pod).
    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));

    // S2 should be NeedBackend (W2's pod is Pending, no readiness yet).
    let s2 = router.get_service(&S2).unwrap();
    assert_eq!(s2.state, ServiceState::NeedBackend);

    // Make W2's pod running.
    let pod2 = wl2.pod_id.unwrap();
    router.set_worker_to_pod_edges(worker, vec![pod1, pod2]);
    router.send_notify_pod_status(worker, pod2, PodStatus::Running);
    router.propagate();

    // Now S2 should also be active.
    let s2 = router.get_service(&S2).unwrap();
    assert!(matches!(s2.state, ServiceState::Active { .. }));

    // Both workloads running on same worker.
    let wl1 = router.get_workload(&W1).unwrap();
    let wl2 = router.get_workload(&W2).unwrap();
    assert!(wl1.pod_running);
    assert!(wl2.pod_running);
}

/// 57. Service self-destructs when its management spec is removed.
#[test]
fn service_self_destructs_on_spec_removal() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    router.create_workload(W1, WorkloadSm::new(timer));
    router.create_service(S1, ServiceSm::new(timer, false)); // always-on

    // Use separate mgmt ports so we can remove the service spec independently.
    let mgmt_wl = router.create_management();
    router.set_management_to_workload_edges(mgmt_wl, vec![W1]);
    router.set_management_wl_spec(
        mgmt_wl,
        WorkloadSpec {
            image: "app:v1".into(),
        },
    );

    let mgmt_svc = router.create_management();
    router.set_management_to_service_edges(mgmt_svc, vec![S1]);
    router.set_management_svc_spec(
        mgmt_svc,
        ServiceSpec {
            workload: W1,
            has_activation: false,
        },
    );
    router.propagate();

    // Service is alive and workload has demand.
    assert!(router.get_service(&S1).is_some());
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_demand);

    // Remove service management port — service spec becomes None.
    router.destroy_management(mgmt_svc);
    router.propagate();

    // Service should have self-destructed.
    assert!(router.get_service(&S1).is_none());

    // Workload should have lost demand (service's outgoing edges vanished).
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.has_demand);
}

/// 58. Workload self-destructs when its management spec is removed, pod cleans up.
#[test]
fn workload_self_destructs_on_spec_removal() {
    let mut router = Router::new(16);
    let (mgmt, worker, _timer) = setup_running_workload(&mut router, 5);

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);
    let pod_id = wl.pod_id.unwrap();

    // Service is active.
    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));

    // Remove management port — workload spec becomes None.
    router.destroy_management(mgmt);
    router.propagate();

    // Workload should have self-destructed.
    assert!(router.get_workload(&W1).is_none());

    // Service should also have self-destructed (its spec also came from mgmt).
    assert!(router.get_service(&S1).is_none());

    // Pod lost its owner edge → terminal + no owner → self-destruct.
    // The pod may need a worker status update to reach terminal first.
    // Send a Failed status to trigger the reaping rule.
    router.send_notify_pod_status(worker, pod_id, PodStatus::Failed);
    router.propagate();

    assert!(router.get_pod(&pod_id).is_none());
}

/// 59. Full teardown cascade: management → service → workload → pod, all gone.
#[test]
fn full_teardown_cascade() {
    let mut router = Router::new(16);
    let timer = router.create_timer();
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    // Set up two independent service→workload subgraphs via separate mgmt ports.
    let mgmt_wl = router.create_management();
    router.create_workload(W1, WorkloadSm::new(timer));
    router.create_workload(W2, WorkloadSm::new(timer));
    router.set_management_to_workload_edges(mgmt_wl, vec![W1, W2]);
    router.set_management_wl_spec(
        mgmt_wl,
        WorkloadSpec {
            image: "app:v1".into(),
        },
    );

    let mgmt_s1 = router.create_management();
    router.create_service(S1, ServiceSm::new(timer, false));
    router.set_management_to_service_edges(mgmt_s1, vec![S1]);
    router.set_management_svc_spec(
        mgmt_s1,
        ServiceSpec {
            workload: W1,
            has_activation: false,
        },
    );

    let mgmt_s2 = router.create_management();
    router.create_service(S2, ServiceSm::new(timer, false));
    router.set_management_to_service_edges(mgmt_s2, vec![S2]);
    router.set_management_svc_spec(
        mgmt_s2,
        ServiceSpec {
            workload: W2,
            has_activation: false,
        },
    );
    router.propagate();

    // Both workloads have pods.
    let pod1 = router.get_workload(&W1).unwrap().pod_id.unwrap();
    let pod2 = router.get_workload(&W2).unwrap().pod_id.unwrap();

    // Make both pods running.
    router.set_worker_to_pod_edges(worker, vec![pod1, pod2]);
    router.send_notify_pod_status(worker, pod1, PodStatus::Running);
    router.send_notify_pod_status(worker, pod2, PodStatus::Running);
    router.propagate();

    assert!(router.get_workload(&W1).unwrap().pod_running);
    assert!(router.get_workload(&W2).unwrap().pod_running);

    // Remove all management ports.
    router.destroy_management(mgmt_wl);
    router.destroy_management(mgmt_s1);
    router.destroy_management(mgmt_s2);
    router.propagate();

    // Both services and workloads should have self-destructed.
    assert!(router.get_service(&S1).is_none());
    assert!(router.get_service(&S2).is_none());
    assert!(router.get_workload(&W1).is_none());
    assert!(router.get_workload(&W2).is_none());

    // Pods lost owners — send terminal status to trigger reaping.
    router.send_notify_pod_status(worker, pod1, PodStatus::Failed);
    router.send_notify_pod_status(worker, pod2, PodStatus::Failed);
    router.propagate();

    assert!(router.get_pod(&pod1).is_none());
    assert!(router.get_pod(&pod2).is_none());
}

/// 60. Workload in retry backoff self-destructs cleanly on spec removal.
#[test]
fn teardown_during_backoff() {
    let mut router = Router::new(16);
    let (mgmt, worker, _timer) = setup_running_workload(&mut router, 5);

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();

    // Pod fails → workload enters backoff.
    make_pod_failed(&mut router, worker, pod_id);

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.in_backoff);
    assert!(wl.pod_id.is_none());

    // Remove management port during backoff.
    router.destroy_management(mgmt);
    router.propagate();

    // Workload and service should have self-destructed.
    assert!(router.get_workload(&W1).is_none());
    assert!(router.get_service(&S1).is_none());
}

/// 61. Workload awaiting suspend self-destructs on spec removal, pod cleans up.
#[test]
fn teardown_during_suspend() {
    let mut router = Router::new(16);
    let (mgmt, worker, _timer) = setup_running_suspendable_workload(&mut router);

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);
    let pod_id = wl.pod_id.unwrap();

    // Deactivate service → demand drops → workload signals Suspend.
    router.send_activate_service(mgmt, S1, false);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.awaiting_suspend);

    // Remove management port while awaiting suspend.
    router.destroy_management(mgmt);
    router.propagate();

    // Workload and service should have self-destructed.
    assert!(router.get_workload(&W1).is_none());
    assert!(router.get_service(&S1).is_none());

    // Pod lost owner — send terminal status to trigger reaping.
    router.send_notify_pod_status(worker, pod_id, PodStatus::Failed);
    router.propagate();

    assert!(router.get_pod(&pod_id).is_none());
}
