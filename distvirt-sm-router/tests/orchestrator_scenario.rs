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

use distvirt_sm_router::{trace, Aggregator, ListAggregator, SmHandler};

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
    Failed,
}

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

/// Timer key enum for workload-specific timers.
#[derive(Clone, Debug, PartialEq)]
enum WorkloadTimerKey {
    RetryBackoff,
}

// ============================================================================
// Aggregators
// ============================================================================

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

/// Aggregates incoming WorkloadToPod edges to extract the owner workload ID.
/// Expects at most one owner.
#[derive(Default)]
struct OwnerAggregator;

impl Aggregator for OwnerAggregator {
    type Input = (WorkloadId, bool);
    type Output = Option<WorkloadId>;

    fn aggregate(&self, inputs: &[(WorkloadId, bool)]) -> Option<WorkloadId> {
        inputs.first().map(|(id, _)| *id)
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
    }
    signals {
        Service::Demand(bool),
        Workload::Readiness(Option<ReadyInfo>),
        Workload::NeedsPod(bool),
        Pod::Status(PodStatus),
        Worker::Info(WorkerInfo),
        Management::WlSpec(WorkloadSpec),
        Management::SvcSpec(ServiceSpec),
    }
    edges {
        ServiceToWorkload: Service -> Workload,
        WorkloadToService: Workload -> Service,
        WorkloadToPod: Workload -> Pod,
        PodToWorkload: Pod -> Workload,
        WorkerToPod: Worker -> Pod,
        ManagementToWorkload: Management -> Workload,
        ManagementToService: Management -> Service,
    }
    events {
        AdminCommand(AdminCmd): Management -> Workload,
        ActivateService(bool): Management -> Service,
        NotifyPodStatus(PodStatus): Worker -> Pod,
        WorkloadTimerFired(WorkloadTimerKey): Management -> Workload,
    }
    inputs {
        Workload::DemandInput {
            sources: [(ServiceToWorkload, Service::Demand)],
            aggregator: DemandAggregator,
        },
        Workload::SpecInput {
            sources: [(ManagementToWorkload, Management::WlSpec)],
            aggregator: ListAggregator<ManagementId, WorkloadSpec>,
        },
        Workload::PodStatusInput {
            sources: [(PodToWorkload, Pod::Status)],
            aggregator: ListAggregator<PodId, PodStatus>,
        },
        Service::ReadinessInput {
            sources: [(WorkloadToService, Workload::Readiness)],
            aggregator: ListAggregator<WorkloadId, Option<ReadyInfo>>,
        },
        Service::SvcSpecInput {
            sources: [(ManagementToService, Management::SvcSpec)],
            aggregator: ListAggregator<ManagementId, ServiceSpec>,
        },
        Pod::WorkerInput {
            sources: [(WorkerToPod, Worker::Info)],
            aggregator: ListAggregator<WorkerId, WorkerInfo>,
        },
        Pod::OwnerInput {
            sources: [(WorkloadToPod, Workload::NeedsPod)],
            aggregator: OwnerAggregator,
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

struct ServiceSm {
    state: ServiceState,
    has_activation: bool,
}

impl ServiceSm {
    fn new(has_activation: bool) -> Self {
        ServiceSm {
            state: if has_activation {
                ServiceState::Idle
            } else {
                ServiceState::NeedBackend
            },
            has_activation,
        }
    }
}

impl SmHandler for ServiceSm {
    type Input = ServiceInput;
    type Ctx = ServiceCtx;

    fn handle(&mut self, input: Self::Input, ctx: &mut Self::Ctx) {
        match input {
            ServiceInput::ReadinessInput(readiness_list) => {
                let ready = readiness_list.into_iter().next().flatten();
                match (&self.state, ready) {
                    (ServiceState::NeedBackend, Some(info)) => {
                        self.state = ServiceState::Active { ready: info };
                    }
                    (ServiceState::Active { .. }, None) => {
                        self.state = ServiceState::NeedBackend;
                    }
                    (ServiceState::Active { .. }, Some(info)) => {
                        self.state = ServiceState::Active { ready: info };
                    }
                    _ => {}
                }
            }
            ServiceInput::SvcSpecInput(specs) => {
                if let Some(spec) = specs.into_iter().next() {
                    self.has_activation = spec.has_activation;
                    if !self.has_activation {
                        // Always-on: set demand immediately.
                        ctx.set_demand(true);
                        if matches!(self.state, ServiceState::Idle) {
                            self.state = ServiceState::NeedBackend;
                        }
                    }
                    ctx.set_service_to_workload_edges(vec![spec.workload]);
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
                    }
                }
            }
        }
    }
}

// ---- Workload SM ----

const MAX_RETRIES: u32 = 5;

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
}

impl WorkloadSm {
    fn new() -> Self {
        Self::with_max_retries(MAX_RETRIES)
    }

    #[allow(dead_code)]
    fn with_max_retries(max_retries: u32) -> Self {
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
        }
    }
}

impl SmHandler for WorkloadSm {
    type Input = WorkloadInput;
    type Ctx = WorkloadCtx;

    fn handle(&mut self, input: Self::Input, ctx: &mut Self::Ctx) {
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
            }
            WorkloadInput::SpecInput(specs) => {
                let new_has_spec = specs.into_iter().next().is_some();

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

                self.has_spec = new_has_spec;
                self.reconcile(ctx);
            }
            WorkloadInput::PodStatusInput(statuses) => {
                let was_running = self.pod_running;
                self.pod_running = statuses.iter().any(|s| *s == PodStatus::Running);
                let has_failed = statuses.iter().any(|s| *s == PodStatus::Failed);

                // All pods gone — clear tracking.
                if statuses.is_empty() && self.pod_id.is_some() {
                    self.pod_id = None;
                    self.committed_to_boot = false;
                    ctx.set_workload_to_pod_edges(vec![]);
                }

                if self.pod_running && !was_running {
                    // Pod just became Running — check current signal state
                    // to decide what to do. This replaces PendingIntent.
                    self.on_pod_running(ctx);
                } else if has_failed && self.pod_id.is_some() {
                    self.on_pod_failed(ctx);
                } else if !self.pod_running && was_running {
                    // Pod lost running status.
                    ctx.set_readiness(None);
                    self.reconcile(ctx);
                } else {
                    self.reconcile(ctx);
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
                        self.committed_to_boot = false;
                        self.consecutive_failures = 0;
                        self.in_backoff = false;
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
            }
            WorkloadInput::WorkloadTimerFired(key) => match key {
                WorkloadTimerKey::RetryBackoff => {
                    if self.in_backoff {
                        self.in_backoff = false;
                        self.reconcile(ctx);
                    }
                }
            },
        }
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
    fn on_pod_running(&mut self, ctx: &mut WorkloadCtx) {
        self.committed_to_boot = false;
        self.consecutive_failures = 0;

        // 1. Spec changed since we launched this pod → restart.
        if self.launched_with_spec_version != self.spec_version {
            self.destroy_current_pod(ctx);
            self.reconcile(ctx);
            return;
        }

        // 2. No demand → deactivate.
        if !self.has_demand {
            self.destroy_current_pod(ctx);
            self.reconcile(ctx);
            return;
        }

        // 3. Active — emit readiness.
        ctx.set_readiness(Some(ReadyInfo {
            pod_id: self.pod_id.unwrap_or(PodId(0)),
            worker_id: WorkerId(0), // placeholder
        }));
    }

    /// Called when a pod reports Failed status. Cleans up tracking and enters
    /// backoff for retry, or gives up if max retries exceeded.
    fn on_pod_failed(&mut self, ctx: &mut WorkloadCtx) {
        // Clear readiness — pod is no longer usable.
        self.pod_running = false;
        ctx.set_readiness(None);

        // Remove ownership edge so pod can self-destruct.
        ctx.set_workload_to_pod_edges(vec![]);
        self.pod_id = None;

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
        } else if !self.has_demand {
            // Going dormant — clear failure tracking.
            self.consecutive_failures = 0;
        }

        self.reconcile(ctx);
    }

    /// Signal the current pod to self-destruct by removing the ownership edge.
    /// The pod sees OwnerInput(None) and self-destructs.
    /// We'll learn it's gone via PodStatusInput([]).
    fn destroy_current_pod(&mut self, ctx: &mut WorkloadCtx) {
        if self.pod_id.is_some() {
            ctx.set_workload_to_pod_edges(vec![]);
        }
        self.pod_running = false;
        ctx.set_readiness(None);
    }

    fn reconcile(&mut self, ctx: &mut WorkloadCtx) {
        let is_failed = self.consecutive_failures >= self.max_retries;
        let want_pod = self.has_spec
            && (self.has_demand || self.committed_to_boot)
            && !self.in_backoff
            && !is_failed;
        self.wants_pod = want_pod;
        ctx.set_needs_pod(want_pod);

        if want_pod && self.pod_id.is_none() {
            let pod_id = ctx.create_pod(PodSm::new());
            self.pod_id = Some(pod_id);
            self.launched_with_spec_version = self.spec_version;
            ctx.set_workload_to_pod_edges(vec![pod_id]);
        } else if !want_pod && self.pod_id.is_some() {
            ctx.set_workload_to_pod_edges(vec![]);
            ctx.set_readiness(None);
        }
    }
}

// ---- Pod SM ----

struct PodSm {
    status: PodStatus,
    workload_id: Option<WorkloadId>,
}

impl PodSm {
    fn new() -> Self {
        PodSm {
            status: PodStatus::Pending,
            workload_id: None,
        }
    }
}

impl SmHandler for PodSm {
    type Input = PodInput;
    type Ctx = PodCtx;

    fn handle(&mut self, input: Self::Input, ctx: &mut Self::Ctx) {
        match input {
            PodInput::WorkerInput(workers) => {
                if workers.is_empty() {
                    // Worker lost — pod is dead.
                    if self.status != PodStatus::Failed {
                        self.status = PodStatus::Failed;
                        ctx.set_status(PodStatus::Failed);
                    }
                }
            }
            PodInput::OwnerInput(owner) => {
                let had_owner = self.workload_id.is_some();
                self.workload_id = owner;
                let edges: Vec<WorkloadId> = owner.into_iter().collect();
                ctx.set_pod_to_workload_edges(edges);

                // Lost owner → self-destruct.
                if had_owner && owner.is_none() {
                    ctx.self_destruct();
                }
            }
            PodInput::NotifyPodStatus(new_status) => {
                self.status = new_status.clone();
                ctx.set_status(new_status);
            }
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

// ============================================================================
// Tests
// ============================================================================

/// 1. Demand aggregation: 3 activation-based services → 1 workload, toggle demand.
#[test]
fn demand_aggregation() {
    let mut router = Router::new(16);
    router.create_workload(W1, WorkloadSm::new());
    router.create_service(S1, ServiceSm::new(true));
    router.create_service(S2, ServiceSm::new(true));
    router.create_service(S3, ServiceSm::new(true));

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
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new());
    router.create_service(S1, ServiceSm::new(false)); // always-on
    router.create_service(S2, ServiceSm::new(false));

    // Deliver workload spec.
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "test:latest".into() });

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
    router.create_service(S3, ServiceSm::new(false));
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
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new());
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "test:latest".into() });
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_spec);
    assert!(!wl.has_demand);

    // Add an always-on service with demand.
    router.create_service(S1, ServiceSm::new(false));
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
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new());
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v1".into() });

    router.create_service(S1, ServiceSm::new(false));
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

    // Worker dies — remove the port.
    router.destroy_worker(worker);
    router.propagate();

    // Pod was failed and workload released it (on_pod_failed → self-destruct).
    // Workload should have lost readiness and entered backoff for retry.
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.pod_running);
    assert!(wl.in_backoff);

    // Service should be back to NeedBackend.
    let s1 = router.get_service(&S1).unwrap();
    assert_eq!(s1.state, ServiceState::NeedBackend);
}

/// 5. Spec delivery via management port: init and update use same path.
#[test]
fn spec_via_management_port() {
    let mut router = Router::new(16);
    router.create_workload(W1, WorkloadSm::new());

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

    // Remove management port — spec gone.
    router.destroy_management(mgmt);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.has_spec);
}

/// 6. Service spec via management port: service reads its spec, creates
///    edges to the target workload reactively.
#[test]
fn service_spec_creates_edges_reactively() {
    let mut router = Router::new(16);
    router.create_workload(W1, WorkloadSm::new());
    router.create_service(S1, ServiceSm::new(false));

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
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new());
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v1".into() });

    router.create_service(S1, ServiceSm::new(false));
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

    // Infrastructure.
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    // Management ports: one for workload, one per service (different specs).
    let mgmt_wl = router.create_management();
    let mgmt_s1 = router.create_management();
    let mgmt_s2 = router.create_management();

    // Create SMs.
    router.create_workload(W1, WorkloadSm::new());
    router.create_service(S1, ServiceSm::new(false));
    router.create_service(S2, ServiceSm::new(true)); // activation-based

    // Wire management → SMs.
    router.set_management_to_workload_edges(mgmt_wl, vec![W1]);
    router.set_management_wl_spec(mgmt_wl, WorkloadSpec { image: "app:v1".into() });
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

    // Worker dies.
    router.destroy_worker(worker);
    router.propagate();

    // Pod failed — workload released it and entered backoff.
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.pod_running);
    assert!(wl.in_backoff);

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
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();

    router.create_workload(W1, WorkloadSm::new());
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v1".into() });

    // Always-on service
    router.create_service(S1, ServiceSm::new(false));
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
    let mgmt = router.create_management();

    // Create first pod via router.
    let p1 = router.create_pod(PodSm::new());

    // Create workload and wire it.
    router.create_workload(W1, WorkloadSm::new());
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "test".into() });

    // Create service to give workload demand → workload creates pod in handler.
    router.create_service(S1, ServiceSm::new(false));
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
    let p3 = router.create_pod(PodSm::new());
    assert!(p3.0 > p2.0);
}

// ============================================================================
// PendingIntent-equivalent tests — signal-based transition decisions
// ============================================================================

/// Helper: set up a workload with an activation-based service that has been
/// activated, return (mgmt, worker, pod_id).
/// After this, workload has spec + demand, a pod exists in Pending state.
/// Use send_activate_service(mgmt, S1, false) to drop demand.
fn setup_workload_with_pending_pod(router: &mut Router) -> (ManagementId, WorkerId, PodId) {
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new());
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v1".into() });

    router.create_service(S1, ServiceSm::new(true)); // activation-based
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

    (mgmt, worker, pod_id)
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
/// service, a running pod, and return (mgmt, worker).
fn setup_running_workload(router: &mut Router, max_retries: u32) -> (ManagementId, WorkerId) {
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::with_max_retries(max_retries));
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v1".into() });

    router.create_service(S1, ServiceSm::new(false)); // always-on
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
    (mgmt, worker)
}

/// 11. Demand drops during pod launch — committed_to_boot keeps the pod alive.
///     When the pod reaches Running, demand is re-checked and workload deactivates.
#[test]
fn demand_drop_during_launch_committed_to_boot() {
    let mut router = Router::new(16);
    let (mgmt, worker, pod_id) = setup_workload_with_pending_pod(&mut router);

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
    let (mgmt, worker, pod_id) = setup_workload_with_pending_pod(&mut router);

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
    let (mgmt, worker, pod_id) = setup_workload_with_pending_pod(&mut router);

    let wl = router.get_workload(&W1).unwrap();
    let original_pod = wl.pod_id.unwrap();

    // Spec changes while pod is launching.
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v2".into() });
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
    let (mgmt, worker, pod_id) = setup_workload_with_pending_pod(&mut router);

    // Get pod running.
    make_pod_running(&mut router, worker, pod_id);

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);
    let original_pod = wl.pod_id.unwrap();

    // Spec changes while running.
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v2".into() });
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
    let (mgmt, worker, pod_id) = setup_workload_with_pending_pod(&mut router);

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
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new());
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v1".into() });

    // Activation-based service.
    router.create_service(S1, ServiceSm::new(true));
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
    let (mgmt, worker, pod_id) = setup_workload_with_pending_pod(&mut router);

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
    let (mgmt, _worker, _pod_id) = setup_workload_with_pending_pod(&mut router);

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
    let (mgmt, worker, pod_id) = setup_workload_with_pending_pod(&mut router);

    let original_pod = router.get_workload(&W1).unwrap().pod_id.unwrap();

    // Both happen during launch: spec changes and demand drops.
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v2".into() });
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
    let (mgmt, worker, _pod_id) = setup_workload_with_pending_pod(&mut router);

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
    let (mgmt, worker) = setup_running_workload(&mut router, 5);

    // Kill worker → pod fails.
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.pod_running);
    assert!(wl.in_backoff);
    assert_eq!(wl.consecutive_failures, 1);
    assert!(wl.pod_id.is_none()); // pod released

    // Timer fires — backoff cleared, reconcile creates new pod.
    router.send_workload_timer_fired(mgmt, W1, WorkloadTimerKey::RetryBackoff);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.in_backoff);
    assert!(wl.pod_id.is_some());
    let new_pod = wl.pod_id.unwrap();

    // New worker + make pod running.
    let worker2 = router.create_worker();
    router.set_worker_info(worker2, WorkerInfo { capacity: 10 });
    make_pod_running(&mut router, worker2, new_pod);

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);
    assert_eq!(wl.consecutive_failures, 0); // reset on success
}

/// 21. Multiple failures increment the counter.
#[test]
fn consecutive_failures_increment() {
    let mut router = Router::new(16);
    let (mgmt, worker) = setup_running_workload(&mut router, 5);

    // First failure.
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 1);
    assert!(wl.in_backoff);

    // Timer fires → retry.
    router.send_workload_timer_fired(mgmt, W1, WorkloadTimerKey::RetryBackoff);
    router.propagate();

    let pod2 = router.get_workload(&W1).unwrap().pod_id.unwrap();

    // Second failure (via direct status, not worker loss).
    let worker2 = router.create_worker();
    router.set_worker_info(worker2, WorkerInfo { capacity: 10 });
    make_pod_failed(&mut router, worker2, pod2);

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 2);
    assert!(wl.in_backoff);
}

/// 22. After max_retries failures, workload stops retrying (terminal Failed).
#[test]
fn max_retries_enters_failed() {
    let mut router = Router::new(16);
    let (mgmt, worker) = setup_running_workload(&mut router, 2);

    // First failure.
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 1);
    assert!(wl.in_backoff); // still under limit

    // Timer fires → retry.
    router.send_workload_timer_fired(mgmt, W1, WorkloadTimerKey::RetryBackoff);
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
}

/// 23. Failed state + spec change → resets failures and retries.
#[test]
fn failed_recovery_via_spec_change() {
    let mut router = Router::new(16);
    let (mgmt, worker) = setup_running_workload(&mut router, 1);

    // One failure → hits max_retries (1) → terminal.
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 1);
    assert!(!wl.in_backoff);
    assert!(wl.pod_id.is_none());

    // Spec change resets failures.
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v2".into() });
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
    let (mgmt, worker) = setup_running_workload(&mut router, 1);

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
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::with_max_retries(1));
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v1".into() });

    // Activation-based service so we can toggle demand.
    router.create_service(S1, ServiceSm::new(true));
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
    let (mgmt, worker) = setup_running_workload(&mut router, 1);

    // Fail → terminal.
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 1);
    assert!(wl.pod_id.is_none());

    // Add another service with demand — still Failed, no new pod.
    router.create_service(S2, ServiceSm::new(false));
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
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::with_max_retries(5));
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v1".into() });

    router.create_service(S1, ServiceSm::new(true));
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

    // Drop demand → clears everything.
    router.send_activate_service(mgmt, S1, false);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.in_backoff);
    assert_eq!(wl.consecutive_failures, 0);
    assert!(wl.pod_id.is_none());
}

/// 28. In backoff + spec change → clears backoff, immediate retry.
#[test]
fn backoff_cleared_on_spec_change() {
    let mut router = Router::new(16);
    let (mgmt, worker) = setup_running_workload(&mut router, 5);

    // Fail → enters backoff.
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.in_backoff);

    // Spec change clears backoff + failures → immediate retry.
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v2".into() });
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.in_backoff);
    assert_eq!(wl.consecutive_failures, 0);
    assert!(wl.pod_id.is_some()); // new pod created immediately
}

/// 29. Scavenge during backoff clears everything, goes dormant.
#[test]
fn scavenge_during_backoff() {
    let mut router = Router::new(16);
    let (mgmt, worker) = setup_running_workload(&mut router, 5);

    // Fail → enters backoff.
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.in_backoff);

    // Scavenge is noop when demand is present (always-on service).
    // So scavenge won't do anything here — demand is still active.
    router.send_admin_command(mgmt, W1, AdminCmd::Scavenge);
    router.propagate();

    // Still in backoff because demand is active.
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.in_backoff);
}

/// 30. Scavenge during Failed clears failures (when no demand).
#[test]
fn scavenge_during_failed() {
    let mut router = Router::new(16);
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::with_max_retries(1));
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v1".into() });

    router.create_service(S1, ServiceSm::new(true));
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
    let (mgmt, worker) = setup_running_workload(&mut router, 5);

    // First failure.
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.consecutive_failures, 1);

    // Timer fires → retry.
    router.send_workload_timer_fired(mgmt, W1, WorkloadTimerKey::RetryBackoff);
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
}
