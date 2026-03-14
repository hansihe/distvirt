//! Integration test: orchestrator-like scenario using the signal router.
//!
//! Models the service → workload → pod lifecycle with worker ports and
//! management ports, exercising demand aggregation, reactive edge creation,
//! readiness propagation, and worker loss via port removal.
//!
//! ## Topology
//!
//! ```text
//! Management ──spec──▶ Service ──demand──▶ Workload ──pod_spec──▶ Pod
//! Management ──spec──▶ Workload              ◀──status──┘         ▲
//!                      Workload ──ready──▶ Service           Worker (port)
//! ```
//!
//! ## Limitations (current router API)
//!
//! SM creation is only possible on the Router, not from within a handler.
//! The workload SM can't directly create Pod SMs — instead it signals that
//! it needs a pod (via `NeedsPod` signal), and external "scheduler" code
//! reads this and creates the pod + edges. This is a known friction point
//! that will be addressed when handler-driven SM creation is added.

use distvirt_sm_router::{Aggregator, ListAggregator, SmHandler};

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
    ForceDeactivate,
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
                        // Demand stays true — we're active.
                    }
                    (ServiceState::Active { .. }, None) => {
                        // Backend went away. Go back to NeedBackend.
                        self.state = ServiceState::NeedBackend;
                    }
                    (ServiceState::Active { .. }, Some(info)) => {
                        // Readiness info updated (e.g. new pod after restart).
                        self.state = ServiceState::Active { ready: info };
                    }
                    _ => {}
                }
            }
            ServiceInput::SvcSpecInput(specs) => {
                if let Some(spec) = specs.into_iter().next() {
                    self.has_activation = spec.has_activation;
                    // Set initial demand based on activation mode.
                    if !self.has_activation {
                        ctx.set_demand(true);
                        if matches!(self.state, ServiceState::Idle) {
                            self.state = ServiceState::NeedBackend;
                        }
                    }
                    // Set edge to target workload.
                    ctx.set_service_to_workload_edges(vec![spec.workload]);
                }
            }
        }
    }
}

// ---- Workload SM ----

struct WorkloadSm {
    has_spec: bool,
    has_demand: bool,
    pod_running: bool,
}

impl WorkloadSm {
    fn new() -> Self {
        WorkloadSm {
            has_spec: false,
            has_demand: false,
            pod_running: false,
        }
    }
}

impl SmHandler for WorkloadSm {
    type Input = WorkloadInput;
    type Ctx = WorkloadCtx;

    fn handle(&mut self, input: Self::Input, ctx: &mut Self::Ctx) {
        match input {
            WorkloadInput::DemandInput(demand) => {
                self.has_demand = demand.demand_count > 0;
                // Always retarget readiness edges to the full service set.
                ctx.set_workload_to_service_edges(demand.service_ids);
                self.reconcile(ctx);
            }
            WorkloadInput::SpecInput(specs) => {
                self.has_spec = specs.into_iter().next().is_some();
                self.reconcile(ctx);
            }
            WorkloadInput::PodStatusInput(statuses) => {
                let was_running = self.pod_running;
                self.pod_running = statuses.iter().any(|s| *s == PodStatus::Running);
                if self.pod_running && !was_running {
                    // Pod just became ready — set readiness signal.
                    // In real code we'd get pod_id/worker_id from the pod;
                    // here we use placeholders since the test drives this externally.
                    ctx.set_readiness(Some(ReadyInfo {
                        pod_id: PodId(0),   // placeholder
                        worker_id: WorkerId(0), // placeholder
                    }));
                } else if !self.pod_running && was_running {
                    ctx.set_readiness(None);
                }
                self.reconcile(ctx);
            }
            WorkloadInput::AdminCommand(cmd) => {
                match cmd {
                    AdminCmd::ForceDeactivate => {
                        self.has_demand = false;
                        ctx.set_readiness(None);
                        ctx.set_needs_pod(false);
                    }
                    AdminCmd::Restart => {
                        // Signal that we need a new pod (scheduler will handle it).
                        self.pod_running = false;
                        ctx.set_readiness(None);
                        self.reconcile(ctx);
                    }
                }
            }
        }
    }
}

impl WorkloadSm {
    fn reconcile(&self, ctx: &mut WorkloadCtx) {
        let want_pod = self.has_spec && self.has_demand;
        ctx.set_needs_pod(want_pod);
        if !want_pod {
            // If we don't want a pod, clear readiness.
            if !self.has_demand {
                ctx.set_readiness(None);
            }
        }
    }
}

// ---- Pod SM ----

struct PodSm {
    status: PodStatus,
}

impl PodSm {
    fn new() -> Self {
        PodSm {
            status: PodStatus::Pending,
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
                // Worker assigned — in real code this would trigger launch.
                // Tests will set status externally via router.set_pod_status().
            }
        }
    }
}

// ============================================================================
// Test helpers
// ============================================================================

/// External "scheduler" that reads the workload's NeedsPod signal and
/// creates/destroys pods. Since SM creation can't happen in handlers,
/// this simulates the external loop.
struct Scheduler {
    /// Tracks which workload has which pod (if any).
    workload_pods: std::collections::HashMap<WorkloadId, (PodId, WorkerId)>,
}

impl Scheduler {
    fn new() -> Self {
        Scheduler {
            workload_pods: std::collections::HashMap::new(),
        }
    }

    /// Check workload NeedsPod signals and create/destroy pods as needed.
    /// Returns true if any changes were made (caller should propagate again).
    fn tick(&mut self, router: &mut Router, worker: WorkerId) -> bool {
        // Collect workloads and their NeedsPod signals.
        let workloads: Vec<(WorkloadId, bool)> = router
            .workload_needs_pod
            .iter()
            .map(|(wid, needs)| (*wid, *needs))
            .collect();

        let mut changed = false;
        for (wid, needs_pod) in workloads {
            if needs_pod && !self.workload_pods.contains_key(&wid) {
                // Create pod.
                let pod_id = router.create_pod(PodSm::new());
                router.set_workload_to_pod_edges(wid, vec![pod_id]);
                router.set_pod_to_workload_edges(pod_id, vec![wid]);
                router.set_worker_to_pod_edges(worker, vec![pod_id]);
                self.workload_pods.insert(wid, (pod_id, worker));
                changed = true;
            } else if !needs_pod {
                if let Some((pod_id, _worker_id)) = self.workload_pods.remove(&wid) {
                    router.set_workload_to_pod_edges(wid, vec![]);
                    router.destroy_pod(pod_id);
                    changed = true;
                }
            }
        }
        changed
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

/// 1. Demand aggregation: 3 services → 1 workload, toggle demand.
#[test]
fn demand_aggregation() {
    let mut router = Router::new(16);
    router.create_workload(W1, WorkloadSm::new());
    router.create_service(S1, ServiceSm::new(true));
    router.create_service(S2, ServiceSm::new(true));
    router.create_service(S3, ServiceSm::new(true));

    // Connect services to workload.
    router.set_service_to_workload_edges(S1, vec![W1]);
    router.set_service_to_workload_edges(S2, vec![W1]);
    router.set_service_to_workload_edges(S3, vec![W1]);
    router.propagate();

    // No demand yet.
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.has_demand);

    // S1 activates.
    router.set_service_demand(S1, true);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_demand);

    // S2 also activates.
    router.set_service_demand(S2, true);
    router.propagate();

    // Still has demand.
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_demand);

    // Both deactivate.
    router.set_service_demand(S1, false);
    router.set_service_demand(S2, false);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.has_demand);
}

/// 2. Reactive readiness edges: workload creates WorkloadToService edges
///    based on which services point at it, then readiness propagates back.
#[test]
fn reactive_readiness_edges() {
    let mut router = Router::new(16);
    router.create_workload(W1, WorkloadSm::new());
    router.create_service(S1, ServiceSm::new(false)); // always-on
    router.create_service(S2, ServiceSm::new(false));

    // Connect services → workload.
    router.set_service_to_workload_edges(S1, vec![W1]);
    router.set_service_to_workload_edges(S2, vec![W1]);
    // Always-on services set demand=true.
    router.set_service_demand(S1, true);
    router.set_service_demand(S2, true);
    router.propagate();

    // Workload handler should have created WorkloadToService edges reactively.
    let wl_edges = router.workload_to_service_fwd.get(&W1).unwrap();
    assert!(wl_edges.contains(&S1));
    assert!(wl_edges.contains(&S2));

    // Both services should have received readiness=None (workload not ready yet).
    let s1 = router.get_service(&S1).unwrap();
    assert_eq!(s1.state, ServiceState::NeedBackend);

    // Simulate readiness.
    router.set_workload_readiness(
        W1,
        Some(ReadyInfo {
            pod_id: PodId(10),
            worker_id: WorkerId(20),
        }),
    );
    router.propagate();

    // Both services should be active now.
    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));
    let s2 = router.get_service(&S2).unwrap();
    assert!(matches!(s2.state, ServiceState::Active { .. }));

    // Add a third service — it should immediately get readiness.
    router.create_service(S3, ServiceSm::new(false));
    router.set_service_to_workload_edges(S3, vec![W1]);
    router.set_service_demand(S3, true);
    router.propagate();

    // Workload re-aggregated demand and re-targeted readiness edges.
    let wl_edges = router.workload_to_service_fwd.get(&W1).unwrap();
    assert!(wl_edges.contains(&S3));

    let s3 = router.get_service(&S3).unwrap();
    assert!(matches!(s3.state, ServiceState::Active { .. }));
}

/// 3. Pod lifecycle through signals: workload creates pod via scheduler,
///    pod status flows back, readiness propagates to services.
#[test]
fn pod_lifecycle() {
    let mut router = Router::new(16);
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    // Set up management port for workload spec.
    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new());
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "test:latest".into() });
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_spec);
    assert!(!wl.has_demand);

    // Add a service with demand.
    router.create_service(S1, ServiceSm::new(false));
    router.set_service_to_workload_edges(S1, vec![W1]);
    router.set_service_demand(S1, true);
    router.propagate();

    // Workload should signal NeedsPod=true.
    assert_eq!(*router.workload_needs_pod.get(&W1).unwrap(), true);

    // External scheduler creates the pod.
    let mut scheduler = Scheduler::new();
    let changed = scheduler.tick(&mut router, worker);
    assert!(changed);
    router.propagate();

    // Pod is pending — workload sees PodStatus::Pending.
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.pod_running);

    // Simulate pod becoming Running.
    let (pod_id, _) = scheduler.workload_pods[&W1];
    router.set_pod_status(pod_id, PodStatus::Running);
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
    router.set_service_to_workload_edges(S1, vec![W1]);
    router.set_service_demand(S1, true);
    router.propagate();

    // Create pod via scheduler.
    let mut scheduler = Scheduler::new();
    scheduler.tick(&mut router, worker);
    router.propagate();

    // Pod starts running.
    let (pod_id, _) = scheduler.workload_pods[&W1];
    router.set_pod_status(pod_id, PodStatus::Running);
    router.propagate();

    // Verify everything is active.
    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));

    // Worker dies — remove the port.
    router.destroy_worker(worker);
    router.propagate();

    // Pod should have seen empty WorkerInput and set status=Failed.
    let pod = router.get_pod(&pod_id).unwrap();
    assert_eq!(pod.status, PodStatus::Failed);

    // Workload should have lost readiness.
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.pod_running);

    // Service should be back to NeedBackend.
    let s1 = router.get_service(&S1).unwrap();
    assert_eq!(s1.state, ServiceState::NeedBackend);
}

/// 5. Spec delivery via management port: init and update use same path.
#[test]
fn spec_via_management_port() {
    let mut router = Router::new(16);
    router.create_workload(W1, WorkloadSm::new());

    // Create management port with initial spec.
    let mgmt = router.create_management();
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "v1".into() });
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_spec);

    // Update spec — same code path, just a signal change.
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "v2".into() });
    router.propagate();

    // Still has spec (no "init vs update" distinction).
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

    // Service should have reactively created ServiceToWorkload edge.
    let edges = router.service_to_workload_fwd.get(&S1).unwrap();
    assert!(edges.contains(&W1));

    // Since has_activation=false, service should have set demand=true.
    let demand = router.service_demand.get(&S1).unwrap();
    assert_eq!(*demand, true);

    // And workload should have received the demand.
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_demand);
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
    router.set_service_to_workload_edges(S1, vec![W1]);
    router.set_service_demand(S1, true);
    router.propagate();

    // Boot a pod.
    let mut scheduler = Scheduler::new();
    scheduler.tick(&mut router, worker);
    router.propagate();
    let (pod_id, _) = scheduler.workload_pods[&W1];
    router.set_pod_status(pod_id, PodStatus::Running);
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

/// 8. Full end-to-end: service activation → demand → pod boot → readiness →
///    service active → worker dies → readiness lost → service back to NeedBackend.
#[test]
fn full_end_to_end() {
    let mut router = Router::new(16);

    // Infrastructure.
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });

    // Management setup.
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
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_spec);
    assert!(wl.has_demand); // S1 always-on → demand=true

    // S2 (activation) is idle — no demand yet.
    let _s2 = router.get_service(&S2).unwrap();

    // Scheduler creates pod.
    let mut scheduler = Scheduler::new();
    scheduler.tick(&mut router, worker);
    router.propagate();

    let (pod_id, _) = scheduler.workload_pods[&W1];

    // Pod starts running.
    router.set_pod_status(pod_id, PodStatus::Running);
    router.propagate();

    // S1 should be active (always-on, backend ready).
    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));

    // S2 should still be Idle (has_activation=true, no demand set).
    let s2 = router.get_service(&S2).unwrap();
    // S2 received readiness but is Idle — it doesn't transition because
    // it hasn't activated (demand is false). Readiness is just info.
    // Actually, the S2 handler transitions to Active on ReadinessInput only from NeedBackend.
    // From Idle, it stays Idle. This is correct for activation-based services.
    assert_eq!(s2.state, ServiceState::Idle);

    // Activate S2.
    router.set_service_demand(S2, true);
    router.propagate();

    // NOTE: Design finding — S2 stays Idle even after demand=true is set externally,
    // because the service SM doesn't receive its own demand signal. It only transitions
    // to NeedBackend via SvcSpecInput (which already fired). In the router model,
    // activation-based services need a way to transition internal state when external
    // code sets demand. This is a modeling gap to address in the real implementation.

    // Worker dies.
    router.destroy_worker(worker);
    router.propagate();

    // Pod goes Failed.
    let pod = router.get_pod(&pod_id).unwrap();
    assert_eq!(pod.status, PodStatus::Failed);

    // Workload loses readiness.
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.pod_running);

    // S1 goes back to NeedBackend.
    let s1 = router.get_service(&S1).unwrap();
    assert_eq!(s1.state, ServiceState::NeedBackend);

    // Workload still wants a pod (demand still >0).
    assert_eq!(*router.workload_needs_pod.get(&W1).unwrap(), true);
}
