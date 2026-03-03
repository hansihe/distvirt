use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::net::Ipv4Addr;
use std::time::Duration;

use stateright::*;

use distvirt_orchestrator::namespace::NamespaceStateMachine;
use distvirt_orchestrator::types::*;

// --- Model Configuration ---

struct NamespaceModel {
    initial_spec: NamespaceSpec,
    worker_count: usize,
    enable_worker_failure: bool,
    enable_delete: bool,
}

// --- Hashable Snapshot of NamespaceStateMachine ---

/// Mirror of `NamespaceStateMachine` using BTreeMap/BTreeSet for deterministic
/// hashing and equality, enabling Stateright state deduplication.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct NamespaceSnapshot {
    namespace_id: NamespaceId,
    spec: SpecSnapshot,
    status: NamespaceStatus,
    workloads: BTreeMap<WorkloadId, WorkloadSnapshot>,
    services: BTreeMap<ServiceId, ServiceSnapshot>,
    service_workload: BTreeMap<ServiceId, WorkloadId>,
    pods: BTreeMap<PodId, PodInfo>,
    workers: BTreeMap<WorkerId, WorkerSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct WorkloadSnapshot {
    state: WorkloadState,
    demand_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ServiceSnapshot {
    state: ServiceState,
    workload_id: WorkloadId,
    has_activation: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SpecSnapshot {
    network: NetworkConfig,
    workloads: BTreeMap<WorkloadId, WorkloadSpec>,
    services: BTreeMap<ServiceId, ServiceSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct WorkerSnapshot {
    fabric_status: FabricStatus,
    pods: BTreeSet<PodId>,
}

impl NamespaceSnapshot {
    fn from_state_machine(sm: &NamespaceStateMachine) -> Self {
        NamespaceSnapshot {
            namespace_id: sm.namespace_id.clone(),
            spec: SpecSnapshot {
                network: sm.spec.network.clone(),
                workloads: sm
                    .spec
                    .workloads
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                services: sm
                    .spec
                    .services
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            },
            status: sm.status.clone(),
            workloads: sm
                .workloads
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        WorkloadSnapshot {
                            state: v.state.clone(),
                            demand_count: v.demand_count,
                        },
                    )
                })
                .collect(),
            services: sm
                .services
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        ServiceSnapshot {
                            state: v.state.clone(),
                            workload_id: v.workload_id.clone(),
                            has_activation: v.has_activation,
                        },
                    )
                })
                .collect(),
            service_workload: sm
                .service_workload
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            pods: sm
                .pods
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            workers: sm
                .workers
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        WorkerSnapshot {
                            fabric_status: v.fabric_status.clone(),
                            pods: v.pods.iter().cloned().collect(),
                        },
                    )
                })
                .collect(),
        }
    }

    fn to_state_machine(&self) -> NamespaceStateMachine {
        let spec = NamespaceSpec {
            network: self.spec.network.clone(),
            workloads: self
                .spec
                .workloads
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            services: self
                .spec
                .services
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        };

        let mut workloads = HashMap::new();
        for (wl_id, wl_snap) in &self.workloads {
            let mut wl = distvirt_orchestrator::workload::WorkloadStateMachine::new(wl_id.clone());
            wl.state = wl_snap.state.clone();
            wl.demand_count = wl_snap.demand_count;
            workloads.insert(wl_id.clone(), wl);
        }

        let mut services = HashMap::new();
        for (svc_id, svc_snap) in &self.services {
            let svc_spec = spec.services.get(svc_id);
            let idle_timeout = svc_spec
                .and_then(|s| s.activation.as_ref())
                .map(|a| a.idle_timeout)
                .unwrap_or(Duration::from_secs(30));
            let mut svc = distvirt_orchestrator::service::ServiceStateMachine::new(
                svc_id.clone(),
                svc_snap.workload_id.clone(),
                svc_snap.has_activation,
                idle_timeout,
            );
            svc.state = svc_snap.state.clone();
            services.insert(svc_id.clone(), svc);
        }

        let service_workload: HashMap<ServiceId, WorkloadId> = self
            .service_workload
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        NamespaceStateMachine {
            namespace_id: self.namespace_id.clone(),
            spec,
            status: self.status.clone(),
            workloads,
            services,
            service_workload,
            pods: self
                .pods
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            workers: self
                .workers
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        NamespaceWorkerState {
                            fabric_status: v.fabric_status.clone(),
                            pods: v.pods.iter().cloned().collect(),
                        },
                    )
                })
                .collect(),
            wg_peers: std::collections::HashMap::new(),
            wg_next_host_offset: 254,
        }
    }
}

// --- Model State ---

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ModelState {
    namespace: NamespaceSnapshot,
    pending_timers: BTreeSet<TimerKey>,
    /// Monotonic flag: set to true once any pod has been launched.
    /// Used only for reachability properties (false→true only, no divergence).
    ever_launched_pod: bool,
    /// Whether all worker commands in the last transition targeted workers
    /// present in the namespace's worker map at the time the commands were emitted.
    last_output_commands_valid: bool,
}

// --- Model Actions ---

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum ModelAction {
    WorkerEvent {
        worker_id: WorkerId,
        event: WorkerEvent,
    },
    TimerFired {
        timer_key: TimerKey,
    },
    WorkerLost {
        worker_id: WorkerId,
    },
    Delete,
}

// --- Helpers ---

fn worker_id(i: usize) -> WorkerId {
    WorkerId(format!("w-{}", i))
}

/// Allocate the lowest free pod ID, so states converge after pod churn.
fn next_free_pod_id(sm: &NamespaceStateMachine) -> PodId {
    for i in 0u64.. {
        let candidate = PodId(format!("pod-{}", i));
        if !sm.pods.contains_key(&candidate) {
            return candidate;
        }
    }
    unreachable!()
}

// --- Model Implementation ---

impl Model for NamespaceModel {
    type State = ModelState;
    type Action = ModelAction;

    fn init_states(&self) -> Vec<Self::State> {
        let mut sm =
            NamespaceStateMachine::new(NamespaceId("model-ns".into()), self.initial_spec.clone());

        // Pre-register workers with Creating fabric status.
        for i in 0..self.worker_count {
            sm.workers.insert(
                worker_id(i),
                NamespaceWorkerState {
                    fabric_status: FabricStatus::Creating,
                    pods: HashSet::new(),
                },
            );
        }

        let snapshot = NamespaceSnapshot::from_state_machine(&sm);
        vec![ModelState {
            namespace: snapshot,
            pending_timers: BTreeSet::new(),
            ever_launched_pod: false,
            last_output_commands_valid: true,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        let ns = &state.namespace;

        // Any pending timer can fire.
        for timer_key in &state.pending_timers {
            actions.push(ModelAction::TimerFired {
                timer_key: timer_key.clone(),
            });
        }

        // Worker events based on current workload/service states.
        for (service_id, svc_snap) in &ns.services {
            let wl_id = &svc_snap.workload_id;
            let wl_snap = ns.workloads.get(wl_id);

            for (wid, _worker) in &ns.workers {
                // Service-level events.
                match &svc_snap.state {
                    ServiceState::Idle => {
                        actions.push(ModelAction::WorkerEvent {
                            worker_id: wid.clone(),
                            event: WorkerEvent::ServiceActivation {
                                service_id: service_id.clone(),
                            },
                        });
                    }
                    ServiceState::Active { .. } => {
                        // Backend need can change from any worker.
                        for need in &[
                            BackendNeed::None,
                            BackendNeed::Traffic,
                            BackendNeed::Active,
                        ] {
                            actions.push(ModelAction::WorkerEvent {
                                worker_id: wid.clone(),
                                event: WorkerEvent::ServiceBackendNeed {
                                    service_id: service_id.clone(),
                                    need: need.clone(),
                                },
                            });
                        }
                    }
                    ServiceState::Pending | ServiceState::NeedBackend => {}
                }

                // Workload-level events.
                if let Some(wl) = wl_snap {
                    match &wl.state {
                        WorkloadState::Launching {
                            pod_id,
                            worker_id: launch_wid,
                            ..
                        } => {
                            if wid.0 == launch_wid.0 {
                                actions.push(ModelAction::WorkerEvent {
                                    worker_id: wid.clone(),
                                    event: WorkerEvent::PodRunning {
                                        pod_id: pod_id.clone(),
                                    },
                                });
                                actions.push(ModelAction::WorkerEvent {
                                    worker_id: wid.clone(),
                                    event: WorkerEvent::PodFailed {
                                        pod_id: pod_id.clone(),
                                        error: "model check failure".into(),
                                    },
                                });
                            }
                        }
                        WorkloadState::Running {
                            pod_id,
                            worker_id: active_wid,
                            ..
                        } => {
                            if wid.0 == active_wid.0 {
                                actions.push(ModelAction::WorkerEvent {
                                    worker_id: wid.clone(),
                                    event: WorkerEvent::PodExited {
                                        pod_id: pod_id.clone(),
                                        exit_code: 0,
                                    },
                                });
                            }
                        }
                        WorkloadState::WaitingForCapacity | WorkloadState::Dormant => {}
                    }
                }
            }
        }

        // NamespaceCreated event from any worker in Creating status.
        for (wid, worker) in &ns.workers {
            if worker.fabric_status == FabricStatus::Creating {
                actions.push(ModelAction::WorkerEvent {
                    worker_id: wid.clone(),
                    event: WorkerEvent::NamespaceCreated,
                });
            }
            // NamespaceDestroyed event from any worker in Destroying status.
            if worker.fabric_status == FabricStatus::Destroying {
                actions.push(ModelAction::WorkerEvent {
                    worker_id: wid.clone(),
                    event: WorkerEvent::NamespaceDestroyed,
                });
            }
        }

        // Worker can disconnect (if enabled).
        if self.enable_worker_failure {
            for (wid, _) in &ns.workers {
                actions.push(ModelAction::WorkerLost {
                    worker_id: wid.clone(),
                });
            }
        }

        // Delete action (if enabled and not already destroying/destroyed).
        if self.enable_delete
            && ns.status != NamespaceStatus::Destroying
            && !ns.workers.is_empty()
        {
            actions.push(ModelAction::Delete);
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut sm = state.namespace.to_state_machine();

        let input = match action {
            ModelAction::WorkerEvent { worker_id, event } => {
                NamespaceInput::WorkerEvent { worker_id, event }
            }
            ModelAction::TimerFired { timer_key } => NamespaceInput::TimerFired { timer_key },
            ModelAction::WorkerLost { worker_id } => NamespaceInput::WorkerLost { worker_id },
            ModelAction::Delete => NamespaceInput::Delete {
                client_id: ClientId(0),
            },
        };

        // Snapshot pre-step workers for command validity check.
        let pre_step_workers: BTreeSet<WorkerId> = sm.workers.keys().cloned().collect();

        let output = sm.step(input);

        // Check that all worker commands target workers present pre-step.
        let mut commands_valid = output
            .worker_commands
            .iter()
            .all(|(wid, _)| pre_step_workers.contains(wid));

        // Update pending timers from output.
        let mut pending_timers = state.pending_timers.clone();
        for (timer_key, _duration) in &output.timers_set {
            pending_timers.insert(timer_key.clone());
        }
        for timer_key in &output.timers_cancel {
            pending_timers.remove(timer_key);
        }

        // Process pod_requests: simulate outer-layer scheduling.
        let mut ever_launched_pod = state.ever_launched_pod;
        for req in &output.pod_requests {
            // Pick the lowest active worker ID for deterministic scheduling.
            let active_worker = sm
                .workers
                .iter()
                .filter(|(_, ws)| ws.fabric_status == FabricStatus::Active)
                .map(|(wid, _)| wid.clone())
                .min();

            if let Some(wid) = active_worker {
                // Allocate lowest free pod ID for state convergence.
                let pod_id = next_free_pod_id(&sm);
                ever_launched_pod = true;

                let launch_out = sm.step(NamespaceInput::LaunchPod {
                    workload_id: req.workload_id.clone(),
                    worker_id: wid,
                    pod_id,
                });

                commands_valid = commands_valid
                    && launch_out
                        .worker_commands
                        .iter()
                        .all(|(wid, _)| sm.workers.contains_key(wid));
                for (timer_key, _duration) in &launch_out.timers_set {
                    pending_timers.insert(timer_key.clone());
                }
                for timer_key in &launch_out.timers_cancel {
                    pending_timers.remove(timer_key);
                }
            }
        }

        Some(ModelState {
            namespace: NamespaceSnapshot::from_state_machine(&sm),
            pending_timers,
            ever_launched_pod,
            last_output_commands_valid: commands_valid,
        })
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut props = vec![
            // Safety: No commands sent to workers not in namespace's worker map.
            Property::<Self>::always("no commands to unknown workers", |_model, state| {
                let ns = &state.namespace;
                for (_wl_id, wl) in &ns.workloads {
                    match &wl.state {
                        WorkloadState::Launching { worker_id, .. }
                        | WorkloadState::Running { worker_id, .. } => {
                            if !ns.workers.contains_key(worker_id) {
                                return false;
                            }
                        }
                        _ => {}
                    }
                }
                true
            }),
            // Safety: Launching/Running workloads reference valid pods.
            Property::<Self>::always("workloads have valid pods", |_model, state| {
                let ns = &state.namespace;
                for (_wl_id, wl) in &ns.workloads {
                    match &wl.state {
                        WorkloadState::Launching { pod_id, .. }
                        | WorkloadState::Running { pod_id, .. } => {
                            if !ns.pods.contains_key(pod_id) {
                                return false;
                            }
                        }
                        _ => {}
                    }
                }
                true
            }),
            // Safety: No duplicate pods per workload.
            Property::<Self>::always("no duplicate pods per workload", |_model, state| {
                let ns = &state.namespace;
                let mut workload_pods: BTreeMap<&WorkloadId, Vec<&PodId>> = BTreeMap::new();
                for (pod_id, pod_info) in &ns.pods {
                    workload_pods
                        .entry(&pod_info.workload_id)
                        .or_default()
                        .push(pod_id);
                }
                for (_wlid, pods) in &workload_pods {
                    let unique: BTreeSet<&&PodId> = pods.iter().collect();
                    if unique.len() != pods.len() {
                        return false;
                    }
                }
                true
            }),
            // Safety: Workloads only reference workers that are present.
            Property::<Self>::always("workloads only reference present workers", |_model, state| {
                let ns = &state.namespace;
                for (_wl_id, wl) in &ns.workloads {
                    match &wl.state {
                        WorkloadState::Launching { worker_id, .. }
                        | WorkloadState::Running { worker_id, .. } => {
                            if !ns.workers.contains_key(worker_id) {
                                return false;
                            }
                        }
                        _ => {}
                    }
                }
                true
            }),
            // Safety: No worker commands to absent workers (checked against
            // pre-transition worker set during next_state).
            Property::<Self>::always("no worker commands to absent workers", |_model, state| {
                state.last_output_commands_valid
            }),
            // Reachability: Can reach a Running workload state.
            Property::<Self>::sometimes("can reach active service", |_model, state| {
                state
                    .namespace
                    .workloads
                    .values()
                    .any(|w| matches!(w.state, WorkloadState::Running { .. }))
            }),
            // Reachability: Can reach Idle after Active (idle timeout scale-down).
            Property::<Self>::sometimes("can reach idle after active", |_model, state| {
                state.ever_launched_pod
                    && state
                        .namespace
                        .services
                        .values()
                        .any(|s| matches!(s.state, ServiceState::Idle))
            }),
        ];

        if self.enable_delete {
            // Fire-and-forget: destroy is immediate.
            props.push(Property::<Self>::sometimes(
                "can reach destroyed",
                |_model, state| {
                    state.namespace.status == NamespaceStatus::Destroying
                        && state.namespace.workers.is_empty()
                },
            ));
        }

        props
    }
}

// --- Test Helpers ---

fn test_network_config() -> NetworkConfig {
    NetworkConfig {
        subnet: Ipv4Addr::new(172, 16, 0, 0),
        gateway: Ipv4Addr::new(172, 16, 0, 1),
        prefix_len: 24,
    }
}

fn test_pod_network_config() -> PodNetworkConfig {
    PodNetworkConfig {
        ip: Ipv4Addr::new(172, 16, 0, 10),
        mac: [0x02, 0x00, 0x00, 0x00, 0x00, 0x10],
        gateway: Ipv4Addr::new(172, 16, 0, 1),
        netmask: "255.255.255.0".into(),
    }
}

fn test_service_policy() -> ServicePolicy {
    ServicePolicy {
        buffer_frames: 100,
        timeout_ms: 5000,
        activator: None,
    }
}

fn test_container_spec() -> ContainerSpec {
    ContainerSpec {
        container_id: "main".into(),
        image_ref: "test:latest".into(),
        config: ContainerConfig {
            entrypoint: "/bin/sh".into(),
            args: vec![],
            env: vec![],
            working_dir: None,
            uid: None,
            gid: None,
            hostname: None,
            capture_output: false,
            stdin: false,
        },
    }
}

fn single_service_spec() -> NamespaceSpec {
    let mut workloads = HashMap::new();
    workloads.insert(
        WorkloadId("svc-1".into()),
        WorkloadSpec {
            containers: vec![test_container_spec()],
            network: test_pod_network_config(),
        },
    );
    let mut services = HashMap::new();
    services.insert(
        ServiceId("svc-1".into()),
        ServiceSpec {
            workload_id: WorkloadId("svc-1".into()),
            ip: Ipv4Addr::new(172, 16, 0, 100),
            mac: [0x02, 0x00, 0x00, 0x00, 0x01, 0x00],
            policy: test_service_policy(),
            activation: Some(ActivationSpec {
                idle_timeout: Duration::from_secs(30),
            }),
        },
    );
    NamespaceSpec {
        network: test_network_config(),
        workloads,
        services,
    }
}

fn two_service_spec() -> NamespaceSpec {
    let mut workloads = HashMap::new();
    workloads.insert(
        WorkloadId("svc-1".into()),
        WorkloadSpec {
            containers: vec![test_container_spec()],
            network: test_pod_network_config(),
        },
    );
    workloads.insert(
        WorkloadId("svc-2".into()),
        WorkloadSpec {
            containers: vec![test_container_spec()],
            network: PodNetworkConfig {
                ip: Ipv4Addr::new(172, 16, 0, 11),
                mac: [0x02, 0x00, 0x00, 0x00, 0x00, 0x11],
                gateway: Ipv4Addr::new(172, 16, 0, 1),
                netmask: "255.255.255.0".into(),
            },
        },
    );
    let mut services = HashMap::new();
    services.insert(
        ServiceId("svc-1".into()),
        ServiceSpec {
            workload_id: WorkloadId("svc-1".into()),
            ip: Ipv4Addr::new(172, 16, 0, 100),
            mac: [0x02, 0x00, 0x00, 0x00, 0x01, 0x00],
            policy: test_service_policy(),
            activation: Some(ActivationSpec {
                idle_timeout: Duration::from_secs(30),
            }),
        },
    );
    services.insert(
        ServiceId("svc-2".into()),
        ServiceSpec {
            workload_id: WorkloadId("svc-2".into()),
            ip: Ipv4Addr::new(172, 16, 0, 101),
            mac: [0x02, 0x00, 0x00, 0x00, 0x01, 0x01],
            policy: test_service_policy(),
            activation: None,
        },
    );
    NamespaceSpec {
        network: test_network_config(),
        workloads,
        services,
    }
}

// --- Tests ---

/// Helper to run a model check with a given depth and print results.
fn run_check(name: &str, model: NamespaceModel, max_depth: usize) {
    let result = model
        .checker()
        .target_max_depth(max_depth)
        .spawn_dfs()
        .join();

    result.assert_properties();
    println!(
        "{}: {} unique states explored (depth={})",
        name,
        result.unique_state_count(),
        max_depth,
    );
}

#[test]
fn check_single_service_activation() {
    run_check(
        "Single service activation",
        NamespaceModel {
            initial_spec: single_service_spec(),
            worker_count: 1,
            enable_worker_failure: false,
            enable_delete: false,
        },
        100,
    );
}

#[test]
fn check_two_services() {
    run_check(
        "Two services",
        NamespaceModel {
            initial_spec: two_service_spec(),
            worker_count: 1,
            enable_worker_failure: false,
            enable_delete: false,
        },
        100,
    );
}

#[test]
fn check_activation_with_worker_failure() {
    run_check(
        "Activation with worker failure",
        NamespaceModel {
            initial_spec: single_service_spec(),
            worker_count: 2,
            enable_worker_failure: true,
            enable_delete: false,
        },
        50,
    );
}

#[test]
fn check_two_workers_two_services() {
    run_check(
        "Two workers, two services",
        NamespaceModel {
            initial_spec: two_service_spec(),
            worker_count: 2,
            enable_worker_failure: true,
            enable_delete: false,
        },
        50,
    );
}

#[test]
fn check_delete_lifecycle() {
    run_check(
        "Delete lifecycle",
        NamespaceModel {
            initial_spec: single_service_spec(),
            worker_count: 1,
            enable_worker_failure: false,
            enable_delete: true,
        },
        100,
    );
}

#[test]
fn check_delete_with_worker_failure() {
    run_check(
        "Delete with worker failure",
        NamespaceModel {
            initial_spec: two_service_spec(),
            worker_count: 2,
            enable_worker_failure: true,
            enable_delete: true,
        },
        50,
    );
}
