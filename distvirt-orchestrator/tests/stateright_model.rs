use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::Hash;
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
    max_steps: usize,
}

// --- Hashable Snapshot of NamespaceStateMachine ---

/// Mirror of `NamespaceStateMachine` using BTreeMap/BTreeSet for deterministic
/// hashing and equality, enabling Stateright state deduplication.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct NamespaceSnapshot {
    namespace_id: NamespaceId,
    spec: SpecSnapshot,
    status: NamespaceStatus,
    services: BTreeMap<ServiceId, ServiceState>,
    pods: BTreeMap<PodId, PodInfo>,
    workers: BTreeMap<WorkerId, WorkerSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SpecSnapshot {
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
                services: sm
                    .spec
                    .services
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            },
            status: sm.status.clone(),
            services: sm
                .services
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
        NamespaceStateMachine {
            namespace_id: self.namespace_id.clone(),
            spec: NamespaceSpec {
                services: self
                    .spec
                    .services
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            },
            status: self.status.clone(),
            services: self
                .services
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
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
        }
    }
}

// --- Model State ---

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ModelState {
    namespace: NamespaceSnapshot,
    pending_timers: BTreeSet<TimerKey>,
    next_pod_id: u64,
    step_count: usize,
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

// --- Helper: generate worker IDs ---

fn worker_id(i: usize) -> WorkerId {
    WorkerId(format!("w-{}", i))
}

// --- Model Implementation ---

impl Model for NamespaceModel {
    type State = ModelState;
    type Action = ModelAction;

    fn init_states(&self) -> Vec<Self::State> {
        let mut sm =
            NamespaceStateMachine::new(NamespaceId("model-ns".into()), self.initial_spec.clone());

        // Pre-register workers with Creating fabric status.
        // The model will explore NamespaceCreated events to transition to Active.
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
            next_pod_id: 0,
            step_count: 0,
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

        // Worker events based on current service states.
        for (service_id, service_state) in &ns.services {
            for (wid, _worker) in &ns.workers {
                match service_state {
                    ServiceState::Pending => {
                        // Worker can report ServiceCreated.
                        actions.push(ModelAction::WorkerEvent {
                            worker_id: wid.clone(),
                            event: WorkerEvent::ServiceCreated {
                                service_id: service_id.clone(),
                            },
                        });
                    }
                    ServiceState::Idle => {
                        // Service can receive activation request.
                        actions.push(ModelAction::WorkerEvent {
                            worker_id: wid.clone(),
                            event: WorkerEvent::ServiceActivation {
                                service_id: service_id.clone(),
                            },
                        });
                    }
                    ServiceState::Launching {
                        pod_id,
                        worker_id: launch_wid,
                        ..
                    } => {
                        // Pod can report running (only from the launching worker).
                        if wid == launch_wid {
                            actions.push(ModelAction::WorkerEvent {
                                worker_id: wid.clone(),
                                event: WorkerEvent::PodRunning {
                                    pod_id: pod_id.clone(),
                                },
                            });
                            // Pod can fail.
                            actions.push(ModelAction::WorkerEvent {
                                worker_id: wid.clone(),
                                event: WorkerEvent::PodFailed {
                                    pod_id: pod_id.clone(),
                                    reason: "model check failure".into(),
                                },
                            });
                        }
                    }
                    ServiceState::Active {
                        pod_id,
                        worker_id: active_wid,
                        ..
                    } => {
                        // Pod can exit (only from the active worker).
                        if wid == active_wid {
                            actions.push(ModelAction::WorkerEvent {
                                worker_id: wid.clone(),
                                event: WorkerEvent::PodExited {
                                    pod_id: pod_id.clone(),
                                },
                            });
                            // Backend need can change.
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
                    }
                    ServiceState::WaitingForCapacity => {
                        // Outer layer can inject LaunchPod — handled in next_state
                        // by processing pod_requests from output.
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
        }

        // NamespaceDestroyed event from any worker (in Destroying namespace).
        if ns.status == NamespaceStatus::Destroying {
            for (wid, _) in &ns.workers {
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
        let mut next_pod_id = state.next_pod_id;

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

        let output = sm.step(input);

        // Update pending timers from output.
        let mut pending_timers = state.pending_timers.clone();
        for (timer_key, _duration) in &output.timers_set {
            pending_timers.insert(timer_key.clone());
        }
        for timer_key in &output.timers_cancel {
            pending_timers.remove(timer_key);
        }

        // Process pod_requests: simulate outer-layer scheduling.
        // Pick first active worker for each request.
        for req in &output.pod_requests {
            let active_worker = sm
                .workers
                .iter()
                .find(|(_, ws)| ws.fabric_status == FabricStatus::Active)
                .map(|(wid, _)| wid.clone());

            if let Some(wid) = active_worker {
                let pod_id = PodId(format!("pod-{}", next_pod_id));
                next_pod_id += 1;

                let launch_out = sm.step(NamespaceInput::LaunchPod {
                    service_id: req.service_id.clone(),
                    worker_id: wid,
                    pod_id,
                });

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
            next_pod_id,
            step_count: state.step_count + 1,
        })
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.step_count <= self.max_steps
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut props = vec![
            // Safety: No commands sent to workers not in namespace's worker map.
            Property::<Self>::always("no commands to unknown workers", |_model, state| {
                let ns = &state.namespace;
                for (_sid, svc) in &ns.services {
                    match svc {
                        ServiceState::Launching { worker_id, .. }
                        | ServiceState::Active { worker_id, .. } => {
                            if !ns.workers.contains_key(worker_id) {
                                return false;
                            }
                        }
                        _ => {}
                    }
                }
                true
            }),
            // Safety: Active/Launching services reference valid pods.
            Property::<Self>::always("active services have valid pods", |_model, state| {
                let ns = &state.namespace;
                for (_sid, svc) in &ns.services {
                    match svc {
                        ServiceState::Launching { pod_id, .. }
                        | ServiceState::Active { pod_id, .. } => {
                            if !ns.pods.contains_key(pod_id) {
                                return false;
                            }
                        }
                        _ => {}
                    }
                }
                true
            }),
            // Safety: No duplicate pods per service.
            Property::<Self>::always("no duplicate pods per service", |_model, state| {
                let ns = &state.namespace;
                let mut service_pods: BTreeMap<&ServiceId, Vec<&PodId>> = BTreeMap::new();
                for (pod_id, pod_info) in &ns.pods {
                    service_pods
                        .entry(&pod_info.service_id)
                        .or_default()
                        .push(pod_id);
                }
                for (_sid, pods) in &service_pods {
                    let unique: BTreeSet<&&PodId> = pods.iter().collect();
                    if unique.len() != pods.len() {
                        return false;
                    }
                }
                true
            }),
            // Safety: Services only reference workers that are present.
            Property::<Self>::always("services only reference present workers", |_model, state| {
                let ns = &state.namespace;
                for (_sid, svc) in &ns.services {
                    match svc {
                        ServiceState::Launching { worker_id, .. }
                        | ServiceState::Active { worker_id, .. } => {
                            if !ns.workers.contains_key(worker_id) {
                                return false;
                            }
                        }
                        _ => {}
                    }
                }
                true
            }),
            // Reachability: Can reach an Active service state.
            Property::<Self>::sometimes("can reach active service", |_model, state| {
                state
                    .namespace
                    .services
                    .values()
                    .any(|s| matches!(s, ServiceState::Active { .. }))
            }),
            // Reachability: Can reach Idle after Active (idle timeout scale-down).
            Property::<Self>::sometimes("can reach idle after active", |_model, state| {
                state.next_pod_id > 0
                    && state
                        .namespace
                        .services
                        .values()
                        .any(|s| matches!(s, ServiceState::Idle))
            }),
        ];

        if self.enable_delete {
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

fn single_service_spec() -> NamespaceSpec {
    let mut services = HashMap::new();
    services.insert(
        ServiceId("svc-1".into()),
        ServiceSpec {
            image: "test:latest".into(),
            activation: Some(ActivationSpec {
                idle_timeout: Duration::from_secs(30),
            }),
        },
    );
    NamespaceSpec { services }
}

fn two_service_spec() -> NamespaceSpec {
    let mut services = HashMap::new();
    services.insert(
        ServiceId("svc-1".into()),
        ServiceSpec {
            image: "test:latest".into(),
            activation: Some(ActivationSpec {
                idle_timeout: Duration::from_secs(30),
            }),
        },
    );
    services.insert(
        ServiceId("svc-2".into()),
        ServiceSpec {
            image: "test:latest".into(),
            activation: None,
        },
    );
    NamespaceSpec { services }
}

// --- Tests ---

#[test]
fn check_single_service_activation() {
    let result = NamespaceModel {
        initial_spec: single_service_spec(),
        worker_count: 1,
        enable_worker_failure: false,
        enable_delete: false,
        max_steps: 15,
    }
    .checker()
    .spawn_dfs()
    .join();

    result.assert_properties();
    println!(
        "Single service activation: {} unique states explored",
        result.unique_state_count()
    );
}

#[test]
fn check_two_services() {
    let result = NamespaceModel {
        initial_spec: two_service_spec(),
        worker_count: 1,
        enable_worker_failure: false,
        enable_delete: false,
        max_steps: 10,
    }
    .checker()
    .spawn_dfs()
    .join();

    result.assert_properties();
    println!(
        "Two services: {} unique states explored",
        result.unique_state_count()
    );
}

#[test]
fn check_activation_with_worker_failure() {
    let result = NamespaceModel {
        initial_spec: single_service_spec(),
        worker_count: 2,
        enable_worker_failure: true,
        enable_delete: false,
        max_steps: 20,
    }
    .checker()
    .spawn_dfs()
    .join();

    result.assert_properties();
    println!(
        "Activation with worker failure: {} unique states explored",
        result.unique_state_count()
    );
}

#[test]
fn check_two_workers_two_services() {
    let result = NamespaceModel {
        initial_spec: two_service_spec(),
        worker_count: 2,
        enable_worker_failure: true,
        enable_delete: false,
        max_steps: 8,
    }
    .checker()
    .spawn_dfs()
    .join();

    result.assert_properties();
    println!(
        "Two workers, two services: {} unique states explored",
        result.unique_state_count()
    );
}

#[test]
fn check_delete_lifecycle() {
    let result = NamespaceModel {
        initial_spec: single_service_spec(),
        worker_count: 1,
        enable_worker_failure: false,
        enable_delete: true,
        max_steps: 15,
    }
    .checker()
    .spawn_dfs()
    .join();

    result.assert_properties();
    println!(
        "Delete lifecycle: {} unique states explored",
        result.unique_state_count()
    );
}

#[test]
fn check_delete_with_worker_failure() {
    let result = NamespaceModel {
        initial_spec: two_service_spec(),
        worker_count: 2,
        enable_worker_failure: true,
        enable_delete: true,
        max_steps: 7,
    }
    .checker()
    .spawn_dfs()
    .join();

    result.assert_properties();
    println!(
        "Delete with worker failure: {} unique states explored",
        result.unique_state_count()
    );
}
