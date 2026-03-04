use std::collections::BTreeSet;

use stateright::*;

use distvirt_orchestrator::types::*;
use distvirt_orchestrator::workload::{WorkloadInput, WorkloadOutput, WorkloadStateMachine};

// --- Model Configuration ---

struct WorkloadModel {
    /// Number of services that can send DemandUp/DemandDown.
    num_services: usize,
    /// Number of workers that can host pods.
    num_workers: usize,
    /// Whether to inject pod failures.
    enable_pod_failure: bool,
    /// Whether to inject worker loss.
    enable_worker_loss: bool,
    /// Whether to enable suspend-on-idle behavior.
    enable_suspend: bool,
    max_steps: usize,
}

// --- Hashable State ---

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct WlModelState {
    state: WorkloadState,
    demand_count: u32,
    pending_timers: BTreeSet<TimerKey>,
    next_pod_id: u64,
    step_count: usize,
}

// --- Actions ---

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum WlModelAction {
    DemandUp,
    DemandDown,
    LaunchPod { worker_id: WorkerId, pod_id: PodId },
    PodRunning { pod_id: PodId },
    PodGone { pod_id: PodId },
    PodSuspended { pod_id: PodId, snapshot_id: SnapshotId },
    PodSuspendFailed { pod_id: PodId },
    WorkerLost { worker_id: WorkerId },
    TimerFired { timer_key: TimerKey },
}

// --- Helpers ---

fn wl_worker_id(i: usize) -> WorkerId {
    WorkerId(format!("w-{}", i))
}

fn wl_id() -> WorkloadId {
    WorkloadId("wl-0".into())
}

fn ns_id() -> NamespaceId {
    NamespaceId("model-ns".into())
}

// --- Model Implementation ---

impl Model for WorkloadModel {
    type State = WlModelState;
    type Action = WlModelAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![WlModelState {
            state: WorkloadState::Dormant,
            demand_count: 0,
            pending_timers: BTreeSet::new(),
            next_pod_id: 0,
            step_count: 0,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        // Any service can send DemandUp.
        if (state.demand_count as usize) < self.num_services {
            actions.push(WlModelAction::DemandUp);
        }

        // Any service with active demand can send DemandDown.
        if state.demand_count > 0 {
            actions.push(WlModelAction::DemandDown);
        }

        // Pending timers can fire.
        for tk in &state.pending_timers {
            actions.push(WlModelAction::TimerFired {
                timer_key: tk.clone(),
            });
        }

        match &state.state {
            WorkloadState::WaitingForCapacity => {
                // Outer layer can schedule on any worker.
                for i in 0..self.num_workers {
                    let pod_id = PodId(format!("pod-{}", state.next_pod_id));
                    actions.push(WlModelAction::LaunchPod {
                        worker_id: wl_worker_id(i),
                        pod_id,
                    });
                }
            }
            WorkloadState::Launching {
                pod_id, worker_id, ..
            } => {
                // Pod can start running.
                actions.push(WlModelAction::PodRunning {
                    pod_id: pod_id.clone(),
                });
                // Pod can fail during launch.
                if self.enable_pod_failure {
                    actions.push(WlModelAction::PodGone {
                        pod_id: pod_id.clone(),
                    });
                }
                // Worker can disconnect.
                if self.enable_worker_loss {
                    actions.push(WlModelAction::WorkerLost {
                        worker_id: worker_id.clone(),
                    });
                }
            }
            WorkloadState::Running {
                pod_id, worker_id, ..
            } => {
                // Pod can exit.
                if self.enable_pod_failure {
                    actions.push(WlModelAction::PodGone {
                        pod_id: pod_id.clone(),
                    });
                }
                // Worker can disconnect.
                if self.enable_worker_loss {
                    actions.push(WlModelAction::WorkerLost {
                        worker_id: worker_id.clone(),
                    });
                }
            }
            WorkloadState::Dormant => {}
            WorkloadState::Suspending {
                pod_id, worker_id, snapshot_id, ..
            } => {
                // Pod can finish suspending successfully.
                actions.push(WlModelAction::PodSuspended {
                    pod_id: pod_id.clone(),
                    snapshot_id: snapshot_id.clone(),
                });
                // Suspend can fail.
                actions.push(WlModelAction::PodSuspendFailed {
                    pod_id: pod_id.clone(),
                });
                // Pod can die during suspend.
                actions.push(WlModelAction::PodGone {
                    pod_id: pod_id.clone(),
                });
                if self.enable_worker_loss {
                    actions.push(WlModelAction::WorkerLost {
                        worker_id: worker_id.clone(),
                    });
                }
            }
            WorkloadState::Suspended { worker_id, .. } => {
                if self.enable_worker_loss {
                    actions.push(WlModelAction::WorkerLost {
                        worker_id: worker_id.clone(),
                    });
                }
            }
            WorkloadState::Resuming {
                pod_id, worker_id, ..
            } => {
                actions.push(WlModelAction::PodRunning {
                    pod_id: pod_id.clone(),
                });
                if self.enable_pod_failure {
                    actions.push(WlModelAction::PodGone {
                        pod_id: pod_id.clone(),
                    });
                }
                if self.enable_worker_loss {
                    actions.push(WlModelAction::WorkerLost {
                        worker_id: worker_id.clone(),
                    });
                }
            }
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut sm = WorkloadStateMachine::new(wl_id(), self.enable_suspend);
        sm.state = state.state.clone();
        sm.demand_count = state.demand_count;

        let mut next_pod_id = state.next_pod_id;
        let ns = ns_id();

        let (input, fired_timer) = match action {
            WlModelAction::DemandUp => (WorkloadInput::DemandUp, None),
            WlModelAction::DemandDown => (WorkloadInput::DemandDown, None),
            WlModelAction::LaunchPod { worker_id, pod_id } => {
                next_pod_id += 1;
                (WorkloadInput::LaunchPod { worker_id, pod_id }, None)
            }
            WlModelAction::PodRunning { pod_id } => (WorkloadInput::PodRunning { pod_id }, None),
            WlModelAction::PodGone { pod_id } => (WorkloadInput::PodGone { pod_id }, None),
            WlModelAction::PodSuspended { pod_id, snapshot_id } => {
                (WorkloadInput::PodSuspended { pod_id, snapshot_id, pool_id: PoolId::from("default-pool") }, None)
            }
            WlModelAction::PodSuspendFailed { pod_id } => {
                (WorkloadInput::PodSuspendFailed { pod_id }, None)
            }
            WlModelAction::WorkerLost { worker_id } => {
                (WorkloadInput::WorkerLost { worker_id }, None)
            }
            WlModelAction::TimerFired { timer_key } => {
                let tk = timer_key.clone();
                (WorkloadInput::TimerFired { timer_key }, Some(tk))
            }
        };

        let outputs = sm.step(input, &ns);

        let mut pending_timers = state.pending_timers.clone();
        // A fired timer is consumed by the runtime (removed from pending).
        if let Some(tk) = fired_timer {
            pending_timers.remove(&tk);
        }
        for out in &outputs {
            match out {
                WorkloadOutput::TimerSet(key, _) => {
                    pending_timers.insert(key.clone());
                }
                WorkloadOutput::TimerCancel(key) => {
                    pending_timers.remove(key);
                }
                _ => {}
            }
        }

        // Process ResumeRequest outputs: simulate outer layer injecting ResumePod.
        for out in &outputs {
            if let WorkloadOutput::ResumeRequest { snapshot_id, worker_id, pool_id } = out {
                let pod_id = PodId(format!("pod-{}", next_pod_id));
                next_pod_id += 1;
                let resume_outputs = sm.step(
                    WorkloadInput::ResumePod {
                        worker_id: worker_id.clone(),
                        pod_id,
                        snapshot_id: snapshot_id.clone(),
                        pool_id: pool_id.clone(),
                    },
                    &ns,
                );
                for rout in &resume_outputs {
                    match rout {
                        WorkloadOutput::TimerSet(key, _) => {
                            pending_timers.insert(key.clone());
                        }
                        WorkloadOutput::TimerCancel(key) => {
                            pending_timers.remove(key);
                        }
                        _ => {}
                    }
                }
            }
        }

        Some(WlModelState {
            state: sm.state,
            demand_count: sm.demand_count,
            pending_timers,
            next_pod_id,
            step_count: state.step_count + 1,
        })
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.step_count <= self.max_steps
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Safety: demand_count == 0 implies Dormant (no pod without demand).
            Property::<Self>::always("no pod without demand", |_model, state| {
                if state.demand_count == 0 {
                    matches!(
                        state.state,
                        WorkloadState::Dormant
                            | WorkloadState::WaitingForCapacity
                            | WorkloadState::Suspending { .. }
                            | WorkloadState::Suspended { .. }
                            | WorkloadState::Resuming { .. }
                    )
                } else {
                    true
                }
            }),
            // Safety: demand_count never exceeds num_services.
            // (Enforced by action generation, but verify state consistency.)
            Property::<Self>::always("demand count bounded", |model, state| {
                (state.demand_count as usize) <= model.num_services
            }),
            // Safety: Launching state always has a pending launch timeout timer.
            Property::<Self>::always("launching has timeout timer", |_model, state| {
                if let WorkloadState::Launching {
                    ref launch_timeout, ..
                } = state.state
                {
                    state.pending_timers.contains(launch_timeout)
                } else {
                    true
                }
            }),
            // Safety: Running state has no launch timeout timer.
            Property::<Self>::always("running has no launch timer", |_model, state| {
                if let WorkloadState::Running { .. } = state.state {
                    !state.pending_timers.iter().any(|tk| {
                        matches!(tk, TimerKey::LaunchTimeout { .. })
                    })
                } else {
                    true
                }
            }),
            // Safety: Dormant state has no timers.
            Property::<Self>::always("dormant has no timers", |_model, state| {
                if matches!(state.state, WorkloadState::Dormant) {
                    state.pending_timers.is_empty()
                } else {
                    true
                }
            }),
            // Reachability: Can reach Running state.
            Property::<Self>::sometimes("can reach running", |_model, state| {
                matches!(state.state, WorkloadState::Running { .. })
            }),
            // Safety: Suspending state always has a pending suspend timeout timer.
            Property::<Self>::always("suspending has timeout timer", |_model, state| {
                if let WorkloadState::Suspending {
                    ref suspend_timeout, ..
                } = state.state
                {
                    state.pending_timers.contains(suspend_timeout)
                } else {
                    true
                }
            }),
            // Safety: Resuming state always has a pending resume timeout timer.
            Property::<Self>::always("resuming has timeout timer", |_model, state| {
                if let WorkloadState::Resuming {
                    ref resume_timeout, ..
                } = state.state
                {
                    state.pending_timers.contains(resume_timeout)
                } else {
                    true
                }
            }),
            // Safety: Suspended state has no timers.
            Property::<Self>::always("suspended has no timers", |_model, state| {
                if matches!(state.state, WorkloadState::Suspended { .. }) {
                    state.pending_timers.is_empty()
                } else {
                    true
                }
            }),
            // Reachability: Can reach Dormant after Running (demand drops to 0).
            Property::<Self>::sometimes("can reach dormant after running", |_model, state| {
                state.next_pod_id > 0 && matches!(state.state, WorkloadState::Dormant)
            }),
        ]
    }
}

// --- Tests ---

#[test]
fn workload_single_service_single_worker() {
    let result = WorkloadModel {
        num_services: 1,
        num_workers: 1,
        enable_pod_failure: false,
        enable_worker_loss: false,
        enable_suspend: false,
        max_steps: 15,
    }
    .checker()
    .spawn_dfs()
    .join();

    result.assert_properties();
    println!(
        "Workload (1 svc, 1 worker, no failures): {} unique states",
        result.unique_state_count()
    );
}

#[test]
fn workload_two_services_single_worker() {
    let result = WorkloadModel {
        num_services: 2,
        num_workers: 1,
        enable_pod_failure: false,
        enable_worker_loss: false,
        enable_suspend: false,
        max_steps: 15,
    }
    .checker()
    .spawn_dfs()
    .join();

    result.assert_properties();
    println!(
        "Workload (2 svc, 1 worker, no failures): {} unique states",
        result.unique_state_count()
    );
}

#[test]
fn workload_with_pod_failure() {
    let result = WorkloadModel {
        num_services: 1,
        num_workers: 1,
        enable_pod_failure: true,
        enable_worker_loss: false,
        enable_suspend: false,
        max_steps: 20,
    }
    .checker()
    .spawn_dfs()
    .join();

    result.assert_properties();
    println!(
        "Workload (pod failure): {} unique states",
        result.unique_state_count()
    );
}

#[test]
fn workload_with_worker_loss() {
    let result = WorkloadModel {
        num_services: 1,
        num_workers: 2,
        enable_pod_failure: false,
        enable_worker_loss: true,
        enable_suspend: false,
        max_steps: 20,
    }
    .checker()
    .spawn_dfs()
    .join();

    result.assert_properties();
    println!(
        "Workload (worker loss): {} unique states",
        result.unique_state_count()
    );
}

#[test]
fn workload_full_chaos() {
    let result = WorkloadModel {
        num_services: 2,
        num_workers: 2,
        enable_pod_failure: true,
        enable_worker_loss: true,
        enable_suspend: false,
        max_steps: 12,
    }
    .checker()
    .spawn_dfs()
    .join();

    result.assert_properties();
    println!(
        "Workload (full chaos): {} unique states",
        result.unique_state_count()
    );
}

#[test]
fn workload_suspend_basic() {
    let result = WorkloadModel {
        num_services: 1,
        num_workers: 1,
        enable_pod_failure: false,
        enable_worker_loss: false,
        enable_suspend: true,
        max_steps: 20,
    }
    .checker()
    .spawn_dfs()
    .join();

    result.assert_properties();
    println!(
        "Workload (suspend, no failures): {} unique states",
        result.unique_state_count()
    );
}

#[test]
fn workload_suspend_with_failures() {
    let result = WorkloadModel {
        num_services: 1,
        num_workers: 2,
        enable_pod_failure: true,
        enable_worker_loss: true,
        enable_suspend: true,
        max_steps: 15,
    }
    .checker()
    .spawn_dfs()
    .join();

    result.assert_properties();
    println!(
        "Workload (suspend, full chaos): {} unique states",
        result.unique_state_count()
    );
}

#[test]
fn workload_suspend_two_services() {
    let result = WorkloadModel {
        num_services: 2,
        num_workers: 1,
        enable_pod_failure: true,
        enable_worker_loss: false,
        enable_suspend: true,
        max_steps: 15,
    }
    .checker()
    .spawn_dfs()
    .join();

    result.assert_properties();
    println!(
        "Workload (suspend, 2 services): {} unique states",
        result.unique_state_count()
    );
}
