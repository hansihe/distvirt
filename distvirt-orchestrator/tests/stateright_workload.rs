use std::collections::BTreeSet;

use stateright::*;

use distvirt_orchestrator::types::*;
use distvirt_orchestrator::workload::{WorkloadInput, WorkloadOutput, WorkloadStateMachine};

/// Must match `WorkloadStateMachine::MAX_RETRIES`.
const MAX_RETRIES: u32 = 5;

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
    /// Whether to enable ForceDeactivate actions.
    enable_force_deactivate: bool,
    max_steps: usize,
}

// --- Hashable State ---

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct WlModelState {
    state: WorkloadState,
    current_demand: u32,
    consecutive_failures: u32,
    pending_timers: BTreeSet<TimerKey>,
    next_pod_id: u64,
    step_count: usize,
    has_active_flows: bool,
    needs_successful_boot: bool,
}

// --- Actions ---

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum WlModelAction {
    SetDemand { count: u32 },
    ForceDeactivate,
    LaunchPod { worker_id: WorkerId, pod_id: PodId },
    PodRunning { pod_id: PodId },
    PodGone { pod_id: PodId },
    PodSuspended { pod_id: PodId, artifact_id: ArtifactId },
    PodSuspendFailed { pod_id: PodId },
    WorkerLost { worker_id: WorkerId },
    TimerFired { timer_key: TimerKey },
    SpecChanged,
    ManualRestart,
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

/// Extract the pending intent from a workload state, if it's a transition state.
fn get_pending(state: &WorkloadState) -> Option<PendingIntent> {
    match state {
        WorkloadState::Launching { pending, .. }
        | WorkloadState::Suspending { pending, .. }
        | WorkloadState::Resuming { pending, .. } => Some(*pending),
        _ => None,
    }
}

// --- Model Implementation ---

impl Model for WorkloadModel {
    type State = WlModelState;
    type Action = WlModelAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![WlModelState {
            state: WorkloadState::Dormant,
            current_demand: 0,
            consecutive_failures: 0,
            pending_timers: BTreeSet::new(),
            next_pod_id: 0,
            step_count: 0,
            has_active_flows: false,
            needs_successful_boot: false,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        // SetDemand: can set to any value from 0 to num_services.
        for count in 0..=(self.num_services as u32) {
            if count != state.current_demand {
                actions.push(WlModelAction::SetDemand { count });
            }
        }

        // ForceDeactivate available from any state except Dormant/WaitingForCapacity.
        if self.enable_force_deactivate {
            match &state.state {
                WorkloadState::Dormant | WorkloadState::WaitingForCapacity => {}
                _ => {
                    actions.push(WlModelAction::ForceDeactivate);
                }
            }
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
                pod_id, worker_id, artifact_id, ..
            } => {
                // Pod can finish suspending successfully.
                actions.push(WlModelAction::PodSuspended {
                    pod_id: pod_id.clone(),
                    artifact_id: artifact_id.clone(),
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
            WorkloadState::Suspended { .. } => {
                // Worker loss for suspended state is now handled by the namespace layer
                // via placement table. The workload SM no longer stores worker_id here.
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
            WorkloadState::RetryBackoff { .. } => {
                // Timer fire is already covered by the pending_timers loop.
                // Recovery via spec change or manual restart.
                actions.push(WlModelAction::SpecChanged);
                actions.push(WlModelAction::ManualRestart);
            }
            WorkloadState::Failed => {
                // Recovery actions from terminal failure state.
                actions.push(WlModelAction::SpecChanged);
                actions.push(WlModelAction::ManualRestart);
            }
            WorkloadState::Transitioning => unreachable!("Transitioning in model"),
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut sm = WorkloadStateMachine::new(wl_id(), self.enable_suspend);
        sm.state = state.state.clone();
        sm.current_demand = state.current_demand;
        sm.consecutive_failures = state.consecutive_failures;
        sm.has_active_flows = state.has_active_flows;
        sm.needs_successful_boot = state.needs_successful_boot;

        let mut next_pod_id = state.next_pod_id;
        let ns = ns_id();

        let (input, fired_timer) = match action {
            WlModelAction::SetDemand { count } => (WorkloadInput::SetDemand { count }, None),
            WlModelAction::ForceDeactivate => (WorkloadInput::ForceDeactivate, None),
            WlModelAction::LaunchPod { worker_id, pod_id } => {
                next_pod_id += 1;
                (WorkloadInput::LaunchPod { worker_id, pod_id }, None)
            }
            WlModelAction::PodRunning { pod_id } => (WorkloadInput::PodRunning { pod_id }, None),
            WlModelAction::PodGone { pod_id } => (WorkloadInput::PodGone { pod_id, reason: None }, None),
            WlModelAction::PodSuspended { pod_id, artifact_id } => {
                (WorkloadInput::PodSuspended { pod_id, artifact_id }, None)
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
            WlModelAction::SpecChanged => (WorkloadInput::SpecChanged, None),
            WlModelAction::ManualRestart => (WorkloadInput::ManualRestart, None),
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
        // In the real system, the namespace layer resolves worker_id from the placement table.
        // Here we use a dummy worker_id since the workload SM doesn't validate it.
        for out in &outputs {
            if let WorkloadOutput::ResumeRequest { artifact_id } = out {
                let pod_id = PodId(format!("pod-{}", next_pod_id));
                next_pod_id += 1;
                let resume_outputs = sm.step(
                    WorkloadInput::ResumePod {
                        worker_id: wl_worker_id(0),
                        pod_id,
                        artifact_id: artifact_id.clone(),
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
            current_demand: sm.current_demand,
            consecutive_failures: sm.consecutive_failures,
            pending_timers,
            next_pod_id,
            step_count: state.step_count + 1,
            has_active_flows: sm.has_active_flows,
            needs_successful_boot: sm.needs_successful_boot,
        })
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.step_count <= self.max_steps
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Safety: Transitioning sentinel must never survive a step() call.
            Property::<Self>::always("no transitioning state", |_model, state| {
                !matches!(state.state, WorkloadState::Transitioning)
            }),
            // Safety: consecutive failures never exceed MAX_RETRIES.
            Property::<Self>::always("consecutive failures bounded", |_model, state| {
                state.consecutive_failures <= MAX_RETRIES
            }),
            // Safety: Failed implies max retries exhausted.
            // Note: current_demand may be 0 if demand dropped during retry while
            // needs_successful_boot kept the workload going.
            Property::<Self>::always("failed implies max retries", |_model, state| {
                if matches!(state.state, WorkloadState::Failed) {
                    state.consecutive_failures >= MAX_RETRIES
                } else {
                    true
                }
            }),
            // Safety: RetryBackoff has its backoff timer in pending_timers.
            Property::<Self>::always("retry backoff has timer", |_model, state| {
                if let WorkloadState::RetryBackoff { ref backoff_timer } = state.state {
                    state.pending_timers.contains(backoff_timer)
                } else {
                    true
                }
            }),
            // Safety: Failed state has no timers.
            Property::<Self>::always("failed has no timers", |_model, state| {
                if matches!(state.state, WorkloadState::Failed) {
                    state.pending_timers.is_empty()
                } else {
                    true
                }
            }),
            // Safety: Running implies consecutive_failures == 0.
            Property::<Self>::always("pod running resets failures", |_model, state| {
                if matches!(state.state, WorkloadState::Running { .. }) {
                    state.consecutive_failures == 0
                } else {
                    true
                }
            }),
            // Safety: current_demand never exceeds num_services.
            // (Enforced by action generation, but verify state consistency.)
            Property::<Self>::always("demand count bounded", |model, state| {
                (state.current_demand as usize) <= model.num_services
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
            // Safety: pending == Demand implies current_demand > 0.
            Property::<Self>::always("pending demand implies demand count", |_model, state| {
                if let Some(PendingIntent::Demand) = get_pending(&state.state) {
                    state.current_demand > 0
                } else {
                    true
                }
            }),
            // Reachability: Can reach Suspended via ForceDeactivate (only when both are enabled).
            Property::<Self>::sometimes("can reach suspended via deactivate", |model, state| {
                if !model.enable_suspend || !model.enable_force_deactivate {
                    // Vacuously satisfied when feature is not enabled.
                    return true;
                }
                matches!(state.state, WorkloadState::Suspended { .. })
            }),
            // Reachability: Can reach Failed state (needs pod_failure + enough steps).
            Property::<Self>::sometimes("can reach failed", |model, state| {
                if !model.enable_pod_failure || model.max_steps < 20 {
                    return true;
                }
                matches!(state.state, WorkloadState::Failed)
            }),
            // Reachability: Can recover from Failed via SpecChanged/ManualRestart.
            Property::<Self>::sometimes("can recover from failed", |model, state| {
                if !model.enable_pod_failure || model.max_steps < 25 {
                    return true;
                }
                // Running after having been through multiple pod attempts.
                matches!(state.state, WorkloadState::Running { .. })
                    && state.next_pod_id > 5
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
        enable_force_deactivate: false,
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
        enable_force_deactivate: false,
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
        enable_force_deactivate: false,
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
        enable_force_deactivate: false,
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
        enable_force_deactivate: false,
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
        enable_force_deactivate: false,
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
        enable_force_deactivate: false,
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
        enable_force_deactivate: false,
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

#[test]
fn workload_force_deactivate() {
    let result = WorkloadModel {
        num_services: 1,
        num_workers: 1,
        enable_pod_failure: false,
        enable_worker_loss: false,
        enable_suspend: true,
        enable_force_deactivate: true,
        max_steps: 20,
    }
    .checker()
    .spawn_dfs()
    .join();

    result.assert_properties();
    println!(
        "Workload (force deactivate, suspend): {} unique states",
        result.unique_state_count()
    );
}

#[test]
fn workload_force_deactivate_no_suspend() {
    let result = WorkloadModel {
        num_services: 1,
        num_workers: 1,
        enable_pod_failure: false,
        enable_worker_loss: false,
        enable_suspend: false,
        enable_force_deactivate: true,
        max_steps: 20,
    }
    .checker()
    .spawn_dfs()
    .join();

    result.assert_properties();
    println!(
        "Workload (force deactivate, no suspend): {} unique states",
        result.unique_state_count()
    );
}

#[test]
fn workload_force_deactivate_full_chaos() {
    let result = WorkloadModel {
        num_services: 2,
        num_workers: 2,
        enable_pod_failure: true,
        enable_worker_loss: true,
        enable_suspend: true,
        enable_force_deactivate: true,
        max_steps: 12,
    }
    .checker()
    .spawn_dfs()
    .join();

    result.assert_properties();
    println!(
        "Workload (force deactivate, full chaos): {} unique states",
        result.unique_state_count()
    );
}

/// Exercises retry/backoff mechanics with 30 steps — enough to reach Failed state
/// and verify the "can recover from failed" liveness property (guarded at max_steps < 25).
#[test]
fn workload_retry_backoff() {
    let result = WorkloadModel {
        num_services: 1,
        num_workers: 1,
        enable_pod_failure: true,
        enable_worker_loss: false,
        enable_suspend: false,
        enable_force_deactivate: false,
        max_steps: 30,
    }
    .checker()
    .spawn_dfs()
    .join();

    result.assert_properties();
    println!(
        "Workload (retry backoff): {} unique states",
        result.unique_state_count()
    );
}

/// Like `workload_retry_backoff` but with 35 steps to explore deeper state space,
/// ensuring recovery paths (SpecChanged/ManualRestart) from Failed are reachable.
#[test]
fn workload_retry_recovery() {
    let result = WorkloadModel {
        num_services: 1,
        num_workers: 1,
        enable_pod_failure: true,
        enable_worker_loss: false,
        enable_suspend: false,
        enable_force_deactivate: false,
        max_steps: 35,
    }
    .checker()
    .spawn_dfs()
    .join();

    result.assert_properties();
    println!(
        "Workload (retry recovery): {} unique states",
        result.unique_state_count()
    );
}

#[test]
fn workload_retry_with_suspend() {
    let result = WorkloadModel {
        num_services: 1,
        num_workers: 1,
        enable_pod_failure: true,
        enable_worker_loss: false,
        enable_suspend: true,
        enable_force_deactivate: false,
        max_steps: 30,
    }
    .checker()
    .spawn_dfs()
    .join();

    result.assert_properties();
    println!(
        "Workload (retry with suspend): {} unique states",
        result.unique_state_count()
    );
}
