use std::collections::BTreeSet;
use std::net::Ipv4Addr;
use std::time::Duration;

use stateright::*;

use distvirt_orchestrator::service::{ServiceInput, ServiceOutput, ServiceStateMachine};
use distvirt_orchestrator::types::*;

// --- Model Configuration ---

struct ServiceModel {
    has_activation: bool,
    idle_timeout: Duration,
    /// Whether the mock workload can become unready.
    enable_workload_failure: bool,
    max_steps: usize,
}

// --- Hashable State ---

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SvcModelState {
    state: ServiceState,
    has_activation: bool,
    pending_timers: BTreeSet<TimerKey>,
    /// Track whether we've ever been active (for reachability).
    was_active: bool,
    step_count: usize,
}

// --- Actions ---

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum SvcModelAction {
    WorkloadReady {
        pod_id: PodId,
        worker_id: WorkerId,
        backend: ServiceBackend,
    },
    WorkloadUnready,
    ServiceActivation,
    ServiceBackendNeed { need: BackendNeed },
    TimerFired { timer_key: TimerKey },
}

fn mock_backend() -> ServiceBackend {
    ServiceBackend {
        pod_ip: Ipv4Addr::new(172, 16, 0, 10),
    }
}

// --- Helpers ---

fn svc_id() -> ServiceId {
    ServiceId("svc-0".into())
}

fn svc_wl_id() -> WorkloadId {
    WorkloadId("wl-0".into())
}

fn svc_ns_id() -> NamespaceId {
    NamespaceId("model-ns".into())
}

fn mock_pod_id() -> PodId {
    PodId("pod-0".into())
}

fn mock_worker_id() -> WorkerId {
    WorkerId("w-0".into())
}

// --- Model Implementation ---

impl Model for ServiceModel {
    type State = SvcModelState;
    type Action = SvcModelAction;

    fn init_states(&self) -> Vec<Self::State> {
        // Start from Idle (activation) or NeedBackend (always-on),
        // since Pending → Idle/NeedBackend is handled by the namespace
        // coordinator's reconcile, not by the service SM itself.
        let initial_state = if self.has_activation {
            ServiceState::Idle
        } else {
            ServiceState::NeedBackend
        };

        vec![SvcModelState {
            state: initial_state,
            has_activation: self.has_activation,
            pending_timers: BTreeSet::new(),
            was_active: false,
            step_count: 0,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        // Pending timers can fire.
        for tk in &state.pending_timers {
            actions.push(SvcModelAction::TimerFired {
                timer_key: tk.clone(),
            });
        }

        match &state.state {
            ServiceState::Idle => {
                // Activation event can arrive.
                actions.push(SvcModelAction::ServiceActivation);
            }
            ServiceState::NeedBackend => {
                // Workload can become ready.
                actions.push(SvcModelAction::WorkloadReady {
                    pod_id: mock_pod_id(),
                    worker_id: mock_worker_id(),
                    backend: mock_backend(),
                });
                // Workload can fail while we're waiting.
                if self.enable_workload_failure {
                    actions.push(SvcModelAction::WorkloadUnready);
                }
            }
            ServiceState::Active { .. } => {
                // Backend need can change.
                for need in &[BackendNeed::None, BackendNeed::Traffic, BackendNeed::Active] {
                    actions.push(SvcModelAction::ServiceBackendNeed {
                        need: need.clone(),
                    });
                }
                // Workload can become unready (pod lost).
                if self.enable_workload_failure {
                    actions.push(SvcModelAction::WorkloadUnready);
                }
            }
            ServiceState::Pending => {
                // Should not reach here given our init_states, but be safe.
            }
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut sm = ServiceStateMachine::new(
            svc_id(),
            svc_wl_id(),
            state.has_activation,
            self.idle_timeout,
        );
        sm.state = state.state.clone();

        let ns = svc_ns_id();

        let (input, fired_timer) = match action {
            SvcModelAction::WorkloadReady {
                pod_id,
                worker_id,
                backend,
            } => (
                ServiceInput::WorkloadReady {
                    pod_id,
                    worker_id,
                    backend,
                },
                None,
            ),
            SvcModelAction::WorkloadUnready => (ServiceInput::WorkloadUnready, None),
            SvcModelAction::ServiceActivation => (ServiceInput::ServiceActivation, None),
            SvcModelAction::ServiceBackendNeed { need } => {
                (ServiceInput::ServiceBackendNeed { need }, None)
            }
            SvcModelAction::TimerFired { timer_key } => {
                let tk = timer_key.clone();
                (ServiceInput::TimerFired { timer_key }, Some(tk))
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
                ServiceOutput::TimerSet(key, _) => {
                    pending_timers.insert(key.clone());
                }
                ServiceOutput::TimerCancel(key) => {
                    pending_timers.remove(key);
                }
                _ => {}
            }
        }

        let was_active =
            state.was_active || matches!(state.state, ServiceState::Active { .. });

        Some(SvcModelState {
            state: sm.state,
            has_activation: state.has_activation,
            pending_timers,
            was_active,
            step_count: state.step_count + 1,
        })
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.step_count <= self.max_steps
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut props = vec![
            // Safety: Idle state has no pending timers.
            Property::<Self>::always("idle has no timers", |_model, state| {
                if matches!(state.state, ServiceState::Idle) {
                    state.pending_timers.is_empty()
                } else {
                    true
                }
            }),
            // Safety: NeedBackend state has no pending timers.
            Property::<Self>::always("need_backend has no timers", |_model, state| {
                if matches!(state.state, ServiceState::NeedBackend) {
                    state.pending_timers.is_empty()
                } else {
                    true
                }
            }),
            // Safety: Active with idle_timer=Some has a matching pending timer.
            Property::<Self>::always("active idle timer consistent", |_model, state| {
                if let ServiceState::Active {
                    ref idle_timer, ..
                } = state.state
                {
                    match idle_timer {
                        Some(tk) => state.pending_timers.contains(tk),
                        None => true,
                    }
                } else {
                    true
                }
            }),
            // Safety: Active with BackendNeed::Active or Traffic has no idle timer.
            Property::<Self>::always(
                "no idle timer during active traffic",
                |_model, state| {
                    if let ServiceState::Active {
                        ref backend_need,
                        ref idle_timer,
                        ..
                    } = state.state
                    {
                        match backend_need {
                            BackendNeed::Traffic | BackendNeed::Active => idle_timer.is_none(),
                            BackendNeed::None => true,
                        }
                    } else {
                        true
                    }
                },
            ),
            // Reachability: Can reach Active state.
            Property::<Self>::sometimes("can reach active", |_model, state| {
                matches!(state.state, ServiceState::Active { .. })
            }),
        ];

        if self.has_activation {
            // Reachability: Activation service can return to Idle after being Active.
            props.push(Property::<Self>::sometimes(
                "can reach idle after active",
                |_model, state| {
                    state.was_active && matches!(state.state, ServiceState::Idle)
                },
            ));
        }

        props
    }
}

// --- Tests ---

#[test]
fn service_activation_basic() {
    let result = ServiceModel {
        has_activation: true,
        idle_timeout: Duration::from_secs(30),
        enable_workload_failure: false,
        max_steps: 15,
    }
    .checker()
    .spawn_dfs()
    .join();

    result.assert_properties();
    println!(
        "Service (activation, no failures): {} unique states",
        result.unique_state_count()
    );
}

#[test]
fn service_always_on_basic() {
    let result = ServiceModel {
        has_activation: false,
        idle_timeout: Duration::from_secs(30),
        enable_workload_failure: false,
        max_steps: 15,
    }
    .checker()
    .spawn_dfs()
    .join();

    result.assert_properties();
    println!(
        "Service (always-on, no failures): {} unique states",
        result.unique_state_count()
    );
}

#[test]
fn service_activation_with_workload_failure() {
    let result = ServiceModel {
        has_activation: true,
        idle_timeout: Duration::from_secs(30),
        enable_workload_failure: true,
        max_steps: 20,
    }
    .checker()
    .spawn_dfs()
    .join();

    result.assert_properties();
    println!(
        "Service (activation, workload failure): {} unique states",
        result.unique_state_count()
    );
}

#[test]
fn service_always_on_with_workload_failure() {
    let result = ServiceModel {
        has_activation: false,
        idle_timeout: Duration::from_secs(30),
        enable_workload_failure: true,
        max_steps: 20,
    }
    .checker()
    .spawn_dfs()
    .join();

    result.assert_properties();
    println!(
        "Service (always-on, workload failure): {} unique states",
        result.unique_state_count()
    );
}
