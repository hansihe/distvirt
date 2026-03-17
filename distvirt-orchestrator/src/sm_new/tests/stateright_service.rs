//! Stateright model checking for the new ServiceSm.
//!
//! Level 1 individual SM model checking: we instantiate ServiceSm in isolation,
//! feed it inputs via CtxConcrete, inspect effects to update environment state,
//! and verify safety/liveness properties.
//!
//! The state space is fully explored (no step bound). All monotonic counters
//! are normalized by Representative, so the state space is finite.

use distvirt_sm_router::{SequentialIds, SmHandler};
use stateright::*;

use super::super::*;

// ============================================================================
// Environment types
// ============================================================================

/// Readiness as seen by the environment (the workload's readiness signal).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum ReadinessEnv {
    /// No workload is ready.
    None,
    /// Workload is ready (has a running pod).
    Ready,
}

// ============================================================================
// Model configuration
// ============================================================================

/// Level 1 model: tests ServiceSm in isolation.
///
/// Note: spec changes that affect `has_activation` are tested here because
/// the service SM handles them directly. Workload readiness is modeled as
/// a simple present/absent toggle — the actual ReadyInfo content doesn't
/// affect service behavior beyond presence.
struct SvcNewModel {
    /// Whether the service has activation (demand-driven vs always-on).
    has_activation: bool,
    /// Whether to inject backend need changes from workers.
    enable_backend_need: bool,
}

// ============================================================================
// Model state
// ============================================================================

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct SvcNewModelState {
    /// The SM under test.
    sm: ServiceSm,
    /// Whether the service is currently activated (for activation-based services).
    activated: bool,
    /// Current readiness from the workload.
    readiness_env: ReadinessEnv,
    /// Current backend need level from workers.
    backend_need: BackendNeed,
    /// Whether the demand signal is set (extracted from effects).
    demand_set: bool,
    /// Whether an idle timer is pending in the environment.
    timer_pending: bool,
    /// Generation of the pending timer.
    timer_generation: u64,
    /// Whether the spec is currently present (SM initialized as if spec delivered).
    spec_present: bool,
    /// Whether self-destruct was triggered (spec removed).
    self_destructed: bool,
}

// ============================================================================
// Actions
// ============================================================================

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum SvcNewAction {
    /// Activate the service (ActivateService(true)).
    Activate,
    /// Deactivate the service (ActivateService(false)).
    Deactivate,
    /// Workload becomes ready (ReadinessInput with Some).
    WorkloadReady,
    /// Workload becomes unready (ReadinessInput with None).
    WorkloadUnready,
    /// Backend need changes to None.
    BackendNeedNone,
    /// Backend need changes to Traffic.
    BackendNeedTraffic,
    /// Backend need changes to Active.
    BackendNeedActive,
    /// Idle timer fires.
    TimerFired,
    /// Remove the spec (SvcSpecInput(None)) — triggers self-destruct.
    RemoveSpec,
    /// Toggle has_activation via spec change.
    ChangeActivation,
}

// ============================================================================
// Helpers
// ============================================================================

/// Deliver an input to the SM, apply effects to env state.
fn apply_svc_input(state: &SvcNewModelState, input: ServiceInput) -> SvcNewModelState {
    let mut next = state.clone();
    let mut alloc = SequentialIds::<NodeKind>::new();
    let svc_id = ServiceId(0);
    let mut ctx = ServiceCtxConcrete::new(svc_id, &mut alloc);
    next.sm.handle(input, &mut ctx);
    let effects = ctx.into_effects();

    // Check self-destruct.
    if effects.pending_self_destruct {
        next.self_destructed = true;
    }

    // Check demand signal changes.
    if let Some(demand) = effects.demand {
        next.demand_set = demand;
    }

    // Check timer signal changes.
    if let Some(ref timers) = effects.svc_wanted_timers {
        if let Some(req) = timers.first() {
            next.timer_pending = true;
            next.timer_generation = req.generation;
        } else {
            next.timer_pending = false;
        }
    }

    next
}

fn make_ready_info() -> ReadyInfo {
    ReadyInfo {
        pod_id: PodId(1),
        worker_id: WorkerId(1),
    }
}

// ============================================================================
// Model implementation
// ============================================================================

impl Model for SvcNewModel {
    type State = SvcNewModelState;
    type Action = SvcNewAction;

    fn init_states(&self) -> Vec<Self::State> {
        let sm = ServiceSm::new(self.has_activation);
        vec![SvcNewModelState {
            demand_set: !self.has_activation, // always-on starts with demand
            sm,
            activated: false,
            readiness_env: ReadinessEnv::None,
            backend_need: BackendNeed::None,
            timer_pending: false,
            timer_generation: 0,
            spec_present: true, // SM initialized as if spec was delivered
            self_destructed: false,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        // Don't generate actions for destroyed services.
        if state.self_destructed {
            return;
        }

        // Activation toggle (only for activation-based services).
        if state.sm.has_activation {
            if !state.activated {
                actions.push(SvcNewAction::Activate);
            } else {
                actions.push(SvcNewAction::Deactivate);
            }
        }

        // Readiness changes.
        match state.readiness_env {
            ReadinessEnv::None => {
                actions.push(SvcNewAction::WorkloadReady);
            }
            ReadinessEnv::Ready => {
                actions.push(SvcNewAction::WorkloadUnready);
                // Also allow re-delivering ready (workload pod changed).
                actions.push(SvcNewAction::WorkloadReady);
            }
        }

        // Backend need changes.
        if self.enable_backend_need {
            if state.backend_need != BackendNeed::None {
                actions.push(SvcNewAction::BackendNeedNone);
            }
            if state.backend_need != BackendNeed::Traffic {
                actions.push(SvcNewAction::BackendNeedTraffic);
            }
            if state.backend_need != BackendNeed::Active {
                actions.push(SvcNewAction::BackendNeedActive);
            }
        }

        // Timer fires.
        if state.timer_pending {
            actions.push(SvcNewAction::TimerFired);
        }

        // Spec changes.
        if state.spec_present {
            actions.push(SvcNewAction::RemoveSpec);
            actions.push(SvcNewAction::ChangeActivation);
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut next = match action {
            SvcNewAction::Activate => {
                let mut s = apply_svc_input(state, ServiceInput::ActivateService(true));
                s.activated = true;
                s
            }
            SvcNewAction::Deactivate => {
                let mut s = apply_svc_input(state, ServiceInput::ActivateService(false));
                s.activated = false;
                s
            }
            SvcNewAction::WorkloadReady => {
                let mut s = apply_svc_input(
                    state,
                    ServiceInput::ReadinessInput(vec![Some(make_ready_info())]),
                );
                s.readiness_env = ReadinessEnv::Ready;
                s
            }
            SvcNewAction::WorkloadUnready => {
                let mut s = apply_svc_input(state, ServiceInput::ReadinessInput(vec![None]));
                s.readiness_env = ReadinessEnv::None;
                s
            }
            SvcNewAction::BackendNeedNone => {
                let mut s =
                    apply_svc_input(state, ServiceInput::BackendNeedInput(BackendNeed::None));
                s.backend_need = BackendNeed::None;
                s
            }
            SvcNewAction::BackendNeedTraffic => {
                let mut s =
                    apply_svc_input(state, ServiceInput::BackendNeedInput(BackendNeed::Traffic));
                s.backend_need = BackendNeed::Traffic;
                s
            }
            SvcNewAction::BackendNeedActive => {
                let mut s =
                    apply_svc_input(state, ServiceInput::BackendNeedInput(BackendNeed::Active));
                s.backend_need = BackendNeed::Active;
                s
            }
            SvcNewAction::TimerFired => {
                // Don't force timer_pending=false — let effects drive it.
                // If the SM processes the timer (guard passes), it calls
                // update_timer_signal which clears the signal via effects.
                // If the timer is a no-op (guard fails), the old signal
                // persists and the timer remains active.
                apply_svc_input(
                    state,
                    ServiceInput::ServiceTimerFired(ServiceTimerKey::IdleTimeout),
                )
            }
            SvcNewAction::RemoveSpec => {
                let mut s = apply_svc_input(state, ServiceInput::SvcSpecInput(None));
                s.spec_present = false;
                s
            }
            SvcNewAction::ChangeActivation => {
                let new_has_activation = !state.sm.has_activation;
                apply_svc_input(
                    state,
                    ServiceInput::SvcSpecInput(Some((
                        ManagementId(0),
                        ServiceSpec {
                            workload: WorkloadId(0),
                            has_activation: new_has_activation,
                            ..Default::default()
                        },
                    ))),
                )
            }
        };

        // Sync activated with SM state (deactivation via idle timer clears
        // demand and transitions to Idle, which means "not activated").
        if matches!(next.sm.state, ServiceState::Idle) && !next.demand_set {
            next.activated = false;
        }

        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut properties = vec![
            // Safety: idle timer is only active when service is Active + has_activation.
            Property::<Self>::always("idle timer only when active+activation", |_model, state| {
                if state.self_destructed {
                    return true;
                }
                if state.sm.idle_timer_active {
                    matches!(state.sm.state, ServiceState::Active { .. }) && state.sm.has_activation
                } else {
                    true
                }
            }),
            // Safety: idle timer pending in env matches SM's idle_timer_active.
            Property::<Self>::always("timer env consistent with SM", |_model, state| {
                if state.self_destructed {
                    return true;
                }
                state.timer_pending == state.sm.idle_timer_active
            }),
            // Safety: demand signal matches state expectations.
            Property::<Self>::always("demand consistent with state", |_model, state| {
                if state.self_destructed {
                    return true;
                }
                match &state.sm.state {
                    ServiceState::Idle => !state.demand_set,
                    ServiceState::NeedBackend | ServiceState::Active { .. } => state.demand_set,
                }
            }),
            // Safety: Active state requires readiness to be present.
            Property::<Self>::always("active implies readiness", |_model, state| {
                if state.self_destructed {
                    return true;
                }
                if matches!(state.sm.state, ServiceState::Active { .. }) {
                    state.readiness_env == ReadinessEnv::Ready
                } else {
                    true
                }
            }),
            // Safety: Idle state only possible with has_activation.
            Property::<Self>::always("idle only with activation", |_model, state| {
                if state.self_destructed {
                    return true;
                }
                if matches!(state.sm.state, ServiceState::Idle) {
                    state.sm.has_activation
                } else {
                    true
                }
            }),
            // Safety: last_readiness cache tracks environment readiness.
            Property::<Self>::always("last_readiness consistent with env", |_model, state| {
                if state.self_destructed {
                    return true;
                }
                state.sm.last_readiness.is_some() == (state.readiness_env == ReadinessEnv::Ready)
            }),
            // Safety: NeedBackend implies readiness is absent. With the
            // last_readiness cache, activate() skips straight to Active when
            // readiness is available, so NeedBackend is unreachable with ready env.
            Property::<Self>::always("NeedBackend implies no readiness", |_model, state| {
                if state.self_destructed {
                    return true;
                }
                if matches!(state.sm.state, ServiceState::NeedBackend) {
                    state.readiness_env == ReadinessEnv::None
                } else {
                    true
                }
            }),
            // Safety: self-destruct only on spec removal.
            Property::<Self>::always("self-destruct only on spec removal", |_model, state| {
                if state.self_destructed {
                    !state.spec_present
                } else {
                    true
                }
            }),
            // Liveness: every path to a terminal state ends in self-destruct.
            // Verifies no other stuck states exist.
            Property::<Self>::eventually("eventually self-destructs", |_model, state| {
                state.self_destructed
            }),
            // Reachability: can reach Active state.
            Property::<Self>::sometimes("can reach active", |_model, state| {
                matches!(state.sm.state, ServiceState::Active { .. })
            }),
        ];

        // Reachability: can return to Idle (activation-based only).
        if self.has_activation {
            properties.push(Property::<Self>::sometimes(
                "can reach idle",
                |_model, state| matches!(state.sm.state, ServiceState::Idle),
            ));
        }
        // Reachability: can have idle timer active (requires activation + backend need).
        if self.has_activation && self.enable_backend_need {
            properties.push(Property::<Self>::sometimes(
                "can have idle timer",
                |_model, state| state.sm.idle_timer_active,
            ));
        }

        properties
    }
}

// ============================================================================
// Symmetry reduction
// ============================================================================

impl Representative for SvcNewModelState {
    fn representative(&self) -> Self {
        let mut s = self.clone();

        // idle_generation: only used to tag timer requests. Our model tracks
        // timer_pending separately.
        s.sm.idle_generation = 0;
        s.timer_generation = 0;

        // Normalize ReadyInfo inside Active state — actual pod/worker IDs
        // don't affect service behavior.
        if let ServiceState::Active { ref mut ready } = s.sm.state {
            ready.pod_id = PodId(0);
            ready.worker_id = WorkerId(0);
        }
        if let Some(ref mut info) = s.sm.last_readiness {
            info.pod_id = PodId(0);
            info.worker_id = WorkerId(0);
        }

        s
    }
}

// ============================================================================
// Tests
// ============================================================================

#[test]
fn service_new_activation_basic() {
    let result = SvcNewModel {
        has_activation: true,
        enable_backend_need: false,
    }
    .checker()
    .symmetry()
    .spawn_dfs()
    .join();

    result.assert_properties();
    eprintln!(
        "Service new (activation, no backend need): {} unique states",
        result.unique_state_count()
    );
}

#[test]
fn service_new_always_on() {
    let result = SvcNewModel {
        has_activation: false,
        enable_backend_need: false,
    }
    .checker()
    .symmetry()
    .spawn_dfs()
    .join();

    result.assert_properties();
    eprintln!(
        "Service new (always-on): {} unique states",
        result.unique_state_count()
    );
}

#[test]
fn service_new_activation_with_backend_need() {
    let result = SvcNewModel {
        has_activation: true,
        enable_backend_need: true,
    }
    .checker()
    .symmetry()
    .spawn_dfs()
    .join();

    result.assert_properties();
    eprintln!(
        "Service new (activation + backend need): {} unique states",
        result.unique_state_count()
    );
}

#[test]
fn service_new_always_on_with_backend_need() {
    let result = SvcNewModel {
        has_activation: false,
        enable_backend_need: true,
    }
    .checker()
    .symmetry()
    .spawn_dfs()
    .join();

    result.assert_properties();
    eprintln!(
        "Service new (always-on + backend need): {} unique states",
        result.unique_state_count()
    );
}

/// Kitchen sink: activation + backend need.
/// This is the most complex service configuration — explores idle timer
/// interactions with activation toggles and backend need fluctuations.
#[test]
fn service_new_kitchen_sink() {
    let result = SvcNewModel {
        has_activation: true,
        enable_backend_need: true,
    }
    .checker()
    .symmetry()
    .spawn_dfs()
    .join();

    result.assert_properties();
    eprintln!(
        "Service new (kitchen sink): {} unique states",
        result.unique_state_count()
    );
}
