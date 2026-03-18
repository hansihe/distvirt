//! Stateright model checking for the EndpointSm.
//!
//! Level 1 individual SM model checking: we instantiate EndpointSm in isolation,
//! feed it inputs via EndpointCtxConcrete, inspect effects to update environment
//! state, and verify safety/liveness properties.
//!
//! The state space is fully explored (no step bound). All monotonic counters
//! are normalized by Representative, so the state space is finite.

use stateright::*;

use super::super::endpoint::*;
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

/// Level 1 model: tests EndpointSm in isolation.
struct EndpointModel {
    /// Whether the endpoint has activation (demand-driven vs always-on).
    has_activation: bool,
    /// Whether to inject backend need changes from workers.
    enable_backend_need: bool,
    /// Whether to inject instantaneous traffic events.
    enable_traffic_event: bool,
}

// ============================================================================
// Model state
// ============================================================================

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct EndpointModelState {
    /// The SM under test.
    sm: EndpointSm,
    /// Whether the endpoint has been configured (config delivered).
    config_present: bool,
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
    /// Whether self-destruct was triggered (config removed).
    self_destructed: bool,
}

// ============================================================================
// Actions
// ============================================================================

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum EndpointAction {
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
    /// Instantaneous traffic event (unit impulse).
    TrafficEvent,
    /// Idle timer fires.
    TimerFired,
    /// Remove the config (ConfigInput(None)) — triggers self-destruct.
    RemoveConfig,
    /// Toggle has_activation via config change.
    ChangeActivation,
}

// ============================================================================
// Helpers
// ============================================================================

/// Deliver an input to the SM, apply effects to env state.
fn apply_endpoint_input(state: &EndpointModelState, input: EndpointInput) -> EndpointModelState {
    let mut next = state.clone();
    let mut ctx = EndpointCtxConcrete::new();
    next.sm.handle(input, &mut ctx);
    let effects = ctx.into_effects();

    if effects.pending_self_destruct {
        next.self_destructed = true;
    }

    if let Some(demand) = effects.demand {
        next.demand_set = demand;
    }

    if let Some(ref timers) = effects.wanted_timers {
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
        pod_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
    }
}

fn make_config(has_activation: bool) -> EndpointConfig {
    EndpointConfig {
        workload: WorkloadId(0),
        has_activation,
        idle_timeout: std::time::Duration::from_secs(30),
        service_ip: std::net::Ipv4Addr::new(10, 0, 0, 100),
        policy: distvirt_worker_protocol::ServicePolicy {
            buffer_frames: 0,
            timeout_ms: 0,
            activator: None,
        },
        dns_entry: None,
    }
}

// ============================================================================
// Model implementation
// ============================================================================

impl Model for EndpointModel {
    type State = EndpointModelState;
    type Action = EndpointAction;

    fn init_states(&self) -> Vec<Self::State> {
        // Start with a fresh SM that has received its initial config.
        let mut sm = EndpointSm::new(self.has_activation);
        let mut ctx = EndpointCtxConcrete::new();
        sm.handle(
            EndpointInput::ConfigInput(Some(make_config(self.has_activation))),
            &mut ctx,
        );
        let effects = ctx.into_effects();

        vec![EndpointModelState {
            demand_set: effects.demand.unwrap_or(!self.has_activation),
            sm,
            config_present: true,
            readiness_env: ReadinessEnv::None,
            backend_need: BackendNeed::None,
            timer_pending: false,
            timer_generation: 0,
            self_destructed: false,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        if state.self_destructed {
            return;
        }

        // Readiness changes.
        match state.readiness_env {
            ReadinessEnv::None => {
                actions.push(EndpointAction::WorkloadReady);
            }
            ReadinessEnv::Ready => {
                actions.push(EndpointAction::WorkloadUnready);
                // Also allow re-delivering ready (workload pod changed).
                actions.push(EndpointAction::WorkloadReady);
            }
        }

        // Backend need changes.
        if self.enable_backend_need {
            if state.backend_need != BackendNeed::None {
                actions.push(EndpointAction::BackendNeedNone);
            }
            if state.backend_need != BackendNeed::Traffic {
                actions.push(EndpointAction::BackendNeedTraffic);
            }
            if state.backend_need != BackendNeed::Active {
                actions.push(EndpointAction::BackendNeedActive);
            }
        }

        // Traffic event.
        if self.enable_traffic_event {
            actions.push(EndpointAction::TrafficEvent);
        }

        // Timer fires — always available. In reality a timer can fire at any
        // moment, including after the SM has logically cancelled it (race
        // between cancellation and delivery). The SM must handle stale fires.
        actions.push(EndpointAction::TimerFired);

        // Config changes.
        if state.config_present {
            actions.push(EndpointAction::RemoveConfig);
            actions.push(EndpointAction::ChangeActivation);
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let next = match action {
            EndpointAction::WorkloadReady => {
                let mut s = apply_endpoint_input(
                    state,
                    EndpointInput::ReadinessInput(Some(make_ready_info())),
                );
                s.readiness_env = ReadinessEnv::Ready;
                s
            }
            EndpointAction::WorkloadUnready => {
                let mut s = apply_endpoint_input(state, EndpointInput::ReadinessInput(None));
                s.readiness_env = ReadinessEnv::None;
                s
            }
            EndpointAction::BackendNeedNone => {
                let mut s =
                    apply_endpoint_input(state, EndpointInput::BackendNeedInput(BackendNeed::None));
                s.backend_need = BackendNeed::None;
                s
            }
            EndpointAction::BackendNeedTraffic => {
                let mut s = apply_endpoint_input(
                    state,
                    EndpointInput::BackendNeedInput(BackendNeed::Traffic),
                );
                s.backend_need = BackendNeed::Traffic;
                s
            }
            EndpointAction::BackendNeedActive => {
                let mut s = apply_endpoint_input(
                    state,
                    EndpointInput::BackendNeedInput(BackendNeed::Active),
                );
                s.backend_need = BackendNeed::Active;
                s
            }
            EndpointAction::TrafficEvent => {
                apply_endpoint_input(state, EndpointInput::TrafficEvent)
            }
            EndpointAction::TimerFired => apply_endpoint_input(
                state,
                EndpointInput::EndpointTimerFired(EndpointTimerKey::IdleTimeout),
            ),
            EndpointAction::RemoveConfig => {
                let mut s = apply_endpoint_input(state, EndpointInput::ConfigInput(None));
                s.config_present = false;
                s
            }
            EndpointAction::ChangeActivation => {
                let new_has_activation = !state.sm.has_activation;
                apply_endpoint_input(
                    state,
                    EndpointInput::ConfigInput(Some(make_config(new_has_activation))),
                )
            }
        };

        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut properties = vec![
            // Safety: idle timer is only active when endpoint is Active + has_activation.
            Property::<Self>::always("idle timer only when active+activation", |_model, state| {
                if state.self_destructed {
                    return true;
                }
                if state.sm.idle_timer_active {
                    matches!(state.sm.state, EndpointState::Active { .. })
                        && state.sm.has_activation
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
                    EndpointState::Idle => !state.demand_set,
                    EndpointState::NeedBackend | EndpointState::Active { .. } => state.demand_set,
                }
            }),
            // Safety: Active state requires readiness to be present.
            Property::<Self>::always("active implies readiness", |_model, state| {
                if state.self_destructed {
                    return true;
                }
                if matches!(state.sm.state, EndpointState::Active { .. }) {
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
                if matches!(state.sm.state, EndpointState::Idle) {
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
                if matches!(state.sm.state, EndpointState::NeedBackend) {
                    state.readiness_env == ReadinessEnv::None
                } else {
                    true
                }
            }),
            // Safety: self-destruct only on config removal.
            Property::<Self>::always("self-destruct only on config removal", |_model, state| {
                if state.self_destructed {
                    !state.config_present
                } else {
                    true
                }
            }),
            // Safety: idle timer implies no sustained need. The timer should
            // only tick when there is no sustained signal holding activation.
            Property::<Self>::always("idle timer implies no sustained need", |_model, state| {
                if state.self_destructed {
                    return true;
                }
                if state.sm.idle_timer_active {
                    state.backend_need == BackendNeed::None
                } else {
                    true
                }
            }),
            // Safety: sustained need prevents idle timeout. When there is
            // sustained demand and the endpoint is Active, the idle timer
            // must not be running.
            Property::<Self>::always("sustained need prevents idle timer", |_model, state| {
                if state.self_destructed {
                    return true;
                }
                if matches!(state.sm.state, EndpointState::Active { .. })
                    && matches!(
                        state.backend_need,
                        BackendNeed::Traffic | BackendNeed::Active
                    )
                {
                    !state.sm.idle_timer_active
                } else {
                    true
                }
            }),
            // Safety: always-on never idles. If activation is disabled, the
            // endpoint must always have demand set and never be in Idle.
            Property::<Self>::always("always-on never idles", |_model, state| {
                if state.self_destructed {
                    return true;
                }
                if !state.sm.has_activation {
                    !matches!(state.sm.state, EndpointState::Idle) && state.demand_set
                } else {
                    true
                }
            }),
            // Liveness: every path ends in self-destruct.
            Property::<Self>::eventually("eventually self-destructs", |_model, state| {
                state.self_destructed
            }),
            // Reachability: can reach Active state.
            Property::<Self>::sometimes("can reach active", |_model, state| {
                matches!(state.sm.state, EndpointState::Active { .. })
            }),
        ];

        // Reachability: can return to Idle (activation-based only).
        if self.has_activation {
            properties.push(Property::<Self>::sometimes(
                "can reach idle",
                |_model, state| matches!(state.sm.state, EndpointState::Idle),
            ));
        }

        // Reachability: can have idle timer active (requires activation + some need mechanism).
        if self.has_activation && (self.enable_backend_need || self.enable_traffic_event) {
            properties.push(Property::<Self>::sometimes(
                "can have idle timer",
                |_model, state| state.sm.idle_timer_active,
            ));
        }

        // Reachability: traffic event can wake from idle.
        if self.has_activation && self.enable_traffic_event {
            properties.push(Property::<Self>::sometimes(
                "can reach active via traffic event",
                |_model, state| {
                    // Active with idle timer = was woken by impulse (no sustained need).
                    matches!(state.sm.state, EndpointState::Active { .. })
                        && state.sm.idle_timer_active
                },
            ));
        }

        // Reachability: full idle timeout cycle.
        if self.has_activation && (self.enable_backend_need || self.enable_traffic_event) {
            properties.push(Property::<Self>::sometimes(
                "can complete idle timeout cycle",
                |_model, state| {
                    // Back to Idle after having been active (idle_generation > 0
                    // means timer was started at least once).
                    matches!(state.sm.state, EndpointState::Idle) && state.sm.idle_generation > 0
                },
            ));
        }

        properties
    }
}

// ============================================================================
// Symmetry reduction
// ============================================================================

impl Representative for EndpointModelState {
    fn representative(&self) -> Self {
        let mut s = self.clone();

        // idle_generation: only used to tag timer requests. Our model tracks
        // timer_pending separately. Normalize to 0/1 (ever-timed vs not).
        if s.sm.idle_generation > 0 {
            s.sm.idle_generation = 1;
        }
        s.timer_generation = 0;

        // Normalize ReadyInfo inside Active state — actual pod/worker IDs
        // and pod_ip don't affect endpoint behavior.
        if let EndpointState::Active { ref mut ready } = s.sm.state {
            ready.pod_id = PodId(0);
            ready.worker_id = WorkerId(0);
            ready.pod_ip = std::net::Ipv4Addr::UNSPECIFIED;
        }
        if let Some(ref mut info) = s.sm.last_readiness {
            info.pod_id = PodId(0);
            info.worker_id = WorkerId(0);
            info.pod_ip = std::net::Ipv4Addr::UNSPECIFIED;
        }

        // Normalize endpoint config fields — don't affect SM logic.
        s.sm.service_ip = std::net::Ipv4Addr::UNSPECIFIED;
        s.sm.service_policy = distvirt_worker_protocol::ServicePolicy {
            buffer_frames: 0,
            timeout_ms: 0,
            activator: None,
        };
        s.sm.dns_entry = None;
        s.sm.idle_timeout = std::time::Duration::ZERO;

        s
    }
}

// ============================================================================
// Tests
// ============================================================================

#[test]
fn endpoint_activation_basic() {
    let result = EndpointModel {
        has_activation: true,
        enable_backend_need: false,
        enable_traffic_event: false,
    }
    .checker()
    .symmetry()
    .spawn_dfs()
    .join();

    result.assert_properties();
    eprintln!(
        "Endpoint (activation, no backend need, no traffic): {} unique states",
        result.unique_state_count()
    );
}

#[test]
fn endpoint_always_on() {
    let result = EndpointModel {
        has_activation: false,
        enable_backend_need: false,
        enable_traffic_event: false,
    }
    .checker()
    .symmetry()
    .spawn_dfs()
    .join();

    result.assert_properties();
    eprintln!(
        "Endpoint (always-on): {} unique states",
        result.unique_state_count()
    );
}

#[test]
fn endpoint_activation_with_backend_need() {
    let result = EndpointModel {
        has_activation: true,
        enable_backend_need: true,
        enable_traffic_event: false,
    }
    .checker()
    .symmetry()
    .spawn_dfs()
    .join();

    result.assert_properties();
    eprintln!(
        "Endpoint (activation + backend need): {} unique states",
        result.unique_state_count()
    );
}

#[test]
fn endpoint_always_on_with_backend_need() {
    let result = EndpointModel {
        has_activation: false,
        enable_backend_need: true,
        enable_traffic_event: false,
    }
    .checker()
    .symmetry()
    .spawn_dfs()
    .join();

    result.assert_properties();
    eprintln!(
        "Endpoint (always-on + backend need): {} unique states",
        result.unique_state_count()
    );
}

#[test]
fn endpoint_activation_with_traffic_event() {
    let result = EndpointModel {
        has_activation: true,
        enable_backend_need: false,
        enable_traffic_event: true,
    }
    .checker()
    .symmetry()
    .spawn_dfs()
    .join();

    result.assert_properties();
    eprintln!(
        "Endpoint (activation + traffic event): {} unique states",
        result.unique_state_count()
    );
}

#[test]
fn endpoint_kitchen_sink() {
    let result = EndpointModel {
        has_activation: true,
        enable_backend_need: true,
        enable_traffic_event: true,
    }
    .checker()
    .symmetry()
    .spawn_dfs()
    .join();

    result.assert_properties();
    eprintln!(
        "Endpoint (kitchen sink): {} unique states",
        result.unique_state_count()
    );
}
