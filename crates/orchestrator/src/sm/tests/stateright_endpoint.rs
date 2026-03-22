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
use distvirt_sm_router::{SequentialIds, SmHandler};

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
    /// Whether to inject active level changes from demand aggregation.
    enable_active_level: bool,
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
    /// Current placement from the workload.
    placement_env: PlacementEnv,
    /// Current active level from demand aggregation.
    active_level: bool,
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
enum PlacementEnv {
    /// No placement known.
    None,
    /// Workload has forwarded worker placement.
    Placed,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum EndpointAction {
    /// Workload becomes ready (ReadinessInput with Some).
    WorkloadReady,
    /// Workload becomes unready (ReadinessInput with None).
    WorkloadUnready,
    /// Workload forwards placement (PlacementInput with Some).
    PlacementSet,
    /// Workload clears placement (PlacementInput with None).
    PlacementClear,
    /// Active level goes high (BackendNeedInput(true)).
    ActiveLevelOn,
    /// Active level goes low (BackendNeedInput(false)).
    ActiveLevelOff,
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

const EP_ID: EndpointId = EndpointId(0);

/// Deliver an input to the SM, apply effects to env state.
/// Also verifies that emitted signals are consistent with SM internal state.
fn apply_endpoint_input(state: &EndpointModelState, input: EndpointInput) -> EndpointModelState {
    let mut next = state.clone();
    let mut alloc = SequentialIds::<NodeKind>::new();
    let mut ctx = EndpointCtxConcrete::new(EP_ID, &mut alloc);
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

    // Verify emitted signals are consistent with SM state.
    // The handler always emits these signals, so they should always be Some.
    if !next.self_destructed {
        if let Some(ref status) = effects.status {
            let expected = match &next.sm.state {
                EndpointState::Idle => EndpointStatus::Idle,
                EndpointState::NeedBackend => EndpointStatus::NeedBackend,
                EndpointState::Active { .. } => EndpointStatus::Active,
            };
            assert_eq!(
                *status, expected,
                "status signal inconsistent with SM state"
            );
        }

        if let Some(ref need) = effects.current_backend_need {
            let expected_demand = if next.sm.has_activation {
                next.sm.active_level || next.sm.idle_timer_active
            } else {
                true
            };
            let expected = if expected_demand {
                BackendNeed::Active
            } else {
                BackendNeed::None
            };
            assert_eq!(
                *need, expected,
                "current_backend_need signal inconsistent with demand"
            );
        }

        if let Some(idle_active) = effects.idle_timer_active {
            assert_eq!(
                idle_active, next.sm.idle_timer_active,
                "idle_timer_active signal inconsistent with SM state"
            );
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
        kind: EndpointKind::Service {
            service_id: ServiceId(1),
            policy: distvirt_worker_protocol::ServicePolicy {
                ports: vec![],
                buffer_frames: 0,
                timeout_ms: 0,
            },
        },
        workload: WorkloadId(0),
        has_activation,
        idle_timeout: std::time::Duration::from_secs(30),
        ip: std::net::Ipv4Addr::new(10, 0, 0, 100),
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
        let mut alloc = SequentialIds::<NodeKind>::new();
        let mut ctx = EndpointCtxConcrete::new(EP_ID, &mut alloc);
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
            placement_env: PlacementEnv::None,
            active_level: false,
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

        // Placement changes.
        match state.placement_env {
            PlacementEnv::None => {
                actions.push(EndpointAction::PlacementSet);
            }
            PlacementEnv::Placed => {
                actions.push(EndpointAction::PlacementClear);
                // Also allow re-delivering placement (worker changed).
                actions.push(EndpointAction::PlacementSet);
            }
        }

        // Active level changes.
        if self.enable_active_level {
            if !state.active_level {
                actions.push(EndpointAction::ActiveLevelOn);
            }
            if state.active_level {
                actions.push(EndpointAction::ActiveLevelOff);
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
                    EndpointInput::ReadinessInput(vec![Some(make_ready_info())]),
                );
                s.readiness_env = ReadinessEnv::Ready;
                s
            }
            EndpointAction::WorkloadUnready => {
                let mut s = apply_endpoint_input(state, EndpointInput::ReadinessInput(vec![None]));
                s.readiness_env = ReadinessEnv::None;
                s
            }
            EndpointAction::PlacementSet => {
                let mut s = apply_endpoint_input(
                    state,
                    EndpointInput::PlacementInput(vec![Some(WorkerId(1))]),
                );
                s.placement_env = PlacementEnv::Placed;
                s
            }
            EndpointAction::PlacementClear => {
                let mut s =
                    apply_endpoint_input(state, EndpointInput::PlacementInput(vec![None]));
                s.placement_env = PlacementEnv::None;
                s
            }
            EndpointAction::ActiveLevelOn => {
                let mut s = apply_endpoint_input(state, EndpointInput::BackendNeedInput(true));
                s.active_level = true;
                s
            }
            EndpointAction::ActiveLevelOff => {
                let mut s = apply_endpoint_input(state, EndpointInput::BackendNeedInput(false));
                s.active_level = false;
                s
            }
            EndpointAction::TrafficEvent => {
                apply_endpoint_input(state, EndpointInput::EndpointDemandTraffic(()))
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
            // Safety: idle timer requires has_activation.
            Property::<Self>::always("idle timer requires activation", |_model, state| {
                if state.self_destructed {
                    return true;
                }
                if state.sm.idle_timer_active {
                    state.sm.has_activation
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
            // Safety: state is correctly derived from (demand, readiness).
            Property::<Self>::always("state derived from demand+readiness", |_model, state| {
                if state.self_destructed {
                    return true;
                }
                let ready = state.readiness_env == ReadinessEnv::Ready;
                match (&state.sm.state, ready, state.demand_set) {
                    (EndpointState::Active { .. }, true, _) => true,
                    (EndpointState::NeedBackend, false, true) => true,
                    (EndpointState::Idle, false, false) => true,
                    _ => false,
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
            // Safety: NeedBackend implies readiness is absent.
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
            // Safety: placement_worker_id tracks placement env.
            Property::<Self>::always("placement env matches SM", |_model, state| {
                if state.self_destructed {
                    return true;
                }
                state.sm.placement_worker_id.is_some()
                    == (state.placement_env == PlacementEnv::Placed)
            }),
            // Safety: env active_level tracks SM active_level.
            Property::<Self>::always("active_level env matches SM", |_model, state| {
                if state.self_destructed {
                    return true;
                }
                state.active_level == state.sm.active_level
            }),
            // Safety: Idle decomposition — for activation endpoints, Idle means
            // no active_level AND no idle timer AND no readiness.
            Property::<Self>::always("Idle decomposition", |_model, state| {
                if state.self_destructed {
                    return true;
                }
                if state.sm.has_activation && matches!(state.sm.state, EndpointState::Idle) {
                    !state.sm.active_level
                        && !state.sm.idle_timer_active
                        && state.readiness_env == ReadinessEnv::None
                } else {
                    true
                }
            }),
            // Safety: idle timer implies endpoint is not Idle (timer sustains demand).
            Property::<Self>::always("timer active implies not Idle", |_model, state| {
                if state.self_destructed {
                    return true;
                }
                if state.sm.idle_timer_active {
                    !matches!(state.sm.state, EndpointState::Idle)
                } else {
                    true
                }
            }),
            // Safety: demand == active_level || idle_timer_active (for activation endpoints).
            // For always-on, demand is unconditionally true.
            Property::<Self>::always("demand matches spec formula", |_model, state| {
                if state.self_destructed {
                    return true;
                }
                let expected = if state.sm.has_activation {
                    state.sm.active_level || state.sm.idle_timer_active
                } else {
                    true
                };
                state.demand_set == expected
            }),
            // Safety: always-on never idles.
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

        // Reachability: can reach NeedBackend (demand present, no readiness).
        if self.enable_active_level || self.enable_traffic_event || !self.has_activation {
            properties.push(Property::<Self>::sometimes(
                "can reach NeedBackend",
                |_model, state| matches!(state.sm.state, EndpointState::NeedBackend),
            ));
        }

        // Reachability: can return to Idle (activation-based only).
        if self.has_activation {
            properties.push(Property::<Self>::sometimes(
                "can reach idle",
                |_model, state| matches!(state.sm.state, EndpointState::Idle),
            ));
        }

        // Reachability: can have idle timer active (requires activation + traffic event).
        if self.has_activation && self.enable_traffic_event {
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

        // Reachability: full idle timeout cycle (requires traffic events to start timer).
        if self.has_activation && self.enable_traffic_event {
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

        // placement_worker_id: only used to construct EndpointInfo backend.
        // The actual WorkerId value doesn't affect SM decision-making.
        if let Some(_) = s.sm.placement_worker_id {
            s.sm.placement_worker_id = Some(WorkerId(0));
        }

        // Normalize endpoint config fields — don't affect SM logic.
        s.sm.ip = std::net::Ipv4Addr::UNSPECIFIED;
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
        enable_active_level: false,
        enable_traffic_event: false,
    }
    .checker()
    .symmetry()
    .spawn_dfs()
    .join();

    result.assert_properties();
    eprintln!(
        "Endpoint (activation, no active level, no traffic): {} unique states",
        result.unique_state_count()
    );
}

#[test]
fn endpoint_always_on() {
    let result = EndpointModel {
        has_activation: false,
        enable_active_level: false,
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
fn endpoint_activation_with_active_level() {
    let result = EndpointModel {
        has_activation: true,
        enable_active_level: true,
        enable_traffic_event: false,
    }
    .checker()
    .symmetry()
    .spawn_dfs()
    .join();

    result.assert_properties();
    eprintln!(
        "Endpoint (activation + active level): {} unique states",
        result.unique_state_count()
    );
}

#[test]
fn endpoint_always_on_with_active_level() {
    let result = EndpointModel {
        has_activation: false,
        enable_active_level: true,
        enable_traffic_event: false,
    }
    .checker()
    .symmetry()
    .spawn_dfs()
    .join();

    result.assert_properties();
    eprintln!(
        "Endpoint (always-on + active level): {} unique states",
        result.unique_state_count()
    );
}

#[test]
fn endpoint_activation_with_traffic_event() {
    let result = EndpointModel {
        has_activation: true,
        enable_active_level: false,
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
        enable_active_level: true,
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
