//! Stateright model checking for the new WorkloadSm.
//!
//! Level 1 individual SM model checking: we instantiate WorkloadSm in isolation,
//! feed it aggregated inputs via CtxConcrete, inspect effects to update
//! environment state, and verify safety/liveness properties.
//!
//! The state space is fully explored (no step bound). All monotonic counters
//! are normalized by Representative, so the state space is finite.
//!
//! Notes on modeling simplifications:
//! - Stale `pod_worker_id` after pod destruction is harmless — the field is
//!   only read when `pod_running` is true, which is cleared on pod death.
//! - `WorkerId(0)` normalization in Representative assumes a single-worker
//!   model. This is fragile if extended to multi-worker scenarios.
//! - Fresh allocator per step produces `PodId(0)` always. This is safe
//!   because Representative normalizes pod IDs anyway.

use distvirt_sm_router::{SequentialIds, SmHandler};
use stateright::*;

use super::super::workload::MAX_RETRIES;
use super::super::*;

// ============================================================================
// Environment types — track what the world outside the SM is doing
// ============================================================================

/// Pod state as seen by the environment (outside the SM).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum PodEnvState {
    /// No pod exists.
    None,
    /// Pod created, pending worker assignment and startup.
    Pending { pod_id: PodId },
    /// Pod reported Running.
    Running { pod_id: PodId },
    /// Pod is suspending (SM sent Suspend intent).
    Suspending { pod_id: PodId },
}

impl PodEnvState {
    fn pod_id(&self) -> Option<PodId> {
        match self {
            PodEnvState::None => None,
            PodEnvState::Pending { pod_id }
            | PodEnvState::Running { pod_id }
            | PodEnvState::Suspending { pod_id } => Some(*pod_id),
        }
    }
}

// ============================================================================
// Model configuration
// ============================================================================

/// Level 1 model: tests WorkloadSm in isolation.
///
/// Note: worker loss is not modeled here because the workload SM doesn't
/// handle it directly — the router delivers it as a PodStatus::Failed via
/// the pod SM. Worker loss → pod failure propagation is tested at the
/// router/integration level (see multi.rs tests).
struct WlNewModel {
    /// Number of services that can generate demand (0..=num_services).
    num_services: usize,
    /// Whether to inject pod failures.
    enable_pod_failure: bool,
    /// Whether to enable suspend-on-idle spec toggling.
    /// When true, the model can deliver specs with suspend_on_idle=true and
    /// toggle it at runtime via ToggleSuspendOnIdle action.
    enable_suspend: bool,
}

// ============================================================================
// Model state — WorkloadSm + environment
// ============================================================================

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct WlNewModelState {
    /// The SM under test. All meaningful state is in its fields.
    sm: WorkloadSm,
    /// What the environment thinks the pod is doing.
    pod_env: PodEnvState,
    /// Current demand level (number of services with demand=true).
    demand_count: u32,
    /// Whether spec has been delivered to the SM.
    spec_present: bool,
    /// Whether a retry backoff timer is pending in the environment.
    timer_pending: bool,
    /// Generation of the pending timer (for matching timer fires).
    timer_generation: u64,
    /// Whether self-destruct was triggered (spec removed).
    self_destructed: bool,
    /// Whether consecutive_failures ever reached max_retries.
    was_ever_max_retries: bool,
}

// ============================================================================
// Actions
// ============================================================================

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum WlNewAction {
    /// Change demand level (delivers DemandInput).
    SetDemand { count: u32 },
    /// Deliver initial spec (delivers SpecInput(Some)).
    DeliverSpec,
    /// Change image in spec (delivers SpecInput(Some) with new image, bumps version).
    ChangeImage,
    /// Toggle suspend_on_idle in spec (delivers SpecInput(Some) with same image).
    ToggleSuspendOnIdle,
    /// Remove spec (delivers SpecInput(None)) — triggers self-destruct.
    RemoveSpec,
    /// Pod transitions to Running (delivers PodStatusInput([Running])).
    PodRunning,
    /// Pod fails (delivers PodStatusInput([Failed])).
    PodFailed,
    /// Pod displaced by infrastructure (delivers PodStatusInput([Displaced])).
    PodDisplaced,
    /// Pod finishes gracefully (delivers PodStatusInput([Finished])).
    PodFinished,
    /// Pod suspended successfully (delivers PodStatusInput([Suspended{..}])).
    PodSuspended,
    /// Pod gone — delivers PodStatusInput([]) to exercise safety-net cleanup.
    PodGone,
    /// Retry backoff timer fires (delivers WorkloadTimerFired).
    TimerFired,
    /// Admin restart command.
    AdminRestart,
    /// Admin scavenge command.
    AdminScavenge,
}

// ============================================================================
// Helpers
// ============================================================================

/// Deliver an input to the SM using CtxConcrete, apply effects to env state.
fn apply_input(state: &WlNewModelState, input: WorkloadInput) -> WlNewModelState {
    let mut next = state.clone();
    let mut alloc = SequentialIds::<NodeKind>::new();
    // Seed allocator to match current pod ID generation state.
    // The SM creates pods via ctx.create_pod() which calls alloc.
    // We need the allocator counter to be consistent.

    let wl_id = WorkloadId(0);
    let mut ctx = WorkloadCtxConcrete::new(wl_id, &mut alloc);
    next.sm.handle(input, &mut ctx);
    let effects = ctx.into_effects();

    // Check self-destruct.
    if effects.pending_self_destruct {
        next.self_destructed = true;
    }

    // Process effects to update environment state.

    // Check for pod creation.
    for create in &effects.pending_creates {
        if let PendingCreate::Pod(pod_id, _pod_sm) = create {
            next.pod_env = PodEnvState::Pending { pod_id: *pod_id };
        }
    }

    // Check for edge changes (workload_to_pod_edges = [] means pod abandoned).
    if let Some(ref edges) = effects.pod_ownership {
        if edges.is_empty() && next.pod_env.pod_id().is_some() {
            // Pod abandoned — in the real system it would self-destruct.
            // For the model, clear pod env immediately.
            next.pod_env = PodEnvState::None;
        }
    }

    // Sync pod_env with SM state before checking intent.
    // The SM may have set pod_running=true (from PodStatusInput) in the same
    // handle() call that also calls reconcile() → sets intent=Suspend.
    // We need pod_env to reflect Running before we can transition it to Suspending.
    if next.sm.pod_running {
        if let PodEnvState::Pending { pod_id } = next.pod_env {
            next.pod_env = PodEnvState::Running { pod_id };
        }
    }

    // Check for pod intent changes (Suspend intent transitions env).
    if let Some(ref intent) = effects.pod_intent {
        if *intent == PodIntent::Suspend {
            if let PodEnvState::Running { pod_id } = next.pod_env {
                next.pod_env = PodEnvState::Suspending { pod_id };
            }
        }
    }

    // Check timer signal to update pending timer state.
    if let Some(ref timers) = effects.wanted_timers {
        if let Some(req) = timers.first() {
            next.timer_pending = true;
            next.timer_generation = req.generation;
        } else {
            next.timer_pending = false;
        }
    }

    // Track if max retries was ever reached.
    if next.sm.consecutive_failures >= next.sm.max_retries {
        next.was_ever_max_retries = true;
    }

    next
}

/// Build a DemandInfo with the given count and synthetic service IDs.
fn make_demand(count: u32) -> DemandInfo {
    let service_ids: Vec<ServiceId> = (0..count).map(|i| ServiceId(i as u64 + 1)).collect();
    DemandInfo {
        demand_count: count,
        service_ids,
    }
}

/// Build a WorkloadSpec with the given image and the current SM's suspend_on_idle.
fn make_spec(image: &str, suspend_on_idle: bool) -> WorkloadSpec {
    WorkloadSpec {
        image: image.into(),
        suspend_on_idle,
        ..Default::default()
    }
}

// ============================================================================
// Model implementation
// ============================================================================

impl Model for WlNewModel {
    type State = WlNewModelState;
    type Action = WlNewAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![WlNewModelState {
            sm: WorkloadSm::new(),
            pod_env: PodEnvState::None,
            demand_count: 0,
            spec_present: false,
            timer_pending: false,
            timer_generation: 0,
            self_destructed: false,
            was_ever_max_retries: false,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        // Don't generate actions for destroyed workloads.
        if state.self_destructed {
            return;
        }

        // Demand changes: any value from 0 to num_services, different from current.
        for count in 0..=(self.num_services as u32) {
            if count != state.demand_count {
                actions.push(WlNewAction::SetDemand { count });
            }
        }

        // Spec delivery / change / removal.
        if !state.spec_present {
            actions.push(WlNewAction::DeliverSpec);
        } else {
            // Spec is present — allow removal and changes.
            actions.push(WlNewAction::RemoveSpec);

            if state.sm.has_demand || state.sm.pod_id.is_some() || state.sm.committed_to_boot {
                // Only explore image changes when the SM has something active.
                // Changing image while fully dormant has no behavioral effect
                // beyond bumping spec_version, which Representative normalizes.
                actions.push(WlNewAction::ChangeImage);
            }

            // Toggle suspend_on_idle via spec change.
            if self.enable_suspend {
                actions.push(WlNewAction::ToggleSuspendOnIdle);
            }
        }

        // Pod lifecycle events — depend on pod env state.
        match &state.pod_env {
            PodEnvState::Pending { .. } => {
                actions.push(WlNewAction::PodRunning);
                if self.enable_pod_failure {
                    actions.push(WlNewAction::PodFailed);
                    actions.push(WlNewAction::PodDisplaced);
                }
            }
            PodEnvState::Running { .. } => {
                if self.enable_pod_failure {
                    actions.push(WlNewAction::PodFailed);
                    actions.push(WlNewAction::PodDisplaced);
                }
                actions.push(WlNewAction::PodFinished);
            }
            PodEnvState::Suspending { .. } => {
                actions.push(WlNewAction::PodSuspended);
                if self.enable_pod_failure {
                    actions.push(WlNewAction::PodFailed);
                    actions.push(WlNewAction::PodDisplaced);
                }
            }
            PodEnvState::None => {}
        }

        // PodGone: deliver empty PodStatusInput to exercise safety-net cleanup.
        // Available when SM thinks it has a pod but env still has one too.
        if state.pod_env != PodEnvState::None && state.sm.pod_id.is_some() {
            actions.push(WlNewAction::PodGone);
        }

        // Timer fires.
        if state.timer_pending {
            actions.push(WlNewAction::TimerFired);
        }

        // Admin commands — available when there's something to act on.
        if state.sm.pod_id.is_some() || state.sm.committed_to_boot || state.sm.in_backoff {
            actions.push(WlNewAction::AdminRestart);
            actions.push(WlNewAction::AdminScavenge);
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut next = match action {
            WlNewAction::SetDemand { count } => {
                let mut s = apply_input(state, WorkloadInput::DemandInput(make_demand(count)));
                s.demand_count = count;
                s
            }
            WlNewAction::DeliverSpec => {
                // Initial spec delivery. When enable_suspend, start with
                // suspend_on_idle=true to exercise suspend paths from the start.
                let suspend = self.enable_suspend;
                let mut s = apply_input(
                    state,
                    WorkloadInput::SpecInput(Some((ManagementId(0), make_spec("app:v1", suspend)))),
                );
                s.spec_present = true;
                s
            }
            WlNewAction::ChangeImage => {
                // Toggle between v1 and v2 to always produce a different image.
                let new_image = if state.sm.current_image.as_deref() == Some("app:v1") {
                    "app:v2"
                } else {
                    "app:v1"
                };
                apply_input(
                    state,
                    WorkloadInput::SpecInput(Some((
                        ManagementId(0),
                        make_spec(new_image, state.sm.suspend_on_idle),
                    ))),
                )
            }
            WlNewAction::ToggleSuspendOnIdle => {
                // Same image, flipped suspend_on_idle.
                let current_image = state.sm.current_image.clone().unwrap_or_default();
                apply_input(
                    state,
                    WorkloadInput::SpecInput(Some((
                        ManagementId(0),
                        make_spec(&current_image, !state.sm.suspend_on_idle),
                    ))),
                )
            }
            WlNewAction::RemoveSpec => {
                let mut s = apply_input(state, WorkloadInput::SpecInput(None));
                s.spec_present = false;
                s
            }
            WlNewAction::PodRunning => {
                let _pod_id = state.pod_env.pod_id().unwrap();
                // First deliver PodStatusInput with Running status.
                let s = apply_input(
                    state,
                    WorkloadInput::PodStatusInput(vec![PodStatus::Running]),
                );
                // Also deliver PodWorkerInput with a worker assignment.
                apply_input(&s, WorkloadInput::PodWorkerInput(vec![Some(WorkerId(1))]))
            }
            WlNewAction::PodFailed => apply_input(
                state,
                WorkloadInput::PodStatusInput(vec![PodStatus::Failed]),
            ),
            WlNewAction::PodDisplaced => apply_input(
                state,
                WorkloadInput::PodStatusInput(vec![PodStatus::Displaced]),
            ),
            WlNewAction::PodFinished => apply_input(
                state,
                WorkloadInput::PodStatusInput(vec![PodStatus::Finished]),
            ),
            WlNewAction::PodSuspended => {
                // Use a synthetic artifact ID based on SM's artifact counter.
                let artifact_id = ArtifactId(state.sm.artifact_counter + 1);
                apply_input(
                    state,
                    WorkloadInput::PodStatusInput(vec![PodStatus::Suspended { artifact_id }]),
                )
            }
            WlNewAction::PodGone => apply_input(state, WorkloadInput::PodStatusInput(vec![])),
            WlNewAction::TimerFired => {
                let mut s = apply_input(
                    state,
                    WorkloadInput::WorkloadTimerFired(WorkloadTimerKey::RetryBackoff),
                );
                // Timer consumed.
                s.timer_pending = false;
                s
            }
            WlNewAction::AdminRestart => {
                apply_input(state, WorkloadInput::AdminCommand(AdminCmd::Restart))
            }
            WlNewAction::AdminScavenge => {
                apply_input(state, WorkloadInput::AdminCommand(AdminCmd::Scavenge))
            }
        };

        // Sync pod_env with SM's view after effect processing:
        // If the SM thinks it has a running pod but env doesn't match, update.
        if next.sm.pod_running {
            if let PodEnvState::Pending { pod_id } = next.pod_env {
                next.pod_env = PodEnvState::Running { pod_id };
            }
        }

        // If SM cleared pod_id, clear env too (safety net).
        if next.sm.pod_id.is_none() && next.pod_env != PodEnvState::None {
            // Check if env still has a pod that SM doesn't know about.
            // This happens when SM destroys pod via edge removal.
            next.pod_env = PodEnvState::None;
        }

        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut props = vec![
            // Safety: consecutive failures never exceed MAX_RETRIES.
            Property::<Self>::always("consecutive failures bounded", |_model, state| {
                if state.self_destructed {
                    return true;
                }
                state.sm.consecutive_failures <= MAX_RETRIES
            }),
            // Safety: if SM reports Failed status, failures must be at max.
            Property::<Self>::always("failed implies max retries", |_model, state| {
                if state.self_destructed {
                    return true;
                }
                let is_failed = state.sm.consecutive_failures >= state.sm.max_retries
                    && (state.sm.has_demand || state.sm.committed_to_boot);
                if !state.sm.has_spec && !state.sm.has_demand {
                    // Dormant — anything is fine.
                    true
                } else if is_failed {
                    state.sm.consecutive_failures >= state.sm.max_retries
                } else {
                    true
                }
            }),
            // Safety: if in_backoff, timer should be pending.
            Property::<Self>::always("backoff has timer", |_model, state| {
                if state.self_destructed {
                    return true;
                }
                if state.sm.in_backoff {
                    state.timer_pending
                } else {
                    true
                }
            }),
            // Safety: if pod_running, consecutive_failures must be 0.
            Property::<Self>::always("running resets failures", |_model, state| {
                if state.self_destructed {
                    return true;
                }
                if state.sm.pod_running {
                    state.sm.consecutive_failures == 0
                } else {
                    true
                }
            }),
            // Safety: dormant state has no timer.
            Property::<Self>::always("dormant has no timer", |_model, state| {
                if state.self_destructed {
                    return true;
                }
                let is_dormant = !state.sm.has_demand
                    && !state.sm.committed_to_boot
                    && state.sm.pod_id.is_none()
                    && !state.sm.in_backoff;
                if is_dormant {
                    !state.timer_pending
                } else {
                    true
                }
            }),
            // Safety: pod_id.is_some() iff pod_env is not None.
            Property::<Self>::always("pod tracking consistent", |_model, state| {
                if state.self_destructed {
                    return true;
                }
                state.sm.pod_id.is_some() == (state.pod_env != PodEnvState::None)
            }),
            // Safety: awaiting_suspend is only true when pod is in Suspending env state.
            Property::<Self>::always(
                "awaiting_suspend implies suspending env",
                |_model, state| {
                    if state.self_destructed {
                        return true;
                    }
                    if state.sm.awaiting_suspend {
                        matches!(state.pod_env, PodEnvState::Suspending { .. })
                    } else {
                        true
                    }
                },
            ),
            // Safety: wants_pod and no pod_id means something is wrong
            // (reconcile should have created one, unless in_backoff or failed).
            // Assumption: each action is a full handle() call which always
            // ends with reconcile(), so the transient state where wants_pod
            // is true but pod_id is None should never be observable here.
            Property::<Self>::always("wants_pod consistency", |_model, state| {
                if state.self_destructed {
                    return true;
                }
                if state.sm.wants_pod && state.sm.pod_id.is_none() {
                    false
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
            // Safety: no suspended artifact while a pod is active.
            // If a pod exists (pod_id is Some), the suspended_artifact should
            // have been consumed (taken) during pod creation, or discarded.
            Property::<Self>::always("no artifact during active pod", |_model, state| {
                if state.self_destructed {
                    return true;
                }
                if state.sm.pod_id.is_some() {
                    state.sm.suspended_artifact.is_none()
                } else {
                    true
                }
            }),
            // Safety: suspend_on_idle=false implies no artifact and no awaiting_suspend.
            // When suspend is disabled, there should never be suspend-related state.
            Property::<Self>::always("no suspend state when disabled", |_model, state| {
                if state.self_destructed {
                    return true;
                }
                if !state.sm.suspend_on_idle {
                    // awaiting_suspend could briefly be true if we just toggled
                    // suspend_on_idle off — but destroy_current_pod clears it.
                    // suspended_artifact is cleared on true→false transition.
                    state.sm.suspended_artifact.is_none() && !state.sm.awaiting_suspend
                } else {
                    true
                }
            }),
            // Liveness: every path to a terminal state ends in self-destruct.
            // Verifies no other stuck states exist.
            Property::<Self>::eventually("eventually self-destructs", |_model, state| {
                state.self_destructed
            }),
            // Reachability: can reach a state where pod is running.
            Property::<Self>::sometimes("can reach running", |_model, state| state.sm.pod_running),
            // Reachability: can return to dormant after having a pod.
            Property::<Self>::sometimes("can reach dormant after pod", |_model, state| {
                state.sm.has_spec
                    && state.sm.pod_id.is_none()
                    && !state.sm.has_demand
                    && !state.sm.committed_to_boot
                    && !state.sm.in_backoff
            }),
        ];

        // Reachability: can recover from failed (only meaningful with pod failures).
        // Checks that after hitting max retries, the system can recover (e.g. via
        // demand drop resetting failures) and get a pod running again.
        if self.enable_pod_failure {
            props.push(Property::<Self>::sometimes(
                "can recover from failed",
                |_model, state| state.was_ever_max_retries && state.sm.pod_running,
            ));
        }

        // Reachability: can reach suspended state (only with suspend enabled).
        if self.enable_suspend {
            props.push(Property::<Self>::sometimes(
                "can reach suspended",
                |_model, state| state.sm.suspended_artifact.is_some(),
            ));
        }

        props
    }
}

// ============================================================================
// Symmetry reduction — normalize counters that don't affect behavior
// ============================================================================

/// Stateright deduplicates states by fingerprint. Without normalization, the
/// model's monotonically increasing counters (spec_version, backoff_generation,
/// artifact_counter) and auto-generated IDs (PodId, WorkerId) cause states that
/// are *behaviorally identical* to hash differently. For example, two states
/// that differ only in `backoff_generation: 3` vs `backoff_generation: 7` will
/// produce the exact same successor states (up to counter values), but
/// stateright treats them as distinct, exploding the state space.
///
/// `Representative` collapses these equivalence classes by mapping each state
/// to a canonical form for fingerprinting. Stateright still explores from the
/// *original* state (counters keep their real values during execution), so SM
/// logic is unaffected — only dedup uses the normalized form.
///
/// This reduced the pod-failure test from 49,734 → 904 unique states (55x).
impl Representative for WlNewModelState {
    fn representative(&self) -> Self {
        let mut s = self.clone();

        // spec_version vs launched_with_spec_version: the SM only checks
        // equality (in on_pod_running). Normalize to canonical values.
        if s.sm.spec_version == s.sm.launched_with_spec_version {
            s.sm.spec_version = 0;
            s.sm.launched_with_spec_version = 0;
        } else {
            s.sm.spec_version = 1;
            s.sm.launched_with_spec_version = 0;
        }

        // backoff_generation: only used to tag timer requests. Our model
        // tracks timer_pending separately and doesn't match on generation.
        s.sm.backoff_generation = 0;
        s.timer_generation = 0;

        // artifact_counter: generates unique artifact IDs but the handler
        // never calls next_artifact_id() — artifacts come from external
        // PodSuspended events.
        s.sm.artifact_counter = 0;

        // pod_id inside SM: only used as Some/None check and to set edges.
        // The actual PodId value doesn't affect behavior.
        if let Some(_) = s.sm.pod_id {
            s.sm.pod_id = Some(PodId(0));
        }

        // pod_env: normalize PodId values (only one pod exists at a time).
        s.pod_env = match s.pod_env {
            PodEnvState::None => PodEnvState::None,
            PodEnvState::Pending { .. } => PodEnvState::Pending { pod_id: PodId(0) },
            PodEnvState::Running { .. } => PodEnvState::Running { pod_id: PodId(0) },
            PodEnvState::Suspending { .. } => PodEnvState::Suspending { pod_id: PodId(0) },
        };

        // suspended_artifact: only used as Some/None check (has artifact to
        // resume from?). The actual ArtifactId value doesn't matter.
        if let Some(_) = s.sm.suspended_artifact {
            s.sm.suspended_artifact = Some(ArtifactId(0));
        }

        // pod_worker_id: only used to construct ReadyInfo. The actual WorkerId
        // value doesn't affect SM decision-making.
        if let Some(_) = s.sm.pod_worker_id {
            s.sm.pod_worker_id = Some(WorkerId(0));
        }

        // current_image: the SM only checks whether the delivered image differs
        // from current_image. The model's ChangeImage action always toggles, so
        // the actual string value doesn't matter — only Some vs None.
        if let Some(_) = s.sm.current_image {
            s.sm.current_image = Some(String::new());
        }

        s
    }
}

// ============================================================================
// Tests
// ============================================================================

#[test]
fn workload_new_basic() {
    let result = WlNewModel {
        num_services: 1,
        enable_pod_failure: false,
        enable_suspend: false,
    }
    .checker()
    .symmetry()
    .spawn_dfs()
    .join();

    result.assert_properties();
    eprintln!(
        "Workload new (1 svc, no failures): {} unique states",
        result.unique_state_count()
    );
}

#[test]
fn workload_new_two_services() {
    let result = WlNewModel {
        num_services: 2,
        enable_pod_failure: false,
        enable_suspend: false,
    }
    .checker()
    .symmetry()
    .spawn_dfs()
    .join();

    result.assert_properties();
    eprintln!(
        "Workload new (2 svc, no failures): {} unique states",
        result.unique_state_count()
    );
}

#[test]
fn workload_new_with_pod_failure() {
    let result = WlNewModel {
        num_services: 1,
        enable_pod_failure: true,
        enable_suspend: false,
    }
    .checker()
    .symmetry()
    .spawn_dfs()
    .join();

    result.assert_properties();
    eprintln!(
        "Workload new (pod failure): {} unique states",
        result.unique_state_count()
    );
}

#[test]
fn workload_new_suspend_basic() {
    let result = WlNewModel {
        num_services: 1,
        enable_pod_failure: false,
        enable_suspend: true,
    }
    .checker()
    .symmetry()
    .spawn_dfs()
    .join();

    result.assert_properties();
    eprintln!(
        "Workload new (suspend, no failures): {} unique states",
        result.unique_state_count()
    );
}

#[test]
fn workload_new_suspend_with_failures() {
    let result = WlNewModel {
        num_services: 1,
        enable_pod_failure: true,
        enable_suspend: true,
    }
    .checker()
    .symmetry()
    .spawn_dfs()
    .join();

    result.assert_properties();
    eprintln!(
        "Workload new (suspend + failures): {} unique states",
        result.unique_state_count()
    );
}

/// Kitchen sink: all features enabled, 2 services, generous step limit.
/// Explores the full action space to catch interaction bugs between
/// suspend, failures, demand fluctuations, spec changes, and admin commands.
#[test]
fn workload_new_kitchen_sink() {
    let result = WlNewModel {
        num_services: 2,
        enable_pod_failure: true,
        enable_suspend: true,
    }
    .checker()
    .symmetry()
    .spawn_dfs()
    .join();

    result.assert_properties();
    eprintln!(
        "Workload new (kitchen sink): {} unique states",
        result.unique_state_count()
    );
}
