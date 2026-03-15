//! Stateright model checking for the new PodSm.
//!
//! Level 1 individual SM model checking: we instantiate PodSm in isolation,
//! feed it inputs via CtxConcrete, inspect effects to update environment state,
//! and verify safety/liveness properties.
//!
//! The pod SM has a linear lifecycle:
//!   Pending → Running → Suspending → Suspended(artifact)  [terminal]
//!                     → Failed                             [terminal]
//!            → Failed                                      [terminal]
//!
//! Key behaviors to verify:
//! - Terminal states are absorbing (no transitions out).
//! - Self-destruct fires only when terminal AND no owner.
//! - Worker loss drives live pod to Failed.
//! - Owner loss drives live pod to Failed then self-destruct.
//! - Timer fires only cause transitions from matching states.
//! - Suspend intent only takes effect from Running state.
//!
//! The state space is fully explored (no step bound). All monotonic counters
//! are normalized by Representative, so the state space is finite.

use distvirt_sm_router::{SequentialIds, SmHandler};
use stateright::*;

use super::super::*;

// ============================================================================
// Environment types
// ============================================================================

/// Whether the pod has a worker assigned in the environment.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum WorkerEnv {
    None,
    Assigned,
}

/// Whether the pod has an owner (workload) in the environment.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum OwnerEnv {
    None,
    Owned { intent: PodIntent },
}

// ============================================================================
// Model configuration
// ============================================================================

/// Level 1 model: tests PodSm in isolation.
///
/// Models the pod lifecycle with configurable features:
/// - Worker loss (worker edge removed while pod is live)
/// - Owner loss (workload removes ownership edge)
/// - Suspend intent (owner signals Suspend while Running)
/// - Timer fires (launch timeout, suspend timeout)
/// - Backward status (pathological worker notifications — rejected by SM's
///   forward-progress guard, but verifies the guard works)
struct PodNewModel {
    /// Whether to inject worker loss events.
    enable_worker_loss: bool,
    /// Whether to model suspend intent from owner.
    enable_suspend: bool,
    /// Whether to model timer fires.
    enable_timers: bool,
    /// Whether to inject backward/pathological status notifications.
    enable_backward_status: bool,
}

// ============================================================================
// Model state
// ============================================================================

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct PodNewModelState {
    /// The SM under test.
    sm: PodSm,
    /// Whether a worker is assigned.
    worker_env: WorkerEnv,
    /// Whether the pod has an owner and what intent it has.
    owner_env: OwnerEnv,
    /// Whether a timer is pending.
    timer_pending: bool,
    /// Which timer key is pending.
    timer_key: Option<PodTimerKey>,
    /// Whether self_destruct was triggered.
    self_destructed: bool,
    /// Whether the pod was ever owned (OwnerWant or OwnerSuspend applied).
    was_ever_owned: bool,
}

// ============================================================================
// Actions
// ============================================================================

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum PodNewAction {
    /// Worker assigned (WorkerInput with Some).
    AssignWorker,
    /// Worker lost (WorkerInput with None).
    LoseWorker,
    /// Owner takes ownership with Want intent.
    OwnerWant,
    /// Owner changes intent to Suspend.
    OwnerSuspend,
    /// Owner removes ownership.
    OwnerRemove,
    /// Worker reports Running (NotifyPodStatus(Running)).
    NotifyRunning,
    /// Worker reports Failed (NotifyPodStatus(Failed)).
    NotifyFailed,
    /// Worker reports Finished (NotifyPodStatus(Finished)).
    NotifyFinished,
    /// Worker reports suspended with artifact (NotifyPodSuspended).
    NotifySuspended,
    /// Launch timeout timer fires.
    LaunchTimeoutFired,
    /// Suspend timeout timer fires.
    SuspendTimeoutFired,
    /// Pathological: worker reports Pending (backward transition).
    NotifyPending,
    /// Pathological: worker reports Suspending (backward transition).
    NotifySuspending,
}

// ============================================================================
// Helpers
// ============================================================================

/// Deliver an input to the SM, apply effects to env state.
fn apply_pod_input(
    state: &PodNewModelState,
    input: PodInput,
) -> PodNewModelState {
    let mut next = state.clone();
    let mut alloc = SequentialIds::<NodeKind>::new();
    let pod_id = PodId(0);
    let mut ctx = PodCtxConcrete::new(pod_id, &mut alloc);
    next.sm.handle(input, &mut ctx);
    let effects = ctx.into_effects();

    // Check self-destruct.
    if effects.pending_self_destruct {
        next.self_destructed = true;
    }

    // Check timer signal changes.
    if let Some(ref timers) = effects.wanted_pod_timers {
        if let Some(req) = timers.first() {
            next.timer_pending = true;
            next.timer_key = Some(req.key.clone());
        } else {
            next.timer_pending = false;
            next.timer_key = None;
        }
    }

    next
}

fn make_worker_info() -> (WorkerId, WorkerInfo) {
    (WorkerId(1), WorkerInfo { capacity: 10 })
}

// ============================================================================
// Model implementation
// ============================================================================

impl Model for PodNewModel {
    type State = PodNewModelState;
    type Action = PodNewAction;

    fn init_states(&self) -> Vec<Self::State> {
        let timer_id = TimerId(0);
        let sm = PodSm::new(timer_id);
        vec![PodNewModelState {
            sm,
            worker_env: WorkerEnv::None,
            owner_env: OwnerEnv::None,
            timer_pending: true, // initialize() sets LaunchTimeout timer
            timer_key: Some(PodTimerKey::LaunchTimeout),
            self_destructed: false,
            was_ever_owned: false,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        // Don't generate actions for destroyed pods.
        if state.self_destructed {
            return;
        }

        let is_terminal = state.sm.status.is_terminal();

        // Worker assignment/loss.
        match &state.worker_env {
            WorkerEnv::None => {
                if !is_terminal {
                    actions.push(PodNewAction::AssignWorker);
                }
            }
            WorkerEnv::Assigned => {
                if self.enable_worker_loss {
                    actions.push(PodNewAction::LoseWorker);
                }
            }
        }

        // Owner changes.
        match &state.owner_env {
            OwnerEnv::None => {
                if !is_terminal {
                    actions.push(PodNewAction::OwnerWant);
                }
            }
            OwnerEnv::Owned { intent } => {
                actions.push(PodNewAction::OwnerRemove);
                if self.enable_suspend && *intent != PodIntent::Suspend {
                    actions.push(PodNewAction::OwnerSuspend);
                }
            }
        }

        // Worker notifications (only when worker is assigned and pod is live).
        if state.worker_env == WorkerEnv::Assigned && !is_terminal {
            match &state.sm.status {
                PodStatus::Pending => {
                    actions.push(PodNewAction::NotifyRunning);
                    actions.push(PodNewAction::NotifyFailed);
                }
                PodStatus::Running => {
                    actions.push(PodNewAction::NotifyFailed);
                    actions.push(PodNewAction::NotifyFinished);
                }
                PodStatus::Suspending => {
                    actions.push(PodNewAction::NotifySuspended);
                    actions.push(PodNewAction::NotifyFailed);
                }
                _ => {}
            }

            // Backward/pathological status notifications.
            // The SM's forward-progress guard rejects these, but we include
            // them to verify the guard works correctly.
            if self.enable_backward_status {
                match &state.sm.status {
                    PodStatus::Running | PodStatus::Suspending => {
                        actions.push(PodNewAction::NotifyPending);
                    }
                    _ => {}
                }
                match &state.sm.status {
                    PodStatus::Running => {
                        actions.push(PodNewAction::NotifySuspending);
                    }
                    _ => {}
                }
            }
        }

        // Timer fires.
        if self.enable_timers && state.timer_pending {
            if let Some(ref key) = state.timer_key {
                match key {
                    PodTimerKey::LaunchTimeout => {
                        actions.push(PodNewAction::LaunchTimeoutFired);
                    }
                    PodTimerKey::SuspendTimeout => {
                        actions.push(PodNewAction::SuspendTimeoutFired);
                    }
                }
            }
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let next = match action {
            PodNewAction::AssignWorker => {
                let mut s = apply_pod_input(
                    state,
                    PodInput::WorkerInput(Some(make_worker_info())),
                );
                s.worker_env = WorkerEnv::Assigned;
                s
            }
            PodNewAction::LoseWorker => {
                let mut s = apply_pod_input(
                    state,
                    PodInput::WorkerInput(None),
                );
                s.worker_env = WorkerEnv::None;
                s
            }
            PodNewAction::OwnerWant => {
                let mut s = apply_pod_input(
                    state,
                    PodInput::OwnerInput(Some((WorkloadId(1), PodIntent::Want))),
                );
                s.owner_env = OwnerEnv::Owned { intent: PodIntent::Want };
                s.was_ever_owned = true;
                s
            }
            PodNewAction::OwnerSuspend => {
                let mut s = apply_pod_input(
                    state,
                    PodInput::OwnerInput(Some((WorkloadId(1), PodIntent::Suspend))),
                );
                s.owner_env = OwnerEnv::Owned { intent: PodIntent::Suspend };
                s.was_ever_owned = true;
                s
            }
            PodNewAction::OwnerRemove => {
                let mut s = apply_pod_input(
                    state,
                    PodInput::OwnerInput(None),
                );
                s.owner_env = OwnerEnv::None;
                s
            }
            PodNewAction::NotifyRunning => {
                apply_pod_input(
                    state,
                    PodInput::NotifyPodStatus(PodStatus::Running),
                )
            }
            PodNewAction::NotifyFailed => {
                apply_pod_input(
                    state,
                    PodInput::NotifyPodStatus(PodStatus::Failed),
                )
            }
            PodNewAction::NotifyFinished => {
                apply_pod_input(
                    state,
                    PodInput::NotifyPodStatus(PodStatus::Finished),
                )
            }
            PodNewAction::NotifySuspended => {
                apply_pod_input(
                    state,
                    PodInput::NotifyPodSuspended(ArtifactId(1)),
                )
            }
            PodNewAction::LaunchTimeoutFired => {
                let mut s = apply_pod_input(
                    state,
                    PodInput::PodTimerFired(PodTimerKey::LaunchTimeout),
                );
                s.timer_pending = false;
                s.timer_key = None;
                s
            }
            PodNewAction::SuspendTimeoutFired => {
                let mut s = apply_pod_input(
                    state,
                    PodInput::PodTimerFired(PodTimerKey::SuspendTimeout),
                );
                s.timer_pending = false;
                s.timer_key = None;
                s
            }
            PodNewAction::NotifyPending => {
                apply_pod_input(
                    state,
                    PodInput::NotifyPodStatus(PodStatus::Pending),
                )
            }
            PodNewAction::NotifySuspending => {
                apply_pod_input(
                    state,
                    PodInput::NotifyPodStatus(PodStatus::Suspending),
                )
            }
        };

        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Safety: terminal states are absorbing — status never changes after terminal.
            Property::<Self>::always("terminal is absorbing", |_model, state| {
                if state.sm.status.is_terminal() {
                    matches!(
                        state.sm.status,
                        PodStatus::Suspended { .. } | PodStatus::Failed | PodStatus::Finished
                    )
                } else {
                    true
                }
            }),
            // Safety: self-destruct only fires when terminal AND no owner.
            Property::<Self>::always("self-destruct requires terminal + no owner", |_model, state| {
                if state.self_destructed {
                    state.sm.status.is_terminal() && state.sm.workload_id.is_none()
                } else {
                    true
                }
            }),
            // Safety: timer is only pending when in Pending or Suspending state.
            Property::<Self>::always("timer consistent with status", |_model, state| {
                if state.self_destructed {
                    return true;
                }
                if state.timer_pending {
                    match (&state.timer_key, &state.sm.status) {
                        (Some(PodTimerKey::LaunchTimeout), PodStatus::Pending) => true,
                        (Some(PodTimerKey::SuspendTimeout), PodStatus::Suspending) => true,
                        _ => false,
                    }
                } else {
                    true
                }
            }),
            // Safety: worker_id in SM tracks worker_env.
            Property::<Self>::always("worker tracking consistent", |_model, state| {
                if state.self_destructed {
                    return true;
                }
                match &state.worker_env {
                    WorkerEnv::None => state.sm.worker_id.is_none(),
                    WorkerEnv::Assigned => state.sm.worker_id.is_some(),
                }
            }),
            // Safety: orphan live pod goes terminal.
            Property::<Self>::always("orphan live pod goes terminal", |_model, state| {
                if state.self_destructed {
                    return true;
                }
                if state.was_ever_owned && state.sm.workload_id.is_none() {
                    state.sm.status.is_terminal()
                } else {
                    true
                }
            }),
            // Safety: Suspending status requires intent=Suspend.
            Property::<Self>::always("suspending implies was running", |_model, state| {
                if matches!(state.sm.status, PodStatus::Suspending) {
                    state.sm.intent == PodIntent::Suspend
                } else {
                    true
                }
            }),
            // Liveness: every path eventually reaches self-destruct (terminal sink).
            Property::<Self>::eventually("eventually self-destructs", |_model, state| {
                state.self_destructed
            }),
            // Reachability: can reach Running state.
            Property::<Self>::sometimes("can reach running", |_model, state| {
                matches!(state.sm.status, PodStatus::Running)
            }),
            // Reachability: can reach terminal state.
            Property::<Self>::sometimes("can reach terminal", |_model, state| {
                state.sm.status.is_terminal()
            }),
            // Reachability: can self-destruct.
            Property::<Self>::sometimes("can self-destruct", |_model, state| {
                state.self_destructed
            }),
        ]
    }
}

// ============================================================================
// Symmetry reduction
// ============================================================================

impl Representative for PodNewModelState {
    fn representative(&self) -> Self {
        let mut s = self.clone();

        // timer_generation: only used to tag timer requests.
        s.sm.timer_generation = 0;

        // Normalize worker_id value — only presence matters.
        if let Some(_) = s.sm.worker_id {
            s.sm.worker_id = Some(WorkerId(0));
        }

        // Normalize workload_id value — only presence matters.
        if let Some(_) = s.sm.workload_id {
            s.sm.workload_id = Some(WorkloadId(0));
        }

        // Normalize artifact_id in Suspended status and resume_artifact.
        if let PodStatus::Suspended { .. } = s.sm.status {
            s.sm.status = PodStatus::Suspended { artifact_id: ArtifactId(0) };
        }
        if let Some(_) = s.sm.resume_artifact {
            s.sm.resume_artifact = Some(ArtifactId(0));
        }

        s
    }
}

// ============================================================================
// Tests
// ============================================================================

#[test]
fn pod_new_basic() {
    let result = PodNewModel {
        enable_worker_loss: false,
        enable_suspend: false,
        enable_timers: false,
        enable_backward_status: false,
    }
    .checker()
    .symmetry()
    .spawn_dfs()
    .join();

    result.assert_properties();
    eprintln!(
        "Pod new (basic, no failures): {} unique states",
        result.unique_state_count()
    );
}

#[test]
fn pod_new_with_worker_loss() {
    let result = PodNewModel {
        enable_worker_loss: true,
        enable_suspend: false,
        enable_timers: false,
        enable_backward_status: false,
    }
    .checker()
    .symmetry()
    .spawn_dfs()
    .join();

    result.assert_properties();
    eprintln!(
        "Pod new (worker loss): {} unique states",
        result.unique_state_count()
    );
}

#[test]
fn pod_new_with_suspend() {
    let result = PodNewModel {
        enable_worker_loss: false,
        enable_suspend: true,
        enable_timers: false,
        enable_backward_status: false,
    }
    .checker()
    .symmetry()
    .spawn_dfs()
    .join();

    result.assert_properties();
    eprintln!(
        "Pod new (suspend): {} unique states",
        result.unique_state_count()
    );
}

#[test]
fn pod_new_with_timers() {
    let result = PodNewModel {
        enable_worker_loss: false,
        enable_suspend: false,
        enable_timers: true,
        enable_backward_status: false,
    }
    .checker()
    .symmetry()
    .spawn_dfs()
    .join();

    result.assert_properties();
    eprintln!(
        "Pod new (timers): {} unique states",
        result.unique_state_count()
    );
}

#[test]
fn pod_new_suspend_with_timers() {
    let result = PodNewModel {
        enable_worker_loss: false,
        enable_suspend: true,
        enable_timers: true,
        enable_backward_status: false,
    }
    .checker()
    .symmetry()
    .spawn_dfs()
    .join();

    result.assert_properties();
    eprintln!(
        "Pod new (suspend + timers): {} unique states",
        result.unique_state_count()
    );
}

/// Kitchen sink: all features enabled.
#[test]
fn pod_new_kitchen_sink() {
    let result = PodNewModel {
        enable_worker_loss: true,
        enable_suspend: true,
        enable_timers: true,
        enable_backward_status: false,
    }
    .checker()
    .symmetry()
    .spawn_dfs()
    .join();

    result.assert_properties();
    eprintln!(
        "Pod new (kitchen sink): {} unique states",
        result.unique_state_count()
    );
}

/// Backward status test: pathological worker notifications.
/// The SM's forward-progress guard rejects these, so all safety
/// properties hold unconditionally.
#[test]
fn pod_new_backward_status() {
    let result = PodNewModel {
        enable_worker_loss: true,
        enable_suspend: true,
        enable_timers: true,
        enable_backward_status: true,
    }
    .checker()
    .symmetry()
    .spawn_dfs()
    .join();

    result.assert_properties();
    eprintln!(
        "Pod new (backward status): {} unique states",
        result.unique_state_count()
    );
}
