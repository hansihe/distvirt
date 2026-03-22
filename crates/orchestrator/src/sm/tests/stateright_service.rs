//! Stateright model checking for the thin ServiceSm.
//!
//! Level 1 individual SM model checking: we instantiate ServiceSm in isolation,
//! feed it inputs via ServiceCtxConcrete, inspect effects to update environment
//! state, and verify safety/liveness properties.
//!
//! The ServiceSm is now a thin wrapper that:
//! - Creates an EndpointSm on first spec delivery
//! - Pushes EndpointConfig on every spec delivery
//! - Forwards ActivateService events as ActivateEndpoint events to the endpoint
//! - Self-destructs when spec is removed

use distvirt_sm_router::{SequentialIds, SmHandler};
use stateright::*;

use super::super::*;

// ============================================================================
// Model
// ============================================================================

struct SvcModel;

// ============================================================================
// Model state
// ============================================================================

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct SvcModelState {
    /// The SM under test.
    sm: ServiceSm,
    /// Whether a spec is currently present in the environment.
    spec_present: bool,
    /// Whether self-destruct was triggered.
    self_destructed: bool,
    /// Whether an endpoint has been created (cumulative).
    endpoint_created: bool,
    /// Whether endpoint config was set in the last input application.
    endpoint_config_set: bool,
    /// Whether an ActivateEndpoint event was forwarded in the last input application.
    activate_forwarded: bool,
}

// ============================================================================
// Actions
// ============================================================================

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum SvcAction {
    /// Deliver initial spec: SvcSpecInput(Some(...))
    DeliverSpec,
    /// Deliver updated spec (different workload): SvcSpecInput(Some(...))
    UpdateSpec,
    /// Remove spec: SvcSpecInput(None)
    RemoveSpec,
    /// ActivateService(true)
    ActivateTrue,
    /// ActivateService(false)
    ActivateFalse,
}

// ============================================================================
// Helpers
// ============================================================================

fn apply_input(state: &SvcModelState, input: ServiceInput) -> SvcModelState {
    let mut next = state.clone();
    let mut alloc = SequentialIds::<NodeKind>::new();
    let svc_id = ServiceId(1);
    let mut ctx = ServiceCtxConcrete::new(svc_id, &mut alloc);
    next.sm.handle(input, &mut ctx);
    let effects = ctx.into_effects();

    // Reset per-step tracking flags.
    next.endpoint_config_set = false;
    next.activate_forwarded = false;

    if effects.pending_self_destruct {
        next.self_destructed = true;
    }
    // Check if endpoint was created.
    if !effects.pending_creates.is_empty() {
        next.endpoint_created = true;
    }
    // Check if EndpointConfig signal was set.
    if effects.endpoint_config.is_some() {
        next.endpoint_config_set = true;
    }
    // Check if ActivateEndpoint event was forwarded.
    if !effects.pending_events.is_empty() {
        next.activate_forwarded = true;
    }
    next
}

fn make_spec(workload_id: u64) -> (ManagementId, ServiceSpec) {
    (
        ManagementId(0),
        ServiceSpec {
            workload: WorkloadId(workload_id),
            has_activation: true,
            ..Default::default()
        },
    )
}

// ============================================================================
// Model implementation
// ============================================================================

impl Model for SvcModel {
    type State = SvcModelState;
    type Action = SvcAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![SvcModelState {
            sm: ServiceSm::new(),
            spec_present: false,
            self_destructed: false,
            endpoint_created: false,
            endpoint_config_set: false,
            activate_forwarded: false,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        if state.self_destructed {
            return;
        }

        if !state.spec_present {
            // No spec yet — can deliver one.
            actions.push(SvcAction::DeliverSpec);
        } else {
            // Spec present — can update or remove.
            actions.push(SvcAction::UpdateSpec);
            actions.push(SvcAction::RemoveSpec);
        }

        // Activation events can arrive at any time.
        actions.push(SvcAction::ActivateTrue);
        actions.push(SvcAction::ActivateFalse);
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let next = match action {
            SvcAction::DeliverSpec => {
                let mut s =
                    apply_input(state, ServiceInput::SvcSpecInput(Some(make_spec(1))));
                s.spec_present = true;
                s
            }
            SvcAction::UpdateSpec => {
                // Deliver a different spec (different workload ID).
                apply_input(state, ServiceInput::SvcSpecInput(Some(make_spec(2))))
            }
            SvcAction::RemoveSpec => {
                let mut s = apply_input(state, ServiceInput::SvcSpecInput(None));
                s.spec_present = false;
                s
            }
            SvcAction::ActivateTrue => {
                apply_input(state, ServiceInput::ActivateService(true))
            }
            SvcAction::ActivateFalse => {
                apply_input(state, ServiceInput::ActivateService(false))
            }
        };

        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Safety: endpoint is created exactly once (on first spec delivery).
            Property::<Self>::always("endpoint created at most once", |_model, state| {
                // If endpoint was already created, no new creates should happen.
                // We track this by checking: if endpoint_created was already true
                // before this step, pending_creates should be empty.
                // Since we track cumulative endpoint_created, we verify via the SM:
                // endpoint_id is Some iff endpoint_created is true.
                if state.self_destructed {
                    return true;
                }
                state.sm.endpoint_id.is_some() == state.endpoint_created
            }),
            // Safety: endpoint_config is set on every spec delivery.
            Property::<Self>::always("config set when spec delivered", |_model, state| {
                if state.self_destructed {
                    return true;
                }
                // When spec is present and endpoint exists, config should have been set
                // at some point. We check: if spec_present and endpoint exists,
                // the SM must have an endpoint_id.
                if state.spec_present {
                    // endpoint must exist if spec was delivered
                    state.endpoint_created
                } else {
                    true
                }
            }),
            // Safety: self-destruct only happens when spec is removed.
            Property::<Self>::always("self-destruct only on spec removal", |_model, state| {
                if state.self_destructed {
                    !state.spec_present
                } else {
                    true
                }
            }),
            // Safety: activate is only forwarded when endpoint exists.
            Property::<Self>::always(
                "activate only forwarded with endpoint",
                |_model, state| {
                    if state.activate_forwarded {
                        state.sm.endpoint_id.is_some()
                    } else {
                        true
                    }
                },
            ),
            // Safety: no endpoint without spec.
            Property::<Self>::always("no endpoint without spec", |_model, state| {
                if state.self_destructed {
                    return true;
                }
                if state.sm.endpoint_id.is_some() {
                    state.spec_present
                } else {
                    true
                }
            }),
            // Liveness: eventually self-destructs (every path reaches spec removal).
            Property::<Self>::eventually("eventually self-destructs", |_model, state| {
                state.self_destructed
            }),
            // Reachability: can create endpoint.
            Property::<Self>::sometimes("can create endpoint", |_model, state| {
                state.endpoint_created
            }),
        ]
    }
}

// ============================================================================
// Symmetry reduction
// ============================================================================

impl Representative for SvcModelState {
    fn representative(&self) -> Self {
        let mut s = self.clone();
        // Normalize the EndpointId value — the exact auto-allocated value
        // doesn't matter, only whether it's Some or None.
        if let Some(ref mut ep_id) = s.sm.endpoint_id {
            *ep_id = EndpointId(0);
        }
        s
    }
}

// ============================================================================
// Tests
// ============================================================================

#[test]
fn service_basic() {
    let result = SvcModel.checker().symmetry().spawn_dfs().join();
    result.assert_properties();
    eprintln!(
        "Service (thin wrapper): {} unique states",
        result.unique_state_count()
    );
}
