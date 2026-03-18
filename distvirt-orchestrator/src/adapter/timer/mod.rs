use std::collections::HashMap;
use std::time::Duration;

use distvirt_sm_router::IncrementalAggregator;

use crate::sm::{
    DRouter, EndpointId, PodId, PodTimerKey, PodTimerRequest,
    TIMER, TimerPortInput, TimerRequest, WorkloadId, WorkloadTimerKey,
    endpoint::{EndpointTimerKey, EndpointTimerRequest},
};

#[cfg(test)]
mod tests;

/// Identifies a specific timer instance across all SM kinds.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TimerIdentity {
    Workload(WorkloadId, WorkloadTimerKey),
    Endpoint(EndpointId, EndpointTimerKey),
    Pod(PodId, PodTimerKey),
}

/// Action returned by reconcile — caller (shell) executes these.
#[derive(Clone, Debug, PartialEq)]
pub enum TimerAction {
    Start {
        identity: TimerIdentity,
        generation: u64,
        duration: Duration,
    },
    Cancel {
        identity: TimerIdentity,
    },
}

/// Configuration for timer durations.
#[derive(Clone, Debug)]
pub struct TimerConfig {
    pub retry_backoff: Duration,
    pub launch_timeout: Duration,
    pub suspend_timeout: Duration,
    pub idle_timeout: Duration,
}

pub(crate) struct TimerAdapter;

impl TimerAdapter {
    pub(crate) fn new(_config: TimerConfig) -> Self {
        TimerAdapter
    }

    /// Drain timer port inputs from the router.
    /// With incremental aggregation the router already produces per-timer deltas,
    /// so no adapter-side diffing or caching is needed.
    ///
    /// Returns `(actions, mutated_router)`. Currently this adapter only drains
    /// outputs and never writes back to the router, so `mutated_router` is
    /// always `false`.
    pub(crate) fn reconcile(&mut self, router: &mut DRouter) -> (Vec<TimerAction>, bool) {
        let inputs = router.drain_timer_inputs();

        let actions = inputs
            .into_iter()
            .filter(|(timer_id, _)| *timer_id == TIMER)
            .flat_map(|(_, input)| match input {
                TimerPortInput::WorkloadTimersInput(actions) => actions,
                TimerPortInput::EndpointTimersInput(actions) => actions,
                TimerPortInput::PodTimersInput(actions) => actions,
            })
            .collect();
        (actions, false)
    }

    /// Dispatch a timer fire event into the router.
    pub(crate) fn fire(&self, router: &mut DRouter, identity: &TimerIdentity) {
        match identity {
            TimerIdentity::Workload(wl_id, key) => {
                router.send_workload_timer_fired(TIMER, *wl_id, key.clone());
            }
            TimerIdentity::Endpoint(ep_id, key) => {
                router.send_endpoint_timer_fired(TIMER, *ep_id, key.clone());
            }
            TimerIdentity::Pod(pod_id, key) => {
                router.send_pod_timer_fired(TIMER, *pod_id, key.clone());
            }
        }
    }
}

// =============================================================================
// Incremental aggregators for timer inputs
// =============================================================================

/// Shared trait for extracting timer identity info from the different request types.
pub trait TimerRequestInfo {
    type SmId: Copy;
    type Key: Eq + std::hash::Hash + Clone;

    fn key(&self) -> &Self::Key;
    fn generation(&self) -> u64;
    fn duration(&self) -> Duration;
    fn make_identity(sm_id: Self::SmId, key: Self::Key) -> TimerIdentity;
}

impl TimerRequestInfo for TimerRequest {
    type SmId = WorkloadId;
    type Key = WorkloadTimerKey;

    fn key(&self) -> &WorkloadTimerKey {
        &self.key
    }
    fn generation(&self) -> u64 {
        self.generation
    }
    fn duration(&self) -> Duration {
        self.duration
    }
    fn make_identity(sm_id: WorkloadId, key: WorkloadTimerKey) -> TimerIdentity {
        TimerIdentity::Workload(sm_id, key)
    }
}

impl TimerRequestInfo for EndpointTimerRequest {
    type SmId = EndpointId;
    type Key = EndpointTimerKey;

    fn key(&self) -> &EndpointTimerKey {
        &self.key
    }
    fn generation(&self) -> u64 {
        self.generation
    }
    fn duration(&self) -> Duration {
        self.duration
    }
    fn make_identity(sm_id: EndpointId, key: EndpointTimerKey) -> TimerIdentity {
        TimerIdentity::Endpoint(sm_id, key)
    }
}

impl TimerRequestInfo for PodTimerRequest {
    type SmId = PodId;
    type Key = PodTimerKey;

    fn key(&self) -> &PodTimerKey {
        &self.key
    }
    fn generation(&self) -> u64 {
        self.generation
    }
    fn duration(&self) -> Duration {
        self.duration
    }
    fn make_identity(sm_id: PodId, key: PodTimerKey) -> TimerIdentity {
        TimerIdentity::Pod(sm_id, key)
    }
}

/// Diff two timer request lists and produce Start/Cancel actions.
fn diff_timer_requests<R: TimerRequestInfo>(
    sm_id: R::SmId,
    old: &[R],
    new: &[R],
) -> Vec<TimerAction> {
    let old_map: HashMap<&R::Key, u64> = old.iter().map(|r| (r.key(), r.generation())).collect();
    let new_map: HashMap<&R::Key, (u64, Duration)> =
        new.iter().map(|r| (r.key(), (r.generation(), r.duration()))).collect();

    let mut actions = Vec::new();

    // Cancel removed or generation-changed timers.
    for (key, generation) in &old_map {
        match new_map.get(key) {
            None => actions.push(TimerAction::Cancel {
                identity: R::make_identity(sm_id, (*key).clone()),
            }),
            Some((new_generation, _)) if *new_generation != *generation => {
                actions.push(TimerAction::Cancel {
                    identity: R::make_identity(sm_id, (*key).clone()),
                });
            }
            _ => {}
        }
    }

    // Start new or generation-changed timers.
    for (key, (generation, dur)) in &new_map {
        match old_map.get(key) {
            None => actions.push(TimerAction::Start {
                identity: R::make_identity(sm_id, (*key).clone()),
                generation: *generation,
                duration: *dur,
            }),
            Some(old_generation) if *old_generation != *generation => {
                actions.push(TimerAction::Start {
                    identity: R::make_identity(sm_id, (*key).clone()),
                    generation: *generation,
                    duration: *dur,
                });
            }
            _ => {}
        }
    }

    actions
}

/// Generic incremental aggregator for timer inputs.
pub struct TimerIncrementalAggregator<R>(std::marker::PhantomData<R>);

impl<R> Default for TimerIncrementalAggregator<R> {
    fn default() -> Self {
        TimerIncrementalAggregator(std::marker::PhantomData)
    }
}

impl<R: TimerRequestInfo> IncrementalAggregator for TimerIncrementalAggregator<R> {
    type Input = (R::SmId, Vec<R>);
    type Output = Vec<TimerAction>;

    fn added(&self, (sm_id, requests): &(R::SmId, Vec<R>)) -> Option<Vec<TimerAction>> {
        let actions: Vec<_> = requests
            .iter()
            .map(|req| TimerAction::Start {
                identity: R::make_identity(*sm_id, req.key().clone()),
                generation: req.generation(),
                duration: req.duration(),
            })
            .collect();
        if actions.is_empty() {
            None
        } else {
            Some(actions)
        }
    }

    fn removed(&self, (sm_id, requests): &(R::SmId, Vec<R>)) -> Option<Vec<TimerAction>> {
        let actions: Vec<_> = requests
            .iter()
            .map(|req| TimerAction::Cancel {
                identity: R::make_identity(*sm_id, req.key().clone()),
            })
            .collect();
        if actions.is_empty() {
            None
        } else {
            Some(actions)
        }
    }

    fn changed(
        &self,
        (sm_id, old_reqs): &(R::SmId, Vec<R>),
        (_, new_reqs): &(R::SmId, Vec<R>),
    ) -> Option<Vec<TimerAction>> {
        let actions = diff_timer_requests(*sm_id, old_reqs, new_reqs);
        if actions.is_empty() {
            None
        } else {
            Some(actions)
        }
    }
}

/// Type aliases for the router declaration.
pub type WorkloadTimerIncrementalAggregator = TimerIncrementalAggregator<TimerRequest>;
pub type EndpointTimerIncrementalAggregator = TimerIncrementalAggregator<EndpointTimerRequest>;
pub type PodTimerIncrementalAggregator = TimerIncrementalAggregator<PodTimerRequest>;
