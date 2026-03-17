//! Timer wheel — tracks pending timers with logical deadlines.
//!
//! Owned by `OrchestratorCore`. Absorbs `TimerAction`s from namespace
//! effects and fires expired timers when time is advanced. Shells only
//! need to drive the logical clock — they never see `TimerAction`s.

use std::collections::HashMap;
use std::time::Duration;

use crate::adapter::timer::{TimerAction, TimerIdentity};
use crate::types::NamespaceId;

/// A fired timer, ready to be fed back into the core.
pub struct FiredTimer {
    pub namespace_id: NamespaceId,
    pub identity: TimerIdentity,
    pub generation: u64,
}

pub struct TimerWheel {
    /// Active timers: (namespace_id, identity) -> (generation, absolute deadline).
    active: HashMap<(NamespaceId, TimerIdentity), (u64, Duration)>,
}

impl TimerWheel {
    pub fn new() -> Self {
        TimerWheel {
            active: HashMap::new(),
        }
    }

    /// Absorb timer actions produced by a namespace, converting relative
    /// durations to absolute deadlines using `now`.
    pub fn absorb(&mut self, namespace_id: &NamespaceId, actions: Vec<TimerAction>, now: Duration) {
        for action in actions {
            match action {
                TimerAction::Start {
                    identity,
                    generation,
                    duration,
                } => {
                    self.active.insert(
                        (namespace_id.clone(), identity),
                        (generation, now + duration),
                    );
                }
                TimerAction::Cancel { identity } => {
                    self.active.remove(&(namespace_id.clone(), identity));
                }
            }
        }
    }

    /// Fire all timers whose deadline has been reached. Returns them
    /// and removes them from the active set.
    pub fn fire_expired(&mut self, now: Duration) -> Vec<FiredTimer> {
        let expired: Vec<_> = self
            .active
            .iter()
            .filter(|(_, (_, deadline))| now >= *deadline)
            .map(|((ns_id, identity), (generation, _))| FiredTimer {
                namespace_id: ns_id.clone(),
                identity: identity.clone(),
                generation: *generation,
            })
            .collect();

        for fired in &expired {
            self.active
                .remove(&(fired.namespace_id.clone(), fired.identity.clone()));
        }

        expired
    }

    /// Returns the earliest deadline across all active timers.
    pub fn next_deadline(&self) -> Option<Duration> {
        self.active.values().map(|(_, deadline)| *deadline).min()
    }

    /// Remove all timers belonging to a namespace (used on namespace destruction).
    pub fn remove_namespace(&mut self, namespace_id: &NamespaceId) {
        self.active.retain(|(ns_id, _), _| ns_id != namespace_id);
    }
}
