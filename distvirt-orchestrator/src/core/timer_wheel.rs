//! Timer wheel — tracks pending timers with logical deadlines.
//!
//! Owned by `OrchestratorCore`. Absorbs `TimerAction`s from namespace
//! effects and fires expired timers when time is advanced. Shells only
//! need to drive the logical clock — they never see `TimerAction`s.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use crate::adapter::timer::{TimerAction, TimerIdentity};
use crate::types::NamespaceId;

/// A fired timer, ready to be fed back into the core.
pub struct FiredTimer {
    pub namespace_id: NamespaceId,
    pub identity: TimerIdentity,
    pub generation: u64,
}

/// Key for the deadline-ordered BTreeMap.
/// Duration is first so the natural Ord gives us deadline ordering.
type DeadlineKey = (Duration, NamespaceId, TimerIdentity);

pub struct TimerWheel {
    /// Deadline-ordered index for efficient expiry scanning.
    by_deadline: BTreeMap<DeadlineKey, u64>,
    /// Reverse lookup: (namespace, identity) -> (generation, deadline).
    /// Used for O(log n) cancellation.
    by_identity: HashMap<(NamespaceId, TimerIdentity), (u64, Duration)>,
}

impl TimerWheel {
    pub fn new() -> Self {
        TimerWheel {
            by_deadline: BTreeMap::new(),
            by_identity: HashMap::new(),
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
                    let id_key = (namespace_id.clone(), identity.clone());
                    // Remove previous entry if this timer is being restarted.
                    if let Some((_, old_deadline)) = self.by_identity.remove(&id_key) {
                        self.by_deadline
                            .remove(&(old_deadline, id_key.0.clone(), id_key.1.clone()));
                    }
                    let deadline = now + duration;
                    self.by_deadline
                        .insert((deadline, namespace_id.clone(), identity), generation);
                    self.by_identity.insert(id_key, (generation, deadline));
                }
                TimerAction::Cancel { identity } => {
                    let id_key = (namespace_id.clone(), identity);
                    if let Some((_, deadline)) = self.by_identity.remove(&id_key) {
                        self.by_deadline
                            .remove(&(deadline, id_key.0, id_key.1));
                    }
                }
            }
        }
    }

    /// Fire all timers whose deadline has been reached. Returns them
    /// and removes them from the active set.
    pub fn fire_expired(&mut self, now: Duration) -> Vec<FiredTimer> {
        // split_off returns everything >= the split key.
        // We want everything with deadline <= now.
        // We need a key that sorts after every timer with deadline == now.
        // Adding 1ns gives a key strictly greater than any (now, ...) entry.
        let split_key = (now + Duration::from_nanos(1), NamespaceId::from(""), TimerIdentity::Workload(crate::sm::WorkloadId(0), Default::default()));
        let remaining = self.by_deadline.split_off(&split_key);
        let expired = std::mem::replace(&mut self.by_deadline, remaining);

        let mut fired = Vec::with_capacity(expired.len());
        for ((_, namespace_id, identity), generation) in expired {
            self.by_identity
                .remove(&(namespace_id.clone(), identity.clone()));
            fired.push(FiredTimer {
                namespace_id,
                identity,
                generation,
            });
        }
        fired
    }

    /// Returns the earliest deadline across all active timers.
    pub fn next_deadline(&self) -> Option<Duration> {
        self.by_deadline.keys().next().map(|(deadline, _, _)| *deadline)
    }

    /// Remove all timers belonging to a namespace (used on namespace destruction).
    pub fn remove_namespace(&mut self, namespace_id: &NamespaceId) {
        self.by_identity.retain(|(ns_id, _), _| ns_id != namespace_id);
        self.by_deadline.retain(|(_, ns_id, _), _| ns_id != namespace_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sm::WorkloadTimerKey;

    fn ns(s: &str) -> NamespaceId {
        NamespaceId::from(s)
    }

    fn wl_identity(id: u64) -> TimerIdentity {
        TimerIdentity::Workload(crate::sm::WorkloadId(id), WorkloadTimerKey::RetryBackoff)
    }

    fn secs(s: u64) -> Duration {
        Duration::from_secs(s)
    }

    #[test]
    fn start_and_fire() {
        let mut tw = TimerWheel::new();
        tw.absorb(
            &ns("a"),
            vec![TimerAction::Start {
                identity: wl_identity(1),
                generation: 0,
                duration: secs(10),
            }],
            secs(100),
        );

        // Not yet expired at t=109.
        assert!(tw.fire_expired(secs(109)).is_empty());

        // Expired at t=110.
        let fired = tw.fire_expired(secs(110));
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].namespace_id, ns("a"));
        assert_eq!(fired[0].generation, 0);

        // Firing again yields nothing.
        assert!(tw.fire_expired(secs(200)).is_empty());
    }

    #[test]
    fn cancel_removes_timer() {
        let mut tw = TimerWheel::new();
        tw.absorb(
            &ns("a"),
            vec![TimerAction::Start {
                identity: wl_identity(1),
                generation: 0,
                duration: secs(10),
            }],
            secs(0),
        );
        tw.absorb(
            &ns("a"),
            vec![TimerAction::Cancel {
                identity: wl_identity(1),
            }],
            secs(5),
        );

        assert!(tw.fire_expired(secs(100)).is_empty());
        assert!(tw.next_deadline().is_none());
    }

    #[test]
    fn restart_updates_deadline() {
        let mut tw = TimerWheel::new();
        tw.absorb(
            &ns("a"),
            vec![TimerAction::Start {
                identity: wl_identity(1),
                generation: 0,
                duration: secs(10),
            }],
            secs(0),
        );
        // Restart same timer with new generation and longer duration.
        tw.absorb(
            &ns("a"),
            vec![TimerAction::Start {
                identity: wl_identity(1),
                generation: 1,
                duration: secs(20),
            }],
            secs(5),
        );

        // Old deadline (t=10) should not fire.
        assert!(tw.fire_expired(secs(10)).is_empty());

        // New deadline is t=25.
        assert_eq!(tw.next_deadline(), Some(secs(25)));
        let fired = tw.fire_expired(secs(25));
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].generation, 1);
    }

    #[test]
    fn next_deadline_returns_earliest() {
        let mut tw = TimerWheel::new();
        tw.absorb(
            &ns("a"),
            vec![TimerAction::Start {
                identity: wl_identity(1),
                generation: 0,
                duration: secs(30),
            }],
            secs(0),
        );
        tw.absorb(
            &ns("b"),
            vec![TimerAction::Start {
                identity: wl_identity(2),
                generation: 0,
                duration: secs(10),
            }],
            secs(0),
        );

        assert_eq!(tw.next_deadline(), Some(secs(10)));
    }

    #[test]
    fn remove_namespace_clears_all_timers() {
        let mut tw = TimerWheel::new();
        tw.absorb(
            &ns("a"),
            vec![
                TimerAction::Start {
                    identity: wl_identity(1),
                    generation: 0,
                    duration: secs(10),
                },
                TimerAction::Start {
                    identity: wl_identity(2),
                    generation: 0,
                    duration: secs(20),
                },
            ],
            secs(0),
        );
        tw.absorb(
            &ns("b"),
            vec![TimerAction::Start {
                identity: wl_identity(3),
                generation: 0,
                duration: secs(15),
            }],
            secs(0),
        );

        tw.remove_namespace(&ns("a"));

        // Only ns "b" timer remains.
        assert_eq!(tw.next_deadline(), Some(secs(15)));
        let fired = tw.fire_expired(secs(100));
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].namespace_id, ns("b"));
    }

    #[test]
    fn multiple_timers_fire_in_order() {
        let mut tw = TimerWheel::new();
        tw.absorb(
            &ns("a"),
            vec![
                TimerAction::Start {
                    identity: wl_identity(1),
                    generation: 0,
                    duration: secs(30),
                },
                TimerAction::Start {
                    identity: wl_identity(2),
                    generation: 0,
                    duration: secs(10),
                },
                TimerAction::Start {
                    identity: wl_identity(3),
                    generation: 0,
                    duration: secs(20),
                },
            ],
            secs(0),
        );

        let fired = tw.fire_expired(secs(25));
        assert_eq!(fired.len(), 2);
        // BTreeMap ordering means earlier deadlines come first.
        assert_eq!(fired[0].identity, wl_identity(2)); // deadline 10
        assert_eq!(fired[1].identity, wl_identity(3)); // deadline 20
    }

    #[test]
    fn empty_wheel() {
        let mut tw = TimerWheel::new();
        assert!(tw.fire_expired(secs(100)).is_empty());
        assert!(tw.next_deadline().is_none());
    }

    #[test]
    fn cancel_nonexistent_is_noop() {
        let mut tw = TimerWheel::new();
        tw.absorb(
            &ns("a"),
            vec![TimerAction::Cancel {
                identity: wl_identity(99),
            }],
            secs(0),
        );
        assert!(tw.next_deadline().is_none());
    }
}
