//! Per-namespace timer wheel — tracks pending timers with logical deadlines.
//!
//! Unlike the global `TimerWheel`, this operates on a single namespace and
//! does not carry `NamespaceId` in its keys. Used by `NamespaceUnit`.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use crate::adapter::timer::{TimerAction, TimerIdentity};

/// A fired timer, ready to be fed back into the namespace core.
pub struct FiredTimer {
    pub identity: TimerIdentity,
    pub generation: u64,
}

/// Key for the deadline-ordered BTreeMap.
/// Duration is first so the natural Ord gives us deadline ordering.
type DeadlineKey = (Duration, TimerIdentity);

pub struct NamespaceTimerWheel {
    /// Deadline-ordered index for efficient expiry scanning.
    by_deadline: BTreeMap<DeadlineKey, u64>,
    /// Reverse lookup: identity -> (generation, deadline).
    /// Used for O(log n) cancellation.
    by_identity: HashMap<TimerIdentity, (u64, Duration)>,
}

impl NamespaceTimerWheel {
    pub fn new() -> Self {
        NamespaceTimerWheel {
            by_deadline: BTreeMap::new(),
            by_identity: HashMap::new(),
        }
    }

    /// Absorb timer actions produced by the namespace, converting relative
    /// durations to absolute deadlines using `now`.
    pub fn absorb(&mut self, actions: Vec<TimerAction>, now: Duration) {
        for action in actions {
            match action {
                TimerAction::Start {
                    identity,
                    generation,
                    duration,
                } => {
                    // Remove previous entry if this timer is being restarted.
                    if let Some((_, old_deadline)) = self.by_identity.remove(&identity) {
                        self.by_deadline.remove(&(old_deadline, identity.clone()));
                    }
                    let deadline = now + duration;
                    self.by_deadline
                        .insert((deadline, identity.clone()), generation);
                    self.by_identity.insert(identity, (generation, deadline));
                }
                TimerAction::Cancel { identity } => {
                    if let Some((_, deadline)) = self.by_identity.remove(&identity) {
                        self.by_deadline.remove(&(deadline, identity));
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
        // Adding 1ns gives a key strictly greater than any (now, ...) entry.
        let split_key = (
            now + Duration::from_nanos(1),
            TimerIdentity::Workload(crate::sm::WorkloadId(0), Default::default()),
        );
        let remaining = self.by_deadline.split_off(&split_key);
        let expired = std::mem::replace(&mut self.by_deadline, remaining);

        let mut fired = Vec::with_capacity(expired.len());
        for ((_, identity), generation) in expired {
            self.by_identity.remove(&identity);
            fired.push(FiredTimer {
                identity,
                generation,
            });
        }
        fired
    }

    /// Returns the earliest deadline across all active timers.
    pub fn next_deadline(&self) -> Option<Duration> {
        self.by_deadline
            .keys()
            .next()
            .map(|(deadline, _)| *deadline)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sm::WorkloadTimerKey;

    fn wl_identity(id: u64) -> TimerIdentity {
        TimerIdentity::Workload(crate::sm::WorkloadId(id), WorkloadTimerKey::RetryBackoff)
    }

    fn secs(s: u64) -> Duration {
        Duration::from_secs(s)
    }

    #[test]
    fn start_and_fire() {
        let mut tw = NamespaceTimerWheel::new();
        tw.absorb(
            vec![TimerAction::Start {
                identity: wl_identity(1),
                generation: 0,
                duration: secs(10),
            }],
            secs(100),
        );

        assert!(tw.fire_expired(secs(109)).is_empty());

        let fired = tw.fire_expired(secs(110));
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].generation, 0);

        assert!(tw.fire_expired(secs(200)).is_empty());
    }

    #[test]
    fn cancel_removes_timer() {
        let mut tw = NamespaceTimerWheel::new();
        tw.absorb(
            vec![TimerAction::Start {
                identity: wl_identity(1),
                generation: 0,
                duration: secs(10),
            }],
            secs(0),
        );
        tw.absorb(
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
        let mut tw = NamespaceTimerWheel::new();
        tw.absorb(
            vec![TimerAction::Start {
                identity: wl_identity(1),
                generation: 0,
                duration: secs(10),
            }],
            secs(0),
        );
        tw.absorb(
            vec![TimerAction::Start {
                identity: wl_identity(1),
                generation: 1,
                duration: secs(20),
            }],
            secs(5),
        );

        assert!(tw.fire_expired(secs(10)).is_empty());
        assert_eq!(tw.next_deadline(), Some(secs(25)));
        let fired = tw.fire_expired(secs(25));
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].generation, 1);
    }

    #[test]
    fn next_deadline_returns_earliest() {
        let mut tw = NamespaceTimerWheel::new();
        tw.absorb(
            vec![TimerAction::Start {
                identity: wl_identity(1),
                generation: 0,
                duration: secs(30),
            }],
            secs(0),
        );
        tw.absorb(
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
    fn multiple_timers_fire_in_order() {
        let mut tw = NamespaceTimerWheel::new();
        tw.absorb(
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
        assert_eq!(fired[0].identity, wl_identity(2));
        assert_eq!(fired[1].identity, wl_identity(3));
    }

    #[test]
    fn empty_wheel() {
        let mut tw = NamespaceTimerWheel::new();
        assert!(tw.fire_expired(secs(100)).is_empty());
        assert!(tw.next_deadline().is_none());
    }

    #[test]
    fn cancel_nonexistent_is_noop() {
        let mut tw = NamespaceTimerWheel::new();
        tw.absorb(
            vec![TimerAction::Cancel {
                identity: wl_identity(99),
            }],
            secs(0),
        );
        assert!(tw.next_deadline().is_none());
    }
}
