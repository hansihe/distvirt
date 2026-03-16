use std::collections::HashMap;
use std::time::Duration;

use crate::sm_new::{
    DRouter, PodTimerKey, ServiceTimerKey, TimerPortInput,
    WorkloadTimerKey,
    PodId, ServiceId, WorkloadId,
    TIMER,
};

#[cfg(test)]
mod tests;

/// Identifies a specific timer instance across all SM kinds.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum TimerIdentity {
    Workload(WorkloadId, WorkloadTimerKey),
    Service(ServiceId, ServiceTimerKey),
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

pub(crate) struct TimerAdapter {
    config: TimerConfig,
    /// Active timers: identity → generation.
    active: HashMap<TimerIdentity, u64>,
}

impl TimerAdapter {
    pub(crate) fn new(config: TimerConfig) -> Self {
        TimerAdapter {
            config,
            active: HashMap::new(),
        }
    }

    /// Drain timer port inputs from the router, diff against active state,
    /// and return Start/Cancel actions. Updates internal active state.
    pub(crate) fn reconcile(&mut self, router: &mut DRouter) -> Vec<TimerAction> {
        let inputs = router.drain_timer_inputs();

        // Start from current active as the wanted set.
        // Only update portions that had new deliveries.
        let mut wanted = self.active.clone();
        let mut had_workload = false;
        let mut had_service = false;
        let mut had_pod = false;

        for (timer_id, input) in inputs {
            if timer_id != TIMER {
                continue;
            }
            match input {
                TimerPortInput::WorkloadTimersInput(timers) => {
                    had_workload = true;
                    wanted.retain(|k, _| !matches!(k, TimerIdentity::Workload(..)));
                    for (wl_id, requests) in timers {
                        for req in requests {
                            wanted.insert(
                                TimerIdentity::Workload(wl_id, req.key.clone()),
                                req.generation,
                            );
                        }
                    }
                }
                TimerPortInput::ServiceTimersInput(timers) => {
                    had_service = true;
                    wanted.retain(|k, _| !matches!(k, TimerIdentity::Service(..)));
                    for (svc_id, requests) in timers {
                        for req in requests {
                            wanted.insert(
                                TimerIdentity::Service(svc_id, req.key.clone()),
                                req.generation,
                            );
                        }
                    }
                }
                TimerPortInput::PodTimersInput(timers) => {
                    had_pod = true;
                    wanted.retain(|k, _| !matches!(k, TimerIdentity::Pod(..)));
                    for (pod_id, requests) in timers {
                        for req in requests {
                            wanted.insert(
                                TimerIdentity::Pod(pod_id, req.key.clone()),
                                req.generation,
                            );
                        }
                    }
                }
            }
        }

        // If no deliveries at all, nothing changed.
        if !had_workload && !had_service && !had_pod {
            return Vec::new();
        }

        let mut actions = Vec::new();

        // Cancel timers no longer wanted or with changed generation.
        for (identity, generation) in &self.active {
            match wanted.get(identity) {
                None => actions.push(TimerAction::Cancel {
                    identity: identity.clone(),
                }),
                Some(new_gen) if *new_gen != *generation => {
                    actions.push(TimerAction::Cancel {
                        identity: identity.clone(),
                    });
                }
                _ => {}
            }
        }

        // Start new timers or restart with new generation.
        for (identity, generation) in &wanted {
            match self.active.get(identity) {
                None => actions.push(TimerAction::Start {
                    identity: identity.clone(),
                    generation: *generation,
                    duration: self.duration_for(identity),
                }),
                Some(old_gen) if *old_gen != *generation => {
                    actions.push(TimerAction::Start {
                        identity: identity.clone(),
                        generation: *generation,
                        duration: self.duration_for(identity),
                    });
                }
                _ => {}
            }
        }

        self.active = wanted;
        actions
    }

    /// Dispatch a timer fire event into the router.
    pub(crate) fn fire(&self, router: &mut DRouter, identity: &TimerIdentity) {
        match identity {
            TimerIdentity::Workload(wl_id, key) => {
                router.send_workload_timer_fired(TIMER, *wl_id, key.clone());
            }
            TimerIdentity::Service(svc_id, key) => {
                router.send_service_timer_fired(TIMER, *svc_id, key.clone());
            }
            TimerIdentity::Pod(pod_id, key) => {
                router.send_pod_timer_fired(TIMER, *pod_id, key.clone());
            }
        }
    }

    /// Map a timer identity to its configured duration.
    fn duration_for(&self, identity: &TimerIdentity) -> Duration {
        match identity {
            TimerIdentity::Workload(_, WorkloadTimerKey::RetryBackoff) => self.config.retry_backoff,
            TimerIdentity::Service(_, ServiceTimerKey::IdleTimeout) => self.config.idle_timeout,
            TimerIdentity::Pod(_, PodTimerKey::LaunchTimeout) => self.config.launch_timeout,
            TimerIdentity::Pod(_, PodTimerKey::SuspendTimeout) => self.config.suspend_timeout,
        }
    }
}
