use super::*;
use distvirt_sm_router::SmHandler;

// ---- Service SM ----

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ServiceState {
    /// Has activation, currently idle (no demand signal).
    Idle,
    /// Wants a backend — demand signal is true.
    NeedBackend,
    /// Active with a ready backend.
    Active { ready: ReadyInfo },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ServiceSm {
    pub state: ServiceState,
    pub has_activation: bool,
    pub idle_generation: u64,
    pub idle_timer_active: bool,
    pub idle_timeout: std::time::Duration,
    /// Cached readiness from the workload. Stored regardless of service state
    /// so that an Idle→NeedBackend transition can skip straight to Active when
    /// readiness was delivered while the service was idle (the router suppresses
    /// re-delivery of unchanged signals).
    pub last_readiness: Option<ReadyInfo>,
    /// DNS entry info from spec, signaled to the DnsRegistry port.
    pub dns_entry: Option<DnsEntryInfo>,
}

impl ServiceSm {
    pub(crate) fn new(has_activation: bool) -> Self {
        ServiceSm {
            state: if has_activation {
                ServiceState::Idle
            } else {
                ServiceState::NeedBackend
            },
            has_activation,
            idle_generation: 0,
            idle_timer_active: false,
            idle_timeout: std::time::Duration::ZERO,
            last_readiness: None,
            dns_entry: None,
        }
    }

    pub(crate) fn update_timer_signal(&self, ctx: &mut impl ServiceCtx) {
        if self.idle_timer_active {
            ctx.set_wanted_timers(vec![ServiceTimerRequest {
                key: ServiceTimerKey::IdleTimeout,
                generation: self.idle_generation,
                duration: self.idle_timeout,
            }]);
        } else {
            ctx.set_wanted_timers(vec![]);
        }
    }

    /// Transition from Idle to NeedBackend (or directly to Active if readiness
    /// is already cached). Call this instead of setting `self.state = NeedBackend`
    /// directly from Idle.
    fn activate(&mut self) {
        debug_assert!(matches!(self.state, ServiceState::Idle));
        if let Some(info) = self.last_readiness.clone() {
            self.state = ServiceState::Active { ready: info };
        } else {
            self.state = ServiceState::NeedBackend;
        }
    }

    pub(crate) fn update_status_signals(&self, ctx: &mut impl ServiceCtx) {
        let status = match &self.state {
            ServiceState::Idle => SvcStatus::Idle,
            ServiceState::NeedBackend => SvcStatus::NeedBackend,
            ServiceState::Active { .. } => SvcStatus::Active,
        };
        ctx.set_status(status);
        ctx.set_idle_timer_active(self.idle_timer_active);

        let endpoint_info = match &self.state {
            ServiceState::Active { ready } => Some(ready.clone()),
            _ => None,
        };
        ctx.set_endpoint_info(endpoint_info);
        ctx.set_dns_entry(self.dns_entry.clone());
    }
}

impl<C: ServiceCtx> SmHandler<C> for ServiceSm {
    type Input = ServiceInput;

    fn initialize(&mut self, ctx: &mut C) {
        ctx.set_service_timers_edges(vec![TIMER]);
        ctx.set_service_endpoints_edges(vec![ENDPOINT]);
        ctx.set_service_dns_edges(vec![DNS_REGISTRY]);
        self.update_status_signals(ctx);
    }

    fn handle(&mut self, input: Self::Input, ctx: &mut C) {
        match input {
            ServiceInput::ReadinessInput(readiness_list) => {
                let ready = readiness_list.into_iter().next().flatten();
                self.last_readiness = ready.clone();
                match (&self.state, ready) {
                    (ServiceState::NeedBackend, Some(info)) => {
                        self.state = ServiceState::Active { ready: info };
                    }
                    (ServiceState::Active { .. }, None) => {
                        self.state = ServiceState::NeedBackend;
                        if self.idle_timer_active {
                            self.idle_timer_active = false;
                            self.update_timer_signal(ctx);
                        }
                    }
                    (ServiceState::Active { .. }, Some(info)) => {
                        self.state = ServiceState::Active { ready: info };
                    }
                    _ => {}
                }
            }
            ServiceInput::SvcSpecInput(spec_opt) => {
                if let Some((_, spec)) = spec_opt {
                    self.has_activation = spec.has_activation;
                    self.idle_timeout = spec.idle_timeout;
                    // Update DNS entry from spec.
                    self.dns_entry = match (spec.dns_name, spec.dns_ip) {
                        (Some(name), Some(ip)) => Some(DnsEntryInfo { name, ip }),
                        _ => None,
                    };
                    if !self.has_activation {
                        // Always-on: set demand immediately.
                        ctx.set_demand(true);
                        if matches!(self.state, ServiceState::Idle) {
                            self.activate();
                        }
                    }
                    // The idle timer is only meaningful with has_activation.
                    // Clear it to avoid stale timer state after a spec change.
                    if self.idle_timer_active && !self.has_activation {
                        self.idle_timer_active = false;
                        self.update_timer_signal(ctx);
                    }
                    ctx.set_service_demand_edges(vec![spec.workload]);
                } else {
                    // Spec removed — self-destruct.
                    ctx.self_destruct();
                }
            }
            ServiceInput::ActivateService(active) => {
                if self.has_activation {
                    ctx.set_demand(active);
                    if active && matches!(self.state, ServiceState::Idle) {
                        self.activate();
                    } else if !active {
                        self.state = ServiceState::Idle;
                        ctx.set_demand(false);
                        if self.idle_timer_active {
                            self.idle_timer_active = false;
                            self.update_timer_signal(ctx);
                        }
                    }
                }
            }
            ServiceInput::BackendNeedInput(need) => match (&self.state, &need) {
                (ServiceState::Active { .. }, BackendNeed::None) if self.has_activation => {
                    if !self.idle_timer_active {
                        self.idle_timer_active = true;
                        self.idle_generation += 1;
                        self.update_timer_signal(ctx);
                    }
                }
                (ServiceState::Active { .. }, BackendNeed::Traffic | BackendNeed::Active) => {
                    if self.idle_timer_active {
                        self.idle_timer_active = false;
                        self.update_timer_signal(ctx);
                    }
                }
                (ServiceState::Idle, BackendNeed::Traffic | BackendNeed::Active) => {
                    ctx.set_demand(true);
                    self.activate();
                }
                _ => {}
            },
            ServiceInput::ServiceTimerFired(key) => match key {
                ServiceTimerKey::IdleTimeout => {
                    if matches!(self.state, ServiceState::Active { .. })
                        && self.idle_timer_active
                        && self.has_activation
                    {
                        self.state = ServiceState::Idle;
                        ctx.set_demand(false);
                        self.idle_timer_active = false;
                        self.update_timer_signal(ctx);
                    }
                }
            },
        }
        self.update_status_signals(ctx);
    }
}
