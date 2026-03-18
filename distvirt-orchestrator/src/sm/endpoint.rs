use super::*;
use distvirt_sm_router::SmHandler;

// ---- Endpoint SM ----

/// Endpoint lifecycle state.
///
/// An endpoint represents a service's presence in the network fabric.
/// It tracks activation, idle timeout, and readiness from its backing workload.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EndpointState {
    /// Has activation, currently idle (no demand signal).
    Idle,
    /// Wants a backend — demand signal is true, waiting for readiness.
    NeedBackend,
    /// Active with a ready backend.
    Active { ready: ReadyInfo },
}

/// Identifies what kind of entity owns this endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EndpointKind {
    Service { service_id: ServiceId },
}

/// Configuration pushed by the owner (Service or Workload) via ownership edge signal.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EndpointConfig {
    pub kind: EndpointKind,
    pub workload: WorkloadId,
    pub has_activation: bool,
    pub idle_timeout: std::time::Duration,
    pub service_ip: std::net::Ipv4Addr,
    pub policy: distvirt_worker_protocol::ServicePolicy,
    pub dns_entry: Option<DnsEntryInfo>,
}

/// Timer key for endpoint-specific timers.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub enum EndpointTimerKey {
    #[default]
    IdleTimeout,
}

/// Timer request emitted by endpoint SM.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct EndpointTimerRequest {
    pub key: EndpointTimerKey,
    pub generation: u64,
    pub duration: std::time::Duration,
}

/// Observable endpoint lifecycle status.
#[derive(Clone, Debug, PartialEq, Default)]
pub enum EndpointStatus {
    /// Has activation, currently idle (no demand signal).
    #[default]
    Idle,
    /// Wants a backend — demand is set but workload not ready.
    NeedBackend,
    /// Active with a ready backend.
    Active,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct EndpointSm {
    pub state: EndpointState,
    pub has_activation: bool,
    pub idle_generation: u64,
    pub idle_timer_active: bool,
    pub idle_timeout: std::time::Duration,
    /// Cached readiness from the workload. Stored regardless of endpoint state
    /// so that an Idle→NeedBackend transition can skip straight to Active when
    /// readiness was delivered while the endpoint was idle (the router suppresses
    /// re-delivery of unchanged signals).
    pub last_readiness: Option<ReadyInfo>,
    /// Current aggregated backend need level.
    pub backend_need: BackendNeed,
    /// What kind of entity owns this endpoint.
    pub kind: Option<EndpointKind>,
    /// Service VIP from config, used to construct ServiceEndpointInfo.
    pub service_ip: std::net::Ipv4Addr,
    /// Service policy from config, used to construct ServiceEndpointInfo.
    pub service_policy: distvirt_worker_protocol::ServicePolicy,
    /// DNS entry info from config, signaled to the DnsRegistry port.
    pub dns_entry: Option<DnsEntryInfo>,
}

impl EndpointSm {
    pub fn new(has_activation: bool) -> Self {
        EndpointSm {
            state: if has_activation {
                EndpointState::Idle
            } else {
                EndpointState::NeedBackend
            },
            has_activation,
            idle_generation: 0,
            idle_timer_active: false,
            idle_timeout: std::time::Duration::ZERO,
            last_readiness: None,
            backend_need: BackendNeed::None,
            kind: None,
            service_ip: std::net::Ipv4Addr::UNSPECIFIED,
            service_policy: distvirt_worker_protocol::ServicePolicy {
                buffer_frames: 0,
                timeout_ms: 0,
                activator: None,
            },
            dns_entry: None,
        }
    }

    /// Transition from Idle to NeedBackend (or directly to Active if readiness
    /// is already cached).
    fn activate(&mut self) {
        debug_assert!(matches!(self.state, EndpointState::Idle));
        if let Some(info) = self.last_readiness.clone() {
            self.state = EndpointState::Active { ready: info };
        } else {
            self.state = EndpointState::NeedBackend;
        }
    }

    fn update_timer_signal(&self, ctx: &mut impl EndpointCtx) {
        if self.idle_timer_active {
            ctx.set_wanted_timers(vec![EndpointTimerRequest {
                key: EndpointTimerKey::IdleTimeout,
                generation: self.idle_generation,
                duration: self.idle_timeout,
            }]);
        } else {
            ctx.set_wanted_timers(vec![]);
        }
    }

    pub(crate) fn update_status_signals(&self, ctx: &mut impl EndpointCtx) {
        let status = match &self.state {
            EndpointState::Idle => EndpointStatus::Idle,
            EndpointState::NeedBackend => EndpointStatus::NeedBackend,
            EndpointState::Active { .. } => EndpointStatus::Active,
        };
        ctx.set_status(status);
        ctx.set_current_backend_need(self.backend_need.clone());
        ctx.set_idle_timer_active(self.idle_timer_active);

        let endpoint_info = match (&self.state, &self.kind) {
            (EndpointState::Active { ready }, Some(EndpointKind::Service { service_id })) => {
                Some(ServiceEndpointInfo {
                    service_id: *service_id,
                    service_ip: self.service_ip,
                    policy: self.service_policy.clone(),
                    pod_ip: ready.pod_ip,
                    worker_id: ready.worker_id,
                })
            }
            _ => None,
        };
        ctx.set_endpoint_info(endpoint_info);
        ctx.set_dns_entry(self.dns_entry.clone());
    }

    // Per-input handle methods — usable from both SmHandler and stateright tests.

    pub(crate) fn handle_config(
        &mut self,
        config: Option<EndpointConfig>,
        ctx: &mut impl EndpointCtx,
    ) {
        if let Some(config) = config {
            self.kind = Some(config.kind);
            self.has_activation = config.has_activation;
            self.idle_timeout = config.idle_timeout;
            self.service_ip = config.service_ip;
            self.service_policy = config.policy;
            self.dns_entry = config.dns_entry;

            // Point demand at the workload.
            ctx.set_endpoint_demand_edges(vec![config.workload]);

            if !self.has_activation {
                // Always-on: set demand immediately.
                ctx.set_demand(true);
                if matches!(self.state, EndpointState::Idle) {
                    self.activate();
                }
            }
            // Clear stale timer if activation was removed.
            if self.idle_timer_active && !self.has_activation {
                self.idle_timer_active = false;
                self.update_timer_signal(ctx);
            }
        } else {
            // Owner gone — self-destruct.
            ctx.self_destruct();
        }
    }

    pub(crate) fn handle_readiness(
        &mut self,
        ready: Option<ReadyInfo>,
        ctx: &mut impl EndpointCtx,
    ) {
        self.last_readiness = ready.clone();
        match (&self.state, ready) {
            (EndpointState::NeedBackend, Some(info)) => {
                self.state = EndpointState::Active { ready: info };
            }
            (EndpointState::Active { .. }, None) => {
                self.state = EndpointState::NeedBackend;
                if self.idle_timer_active {
                    self.idle_timer_active = false;
                    self.update_timer_signal(ctx);
                }
            }
            (EndpointState::Active { .. }, Some(info)) => {
                self.state = EndpointState::Active { ready: info };
            }
            _ => {}
        }
    }

    pub(crate) fn handle_backend_need(&mut self, need: BackendNeed, ctx: &mut impl EndpointCtx) {
        self.backend_need = need.clone();
        match (&self.state, &need) {
            // Active, need drops — start idle timer.
            (EndpointState::Active { .. }, BackendNeed::None) if self.has_activation => {
                if !self.idle_timer_active {
                    self.idle_timer_active = true;
                    self.idle_generation += 1;
                    self.update_timer_signal(ctx);
                }
            }
            // Active, sustained need — cancel idle timer.
            (EndpointState::Active { .. }, BackendNeed::Traffic | BackendNeed::Active) => {
                if self.idle_timer_active {
                    self.idle_timer_active = false;
                    self.update_timer_signal(ctx);
                }
            }
            // Idle, sustained need — activate.
            (EndpointState::Idle, BackendNeed::Traffic | BackendNeed::Active) => {
                ctx.set_demand(true);
                self.activate();
            }
            _ => {}
        }
    }

    pub(crate) fn handle_activate(&mut self, active: bool, ctx: &mut impl EndpointCtx) {
        if self.has_activation {
            ctx.set_demand(active);
            if active && matches!(self.state, EndpointState::Idle) {
                self.activate();
            } else if !active {
                self.state = EndpointState::Idle;
                if self.idle_timer_active {
                    self.idle_timer_active = false;
                    self.update_timer_signal(ctx);
                }
            }
        }
    }

    pub(crate) fn handle_traffic_event(&mut self, ctx: &mut impl EndpointCtx) {
        if self.has_activation && matches!(self.state, EndpointState::Idle) {
            ctx.set_demand(true);
            self.activate();
            // Unit impulse: start idle timer immediately, but only if
            // we actually reached Active (readiness was cached).
            if matches!(self.state, EndpointState::Active { .. }) && !self.idle_timer_active {
                self.idle_timer_active = true;
                self.idle_generation += 1;
                self.update_timer_signal(ctx);
            }
        }
    }

    pub(crate) fn handle_timer_fired(
        &mut self,
        _key: EndpointTimerKey,
        ctx: &mut impl EndpointCtx,
    ) {
        if matches!(self.state, EndpointState::Active { .. })
            && self.idle_timer_active
            && self.has_activation
        {
            self.state = EndpointState::Idle;
            ctx.set_demand(false);
            self.idle_timer_active = false;
            self.update_timer_signal(ctx);
        }
    }
}

impl<C: EndpointCtx> SmHandler<C> for EndpointSm {
    type Input = EndpointInput;

    fn initialize(&mut self, ctx: &mut C) {
        ctx.set_endpoint_timers_edges(vec![TIMER]);
        ctx.set_endpoint_fabric_endpoints_edges(vec![FABRIC_ENDPOINT]);
        ctx.set_endpoint_dns_edges(vec![DNS_REGISTRY]);
        self.update_status_signals(ctx);
    }

    fn handle(&mut self, input: Self::Input, ctx: &mut C) {
        match input {
            EndpointInput::ConfigInput(config) => self.handle_config(config, ctx),
            EndpointInput::ReadinessInput(readiness_list) => {
                let ready = readiness_list.into_iter().next().flatten();
                self.handle_readiness(ready, ctx);
            }
            EndpointInput::BackendNeedInput(need) => self.handle_backend_need(need, ctx),
            EndpointInput::ActivateEndpoint(active) => self.handle_activate(active, ctx),
            EndpointInput::EndpointTimerFired(key) => self.handle_timer_fired(key, ctx),
        }
        self.update_status_signals(ctx);
    }
}
