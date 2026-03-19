use super::*;
use distvirt_sm_router::SmHandler;

// ---- Endpoint SM ----

/// Endpoint lifecycle state.
///
/// Derived from (demand, readiness):
/// - No demand, no ready → Idle
/// - Demand, no ready → NeedBackend
/// - Ready (any demand) → Active
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EndpointState {
    /// No demand and no readiness.
    Idle,
    /// Demand present but no readiness yet.
    NeedBackend,
    /// Ready backend available.
    Active { ready: ReadyInfo },
}

/// Identifies what kind of entity owns this endpoint, carrying kind-specific data.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EndpointKind {
    Service {
        service_id: ServiceId,
        policy: distvirt_worker_protocol::ServicePolicy,
    },
    Workload,
}

/// Configuration pushed by the owner (Service or Workload) via ownership edge signal.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EndpointConfig {
    pub kind: EndpointKind,
    pub workload: WorkloadId,
    pub has_activation: bool,
    pub idle_timeout: std::time::Duration,
    pub ip: std::net::Ipv4Addr,
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
    /// No demand, no readiness.
    #[default]
    Idle,
    /// Demand present, waiting for readiness.
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
    /// Cached readiness from the workload.
    pub last_readiness: Option<ReadyInfo>,
    /// Current active level from demand aggregation. Only written by handle_activate.
    pub active_level: bool,
    /// What kind of entity owns this endpoint.
    pub kind: Option<EndpointKind>,
    /// IP from config, used to construct EndpointInfo.
    pub ip: std::net::Ipv4Addr,
    /// DNS entry info from config, signaled to the DnsRegistry port.
    pub dns_entry: Option<DnsEntryInfo>,
}

impl EndpointSm {
    pub fn new(has_activation: bool) -> Self {
        let mut sm = EndpointSm {
            state: EndpointState::Idle, // overwritten by derive_state
            has_activation,
            idle_generation: 0,
            idle_timer_active: false,
            idle_timeout: std::time::Duration::ZERO,
            last_readiness: None,
            active_level: false,
            kind: None,
            ip: std::net::Ipv4Addr::UNSPECIFIED,
            dns_entry: None,
        };
        sm.derive_state();
        sm
    }

    /// Compute the demand signal.
    /// Always-on endpoints always have demand. Activation endpoints derive
    /// demand from active_level || idle_timer_active.
    fn compute_demand(&self) -> bool {
        if !self.has_activation {
            true
        } else {
            self.active_level || self.idle_timer_active
        }
    }

    /// Derive endpoint state from (demand, readiness).
    fn derive_state(&mut self) {
        let demand = self.compute_demand();
        self.state = if let Some(info) = self.last_readiness.clone() {
            EndpointState::Active { ready: info }
        } else if demand {
            EndpointState::NeedBackend
        } else {
            EndpointState::Idle
        };
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
        ctx.set_current_backend_need(if self.compute_demand() {
            BackendNeed::Active
        } else {
            BackendNeed::None
        });
        ctx.set_idle_timer_active(self.idle_timer_active);

        let endpoint_info = match (&self.state, &self.kind) {
            (EndpointState::Active { ready }, Some(kind)) => {
                let backend_ip = match kind {
                    EndpointKind::Service { .. } => Some(ready.pod_ip),
                    EndpointKind::Workload => None,
                };
                Some(EndpointInfo {
                    kind: kind.clone(),
                    ip: self.ip,
                    backend: Some(EndpointBackendInfo {
                        ip: backend_ip,
                        worker_id: ready.worker_id,
                    }),
                })
            }
            (EndpointState::NeedBackend | EndpointState::Idle, Some(kind))
                if self.has_activation =>
            {
                Some(EndpointInfo {
                    kind: kind.clone(),
                    ip: self.ip,
                    backend: None,
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
            self.ip = config.ip;
            self.dns_entry = config.dns_entry;

            // Point demand at the workload.
            ctx.set_endpoint_demand_edges(vec![config.workload]);

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

    pub(crate) fn handle_readiness(&mut self, ready: Option<ReadyInfo>) {
        self.last_readiness = ready;
    }

    /// Level-based active input from demand aggregation.
    /// Sets active_level. Cancels idle timer when active goes high.
    pub(crate) fn handle_activate(&mut self, active: bool, ctx: &mut impl EndpointCtx) {
        self.active_level = active;
        if active && self.idle_timer_active {
            self.idle_timer_active = false;
            self.update_timer_signal(ctx);
        }
    }

    /// Instantaneous traffic event. Starts or restarts the idle timer.
    /// Does not modify active_level.
    pub(crate) fn handle_traffic_event(&mut self, ctx: &mut impl EndpointCtx) {
        if self.has_activation {
            self.idle_timer_active = true;
            self.idle_generation += 1;
            self.update_timer_signal(ctx);
        }
    }

    /// Idle timer expired. Clears idle_timer_active.
    pub(crate) fn handle_timer_fired(
        &mut self,
        _key: EndpointTimerKey,
        ctx: &mut impl EndpointCtx,
    ) {
        if self.idle_timer_active {
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
        ctx.set_endpoint_observability_edges(vec![OBSERVABILITY]);
        self.derive_state();
        ctx.set_demand(self.compute_demand());
        self.update_status_signals(ctx);
    }

    fn handle(&mut self, input: Self::Input, ctx: &mut C) {
        match input {
            EndpointInput::ConfigInput(config) => self.handle_config(config, ctx),
            EndpointInput::ReadinessInput(readiness_list) => {
                let ready = readiness_list.into_iter().next().flatten();
                self.handle_readiness(ready);
            }
            EndpointInput::EndpointDemandTraffic(()) => self.handle_traffic_event(ctx),
            EndpointInput::BackendNeedInput(need) => self.handle_activate(need, ctx),
            EndpointInput::ActivateEndpoint(_) => {} // noop for now
            EndpointInput::EndpointTimerFired(key) => self.handle_timer_fired(key, ctx),
        }
        self.derive_state();
        ctx.set_demand(self.compute_demand());
        self.update_status_signals(ctx);
    }
}
