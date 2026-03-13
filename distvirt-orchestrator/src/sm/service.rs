use crate::types::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ServiceState {
    Idle,
    NeedBackend,
    Active {
        pod_id: PodId,
        worker_id: WorkerId,
        backend_need: BackendNeed,
        idle_timer: Option<TimerKey>,
    },
}

impl ServiceState {
    pub fn as_str(&self) -> &'static str {
        match self {
            ServiceState::Idle => "idle",
            ServiceState::NeedBackend => "need_backend",
            ServiceState::Active { .. } => "active",
        }
    }
}

pub struct ServiceStateMachine {
    pub service_id: ServiceId,
    pub state: ServiceState,
    pub workload_id: WorkloadId,
    pub has_activation: bool,
    pub idle_timeout: std::time::Duration,
    /// Active conditions for observability (key → message).
    pub conditions: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ServiceInput {
    WorkloadReady {
        pod_id: PodId,
        worker_id: WorkerId,
        backend: ServiceBackend,
    },
    WorkloadUnready,
    ServiceActivation,
    ServiceBackendNeed { need: BackendNeed },
    TimerFired { timer_key: TimerKey },
    ForceDeactivate,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ServiceOutput {
    /// The endpoint spec for this service changed; broadcast an update.
    EndpointChanged,
    TimerSet(TimerKey, std::time::Duration),
    TimerCancel(TimerKey),
    ConditionSet { key: String, message: String },
    ConditionClear { key: String },
    IdleTimerStarted { timeout: std::time::Duration },
    IdleTimerCancelled { reason: IdleTimerCancelReason },
    IdleTimeoutFired,
    Deactivated { reason: ServiceDeactivationReason },
    Activated { trigger: ServiceActivationTrigger },
    BackendReady,
}

impl ServiceStateMachine {
    pub fn new(
        service_id: ServiceId,
        workload_id: WorkloadId,
        has_activation: bool,
        idle_timeout: std::time::Duration,
    ) -> Self {
        let initial_state = if has_activation {
            ServiceState::Idle
        } else {
            ServiceState::NeedBackend
        };
        ServiceStateMachine {
            service_id,
            state: initial_state,
            workload_id,
            has_activation,
            idle_timeout,
            conditions: std::collections::BTreeMap::new(),
        }
    }

    pub fn is_active(&self) -> bool {
        matches!(self.state, ServiceState::Active { .. })
    }

    pub fn active_backend_need(&self) -> Option<&BackendNeed> {
        match &self.state {
            ServiceState::Active { backend_need, .. } => Some(backend_need),
            _ => None,
        }
    }

    pub fn active_worker_id(&self) -> Option<&WorkerId> {
        match &self.state {
            ServiceState::Active { worker_id, .. } => Some(worker_id),
            _ => None,
        }
    }

    /// Returns true if this service currently wants a backend (i.e., contributes demand).
    /// True for NeedBackend and Active states.
    pub fn wants_backend(&self) -> bool {
        matches!(self.state, ServiceState::NeedBackend | ServiceState::Active { .. })
    }

    pub fn step(&mut self, input: ServiceInput, _namespace_id: &NamespaceId) -> Vec<ServiceOutput> {
        let mut outputs = Vec::new();

        match input {
            ServiceInput::WorkloadReady {
                pod_id,
                worker_id,
                backend: _,
            } => {
                match &self.state {
                    ServiceState::NeedBackend => {
                        self.state = ServiceState::Active {
                            pod_id: pod_id.clone(),
                            worker_id: worker_id.clone(),
                            backend_need: BackendNeed::Active,
                            idle_timer: None,
                        };
                        outputs.push(ServiceOutput::BackendReady);
                        outputs.push(ServiceOutput::ConditionClear {
                            key: "activation-pending".into(),
                        });
                        outputs.push(ServiceOutput::EndpointChanged);
                    }
                    _ => {}
                }
            }
            ServiceInput::WorkloadUnready => {
                match std::mem::replace(&mut self.state, ServiceState::NeedBackend) {
                    ServiceState::Active { idle_timer, .. } => {
                        if let Some(tk) = idle_timer {
                            outputs.push(ServiceOutput::TimerCancel(tk));
                        }
                        outputs.push(ServiceOutput::EndpointChanged);
                        // Both activation and always-on services go to NeedBackend.
                        // The workload SM handles restart/retry; the service keeps
                        // its demand signal through the restart.
                        self.state = ServiceState::NeedBackend;
                    }
                    ServiceState::NeedBackend => {
                        // Already NeedBackend — no state change needed.
                        self.state = ServiceState::NeedBackend;
                    }
                    other => {
                        // Restore state if not applicable.
                        self.state = other;
                    }
                }
            }
            ServiceInput::ServiceActivation => {
                if matches!(self.state, ServiceState::Idle) {
                    self.state = ServiceState::NeedBackend;
                    outputs.push(ServiceOutput::Activated { trigger: ServiceActivationTrigger::Traffic });
                    outputs.push(ServiceOutput::ConditionSet {
                        key: "activation-pending".into(),
                        message: "waiting for backend to become ready".into(),
                    });
                }
            }
            ServiceInput::ServiceBackendNeed { need } => {
                if let ServiceState::Active {
                    ref mut backend_need,
                    ref mut idle_timer,
                    ..
                } = self.state
                {
                    *backend_need = need.clone();
                    match need {
                        BackendNeed::None => {
                            if self.has_activation && idle_timer.is_none() {
                                let timer_key = TimerKey::IdleTimeout {
                                    service_id: self.service_id.clone(),
                                };
                                outputs.push(ServiceOutput::TimerSet(
                                    timer_key.clone(),
                                    self.idle_timeout,
                                ));
                                *idle_timer = Some(timer_key);
                                outputs.push(ServiceOutput::IdleTimerStarted { timeout: self.idle_timeout });
                            }
                        }
                        BackendNeed::Traffic | BackendNeed::Active => {
                            if let Some(timer_key) = idle_timer.take() {
                                outputs.push(ServiceOutput::TimerCancel(timer_key));
                                outputs.push(ServiceOutput::IdleTimerCancelled { reason: IdleTimerCancelReason::NewTraffic });
                            }
                        }
                    }
                }
            }
            ServiceInput::TimerFired { timer_key } => {
                if let ServiceState::Active {
                    ref idle_timer,
                    ref backend_need,
                    ..
                } = self.state
                {
                    if idle_timer.as_ref() == Some(&timer_key)
                        && *backend_need == BackendNeed::None
                        && self.has_activation
                    {
                        outputs.push(ServiceOutput::IdleTimeoutFired);
                        outputs.push(ServiceOutput::Deactivated { reason: ServiceDeactivationReason::IdleTimeout });
                        outputs.push(ServiceOutput::EndpointChanged);
                        self.state = ServiceState::Idle;
                    }
                }
            }
            ServiceInput::ForceDeactivate => {
                if let ServiceState::Active {
                    ref idle_timer,
                    ..
                } = self.state
                {
                    if let Some(tk) = idle_timer.clone() {
                        outputs.push(ServiceOutput::TimerCancel(tk));
                    }
                    outputs.push(ServiceOutput::Deactivated { reason: ServiceDeactivationReason::ForceDeactivate });
                    outputs.push(ServiceOutput::EndpointChanged);
                    if self.has_activation {
                        self.state = ServiceState::Idle;
                    } else {
                        self.state = ServiceState::NeedBackend;
                    }
                }
            }
        }

        outputs
    }
}
