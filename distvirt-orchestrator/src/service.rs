use crate::types::*;

pub struct ServiceStateMachine {
    pub service_id: ServiceId,
    pub state: ServiceState,
    pub workload_id: WorkloadId,
    pub has_activation: bool,
    pub idle_timeout: std::time::Duration,
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
}

#[derive(Debug, Clone, PartialEq)]
pub enum ServiceOutput {
    DemandUp,
    DemandDown,
    WorkerCommand(WorkerId, WorkerCommand),
    /// Emit to all active workers.
    BroadcastWorkerCommand(WorkerCommand),
    TimerSet(TimerKey, std::time::Duration),
    TimerCancel(TimerKey),
}

impl ServiceStateMachine {
    pub fn new(
        service_id: ServiceId,
        workload_id: WorkloadId,
        has_activation: bool,
        idle_timeout: std::time::Duration,
    ) -> Self {
        ServiceStateMachine {
            service_id,
            state: ServiceState::Pending,
            workload_id,
            has_activation,
            idle_timeout,
        }
    }

    pub fn step(&mut self, input: ServiceInput, namespace_id: &NamespaceId) -> Vec<ServiceOutput> {
        let mut outputs = Vec::new();

        match input {
            ServiceInput::WorkloadReady {
                pod_id,
                worker_id,
                backend,
            } => {
                match &self.state {
                    ServiceState::NeedBackend | ServiceState::Pending => {
                        self.state = ServiceState::Active {
                            pod_id: pod_id.clone(),
                            worker_id: worker_id.clone(),
                            backend_need: BackendNeed::Active,
                            idle_timer: None,
                        };
                        outputs.push(ServiceOutput::BroadcastWorkerCommand(
                            WorkerCommand::UpdateServiceBackend {
                                namespace_id: namespace_id.clone(),
                                service_id: self.service_id.clone(),
                                backend: Some(backend),
                            },
                        ));
                        outputs.push(ServiceOutput::BroadcastWorkerCommand(
                            WorkerCommand::ServiceReady {
                                namespace_id: namespace_id.clone(),
                                service_id: self.service_id.clone(),
                            },
                        ));
                    }
                    _ => {}
                }
            }
            ServiceInput::WorkloadUnready => {
                match std::mem::replace(&mut self.state, ServiceState::Pending) {
                    ServiceState::Active { idle_timer, .. } => {
                        if let Some(tk) = idle_timer {
                            outputs.push(ServiceOutput::TimerCancel(tk));
                        }
                        outputs.push(ServiceOutput::BroadcastWorkerCommand(
                            WorkerCommand::UpdateServiceBackend {
                                namespace_id: namespace_id.clone(),
                                service_id: self.service_id.clone(),
                                backend: None,
                            },
                        ));
                        if self.has_activation {
                            self.state = ServiceState::Idle;
                            outputs.push(ServiceOutput::DemandDown);
                        } else {
                            // Always-on: stay NeedBackend, workload will retry.
                            self.state = ServiceState::NeedBackend;
                        }
                    }
                    ServiceState::NeedBackend => {
                        if self.has_activation {
                            // Activation service: go back to Idle, drop demand.
                            self.state = ServiceState::Idle;
                            outputs.push(ServiceOutput::DemandDown);
                        } else {
                            // Always-on: stay NeedBackend, workload will retry.
                            self.state = ServiceState::NeedBackend;
                        }
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
                    outputs.push(ServiceOutput::DemandUp);
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
                            }
                        }
                        BackendNeed::Traffic | BackendNeed::Active => {
                            if let Some(timer_key) = idle_timer.take() {
                                outputs.push(ServiceOutput::TimerCancel(timer_key));
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
                        outputs.push(ServiceOutput::DemandDown);
                        outputs.push(ServiceOutput::BroadcastWorkerCommand(
                            WorkerCommand::UpdateServiceBackend {
                                namespace_id: namespace_id.clone(),
                                service_id: self.service_id.clone(),
                                backend: None,
                            },
                        ));
                        self.state = ServiceState::Idle;
                    }
                }
            }
        }

        outputs
    }
}
