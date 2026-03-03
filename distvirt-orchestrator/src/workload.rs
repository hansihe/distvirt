use crate::types::*;

pub struct WorkloadStateMachine {
    pub workload_id: WorkloadId,
    pub state: WorkloadState,
    pub demand_count: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WorkloadInput {
    DemandUp,
    DemandDown,
    LaunchPod { worker_id: WorkerId, pod_id: PodId },
    PodRunning { pod_id: PodId },
    PodGone { pod_id: PodId },
    WorkerLost { worker_id: WorkerId },
    TimerFired { timer_key: TimerKey },
}

#[derive(Debug, Clone, PartialEq)]
pub enum WorkloadOutput {
    PodRequest,
    BecameReady { pod_id: PodId, worker_id: WorkerId },
    BecameUnready,
    WorkerCommand(WorkerId, WorkerCommand),
    TimerSet(TimerKey, std::time::Duration),
    TimerCancel(TimerKey),
}

impl WorkloadStateMachine {
    pub fn new(workload_id: WorkloadId) -> Self {
        WorkloadStateMachine {
            workload_id,
            state: WorkloadState::Dormant,
            demand_count: 0,
        }
    }

    pub fn step(&mut self, input: WorkloadInput, namespace_id: &NamespaceId) -> Vec<WorkloadOutput> {
        let mut outputs = Vec::new();

        match input {
            WorkloadInput::DemandUp => {
                self.demand_count += 1;
                if self.demand_count == 1 && matches!(self.state, WorkloadState::Dormant) {
                    self.state = WorkloadState::WaitingForCapacity;
                    outputs.push(WorkloadOutput::PodRequest);
                }
            }
            WorkloadInput::DemandDown => {
                if self.demand_count > 0 {
                    self.demand_count -= 1;
                }
                if self.demand_count == 0 {
                    match std::mem::replace(&mut self.state, WorkloadState::Dormant) {
                        WorkloadState::WaitingForCapacity => {
                            // Just go dormant, no pod to stop.
                        }
                        WorkloadState::Launching {
                            pod_id,
                            worker_id,
                            launch_timeout,
                        } => {
                            outputs.push(WorkloadOutput::TimerCancel(launch_timeout));
                            outputs.push(WorkloadOutput::WorkerCommand(
                                worker_id,
                                WorkerCommand::StopPod {
                                    namespace_id: namespace_id.clone(),
                                    pod_id,
                                    graceful: false,
                                },
                            ));
                        }
                        WorkloadState::Running {
                            pod_id, worker_id, ..
                        } => {
                            outputs.push(WorkloadOutput::BecameUnready);
                            outputs.push(WorkloadOutput::WorkerCommand(
                                worker_id,
                                WorkerCommand::StopPod {
                                    namespace_id: namespace_id.clone(),
                                    pod_id,
                                    graceful: true,
                                },
                            ));
                        }
                        WorkloadState::Dormant => {}
                    }
                }
            }
            WorkloadInput::LaunchPod { worker_id, pod_id } => {
                if !matches!(self.state, WorkloadState::WaitingForCapacity) {
                    return outputs;
                }
                let launch_timeout = TimerKey::LaunchTimeout {
                    workload_id: self.workload_id.clone(),
                    pod_id: pod_id.clone(),
                };
                outputs.push(WorkloadOutput::TimerSet(
                    launch_timeout.clone(),
                    std::time::Duration::from_secs(60),
                ));
                self.state = WorkloadState::Launching {
                    pod_id,
                    worker_id,
                    launch_timeout,
                };
            }
            WorkloadInput::PodRunning { pod_id } => {
                let is_launching = matches!(
                    &self.state,
                    WorkloadState::Launching { pod_id: pid, .. } if *pid == pod_id
                );
                if !is_launching {
                    return outputs;
                }
                if let WorkloadState::Launching {
                    pod_id,
                    worker_id,
                    launch_timeout,
                } = std::mem::replace(&mut self.state, WorkloadState::Dormant)
                {
                    outputs.push(WorkloadOutput::TimerCancel(launch_timeout));
                    outputs.push(WorkloadOutput::BecameReady {
                        pod_id: pod_id.clone(),
                        worker_id: worker_id.clone(),
                    });
                    self.state = WorkloadState::Running {
                        pod_id,
                        worker_id,
                    };
                }
            }
            WorkloadInput::PodGone { pod_id } => {
                match &self.state {
                    WorkloadState::Launching {
                        pod_id: pid,
                        launch_timeout,
                        ..
                    } if *pid == pod_id => {
                        outputs.push(WorkloadOutput::TimerCancel(launch_timeout.clone()));
                        outputs.push(WorkloadOutput::BecameUnready);
                        if self.demand_count > 0 {
                            self.state = WorkloadState::WaitingForCapacity;
                            outputs.push(WorkloadOutput::PodRequest);
                        } else {
                            self.state = WorkloadState::Dormant;
                        }
                    }
                    WorkloadState::Running {
                        pod_id: pid, ..
                    } if *pid == pod_id => {
                        outputs.push(WorkloadOutput::BecameUnready);
                        if self.demand_count > 0 {
                            self.state = WorkloadState::WaitingForCapacity;
                            outputs.push(WorkloadOutput::PodRequest);
                        } else {
                            self.state = WorkloadState::Dormant;
                        }
                    }
                    _ => {}
                }
            }
            WorkloadInput::WorkerLost { worker_id } => {
                match &self.state {
                    WorkloadState::Launching {
                        worker_id: wid,
                        launch_timeout,
                        ..
                    } if *wid == worker_id => {
                        outputs.push(WorkloadOutput::TimerCancel(launch_timeout.clone()));
                        outputs.push(WorkloadOutput::BecameUnready);
                        if self.demand_count > 0 {
                            self.state = WorkloadState::WaitingForCapacity;
                            outputs.push(WorkloadOutput::PodRequest);
                        } else {
                            self.state = WorkloadState::Dormant;
                        }
                    }
                    WorkloadState::Running {
                        worker_id: wid, ..
                    } if *wid == worker_id => {
                        outputs.push(WorkloadOutput::BecameUnready);
                        if self.demand_count > 0 {
                            self.state = WorkloadState::WaitingForCapacity;
                            outputs.push(WorkloadOutput::PodRequest);
                        } else {
                            self.state = WorkloadState::Dormant;
                        }
                    }
                    _ => {}
                }
            }
            WorkloadInput::TimerFired { timer_key } => {
                if let WorkloadState::Launching {
                    ref pod_id,
                    ref worker_id,
                    ref launch_timeout,
                } = self.state
                {
                    if *launch_timeout == timer_key {
                        let pod_id = pod_id.clone();
                        let worker_id = worker_id.clone();
                        outputs.push(WorkloadOutput::WorkerCommand(
                            worker_id.clone(),
                            WorkerCommand::StopPod {
                                namespace_id: namespace_id.clone(),
                                pod_id,
                                graceful: false,
                            },
                        ));
                        outputs.push(WorkloadOutput::BecameUnready);
                        if self.demand_count > 0 {
                            self.state = WorkloadState::WaitingForCapacity;
                            outputs.push(WorkloadOutput::PodRequest);
                        } else {
                            self.state = WorkloadState::Dormant;
                        }
                    }
                }
            }
        }

        outputs
    }
}
