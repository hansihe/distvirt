use crate::types::*;

pub struct WorkloadStateMachine {
    pub workload_id: WorkloadId,
    pub state: WorkloadState,
    pub demand_count: u32,
    /// Whether to suspend the pod instead of stopping it when demand drops to zero.
    pub suspend_on_idle: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WorkloadInput {
    DemandUp,
    DemandDown,
    LaunchPod { worker_id: WorkerId, pod_id: PodId },
    /// Outer layer has generated a pod_id for resuming from snapshot.
    ResumePod { worker_id: WorkerId, pod_id: PodId, snapshot_id: SnapshotId },
    PodRunning { pod_id: PodId },
    PodGone { pod_id: PodId },
    PodSuspended { pod_id: PodId, snapshot_id: SnapshotId },
    PodSuspendFailed { pod_id: PodId },
    WorkerLost { worker_id: WorkerId },
    TimerFired { timer_key: TimerKey },
}

#[derive(Debug, Clone, PartialEq)]
pub enum WorkloadOutput {
    PodRequest,
    /// Workload needs a pod resumed from snapshot. The namespace layer
    /// generates a new pod_id and injects ResumePod back.
    ResumeRequest { snapshot_id: SnapshotId, worker_id: WorkerId },
    BecameReady { pod_id: PodId, worker_id: WorkerId },
    BecameUnready,
    WorkerCommand(WorkerId, WorkerCommand),
    TimerSet(TimerKey, std::time::Duration),
    TimerCancel(TimerKey),
}

const LAUNCH_TIMEOUT_SECS: u64 = 60;
const SUSPEND_TIMEOUT_SECS: u64 = 30;
const RESUME_TIMEOUT_SECS: u64 = 60;

impl WorkloadStateMachine {
    pub fn new(workload_id: WorkloadId, suspend_on_idle: bool) -> Self {
        WorkloadStateMachine {
            workload_id,
            state: WorkloadState::Dormant,
            demand_count: 0,
            suspend_on_idle,
        }
    }

    /// Helper: transition to dormant or waiting-for-capacity based on demand.
    fn transition_on_demand(&mut self, outputs: &mut Vec<WorkloadOutput>) {
        if self.demand_count > 0 {
            self.state = WorkloadState::WaitingForCapacity;
            outputs.push(WorkloadOutput::PodRequest);
        } else {
            self.state = WorkloadState::Dormant;
        }
    }

    pub fn step(&mut self, input: WorkloadInput, namespace_id: &NamespaceId) -> Vec<WorkloadOutput> {
        let mut outputs = Vec::new();

        match input {
            WorkloadInput::DemandUp => {
                self.demand_count += 1;
                match &self.state {
                    WorkloadState::Dormant if self.demand_count == 1 => {
                        self.state = WorkloadState::WaitingForCapacity;
                        outputs.push(WorkloadOutput::PodRequest);
                    }
                    WorkloadState::Suspended { worker_id, snapshot_id } if self.demand_count == 1 => {
                        // Resume from snapshot instead of cold boot.
                        let worker_id = worker_id.clone();
                        let snapshot_id = snapshot_id.clone();
                        outputs.push(WorkloadOutput::ResumeRequest {
                            snapshot_id: snapshot_id.clone(),
                            worker_id: worker_id.clone(),
                        });
                    }
                    // If Suspending and demand comes back, we'll handle it
                    // when PodSuspended arrives (check demand_count there).
                    _ => {}
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
                            if self.suspend_on_idle {
                                // Suspend instead of stop.
                                let snapshot_id = SnapshotId::from(format!(
                                    "{}-{}-{}",
                                    namespace_id.0, self.workload_id.0, pod_id.0
                                ));
                                let suspend_timeout = TimerKey::SuspendTimeout {
                                    workload_id: self.workload_id.clone(),
                                    pod_id: pod_id.clone(),
                                };
                                outputs.push(WorkloadOutput::TimerSet(
                                    suspend_timeout.clone(),
                                    std::time::Duration::from_secs(SUSPEND_TIMEOUT_SECS),
                                ));
                                outputs.push(WorkloadOutput::WorkerCommand(
                                    worker_id.clone(),
                                    WorkerCommand::SuspendPod {
                                        namespace_id: namespace_id.clone(),
                                        pod_id: pod_id.clone(),
                                        snapshot_id: snapshot_id.clone(),
                                    },
                                ));
                                self.state = WorkloadState::Suspending {
                                    pod_id,
                                    worker_id,
                                    snapshot_id,
                                    suspend_timeout,
                                };
                            } else {
                                outputs.push(WorkloadOutput::WorkerCommand(
                                    worker_id,
                                    WorkerCommand::StopPod {
                                        namespace_id: namespace_id.clone(),
                                        pod_id,
                                        graceful: true,
                                    },
                                ));
                            }
                        }
                        WorkloadState::Dormant => {}
                        // If already suspending/suspended/resuming and demand drops
                        // further, restore the state — these states handle their own
                        // lifecycle.
                        other @ (WorkloadState::Suspending { .. }
                            | WorkloadState::Suspended { .. }
                            | WorkloadState::Resuming { .. }) => {
                            self.state = other;
                        }
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
                    std::time::Duration::from_secs(LAUNCH_TIMEOUT_SECS),
                ));
                self.state = WorkloadState::Launching {
                    pod_id,
                    worker_id,
                    launch_timeout,
                };
            }
            WorkloadInput::ResumePod { worker_id, pod_id, snapshot_id } => {
                if !matches!(self.state, WorkloadState::Suspended { .. }) {
                    return outputs;
                }
                let resume_timeout = TimerKey::ResumeTimeout {
                    workload_id: self.workload_id.clone(),
                    pod_id: pod_id.clone(),
                };
                outputs.push(WorkloadOutput::TimerSet(
                    resume_timeout.clone(),
                    std::time::Duration::from_secs(RESUME_TIMEOUT_SECS),
                ));
                self.state = WorkloadState::Resuming {
                    pod_id,
                    worker_id,
                    snapshot_id,
                    resume_timeout,
                };
            }
            WorkloadInput::PodRunning { pod_id } => {
                match &self.state {
                    WorkloadState::Launching { pod_id: pid, .. } if *pid == pod_id => {
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
                            self.state = WorkloadState::Running { pod_id, worker_id };
                        }
                    }
                    WorkloadState::Resuming { pod_id: pid, .. } if *pid == pod_id => {
                        if let WorkloadState::Resuming {
                            pod_id,
                            worker_id,
                            snapshot_id,
                            resume_timeout,
                        } = std::mem::replace(&mut self.state, WorkloadState::Dormant)
                        {
                            outputs.push(WorkloadOutput::TimerCancel(resume_timeout));
                            // Delete the snapshot now that the pod is running again.
                            outputs.push(WorkloadOutput::WorkerCommand(
                                worker_id.clone(),
                                WorkerCommand::DeleteSnapshot { snapshot_id },
                            ));

                            if self.demand_count > 0 {
                                outputs.push(WorkloadOutput::BecameReady {
                                    pod_id: pod_id.clone(),
                                    worker_id: worker_id.clone(),
                                });
                                self.state = WorkloadState::Running { pod_id, worker_id };
                            } else {
                                // Demand dropped while we were resuming. Stop/suspend immediately.
                                if self.suspend_on_idle {
                                    let new_snapshot_id = SnapshotId::from(format!(
                                        "{}-{}-{}",
                                        namespace_id.0, self.workload_id.0, pod_id.0
                                    ));
                                    let suspend_timeout = TimerKey::SuspendTimeout {
                                        workload_id: self.workload_id.clone(),
                                        pod_id: pod_id.clone(),
                                    };
                                    outputs.push(WorkloadOutput::TimerSet(
                                        suspend_timeout.clone(),
                                        std::time::Duration::from_secs(SUSPEND_TIMEOUT_SECS),
                                    ));
                                    outputs.push(WorkloadOutput::WorkerCommand(
                                        worker_id.clone(),
                                        WorkerCommand::SuspendPod {
                                            namespace_id: namespace_id.clone(),
                                            pod_id: pod_id.clone(),
                                            snapshot_id: new_snapshot_id.clone(),
                                        },
                                    ));
                                    self.state = WorkloadState::Suspending {
                                        pod_id,
                                        worker_id,
                                        snapshot_id: new_snapshot_id,
                                        suspend_timeout,
                                    };
                                } else {
                                    outputs.push(WorkloadOutput::WorkerCommand(
                                        worker_id,
                                        WorkerCommand::StopPod {
                                            namespace_id: namespace_id.clone(),
                                            pod_id,
                                            graceful: true,
                                        },
                                    ));
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            WorkloadInput::PodSuspended { pod_id, snapshot_id } => {
                // Verify we're suspending this pod.
                let is_suspending = matches!(
                    &self.state,
                    WorkloadState::Suspending { pod_id: pid, snapshot_id: sid, .. }
                        if *pid == pod_id && *sid == snapshot_id
                );
                if !is_suspending {
                    return outputs;
                }
                if let WorkloadState::Suspending {
                    worker_id,
                    snapshot_id,
                    suspend_timeout,
                    ..
                } = std::mem::replace(&mut self.state, WorkloadState::Dormant)
                {
                    outputs.push(WorkloadOutput::TimerCancel(suspend_timeout));

                    if self.demand_count > 0 {
                        // Demand came back while we were suspending — immediately resume.
                        outputs.push(WorkloadOutput::ResumeRequest {
                            snapshot_id,
                            worker_id,
                        });
                    } else {
                        self.state = WorkloadState::Suspended {
                            worker_id,
                            snapshot_id,
                        };
                    }
                }
            }
            WorkloadInput::PodSuspendFailed { pod_id } => {
                let is_suspending = matches!(
                    &self.state,
                    WorkloadState::Suspending { pod_id: pid, .. } if *pid == pod_id
                );
                if !is_suspending {
                    return outputs;
                }
                if let WorkloadState::Suspending {
                    suspend_timeout,
                    ..
                } = std::mem::replace(&mut self.state, WorkloadState::Dormant)
                {
                    outputs.push(WorkloadOutput::TimerCancel(suspend_timeout));
                    // Pod is dead after failed suspend. Transition based on demand.
                    self.transition_on_demand(&mut outputs);
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
                        self.transition_on_demand(&mut outputs);
                    }
                    WorkloadState::Running {
                        pod_id: pid, ..
                    } if *pid == pod_id => {
                        outputs.push(WorkloadOutput::BecameUnready);
                        self.transition_on_demand(&mut outputs);
                    }
                    WorkloadState::Suspending {
                        pod_id: pid,
                        suspend_timeout,
                        ..
                    } if *pid == pod_id => {
                        // Pod died while we were trying to suspend it.
                        outputs.push(WorkloadOutput::TimerCancel(suspend_timeout.clone()));
                        // BecameUnready was already emitted when we entered Suspending.
                        self.transition_on_demand(&mut outputs);
                    }
                    WorkloadState::Resuming {
                        pod_id: pid,
                        resume_timeout,
                        snapshot_id,
                        worker_id,
                        ..
                    } if *pid == pod_id => {
                        // Pod died during resume. Snapshot may be corrupted, delete it.
                        outputs.push(WorkloadOutput::TimerCancel(resume_timeout.clone()));
                        outputs.push(WorkloadOutput::WorkerCommand(
                            worker_id.clone(),
                            WorkerCommand::DeleteSnapshot {
                                snapshot_id: snapshot_id.clone(),
                            },
                        ));
                        outputs.push(WorkloadOutput::BecameUnready);
                        self.transition_on_demand(&mut outputs);
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
                        self.transition_on_demand(&mut outputs);
                    }
                    WorkloadState::Running {
                        worker_id: wid, ..
                    } if *wid == worker_id => {
                        outputs.push(WorkloadOutput::BecameUnready);
                        self.transition_on_demand(&mut outputs);
                    }
                    WorkloadState::Suspending {
                        worker_id: wid,
                        suspend_timeout,
                        ..
                    } if *wid == worker_id => {
                        outputs.push(WorkloadOutput::TimerCancel(suspend_timeout.clone()));
                        // BecameUnready already emitted on entry to Suspending.
                        self.transition_on_demand(&mut outputs);
                    }
                    WorkloadState::Suspended {
                        worker_id: wid,
                        ..
                    } if *wid == worker_id => {
                        // Snapshot is gone with the worker. Fall back to cold boot.
                        self.transition_on_demand(&mut outputs);
                    }
                    WorkloadState::Resuming {
                        worker_id: wid,
                        resume_timeout,
                        ..
                    } if *wid == worker_id => {
                        outputs.push(WorkloadOutput::TimerCancel(resume_timeout.clone()));
                        outputs.push(WorkloadOutput::BecameUnready);
                        self.transition_on_demand(&mut outputs);
                    }
                    _ => {}
                }
            }
            WorkloadInput::TimerFired { timer_key } => {
                match &self.state {
                    WorkloadState::Launching {
                        pod_id,
                        worker_id,
                        launch_timeout,
                    } if *launch_timeout == timer_key => {
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
                        self.transition_on_demand(&mut outputs);
                    }
                    WorkloadState::Suspending {
                        pod_id,
                        worker_id,
                        suspend_timeout,
                        ..
                    } if *suspend_timeout == timer_key => {
                        // Suspend timed out. Force-kill the pod.
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
                        // BecameUnready already emitted on entry to Suspending.
                        self.transition_on_demand(&mut outputs);
                    }
                    WorkloadState::Resuming {
                        pod_id,
                        worker_id,
                        snapshot_id,
                        resume_timeout,
                    } if *resume_timeout == timer_key => {
                        // Resume timed out. Kill the pod and delete snapshot.
                        let pod_id = pod_id.clone();
                        let worker_id = worker_id.clone();
                        let snapshot_id = snapshot_id.clone();
                        outputs.push(WorkloadOutput::WorkerCommand(
                            worker_id.clone(),
                            WorkerCommand::StopPod {
                                namespace_id: namespace_id.clone(),
                                pod_id,
                                graceful: false,
                            },
                        ));
                        outputs.push(WorkloadOutput::WorkerCommand(
                            worker_id,
                            WorkerCommand::DeleteSnapshot { snapshot_id },
                        ));
                        outputs.push(WorkloadOutput::BecameUnready);
                        self.transition_on_demand(&mut outputs);
                    }
                    _ => {
                        // Stale timer, no-op.
                    }
                }
            }
        }

        outputs
    }
}
