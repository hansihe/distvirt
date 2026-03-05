use crate::types::*;

/// Reason a pod was lost, propagated from the namespace event layer.
#[derive(Debug, Clone, PartialEq)]
pub enum PodGoneReason {
    Exited { exit_code: i32 },
    Failed { error: String },
    WorkerLost,
    Timeout,
}

impl std::fmt::Display for PodGoneReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PodGoneReason::Exited { exit_code } => write!(f, "exited with code {}", exit_code),
            PodGoneReason::Failed { error } => write!(f, "{}", error),
            PodGoneReason::WorkerLost => write!(f, "worker lost"),
            PodGoneReason::Timeout => write!(f, "operation timed out"),
        }
    }
}

pub struct WorkloadStateMachine {
    pub workload_id: WorkloadId,
    pub state: WorkloadState,
    pub demand_count: u32,
    /// Whether to suspend the pod instead of stopping it when demand drops to zero.
    pub suspend_on_idle: bool,
    /// Reason for the most recent pod failure, for observability.
    pub last_failure_reason: Option<PodGoneReason>,
    /// Number of consecutive pod failures without a successful PodRunning in between.
    pub consecutive_failures: u32,
    /// Maximum number of retries before entering terminal Failed state.
    /// Defaults to MAX_RETRIES (5). Can be lowered for model checking.
    pub max_retries: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WorkloadInput {
    DemandUp,
    DemandDown,
    LaunchPod { worker_id: WorkerId, pod_id: PodId },
    /// Outer layer has generated a pod_id for resuming from snapshot.
    ResumePod { worker_id: WorkerId, pod_id: PodId, artifact_id: ArtifactId },
    PodRunning { pod_id: PodId },
    PodGone { pod_id: PodId, reason: Option<PodGoneReason> },
    PodSuspended { pod_id: PodId, artifact_id: ArtifactId },
    PodSuspendFailed { pod_id: PodId },
    WorkerLost { worker_id: WorkerId },
    TimerFired { timer_key: TimerKey },
    /// Admin override: deactivate the workload regardless of demand.
    ForceDeactivate,
    /// Spec has changed — restart with new config (resets failure counter).
    SpecChanged,
    /// Manual restart request (e.g. from CLI) — resets failure counter.
    ManualRestart,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WorkloadOutput {
    PodRequest,
    /// Workload needs a pod suspended. The namespace output forwarding layer
    /// resolves the pool_id from the worker state, creates a placement table entry,
    /// and emits the actual WorkerCommand.
    SuspendRequest { worker_id: WorkerId, artifact_id: ArtifactId },
    /// Workload needs a pod resumed from snapshot. The namespace layer
    /// looks up the placement table and generates a new pod_id.
    ResumeRequest { artifact_id: ArtifactId },
    /// Artifact should be deleted. The namespace layer resolves placement
    /// and emits the actual WorkerCommand::DeleteArtifact.
    DeleteArtifact { artifact_id: ArtifactId },
    BecameReady { pod_id: PodId, worker_id: WorkerId },
    BecameUnready,
    WorkerCommand(WorkerId, WorkerCommand),
    TimerSet(TimerKey, std::time::Duration),
    TimerCancel(TimerKey),
    ConditionSet { key: String, message: String },
    ConditionClear { key: String },
}

const LAUNCH_TIMEOUT_SECS: u64 = 60;
const SUSPEND_TIMEOUT_SECS: u64 = 30;
const RESUME_TIMEOUT_SECS: u64 = 60;
const MAX_RETRIES: u32 = 5;

fn backoff_delay(failures: u32) -> std::time::Duration {
    let secs = 1u64 << (failures - 1).min(5);
    std::time::Duration::from_secs(secs)
}

impl WorkloadStateMachine {
    pub fn new(workload_id: WorkloadId, suspend_on_idle: bool) -> Self {
        WorkloadStateMachine {
            workload_id,
            state: WorkloadState::Dormant,
            demand_count: 0,
            suspend_on_idle,
            last_failure_reason: None,
            consecutive_failures: 0,
            max_retries: MAX_RETRIES,
        }
    }

    /// Helper: transition to dormant or waiting-for-capacity based on demand,
    /// with exponential backoff on consecutive failures.
    fn transition_on_demand(&mut self, outputs: &mut Vec<WorkloadOutput>) {
        if self.demand_count > 0 {
            if self.consecutive_failures >= self.max_retries {
                self.state = WorkloadState::Failed;
                outputs.push(WorkloadOutput::ConditionSet {
                    key: "failed".into(),
                    message: format!(
                        "{} (attempt {}/{})",
                        self.last_failure_reason
                            .as_ref()
                            .map(|r| r.to_string())
                            .unwrap_or_else(|| "unknown".into()),
                        self.consecutive_failures,
                        self.max_retries,
                    ),
                });
            } else if self.consecutive_failures > 0 {
                let delay = backoff_delay(self.consecutive_failures);
                let timer_key = TimerKey::RetryBackoffTimeout {
                    workload_id: self.workload_id.clone(),
                };
                outputs.push(WorkloadOutput::TimerSet(timer_key.clone(), delay));
                outputs.push(WorkloadOutput::ConditionSet {
                    key: "retry-backoff".into(),
                    message: format!(
                        "attempt {}/{}, next retry in {:?}",
                        self.consecutive_failures + 1,
                        self.max_retries,
                        delay,
                    ),
                });
                self.state = WorkloadState::RetryBackoff { backoff_timer: timer_key };
            } else {
                self.state = WorkloadState::WaitingForCapacity;
                outputs.push(WorkloadOutput::PodRequest);
            }
        } else {
            self.state = WorkloadState::Dormant;
        }
    }

    /// Helper: transition based on a pending intent captured from a transition state.
    /// Used at error/abort paths where the pod is gone.
    fn transition_on_intent(&mut self, intent: PendingIntent, outputs: &mut Vec<WorkloadOutput>) {
        match intent {
            PendingIntent::Deactivate => {
                // Admin override: go Dormant regardless of demand.
                self.state = WorkloadState::Dormant;
            }
            PendingIntent::Restart => {
                // New spec = fresh attempt, reset failure counter.
                self.consecutive_failures = 0;
                self.transition_on_demand(outputs);
            }
            PendingIntent::Demand | PendingIntent::None => {
                // Fall back to demand_count check (existing behavior).
                self.transition_on_demand(outputs);
            }
        }
    }

    /// Sets `pending = max(pending, intent)` on the current transition state.
    fn upgrade_pending(&mut self, intent: PendingIntent) -> bool {
        match &mut self.state {
            WorkloadState::Launching { pending, .. }
            | WorkloadState::Suspending { pending, .. }
            | WorkloadState::Resuming { pending, .. } => {
                *pending = (*pending).max(intent);
                true
            }
            _ => false,
        }
    }

    pub fn step(&mut self, input: WorkloadInput, namespace_id: &NamespaceId) -> Vec<WorkloadOutput> {
        let mut outputs = Vec::new();

        match input {
            // INVARIANT: every DemandUp must have a corresponding entity that will eventually DemandDown
            WorkloadInput::DemandUp => {
                self.demand_count += 1;
                match &self.state {
                    WorkloadState::Dormant if self.demand_count == 1 => {
                        self.state = WorkloadState::WaitingForCapacity;
                        outputs.push(WorkloadOutput::PodRequest);
                    }
                    WorkloadState::Suspended { artifact_id } if self.demand_count == 1 => {
                        // Resume from snapshot instead of cold boot.
                        outputs.push(WorkloadOutput::ResumeRequest {
                            artifact_id: artifact_id.clone(),
                        });
                    }
                    WorkloadState::Launching { .. }
                    | WorkloadState::Suspending { .. }
                    | WorkloadState::Resuming { .. } => {
                        self.upgrade_pending(PendingIntent::Demand);
                    }
                    _ => {}
                }
            }
            WorkloadInput::DemandDown => {
                if self.demand_count > 0 {
                    self.demand_count -= 1;
                }
                if self.demand_count == 0 {
                    match std::mem::replace(&mut self.state, WorkloadState::Transitioning) {
                        WorkloadState::WaitingForCapacity => {
                            self.state = WorkloadState::Dormant;
                        }
                        WorkloadState::Launching {
                            pod_id,
                            worker_id,
                            launch_timeout,
                            ..
                        } => {
                            outputs.push(WorkloadOutput::TimerCancel(launch_timeout));
                            self.state = WorkloadState::Dormant;
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
                                let artifact_id = ArtifactId::from(format!(
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
                                outputs.push(WorkloadOutput::SuspendRequest {
                                    worker_id: worker_id.clone(),
                                    artifact_id: artifact_id.clone(),
                                });
                                self.state = WorkloadState::Suspending {
                                    pod_id,
                                    worker_id,
                                    artifact_id,
                                    suspend_timeout,
                                    pending: PendingIntent::None,
                                };
                            } else {
                                self.state = WorkloadState::Dormant;
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
                        WorkloadState::Dormant => {
                            self.state = WorkloadState::Dormant;
                        }
                        // If already suspending/suspended/resuming and demand drops
                        // further, restore the state — these states handle their own
                        // lifecycle.
                        WorkloadState::RetryBackoff { backoff_timer } => {
                            outputs.push(WorkloadOutput::TimerCancel(backoff_timer));
                            outputs.push(WorkloadOutput::ConditionClear { key: "retry-backoff".into() });
                            self.consecutive_failures = 0;
                            self.state = WorkloadState::Dormant;
                        }
                        WorkloadState::Failed => {
                            outputs.push(WorkloadOutput::ConditionClear { key: "failed".into() });
                            self.consecutive_failures = 0;
                            self.state = WorkloadState::Dormant;
                        }
                        other @ (WorkloadState::Suspending { .. }
                            | WorkloadState::Suspended { .. }
                            | WorkloadState::Resuming { .. }) => {
                            // Maintain invariant: clear Demand intent if demand dropped to 0
                            self.state = other;
                            match &mut self.state {
                                WorkloadState::Suspending { pending, .. }
                                | WorkloadState::Resuming { pending, .. }
                                    if *pending == PendingIntent::Demand =>
                                {
                                    *pending = PendingIntent::None;
                                }
                                _ => {}
                            }
                        }
                        WorkloadState::Transitioning => unreachable!("Transitioning in DemandDown"),
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
                    pending: PendingIntent::None,
                };
            }
            WorkloadInput::ResumePod { worker_id, pod_id, artifact_id } => {
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
                    artifact_id,
                    resume_timeout,
                    pending: PendingIntent::None,
                };
            }
            WorkloadInput::PodRunning { pod_id } => {
                self.consecutive_failures = 0;
                self.last_failure_reason = None;
                outputs.push(WorkloadOutput::ConditionClear { key: "retry-backoff".into() });
                match &self.state {
                    WorkloadState::Launching { pod_id: pid, .. } if *pid == pod_id => {
                        if let WorkloadState::Launching {
                            pod_id,
                            worker_id,
                            launch_timeout,
                            pending,
                        } = std::mem::replace(&mut self.state, WorkloadState::Transitioning)
                        {
                            outputs.push(WorkloadOutput::TimerCancel(launch_timeout));
                            match pending {
                                PendingIntent::None | PendingIntent::Demand => {
                                    outputs.push(WorkloadOutput::BecameReady {
                                        pod_id: pod_id.clone(),
                                        worker_id: worker_id.clone(),
                                    });
                                    self.state = WorkloadState::Running { pod_id, worker_id };
                                }
                                PendingIntent::Deactivate | PendingIntent::Restart => {
                                    // Admin override or restart: stop pod immediately.
                                    outputs.push(WorkloadOutput::WorkerCommand(
                                        worker_id,
                                        WorkerCommand::StopPod {
                                            namespace_id: namespace_id.clone(),
                                            pod_id,
                                            graceful: false,
                                        },
                                    ));
                                    self.state = WorkloadState::Dormant;
                                }
                            }
                        }
                    }
                    WorkloadState::Resuming { pod_id: pid, .. } if *pid == pod_id => {
                        if let WorkloadState::Resuming {
                            pod_id,
                            worker_id,
                            artifact_id,
                            resume_timeout,
                            pending,
                        } = std::mem::replace(&mut self.state, WorkloadState::Transitioning)
                        {
                            outputs.push(WorkloadOutput::TimerCancel(resume_timeout));
                            // Delete the artifact now that the pod is running again.
                            outputs.push(WorkloadOutput::DeleteArtifact {
                                artifact_id: artifact_id.clone(),
                            });

                            match pending {
                                PendingIntent::Deactivate | PendingIntent::Restart => {
                                    // Admin override: stop pod, go Dormant.
                                    outputs.push(WorkloadOutput::WorkerCommand(
                                        worker_id,
                                        WorkerCommand::StopPod {
                                            namespace_id: namespace_id.clone(),
                                            pod_id,
                                            graceful: false,
                                        },
                                    ));
                                    self.state = WorkloadState::Dormant;
                                }
                                PendingIntent::Demand => {
                                    // Demand is guaranteed > 0 by invariant.
                                    outputs.push(WorkloadOutput::BecameReady {
                                        pod_id: pod_id.clone(),
                                        worker_id: worker_id.clone(),
                                    });
                                    self.state = WorkloadState::Running { pod_id, worker_id };
                                }
                                PendingIntent::None => {
                                    if self.demand_count > 0 {
                                        outputs.push(WorkloadOutput::BecameReady {
                                            pod_id: pod_id.clone(),
                                            worker_id: worker_id.clone(),
                                        });
                                        self.state = WorkloadState::Running { pod_id, worker_id };
                                    } else {
                                        // Demand dropped while we were resuming. Stop/suspend immediately.
                                        if self.suspend_on_idle {
                                            let new_artifact_id = ArtifactId::from(format!(
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
                                            outputs.push(WorkloadOutput::SuspendRequest {
                                                worker_id: worker_id.clone(),
                                                artifact_id: new_artifact_id.clone(),
                                            });
                                            self.state = WorkloadState::Suspending {
                                                pod_id,
                                                worker_id,
                                                artifact_id: new_artifact_id,
                                                suspend_timeout,
                                                pending: PendingIntent::None,
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
                        }
                    }
                    _ => {}
                }
            }
            WorkloadInput::PodSuspended { pod_id, artifact_id } => {
                // Verify we're suspending this pod.
                let is_suspending = matches!(
                    &self.state,
                    WorkloadState::Suspending { pod_id: pid, artifact_id: aid, .. }
                        if *pid == pod_id && *aid == artifact_id
                );
                if !is_suspending {
                    return outputs;
                }
                if let WorkloadState::Suspending {
                    artifact_id,
                    suspend_timeout,
                    pending,
                    ..
                } = std::mem::replace(&mut self.state, WorkloadState::Transitioning)
                {
                    outputs.push(WorkloadOutput::TimerCancel(suspend_timeout));

                    match pending {
                        PendingIntent::Deactivate => {
                            // Don't resume, even if demand > 0.
                            self.state = WorkloadState::Suspended { artifact_id };
                        }
                        PendingIntent::Restart => {
                            // Task 1.3: old snapshot incompatible with new spec.
                            outputs.push(WorkloadOutput::DeleteArtifact {
                                artifact_id,
                            });
                            self.transition_on_demand(&mut outputs);
                        }
                        PendingIntent::Demand => {
                            // Demand is guaranteed > 0 by invariant — immediately resume.
                            self.state = WorkloadState::Suspended { artifact_id: artifact_id.clone() };
                            outputs.push(WorkloadOutput::ResumeRequest { artifact_id });
                        }
                        PendingIntent::None => {
                            if self.demand_count > 0 {
                                // Demand came back while we were suspending — immediately resume.
                                self.state = WorkloadState::Suspended { artifact_id: artifact_id.clone() };
                                outputs.push(WorkloadOutput::ResumeRequest { artifact_id });
                            } else {
                                self.state = WorkloadState::Suspended { artifact_id };
                            }
                        }
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
                self.consecutive_failures += 1;
                if let WorkloadState::Suspending {
                    suspend_timeout,
                    pending,
                    ..
                } = std::mem::replace(&mut self.state, WorkloadState::Transitioning)
                {
                    outputs.push(WorkloadOutput::TimerCancel(suspend_timeout));
                    // Pod is dead after failed suspend. Transition based on intent.
                    self.transition_on_intent(pending, &mut outputs);
                }
            }
            WorkloadInput::PodGone { pod_id, reason } => {
                self.last_failure_reason = reason.clone();
                // Only count as a failure for backoff if it's not a clean exit.
                let is_failure = match &reason {
                    Some(PodGoneReason::Exited { exit_code }) => *exit_code != 0,
                    Some(_) => true,
                    None => true,  // Unknown reason treated as failure.
                };
                if is_failure {
                    self.consecutive_failures += 1;
                }
                match &self.state {
                    WorkloadState::Launching {
                        pod_id: pid,
                        ..
                    } if *pid == pod_id => {
                        if let WorkloadState::Launching {
                            launch_timeout,
                            pending,
                            ..
                        } = std::mem::replace(&mut self.state, WorkloadState::Transitioning)
                        {
                            outputs.push(WorkloadOutput::TimerCancel(launch_timeout));
                            outputs.push(WorkloadOutput::BecameUnready);
                            self.transition_on_intent(pending, &mut outputs);
                        }
                    }
                    WorkloadState::Running {
                        pod_id: pid, ..
                    } if *pid == pod_id => {
                        outputs.push(WorkloadOutput::BecameUnready);
                        self.transition_on_demand(&mut outputs);
                    }
                    WorkloadState::Suspending {
                        pod_id: pid,
                        ..
                    } if *pid == pod_id => {
                        if let WorkloadState::Suspending {
                            suspend_timeout,
                            pending,
                            ..
                        } = std::mem::replace(&mut self.state, WorkloadState::Transitioning)
                        {
                            // Pod died while we were trying to suspend it.
                            outputs.push(WorkloadOutput::TimerCancel(suspend_timeout));
                            // BecameUnready was already emitted when we entered Suspending.
                            self.transition_on_intent(pending, &mut outputs);
                        }
                    }
                    WorkloadState::Resuming {
                        pod_id: pid,
                        ..
                    } if *pid == pod_id => {
                        if let WorkloadState::Resuming {
                            resume_timeout,
                            artifact_id,
                            pending,
                            ..
                        } = std::mem::replace(&mut self.state, WorkloadState::Transitioning)
                        {
                            // Pod died during resume. Snapshot may be corrupted, delete it.
                            outputs.push(WorkloadOutput::TimerCancel(resume_timeout));
                            outputs.push(WorkloadOutput::DeleteArtifact {
                                artifact_id,
                            });
                            outputs.push(WorkloadOutput::BecameUnready);
                            self.transition_on_intent(pending, &mut outputs);
                        }
                    }
                    _ => {}
                }
            }
            WorkloadInput::WorkerLost { worker_id } => {
                match &self.state {
                    WorkloadState::Launching {
                        worker_id: wid,
                        ..
                    } if *wid == worker_id => {
                        if let WorkloadState::Launching {
                            launch_timeout,
                            pending,
                            ..
                        } = std::mem::replace(&mut self.state, WorkloadState::Transitioning)
                        {
                            outputs.push(WorkloadOutput::TimerCancel(launch_timeout));
                            outputs.push(WorkloadOutput::BecameUnready);
                            self.transition_on_intent(pending, &mut outputs);
                        }
                    }
                    WorkloadState::Running {
                        worker_id: wid, ..
                    } if *wid == worker_id => {
                        outputs.push(WorkloadOutput::BecameUnready);
                        self.transition_on_demand(&mut outputs);
                    }
                    WorkloadState::Suspending {
                        worker_id: wid,
                        ..
                    } if *wid == worker_id => {
                        if let WorkloadState::Suspending {
                            suspend_timeout,
                            pending,
                            ..
                        } = std::mem::replace(&mut self.state, WorkloadState::Transitioning)
                        {
                            outputs.push(WorkloadOutput::TimerCancel(suspend_timeout));
                            // BecameUnready already emitted on entry to Suspending.
                            self.transition_on_intent(pending, &mut outputs);
                        }
                    }
                    WorkloadState::Suspended { .. } => {
                        // Artifact is gone with the worker (placement table cleanup
                        // handled by namespace layer). Fall back to cold boot.
                        self.transition_on_demand(&mut outputs);
                    }
                    WorkloadState::Resuming {
                        worker_id: wid,
                        ..
                    } if *wid == worker_id => {
                        if let WorkloadState::Resuming {
                            resume_timeout,
                            pending,
                            ..
                        } = std::mem::replace(&mut self.state, WorkloadState::Transitioning)
                        {
                            outputs.push(WorkloadOutput::TimerCancel(resume_timeout));
                            outputs.push(WorkloadOutput::BecameUnready);
                            self.transition_on_intent(pending, &mut outputs);
                        }
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
                        ..
                    } if *launch_timeout == timer_key => {
                        let pod_id = pod_id.clone();
                        let worker_id = worker_id.clone();
                        let pending = if let WorkloadState::Launching { pending, .. } = &self.state {
                            *pending
                        } else {
                            PendingIntent::None
                        };
                        outputs.push(WorkloadOutput::WorkerCommand(
                            worker_id.clone(),
                            WorkerCommand::StopPod {
                                namespace_id: namespace_id.clone(),
                                pod_id,
                                graceful: false,
                            },
                        ));
                        outputs.push(WorkloadOutput::BecameUnready);
                        self.transition_on_intent(pending, &mut outputs);
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
                        let pending = if let WorkloadState::Suspending { pending, .. } = &self.state {
                            *pending
                        } else {
                            PendingIntent::None
                        };
                        outputs.push(WorkloadOutput::WorkerCommand(
                            worker_id.clone(),
                            WorkerCommand::StopPod {
                                namespace_id: namespace_id.clone(),
                                pod_id,
                                graceful: false,
                            },
                        ));
                        // BecameUnready already emitted on entry to Suspending.
                        self.transition_on_intent(pending, &mut outputs);
                    }
                    WorkloadState::Resuming {
                        pod_id,
                        worker_id,
                        artifact_id,
                        resume_timeout,
                        ..
                    } if *resume_timeout == timer_key => {
                        // Resume timed out. Kill the pod and delete artifact.
                        let pod_id = pod_id.clone();
                        let worker_id = worker_id.clone();
                        let artifact_id = artifact_id.clone();
                        let pending = if let WorkloadState::Resuming { pending, .. } = &self.state {
                            *pending
                        } else {
                            PendingIntent::None
                        };
                        outputs.push(WorkloadOutput::WorkerCommand(
                            worker_id.clone(),
                            WorkerCommand::StopPod {
                                namespace_id: namespace_id.clone(),
                                pod_id,
                                graceful: false,
                            },
                        ));
                        outputs.push(WorkloadOutput::DeleteArtifact { artifact_id });
                        outputs.push(WorkloadOutput::BecameUnready);
                        self.transition_on_intent(pending, &mut outputs);
                    }
                    WorkloadState::RetryBackoff { backoff_timer }
                        if *backoff_timer == timer_key =>
                    {
                        outputs.push(WorkloadOutput::ConditionClear { key: "retry-backoff".into() });
                        self.state = WorkloadState::WaitingForCapacity;
                        outputs.push(WorkloadOutput::PodRequest);
                    }
                    _ => {
                        // Stale timer, no-op.
                    }
                }
            }
            WorkloadInput::ForceDeactivate => {
                match &self.state {
                    WorkloadState::Dormant | WorkloadState::WaitingForCapacity => {
                        // Already inactive, no-op.
                    }
                    WorkloadState::Running { .. } => {
                        if let WorkloadState::Running { pod_id, worker_id } =
                            std::mem::replace(&mut self.state, WorkloadState::Transitioning)
                        {
                            outputs.push(WorkloadOutput::BecameUnready);
                            if self.suspend_on_idle {
                                let artifact_id = ArtifactId::from(format!(
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
                                outputs.push(WorkloadOutput::SuspendRequest {
                                    worker_id: worker_id.clone(),
                                    artifact_id: artifact_id.clone(),
                                });
                                self.state = WorkloadState::Suspending {
                                    pod_id,
                                    worker_id,
                                    artifact_id,
                                    suspend_timeout,
                                    pending: PendingIntent::Deactivate,
                                };
                            } else {
                                self.state = WorkloadState::Dormant;
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
                    WorkloadState::Launching { .. }
                    | WorkloadState::Suspending { .. }
                    | WorkloadState::Resuming { .. } => {
                        self.upgrade_pending(PendingIntent::Deactivate);
                    }
                    WorkloadState::Suspended { .. } => {
                        // Drop from suspended to fully off.
                        self.state = WorkloadState::Dormant;
                    }
                    WorkloadState::RetryBackoff { .. } => {
                        if let WorkloadState::RetryBackoff { backoff_timer } =
                            std::mem::replace(&mut self.state, WorkloadState::Transitioning)
                        {
                            outputs.push(WorkloadOutput::TimerCancel(backoff_timer));
                            outputs.push(WorkloadOutput::ConditionClear { key: "retry-backoff".into() });
                            self.consecutive_failures = 0;
                            self.state = WorkloadState::Dormant;
                        }
                    }
                    WorkloadState::Failed => {
                        outputs.push(WorkloadOutput::ConditionClear { key: "failed".into() });
                        self.consecutive_failures = 0;
                        self.state = WorkloadState::Dormant;
                    }
                    WorkloadState::Transitioning => unreachable!("Transitioning in ForceDeactivate"),
                }
            }
            WorkloadInput::SpecChanged => {
                match &self.state {
                    WorkloadState::Dormant | WorkloadState::WaitingForCapacity => {
                        // No-op: will launch with new spec next time.
                    }
                    WorkloadState::Launching { .. }
                    | WorkloadState::Suspending { .. }
                    | WorkloadState::Resuming { .. } => {
                        self.upgrade_pending(PendingIntent::Restart);
                    }
                    WorkloadState::Running { .. } => {
                        if let WorkloadState::Running { pod_id, worker_id } =
                            std::mem::replace(&mut self.state, WorkloadState::Transitioning)
                        {
                            outputs.push(WorkloadOutput::WorkerCommand(
                                worker_id,
                                WorkerCommand::StopPod {
                                    namespace_id: namespace_id.clone(),
                                    pod_id,
                                    graceful: false,
                                },
                            ));
                            outputs.push(WorkloadOutput::BecameUnready);
                            self.consecutive_failures = 0;
                            self.transition_on_demand(&mut outputs);
                        }
                    }
                    WorkloadState::Suspended { .. } => {
                        if let WorkloadState::Suspended { artifact_id } =
                            std::mem::replace(&mut self.state, WorkloadState::Transitioning)
                        {
                            outputs.push(WorkloadOutput::DeleteArtifact { artifact_id });
                            self.consecutive_failures = 0;
                            self.transition_on_demand(&mut outputs);
                        }
                    }
                    WorkloadState::RetryBackoff { .. } => {
                        if let WorkloadState::RetryBackoff { backoff_timer } =
                            std::mem::replace(&mut self.state, WorkloadState::Transitioning)
                        {
                            outputs.push(WorkloadOutput::TimerCancel(backoff_timer));
                            outputs.push(WorkloadOutput::ConditionClear { key: "retry-backoff".into() });
                            self.consecutive_failures = 0;
                            self.transition_on_demand(&mut outputs);
                        }
                    }
                    WorkloadState::Failed => {
                        self.consecutive_failures = 0;
                        outputs.push(WorkloadOutput::ConditionClear { key: "failed".into() });
                        self.transition_on_demand(&mut outputs);
                    }
                    WorkloadState::Transitioning => unreachable!("Transitioning in SpecChanged"),
                }
            }
            WorkloadInput::ManualRestart => {
                match &self.state {
                    WorkloadState::Running { .. } => {
                        if let WorkloadState::Running { pod_id, worker_id } =
                            std::mem::replace(&mut self.state, WorkloadState::Transitioning)
                        {
                            outputs.push(WorkloadOutput::WorkerCommand(
                                worker_id,
                                WorkerCommand::StopPod {
                                    namespace_id: namespace_id.clone(),
                                    pod_id,
                                    graceful: false,
                                },
                            ));
                            outputs.push(WorkloadOutput::BecameUnready);
                            self.consecutive_failures = 0;
                            self.transition_on_demand(&mut outputs);
                        }
                    }
                    WorkloadState::RetryBackoff { .. } => {
                        if let WorkloadState::RetryBackoff { backoff_timer } =
                            std::mem::replace(&mut self.state, WorkloadState::Transitioning)
                        {
                            outputs.push(WorkloadOutput::TimerCancel(backoff_timer));
                            outputs.push(WorkloadOutput::ConditionClear { key: "retry-backoff".into() });
                            self.consecutive_failures = 0;
                            self.transition_on_demand(&mut outputs);
                        }
                    }
                    WorkloadState::Failed => {
                        self.consecutive_failures = 0;
                        outputs.push(WorkloadOutput::ConditionClear { key: "failed".into() });
                        self.transition_on_demand(&mut outputs);
                    }
                    _ => {
                        // Other states: no-op.
                    }
                }
            }
        }

        outputs
    }
}
