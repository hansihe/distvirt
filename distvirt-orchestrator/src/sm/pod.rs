use std::time::Duration;

use crate::types::*;

pub const LAUNCH_TIMEOUT_SECS: u64 = 60;
pub const SUSPEND_TIMEOUT_SECS: u64 = 30;
pub const RESUME_TIMEOUT_SECS: u64 = 60;

/// Input events for the pod lifecycle state machine.
#[derive(Debug, Clone, PartialEq)]
pub enum PodInput {
    PodRunning,
    /// Pod is gone. `worker_lost` suppresses artifact deletion for Resuming pods
    /// (the artifact may be on a different worker; namespace layer handles cleanup).
    PodGone { worker_lost: bool },
    PodSuspended { artifact_id: ArtifactId },
    PodSuspendFailed,
    TimerFired { timer_key: TimerKey },
}

/// Side-effect outputs from pod lifecycle transitions.
#[derive(Debug, Clone, PartialEq)]
pub enum PodOutput {
    TimerSet(TimerKey, Duration),
    TimerCancel(TimerKey),
    DeleteArtifact { artifact_id: ArtifactId },
    SuspendRequest { worker_id: WorkerId, artifact_id: ArtifactId },
}

/// Result of a pod lifecycle step, consumed by the workload coordinator.
#[derive(Debug, Clone, PartialEq)]
pub enum PodOutcome {
    /// Pod is now Running.
    Running,
    /// Pod is gone (exited/failed/killed).
    Gone,
    /// Pod suspended successfully.
    Suspended { artifact_id: ArtifactId },
    /// Pod suspend failed (pod is dead).
    SuspendFailed,
    /// A timeout fired. Caller should retire the pod (send StopPod).
    TimedOut,
    /// No state change (stale timer, wrong state).
    Noop,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PodState {
    Launching { launch_timeout: TimerKey },
    Running,
    Suspending { artifact_id: ArtifactId, suspend_timeout: TimerKey },
    Resuming { artifact_id: ArtifactId, resume_timeout: TimerKey },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PodSlot {
    pub pod_id: PodId,
    pub worker_id: WorkerId,
    pub pod_state: PodState,
}

impl PodSlot {
    /// Create a new pod in Launching state with its launch timeout.
    pub fn new_launching(
        pod_id: PodId,
        worker_id: WorkerId,
        workload_id: &WorkloadId,
    ) -> (Self, Vec<PodOutput>) {
        let launch_timeout = TimerKey::LaunchTimeout {
            workload_id: workload_id.clone(),
            pod_id: pod_id.clone(),
        };
        let outputs = vec![PodOutput::TimerSet(
            launch_timeout.clone(),
            Duration::from_secs(LAUNCH_TIMEOUT_SECS),
        )];
        (
            PodSlot {
                pod_id,
                worker_id,
                pod_state: PodState::Launching { launch_timeout },
            },
            outputs,
        )
    }

    /// Create a new pod in Resuming state with its resume timeout.
    pub fn new_resuming(
        pod_id: PodId,
        worker_id: WorkerId,
        workload_id: &WorkloadId,
        artifact_id: ArtifactId,
    ) -> (Self, Vec<PodOutput>) {
        let resume_timeout = TimerKey::ResumeTimeout {
            workload_id: workload_id.clone(),
            pod_id: pod_id.clone(),
        };
        let outputs = vec![PodOutput::TimerSet(
            resume_timeout.clone(),
            Duration::from_secs(RESUME_TIMEOUT_SECS),
        )];
        (
            PodSlot {
                pod_id,
                worker_id,
                pod_state: PodState::Resuming {
                    artifact_id,
                    resume_timeout,
                },
            },
            outputs,
        )
    }

    /// Initiate suspension of a Running pod. Transitions to Suspending state.
    pub fn initiate_suspend(
        &mut self,
        workload_id: &WorkloadId,
        artifact_id: ArtifactId,
    ) -> Vec<PodOutput> {
        assert!(
            matches!(self.pod_state, PodState::Running),
            "initiate_suspend called on non-Running pod"
        );
        let suspend_timeout = TimerKey::SuspendTimeout {
            workload_id: workload_id.clone(),
            pod_id: self.pod_id.clone(),
        };
        self.pod_state = PodState::Suspending {
            artifact_id: artifact_id.clone(),
            suspend_timeout: suspend_timeout.clone(),
        };
        vec![
            PodOutput::TimerSet(
                suspend_timeout,
                Duration::from_secs(SUSPEND_TIMEOUT_SECS),
            ),
            PodOutput::SuspendRequest {
                worker_id: self.worker_id.clone(),
                artifact_id,
            },
        ]
    }

    /// Process a pod lifecycle event. Returns the outcome and side-effect outputs.
    ///
    /// The caller (workload SM) interprets the outcome for workload-level transitions
    /// and converts `PodOutput`s into `WorkloadOutput`s.
    pub fn step(&mut self, input: PodInput) -> (PodOutcome, Vec<PodOutput>) {
        let mut outputs = Vec::new();
        let outcome = match input {
            PodInput::PodRunning => {
                match std::mem::replace(&mut self.pod_state, PodState::Running) {
                    PodState::Launching { launch_timeout } => {
                        outputs.push(PodOutput::TimerCancel(launch_timeout));
                        PodOutcome::Running
                    }
                    PodState::Resuming {
                        artifact_id,
                        resume_timeout,
                    } => {
                        outputs.push(PodOutput::TimerCancel(resume_timeout));
                        outputs.push(PodOutput::DeleteArtifact { artifact_id });
                        PodOutcome::Running
                    }
                    other => {
                        self.pod_state = other;
                        PodOutcome::Noop
                    }
                }
            }
            PodInput::PodGone { worker_lost } => match &self.pod_state {
                PodState::Launching { launch_timeout } => {
                    outputs.push(PodOutput::TimerCancel(launch_timeout.clone()));
                    PodOutcome::Gone
                }
                PodState::Running => PodOutcome::Gone,
                PodState::Suspending {
                    suspend_timeout, ..
                } => {
                    outputs.push(PodOutput::TimerCancel(suspend_timeout.clone()));
                    PodOutcome::Gone
                }
                PodState::Resuming {
                    artifact_id,
                    resume_timeout,
                } => {
                    outputs.push(PodOutput::TimerCancel(resume_timeout.clone()));
                    if !worker_lost {
                        outputs.push(PodOutput::DeleteArtifact {
                            artifact_id: artifact_id.clone(),
                        });
                    }
                    PodOutcome::Gone
                }
            },
            PodInput::PodSuspended { artifact_id } => match &self.pod_state {
                PodState::Suspending {
                    artifact_id: aid, ..
                } if *aid == artifact_id => {
                    // Use mem::replace to take ownership of the state fields.
                    if let PodState::Suspending {
                        artifact_id,
                        suspend_timeout,
                    } = std::mem::replace(&mut self.pod_state, PodState::Running)
                    {
                        outputs.push(PodOutput::TimerCancel(suspend_timeout));
                        PodOutcome::Suspended { artifact_id }
                    } else {
                        unreachable!()
                    }
                }
                _ => PodOutcome::Noop,
            },
            PodInput::PodSuspendFailed => match &self.pod_state {
                PodState::Suspending {
                    suspend_timeout, ..
                } => {
                    outputs.push(PodOutput::TimerCancel(suspend_timeout.clone()));
                    PodOutcome::SuspendFailed
                }
                _ => PodOutcome::Noop,
            },
            PodInput::TimerFired { timer_key } => match &self.pod_state {
                PodState::Launching { launch_timeout } if *launch_timeout == timer_key => {
                    PodOutcome::TimedOut
                }
                PodState::Suspending { suspend_timeout, .. } if *suspend_timeout == timer_key => {
                    PodOutcome::TimedOut
                }
                PodState::Resuming {
                    artifact_id,
                    resume_timeout,
                } if *resume_timeout == timer_key => {
                    outputs.push(PodOutput::DeleteArtifact {
                        artifact_id: artifact_id.clone(),
                    });
                    PodOutcome::TimedOut
                }
                _ => PodOutcome::Noop,
            },
        };
        (outcome, outputs)
    }
}
