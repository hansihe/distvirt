use crate::types::*;
use super::pod::{PodInput, PodOutput, PodOutcome, PodSlot, PodState};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub enum PendingIntent {
    #[default]
    None,
    Demand,
    Deactivate,
    Restart, // Produced by WorkloadInput::SpecChanged when spec changes during a transition
}

/// A pod that has been told to stop but hasn't confirmed gone yet.
/// Tracked in `WorkloadStateMachine.retiring` until PodGone confirms termination.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RetiredPod {
    pub pod_id: PodId,
    pub worker_id: WorkerId,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WorkloadState {
    Dormant,
    WaitingForCapacity,
    Active { pod: PodSlot, pending: PendingIntent },
    /// Pod is suspended. Artifact tracked in placement table.
    Suspended {
        artifact_id: ArtifactId,
    },
    /// Waiting before retrying after a pod failure.
    RetryBackoff {
        backoff_timer: TimerKey,
    },
    /// Terminal failure state after max retries exhausted.
    Failed,
    /// Transient sentinel used during `mem::replace` destructuring.
    /// Must never be observed outside of a single `step()` call.
    /// If this variant survives, it means a code path forgot to set the final state.
    Transitioning,
}

impl WorkloadState {
    pub fn as_str(&self) -> &'static str {
        match self {
            WorkloadState::Dormant => "dormant",
            WorkloadState::WaitingForCapacity => "waiting_for_capacity",
            WorkloadState::Active { pod, .. } => match &pod.pod_state {
                PodState::Launching { .. } => "launching",
                PodState::Running => "running",
                PodState::Suspending { .. } => "suspending",
                PodState::Resuming { .. } => "resuming",
            },
            WorkloadState::Suspended { .. } => "suspended",
            WorkloadState::RetryBackoff { .. } => "retry_backoff",
            WorkloadState::Failed => "failed",
            WorkloadState::Transitioning => panic!("WorkloadState::Transitioning leaked outside step()"),
        }
    }

    pub fn pod_id(&self) -> Option<&PodId> {
        match self {
            WorkloadState::Active { pod, .. } => Some(&pod.pod_id),
            WorkloadState::Transitioning => panic!("WorkloadState::Transitioning leaked outside step()"),
            _ => None,
        }
    }

    pub fn worker_id(&self) -> Option<&WorkerId> {
        match self {
            WorkloadState::Active { pod, .. } => Some(&pod.worker_id),
            WorkloadState::Transitioning => panic!("WorkloadState::Transitioning leaked outside step()"),
            _ => None,
        }
    }

    pub fn artifact_id(&self) -> Option<&ArtifactId> {
        match self {
            WorkloadState::Active {
                pod: PodSlot {
                    pod_state: PodState::Suspending { artifact_id, .. }
                        | PodState::Resuming { artifact_id, .. },
                    ..
                },
                ..
            }
            | WorkloadState::Suspended { artifact_id, .. } => Some(artifact_id),
            WorkloadState::Transitioning => panic!("WorkloadState::Transitioning leaked outside step()"),
            _ => None,
        }
    }

    pub fn is_running(&self) -> bool {
        matches!(
            self,
            WorkloadState::Active {
                pod: PodSlot { pod_state: PodState::Running, .. },
                ..
            }
        )
    }

    pub fn suspended_artifact_id(&self) -> Option<&ArtifactId> {
        match self {
            WorkloadState::Suspended { artifact_id } => Some(artifact_id),
            _ => None,
        }
    }

    pub fn active_pod(&self) -> Option<&PodSlot> {
        match self {
            WorkloadState::Active { pod, .. } => Some(pod),
            _ => None,
        }
    }
}

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
    /// Current demand level, set authoritatively by the namespace reconciliation layer.
    pub current_demand: u32,
    /// Whether to suspend the pod instead of stopping it when demand drops to zero.
    pub suspend_on_idle: bool,
    /// Whether this workload has activation configured. If false, the workload is
    /// always-on and will self-activate on Initialize, ignoring SetDemand(0).
    pub has_activation: bool,
    /// Reason for the most recent pod failure, for observability.
    pub last_failure_reason: Option<PodGoneReason>,
    /// Number of consecutive pod failures without a successful PodRunning in between.
    pub consecutive_failures: u32,
    /// Maximum number of retries before entering terminal Failed state.
    /// Defaults to MAX_RETRIES (5). Can be lowered for model checking.
    pub max_retries: u32,
    /// Once demand appears (0→non-zero) or the workload loses its pod (WorkerLost,
    /// PodGone), the workload is committed to reaching Running before it can go
    /// Dormant via SetDemand(0). Cleared on successful PodRunning→Running or on
    /// entering Failed (retries exhausted). This prevents demand fluctuations from
    /// aborting an in-progress boot/retry sequence.
    pub needs_successful_boot: bool,
    /// Active conditions for observability (key → message).
    pub conditions: std::collections::BTreeMap<String, String>,
    /// Pods that have been told to stop but haven't confirmed gone yet.
    /// Populated when `step()` emits `StopPod`; cleaned up when `PodGone` arrives for the pod.
    pub retiring: Vec<RetiredPod>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WorkloadInput {
    /// Initialize the workload. For always-on workloads (!has_activation),
    /// this transitions to WaitingForCapacity and emits PodRequest.
    /// For activation-based workloads, this is a no-op (stays Dormant).
    Initialize,
    SetDemand { count: u32 },
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
    SuspendRequest { pod_id: PodId, worker_id: WorkerId, artifact_id: ArtifactId },
    /// Workload needs a pod resumed from snapshot. The namespace layer
    /// looks up the placement table and generates a new pod_id.
    ResumeRequest { artifact_id: ArtifactId },
    /// SM accepted a LaunchPod input. The namespace layer registers the pod,
    /// builds the WorkerCommand, and emits endpoint/event updates.
    LaunchRequest { worker_id: WorkerId, pod_id: PodId },
    /// SM accepted a ResumePod input. The namespace layer registers the pod,
    /// builds the WorkerCommand from placement table, and emits endpoint/event updates.
    ResumeFromArtifact { worker_id: WorkerId, pod_id: PodId, artifact_id: ArtifactId },
    /// Artifact should be deleted. The namespace layer resolves placement
    /// and emits the actual WorkerCommand::DeleteArtifact.
    DeleteArtifact { artifact_id: ArtifactId },
    WorkerCommand(WorkerId, WorkerCommand),
    TimerSet(TimerKey, std::time::Duration),
    TimerCancel(TimerKey),
    ConditionSet { key: String, message: String },
    ConditionClear { key: String },
    /// Workload just entered Running state and is ready to serve traffic.
    BecameReady { pod_id: PodId, worker_id: WorkerId },
    /// Workload left Running state and is no longer ready.
    BecameUnready,
}

const MAX_RETRIES: u32 = 5;

fn backoff_delay(failures: u32) -> std::time::Duration {
    let secs = 1u64 << (failures - 1).min(5);
    std::time::Duration::from_secs(secs)
}

impl WorkloadStateMachine {
    pub fn new(workload_id: WorkloadId, suspend_on_idle: bool, has_activation: bool) -> Self {
        WorkloadStateMachine {
            workload_id,
            state: WorkloadState::Dormant,
            current_demand: 0,
            suspend_on_idle,
            has_activation,
            last_failure_reason: None,
            consecutive_failures: 0,
            max_retries: MAX_RETRIES,
            needs_successful_boot: false,
            conditions: std::collections::BTreeMap::new(),
            retiring: Vec::new(),
        }
    }

    /// Helper: transition to dormant or waiting-for-capacity based on demand,
    /// with exponential backoff on consecutive failures.
    fn transition_on_demand(&mut self, outputs: &mut Vec<WorkloadOutput>) {
        if self.current_demand > 0 || self.needs_successful_boot {
            if self.consecutive_failures >= self.max_retries {
                self.state = WorkloadState::Failed;
                self.needs_successful_boot = false;
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
                // Fall back to current_demand check (existing behavior).
                self.transition_on_demand(outputs);
            }
        }
    }

    /// Sets `pending = max(pending, intent)` on the current transition state.
    fn upgrade_pending(&mut self, intent: PendingIntent) -> bool {
        match &mut self.state {
            WorkloadState::Active {
                pod: PodSlot {
                    pod_state: PodState::Launching { .. }
                        | PodState::Suspending { .. }
                        | PodState::Resuming { .. },
                    ..
                },
                pending,
            } => {
                *pending = (*pending).max(intent);
                true
            }
            _ => false,
        }
    }

    /// Stop a pod and track it in the retiring list until confirmed gone.
    fn retire_pod(
        &mut self,
        pod_id: PodId,
        worker_id: WorkerId,
        namespace_id: &NamespaceId,
        graceful: bool,
        outputs: &mut Vec<WorkloadOutput>,
    ) {
        self.retiring.push(RetiredPod {
            pod_id: pod_id.clone(),
            worker_id: worker_id.clone(),
        });
        outputs.push(WorkloadOutput::WorkerCommand(
            worker_id,
            WorkerCommand::StopPod {
                namespace_id: namespace_id.clone(),
                pod_id,
                graceful,
            },
        ));
    }

    /// Check whether a pod_id belongs to a retiring pod.
    pub fn is_retiring(&self, pod_id: &PodId) -> bool {
        self.retiring.iter().any(|r| r.pod_id == *pod_id)
    }

    /// Convert pod SM outputs to workload outputs.
    fn collect_pod_outputs(pod_outputs: Vec<PodOutput>, outputs: &mut Vec<WorkloadOutput>) {
        for po in pod_outputs {
            match po {
                PodOutput::TimerSet(k, d) => outputs.push(WorkloadOutput::TimerSet(k, d)),
                PodOutput::TimerCancel(k) => outputs.push(WorkloadOutput::TimerCancel(k)),
                PodOutput::DeleteArtifact { artifact_id } => {
                    outputs.push(WorkloadOutput::DeleteArtifact { artifact_id })
                }
                PodOutput::SuspendRequest {
                    pod_id,
                    worker_id,
                    artifact_id,
                } => outputs.push(WorkloadOutput::SuspendRequest {
                    pod_id,
                    worker_id,
                    artifact_id,
                }),
            }
        }
    }

    pub fn is_preemptable(&self) -> bool {
        !matches!(self.state, WorkloadState::Dormant | WorkloadState::WaitingForCapacity | WorkloadState::Failed)
    }

    pub fn step(&mut self, input: WorkloadInput, namespace_id: &NamespaceId) -> Vec<WorkloadOutput> {
        let mut outputs = Vec::new();

        match input {
            WorkloadInput::Initialize => {
                if !self.has_activation && matches!(self.state, WorkloadState::Dormant) {
                    // Always-on workload: self-activate on initialize.
                    self.needs_successful_boot = true;
                    self.transition_on_demand(&mut outputs);
                }
                // Activation-based workloads stay Dormant until demand arrives.
            }
            WorkloadInput::SetDemand { count } => {
                let old = self.current_demand;
                self.current_demand = count;

                if count > 0 && old == 0 {
                    // Demand appeared: wake workload. Commit to reaching Running.
                    self.needs_successful_boot = true;
                    match &self.state {
                        WorkloadState::Dormant => {
                            self.state = WorkloadState::WaitingForCapacity;
                            outputs.push(WorkloadOutput::PodRequest);
                        }
                        WorkloadState::Suspended { artifact_id } => {
                            // Resume from snapshot instead of cold boot.
                            outputs.push(WorkloadOutput::ResumeRequest {
                                artifact_id: artifact_id.clone(),
                            });
                        }
                        WorkloadState::Active {
                            pod: PodSlot {
                                pod_state: PodState::Launching { .. }
                                    | PodState::Suspending { .. }
                                    | PodState::Resuming { .. },
                                ..
                            },
                            ..
                        } => {
                            self.upgrade_pending(PendingIntent::Demand);
                        }
                        _ => {}
                    }
                } else if count == 0 && old > 0 {
                    if !self.has_activation {
                        // Always-on workload: update counter for observability but
                        // don't transition state. ForceDeactivate is the only way to shut down.
                        // Clear any Demand pending since we won't act on it.
                        if let WorkloadState::Active { ref mut pending, .. } = self.state {
                            if *pending == PendingIntent::Demand {
                                *pending = PendingIntent::None;
                            }
                        }
                        return outputs;
                    }
                    // Demand dropped to zero: shut down (unless committed to booting).
                    match std::mem::replace(&mut self.state, WorkloadState::Transitioning) {
                        WorkloadState::WaitingForCapacity if self.needs_successful_boot => {
                            // Committed to booting — stay in WaitingForCapacity.
                            self.state = WorkloadState::WaitingForCapacity;
                        }
                        WorkloadState::WaitingForCapacity => {
                            self.state = WorkloadState::Dormant;
                        }
                        mut state @ WorkloadState::Active {
                            pod: PodSlot { pod_state: PodState::Launching { .. }, .. },
                            ..
                        } if self.needs_successful_boot => {
                            // Committed to booting — let launch complete.
                            // Clear Demand pending since demand is now 0.
                            if let WorkloadState::Active { ref mut pending, .. } = state {
                                if *pending == PendingIntent::Demand {
                                    *pending = PendingIntent::None;
                                }
                            }
                            self.state = state;
                        }
                        WorkloadState::Active {
                            mut pod,
                            ..
                        } if matches!(pod.pod_state, PodState::Launching { .. }) => {
                            // Cancel launch timer via pod SM, then retire.
                            let (_, pod_outputs) = pod.step(PodInput::PodGone { worker_lost: false });
                            Self::collect_pod_outputs(pod_outputs, &mut outputs);
                            self.retire_pod(pod.pod_id, pod.worker_id, namespace_id, false, &mut outputs);
                            self.state = WorkloadState::Dormant;
                        }
                        WorkloadState::Active {
                            mut pod,
                            ..
                        } if matches!(pod.pod_state, PodState::Running) => {
                            outputs.push(WorkloadOutput::BecameUnready);
                            if self.suspend_on_idle {
                                let artifact_id = ArtifactId::from(format!(
                                    "{}-{}-{}",
                                    namespace_id.0, self.workload_id.0, pod.pod_id.0
                                ));
                                let pod_outs = pod.initiate_suspend(
                                    &self.workload_id,
                                    artifact_id,
                                );
                                Self::collect_pod_outputs(pod_outs, &mut outputs);
                                self.state = WorkloadState::Active {
                                    pod,
                                    pending: PendingIntent::None,
                                };
                            } else {
                                self.retire_pod(pod.pod_id, pod.worker_id, namespace_id, true, &mut outputs);
                                self.state = WorkloadState::Dormant;
                            }
                        }
                        WorkloadState::Dormant => {
                            self.state = WorkloadState::Dormant;
                        }
                        state @ WorkloadState::RetryBackoff { .. } if self.needs_successful_boot => {
                            // Committed to booting — keep retrying.
                            self.state = state;
                        }
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
                        // Remaining Active states (Suspending/Resuming) and Suspended
                        other @ (WorkloadState::Active { .. } | WorkloadState::Suspended { .. }) => {
                            self.state = other;
                            // Clear Demand pending if demand dropped to 0
                            if let WorkloadState::Active { ref mut pending, .. } = self.state {
                                if *pending == PendingIntent::Demand {
                                    *pending = PendingIntent::None;
                                }
                            }
                        }
                        WorkloadState::Transitioning => unreachable!("Transitioning in SetDemand"),
                    }
                }
                // Other transitions (count changed but still >0 or still 0): no-op.
            }
            WorkloadInput::LaunchPod { worker_id, pod_id } => {
                if !matches!(self.state, WorkloadState::WaitingForCapacity) {
                    return outputs;
                }
                let (pod, pod_outputs) =
                    PodSlot::new_launching(pod_id.clone(), worker_id.clone(), &self.workload_id);
                Self::collect_pod_outputs(pod_outputs, &mut outputs);
                outputs.push(WorkloadOutput::LaunchRequest {
                    worker_id,
                    pod_id,
                });
                self.state = WorkloadState::Active {
                    pod,
                    pending: PendingIntent::None,
                };
            }
            WorkloadInput::ResumePod { worker_id, pod_id, artifact_id } => {
                if !matches!(self.state, WorkloadState::Suspended { .. }) {
                    return outputs;
                }
                let (pod, pod_outputs) =
                    PodSlot::new_resuming(pod_id.clone(), worker_id.clone(), &self.workload_id, artifact_id.clone());
                Self::collect_pod_outputs(pod_outputs, &mut outputs);
                outputs.push(WorkloadOutput::ResumeFromArtifact {
                    worker_id,
                    pod_id,
                    artifact_id,
                });
                self.state = WorkloadState::Active {
                    pod,
                    pending: PendingIntent::None,
                };
            }
            WorkloadInput::PodRunning { pod_id } => {
                if self.is_retiring(&pod_id) {
                    return outputs;
                }

                let pod_matches = matches!(
                    &self.state,
                    WorkloadState::Active { pod, .. } if pod.pod_id == pod_id
                );
                if !pod_matches {
                    return outputs;
                }

                let was_resuming = matches!(
                    &self.state,
                    WorkloadState::Active {
                        pod: PodSlot { pod_state: PodState::Resuming { .. }, .. },
                        ..
                    }
                );

                if let WorkloadState::Active { mut pod, pending } =
                    std::mem::replace(&mut self.state, WorkloadState::Transitioning)
                {
                    let (outcome, pod_outputs) = pod.step(PodInput::PodRunning);
                    Self::collect_pod_outputs(pod_outputs, &mut outputs);

                    match outcome {
                        PodOutcome::Running => {
                            self.consecutive_failures = 0;
                            self.last_failure_reason = None;
                            self.needs_successful_boot = false;
                            outputs.push(WorkloadOutput::ConditionClear { key: "retry-backoff".into() });

                            match pending {
                                PendingIntent::Deactivate => {
                                    // Pod is immediately retired — never actually "became ready".
                                    self.retire_pod(pod.pod_id, pod.worker_id, namespace_id, false, &mut outputs);
                                    self.state = WorkloadState::Dormant;
                                }
                                PendingIntent::Restart => {
                                    // Pod is immediately retired — never actually "became ready".
                                    self.retire_pod(pod.pod_id, pod.worker_id, namespace_id, false, &mut outputs);
                                    self.transition_on_demand(&mut outputs);
                                }
                                PendingIntent::Demand => {
                                    outputs.push(WorkloadOutput::BecameReady {
                                        pod_id: pod.pod_id.clone(),
                                        worker_id: pod.worker_id.clone(),
                                    });
                                    self.state = WorkloadState::Active {
                                        pod,
                                        pending: PendingIntent::None,
                                    };
                                }
                                PendingIntent::None => {
                                    if !was_resuming || self.current_demand > 0 {
                                        outputs.push(WorkloadOutput::BecameReady {
                                            pod_id: pod.pod_id.clone(),
                                            worker_id: pod.worker_id.clone(),
                                        });
                                        self.state = WorkloadState::Active {
                                            pod,
                                            pending: PendingIntent::None,
                                        };
                                    } else if self.suspend_on_idle {
                                        // Resumed with no demand — immediately suspend again, never "became ready".
                                        let artifact_id = ArtifactId::from(format!(
                                            "{}-{}-{}",
                                            namespace_id.0, self.workload_id.0, pod.pod_id.0
                                        ));
                                        let pod_outs = pod.initiate_suspend(
                                            &self.workload_id,
                                            artifact_id,
                                        );
                                        Self::collect_pod_outputs(pod_outs, &mut outputs);
                                        self.state = WorkloadState::Active {
                                            pod,
                                            pending: PendingIntent::None,
                                        };
                                    } else {
                                        // Resumed with no demand, no suspend — retire immediately.
                                        self.retire_pod(pod.pod_id, pod.worker_id, namespace_id, true, &mut outputs);
                                        self.state = WorkloadState::Dormant;
                                    }
                                }
                            }
                        }
                        _ => {
                            self.state = WorkloadState::Active { pod, pending };
                        }
                    }
                }
            }
            WorkloadInput::PodSuspended { pod_id, artifact_id } => {
                let pod_matches = matches!(
                    &self.state,
                    WorkloadState::Active { pod, .. } if pod.pod_id == pod_id
                );
                if !pod_matches {
                    return outputs;
                }

                if let WorkloadState::Active { mut pod, pending } =
                    std::mem::replace(&mut self.state, WorkloadState::Transitioning)
                {
                    let (outcome, pod_outputs) = pod.step(PodInput::PodSuspended { artifact_id });
                    Self::collect_pod_outputs(pod_outputs, &mut outputs);

                    match outcome {
                        PodOutcome::Suspended { artifact_id } => {
                            match pending {
                                PendingIntent::Deactivate => {
                                    self.state = WorkloadState::Suspended { artifact_id };
                                }
                                PendingIntent::Restart => {
                                    outputs.push(WorkloadOutput::DeleteArtifact { artifact_id });
                                    self.transition_on_demand(&mut outputs);
                                }
                                PendingIntent::Demand => {
                                    self.state = WorkloadState::Suspended { artifact_id: artifact_id.clone() };
                                    outputs.push(WorkloadOutput::ResumeRequest { artifact_id });
                                }
                                PendingIntent::None => {
                                    if self.current_demand > 0 {
                                        self.state = WorkloadState::Suspended { artifact_id: artifact_id.clone() };
                                        outputs.push(WorkloadOutput::ResumeRequest { artifact_id });
                                    } else {
                                        self.state = WorkloadState::Suspended { artifact_id };
                                    }
                                }
                            }
                        }
                        PodOutcome::Noop => {
                            self.state = WorkloadState::Active { pod, pending };
                        }
                        _ => unreachable!("PodSuspended can only produce Suspended or Noop"),
                    }
                }
            }
            WorkloadInput::PodSuspendFailed { pod_id } => {
                if self.retiring.iter().any(|r| r.pod_id == pod_id) {
                    self.retiring.retain(|r| r.pod_id != pod_id);
                    return outputs;
                }

                let pod_matches = matches!(
                    &self.state,
                    WorkloadState::Active { pod, .. } if pod.pod_id == pod_id
                );
                if !pod_matches {
                    return outputs;
                }

                if let WorkloadState::Active { mut pod, pending } =
                    std::mem::replace(&mut self.state, WorkloadState::Transitioning)
                {
                    let (outcome, pod_outputs) = pod.step(PodInput::PodSuspendFailed);
                    Self::collect_pod_outputs(pod_outputs, &mut outputs);

                    match outcome {
                        PodOutcome::SuspendFailed => {
                            self.transition_on_intent(pending, &mut outputs);
                        }
                        PodOutcome::Noop => {
                            self.state = WorkloadState::Active { pod, pending };
                        }
                        _ => unreachable!("PodSuspendFailed can only produce SuspendFailed or Noop"),
                    }
                }
            }
            WorkloadInput::PodGone { pod_id, reason } => {
                if self.retiring.iter().any(|r| r.pod_id == pod_id) {
                    self.retiring.retain(|r| r.pod_id != pod_id);
                    return outputs;
                }

                let pod_matches = matches!(
                    &self.state,
                    WorkloadState::Active { pod, .. } if pod.pod_id == pod_id
                );
                if !pod_matches {
                    return outputs;
                }

                self.last_failure_reason = reason.clone();
                let is_failure = match &reason {
                    Some(PodGoneReason::Exited { exit_code }) => *exit_code != 0,
                    Some(_) => true,
                    None => true,
                };
                let in_suspending = matches!(
                    &self.state,
                    WorkloadState::Active {
                        pod: PodSlot { pod_state: PodState::Suspending { .. }, .. },
                        ..
                    }
                );
                let was_running = matches!(
                    &self.state,
                    WorkloadState::Active {
                        pod: PodSlot { pod_state: PodState::Running, .. },
                        ..
                    }
                );
                if is_failure && !in_suspending {
                    self.consecutive_failures += 1;
                }
                if !in_suspending {
                    self.needs_successful_boot = true;
                }

                if let WorkloadState::Active { mut pod, pending } =
                    std::mem::replace(&mut self.state, WorkloadState::Transitioning)
                {
                    let (outcome, pod_outputs) = pod.step(PodInput::PodGone { worker_lost: false });
                    Self::collect_pod_outputs(pod_outputs, &mut outputs);

                    match outcome {
                        PodOutcome::Gone => {
                            if was_running {
                                outputs.push(WorkloadOutput::BecameUnready);
                                self.transition_on_demand(&mut outputs);
                            } else {
                                self.transition_on_intent(pending, &mut outputs);
                            }
                        }
                        _ => {
                            self.state = WorkloadState::Active { pod, pending };
                        }
                    }
                }
            }
            WorkloadInput::WorkerLost { worker_id } => {
                self.retiring.retain(|r| r.worker_id != worker_id);

                let active_on_worker = matches!(
                    &self.state,
                    WorkloadState::Active { pod, .. } if pod.worker_id == worker_id
                );
                let was_running = matches!(
                    &self.state,
                    WorkloadState::Active {
                        pod: PodSlot { pod_state: PodState::Running, .. },
                        ..
                    }
                );

                if active_on_worker {
                    // Commit to reaching Running for active pods (not Suspending).
                    let in_suspending = matches!(
                        &self.state,
                        WorkloadState::Active {
                            pod: PodSlot { pod_state: PodState::Suspending { .. }, .. },
                            ..
                        }
                    );
                    if !in_suspending {
                        self.needs_successful_boot = true;
                    }

                    if let WorkloadState::Active { mut pod, pending } =
                        std::mem::replace(&mut self.state, WorkloadState::Transitioning)
                    {
                        let (outcome, pod_outputs) = pod.step(PodInput::PodGone { worker_lost: true });
                        Self::collect_pod_outputs(pod_outputs, &mut outputs);

                        match outcome {
                            PodOutcome::Gone => {
                                if was_running {
                                    outputs.push(WorkloadOutput::BecameUnready);
                                    self.transition_on_demand(&mut outputs);
                                } else {
                                    self.transition_on_intent(pending, &mut outputs);
                                }
                            }
                            _ => {
                                self.state = WorkloadState::Active { pod, pending };
                            }
                        }
                    }
                } else if matches!(self.state, WorkloadState::Suspended { .. }) {
                    // Artifact is gone with the worker (placement table cleanup
                    // handled by namespace layer). Fall back to cold boot.
                    self.transition_on_demand(&mut outputs);
                }
            }
            WorkloadInput::TimerFired { timer_key } => {
                // Non-pod timer: RetryBackoff.
                if let WorkloadState::RetryBackoff { backoff_timer } = &self.state {
                    if *backoff_timer == timer_key {
                        outputs.push(WorkloadOutput::ConditionClear { key: "retry-backoff".into() });
                        self.state = WorkloadState::WaitingForCapacity;
                        outputs.push(WorkloadOutput::PodRequest);
                        return outputs;
                    }
                }

                // Pod-level timer: delegate to pod SM.
                if !matches!(&self.state, WorkloadState::Active { .. }) {
                    return outputs;
                }

                if let WorkloadState::Active { mut pod, pending } =
                    std::mem::replace(&mut self.state, WorkloadState::Transitioning)
                {
                    let (outcome, pod_outputs) = pod.step(PodInput::TimerFired { timer_key });
                    Self::collect_pod_outputs(pod_outputs, &mut outputs);

                    match outcome {
                        PodOutcome::TimedOut => {
                            self.retire_pod(pod.pod_id, pod.worker_id, namespace_id, false, &mut outputs);
                            self.transition_on_intent(pending, &mut outputs);
                        }
                        PodOutcome::Noop => {
                            // Stale timer.
                            self.state = WorkloadState::Active { pod, pending };
                        }
                        _ => unreachable!("TimerFired can only produce TimedOut or Noop"),
                    }
                }
            }
            WorkloadInput::ForceDeactivate => {
                self.needs_successful_boot = false;
                self.consecutive_failures = 0;
                match &self.state {
                    WorkloadState::Dormant | WorkloadState::WaitingForCapacity => {
                        // Already inactive, no-op.
                    }
                    WorkloadState::Active {
                        pod: PodSlot { pod_state: PodState::Running, .. },
                        ..
                    } => {
                        if let WorkloadState::Active { mut pod, .. } =
                            std::mem::replace(&mut self.state, WorkloadState::Transitioning)
                        {
                            outputs.push(WorkloadOutput::BecameUnready);
                            if self.suspend_on_idle {
                                let artifact_id = ArtifactId::from(format!(
                                    "{}-{}-{}",
                                    namespace_id.0, self.workload_id.0, pod.pod_id.0
                                ));
                                let pod_outs = pod.initiate_suspend(
                                    &self.workload_id,
                                    artifact_id,
                                );
                                Self::collect_pod_outputs(pod_outs, &mut outputs);
                                self.state = WorkloadState::Active {
                                    pod,
                                    pending: PendingIntent::Deactivate,
                                };
                            } else {
                                self.retire_pod(pod.pod_id, pod.worker_id, namespace_id, true, &mut outputs);
                                self.state = WorkloadState::Dormant;
                            }
                        }
                    }
                    WorkloadState::Active {
                        pod: PodSlot {
                            pod_state: PodState::Launching { .. }
                                | PodState::Suspending { .. }
                                | PodState::Resuming { .. },
                            ..
                        },
                        ..
                    } => {
                        self.upgrade_pending(PendingIntent::Deactivate);
                    }
                    WorkloadState::Suspended { .. } => {
                        if let WorkloadState::Suspended { artifact_id } =
                            std::mem::replace(&mut self.state, WorkloadState::Transitioning)
                        {
                            outputs.push(WorkloadOutput::DeleteArtifact { artifact_id });
                            self.state = WorkloadState::Dormant;
                        }
                    }
                    WorkloadState::RetryBackoff { .. } => {
                        if let WorkloadState::RetryBackoff { backoff_timer } =
                            std::mem::replace(&mut self.state, WorkloadState::Transitioning)
                        {
                            outputs.push(WorkloadOutput::TimerCancel(backoff_timer));
                            outputs.push(WorkloadOutput::ConditionClear { key: "retry-backoff".into() });
                            self.state = WorkloadState::Dormant;
                        }
                    }
                    WorkloadState::Failed => {
                        outputs.push(WorkloadOutput::ConditionClear { key: "failed".into() });
                        self.state = WorkloadState::Dormant;
                    }
                    WorkloadState::Transitioning => unreachable!("Transitioning in ForceDeactivate"),
                }
            }
            WorkloadInput::SpecChanged => {
                self.needs_successful_boot = false;
                match &self.state {
                    WorkloadState::Dormant | WorkloadState::WaitingForCapacity => {
                        // No-op: will launch with new spec next time.
                    }
                    WorkloadState::Active {
                        pod: PodSlot {
                            pod_state: PodState::Launching { .. }
                                | PodState::Suspending { .. }
                                | PodState::Resuming { .. },
                            ..
                        },
                        ..
                    } => {
                        self.upgrade_pending(PendingIntent::Restart);
                    }
                    WorkloadState::Active {
                        pod: PodSlot { pod_state: PodState::Running, .. },
                        ..
                    } => {
                        if let WorkloadState::Active { pod, .. } =
                            std::mem::replace(&mut self.state, WorkloadState::Transitioning)
                        {
                            outputs.push(WorkloadOutput::BecameUnready);
                            self.retire_pod(pod.pod_id, pod.worker_id, namespace_id, false, &mut outputs);
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
                    WorkloadState::Active {
                        pod: PodSlot { pod_state: PodState::Running, .. },
                        ..
                    } => {
                        if let WorkloadState::Active { pod, .. } =
                            std::mem::replace(&mut self.state, WorkloadState::Transitioning)
                        {
                            outputs.push(WorkloadOutput::BecameUnready);
                            self.retire_pod(pod.pod_id, pod.worker_id, namespace_id, false, &mut outputs);

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
