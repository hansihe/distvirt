use distvirt_sm_router::IncrementalAggregator;

use crate::sm::{ArtifactId, DRouter, PodId, PodScheduleRequest, WorkerPortInput};

#[cfg(test)]
mod tests;

/// Action returned by reconcile — caller (shell) executes these.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum PodAssignmentAction {
    Launch {
        worker_id: crate::sm::WorkerId,
        pod_id: crate::sm::PodId,
        request: crate::sm::PodScheduleRequest,
    },
    Resume {
        worker_id: crate::sm::WorkerId,
        pod_id: crate::sm::PodId,
        artifact_id: crate::sm::ArtifactId,
    },
    Stop {
        worker_id: crate::sm::WorkerId,
        pod_id: crate::sm::PodId,
    },
    Suspend {
        worker_id: crate::sm::WorkerId,
        pod_id: crate::sm::PodId,
    },
}

/// Delta produced by the incremental pod-assignment aggregator.
#[derive(Clone, Debug, PartialEq)]
pub enum PodAssignmentDelta {
    Launch {
        pod_id: PodId,
        request: PodScheduleRequest,
    },
    Resume {
        pod_id: PodId,
        artifact_id: ArtifactId,
    },
    Stop {
        pod_id: PodId,
    },
    Suspend {
        pod_id: PodId,
    },
}

pub(crate) struct PodAssignmentAdapter;

impl PodAssignmentAdapter {
    pub(crate) fn new() -> Self {
        PodAssignmentAdapter
    }

    /// Drain worker inputs from the router.
    /// With incremental aggregation the router already produces per-pod deltas,
    /// so no adapter-side diffing or caching is needed.
    pub(crate) fn reconcile(&mut self, router: &mut DRouter) -> Vec<PodAssignmentAction> {
        let inputs = router.drain_worker_inputs();

        inputs
            .into_iter()
            .map(|(worker_id, input)| match input {
                WorkerPortInput::AssignedPodsInput(delta) => match delta {
                    PodAssignmentDelta::Launch { pod_id, request } => PodAssignmentAction::Launch {
                        worker_id,
                        pod_id,
                        request,
                    },
                    PodAssignmentDelta::Resume {
                        pod_id,
                        artifact_id,
                    } => PodAssignmentAction::Resume {
                        worker_id,
                        pod_id,
                        artifact_id,
                    },
                    PodAssignmentDelta::Stop { pod_id } => {
                        PodAssignmentAction::Stop { worker_id, pod_id }
                    }
                    PodAssignmentDelta::Suspend { pod_id } => {
                        PodAssignmentAction::Suspend { worker_id, pod_id }
                    }
                },
            })
            .collect()
    }
}

/// Incremental aggregator for worker pod-assignment inputs.
/// Produces `PodAssignmentDelta` directly — no adapter-side diffing needed.
#[derive(Default)]
pub struct PodAssignmentIncrementalAggregator;

impl IncrementalAggregator for PodAssignmentIncrementalAggregator {
    type Input = (PodId, PodScheduleRequest);
    type Output = PodAssignmentDelta;

    fn added(&self, (pod_id, request): &(PodId, PodScheduleRequest)) -> Option<PodAssignmentDelta> {
        if let Some(artifact_id) = request.resume_artifact {
            Some(PodAssignmentDelta::Resume {
                pod_id: *pod_id,
                artifact_id,
            })
        } else {
            Some(PodAssignmentDelta::Launch {
                pod_id: *pod_id,
                request: request.clone(),
            })
        }
    }

    fn removed(&self, (pod_id, _): &(PodId, PodScheduleRequest)) -> Option<PodAssignmentDelta> {
        Some(PodAssignmentDelta::Stop { pod_id: *pod_id })
    }

    fn changed(
        &self,
        (_, old_req): &(PodId, PodScheduleRequest),
        (pod_id, new_req): &(PodId, PodScheduleRequest),
    ) -> Option<PodAssignmentDelta> {
        if new_req.suspend && !old_req.suspend {
            Some(PodAssignmentDelta::Suspend { pod_id: *pod_id })
        } else {
            None
        }
    }
}
