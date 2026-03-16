use std::collections::HashMap;

use crate::sm_new::{
    ArtifactId, DRouter, PodId, PodScheduleRequest, WorkerId, WorkerPortInput,
};

#[cfg(test)]
mod tests;

/// Action returned by reconcile — caller (shell) executes these.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum PodAssignmentAction {
    Launch {
        worker_id: WorkerId,
        pod_id: PodId,
        request: PodScheduleRequest,
    },
    Resume {
        worker_id: WorkerId,
        pod_id: PodId,
        artifact_id: ArtifactId,
    },
    Stop {
        worker_id: WorkerId,
        pod_id: PodId,
    },
    Suspend {
        worker_id: WorkerId,
        pod_id: PodId,
    },
}

pub(crate) struct PodAssignmentAdapter {
    /// Cached: worker_id → { pod_id → request }
    assigned: HashMap<WorkerId, HashMap<PodId, PodScheduleRequest>>,
}

impl PodAssignmentAdapter {
    pub(crate) fn new() -> Self {
        PodAssignmentAdapter {
            assigned: HashMap::new(),
        }
    }

    /// Drain worker inputs from the router, diff against cached state,
    /// and return Launch/Resume/Stop actions. Updates internal cache.
    pub(crate) fn reconcile(&mut self, router: &mut DRouter) -> Vec<PodAssignmentAction> {
        let inputs = router.drain_worker_inputs();

        let mut actions = Vec::new();

        for (worker_id, input) in inputs {
            match input {
                WorkerPortInput::AssignedPodsInput(pods) => {
                    let new_pods: HashMap<PodId, PodScheduleRequest> =
                        pods.into_iter().collect();

                    let cached = self.assigned.entry(worker_id).or_default();

                    // Pods in new but not cached → Launch or Resume
                    for (pod_id, request) in &new_pods {
                        if !cached.contains_key(pod_id) {
                            if let Some(artifact_id) = request.resume_artifact {
                                actions.push(PodAssignmentAction::Resume {
                                    worker_id,
                                    pod_id: *pod_id,
                                    artifact_id,
                                });
                            } else {
                                actions.push(PodAssignmentAction::Launch {
                                    worker_id,
                                    pod_id: *pod_id,
                                    request: request.clone(),
                                });
                            }
                        } else if request.suspend && !cached[pod_id].suspend {
                            // Pod transitioned to suspend state → Suspend
                            actions.push(PodAssignmentAction::Suspend {
                                worker_id,
                                pod_id: *pod_id,
                            });
                        }
                    }

                    // Pods in cached but not new → Stop
                    for pod_id in cached.keys() {
                        if !new_pods.contains_key(pod_id) {
                            actions.push(PodAssignmentAction::Stop {
                                worker_id,
                                pod_id: *pod_id,
                            });
                        }
                    }

                    *cached = new_pods;
                }
            }
        }

        actions
    }

    /// Remove cached state for a disconnected worker.
    /// Prevents stale entries from accumulating when no new deliveries arrive
    /// for a destroyed worker port.
    pub(crate) fn remove_worker(&mut self, worker_id: &WorkerId) {
        self.assigned.remove(worker_id);
    }

    /// Read-only access to assigned state.
    pub(crate) fn assigned(&self) -> &HashMap<WorkerId, HashMap<PodId, PodScheduleRequest>> {
        &self.assigned
    }
}
