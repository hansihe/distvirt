use std::collections::HashMap;

use crate::sm_new::{
    DRouter, PodId, PodScheduleRequest, ScheduleRequestId, ScheduleRequestPortInput,
};

#[cfg(test)]
mod tests;

/// Delta returned by reconcile.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ScheduleRequestDelta {
    Request {
        pod_id: PodId,
        request: PodScheduleRequest,
    },
    Drop {
        pod_id: PodId,
    },
}

pub(crate) struct ScheduleRequestAdapter {
    schedule_request_id: ScheduleRequestId,
    /// What we've told the scheduler: pod_id → request
    sent_requests: HashMap<PodId, PodScheduleRequest>,
}

impl ScheduleRequestAdapter {
    pub(crate) fn new(schedule_request_id: ScheduleRequestId) -> Self {
        ScheduleRequestAdapter {
            schedule_request_id,
            sent_requests: HashMap::new(),
        }
    }

    /// Drain schedule request inputs from the router, diff against sent state,
    /// and return Request/Drop deltas. Updates internal cache.
    pub(crate) fn reconcile(&mut self, router: &mut DRouter) -> Vec<ScheduleRequestDelta> {
        let inputs = router.drain_schedule_request_inputs();

        for (sr_id, input) in inputs {
            if sr_id != self.schedule_request_id {
                continue;
            }
            match input {
                ScheduleRequestPortInput::PodRequestsInput(requests) => {
                    let new_requests: HashMap<PodId, PodScheduleRequest> =
                        requests.into_iter().collect();

                    let mut deltas = Vec::new();

                    // Pods in new but not sent → Request
                    for (pod_id, request) in &new_requests {
                        if !self.sent_requests.contains_key(pod_id) {
                            deltas.push(ScheduleRequestDelta::Request {
                                pod_id: *pod_id,
                                request: request.clone(),
                            });
                        }
                    }

                    // Pods in sent but not new → Drop
                    for pod_id in self.sent_requests.keys() {
                        if !new_requests.contains_key(pod_id) {
                            deltas.push(ScheduleRequestDelta::Drop { pod_id: *pod_id });
                        }
                    }

                    self.sent_requests = new_requests;
                    return deltas;
                }
            }
        }

        Vec::new()
    }

    /// Read-only access to sent requests.
    pub(crate) fn sent_requests(&self) -> &HashMap<PodId, PodScheduleRequest> {
        &self.sent_requests
    }
}
