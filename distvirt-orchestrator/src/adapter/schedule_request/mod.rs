use distvirt_sm_router::IncrementalAggregator;

use crate::sm::{DRouter, PodId, PodScheduleRequest, ScheduleRequestId, ScheduleRequestPortInput};

#[cfg(test)]
mod tests;

pub(crate) use crate::sm::ScheduleRequestDelta;

pub(crate) struct ScheduleRequestAdapter {
    schedule_request_id: ScheduleRequestId,
}

impl ScheduleRequestAdapter {
    pub(crate) fn new(schedule_request_id: ScheduleRequestId) -> Self {
        ScheduleRequestAdapter {
            schedule_request_id,
        }
    }

    /// Drain schedule request inputs from the router.
    /// With incremental aggregation the router already produces per-pod deltas,
    /// so no adapter-side diffing or caching is needed.
    ///
    /// Returns `(deltas, mutated_router)`. Currently only drains, so
    /// `mutated_router` is always `false`.
    pub(crate) fn reconcile(&mut self, router: &mut DRouter) -> (Vec<ScheduleRequestDelta>, bool) {
        let inputs = router.drain_schedule_request_inputs();

        let deltas = inputs
            .into_iter()
            .filter(|(sr_id, _)| *sr_id == self.schedule_request_id)
            .map(|(_, input)| match input {
                ScheduleRequestPortInput::PodRequestsInput(delta) => delta,
            })
            .collect();
        (deltas, false)
    }
}

/// Incremental aggregator for schedule request inputs.
/// Produces `ScheduleRequestDelta` directly — no adapter-side diffing needed.
#[derive(Default)]
pub struct ScheduleRequestIncrementalAggregator;

impl IncrementalAggregator for ScheduleRequestIncrementalAggregator {
    type Input = (PodId, PodScheduleRequest);
    type Output = ScheduleRequestDelta;

    fn added(
        &self,
        (pod_id, request): &(PodId, PodScheduleRequest),
    ) -> Option<ScheduleRequestDelta> {
        Some(ScheduleRequestDelta::Request {
            pod_id: *pod_id,
            request: request.clone(),
        })
    }

    fn removed(&self, (pod_id, _): &(PodId, PodScheduleRequest)) -> Option<ScheduleRequestDelta> {
        Some(ScheduleRequestDelta::Drop { pod_id: *pod_id })
    }

    fn changed(
        &self,
        _old: &(PodId, PodScheduleRequest),
        _new: &(PodId, PodScheduleRequest),
    ) -> Option<ScheduleRequestDelta> {
        // Pod schedule requests are one-and-done; changes are not expected.
        None
    }
}
