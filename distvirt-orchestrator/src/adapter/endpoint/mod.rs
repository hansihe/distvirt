use distvirt_sm_router::IncrementalAggregator;

use crate::sm::{DRouter, EndpointId, EndpointPortInput, ReadyInfo, ServiceId};

#[cfg(test)]
mod tests;

/// Action returned by endpoint reconciliation.
#[derive(Clone, Debug, PartialEq)]
pub enum EndpointAction {
    Update {
        service_id: ServiceId,
        ready: ReadyInfo,
    },
    Remove {
        service_id: ServiceId,
    },
}

pub(crate) struct EndpointAdapter {
    endpoint_id: EndpointId,
}

impl EndpointAdapter {
    pub(crate) fn new(endpoint_id: EndpointId) -> Self {
        EndpointAdapter { endpoint_id }
    }

    /// Drain endpoint inputs from the router.
    /// With incremental aggregation the router already produces per-service deltas,
    /// so no adapter-side diffing or caching is needed.
    pub(crate) fn reconcile(&mut self, router: &mut DRouter) -> Vec<EndpointAction> {
        let inputs = router.drain_endpoint_inputs();

        inputs
            .into_iter()
            .filter(|(ep_id, _)| *ep_id == self.endpoint_id)
            .flat_map(|(_, input)| match input {
                EndpointPortInput::ServiceEndpointsInput(action) => action,
            })
            .collect()
    }
}

// =============================================================================
// Incremental aggregator
// =============================================================================

/// Incremental aggregator for endpoint inputs.
/// Produces `EndpointAction` directly — no adapter-side diffing needed.
#[derive(Default)]
pub struct EndpointIncrementalAggregator;

impl IncrementalAggregator for EndpointIncrementalAggregator {
    type Input = (ServiceId, Option<ReadyInfo>);
    type Output = Vec<EndpointAction>;

    fn added(
        &self,
        (service_id, info): &(ServiceId, Option<ReadyInfo>),
    ) -> Option<Vec<EndpointAction>> {
        match info {
            Some(ready) => Some(vec![EndpointAction::Update {
                service_id: *service_id,
                ready: ready.clone(),
            }]),
            None => None,
        }
    }

    fn removed(
        &self,
        (service_id, info): &(ServiceId, Option<ReadyInfo>),
    ) -> Option<Vec<EndpointAction>> {
        match info {
            Some(_) => Some(vec![EndpointAction::Remove {
                service_id: *service_id,
            }]),
            None => None,
        }
    }

    fn changed(
        &self,
        (_service_id, old_info): &(ServiceId, Option<ReadyInfo>),
        (service_id, new_info): &(ServiceId, Option<ReadyInfo>),
    ) -> Option<Vec<EndpointAction>> {
        match (old_info, new_info) {
            (_, Some(ready)) => Some(vec![EndpointAction::Update {
                service_id: *service_id,
                ready: ready.clone(),
            }]),
            (Some(_), None) => Some(vec![EndpointAction::Remove {
                service_id: *service_id,
            }]),
            (None, None) => None,
        }
    }
}
