use std::collections::HashMap;

use distvirt_sm_router::IncrementalAggregator;

use crate::sm::{DRouter, EndpointId, EndpointPortInput, ServiceEndpointInfo, ServiceId};

#[cfg(test)]
mod tests;

/// Action returned by endpoint reconciliation.
#[derive(Clone, Debug, PartialEq)]
pub enum EndpointAction {
    Update {
        service_id: ServiceId,
        info: ServiceEndpointInfo,
    },
    Remove {
        service_id: ServiceId,
        /// The last known endpoint info, carried from the old signal value
        /// so the boundary layer can build the removal command without lookups.
        old_info: ServiceEndpointInfo,
    },
}

pub(crate) struct EndpointAdapter {
    endpoint_id: EndpointId,
    /// Cached endpoint state, maintained from incremental actions.
    /// Used to build full syncs for new workers.
    cache: HashMap<ServiceId, ServiceEndpointInfo>,
}

impl EndpointAdapter {
    pub(crate) fn new(endpoint_id: EndpointId) -> Self {
        EndpointAdapter {
            endpoint_id,
            cache: HashMap::new(),
        }
    }

    /// Drain endpoint inputs from the router and update cache.
    ///
    /// Returns `(actions, mutated_router)`. Currently only drains, so
    /// `mutated_router` is always `false`.
    pub(crate) fn reconcile(&mut self, router: &mut DRouter) -> (Vec<EndpointAction>, bool) {
        let inputs = router.drain_endpoint_inputs();

        let actions: Vec<EndpointAction> = inputs
            .into_iter()
            .filter(|(ep_id, _)| *ep_id == self.endpoint_id)
            .flat_map(|(_, input)| match input {
                EndpointPortInput::ServiceEndpointsInput(action) => action,
            })
            .collect();

        // Update cache from actions.
        for action in &actions {
            match action {
                EndpointAction::Update { service_id, info } => {
                    self.cache.insert(*service_id, info.clone());
                }
                EndpointAction::Remove { service_id, .. } => {
                    self.cache.remove(service_id);
                }
            }
        }

        (actions, false)
    }

    /// Build a full sync snapshot from cached state (for new workers).
    pub(crate) fn build_sync(&self) -> Vec<(ServiceId, ServiceEndpointInfo)> {
        self.cache
            .iter()
            .map(|(sid, info)| (*sid, info.clone()))
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
    type Input = (ServiceId, Option<ServiceEndpointInfo>);
    type Output = Vec<EndpointAction>;

    fn added(
        &self,
        (service_id, info): &(ServiceId, Option<ServiceEndpointInfo>),
    ) -> Option<Vec<EndpointAction>> {
        match info {
            Some(ep_info) => Some(vec![EndpointAction::Update {
                service_id: *service_id,
                info: ep_info.clone(),
            }]),
            None => None,
        }
    }

    fn removed(
        &self,
        (service_id, info): &(ServiceId, Option<ServiceEndpointInfo>),
    ) -> Option<Vec<EndpointAction>> {
        match info {
            Some(old) => Some(vec![EndpointAction::Remove {
                service_id: *service_id,
                old_info: old.clone(),
            }]),
            None => None,
        }
    }

    fn changed(
        &self,
        (_service_id, old_info): &(ServiceId, Option<ServiceEndpointInfo>),
        (service_id, new_info): &(ServiceId, Option<ServiceEndpointInfo>),
    ) -> Option<Vec<EndpointAction>> {
        match (old_info, new_info) {
            (_, Some(ep_info)) => Some(vec![EndpointAction::Update {
                service_id: *service_id,
                info: ep_info.clone(),
            }]),
            (Some(old), None) => Some(vec![EndpointAction::Remove {
                service_id: *service_id,
                old_info: old.clone(),
            }]),
            (None, None) => None,
        }
    }
}
