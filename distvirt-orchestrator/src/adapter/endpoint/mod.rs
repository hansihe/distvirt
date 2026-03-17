use std::collections::HashMap;

use distvirt_sm_router::IncrementalAggregator;

use crate::sm::{
    DRouter, EndpointId, EndpointPortInput, EndpointsInputSource, ServiceEndpointInfo, ServiceId,
    WireGuardPeerEndpointInfo, WireGuardPeerId,
};

#[cfg(test)]
mod tests;

/// Action returned by endpoint reconciliation.
#[derive(Clone, Debug, PartialEq)]
pub enum EndpointAction {
    ServiceUpdate {
        service_id: ServiceId,
        info: ServiceEndpointInfo,
    },
    ServiceRemove {
        service_id: ServiceId,
        /// The last known endpoint info, carried from the old signal value
        /// so the boundary layer can build the removal command without lookups.
        old_info: ServiceEndpointInfo,
    },
    WireGuardPeerUpdate {
        peer_id: WireGuardPeerId,
        info: WireGuardPeerEndpointInfo,
    },
    WireGuardPeerRemove {
        peer_id: WireGuardPeerId,
        old_info: WireGuardPeerEndpointInfo,
    },
}

pub(crate) struct EndpointAdapter {
    endpoint_id: EndpointId,
    /// Cached service endpoint state, maintained from incremental actions.
    /// Used to build full syncs for new workers.
    service_cache: HashMap<ServiceId, ServiceEndpointInfo>,
    /// Cached WireGuard peer endpoint state.
    wg_peer_cache: HashMap<WireGuardPeerId, WireGuardPeerEndpointInfo>,
}

impl EndpointAdapter {
    pub(crate) fn new(endpoint_id: EndpointId) -> Self {
        EndpointAdapter {
            endpoint_id,
            service_cache: HashMap::new(),
            wg_peer_cache: HashMap::new(),
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
                EndpointPortInput::EndpointsInput(action) => action,
            })
            .collect();

        // Update caches from actions.
        for action in &actions {
            match action {
                EndpointAction::ServiceUpdate { service_id, info } => {
                    self.service_cache.insert(*service_id, info.clone());
                }
                EndpointAction::ServiceRemove { service_id, .. } => {
                    self.service_cache.remove(service_id);
                }
                EndpointAction::WireGuardPeerUpdate { peer_id, info } => {
                    self.wg_peer_cache.insert(*peer_id, info.clone());
                }
                EndpointAction::WireGuardPeerRemove { peer_id, .. } => {
                    self.wg_peer_cache.remove(peer_id);
                }
            }
        }

        (actions, false)
    }

    /// Build a full sync snapshot of service endpoints from cached state (for new workers).
    pub(crate) fn build_service_sync(&self) -> Vec<(ServiceId, ServiceEndpointInfo)> {
        self.service_cache
            .iter()
            .map(|(sid, info)| (*sid, info.clone()))
            .collect()
    }

    /// Build a full sync snapshot of WireGuard peer endpoints from cached state (for new workers).
    pub(crate) fn build_wg_peer_sync(&self) -> Vec<(WireGuardPeerId, WireGuardPeerEndpointInfo)> {
        self.wg_peer_cache
            .iter()
            .map(|(pid, info)| (*pid, info.clone()))
            .collect()
    }
}

// =============================================================================
// Incremental aggregator
// =============================================================================

/// Incremental aggregator for endpoint inputs.
/// Handles both service and WireGuard peer sources via the generated
/// `EndpointsInputSource` enum.
/// Produces `EndpointAction` directly — no adapter-side diffing needed.
#[derive(Default)]
pub struct EndpointIncrementalAggregator;

impl IncrementalAggregator for EndpointIncrementalAggregator {
    type Input = EndpointsInputSource;
    type Output = Vec<EndpointAction>;

    fn added(&self, input: &EndpointsInputSource) -> Option<Vec<EndpointAction>> {
        match input {
            EndpointsInputSource::ServiceEndpointInfo(service_id, Some(info)) => {
                Some(vec![EndpointAction::ServiceUpdate {
                    service_id: *service_id,
                    info: info.clone(),
                }])
            }
            EndpointsInputSource::WireGuardPeerEndpointInfo(peer_id, Some(info)) => {
                Some(vec![EndpointAction::WireGuardPeerUpdate {
                    peer_id: *peer_id,
                    info: info.clone(),
                }])
            }
            _ => None,
        }
    }

    fn removed(&self, input: &EndpointsInputSource) -> Option<Vec<EndpointAction>> {
        match input {
            EndpointsInputSource::ServiceEndpointInfo(service_id, Some(old)) => {
                Some(vec![EndpointAction::ServiceRemove {
                    service_id: *service_id,
                    old_info: old.clone(),
                }])
            }
            EndpointsInputSource::WireGuardPeerEndpointInfo(peer_id, Some(old)) => {
                Some(vec![EndpointAction::WireGuardPeerRemove {
                    peer_id: *peer_id,
                    old_info: old.clone(),
                }])
            }
            _ => None,
        }
    }

    fn changed(
        &self,
        old: &EndpointsInputSource,
        new: &EndpointsInputSource,
    ) -> Option<Vec<EndpointAction>> {
        match (old, new) {
            // Service endpoint changed
            (
                EndpointsInputSource::ServiceEndpointInfo(_, old_info),
                EndpointsInputSource::ServiceEndpointInfo(service_id, new_info),
            ) => match (old_info, new_info) {
                (_, Some(info)) => Some(vec![EndpointAction::ServiceUpdate {
                    service_id: *service_id,
                    info: info.clone(),
                }]),
                (Some(old), None) => Some(vec![EndpointAction::ServiceRemove {
                    service_id: *service_id,
                    old_info: old.clone(),
                }]),
                (None, None) => None,
            },
            // WireGuard peer endpoint changed
            (
                EndpointsInputSource::WireGuardPeerEndpointInfo(_, old_info),
                EndpointsInputSource::WireGuardPeerEndpointInfo(peer_id, new_info),
            ) => match (old_info, new_info) {
                (_, Some(info)) => Some(vec![EndpointAction::WireGuardPeerUpdate {
                    peer_id: *peer_id,
                    info: info.clone(),
                }]),
                (Some(old), None) => Some(vec![EndpointAction::WireGuardPeerRemove {
                    peer_id: *peer_id,
                    old_info: old.clone(),
                }]),
                (None, None) => None,
            },
            // Cross-source change shouldn't happen, but handle gracefully
            _ => None,
        }
    }
}
