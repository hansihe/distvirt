use std::collections::HashMap;

use distvirt_sm_router::IncrementalAggregator;

use crate::sm::{
    DRouter, EndpointId, FabricEndpointId, FabricEndpointPortInput, EndpointsInputSource,
    ServiceEndpointInfo, WireGuardPeerEndpointInfo, WireGuardPeerId,
};

#[cfg(test)]
mod tests;

/// Action returned by endpoint reconciliation.
#[derive(Clone, Debug, PartialEq)]
pub enum EndpointAction {
    ServiceUpdate {
        endpoint_id: EndpointId,
        info: ServiceEndpointInfo,
    },
    ServiceRemove {
        endpoint_id: EndpointId,
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
    fabric_endpoint_id: FabricEndpointId,
    /// Cached service endpoint state, maintained from incremental actions.
    /// Used to build full syncs for new workers.
    service_cache: HashMap<EndpointId, ServiceEndpointInfo>,
    /// Cached WireGuard peer endpoint state.
    wg_peer_cache: HashMap<WireGuardPeerId, WireGuardPeerEndpointInfo>,
}

impl EndpointAdapter {
    pub(crate) fn new(fabric_endpoint_id: FabricEndpointId) -> Self {
        EndpointAdapter {
            fabric_endpoint_id,
            service_cache: HashMap::new(),
            wg_peer_cache: HashMap::new(),
        }
    }

    /// Drain endpoint inputs from the router and update cache.
    pub(crate) fn reconcile(&mut self, router: &mut DRouter) -> (Vec<EndpointAction>, bool) {
        let inputs = router.drain_fabric_endpoint_inputs();

        let actions: Vec<EndpointAction> = inputs
            .into_iter()
            .filter(|(ep_id, _)| *ep_id == self.fabric_endpoint_id)
            .flat_map(|(_, input)| match input {
                FabricEndpointPortInput::EndpointsInput(action) => action,
            })
            .collect();

        // Update caches from actions.
        for action in &actions {
            match action {
                EndpointAction::ServiceUpdate { endpoint_id, info } => {
                    self.service_cache.insert(*endpoint_id, info.clone());
                }
                EndpointAction::ServiceRemove { endpoint_id, .. } => {
                    self.service_cache.remove(endpoint_id);
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
    pub(crate) fn build_service_sync(&self) -> Vec<(EndpointId, ServiceEndpointInfo)> {
        self.service_cache
            .iter()
            .map(|(eid, info)| (*eid, info.clone()))
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
/// Handles both endpoint SM and WireGuard peer sources via the generated
/// `EndpointsInputSource` enum.
#[derive(Default)]
pub struct EndpointIncrementalAggregator;

impl IncrementalAggregator for EndpointIncrementalAggregator {
    type Input = EndpointsInputSource;
    type Output = Vec<EndpointAction>;

    fn added(&self, input: &EndpointsInputSource) -> Option<Vec<EndpointAction>> {
        match input {
            EndpointsInputSource::EndpointEndpointInfo(endpoint_id, Some(info)) => {
                Some(vec![EndpointAction::ServiceUpdate {
                    endpoint_id: *endpoint_id,
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
            EndpointsInputSource::EndpointEndpointInfo(endpoint_id, Some(old)) => {
                Some(vec![EndpointAction::ServiceRemove {
                    endpoint_id: *endpoint_id,
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
            (
                EndpointsInputSource::EndpointEndpointInfo(_, old_info),
                EndpointsInputSource::EndpointEndpointInfo(endpoint_id, new_info),
            ) => match (old_info, new_info) {
                (_, Some(info)) => Some(vec![EndpointAction::ServiceUpdate {
                    endpoint_id: *endpoint_id,
                    info: info.clone(),
                }]),
                (Some(old), None) => Some(vec![EndpointAction::ServiceRemove {
                    endpoint_id: *endpoint_id,
                    old_info: old.clone(),
                }]),
                (None, None) => None,
            },
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
            _ => None,
        }
    }
}
