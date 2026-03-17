use std::collections::HashMap;
use std::net::Ipv4Addr;

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

/// Action returned by service registry reconciliation.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum RegistryAction {
    /// Full replacement of the registry (sent on first update or to new workers).
    Sync { entries: Vec<RegistryEntry> },
    /// Incremental update: added and removed entries.
    Update {
        added: Vec<RegistryEntry>,
        removed: Vec<String>,
    },
}

/// A service name → IP mapping for worker DNS resolution.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct RegistryEntry {
    pub name: String,
    pub ip: Ipv4Addr,
}

pub(crate) struct EndpointAdapter {
    endpoint_id: EndpointId,
    /// Cached service registry: name → IP.
    /// This is spec-driven (not from router inputs), so it stays cached.
    registry: HashMap<String, Ipv4Addr>,
}

impl EndpointAdapter {
    pub(crate) fn new(endpoint_id: EndpointId) -> Self {
        EndpointAdapter {
            endpoint_id,
            registry: HashMap::new(),
        }
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

    /// Update the service registry from a new namespace spec.
    /// Diffs against the cached registry and returns an incremental Update action,
    /// or None if nothing changed.
    pub(crate) fn update_registry(
        &mut self,
        services: impl Iterator<Item = (String, Ipv4Addr)>,
    ) -> Option<RegistryAction> {
        let new_registry: HashMap<String, Ipv4Addr> = services.collect();

        if new_registry == self.registry {
            return None;
        }

        let mut added = Vec::new();
        let mut removed = Vec::new();

        // New or changed entries.
        for (name, ip) in &new_registry {
            match self.registry.get(name) {
                Some(old_ip) if old_ip == ip => {}
                Some(_) => {
                    // IP changed: remove old, add new.
                    removed.push(name.clone());
                    added.push(RegistryEntry {
                        name: name.clone(),
                        ip: *ip,
                    });
                }
                None => {
                    added.push(RegistryEntry {
                        name: name.clone(),
                        ip: *ip,
                    });
                }
            }
        }

        // Removed entries.
        for name in self.registry.keys() {
            if !new_registry.contains_key(name) {
                removed.push(name.clone());
            }
        }

        self.registry = new_registry;

        Some(RegistryAction::Update { added, removed })
    }

    /// Build a full RegistrySync action from current cached state.
    /// Used when a new worker connects and needs the full registry.
    pub(crate) fn build_registry_sync(&self) -> RegistryAction {
        let entries = self
            .registry
            .iter()
            .map(|(name, ip)| RegistryEntry {
                name: name.clone(),
                ip: *ip,
            })
            .collect();
        RegistryAction::Sync { entries }
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
