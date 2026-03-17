use std::collections::HashMap;
use std::net::Ipv4Addr;

use crate::sm_new::{DRouter, EndpointId, EndpointPortInput, ReadyInfo, ServiceId};

#[cfg(test)]
mod tests;

/// Action returned by endpoint reconciliation.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum EndpointAction {
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
    /// Cached state: service_id → ReadyInfo for services currently active.
    cached: HashMap<ServiceId, ReadyInfo>,
    /// Cached service registry: name → IP.
    registry: HashMap<String, Ipv4Addr>,
}

impl EndpointAdapter {
    pub(crate) fn new(endpoint_id: EndpointId) -> Self {
        EndpointAdapter {
            endpoint_id,
            cached: HashMap::new(),
            registry: HashMap::new(),
        }
    }

    /// Drain endpoint inputs from the router, diff against cached state,
    /// and return Update/Remove actions. Updates internal cache.
    pub(crate) fn reconcile(&mut self, router: &mut DRouter) -> Vec<EndpointAction> {
        let inputs = router.drain_endpoint_inputs();

        // Process all queued inputs, keeping only the last state per endpoint.
        // Multiple propagate() calls may queue multiple inputs.
        let mut latest_entries: Option<Vec<(ServiceId, Option<ReadyInfo>)>> = None;

        for (ep_id, input) in inputs {
            if ep_id != self.endpoint_id {
                continue;
            }
            match input {
                EndpointPortInput::ServiceEndpointsInput(entries) => {
                    latest_entries = Some(entries);
                }
            }
        }

        let entries = match latest_entries {
            Some(e) => e,
            None => return Vec::new(),
        };

        // Build new state from the latest aggregated input.
        let mut new_state: HashMap<ServiceId, ReadyInfo> = HashMap::new();
        for (service_id, info_opt) in entries {
            if let Some(info) = info_opt {
                new_state.insert(service_id, info);
            }
        }

        let mut actions = Vec::new();

        // Services in new state but not cached, or with changed ReadyInfo → Update.
        for (service_id, ready) in &new_state {
            match self.cached.get(service_id) {
                Some(old) if old == ready => {}
                _ => {
                    actions.push(EndpointAction::Update {
                        service_id: *service_id,
                        ready: ready.clone(),
                    });
                }
            }
        }

        // Services in cached but not in new state → Remove.
        for service_id in self.cached.keys() {
            if !new_state.contains_key(service_id) {
                actions.push(EndpointAction::Remove {
                    service_id: *service_id,
                });
            }
        }

        self.cached = new_state;
        actions
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
