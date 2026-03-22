use std::collections::HashMap;
use std::net::Ipv4Addr;

use distvirt_sm_router::IncrementalAggregator;

use crate::sm::{DRouter, DnsRegistryId, DnsRegistryPortInput, DnsEntryInfo, EndpointId, WorkloadId};

#[cfg(test)]
mod tests;

/// Action returned by DNS registry reconciliation.
#[derive(Clone, Debug, PartialEq)]
pub enum DnsRegistryAction {
    Add { name: String, ip: Ipv4Addr },
    Remove { name: String },
}

pub(crate) struct DnsRegistryAdapter {
    dns_registry_id: DnsRegistryId,
    /// Cached registry state, maintained from incremental actions.
    /// Used to build full syncs for new workers.
    cache: HashMap<String, Ipv4Addr>,
}

impl DnsRegistryAdapter {
    pub(crate) fn new(dns_registry_id: DnsRegistryId) -> Self {
        DnsRegistryAdapter {
            dns_registry_id,
            cache: HashMap::new(),
        }
    }

    /// Drain DNS registry inputs from the router and update cache.
    ///
    /// Returns `(actions, mutated_router)`. Currently only drains, so
    /// `mutated_router` is always `false`.
    pub(crate) fn reconcile(&mut self, router: &mut DRouter) -> (Vec<DnsRegistryAction>, bool) {
        let inputs = router.drain_dns_registry_inputs();

        let actions: Vec<DnsRegistryAction> = inputs
            .into_iter()
            .filter(|(id, _)| *id == self.dns_registry_id)
            .flat_map(|(_, input)| match input {
                DnsRegistryPortInput::EndpointDnsInput(actions) => actions,
                DnsRegistryPortInput::WorkloadDnsInput(actions) => actions,
            })
            .collect();

        // Update cache from actions.
        for action in &actions {
            match action {
                DnsRegistryAction::Add { name, ip } => {
                    self.cache.insert(name.clone(), *ip);
                }
                DnsRegistryAction::Remove { name } => {
                    self.cache.remove(name);
                }
            }
        }

        (actions, false)
    }

    /// Build a full sync snapshot from cached state (for new workers).
    pub(crate) fn build_sync(&self) -> Vec<(String, Ipv4Addr)> {
        self.cache.iter().map(|(n, ip)| (n.clone(), *ip)).collect()
    }
}

// =============================================================================
// Incremental aggregators
// =============================================================================

fn dns_added(info: &Option<DnsEntryInfo>) -> Option<Vec<DnsRegistryAction>> {
    info.as_ref().map(|entry| {
        vec![DnsRegistryAction::Add {
            name: entry.name.clone(),
            ip: entry.ip,
        }]
    })
}

fn dns_removed(info: &Option<DnsEntryInfo>) -> Option<Vec<DnsRegistryAction>> {
    info.as_ref().map(|entry| {
        vec![DnsRegistryAction::Remove {
            name: entry.name.clone(),
        }]
    })
}

fn dns_changed(
    old_info: &Option<DnsEntryInfo>,
    new_info: &Option<DnsEntryInfo>,
) -> Option<Vec<DnsRegistryAction>> {
    match (old_info, new_info) {
        (Some(old), Some(new)) => {
            let mut actions = Vec::new();
            if old.name != new.name {
                actions.push(DnsRegistryAction::Remove {
                    name: old.name.clone(),
                });
            }
            actions.push(DnsRegistryAction::Add {
                name: new.name.clone(),
                ip: new.ip,
            });
            Some(actions)
        }
        (Some(old), None) => Some(vec![DnsRegistryAction::Remove {
            name: old.name.clone(),
        }]),
        (None, Some(new)) => Some(vec![DnsRegistryAction::Add {
            name: new.name.clone(),
            ip: new.ip,
        }]),
        (None, None) => None,
    }
}

/// Incremental aggregator for Endpoint → DnsRegistry inputs.
#[derive(Default)]
pub struct EndpointDnsIncrementalAggregator;

impl IncrementalAggregator for EndpointDnsIncrementalAggregator {
    type Input = (EndpointId, Option<DnsEntryInfo>);
    type Output = Vec<DnsRegistryAction>;

    fn added(&self, (_, info): &Self::Input) -> Option<Self::Output> {
        dns_added(info)
    }

    fn removed(&self, (_, info): &Self::Input) -> Option<Self::Output> {
        dns_removed(info)
    }

    fn changed(&self, (_, old_info): &Self::Input, (_, new_info): &Self::Input) -> Option<Self::Output> {
        dns_changed(old_info, new_info)
    }
}

/// Incremental aggregator for Workload → DnsRegistry inputs.
#[derive(Default)]
pub struct WorkloadDnsIncrementalAggregator;

impl IncrementalAggregator for WorkloadDnsIncrementalAggregator {
    type Input = (WorkloadId, Option<DnsEntryInfo>);
    type Output = Vec<DnsRegistryAction>;

    fn added(&self, (_, info): &Self::Input) -> Option<Self::Output> {
        dns_added(info)
    }

    fn removed(&self, (_, info): &Self::Input) -> Option<Self::Output> {
        dns_removed(info)
    }

    fn changed(&self, (_, old_info): &Self::Input, (_, new_info): &Self::Input) -> Option<Self::Output> {
        dns_changed(old_info, new_info)
    }
}
