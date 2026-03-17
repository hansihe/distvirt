use std::collections::HashMap;

use crate::sm::{BackendNeedId, DRouter, ServiceId, WorkerId};

#[cfg(test)]
mod tests;

/// Push-based adapter for flow-sourced demand.
///
/// When a worker reports EndpointFlowStatus with active flows for a service,
/// this adapter creates a BackendNeed port (same type as BackendNeedAdapter)
/// and sets it to Traffic level. The service SM's BackendNeedAggregator sees
/// both worker-reported need and flow-sourced need, taking the max.
///
/// This keeps services alive (prevents idle timeout) while there are active
/// TCP flows, enabling correct scale-to-zero behavior.
pub(crate) struct FlowDemandAdapter {
    /// Maps (WorkerId, ServiceId) -> BackendNeedPortId for flow-sourced demand.
    ports: HashMap<(WorkerId, ServiceId), BackendNeedId>,
}

impl FlowDemandAdapter {
    pub(crate) fn new() -> Self {
        FlowDemandAdapter {
            ports: HashMap::new(),
        }
    }

    /// A worker reports active flows for a service. Creates the BackendNeed
    /// port if it doesn't exist, then sets the level to Traffic.
    pub(crate) fn set_active(
        &mut self,
        router: &mut DRouter,
        worker_id: WorkerId,
        service_id: ServiceId,
    ) {
        let key = (worker_id, service_id);
        let port_id = *self.ports.entry(key).or_insert_with(|| {
            let id = router.create_backend_need();
            router.set_backend_need_to_service_edges(id, vec![service_id]);
            id
        });
        router.set_backend_need_level(port_id, crate::sm::BackendNeed::Traffic);
    }

    /// A worker reports no active flows for a service. Sets the level to None
    /// but keeps the port (it will be removed on worker disconnect).
    pub(crate) fn set_inactive(
        &mut self,
        router: &mut DRouter,
        worker_id: WorkerId,
        service_id: ServiceId,
    ) {
        let key = (worker_id, service_id);
        if let Some(&port_id) = self.ports.get(&key) {
            router.set_backend_need_level(port_id, crate::sm::BackendNeed::None);
        }
    }

    /// Remove all flow demand ports for a disconnected worker.
    pub(crate) fn remove_worker(&mut self, router: &mut DRouter, worker_id: &WorkerId) {
        let to_remove: Vec<(WorkerId, ServiceId)> = self
            .ports
            .keys()
            .filter(|(w, _)| w == worker_id)
            .copied()
            .collect();
        for key in to_remove {
            if let Some(port_id) = self.ports.remove(&key) {
                router.destroy_backend_need(port_id);
            }
        }
    }
}
