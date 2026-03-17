use std::collections::HashMap;

use crate::sm::{BackendNeedId, DRouter, ServiceId, WorkerId};

#[cfg(test)]
mod tests;

/// Push-based adapter for endpoint demand signals.
///
/// Manages BackendNeed ports in the router — one per (worker, service) pair.
/// When a worker reports `EndpointDemand`, the adapter creates or updates the
/// port. When a worker disconnects, all its ports are removed.
///
/// No reconcile method — this adapter is purely push-based. The service SM
/// reads the aggregated need via BackendNeedInput and reacts.
pub(crate) struct EndpointDemandAdapter {
    /// Maps (WorkerId, ServiceId) → BackendNeedPortId.
    ports: HashMap<(WorkerId, ServiceId), BackendNeedId>,
}

impl EndpointDemandAdapter {
    pub(crate) fn new() -> Self {
        EndpointDemandAdapter {
            ports: HashMap::new(),
        }
    }

    /// A worker reports demand for a service. Creates the port if it
    /// doesn't exist, then sets the level signal.
    pub(crate) fn push_need(
        &mut self,
        router: &mut DRouter,
        worker_id: WorkerId,
        service_id: ServiceId,
        need: crate::sm::BackendNeed,
    ) {
        let key = (worker_id, service_id);
        let port_id = *self.ports.entry(key).or_insert_with(|| {
            let id = router.create_backend_need();
            router.set_traffic_demand_edges(id, vec![service_id]);
            id
        });
        router.set_backend_need_level(port_id, need);
    }

    /// Remove all demand ports for a disconnected worker.
    /// The signal naturally falls away on the services.
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
