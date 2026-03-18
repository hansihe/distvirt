use std::collections::HashMap;

use crate::core::EndpointDemandSignal;
use crate::sm::{BackendNeedId, DRouter, EndpointId, WorkerId};

#[cfg(test)]
mod tests;

/// Push-based adapter for endpoint demand signals.
///
/// Receives unified `EndpointDemandSignal` events (traffic impulses and
/// activation level changes) and translates them into BackendNeed ports
/// in the router.
///
/// Manages one BackendNeed port per (worker, endpoint) pair. When a worker
/// disconnects, all its ports are removed.
///
/// No reconcile method — this adapter is purely push-based. The endpoint SM
/// reads the aggregated need via BackendNeedInput and reacts.
pub(crate) struct EndpointDemandAdapter {
    /// Maps (WorkerId, EndpointId) → BackendNeedPortId.
    ports: HashMap<(WorkerId, EndpointId), BackendNeedId>,
}

impl EndpointDemandAdapter {
    pub(crate) fn new() -> Self {
        EndpointDemandAdapter {
            ports: HashMap::new(),
        }
    }

    /// A worker reports demand for an endpoint. Creates the port if it
    /// doesn't exist, then sets the level signal based on signal type.
    pub(crate) fn push_demand(
        &mut self,
        router: &mut DRouter,
        worker_id: WorkerId,
        endpoint_id: EndpointId,
        signal: EndpointDemandSignal,
    ) {
        let need = match signal {
            EndpointDemandSignal::Traffic => crate::sm::BackendNeed::Traffic,
            EndpointDemandSignal::Active { active: true } => crate::sm::BackendNeed::Active,
            EndpointDemandSignal::Active { active: false } => crate::sm::BackendNeed::None,
        };

        let key = (worker_id, endpoint_id);
        let port_id = *self.ports.entry(key).or_insert_with(|| {
            let id = router.create_backend_need();
            router.set_traffic_demand_edges(id, vec![endpoint_id]);
            id
        });
        router.set_backend_need_level(port_id, need);
    }

    /// Remove all demand ports for a disconnected worker.
    /// The signal naturally falls away on the endpoints.
    pub(crate) fn remove_worker(&mut self, router: &mut DRouter, worker_id: &WorkerId) {
        let to_remove: Vec<(WorkerId, EndpointId)> = self
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
