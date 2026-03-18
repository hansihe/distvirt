use std::collections::HashMap;

use crate::core::EndpointDemandSignal;
use crate::sm::{DRouter, EndpointDemandId, EndpointId, WorkerId};

#[cfg(test)]
mod tests;

/// Push-based adapter for endpoint demand signals.
///
/// Receives unified `EndpointDemandSignal` events (traffic impulses and
/// activation level changes) and translates them into EndpointDemand ports
/// in the router.
///
/// Manages one EndpointDemand port per (worker, endpoint) pair. When a worker
/// disconnects, all its ports are removed.
///
/// Active signals set the level on the port. Traffic signals send an event
/// to the endpoint (unit impulse).
pub(crate) struct EndpointDemandAdapter {
    /// Maps (WorkerId, EndpointId) → EndpointDemandId.
    ports: HashMap<(WorkerId, EndpointId), EndpointDemandId>,
}

impl EndpointDemandAdapter {
    pub(crate) fn new() -> Self {
        EndpointDemandAdapter {
            ports: HashMap::new(),
        }
    }

    /// A worker reports demand for an endpoint. Creates the port if it
    /// doesn't exist, then either sets the active level or sends a traffic event.
    pub(crate) fn push_demand(
        &mut self,
        router: &mut DRouter,
        worker_id: WorkerId,
        endpoint_id: EndpointId,
        signal: EndpointDemandSignal,
    ) {
        let key = (worker_id, endpoint_id);
        let port_id = *self.ports.entry(key).or_insert_with(|| {
            let id = router.create_endpoint_demand();
            router.set_endpoint_port_demand_edges(id, vec![endpoint_id]);
            id
        });

        match signal {
            EndpointDemandSignal::Active { active } => {
                router.set_endpoint_demand_active(port_id, active);
            }
            EndpointDemandSignal::Traffic => {
                router.send_endpoint_demand_traffic(port_id, endpoint_id, ());
            }
        }
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
                router.destroy_endpoint_demand(port_id);
            }
        }
    }
}
