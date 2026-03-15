use std::collections::HashMap;

use crate::sm_new::{BackendNeedId, Router, ServiceId, WorkerId};

#[cfg(test)]
mod tests;

/// Pure push-based adapter: manages BackendNeed ports in the router.
///
/// One BackendNeed port per (worker, service) pair. When a worker reports
/// ServiceBackendNeed, the adapter creates or updates the port. When a worker
/// disconnects, all its ports are removed.
///
/// No reconcile method — this adapter is purely push-based. The service SM
/// reads the aggregated need via BackendNeedInput and reacts.
pub(crate) struct BackendNeedAdapter {
    /// Maps (WorkerId, ServiceId) → BackendNeedPortId.
    ports: HashMap<(WorkerId, ServiceId), BackendNeedId>,
}

impl BackendNeedAdapter {
    pub(crate) fn new() -> Self {
        BackendNeedAdapter {
            ports: HashMap::new(),
        }
    }

    /// A worker reports backend need for a service. Creates the port if it
    /// doesn't exist, then sets the level signal.
    pub(crate) fn push_need(
        &mut self,
        router: &mut Router,
        worker_id: WorkerId,
        service_id: ServiceId,
        need: crate::sm_new::BackendNeed,
    ) {
        let key = (worker_id, service_id);
        let port_id = *self.ports.entry(key).or_insert_with(|| {
            let id = router.create_backend_need();
            router.set_backend_need_to_service_edges(id, vec![service_id]);
            id
        });
        router.set_backend_need_level(port_id, need);
    }

    /// Remove all BackendNeed ports for a disconnected worker.
    /// The signal naturally falls away on the services.
    pub(crate) fn remove_worker(&mut self, router: &mut Router, worker_id: &WorkerId) {
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
