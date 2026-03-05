use std::collections::HashSet;

use crate::types::*;

use super::Orchestrator;

impl Orchestrator {
    pub(crate) fn handle_worker_connected(
        &mut self,
        worker_id: WorkerId,
        capabilities: WorkerCapabilities,
        wg_config: Option<WorkerWgConfig>,
        tunnel_config: Option<WorkerTunnelConfig>,
        out: &mut OrchestratorOutput,
    ) {
        // If this worker was already connected, clean up old assignments first.
        if self.workers.contains_key(&worker_id) {
            self.handle_worker_disconnected(worker_id.clone(), out);
        }

        self.workers.insert(
            worker_id.clone(),
            WorkerState {
                capabilities,
                namespaces: HashSet::new(),
                wg_config,
                tunnel_config,
                conditions: std::collections::HashMap::new(),
                transfer_listen_port: None,
            },
        );

        // Assign this worker to all non-Destroying namespaces that don't already have it.
        let assignable: Vec<NamespaceId> = self
            .namespaces
            .iter()
            .filter(|(_, ns)| {
                ns.status != NamespaceStatus::Destroying && !ns.workers.contains_key(&worker_id)
            })
            .map(|(ns_id, _)| ns_id.clone())
            .collect();

        for ns_id in assignable {
            self.assign_worker_to_namespace(&ns_id, &worker_id, out);
        }

        // Push worker registry to all workers.
        self.push_worker_registry(out);

        // Check all namespaces for workloads waiting for capacity and schedule them.
        self.schedule_waiting_pods(out);
    }

    pub(crate) fn handle_worker_disconnected(
        &mut self,
        worker_id: WorkerId,
        out: &mut OrchestratorOutput,
    ) {
        let worker_state = self.workers.remove(&worker_id);

        // Fan out WorkerLost to every namespace that had this worker.
        if let Some(ws) = worker_state {
            for ns_id in ws.namespaces {
                self.route_namespace_input(
                    ns_id,
                    NamespaceInput::WorkerLost {
                        worker_id: worker_id.clone(),
                    },
                    out,
                );
            }
        }

        // Push updated worker registry to remaining workers.
        self.push_worker_registry(out);
    }
}
