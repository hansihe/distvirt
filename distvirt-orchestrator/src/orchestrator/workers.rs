use std::collections::HashSet;

use crate::types::*;

use super::Orchestrator;

impl Orchestrator {
    pub(crate) fn handle_worker_connected(
        &mut self,
        worker_id: WorkerId,
        capabilities: WorkerCapabilities,
        wg_config: Option<WorkerWgConfig>,
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
            },
        );

        // Assign this worker to any namespace in Creating state with no workers.
        let workerless: Vec<NamespaceId> = self
            .namespaces
            .iter()
            .filter(|(_, ns)| {
                ns.status == NamespaceStatus::Creating && ns.workers.is_empty()
            })
            .map(|(ns_id, _)| ns_id.clone())
            .collect();

        for ns_id in workerless {
            self.assign_worker_to_namespace(&ns_id, &worker_id, out);
        }

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
    }
}
