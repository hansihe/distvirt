use std::collections::HashSet;

use crate::types::*;

use super::Orchestrator;

impl Orchestrator {
    pub(crate) fn handle_connect(
        &mut self,
        client_id: ClientId,
        namespace_id: NamespaceId,
        client_public_key: [u8; 32],
        out: &mut OrchestratorOutput,
    ) {
        // Find the namespace and an active worker with WG config.
        let ns = match self.namespaces.get(&namespace_id) {
            Some(ns) => ns,
            None => {
                out.client_events.push((
                    client_id,
                    ClientEvent::Error {
                        message: "namespace not found".to_string(),
                    },
                ));
                return;
            }
        };

        // Find an active worker for this namespace.
        let worker_id = match ns
            .workers
            .iter()
            .find(|(_, ws)| ws.fabric_status == FabricStatus::Active)
            .map(|(wid, _)| wid.clone())
        {
            Some(wid) => wid,
            None => {
                out.client_events.push((
                    client_id,
                    ClientEvent::Error {
                        message: "no active worker for namespace".to_string(),
                    },
                ));
                return;
            }
        };

        // Look up worker's WG config and public endpoint.
        let ws = match self.workers.get(&worker_id) {
            Some(ws) => ws,
            None => {
                out.client_events.push((
                    client_id,
                    ClientEvent::Error {
                        message: "worker not found".to_string(),
                    },
                ));
                return;
            }
        };

        let wg_config = match &ws.wg_config {
            Some(cfg) => cfg.clone(),
            None => {
                out.client_events.push((
                    client_id,
                    ClientEvent::Error {
                        message: "worker does not have WireGuard configured".to_string(),
                    },
                ));
                return;
            }
        };

        if ws.capabilities.public_endpoint.is_empty() {
            out.client_events.push((
                client_id,
                ClientEvent::Error {
                    message: "worker has no public endpoint".to_string(),
                },
            ));
            return;
        }

        let worker_endpoint = format!("{}:{}", ws.capabilities.public_endpoint, wg_config.listen_port);

        self.route_namespace_input(
            namespace_id,
            NamespaceInput::Connect {
                client_id,
                client_public_key,
                worker_wg_public_key: wg_config.public_key,
                worker_endpoint,
            },
            out,
        );
    }

    pub(crate) fn pick_worker_for_namespace(&self) -> Option<WorkerId> {
        self.workers.keys().next().cloned()
    }

    pub(crate) fn assign_worker_to_namespace(
        &mut self,
        namespace_id: &NamespaceId,
        worker_id: &WorkerId,
        out: &mut OrchestratorOutput,
    ) {
        if let Some(ns) = self.namespaces.get_mut(namespace_id) {
            ns.workers.insert(
                worker_id.clone(),
                NamespaceWorkerState {
                    fabric_status: FabricStatus::Creating,
                    pods: HashSet::new(),
                },
            );
        }
        if let Some(ws) = self.workers.get_mut(worker_id) {
            ws.namespaces.insert(namespace_id.clone());
        }
        let network = self
            .namespaces
            .get(namespace_id)
            .map(|ns| ns.spec.network.clone());
        if let Some(network) = network {
            out.worker_commands.push((
                worker_id.clone(),
                WorkerCommand::CreateNamespace {
                    namespace_id: namespace_id.clone(),
                    network,
                },
            ));
        }
    }
}
