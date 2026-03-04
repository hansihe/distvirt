mod client;
mod networking;
mod scheduling;
mod workers;

use std::collections::{HashMap, HashSet};

use crate::namespace::NamespaceStateMachine;
use crate::types::*;

pub struct Orchestrator {
    pub namespaces: HashMap<NamespaceId, NamespaceStateMachine>,
    pub workers: HashMap<WorkerId, WorkerState>,
    pub clients: HashSet<ClientId>,
    pub next_pod_id: u64,
}

impl Default for Orchestrator {
    fn default() -> Self {
        Self::new()
    }
}

impl Orchestrator {
    pub fn new() -> Self {
        Orchestrator {
            namespaces: HashMap::new(),
            workers: HashMap::new(),
            clients: HashSet::new(),
            next_pod_id: 0,
        }
    }

    pub fn step(&mut self, input: OrchestratorInput) -> OrchestratorOutput {
        let mut out = OrchestratorOutput::default();

        match input {
            OrchestratorInput::ClientConnected { client_id } => {
                self.clients.insert(client_id);
            }
            OrchestratorInput::ClientDisconnected { client_id } => {
                self.clients.remove(&client_id);
            }
            OrchestratorInput::ClientCommand { client_id, command } => {
                self.handle_client_command(client_id, command, &mut out);
            }
            OrchestratorInput::WorkerConnected {
                worker_id,
                capabilities,
                wg_config,
            } => {
                self.handle_worker_connected(worker_id, capabilities, wg_config, &mut out);
            }
            OrchestratorInput::WorkerDisconnected { worker_id } => {
                self.handle_worker_disconnected(worker_id, &mut out);
            }
            OrchestratorInput::NamespaceInput {
                namespace_id,
                input,
            } => {
                self.route_namespace_input(namespace_id, input, &mut out);
            }
        }

        out
    }

    pub(crate) fn route_namespace_input(
        &mut self,
        namespace_id: NamespaceId,
        input: NamespaceInput,
        out: &mut OrchestratorOutput,
    ) {
        if let Some(ns) = self.namespaces.get_mut(&namespace_id) {
            let ns_out = ns.step(input);
            self.process_namespace_output(namespace_id, ns_out, out);
        } else {
            // If the input carries a client_id, send an error back.
            if let Some(client_id) = extract_client_id(&input) {
                out.client_events.push((
                    client_id,
                    ClientEvent::Error {
                        message: "namespace not found".to_string(),
                    },
                ));
            }
        }
    }
}

/// Extract client_id from namespace inputs that carry one.
fn extract_client_id(input: &NamespaceInput) -> Option<ClientId> {
    match input {
        NamespaceInput::UpdateSpec { client_id, .. }
        | NamespaceInput::Delete { client_id, .. }
        | NamespaceInput::GetStatus { client_id, .. }
        | NamespaceInput::Splice { client_id, .. }
        | NamespaceInput::Unsplice { client_id, .. }
        | NamespaceInput::StreamLogs { client_id, .. }
        | NamespaceInput::Connect { client_id, .. }
        | NamespaceInput::Disconnect { client_id, .. }
        | NamespaceInput::DeactivateWorkload { client_id, .. } => Some(client_id.clone()),
        _ => None,
    }
}
