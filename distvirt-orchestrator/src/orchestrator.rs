use std::collections::{HashMap, HashSet};

use crate::namespace::NamespaceStateMachine;
use crate::types::*;

pub struct Orchestrator {
    pub namespaces: HashMap<NamespaceId, NamespaceStateMachine>,
    pub workers: HashMap<WorkerId, WorkerState>,
    pub clients: HashSet<ClientId>,
}

impl Orchestrator {
    pub fn new() -> Self {
        Orchestrator {
            namespaces: HashMap::new(),
            workers: HashMap::new(),
            clients: HashSet::new(),
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
            } => {
                self.handle_worker_connected(worker_id, capabilities, &mut out);
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

    fn handle_client_command(
        &mut self,
        client_id: ClientId,
        command: ClientCommand,
        out: &mut OrchestratorOutput,
    ) {
        match command {
            ClientCommand::CreateNamespace { namespace_id, spec } => {
                if self.namespaces.contains_key(&namespace_id) {
                    out.client_events.push((
                        client_id,
                        ClientEvent::Error {
                            message: format!("namespace {:?} already exists", namespace_id.0),
                        },
                    ));
                    return;
                }
                let ns = NamespaceStateMachine::new(spec);
                self.namespaces.insert(namespace_id, ns);
                out.client_events.push((client_id, ClientEvent::Ok));
            }
            ClientCommand::DeleteNamespace { namespace_id } => {
                self.route_namespace_input(
                    namespace_id,
                    NamespaceInput::Delete { client_id },
                    out,
                );
            }
            ClientCommand::UpdateNamespace { namespace_id, spec } => {
                self.route_namespace_input(
                    namespace_id,
                    NamespaceInput::UpdateSpec { client_id, spec },
                    out,
                );
            }
            ClientCommand::GetNamespaceStatus { namespace_id } => {
                self.route_namespace_input(
                    namespace_id,
                    NamespaceInput::GetStatus { client_id },
                    out,
                );
            }
            ClientCommand::ListNamespaces => {
                // TODO: build list from all namespace state machines
                out.client_events.push((
                    client_id,
                    ClientEvent::NamespaceList {
                        namespaces: Vec::new(),
                    },
                ));
            }
            ClientCommand::Splice {
                namespace_id,
                service_id,
                worker_id,
            } => {
                self.route_namespace_input(
                    namespace_id,
                    NamespaceInput::Splice {
                        client_id,
                        service_id,
                        worker_id,
                    },
                    out,
                );
            }
            ClientCommand::Unsplice {
                namespace_id,
                service_id,
            } => {
                self.route_namespace_input(
                    namespace_id,
                    NamespaceInput::Unsplice {
                        client_id,
                        service_id,
                    },
                    out,
                );
            }
            ClientCommand::CloneNamespace {
                source_namespace_id: _,
                target_namespace_id: _,
            } => {
                // TODO: implement clone flow
                out.client_events.push((
                    client_id,
                    ClientEvent::Error {
                        message: "CloneNamespace not yet implemented".to_string(),
                    },
                ));
            }
            ClientCommand::StreamLogs {
                namespace_id,
                service_id,
            } => {
                self.route_namespace_input(
                    namespace_id,
                    NamespaceInput::StreamLogs {
                        client_id,
                        service_id,
                    },
                    out,
                );
            }
        }
    }

    fn handle_worker_connected(
        &mut self,
        worker_id: WorkerId,
        capabilities: WorkerCapabilities,
        out: &mut OrchestratorOutput,
    ) {
        self.workers.insert(
            worker_id,
            WorkerState {
                capabilities,
                status: WorkerStatus::Connected,
                namespaces: HashSet::new(),
            },
        );

        // Notify all namespaces that new capacity is available.
        let ns_ids: Vec<_> = self.namespaces.keys().cloned().collect();
        for ns_id in ns_ids {
            self.route_namespace_input(ns_id, NamespaceInput::CapacityAvailable, out);
        }
    }

    fn handle_worker_disconnected(
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

    fn route_namespace_input(
        &mut self,
        namespace_id: NamespaceId,
        input: NamespaceInput,
        out: &mut OrchestratorOutput,
    ) {
        if let Some(ns) = self.namespaces.get_mut(&namespace_id) {
            let ns_out = ns.step(input);
            if ns_out != NamespaceOutput::default() {
                out.namespace_outputs.push((namespace_id, ns_out));
            }
        } else {
            // If the input carries a client_id, send an error back.
            if let Some(client_id) = extract_client_id(&input) {
                out.client_events.push((
                    client_id,
                    ClientEvent::Error {
                        message: format!("namespace not found"),
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
        | NamespaceInput::StreamLogs { client_id, .. } => Some(client_id.clone()),
        _ => None,
    }
}
