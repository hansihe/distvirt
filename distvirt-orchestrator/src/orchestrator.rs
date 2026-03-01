use std::collections::{HashMap, HashSet};

use crate::namespace::NamespaceStateMachine;
use crate::types::*;

pub struct Orchestrator {
    pub namespaces: HashMap<NamespaceId, NamespaceStateMachine>,
    pub workers: HashMap<WorkerId, WorkerState>,
    pub clients: HashSet<ClientId>,
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
                let ns = NamespaceStateMachine::new(namespace_id.clone(), spec);
                self.namespaces.insert(namespace_id.clone(), ns);

                // Assign a connected worker if available.
                if let Some(worker_id) = self.pick_worker_for_namespace() {
                    self.assign_worker_to_namespace(&namespace_id, &worker_id, out);
                }

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
        // If this worker was already connected, clean up old assignments first.
        if self.workers.contains_key(&worker_id) {
            self.handle_worker_disconnected(worker_id.clone(), out);
        }

        self.workers.insert(
            worker_id.clone(),
            WorkerState {
                capabilities,
                status: WorkerStatus::Connected,
                namespaces: HashSet::new(),
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

            // Merge namespace output into top-level output.
            out.worker_commands.extend(ns_out.worker_commands.iter().cloned());
            out.timers_set.extend(ns_out.timers_set.iter().cloned());
            out.timers_cancel.extend(ns_out.timers_cancel.iter().cloned());

            if ns_out != NamespaceOutput::default() {
                out.namespace_outputs.push((namespace_id, ns_out));
            }
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

    fn pick_worker_for_namespace(&self) -> Option<WorkerId> {
        self.workers
            .iter()
            .find(|(_, ws)| ws.status == WorkerStatus::Connected)
            .map(|(wid, _)| wid.clone())
    }

    fn assign_worker_to_namespace(
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
        out.worker_commands.push((
            worker_id.clone(),
            WorkerCommand::CreateNamespace {
                namespace_id: namespace_id.clone(),
            },
        ));
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
