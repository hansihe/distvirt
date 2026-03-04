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
    pub next_segment_id: u16,
    pub active_segment_ids: HashSet<u16>,
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
            next_segment_id: 1,
            active_segment_ids: HashSet::new(),
        }
    }

    pub fn alloc_segment_id(&mut self) -> u16 {
        loop {
            let id = self.next_segment_id;
            self.next_segment_id = self.next_segment_id.wrapping_add(1);
            if id == 0 {
                continue;
            }
            if !self.active_segment_ids.contains(&id) {
                self.active_segment_ids.insert(id);
                return id;
            }
        }
    }

    pub fn free_segment_id(&mut self, id: u16) {
        self.active_segment_ids.remove(&id);
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
                tunnel_config,
            } => {
                self.handle_worker_connected(worker_id, capabilities, wg_config, tunnel_config, &mut out);
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

        #[cfg(debug_assertions)]
        self.check_invariants();

        out
    }

    #[cfg(debug_assertions)]
    fn check_invariants(&self) {
        // Verify worker↔namespace consistency.
        //
        // Direction 1: If a namespace lists a worker, that worker should
        // reference the namespace (unless the worker has disconnected and the
        // namespace hasn't processed WorkerLost yet).
        for (ns_id, ns) in &self.namespaces {
            for wid in ns.workers.keys() {
                if let Some(ws) = self.workers.get(wid) {
                    debug_assert!(
                        ws.namespaces.contains(ns_id),
                        "Namespace {:?} has worker {:?} but worker doesn't list the namespace",
                        ns_id,
                        wid,
                    );
                }
            }
        }

        // Direction 2: If a worker references a namespace, that namespace must
        // exist. (The namespace may have internally removed the worker via
        // NamespaceFailed/WorkerLost without the orchestrator updating
        // WorkerState.namespaces, so we only check existence, not membership.)
        for (wid, ws) in &self.workers {
            for ns_id in &ws.namespaces {
                debug_assert!(
                    self.namespaces.contains_key(ns_id),
                    "Worker {:?} references namespace {:?} which doesn't exist",
                    wid,
                    ns_id,
                );
            }
        }
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
