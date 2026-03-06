mod client;
mod networking;
mod scheduling;
mod workers;

use std::collections::{BTreeMap, BTreeSet};

use crate::namespace::NamespaceStateMachine;
use crate::types::*;

pub struct Orchestrator {
    pub namespaces: BTreeMap<NamespaceId, NamespaceStateMachine>,
    pub workers: BTreeMap<WorkerId, WorkerState>,
    pub clients: BTreeSet<ClientId>,
    pub next_pod_id: u64,
    pub next_segment_id: u16,
    pub active_segment_ids: BTreeSet<u16>,
    /// Global artifact placement table tracking where artifacts are stored across the cluster.
    ///
    /// Currently a simple data structure passed by `&mut` reference to namespace state machines.
    /// As artifact lifecycle grows more complex (transfers, eviction policies, multi-step operations),
    /// this should evolve into a separate state machine with its own command/event interface:
    ///   Namespace → PlacementSM: RegisterArtifact, Lock, Unlock, Remove
    ///   PlacementSM → Namespace: ArtifactLost (eviction), TransferComplete
    /// This would enable independent model-checking of placement invariants.
    pub placement_table: PlacementTable,
    pub lease_table: LeaseTable,
}

impl Default for Orchestrator {
    fn default() -> Self {
        Self::new()
    }
}

impl Orchestrator {
    pub fn new() -> Self {
        Orchestrator {
            namespaces: BTreeMap::new(),
            workers: BTreeMap::new(),
            clients: BTreeSet::new(),
            next_pod_id: 0,
            next_segment_id: 1,
            active_segment_ids: BTreeSet::new(),
            placement_table: PlacementTable::default(),
            lease_table: LeaseTable::default(),
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

        // Verify every lease references an existing worker.
        for (pod_id, lease) in self.lease_table.iter() {
            debug_assert!(
                self.workers.contains_key(&lease.worker_id),
                "Lease for pod {:?} references worker {:?} which doesn't exist",
                pod_id,
                lease.worker_id,
            );
        }
    }

    /// Recompute pressure scores for a specific worker based on pod counts across all namespaces.
    pub fn recompute_worker_pressure(&mut self, worker_id: &WorkerId) {
        let pod_count: usize = self.namespaces.values()
            .map(|ns| ns.pod_map.worker_pod_count(worker_id))
            .sum();
        let committed_mb = pod_count as u64 * DEFAULT_POD_MEMORY_MB
            + self.lease_table.leased_memory_mb(worker_id);
        if let Some(ws) = self.workers.get_mut(worker_id) {
            ws.recompute_pressure(committed_mb);
        }
        self.propagate_pressure_to_namespaces(worker_id);
    }

    /// Propagate a worker's max pressure band to all namespace worker states for that worker.
    fn propagate_pressure_to_namespaces(&mut self, worker_id: &WorkerId) {
        let band = match self.workers.get(worker_id) {
            Some(ws) => ws.pressure_bands.max_band(),
            None => return,
        };
        for ns in self.namespaces.values_mut() {
            if let Some(nws) = ns.workers.get_mut(worker_id) {
                nws.pressure_band = band;
            }
        }
    }

    /// Recompute pressure scores for all connected workers.
    pub fn recompute_all_worker_pressure(&mut self) {
        let worker_ids: Vec<WorkerId> = self.workers.keys().cloned().collect();
        for wid in worker_ids {
            self.recompute_worker_pressure(&wid);
        }
    }

    pub(crate) fn route_namespace_input(
        &mut self,
        namespace_id: NamespaceId,
        input: NamespaceInput,
        out: &mut OrchestratorOutput,
    ) {
        // Release leases for pod lifecycle events and timeouts before forwarding.
        match &input {
            NamespaceInput::WorkerEvent { event, .. } => match event {
                WorkerEvent::PodRunning { pod_id }
                | WorkerEvent::PodExited { pod_id, .. }
                | WorkerEvent::PodFailed { pod_id, .. }
                | WorkerEvent::PodSuspended { pod_id, .. }
                | WorkerEvent::PodSuspendFailed { pod_id, .. } => {
                    self.lease_table.release(pod_id);
                }
                _ => {}
            },
            NamespaceInput::TimerFired { timer_key } => match timer_key {
                TimerKey::LaunchTimeout { pod_id, .. }
                | TimerKey::ResumeTimeout { pod_id, .. } => {
                    self.lease_table.release(pod_id);
                }
                _ => {}
            },
            _ => {}
        }

        if let Some(ns) = self.namespaces.get_mut(&namespace_id) {
            let ns_out = ns.step(input, &mut self.placement_table);
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
