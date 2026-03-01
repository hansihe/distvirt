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
                let namespaces = self
                    .namespaces
                    .iter()
                    .map(|(ns_id, ns)| {
                        let mut services = HashMap::new();
                        for (svc_id, svc) in &ns.services {
                            let wl_id = &svc.workload_id;
                            let wl = ns.workloads.get(wl_id);

                            let (wl_state_str, pod_id, worker_id) = match wl.map(|w| &w.state) {
                                Some(WorkloadState::Dormant) | None => {
                                    ("dormant".to_string(), None, None)
                                }
                                Some(WorkloadState::WaitingForCapacity) => {
                                    ("waiting_for_capacity".to_string(), None, None)
                                }
                                Some(WorkloadState::Launching {
                                    pod_id, worker_id, ..
                                }) => (
                                    "launching".to_string(),
                                    Some(pod_id.clone()),
                                    Some(worker_id.clone()),
                                ),
                                Some(WorkloadState::Running {
                                    pod_id, worker_id, ..
                                }) => (
                                    "running".to_string(),
                                    Some(pod_id.clone()),
                                    Some(worker_id.clone()),
                                ),
                            };

                            let svc_state_str = match &svc.state {
                                ServiceState::Pending => "pending".to_string(),
                                ServiceState::Idle => "idle".to_string(),
                                ServiceState::NeedBackend => "need_backend".to_string(),
                                ServiceState::Active { .. } => "active".to_string(),
                            };

                            let backend_need = match &svc.state {
                                ServiceState::Active { backend_need, .. } => {
                                    Some(backend_need.clone())
                                }
                                _ => None,
                            };

                            let spliced = matches!(
                                wl.map(|w| &w.state),
                                Some(WorkloadState::Running {
                                    hosting: WorkloadHosting::Spliced { .. },
                                    ..
                                })
                            );

                            services.insert(
                                svc_id.clone(),
                                ServiceStatusReport {
                                    service_state: svc_state_str,
                                    workload_id: wl_id.clone(),
                                    workload_state: wl_state_str,
                                    pod_id,
                                    worker_id,
                                    backend_need,
                                    activation_enabled: svc.has_activation,
                                    spliced,
                                },
                            );
                        }

                        NamespaceStatusReport {
                            namespace_id: ns_id.clone(),
                            status: ns.status.clone(),
                            services,
                        }
                    })
                    .collect();

                out.client_events.push((
                    client_id,
                    ClientEvent::NamespaceList { namespaces },
                ));
            }
            ClientCommand::Splice {
                namespace_id,
                workload_id,
                worker_id,
            } => {
                self.route_namespace_input(
                    namespace_id,
                    NamespaceInput::Splice {
                        client_id,
                        workload_id,
                        worker_id,
                    },
                    out,
                );
            }
            ClientCommand::Unsplice {
                namespace_id,
                workload_id,
            } => {
                self.route_namespace_input(
                    namespace_id,
                    NamespaceInput::Unsplice {
                        client_id,
                        workload_id,
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

        // Check all namespaces for workloads waiting for capacity and schedule them.
        self.schedule_waiting_pods(out);
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

    fn process_namespace_output(
        &mut self,
        namespace_id: NamespaceId,
        ns_out: NamespaceOutput,
        out: &mut OrchestratorOutput,
    ) {
        // Merge namespace output into top-level output.
        out.worker_commands
            .extend(ns_out.worker_commands.iter().cloned());
        out.timers_set.extend(ns_out.timers_set.iter().cloned());
        out.timers_cancel
            .extend(ns_out.timers_cancel.iter().cloned());

        let destroyed = ns_out.destroyed;
        let pod_requests = ns_out.pod_requests.clone();

        if ns_out != NamespaceOutput::default() {
            out.namespace_outputs
                .push((namespace_id.clone(), ns_out));
        }

        // Process pod scheduling requests from the namespace.
        for req in pod_requests {
            if let Some(worker_id) = self.select_worker_for_pod(&namespace_id) {
                let pod_id = self.gen_pod_id();
                if let Some(ns) = self.namespaces.get_mut(&namespace_id) {
                    let launch_out = ns.step(NamespaceInput::LaunchPod {
                        workload_id: req.workload_id,
                        worker_id,
                        pod_id,
                    });
                    // Recursively process outputs from LaunchPod (it won't emit more pod_requests).
                    out.worker_commands
                        .extend(launch_out.worker_commands.iter().cloned());
                    out.timers_set
                        .extend(launch_out.timers_set.iter().cloned());
                    out.timers_cancel
                        .extend(launch_out.timers_cancel.iter().cloned());
                    if launch_out != NamespaceOutput::default() {
                        out.namespace_outputs
                            .push((namespace_id.clone(), launch_out));
                    }
                }
            }
            // If no worker available, workload stays in WaitingForCapacity.
        }

        // If namespace is fully destroyed, remove it and clean up worker references.
        if destroyed {
            self.namespaces.remove(&namespace_id);
            for ws in self.workers.values_mut() {
                ws.namespaces.remove(&namespace_id);
            }
        }
    }

    fn schedule_waiting_pods(&mut self, out: &mut OrchestratorOutput) {
        // Collect (namespace_id, workload_id) pairs for workloads waiting for capacity.
        // Skip namespaces in Destroying state.
        let waiting: Vec<(NamespaceId, WorkloadId)> = self
            .namespaces
            .iter()
            .filter(|(_, ns)| ns.status != NamespaceStatus::Destroying)
            .flat_map(|(ns_id, ns)| {
                ns.workloads
                    .iter()
                    .filter(|(_, wl)| matches!(wl.state, WorkloadState::WaitingForCapacity))
                    .map(move |(wl_id, _)| (ns_id.clone(), wl_id.clone()))
            })
            .collect();

        for (ns_id, wl_id) in waiting {
            if let Some(worker_id) = self.select_worker_for_pod(&ns_id) {
                let pod_id = self.gen_pod_id();
                if let Some(ns) = self.namespaces.get_mut(&ns_id) {
                    let launch_out = ns.step(NamespaceInput::LaunchPod {
                        workload_id: wl_id,
                        worker_id,
                        pod_id,
                    });
                    out.worker_commands
                        .extend(launch_out.worker_commands.iter().cloned());
                    out.timers_set
                        .extend(launch_out.timers_set.iter().cloned());
                    out.timers_cancel
                        .extend(launch_out.timers_cancel.iter().cloned());
                    if launch_out != NamespaceOutput::default() {
                        out.namespace_outputs
                            .push((ns_id.clone(), launch_out));
                    }
                }
            }
        }
    }

    fn gen_pod_id(&mut self) -> PodId {
        let id = self.next_pod_id;
        self.next_pod_id += 1;
        PodId(format!("pod-{}", id))
    }

    fn select_worker_for_pod(&self, namespace_id: &NamespaceId) -> Option<WorkerId> {
        let ns = self.namespaces.get(namespace_id)?;
        ns.workers
            .iter()
            .find(|(_, ws)| ws.fabric_status == FabricStatus::Active)
            .map(|(wid, _)| wid.clone())
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
