use crate::types::*;

use super::Orchestrator;

impl Orchestrator {
    pub(crate) fn handle_client_command(
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
                let ns = crate::namespace::NamespaceStateMachine::new(namespace_id.clone(), spec);
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
                    .values()
                    .map(|ns| ns.status_report())
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
            ClientCommand::ListWorkers => {
                let workers = self
                    .workers
                    .iter()
                    .map(|(worker_id, ws)| {
                        let active_pods: u32 = self
                            .namespaces
                            .values()
                            .flat_map(|ns| ns.pods.values())
                            .filter(|p| p.worker_id == *worker_id)
                            .count() as u32;
                        WorkerStatusReport {
                            worker_id: worker_id.clone(),
                            max_pods: ws.capabilities.max_pods,
                            available_memory_mb: ws.capabilities.available_memory_mb,
                            active_pods,
                        }
                    })
                    .collect();
                out.client_events
                    .push((client_id, ClientEvent::WorkerList { workers }));
            }
            ClientCommand::GetWorker { worker_id } => {
                if let Some(ws) = self.workers.get(&worker_id) {
                    let active_pods: u32 = self
                        .namespaces
                        .values()
                        .flat_map(|ns| ns.pods.values())
                        .filter(|p| p.worker_id == worker_id)
                        .count() as u32;
                    out.client_events.push((
                        client_id,
                        ClientEvent::WorkerStatus {
                            worker: WorkerStatusReport {
                                worker_id,
                                max_pods: ws.capabilities.max_pods,
                                available_memory_mb: ws.capabilities.available_memory_mb,
                                active_pods,
                            },
                        },
                    ));
                } else {
                    out.client_events.push((
                        client_id,
                        ClientEvent::Error {
                            message: format!("worker '{}' not found", worker_id.0),
                        },
                    ));
                }
            }
            ClientCommand::ListPods { namespace_id } => {
                if let Some(ns) = self.namespaces.get(&namespace_id) {
                    let pods = ns
                        .pods
                        .iter()
                        .map(|(pod_id, info)| {
                            let is_running = ns
                                .workloads
                                .get(&info.workload_id)
                                .map_or(false, |wl| match &wl.state {
                                    WorkloadState::Running {
                                        pod_id: running_pod, ..
                                    } => running_pod == pod_id,
                                    _ => false,
                                });
                            let state = if is_running {
                                PodStatus::Running
                            } else {
                                PodStatus::Launching
                            };
                            let (ip, mac) = ns.spec.workloads.get(&info.workload_id)
                                .map(|wl| {
                                    let mac_bytes = wl.network.mac;
                                    let mac_str = format!(
                                        "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
                                        mac_bytes[0], mac_bytes[1], mac_bytes[2],
                                        mac_bytes[3], mac_bytes[4], mac_bytes[5],
                                    );
                                    (wl.network.ip.to_string(), mac_str)
                                })
                                .unwrap_or_default();
                            PodStatusReport {
                                pod_id: pod_id.clone(),
                                workload_id: info.workload_id.clone(),
                                worker_id: info.worker_id.clone(),
                                ip,
                                mac,
                                state,
                            }
                        })
                        .collect();
                    out.client_events
                        .push((client_id, ClientEvent::PodList { pods }));
                } else {
                    out.client_events.push((
                        client_id,
                        ClientEvent::Error {
                            message: format!("namespace '{}' not found", namespace_id.0),
                        },
                    ));
                }
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
            ClientCommand::Connect {
                namespace_id,
                client_public_key,
            } => {
                self.handle_connect(client_id, namespace_id, client_public_key, out);
            }
            ClientCommand::Disconnect {
                namespace_id,
                client_public_key,
            } => {
                self.route_namespace_input(
                    namespace_id,
                    NamespaceInput::Disconnect {
                        client_id,
                        client_public_key,
                    },
                    out,
                );
            }
            ClientCommand::DeactivateWorkload {
                namespace_id,
                workload_id,
            } => {
                self.route_namespace_input(
                    namespace_id,
                    NamespaceInput::DeactivateWorkload {
                        client_id,
                        workload_id,
                    },
                    out,
                );
            }
        }
    }
}
