use crate::service::ServiceInput;
use crate::types::*;
use crate::workload::WorkloadInput;

use super::NamespaceStateMachine;

impl NamespaceStateMachine {
    pub(super) fn handle_worker_event(
        &mut self,
        worker_id: &WorkerId,
        event: WorkerEvent,
        placement_table: &mut PlacementTable,
        out: &mut NamespaceOutput,
    ) {
        if !self.workers.contains_key(worker_id) {
            return;
        }

        match event {
            WorkerEvent::NamespaceFailed { .. } => {
                // Treat like worker loss for this namespace.
                self.handle_worker_lost(worker_id, placement_table, out);
                return;
            }
            WorkerEvent::NamespaceDestroyed => {
                if self.status == NamespaceStatus::Destroying {
                    self.workers.remove(worker_id);
                    if self.workers.is_empty() {
                        out.destroyed = true;
                    }
                } else {
                    // Unexpected: worker destroyed namespace without being asked.
                    self.handle_worker_lost(worker_id, placement_table, out);
                }
                return;
            }
            _ if self.status == NamespaceStatus::Destroying => {
                return;
            }
            WorkerEvent::NamespaceCreated => {
                if let Some(ws) = self.workers.get_mut(worker_id)
                    && ws.fabric_status == FabricStatus::Creating
                {
                    ws.fabric_status = FabricStatus::Active;
                }
                if self.status == NamespaceStatus::Creating {
                    let has_active = self
                        .workers
                        .values()
                        .any(|ws| ws.fabric_status == FabricStatus::Active);
                    if has_active {
                        self.status = NamespaceStatus::Active;
                        self.emit_registry_sync(out);
                        self.emit_fabric_route_sync(out);
                        self.reconcile_all_services(placement_table, out);
                    }
                } else if self.status == NamespaceStatus::Active {
                    // Second+ worker joining an already-active namespace.
                    self.emit_registry_sync_to_worker(worker_id, out);
                    self.emit_fabric_route_sync_to_worker(worker_id, out);
                }
            }
            WorkerEvent::ServiceActivation { service_id } => {
                if let Some(svc) = self.services.get_mut(&service_id) {
                    let wl_id = svc.workload_id.clone();
                    out.events.push(SmNamespaceEvent::Service {
                        service_id: service_id.clone(),
                        workload_id: wl_id.clone(),
                        event: SmServiceEvent::Activated {
                            trigger: ServiceActivationTrigger::Traffic,
                        },
                    });
                    let svc_outputs =
                        svc.step(ServiceInput::ServiceActivation, &self.namespace_id);
                    self.forward_service_outputs(&service_id.clone(), &wl_id, svc_outputs, placement_table, out);
                }
            }
            WorkerEvent::ServiceBackendNeed { service_id, need } => {
                if let Some(svc) = self.services.get_mut(&service_id) {
                    let wl_id = svc.workload_id.clone();
                    // Emit idle timer events based on need transitions.
                    match &need {
                        BackendNeed::None => {
                            if svc.has_activation {
                                if let ServiceState::Active { idle_timer: None, .. } = &svc.state {
                                    out.events.push(SmNamespaceEvent::Service {
                                        service_id: service_id.clone(),
                                        workload_id: wl_id.clone(),
                                        event: SmServiceEvent::IdleTimerStarted {
                                            timeout: svc.idle_timeout,
                                        },
                                    });
                                }
                            }
                        }
                        BackendNeed::Traffic | BackendNeed::Active => {
                            if let ServiceState::Active { idle_timer: Some(_), .. } = &svc.state {
                                out.events.push(SmNamespaceEvent::Service {
                                    service_id: service_id.clone(),
                                    workload_id: wl_id.clone(),
                                    event: SmServiceEvent::IdleTimerCancelled {
                                        reason: IdleTimerCancelReason::NewTraffic,
                                    },
                                });
                            }
                        }
                    }
                    let svc_outputs = svc.step(
                        ServiceInput::ServiceBackendNeed { need },
                        &self.namespace_id,
                    );
                    self.forward_service_outputs(&service_id.clone(), &wl_id, svc_outputs, placement_table, out);
                }
            }
            WorkerEvent::PodRunning { pod_id } => {
                let pod_info = match self.pod_map.get(&pod_id) {
                    Some(info) => info.clone(),
                    None => return,
                };
                let wl_id = pod_info.workload_id.clone();
                out.events.push(SmNamespaceEvent::Workload {
                    workload_id: wl_id.clone(),
                    event: SmWorkloadEvent::PodRunning {
                        pod_id: pod_id.clone(),
                        worker_id: pod_info.worker_id.clone(),
                    },
                });
                let wl_outputs = if let Some(wl) = self.workloads.get_mut(&wl_id) {
                    wl.step(
                        WorkloadInput::PodRunning {
                            pod_id: pod_id.clone(),
                        },
                        &self.namespace_id,
                    )
                } else {
                    return;
                };
                self.forward_workload_outputs(&wl_id, wl_outputs, placement_table, out);
            }
            WorkerEvent::PodExited { pod_id, exit_code } => {
                let pod_info = match self.pod_map.remove(&pod_id) {
                    Some(info) => info,
                    None => return,
                };
                let wl_id = pod_info.workload_id.clone();
                // Remove fabric route if multi-worker.
                if self.workers.len() > 1 {
                    if let Some(wl_spec) = self.spec.workloads.get(&wl_id) {
                        self.emit_fabric_route_remove(wl_spec.network.ip, out);
                    }
                }
                out.events.push(SmNamespaceEvent::Workload {
                    workload_id: wl_id.clone(),
                    event: SmWorkloadEvent::PodStopped {
                        exit_code,
                    },
                });
                let wl_outputs = if let Some(wl) = self.workloads.get_mut(&wl_id) {
                    wl.step(
                        WorkloadInput::PodGone {
                            pod_id: pod_id.clone(),
                        },
                        &self.namespace_id,
                    )
                } else {
                    return;
                };
                self.forward_workload_outputs(&wl_id, wl_outputs, placement_table, out);
            }
            WorkerEvent::PodFailed { pod_id, error } => {
                let pod_info = match self.pod_map.remove(&pod_id) {
                    Some(info) => info,
                    None => return,
                };
                let wl_id = pod_info.workload_id.clone();
                // Remove fabric route if multi-worker.
                if self.workers.len() > 1 {
                    if let Some(wl_spec) = self.spec.workloads.get(&wl_id) {
                        self.emit_fabric_route_remove(wl_spec.network.ip, out);
                    }
                }
                out.events.push(SmNamespaceEvent::Workload {
                    workload_id: wl_id.clone(),
                    event: SmWorkloadEvent::PodFailed {
                        reason: error,
                    },
                });
                let wl_outputs = if let Some(wl) = self.workloads.get_mut(&wl_id) {
                    wl.step(
                        WorkloadInput::PodGone {
                            pod_id: pod_id.clone(),
                        },
                        &self.namespace_id,
                    )
                } else {
                    return;
                };
                self.forward_workload_outputs(&wl_id, wl_outputs, placement_table, out);
            }
            WorkerEvent::PodSuspended { pod_id, artifact_id, pool_id } => {
                let pod_info = match self.pod_map.remove(&pod_id) {
                    Some(info) => info,
                    None => return,
                };
                let wl_id = pod_info.workload_id.clone();
                // Remove fabric route if multi-worker (pod no longer running).
                if self.workers.len() > 1 {
                    if let Some(wl_spec) = self.spec.workloads.get(&wl_id) {
                        self.emit_fabric_route_remove(wl_spec.network.ip, out);
                    }
                }
                // Update placement table with actual pool_id from worker.
                placement_table.insert(
                    artifact_id.clone(),
                    ArtifactPlacement {
                        pool_id,
                        worker_id: pod_info.worker_id.clone(),
                        locked_by: None,
                    },
                );
                out.events.push(SmNamespaceEvent::Workload {
                    workload_id: wl_id.clone(),
                    event: SmWorkloadEvent::PodSuspended {
                        worker_id: pod_info.worker_id.clone(),
                        artifact_id: artifact_id.clone(),
                    },
                });
                let wl_outputs = if let Some(wl) = self.workloads.get_mut(&wl_id) {
                    wl.step(
                        WorkloadInput::PodSuspended {
                            pod_id,
                            artifact_id,
                        },
                        &self.namespace_id,
                    )
                } else {
                    return;
                };
                self.forward_workload_outputs(&wl_id, wl_outputs, placement_table, out);
            }
            WorkerEvent::FabricRouteMiss { dst_ip } => {
                // Look up workload whose network.ip == dst_ip.
                let wl_match = self
                    .spec
                    .workloads
                    .iter()
                    .find(|(_, wl_spec)| wl_spec.network.ip == dst_ip)
                    .map(|(wl_id, _)| wl_id.clone());
                if let Some(wl_id) = wl_match {
                    if let Some(wl) = self.workloads.get(&wl_id) {
                        match &wl.state {
                            WorkloadState::Dormant | WorkloadState::Suspended { .. } => {
                                // Trigger demand up (wake on traffic).
                                let wl = self.workloads.get_mut(&wl_id)
                                    .expect("invariant: wl_id matched from spec must exist in workloads");
                                let wl_outputs = wl.step(
                                    crate::workload::WorkloadInput::DemandUp,
                                    &self.namespace_id,
                                );
                                self.forward_workload_outputs(&wl_id, wl_outputs, placement_table, out);
                            }
                            _ => {
                                // Transient miss during pod startup, ignore.
                            }
                        }
                    }
                }
            }
            WorkerEvent::PodSuspendFailed { pod_id, error } => {
                let pod_info = match self.pod_map.remove(&pod_id) {
                    Some(info) => info,
                    None => return,
                };
                let wl_id = pod_info.workload_id.clone();
                // Remove fabric route if multi-worker.
                if self.workers.len() > 1 {
                    if let Some(wl_spec) = self.spec.workloads.get(&wl_id) {
                        self.emit_fabric_route_remove(wl_spec.network.ip, out);
                    }
                }
                out.events.push(SmNamespaceEvent::Workload {
                    workload_id: wl_id.clone(),
                    event: SmWorkloadEvent::PodSuspendFailed {
                        reason: error,
                    },
                });
                let wl_outputs = if let Some(wl) = self.workloads.get_mut(&wl_id) {
                    wl.step(
                        WorkloadInput::PodSuspendFailed {
                            pod_id,
                        },
                        &self.namespace_id,
                    )
                } else {
                    return;
                };
                self.forward_workload_outputs(&wl_id, wl_outputs, placement_table, out);
            }
        }
    }

    pub(super) fn handle_worker_lost(&mut self, worker_id: &WorkerId, placement_table: &mut PlacementTable, out: &mut NamespaceOutput) {
        // Remove all artifacts placed on this worker.
        placement_table.remove_by_worker(worker_id);
        // Remove all pods on this worker and collect affected workloads.
        let _lost_pods = self.pod_map.remove_worker_pods(worker_id);
        let affected_workloads: Vec<WorkloadId> = {
            // We already removed the pods, but we need the workload IDs.
            // Collect unique workload IDs from workloads that reference this worker.
            self.workloads
                .iter()
                .filter(|(_, wl)| wl.state.worker_id() == Some(worker_id))
                .map(|(wl_id, _)| wl_id.clone())
                .collect()
        };

        // Forward WorkerLost to affected workloads.
        for wl_id in affected_workloads {
            let wl_outputs = if let Some(wl) = self.workloads.get_mut(&wl_id) {
                wl.step(
                    WorkloadInput::WorkerLost {
                        worker_id: worker_id.clone(),
                    },
                    &self.namespace_id,
                )
            } else {
                continue;
            };
            self.forward_workload_outputs(&wl_id, wl_outputs, placement_table, out);
        }

        self.workers.remove(worker_id);

        if self.status == NamespaceStatus::Destroying {
            if self.workers.is_empty() {
                out.destroyed = true;
            }
        } else if self.workers.is_empty() && self.status == NamespaceStatus::Active {
            self.status = NamespaceStatus::Creating;
        } else if self.status == NamespaceStatus::Active {
            self.reconcile_all_services(placement_table, out);
        }
    }

    pub(super) fn handle_timer_fired(&mut self, timer_key: &TimerKey, placement_table: &mut PlacementTable, out: &mut NamespaceOutput) {
        match timer_key {
            TimerKey::IdleTimeout { service_id } => {
                let service_id = service_id.clone();
                if let Some(svc) = self.services.get_mut(&service_id) {
                    let wl_id = svc.workload_id.clone();
                    // Check if this timer fire will cause deactivation.
                    if let ServiceState::Active { ref idle_timer, ref backend_need, .. } = svc.state {
                        if idle_timer.as_ref() == Some(timer_key)
                            && *backend_need == BackendNeed::None
                            && svc.has_activation
                        {
                            out.events.push(SmNamespaceEvent::Service {
                                service_id: service_id.clone(),
                                workload_id: wl_id.clone(),
                                event: SmServiceEvent::IdleTimeoutFired,
                            });
                            out.events.push(SmNamespaceEvent::Service {
                                service_id: service_id.clone(),
                                workload_id: wl_id.clone(),
                                event: SmServiceEvent::Deactivated {
                                    reason: ServiceDeactivationReason::IdleTimeout,
                                },
                            });
                        }
                    }
                    let svc_outputs = svc.step(
                        ServiceInput::TimerFired {
                            timer_key: timer_key.clone(),
                        },
                        &self.namespace_id,
                    );
                    self.forward_service_outputs(&service_id, &wl_id, svc_outputs, placement_table, out);
                }
            }
            TimerKey::LaunchTimeout {
                workload_id,
                ..
            } => {
                let workload_id = workload_id.clone();
                let wl_outputs = if let Some(wl) = self.workloads.get_mut(&workload_id) {
                    wl.step(
                        WorkloadInput::TimerFired {
                            timer_key: timer_key.clone(),
                        },
                        &self.namespace_id,
                    )
                } else {
                    return;
                };
                // Clean up pod from pods map.
                if let TimerKey::LaunchTimeout { pod_id, .. } = timer_key {
                    self.pod_map.remove(pod_id);
                }
                self.forward_workload_outputs(&workload_id, wl_outputs, placement_table, out);
            }
            TimerKey::SuspendTimeout {
                workload_id,
                pod_id,
            } => {
                let workload_id = workload_id.clone();
                let pod_id = pod_id.clone();
                let wl_outputs = if let Some(wl) = self.workloads.get_mut(&workload_id) {
                    wl.step(
                        WorkloadInput::TimerFired {
                            timer_key: timer_key.clone(),
                        },
                        &self.namespace_id,
                    )
                } else {
                    return;
                };
                // Clean up pod from pods map on timeout.
                self.pod_map.remove(&pod_id);
                self.forward_workload_outputs(&workload_id, wl_outputs, placement_table, out);
            }
            TimerKey::ResumeTimeout {
                workload_id,
                pod_id,
            } => {
                let workload_id = workload_id.clone();
                let pod_id = pod_id.clone();
                let wl_outputs = if let Some(wl) = self.workloads.get_mut(&workload_id) {
                    wl.step(
                        WorkloadInput::TimerFired {
                            timer_key: timer_key.clone(),
                        },
                        &self.namespace_id,
                    )
                } else {
                    return;
                };
                // Clean up pod from pods map on timeout.
                self.pod_map.remove(&pod_id);
                self.forward_workload_outputs(&workload_id, wl_outputs, placement_table, out);
            }
        }
    }
}
