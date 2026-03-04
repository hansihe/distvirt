use crate::service::ServiceInput;
use crate::types::*;
use crate::workload::WorkloadInput;

use super::NamespaceStateMachine;

impl NamespaceStateMachine {
    pub(super) fn handle_worker_event(
        &mut self,
        worker_id: &WorkerId,
        event: WorkerEvent,
        out: &mut NamespaceOutput,
    ) {
        if !self.workers.contains_key(worker_id) {
            return;
        }

        match event {
            WorkerEvent::NamespaceFailed { .. } => {
                // Treat like worker loss for this namespace.
                self.handle_worker_lost(worker_id, out);
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
                    self.handle_worker_lost(worker_id, out);
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
                        self.reconcile_all_services(out);
                    }
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
                    self.forward_service_outputs(&service_id.clone(), &wl_id, svc_outputs, out);
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
                    self.forward_service_outputs(&service_id.clone(), &wl_id, svc_outputs, out);
                }
            }
            WorkerEvent::PodRunning { pod_id } => {
                let pod_info = match self.pods.get(&pod_id) {
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
                self.forward_workload_outputs(&wl_id, wl_outputs, out);
            }
            WorkerEvent::PodExited { pod_id, exit_code } => {
                let pod_info = match self.remove_pod(&pod_id) {
                    Some(info) => info,
                    None => return,
                };
                let wl_id = pod_info.workload_id.clone();
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
                self.forward_workload_outputs(&wl_id, wl_outputs, out);
            }
            WorkerEvent::PodFailed { pod_id, error } => {
                let pod_info = match self.remove_pod(&pod_id) {
                    Some(info) => info,
                    None => return,
                };
                let wl_id = pod_info.workload_id.clone();
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
                self.forward_workload_outputs(&wl_id, wl_outputs, out);
            }
            WorkerEvent::PodSuspended { pod_id, snapshot_id } => {
                let pod_info = match self.remove_pod(&pod_id) {
                    Some(info) => info,
                    None => return,
                };
                let wl_id = pod_info.workload_id.clone();
                out.events.push(SmNamespaceEvent::Workload {
                    workload_id: wl_id.clone(),
                    event: SmWorkloadEvent::PodSuspended {
                        worker_id: pod_info.worker_id.clone(),
                        snapshot_id: snapshot_id.clone(),
                    },
                });
                let wl_outputs = if let Some(wl) = self.workloads.get_mut(&wl_id) {
                    wl.step(
                        WorkloadInput::PodSuspended {
                            pod_id,
                            snapshot_id,
                        },
                        &self.namespace_id,
                    )
                } else {
                    return;
                };
                self.forward_workload_outputs(&wl_id, wl_outputs, out);
            }
            WorkerEvent::PodSuspendFailed { pod_id, error } => {
                let pod_info = match self.remove_pod(&pod_id) {
                    Some(info) => info,
                    None => return,
                };
                let wl_id = pod_info.workload_id.clone();
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
                self.forward_workload_outputs(&wl_id, wl_outputs, out);
            }
        }
    }

    pub(super) fn handle_worker_lost(&mut self, worker_id: &WorkerId, out: &mut NamespaceOutput) {
        // Find all workloads affected by this worker loss.
        let affected_workloads: Vec<WorkloadId> = self
            .pods
            .iter()
            .filter(|(_, info)| info.worker_id == *worker_id)
            .map(|(_, info)| info.workload_id.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        // Remove pods on this worker.
        let lost_pods: Vec<PodId> = self
            .pods
            .iter()
            .filter(|(_, info)| info.worker_id == *worker_id)
            .map(|(pid, _)| pid.clone())
            .collect();
        for pod_id in &lost_pods {
            self.pods.remove(pod_id);
        }

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
            self.forward_workload_outputs(&wl_id, wl_outputs, out);
        }

        self.workers.remove(worker_id);

        if self.status == NamespaceStatus::Destroying {
            if self.workers.is_empty() {
                out.destroyed = true;
            }
        } else if self.workers.is_empty() && self.status == NamespaceStatus::Active {
            self.status = NamespaceStatus::Creating;
        } else if self.status == NamespaceStatus::Active {
            self.reconcile_all_services(out);
        }
    }

    pub(super) fn handle_timer_fired(&mut self, timer_key: &TimerKey, out: &mut NamespaceOutput) {
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
                    self.forward_service_outputs(&service_id, &wl_id, svc_outputs, out);
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
                    self.remove_pod(pod_id);
                }
                self.forward_workload_outputs(&workload_id, wl_outputs, out);
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
                self.remove_pod(&pod_id);
                self.forward_workload_outputs(&workload_id, wl_outputs, out);
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
                self.remove_pod(&pod_id);
                self.forward_workload_outputs(&workload_id, wl_outputs, out);
            }
        }
    }
}
