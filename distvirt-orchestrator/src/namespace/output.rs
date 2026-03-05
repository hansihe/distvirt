use crate::broadcast::broadcast_to_active_workers;
use crate::service::{ServiceInput, ServiceOutput};
use crate::types::*;
use crate::workload::{WorkloadInput, WorkloadOutput};

use super::NamespaceStateMachine;

impl NamespaceStateMachine {
    pub(crate) fn forward_service_outputs(
        &mut self,
        _service_id: &ServiceId,
        workload_id: &WorkloadId,
        outputs: Vec<ServiceOutput>,
        placement_table: &mut PlacementTable,
        out: &mut NamespaceOutput,
    ) {
        for svc_out in outputs {
            match svc_out {
                ServiceOutput::DemandUp | ServiceOutput::DemandDown => {
                    let wl_input = match svc_out {
                        ServiceOutput::DemandUp => WorkloadInput::DemandUp,
                        ServiceOutput::DemandDown => WorkloadInput::DemandDown,
                        _ => unreachable!(),
                    };
                    let wl_outputs = if let Some(wl) = self.workloads.get_mut(workload_id) {
                        wl.step(wl_input, &self.namespace_id)
                    } else {
                        continue;
                    };
                    if let Some(wl) = self.workloads.get(workload_id) {
                        out.events.push(SmNamespaceEvent::Workload {
                            workload_id: workload_id.clone(),
                            event: SmWorkloadEvent::DemandChanged {
                                demanding_services: wl.demand_count,
                            },
                        });
                    }
                    self.forward_workload_outputs(workload_id, wl_outputs, placement_table, out);
                }
                ServiceOutput::WorkerCommand(wid, cmd) => {
                    out.worker_commands.push((wid, cmd));
                }
                ServiceOutput::BroadcastWorkerCommand(cmd) => {
                    broadcast_to_active_workers(&self.workers, out, |_| cmd.clone());
                }
                ServiceOutput::TimerSet(key, duration) => {
                    out.timers_set.push((key, duration));
                }
                ServiceOutput::TimerCancel(key) => {
                    out.timers_cancel.push(key);
                }
            }
        }
    }

    pub(crate) fn forward_workload_outputs(
        &mut self,
        workload_id: &WorkloadId,
        outputs: Vec<WorkloadOutput>,
        placement_table: &mut PlacementTable,
        out: &mut NamespaceOutput,
    ) {
        for wl_out in outputs {
            match wl_out {
                WorkloadOutput::PodRequest => {
                    out.pod_requests.push(PodRequest {
                        workload_id: workload_id.clone(),
                    });
                }
                WorkloadOutput::BecameReady { pod_id, worker_id } => {
                    // Emit backend ready for all services on this workload.
                    let svc_ids: Vec<(ServiceId, WorkloadId)> = self
                        .service_workload
                        .iter()
                        .filter(|(_, wl_id)| *wl_id == workload_id)
                        .map(|(sid, wlid)| (sid.clone(), wlid.clone()))
                        .collect();
                    for (sid, wlid) in &svc_ids {
                        out.events.push(SmNamespaceEvent::Service {
                            service_id: sid.clone(),
                            workload_id: wlid.clone(),
                            event: SmServiceEvent::BackendReady,
                        });
                    }

                    // Construct ServiceBackend from workload's PodNetworkConfig.
                    let backend = self.spec.workloads.get(workload_id).map(|wl_spec| {
                        ServiceBackend {
                            pod_ip: wl_spec.network.ip,
                        }
                    });
                    let backend = match backend {
                        Some(b) => b,
                        None => continue,
                    };

                    // Forward to all services mapped to this workload.
                    let svc_ids: Vec<ServiceId> = self
                        .service_workload
                        .iter()
                        .filter(|(_, wl_id)| *wl_id == workload_id)
                        .map(|(sid, _)| sid.clone())
                        .collect();
                    for sid in svc_ids {
                        if let Some(svc) = self.services.get_mut(&sid) {
                            let svc_outputs = svc.step(
                                ServiceInput::WorkloadReady {
                                    pod_id: pod_id.clone(),
                                    worker_id: worker_id.clone(),
                                    backend: backend.clone(),
                                },
                                &self.namespace_id,
                            );
                            // Forward service outputs (but don't recurse into workload
                            // since BecameReady shouldn't trigger DemandUp/Down).
                            for svc_out in svc_outputs {
                                match svc_out {
                                    ServiceOutput::BroadcastWorkerCommand(cmd) => {
                                        for wid in self.active_worker_ids() {
                                            out.worker_commands.push((wid, cmd.clone()));
                                        }
                                    }
                                    ServiceOutput::WorkerCommand(wid, cmd) => {
                                        out.worker_commands.push((wid, cmd));
                                    }
                                    ServiceOutput::TimerSet(key, duration) => {
                                        out.timers_set.push((key, duration));
                                    }
                                    ServiceOutput::TimerCancel(key) => {
                                        out.timers_cancel.push(key);
                                    }
                                    ServiceOutput::DemandUp | ServiceOutput::DemandDown => {
                                        // Should not happen in response to BecameReady.
                                    }
                                }
                            }
                        }
                    }
                }
                WorkloadOutput::BecameUnready => {
                    // Forward to all services mapped to this workload.
                    let svc_ids: Vec<ServiceId> = self
                        .service_workload
                        .iter()
                        .filter(|(_, wl_id)| *wl_id == workload_id)
                        .map(|(sid, _)| sid.clone())
                        .collect();
                    for sid in svc_ids {
                        if let Some(svc) = self.services.get_mut(&sid) {
                            let svc_outputs = svc.step(
                                ServiceInput::WorkloadUnready,
                                &self.namespace_id,
                            );
                            let wl_id = svc.workload_id.clone();
                            // Need to handle DemandUp from always-on services.
                            self.forward_service_outputs(&sid, &wl_id, svc_outputs, placement_table, out);
                        }
                    }
                }
                WorkloadOutput::SuspendRequest { worker_id, artifact_id } => {
                    // pod_id must exist: SuspendRequest is only emitted when
                    // the workload transitions to Suspending { pod_id, .. }.
                    let pod_id = self
                        .workloads
                        .get(workload_id)
                        .and_then(|wl| wl.state.pod_id().cloned())
                        .expect("invariant: workload must be in Suspending state when SuspendRequest is emitted");

                    // Resolve pool_id from the worker's primary pool.
                    // If the worker has no pool, we cannot suspend — feed failure
                    // back to the workload SM so it recovers gracefully.
                    let pool_id = match self
                        .workers
                        .get(&worker_id)
                        .and_then(|ws| ws.primary_pool_id.clone())
                    {
                        Some(id) => id,
                        None => {
                            out.events.push(SmNamespaceEvent::Workload {
                                workload_id: workload_id.clone(),
                                event: SmWorkloadEvent::PodSuspendFailed {
                                    reason: "worker has no storage pool".into(),
                                },
                            });
                            let wl_outputs = if let Some(wl) = self.workloads.get_mut(workload_id) {
                                wl.step(
                                    WorkloadInput::PodSuspendFailed { pod_id },
                                    &self.namespace_id,
                                )
                            } else {
                                continue;
                            };
                            let wl_id = workload_id.clone();
                            self.forward_workload_outputs(&wl_id, wl_outputs, placement_table, out);
                            continue;
                        }
                    };

                    // Placement is created when ArtifactWriteStarted arrives from the worker.
                    out.worker_commands.push((
                        worker_id,
                        WorkerCommand::SuspendPod {
                            namespace_id: self.namespace_id.clone(),
                            pod_id,
                            artifact_id,
                            pool_id,
                        },
                    ));
                }
                WorkloadOutput::ResumeRequest { artifact_id } => {
                    out.resume_requests.push(ResumeRequest {
                        workload_id: workload_id.clone(),
                        artifact_id,
                    });
                }
                WorkloadOutput::DeleteArtifact { artifact_id } => {
                    // Look up placement and emit DeleteArtifact to correct worker.
                    if let Some(placement) = placement_table.remove(&artifact_id) {
                        out.worker_commands.push((
                            placement.worker_id,
                            WorkerCommand::DeleteArtifact {
                                artifact_id,
                                pool_id: placement.pool_id,
                            },
                        ));
                    }
                }
                WorkloadOutput::WorkerCommand(wid, cmd) => {
                    out.worker_commands.push((wid, cmd));
                }
                WorkloadOutput::TimerSet(key, duration) => {
                    out.timers_set.push((key, duration));
                }
                WorkloadOutput::TimerCancel(key) => {
                    out.timers_cancel.push(key);
                }
                WorkloadOutput::ConditionSet { key, message } => {
                    log::debug!(
                        "workload {:?} condition set: {} = {}",
                        workload_id, key, message,
                    );
                }
                WorkloadOutput::ConditionClear { key } => {
                    log::debug!(
                        "workload {:?} condition clear: {}",
                        workload_id, key,
                    );
                }
            }
        }
    }
}
