use std::collections::VecDeque;

use crate::broadcast::broadcast_to_active_workers;
use crate::service::{ServiceInput, ServiceOutput};
use crate::types::*;
use crate::workload::{WorkloadInput, WorkloadOutput};

use super::NamespaceStateMachine;

/// Pending work items for the output processing loop.
pub(crate) enum PendingOutput {
    Workload {
        workload_id: WorkloadId,
        outputs: Vec<WorkloadOutput>,
    },
    Service {
        service_id: ServiceId,
        workload_id: WorkloadId,
        outputs: Vec<ServiceOutput>,
    },
}

impl NamespaceStateMachine {
    /// Process outputs from workload/service state machines using a queue to avoid
    /// recursive calls between forward_workload_outputs and forward_service_outputs.
    pub(crate) fn process_outputs(
        &mut self,
        initial: PendingOutput,
        placement_table: &mut PlacementTable,
        out: &mut NamespaceOutput,
    ) {
        let mut queue: VecDeque<PendingOutput> = VecDeque::new();
        queue.push_back(initial);
        let mut iterations = 0;
        const MAX_ITERATIONS: usize = 100;

        while let Some(pending) = queue.pop_front() {
            iterations += 1;
            assert!(
                iterations <= MAX_ITERATIONS,
                "output processing loop exceeded iteration cap"
            );

            match pending {
                PendingOutput::Workload {
                    workload_id,
                    outputs,
                } => {
                    self.translate_workload_outputs(
                        &workload_id,
                        outputs,
                        placement_table,
                        out,
                        &mut queue,
                    );
                }
                PendingOutput::Service {
                    service_id,
                    workload_id,
                    outputs,
                } => {
                    self.translate_service_outputs(
                        &service_id,
                        &workload_id,
                        outputs,
                        placement_table,
                        out,
                        &mut queue,
                    );
                }
            }
        }
    }

    /// Translate service outputs. Services no longer emit demand events;
    /// only pass-through for WorkerCommand, BroadcastWorkerCommand, TimerSet, TimerCancel.
    fn translate_service_outputs(
        &mut self,
        _service_id: &ServiceId,
        _workload_id: &WorkloadId,
        outputs: Vec<ServiceOutput>,
        _placement_table: &mut PlacementTable,
        out: &mut NamespaceOutput,
        _queue: &mut VecDeque<PendingOutput>,
    ) {
        for svc_out in outputs {
            match svc_out {
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

    /// Translate workload outputs. BecameReady/BecameUnready are forwarded to services.
    fn translate_workload_outputs(
        &mut self,
        workload_id: &WorkloadId,
        outputs: Vec<WorkloadOutput>,
        placement_table: &mut PlacementTable,
        out: &mut NamespaceOutput,
        queue: &mut VecDeque<PendingOutput>,
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

                    // Forward WorkloadReady to all services mapped to this workload.
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
                            if !svc_outputs.is_empty() {
                                queue.push_back(PendingOutput::Service {
                                    service_id: sid.clone(),
                                    workload_id: workload_id.clone(),
                                    outputs: svc_outputs,
                                });
                            }
                        }
                    }
                }
                WorkloadOutput::BecameUnready => {
                    // Check if the workload is retrying (RetryBackoff or WaitingForCapacity).
                    let is_retrying = self
                        .workloads
                        .get(workload_id)
                        .map(|wl| {
                            matches!(
                                wl.state,
                                WorkloadState::RetryBackoff { .. }
                                    | WorkloadState::WaitingForCapacity
                            )
                        })
                        .unwrap_or(false);

                    // Forward WorkloadUnready to all services mapped to this workload.
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
                            if !svc_outputs.is_empty() {
                                queue.push_back(PendingOutput::Service {
                                    service_id: sid.clone(),
                                    workload_id: wl_id.clone(),
                                    outputs: svc_outputs,
                                });
                            }
                            // During retry, re-activate activation services that went Idle
                            // so they transition Idle → NeedBackend and preserve demand
                            // through reconciliation (wants_backend() stays true).
                            if is_retrying
                                && svc.has_activation
                                && matches!(svc.state, ServiceState::Idle)
                            {
                                let reactivate_outputs = svc.step(
                                    ServiceInput::ServiceActivation,
                                    &self.namespace_id,
                                );
                                if !reactivate_outputs.is_empty() {
                                    queue.push_back(PendingOutput::Service {
                                        service_id: sid.clone(),
                                        workload_id: wl_id,
                                        outputs: reactivate_outputs,
                                    });
                                }
                            }
                        }
                    }
                }
                WorkloadOutput::SuspendRequest {
                    worker_id,
                    artifact_id,
                } => {
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
                            let wl_outputs =
                                if let Some(wl) = self.workloads.get_mut(workload_id) {
                                    wl.step(
                                        WorkloadInput::PodSuspendFailed { pod_id },
                                        &self.namespace_id,
                                    )
                                } else {
                                    continue;
                                };
                            queue.push_back(PendingOutput::Workload {
                                workload_id: workload_id.clone(),
                                outputs: wl_outputs,
                            });
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
                        workload_id,
                        key,
                        message,
                    );
                }
                WorkloadOutput::ConditionClear { key } => {
                    log::debug!(
                        "workload {:?} condition clear: {}",
                        workload_id,
                        key,
                    );
                }
            }
        }
    }
}
