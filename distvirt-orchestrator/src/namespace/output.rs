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
    /// This ensures all outputs from one step are fully collected before processing
    /// side effects, preventing bugs where DemandDown zeroes demand_count before
    /// retry logic runs.
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

    /// Non-recursive version of the old `forward_service_outputs`.
    /// Instead of calling forward_workload_outputs directly, pushes to the queue.
    fn translate_service_outputs(
        &mut self,
        service_id: &ServiceId,
        workload_id: &WorkloadId,
        outputs: Vec<ServiceOutput>,
        _placement_table: &mut PlacementTable,
        out: &mut NamespaceOutput,
        queue: &mut VecDeque<PendingOutput>,
    ) {
        for svc_out in outputs {
            match svc_out {
                // Services are the canonical demand holders — see demand model invariants
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

                    // Fix 3: Late-joiner WorkloadReady
                    // When DemandUp arrives on an already-Running workload, the workload SM
                    // doesn't emit BecameReady. Notify the originating service directly.
                    if matches!(svc_out, ServiceOutput::DemandUp) {
                        if let Some(wl) = self.workloads.get(workload_id) {
                            if let WorkloadState::Running { ref pod_id, ref worker_id, .. } = wl.state {
                                if let Some(wl_spec) = self.spec.workloads.get(workload_id) {
                                    let backend = ServiceBackend {
                                        pod_ip: wl_spec.network.ip,
                                    };
                                    let pod_id = pod_id.clone();
                                    let worker_id = worker_id.clone();
                                    if let Some(svc) = self.services.get_mut(service_id) {
                                        let svc_outputs = svc.step(
                                            ServiceInput::WorkloadReady {
                                                pod_id,
                                                worker_id,
                                                backend,
                                            },
                                            &self.namespace_id,
                                        );
                                        // Filter out DemandUp/DemandDown (same as BecameReady handling)
                                        let filtered: Vec<ServiceOutput> = svc_outputs
                                            .into_iter()
                                            .filter(|o| {
                                                !matches!(
                                                    o,
                                                    ServiceOutput::DemandUp
                                                        | ServiceOutput::DemandDown
                                                )
                                            })
                                            .collect();
                                        if !filtered.is_empty() {
                                            queue.push_back(PendingOutput::Service {
                                                service_id: service_id.clone(),
                                                workload_id: workload_id.clone(),
                                                outputs: filtered,
                                            });
                                        }
                                    }
                                }
                            }
                        }
                    }

                    queue.push_back(PendingOutput::Workload {
                        workload_id: workload_id.clone(),
                        outputs: wl_outputs,
                    });
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

    /// Non-recursive version of the old `forward_workload_outputs`.
    /// Instead of calling forward_service_outputs directly, pushes to the queue.
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
                            // Filter out DemandUp/DemandDown from BecameReady responses.
                            let filtered: Vec<ServiceOutput> = svc_outputs
                                .into_iter()
                                .filter(|o| {
                                    !matches!(
                                        o,
                                        ServiceOutput::DemandUp | ServiceOutput::DemandDown
                                    )
                                })
                                .collect();
                            if !filtered.is_empty() {
                                queue.push_back(PendingOutput::Service {
                                    service_id: sid.clone(),
                                    workload_id: workload_id.clone(),
                                    outputs: filtered,
                                });
                            }
                        }
                    }
                }
                WorkloadOutput::BecameUnready => {
                    // Check if the workload is retrying (RetryBackoff or WaitingForCapacity).
                    // If so, preserve demand: filter out DemandDown from service responses
                    // and re-activate activation services so they wait for the workload to
                    // recover instead of dropping demand.
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

                            if is_retrying {
                                // Workload is retrying — preserve demand.
                                // Filter DemandDown from service outputs so the workload's
                                // demand_count stays correct.
                                let filtered: Vec<ServiceOutput> = svc_outputs
                                    .into_iter()
                                    .filter(|o| {
                                        !matches!(
                                            o,
                                            ServiceOutput::DemandUp | ServiceOutput::DemandDown
                                        )
                                    })
                                    .collect();
                                if !filtered.is_empty() {
                                    queue.push_back(PendingOutput::Service {
                                        service_id: sid.clone(),
                                        workload_id: wl_id.clone(),
                                        outputs: filtered,
                                    });
                                }
                                // The service internally went Idle (activation) or stayed
                                // NeedBackend (always-on). For activation services, re-activate
                                // so they transition Idle → NeedBackend and wait for recovery.
                                if svc.has_activation
                                    && matches!(svc.state, ServiceState::Idle)
                                {
                                    let reactivate_outputs = svc.step(
                                        ServiceInput::ServiceActivation,
                                        &self.namespace_id,
                                    );
                                    // Filter out DemandUp — demand is already counted.
                                    let filtered: Vec<ServiceOutput> = reactivate_outputs
                                        .into_iter()
                                        .filter(|o| {
                                            !matches!(
                                                o,
                                                ServiceOutput::DemandUp | ServiceOutput::DemandDown
                                            )
                                        })
                                        .collect();
                                    if !filtered.is_empty() {
                                        queue.push_back(PendingOutput::Service {
                                            service_id: sid.clone(),
                                            workload_id: wl_id,
                                            outputs: filtered,
                                        });
                                    }
                                }
                            } else {
                                queue.push_back(PendingOutput::Service {
                                    service_id: sid.clone(),
                                    workload_id: wl_id,
                                    outputs: svc_outputs,
                                });
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
