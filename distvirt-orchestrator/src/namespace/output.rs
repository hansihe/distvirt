use crate::service::ServiceOutput;
use crate::types::*;
use crate::workload::{WorkloadInput, WorkloadOutput};

use super::NamespaceStateMachine;

impl NamespaceStateMachine {
    /// Translate workload outputs into namespace-level actions.
    ///
    /// Handles the SuspendRequest cascade: if a pool lookup fails,
    /// PodSuspendFailed is fed back to the workload SM and its outputs are
    /// processed in a second pass. The cascade is bounded (PodSuspendFailed
    /// never produces another SuspendRequest), so at most one re-drive.
    pub(crate) fn translate_workload_effects(
        &mut self,
        workload_id: &WorkloadId,
        outputs: Vec<WorkloadOutput>,
        placement_table: &mut PlacementTable,
        out: &mut NamespaceOutput,
    ) {
        let mut pending = outputs;
        let mut did_cascade = false;

        loop {
            let mut cascade = Vec::new();
            for wl_out in pending {
                if let Some(extra) = self.translate_single_workload_output(
                    workload_id, wl_out, placement_table, out,
                ) {
                    cascade.extend(extra);
                }
            }
            if cascade.is_empty() {
                break;
            }
            assert!(
                !did_cascade,
                "SuspendRequest cascade did not converge: {:?}",
                cascade,
            );
            did_cascade = true;
            pending = cascade;
        }
    }

    /// Translate a single `WorkloadOutput` into namespace-level actions.
    ///
    /// Returns `Some(outputs)` when the output triggers a cascade (the workload
    /// SM is re-stepped and produces new outputs that must be translated).
    /// Returns `None` in the common case.
    fn translate_single_workload_output(
        &mut self,
        workload_id: &WorkloadId,
        wl_out: WorkloadOutput,
        placement_table: &mut PlacementTable,
        out: &mut NamespaceOutput,
    ) -> Option<Vec<WorkloadOutput>> {
        match wl_out {
            WorkloadOutput::PodRequest => {
                out.pod_requests.push(PodRequest {
                    workload_id: workload_id.clone(),
                });
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
                                return None;
                            };
                        return Some(wl_outputs);
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
                if let Some(wl) = self.workloads.get_mut(workload_id) {
                    wl.conditions.insert(key, message);
                }
            }
            WorkloadOutput::ConditionClear { key } => {
                if let Some(wl) = self.workloads.get_mut(workload_id) {
                    wl.conditions.remove(&key);
                }
            }
        }
        None
    }

    /// Translate service outputs into namespace-level actions.
    pub(crate) fn translate_service_effects(
        &mut self,
        service_id: &ServiceId,
        outputs: Vec<ServiceOutput>,
        out: &mut NamespaceOutput,
    ) {
        for svc_out in outputs {
            match svc_out {
                ServiceOutput::TimerSet(key, duration) => {
                    let adjusted = if matches!(key, TimerKey::IdleTimeout { .. }) {
                        self.pressure_adjusted_idle_timeout(service_id, duration)
                    } else {
                        duration
                    };
                    out.timers_set.push((key, adjusted));
                }
                ServiceOutput::TimerCancel(key) => {
                    out.timers_cancel.push(key);
                }
                ServiceOutput::EndpointChanged => {
                    self.emit_endpoint_update_for_service(service_id, out);
                }
                ServiceOutput::ConditionSet { key, message } => {
                    if let Some(svc) = self.services.get_mut(service_id) {
                        svc.conditions.insert(key, message);
                    }
                }
                ServiceOutput::ConditionClear { key } => {
                    if let Some(svc) = self.services.get_mut(service_id) {
                        svc.conditions.remove(&key);
                    }
                }
            }
        }
    }

    /// Look up the pressure band for a service's hosting worker and adjust the timeout.
    fn pressure_adjusted_idle_timeout(
        &self,
        service_id: &ServiceId,
        configured: std::time::Duration,
    ) -> std::time::Duration {
        let band = self
            .services
            .get(service_id)
            .and_then(|svc| {
                if let ServiceState::Active { ref worker_id, .. } = svc.state {
                    Some(worker_id)
                } else {
                    None
                }
            })
            .and_then(|wid| self.workers.get(wid))
            .map(|nws| nws.pressure_band)
            .unwrap_or(PressureBand::Normal);
        band.adjust_idle_timeout(configured)
    }
}
