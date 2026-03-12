use crate::sm::service::ServiceOutput;
use crate::types::*;
use crate::sm::workload::{WorkloadInput, WorkloadOutput};

use super::{prefix_len_to_netmask, NamespaceStateMachine};

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
                pod_id,
                worker_id,
                artifact_id,
            } => {

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
            WorkloadOutput::LaunchRequest { worker_id, pod_id } => {
                // Register pod in pod_map.
                debug_assert!(
                    !self.pod_map.contains(&pod_id),
                    "Pod {:?} already exists in pods map — outer-layer bug",
                    pod_id
                );
                self.pod_map.insert(
                    pod_id.clone(),
                    PodInfo {
                        workload_id: workload_id.clone(),
                        worker_id: worker_id.clone(),
                    },
                );

                // Build and emit WorkerCommand::LaunchPod.
                let wl_spec = match self.spec.workloads.get(workload_id) {
                    Some(s) => s,
                    None => return None,
                };
                let mut pod_network = wl_spec.network.clone();
                pod_network.gateway = self.spec.network.gateway;
                pod_network.netmask = prefix_len_to_netmask(self.spec.network.prefix_len);
                let resources = wl_spec.resources.as_ref().map(|r| {
                    distvirt_worker_protocol::ResourceRequirements {
                        requests: r.requests.as_ref().map(|v| distvirt_worker_protocol::ResourceValues {
                            memory_mib: v.memory_mb,
                            vcpus: v.vcpus,
                        }),
                        limits: r.limits.as_ref().map(|v| distvirt_worker_protocol::ResourceValues {
                            memory_mib: v.memory_mb,
                            vcpus: v.vcpus,
                        }),
                    }
                });
                out.worker_commands.push((
                    worker_id.clone(),
                    WorkerCommand::LaunchPod {
                        namespace_id: self.namespace_id.clone(),
                        pod_id: pod_id.clone(),
                        network: pod_network,
                        containers: wl_spec.containers.clone(),
                        resources,
                    },
                ));

                // Broadcast endpoint update.
                self.emit_endpoint_update_for_workload(workload_id, out);

                // Emit pod launching event.
                out.events.push(SmNamespaceEvent::Workload {
                    workload_id: workload_id.clone(),
                    event: SmWorkloadEvent::PodLaunching {
                        pod_id,
                        worker_id,
                    },
                });
            }
            WorkloadOutput::ResumeFromArtifact { worker_id, pod_id, artifact_id } => {
                // Register pod in pod_map.
                debug_assert!(
                    !self.pod_map.contains(&pod_id),
                    "Pod {:?} already exists in pods map — outer-layer bug",
                    pod_id
                );
                self.pod_map.insert(
                    pod_id.clone(),
                    PodInfo {
                        workload_id: workload_id.clone(),
                        worker_id: worker_id.clone(),
                    },
                );

                // Build and emit WorkerCommand::ResumePod.
                let wl_spec = match self.spec.workloads.get(workload_id) {
                    Some(s) => s,
                    None => return None,
                };
                let placement = match placement_table.get(&artifact_id) {
                    Some(p) => p.clone(),
                    None => return None,
                };
                let mut pod_network = wl_spec.network.clone();
                pod_network.gateway = self.spec.network.gateway;
                pod_network.netmask = prefix_len_to_netmask(self.spec.network.prefix_len);
                out.worker_commands.push((
                    worker_id.clone(),
                    WorkerCommand::ResumePod {
                        namespace_id: self.namespace_id.clone(),
                        pod_id: pod_id.clone(),
                        artifact_id,
                        network: pod_network,
                        pool_id: placement.pool_id,
                    },
                ));

                // Broadcast endpoint update.
                self.emit_endpoint_update_for_workload(workload_id, out);

                // Emit resume event.
                out.events.push(SmNamespaceEvent::Workload {
                    workload_id: workload_id.clone(),
                    event: SmWorkloadEvent::PodResuming {
                        pod_id,
                        worker_id,
                    },
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
            WorkloadOutput::BecameReady { pod_id, worker_id } => {
                self.workload_readiness.insert(
                    workload_id.clone(),
                    super::WorkloadReadyInfo { pod_id, worker_id },
                );
            }
            WorkloadOutput::BecameUnready => {
                self.workload_readiness.remove(workload_id);
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
                ServiceOutput::IdleTimerStarted { timeout } => {
                    if let Some(svc) = self.services.get(service_id) {
                        out.events.push(SmNamespaceEvent::Service {
                            service_id: service_id.clone(),
                            workload_id: svc.workload_id.clone(),
                            event: SmServiceEvent::IdleTimerStarted { timeout },
                        });
                    }
                }
                ServiceOutput::IdleTimerCancelled { reason } => {
                    if let Some(svc) = self.services.get(service_id) {
                        out.events.push(SmNamespaceEvent::Service {
                            service_id: service_id.clone(),
                            workload_id: svc.workload_id.clone(),
                            event: SmServiceEvent::IdleTimerCancelled { reason },
                        });
                    }
                }
                ServiceOutput::IdleTimeoutFired => {
                    if let Some(svc) = self.services.get(service_id) {
                        out.events.push(SmNamespaceEvent::Service {
                            service_id: service_id.clone(),
                            workload_id: svc.workload_id.clone(),
                            event: SmServiceEvent::IdleTimeoutFired,
                        });
                    }
                }
                ServiceOutput::Deactivated { reason } => {
                    if let Some(svc) = self.services.get(service_id) {
                        out.events.push(SmNamespaceEvent::Service {
                            service_id: service_id.clone(),
                            workload_id: svc.workload_id.clone(),
                            event: SmServiceEvent::Deactivated { reason },
                        });
                    }
                }
                ServiceOutput::Activated { trigger } => {
                    if let Some(svc) = self.services.get(service_id) {
                        out.events.push(SmNamespaceEvent::Service {
                            service_id: service_id.clone(),
                            workload_id: svc.workload_id.clone(),
                            event: SmServiceEvent::Activated { trigger },
                        });
                    }
                }
                ServiceOutput::BackendReady => {
                    if let Some(svc) = self.services.get(service_id) {
                        out.events.push(SmNamespaceEvent::Service {
                            service_id: service_id.clone(),
                            workload_id: svc.workload_id.clone(),
                            event: SmServiceEvent::BackendReady,
                        });
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
            .and_then(|svc| svc.active_worker_id())
            .and_then(|wid| self.workers.get(wid))
            .map(|nws| nws.pressure_band)
            .unwrap_or(PressureBand::Normal);
        band.adjust_idle_timeout(configured)
    }
}
