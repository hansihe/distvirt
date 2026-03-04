use crate::service::{ServiceInput, ServiceStateMachine};
use crate::types::*;
use crate::workload::{WorkloadInput, WorkloadStateMachine};

use super::{prefix_len_to_netmask, NamespaceStateMachine};

impl NamespaceStateMachine {
    pub(super) fn handle_update_spec(
        &mut self,
        client_id: ClientId,
        spec: NamespaceSpec,
        out: &mut NamespaceOutput,
    ) {
        if self.status == NamespaceStatus::Destroying {
            out.client_events.push((
                client_id,
                ClientEvent::Error {
                    message: "namespace is being destroyed".to_string(),
                },
            ));
            return;
        }
        // Add new workloads.
        for (wl_id, wl_spec) in &spec.workloads {
            if !self.workloads.contains_key(wl_id) {
                self.workloads.insert(
                    wl_id.clone(),
                    WorkloadStateMachine::new(wl_id.clone(), wl_spec.suspend_on_idle),
                );
            }
        }

        // Remove workloads no longer in spec.
        let removed_workloads: Vec<WorkloadId> = self
            .workloads
            .keys()
            .filter(|wl_id| !spec.workloads.contains_key(wl_id))
            .cloned()
            .collect();

        for wl_id in removed_workloads {
            if let Some(wl) = self.workloads.remove(&wl_id) {
                match &wl.state {
                    WorkloadState::Launching {
                        pod_id,
                        worker_id,
                        launch_timeout,
                    } => {
                        out.timers_cancel.push(launch_timeout.clone());
                        out.worker_commands.push((
                            worker_id.clone(),
                            WorkerCommand::StopPod {
                                namespace_id: self.namespace_id.clone(),
                                pod_id: pod_id.clone(),
                                graceful: false,
                            },
                        ));
                        self.pod_map.remove(pod_id);
                    }
                    WorkloadState::Running {
                        pod_id, worker_id, ..
                    } => {
                        out.worker_commands.push((
                            worker_id.clone(),
                            WorkerCommand::StopPod {
                                namespace_id: self.namespace_id.clone(),
                                pod_id: pod_id.clone(),
                                graceful: true,
                            },
                        ));
                        self.pod_map.remove(pod_id);
                    }
                    WorkloadState::Dormant | WorkloadState::WaitingForCapacity => {}
                    WorkloadState::Suspending {
                        pod_id,
                        worker_id,
                        suspend_timeout,
                        ..
                    } => {
                        out.timers_cancel.push(suspend_timeout.clone());
                        out.worker_commands.push((
                            worker_id.clone(),
                            WorkerCommand::StopPod {
                                namespace_id: self.namespace_id.clone(),
                                pod_id: pod_id.clone(),
                                graceful: false,
                            },
                        ));
                        self.pod_map.remove(pod_id);
                    }
                    WorkloadState::Suspended {
                        worker_id,
                        snapshot_id,
                        pool_id,
                    } => {
                        out.worker_commands.push((
                            worker_id.clone(),
                            WorkerCommand::DeleteSnapshot {
                                snapshot_id: snapshot_id.clone(),
                                pool_id: pool_id.clone(),
                            },
                        ));
                    }
                    WorkloadState::Resuming {
                        pod_id,
                        worker_id,
                        snapshot_id,
                        pool_id,
                        resume_timeout,
                    } => {
                        out.timers_cancel.push(resume_timeout.clone());
                        out.worker_commands.push((
                            worker_id.clone(),
                            WorkerCommand::StopPod {
                                namespace_id: self.namespace_id.clone(),
                                pod_id: pod_id.clone(),
                                graceful: false,
                            },
                        ));
                        out.worker_commands.push((
                            worker_id.clone(),
                            WorkerCommand::DeleteSnapshot {
                                snapshot_id: snapshot_id.clone(),
                                pool_id: pool_id.clone(),
                            },
                        ));
                        self.pod_map.remove(pod_id);
                    }
                }
            }
        }

        // Add new services.
        for (svc_id, svc_spec) in &spec.services {
            if !self.services.contains_key(svc_id) {
                let wl_id = svc_spec.workload_id.clone();
                let has_activation = svc_spec.activation.is_some();
                let idle_timeout = svc_spec
                    .activation
                    .as_ref()
                    .map(|a| a.idle_timeout)
                    .unwrap_or(std::time::Duration::from_secs(30));

                self.services.insert(
                    svc_id.clone(),
                    ServiceStateMachine::new(
                        svc_id.clone(),
                        wl_id.clone(),
                        has_activation,
                        idle_timeout,
                    ),
                );
                self.service_workload.insert(svc_id.clone(), wl_id);
            }
        }

        // Remove services no longer in spec.
        let removed_services: Vec<ServiceId> = self
            .services
            .keys()
            .filter(|svc_id| !spec.services.contains_key(svc_id))
            .cloned()
            .collect();

        for svc_id in &removed_services {
            self.service_workload.remove(svc_id);
            self.services.remove(svc_id);

            // Emit DestroyService to all active workers.
            let ns_id = self.namespace_id.clone();
            let svc_id_clone = svc_id.clone();
            crate::broadcast::broadcast_to_active_workers(&self.workers, out, |_| {
                WorkerCommand::DestroyService {
                    namespace_id: ns_id.clone(),
                    service_id: svc_id_clone.clone(),
                }
            });
        }

        // Warn about in-place spec changes (not yet handled beyond add/remove).
        for (wl_id, new_wl_spec) in &spec.workloads {
            if let Some(old_wl_spec) = self.spec.workloads.get(wl_id) {
                if old_wl_spec != new_wl_spec {
                    log::warn!(
                        "Workload {:?} spec changed in-place (not yet handled, update silently applied)",
                        wl_id
                    );
                }
            }
        }
        for (svc_id, new_svc_spec) in &spec.services {
            if let Some(old_svc_spec) = self.spec.services.get(svc_id) {
                if old_svc_spec != new_svc_spec {
                    log::warn!(
                        "Service {:?} spec changed in-place (not yet handled, update silently applied)",
                        svc_id
                    );
                }
            }
        }

        self.spec = spec;

        if self.status == NamespaceStatus::Active {
            if !removed_services.is_empty() {
                self.emit_registry_sync(out);
            }
            self.reconcile_all_services(out);
        }

        out.client_events.push((client_id, ClientEvent::Ok));
    }

    pub(super) fn handle_delete(&mut self, client_id: ClientId, out: &mut NamespaceOutput) {
        self.status = NamespaceStatus::Destroying;
        let ns_id = self.namespace_id.clone();

        // Cancel all active timers and clean up snapshots.
        for wl in self.workloads.values() {
            match &wl.state {
                WorkloadState::Launching {
                    launch_timeout, ..
                } => {
                    out.timers_cancel.push(launch_timeout.clone());
                }
                WorkloadState::Suspending {
                    suspend_timeout,
                    ..
                } => {
                    out.timers_cancel.push(suspend_timeout.clone());
                }
                WorkloadState::Suspended {
                    worker_id,
                    snapshot_id,
                    pool_id,
                } => {
                    out.worker_commands.push((
                        worker_id.clone(),
                        WorkerCommand::DeleteSnapshot {
                            snapshot_id: snapshot_id.clone(),
                            pool_id: pool_id.clone(),
                        },
                    ));
                }
                WorkloadState::Resuming {
                    worker_id,
                    snapshot_id,
                    pool_id,
                    resume_timeout,
                    ..
                } => {
                    out.timers_cancel.push(resume_timeout.clone());
                    out.worker_commands.push((
                        worker_id.clone(),
                        WorkerCommand::DeleteSnapshot {
                            snapshot_id: snapshot_id.clone(),
                            pool_id: pool_id.clone(),
                        },
                    ));
                }
                _ => {}
            }
        }
        for svc in self.services.values() {
            if let ServiceState::Active {
                idle_timer: Some(tk),
                ..
            } = &svc.state
            {
                out.timers_cancel.push(tk.clone());
            }
        }

        // Reset workloads and services so no stale references remain.
        for wl in self.workloads.values_mut() {
            wl.state = WorkloadState::Dormant;
            wl.demand_count = 0;
        }
        for svc in self.services.values_mut() {
            svc.state = ServiceState::Idle;
        }
        self.pod_map.clear();

        // Send DestroyNamespace to each worker, set fabric_status to Destroying.
        // DestroyNamespace handles stopping pods on the worker side.
        for (wid, ws) in &mut self.workers {
            ws.fabric_status = FabricStatus::Destroying;
            out.worker_commands.push((
                wid.clone(),
                WorkerCommand::DestroyNamespace {
                    namespace_id: ns_id.clone(),
                },
            ));
        }

        // If no workers, destruction is immediate.
        if self.workers.is_empty() {
            out.destroyed = true;
        }

        out.client_events.push((client_id, ClientEvent::Ok));
    }

    pub(super) fn handle_get_status(&self, client_id: ClientId, out: &mut NamespaceOutput) {
        let status = self.status_report();
        out.client_events.push((
            client_id,
            ClientEvent::NamespaceStatus {
                namespace_id: self.namespace_id.clone(),
                status,
            },
        ));
    }

    pub(super) fn handle_launch_pod(
        &mut self,
        workload_id: &WorkloadId,
        worker_id: &WorkerId,
        pod_id: &PodId,
        out: &mut NamespaceOutput,
    ) {
        if self.status == NamespaceStatus::Destroying {
            return;
        }
        // Only act if workload is waiting for capacity.
        match self.workloads.get(workload_id) {
            Some(wl) if matches!(wl.state, WorkloadState::WaitingForCapacity) => {}
            _ => return,
        }

        // Worker must be in our map and active.
        if !matches!(
            self.workers.get(worker_id),
            Some(ws) if ws.fabric_status == FabricStatus::Active
        ) {
            return;
        }

        // Get workload spec for pod config.
        let wl_spec = match self.spec.workloads.get(workload_id) {
            Some(s) => s.clone(),
            None => return,
        };

        // Find associated service to get the service spec.
        let svc_id = self
            .service_workload
            .iter()
            .find(|(_, wl_id)| *wl_id == workload_id)
            .map(|(sid, _)| sid.clone());
        let svc_id = match svc_id {
            Some(id) => id,
            None => return,
        };
        let svc_spec = match self.spec.services.get(&svc_id) {
            Some(s) => s.clone(),
            None => return,
        };

        // Register pod.
        debug_assert!(
            !self.pod_map.contains(pod_id),
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

        // Send worker commands to create the service and launch the pod.
        let ns_id = self.namespace_id.clone();
        out.worker_commands.push((
            worker_id.clone(),
            WorkerCommand::CreateService {
                namespace_id: ns_id.clone(),
                service_id: svc_id.clone(),
                ip: svc_spec.ip,
                policy: svc_spec.policy.clone(),
            },
        ));
        // Override gateway and netmask from the namespace's network config,
        // since the workload spec may not have them populated.
        let mut pod_network = wl_spec.network.clone();
        pod_network.gateway = self.spec.network.gateway;
        pod_network.netmask = prefix_len_to_netmask(self.spec.network.prefix_len);
        out.worker_commands.push((
            worker_id.clone(),
            WorkerCommand::LaunchPod {
                namespace_id: ns_id.clone(),
                pod_id: pod_id.clone(),
                network: pod_network,
                containers: wl_spec.containers.clone(),
            },
        ));

        // Emit fabric route add if multi-worker.
        if self.workers.len() > 1 {
            let pod_ip = wl_spec.network.ip;
            self.emit_fabric_route_add(pod_ip, worker_id, out);
        }

        // Emit pod launching event.
        out.events.push(SmNamespaceEvent::Workload {
            workload_id: workload_id.clone(),
            event: SmWorkloadEvent::PodLaunching {
                pod_id: pod_id.clone(),
                worker_id: worker_id.clone(),
            },
        });

        // Step the workload SM.
        let wl = self.workloads.get_mut(workload_id).unwrap();
        let wl_outputs = wl.step(
            WorkloadInput::LaunchPod {
                worker_id: worker_id.clone(),
                pod_id: pod_id.clone(),
            },
            &self.namespace_id,
        );
        let wl_id = workload_id.clone();
        self.forward_workload_outputs(&wl_id, wl_outputs, out);
    }

    pub(super) fn handle_resume_pod(
        &mut self,
        workload_id: &WorkloadId,
        worker_id: &WorkerId,
        pod_id: &PodId,
        snapshot_id: &SnapshotId,
        pool_id: &PoolId,
        out: &mut NamespaceOutput,
    ) {
        if self.status == NamespaceStatus::Destroying {
            return;
        }
        // Only act if workload is suspended.
        match self.workloads.get(workload_id) {
            Some(wl) if matches!(wl.state, WorkloadState::Suspended { .. }) => {}
            _ => return,
        }

        // Worker must be in our map and active.
        if !matches!(
            self.workers.get(worker_id),
            Some(ws) if ws.fabric_status == FabricStatus::Active
        ) {
            return;
        }

        // Get workload spec for pod network config.
        let wl_spec = match self.spec.workloads.get(workload_id) {
            Some(s) => s.clone(),
            None => return,
        };

        // Register pod.
        debug_assert!(
            !self.pod_map.contains(pod_id),
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

        // Send ResumePod to worker.
        let mut pod_network = wl_spec.network.clone();
        pod_network.gateway = self.spec.network.gateway;
        pod_network.netmask = prefix_len_to_netmask(self.spec.network.prefix_len);
        out.worker_commands.push((
            worker_id.clone(),
            WorkerCommand::ResumePod {
                namespace_id: self.namespace_id.clone(),
                pod_id: pod_id.clone(),
                snapshot_id: snapshot_id.clone(),
                network: pod_network,
                pool_id: pool_id.clone(),
            },
        ));

        // Emit fabric route add if multi-worker.
        if self.workers.len() > 1 {
            let pod_ip = wl_spec.network.ip;
            self.emit_fabric_route_add(pod_ip, worker_id, out);
        }

        // Emit resume event.
        out.events.push(SmNamespaceEvent::Workload {
            workload_id: workload_id.clone(),
            event: SmWorkloadEvent::PodResuming {
                pod_id: pod_id.clone(),
                worker_id: worker_id.clone(),
            },
        });

        // Step the workload SM.
        let wl = self.workloads.get_mut(workload_id).unwrap();
        let wl_outputs = wl.step(
            WorkloadInput::ResumePod {
                worker_id: worker_id.clone(),
                pod_id: pod_id.clone(),
                snapshot_id: snapshot_id.clone(),
                pool_id: pool_id.clone(),
            },
            &self.namespace_id,
        );
        let wl_id = workload_id.clone();
        self.forward_workload_outputs(&wl_id, wl_outputs, out);
    }

    pub(super) fn handle_deactivate_workload(
        &mut self,
        client_id: ClientId,
        workload_id: WorkloadId,
        out: &mut NamespaceOutput,
    ) {
        // Check workload exists.
        if !self.workloads.contains_key(&workload_id) {
            out.client_events.push((
                client_id,
                ClientEvent::DeactivateWorkloadResult {
                    deactivated: false,
                    reason: format!("workload '{}' not found", workload_id.0),
                },
            ));
            return;
        }

        // Find all activation-enabled services for this workload.
        let svc_ids: Vec<ServiceId> = self
            .services
            .iter()
            .filter(|(_, svc)| svc.workload_id == workload_id && svc.has_activation)
            .map(|(sid, _)| sid.clone())
            .collect();

        if svc_ids.is_empty() {
            out.client_events.push((
                client_id,
                ClientEvent::DeactivateWorkloadResult {
                    deactivated: false,
                    reason: "no activation-enabled services on this workload".to_string(),
                },
            ));
            return;
        }

        // Check if any qualifying service has real demand (backend_need != None).
        for sid in &svc_ids {
            if let Some(svc) = self.services.get(sid) {
                if let ServiceState::Active { ref backend_need, .. } = svc.state {
                    if *backend_need != BackendNeed::None {
                        out.client_events.push((
                            client_id,
                            ClientEvent::DeactivateWorkloadResult {
                                deactivated: false,
                                reason: format!(
                                    "service '{}' has active demand ({:?})",
                                    sid.0, backend_need,
                                ),
                            },
                        ));
                        return;
                    }
                }
            }
        }

        // Force-deactivate each qualifying service.
        let mut deactivated_any = false;
        for sid in svc_ids {
            let svc = match self.services.get_mut(&sid) {
                Some(svc) => svc,
                None => continue,
            };
            if !matches!(svc.state, ServiceState::Active { .. }) {
                continue;
            }
            let wl_id = svc.workload_id.clone();

            // Emit deactivation events.
            out.events.push(SmNamespaceEvent::Service {
                service_id: sid.clone(),
                workload_id: wl_id.clone(),
                event: SmServiceEvent::Deactivated {
                    reason: ServiceDeactivationReason::ForceDeactivate,
                },
            });

            let svc_outputs = svc.step(ServiceInput::ForceDeactivate, &self.namespace_id);
            self.forward_service_outputs(&sid, &wl_id, svc_outputs, out);
            deactivated_any = true;
        }

        if deactivated_any {
            out.client_events.push((
                client_id,
                ClientEvent::DeactivateWorkloadResult {
                    deactivated: true,
                    reason: String::new(),
                },
            ));
        } else {
            out.client_events.push((
                client_id,
                ClientEvent::DeactivateWorkloadResult {
                    deactivated: false,
                    reason: "no services were in active state".to_string(),
                },
            ));
        }
    }
}
