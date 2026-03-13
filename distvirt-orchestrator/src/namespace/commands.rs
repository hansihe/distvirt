use crate::sm::service::{ServiceInput, ServiceStateMachine};
use crate::types::*;
use crate::sm::workload::{WorkloadInput, WorkloadStateMachine};

use super::NamespaceStateMachine;

impl NamespaceStateMachine {
    pub(super) fn handle_update_spec(
        &mut self,
        client_id: ClientId,
        spec: NamespaceSpec,
        placement_table: &mut PlacementTable,
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
                let has_services = spec.services.values().any(|s| s.workload_id == *wl_id);
                let has_activation = wl_spec.activation.is_some() || has_services;
                self.workloads.insert(
                    wl_id.clone(),
                    WorkloadStateMachine::new(
                        wl_id.clone(),
                        wl_spec.suspend_on_idle,
                        has_activation,
                    ),
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
                self.teardown_workload(wl, placement_table, out);
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

        // Collect IPs of removed services for endpoint update.
        let mut removed_ips = Vec::new();
        for svc_id in &removed_services {
            if let Some(svc_spec) = self.spec.services.get(svc_id) {
                removed_ips.push(svc_spec.ip);
            }
            self.service_workload.remove(svc_id);
            self.services.remove(svc_id);
        }
        if !removed_ips.is_empty() {
            let ns_id = self.namespace_id.clone();
            crate::broadcast::broadcast_to_active_workers(&self.workers, out, |_| {
                WorkerCommand::EndpointUpdate {
                    namespace_id: ns_id.clone(),
                    upserted: vec![],
                    removed_ips: removed_ips.clone(),
                }
            });
        }

        // Detect in-place workload spec changes and dispatch SpecChanged.
        for (wl_id, new_wl_spec) in &spec.workloads {
            if let Some(old_wl_spec) = self.spec.workloads.get(wl_id) {
                if old_wl_spec != new_wl_spec {
                    let image_changed = old_wl_spec.containers != new_wl_spec.containers;
                    let suspend_changed = old_wl_spec.suspend_on_idle != new_wl_spec.suspend_on_idle;

                    if image_changed {
                        if let Some(wl) = self.workloads.get_mut(wl_id) {
                            let wl_outputs = wl.step(
                                WorkloadInput::SpecChanged,
                                &self.namespace_id,
                            );
                            self.translate_workload_effects(wl_id, wl_outputs, placement_table, out);
                            self.reconcile_demand(wl_id, placement_table, out);
                        }
                    }
                    if suspend_changed {
                        if let Some(wl) = self.workloads.get_mut(wl_id) {
                            wl.suspend_on_idle = new_wl_spec.suspend_on_idle;
                        }
                    }
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
            self.reconcile_all_services(placement_table, out);
        }

        out.client_events.push((client_id, ClientEvent::Ok));
    }

    pub(super) fn handle_delete(&mut self, client_id: ClientId, placement_table: &mut PlacementTable, out: &mut NamespaceOutput) {
        self.status = NamespaceStatus::Destroying;
        let ns_id = self.namespace_id.clone();

        // Cancel all active timers and clean up snapshots.
        for wl in self.workloads.values() {
            match &wl.state {
                WorkloadState::Active { pod: PodSlot { pod_state, .. }, .. } => {
                    match pod_state {
                        PodState::Launching { launch_timeout } => {
                            out.timers_cancel.push(launch_timeout.clone());
                        }
                        PodState::Suspending { suspend_timeout, .. } => {
                            out.timers_cancel.push(suspend_timeout.clone());
                        }
                        PodState::Resuming { artifact_id, resume_timeout } => {
                            out.timers_cancel.push(resume_timeout.clone());
                            if let Some(placement) = placement_table.remove(artifact_id) {
                                out.worker_commands.push((
                                    placement.worker_id,
                                    WorkerCommand::DeleteArtifact {
                                        artifact_id: artifact_id.clone(),
                                        pool_id: placement.pool_id,
                                    },
                                ));
                            }
                        }
                        PodState::Running => {}
                    }
                }
                WorkloadState::Suspended {
                    artifact_id,
                } => {
                    if let Some(placement) = placement_table.remove(artifact_id) {
                        out.worker_commands.push((
                            placement.worker_id,
                            WorkerCommand::DeleteArtifact {
                                artifact_id: artifact_id.clone(),
                                pool_id: placement.pool_id,
                            },
                        ));
                    }
                }
                WorkloadState::RetryBackoff { backoff_timer } => {
                    out.timers_cancel.push(backoff_timer.clone());
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

        // Direct state reset (bypasses step()): workloads and services are
        // force-reset to initial states after all cleanup outputs (timer cancels,
        // artifact deletes) have been explicitly emitted above.
        for wl in self.workloads.values_mut() {
            wl.state = WorkloadState::Dormant;
            wl.current_demand = 0;
        }
        for svc in self.services.values_mut() {
            svc.state = ServiceState::Idle;
        }
        self.pod_map.clear();
        self.workload_readiness.clear();
        self.active_flows.clear();

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
        placement_table: &mut PlacementTable,
        out: &mut NamespaceOutput,
    ) {
        if self.status == NamespaceStatus::Destroying {
            return;
        }

        // Worker must be in our map and active.
        if !matches!(
            self.workers.get(worker_id),
            Some(ws) if ws.fabric_status == FabricStatus::Active
        ) {
            return;
        }

        // Spec must exist.
        if !self.spec.workloads.contains_key(workload_id) {
            return;
        }

        // Step the workload SM — it is the sole authority on whether
        // to accept a LaunchPod. Side effects are driven by SM outputs.
        let wl = match self.workloads.get_mut(workload_id) {
            Some(wl) => wl,
            None => return,
        };
        let wl_outputs = wl.step(
            WorkloadInput::LaunchPod {
                worker_id: worker_id.clone(),
                pod_id: pod_id.clone(),
            },
            &self.namespace_id,
        );

        let wl_id = workload_id.clone();
        self.translate_workload_effects(&wl_id, wl_outputs, placement_table, out);
        self.reconcile_demand(&wl_id, placement_table, out);
    }

    pub(super) fn handle_resume_pod(
        &mut self,
        workload_id: &WorkloadId,
        worker_id: &WorkerId,
        pod_id: &PodId,
        artifact_id: &ArtifactId,
        placement_table: &mut PlacementTable,
        out: &mut NamespaceOutput,
    ) {
        if self.status == NamespaceStatus::Destroying {
            return;
        }

        // Worker must be in our map and active.
        if !matches!(
            self.workers.get(worker_id),
            Some(ws) if ws.fabric_status == FabricStatus::Active
        ) {
            return;
        }

        // Look up placement table — only resume from Ready artifacts.
        if !matches!(
            placement_table.get(artifact_id),
            Some(p) if p.status == ArtifactStatus::Ready
        ) {
            return;
        }

        // Spec must exist.
        if !self.spec.workloads.contains_key(workload_id) {
            return;
        }

        // Step the workload SM — it is the sole authority on whether
        // to accept a ResumePod. Side effects are driven by SM outputs.
        let wl = match self.workloads.get_mut(workload_id) {
            Some(wl) => wl,
            None => return,
        };
        let wl_outputs = wl.step(
            WorkloadInput::ResumePod {
                worker_id: worker_id.clone(),
                pod_id: pod_id.clone(),
                artifact_id: artifact_id.clone(),
            },
            &self.namespace_id,
        );

        let wl_id = workload_id.clone();
        self.translate_workload_effects(&wl_id, wl_outputs, placement_table, out);
        self.reconcile_demand(&wl_id, placement_table, out);
    }

    pub(super) fn handle_deactivate_workload(
        &mut self,
        client_id: ClientId,
        workload_id: WorkloadId,
        placement_table: &mut PlacementTable,
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
                if let Some(backend_need) = svc.active_backend_need() {
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
            if !svc.is_active() {
                continue;
            }
            let wl_id = svc.workload_id.clone();

            let svc_outputs = svc.step(ServiceInput::ForceDeactivate, &self.namespace_id);
            self.translate_service_effects(&sid, svc_outputs, out);
            self.reconcile_demand(&wl_id, placement_table, out);
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

    /// Preempt a workload: force-deactivate it and set a "preempted" condition.
    /// Called by the orchestrator's scheduling layer when a higher-priority workload
    /// needs capacity and this workload has been selected as the victim.
    pub(super) fn handle_preempt_workload(
        &mut self,
        workload_id: WorkloadId,
        placement_table: &mut PlacementTable,
        out: &mut NamespaceOutput,
    ) {
        let wl = match self.workloads.get(&workload_id) {
            Some(wl) => wl,
            None => return,
        };

        // Only preempt workloads that have a pod (Running, Launching, etc.)
        if !wl.is_preemptable() {
            return;
        }

        // Force-deactivate services for this workload (same as handle_deactivate_workload
        // but without client interaction and without checking backend_need).
        let svc_ids: Vec<ServiceId> = self
            .services
            .iter()
            .filter(|(_, svc)| svc.workload_id == workload_id && svc.has_activation)
            .map(|(sid, _)| sid.clone())
            .collect();

        for sid in svc_ids {
            let svc = match self.services.get_mut(&sid) {
                Some(svc) => svc,
                None => continue,
            };
            if !svc.is_active() {
                continue;
            }
            let wl_id = svc.workload_id.clone();

            let svc_outputs = svc.step(ServiceInput::ForceDeactivate, &self.namespace_id);
            self.translate_service_effects(&sid, svc_outputs, out);
            self.reconcile_demand(&wl_id, placement_table, out);
        }

        // Step the workload SM with ForceDeactivate.
        let wl = match self.workloads.get_mut(&workload_id) {
            Some(wl) => wl,
            None => return,
        };
        let wl_outputs = wl.step(WorkloadInput::ForceDeactivate, &self.namespace_id);
        // Set preempted condition.
        wl.conditions.insert(
            "preempted".to_string(),
            "preempted for higher-priority workload".to_string(),
        );
        self.translate_workload_effects(&workload_id, wl_outputs, placement_table, out);
        self.reconcile_demand(&workload_id, placement_table, out);
    }

    /// Clean up a removed workload: cancel timers, stop pods, delete artifacts.
    pub(super) fn teardown_workload(
        &mut self,
        wl: WorkloadStateMachine,
        placement_table: &mut PlacementTable,
        out: &mut NamespaceOutput,
    ) {
        match &wl.state {
            WorkloadState::Active { pod: PodSlot { pod_id, worker_id, pod_state }, .. } => {
                match pod_state {
                    PodState::Launching { launch_timeout } => {
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
                    PodState::Running => {
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
                    PodState::Suspending { suspend_timeout, .. } => {
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
                    PodState::Resuming { artifact_id, resume_timeout } => {
                        out.timers_cancel.push(resume_timeout.clone());
                        if let Some(placement) = placement_table.remove(artifact_id) {
                            out.worker_commands.push((
                                placement.worker_id.clone(),
                                WorkerCommand::StopPod {
                                    namespace_id: self.namespace_id.clone(),
                                    pod_id: pod_id.clone(),
                                    graceful: false,
                                },
                            ));
                            out.worker_commands.push((
                                placement.worker_id,
                                WorkerCommand::DeleteArtifact {
                                    artifact_id: artifact_id.clone(),
                                    pool_id: placement.pool_id,
                                },
                            ));
                        }
                        self.pod_map.remove(pod_id);
                    }
                }
            }
            WorkloadState::Dormant | WorkloadState::WaitingForCapacity | WorkloadState::Failed => {}
            WorkloadState::Transitioning => unreachable!("Transitioning in workload removal"),
            WorkloadState::RetryBackoff { backoff_timer } => {
                out.timers_cancel.push(backoff_timer.clone());
            }
            WorkloadState::Suspended {
                artifact_id,
            } => {
                if let Some(placement) = placement_table.remove(artifact_id) {
                    out.worker_commands.push((
                        placement.worker_id,
                        WorkerCommand::DeleteArtifact {
                            artifact_id: artifact_id.clone(),
                            pool_id: placement.pool_id,
                        },
                    ));
                }
            }
        }
    }
}
