use std::collections::HashMap;
use std::net::Ipv4Addr;

use crate::service::{ServiceInput, ServiceOutput, ServiceStateMachine};
use crate::types::*;
use crate::wg_peers::{ConnectResult, WgPeerOutput, WireGuardPeerManager};
use crate::workload::{WorkloadInput, WorkloadOutput, WorkloadStateMachine};

/// Convert a CIDR prefix length (e.g. 24) to a dotted-decimal netmask string (e.g. "255.255.255.0").
fn prefix_len_to_netmask(prefix_len: u8) -> String {
    let mask = if prefix_len == 0 {
        0u32
    } else {
        !0u32 << (32 - prefix_len)
    };
    Ipv4Addr::from(mask).to_string()
}

pub struct NamespaceStateMachine {
    pub namespace_id: NamespaceId,
    pub spec: NamespaceSpec,
    pub status: NamespaceStatus,
    pub workloads: HashMap<WorkloadId, WorkloadStateMachine>,
    pub services: HashMap<ServiceId, ServiceStateMachine>,
    pub service_workload: HashMap<ServiceId, WorkloadId>,
    pub pods: HashMap<PodId, PodInfo>,
    pub workers: HashMap<WorkerId, NamespaceWorkerState>,
    /// WireGuard peer IP allocation and tracking.
    pub wg_peer_manager: WireGuardPeerManager,
}

impl NamespaceStateMachine {
    pub fn new(namespace_id: NamespaceId, spec: NamespaceSpec) -> Self {
        let mut workloads = HashMap::new();
        let mut services = HashMap::new();
        let mut service_workload = HashMap::new();

        for (wl_id, _wl_spec) in &spec.workloads {
            workloads.insert(wl_id.clone(), WorkloadStateMachine::new(wl_id.clone()));
        }

        for (svc_id, svc_spec) in &spec.services {
            let wl_id = svc_spec.workload_id.clone();
            let has_activation = svc_spec.activation.is_some();
            let idle_timeout = svc_spec
                .activation
                .as_ref()
                .map(|a| a.idle_timeout)
                .unwrap_or(std::time::Duration::from_secs(30));

            services.insert(
                svc_id.clone(),
                ServiceStateMachine::new(
                    svc_id.clone(),
                    wl_id.clone(),
                    has_activation,
                    idle_timeout,
                ),
            );
            service_workload.insert(svc_id.clone(), wl_id);
        }

        let wg_peer_manager = WireGuardPeerManager::new(spec.network.subnet, spec.network.prefix_len);

        NamespaceStateMachine {
            namespace_id,
            spec,
            status: NamespaceStatus::Creating,
            workloads,
            services,
            service_workload,
            pods: HashMap::new(),
            workers: HashMap::new(),
            wg_peer_manager,
        }
    }

    fn active_worker_ids(&self) -> Vec<WorkerId> {
        self.workers
            .iter()
            .filter(|(_, ws)| ws.fabric_status == FabricStatus::Active)
            .map(|(wid, _)| wid.clone())
            .collect()
    }

    fn build_registry_entries(&self) -> Vec<RegistryEntry> {
        self.spec
            .services
            .iter()
            .map(|(svc_id, svc_spec)| RegistryEntry {
                name: svc_id.0.clone(),
                ip: svc_spec.ip,
            })
            .collect()
    }

    fn emit_registry_sync(&self, out: &mut NamespaceOutput) {
        let entries = self.build_registry_entries();
        for wid in self.active_worker_ids() {
            out.worker_commands.push((
                wid,
                WorkerCommand::RegistrySync {
                    namespace_id: self.namespace_id.clone(),
                    entries: entries.clone(),
                },
            ));
        }
    }

    /// Pure state transition. No I/O.
    pub fn step(&mut self, input: NamespaceInput) -> NamespaceOutput {
        let mut out = NamespaceOutput::default();

        match input {
            NamespaceInput::WorkerEvent { worker_id, event } => {
                self.handle_worker_event(&worker_id, event, &mut out);
            }
            NamespaceInput::WorkerLost { worker_id } => {
                self.handle_worker_lost(&worker_id, &mut out);
            }
            NamespaceInput::TimerFired { timer_key } => {
                self.handle_timer_fired(&timer_key, &mut out);
            }
            NamespaceInput::UpdateSpec { client_id, spec } => {
                self.handle_update_spec(client_id, spec, &mut out);
            }
            NamespaceInput::Delete { client_id } => {
                self.handle_delete(client_id, &mut out);
            }
            NamespaceInput::GetStatus { client_id } => {
                self.handle_get_status(client_id, &mut out);
            }
            NamespaceInput::Splice {
                client_id,
                workload_id: _,
                worker_id: _,
            } => {
                out.client_events.push((
                    client_id,
                    ClientEvent::Error {
                        message: "Splice not yet implemented".to_string(),
                    },
                ));
            }
            NamespaceInput::Unsplice {
                client_id,
                workload_id: _,
            } => {
                out.client_events.push((
                    client_id,
                    ClientEvent::Error {
                        message: "Unsplice not yet implemented".to_string(),
                    },
                ));
            }
            NamespaceInput::StreamLogs {
                client_id,
                service_id: _,
            } => {
                out.client_events.push((
                    client_id,
                    ClientEvent::Error {
                        message: "StreamLogs not yet implemented".to_string(),
                    },
                ));
            }
            NamespaceInput::LaunchPod {
                workload_id,
                worker_id,
                pod_id,
            } => {
                self.handle_launch_pod(&workload_id, &worker_id, &pod_id, &mut out);
            }
            NamespaceInput::Connect {
                client_id,
                client_public_key,
                worker_wg_public_key,
                worker_endpoint,
            } => {
                self.handle_connect(
                    client_id,
                    client_public_key,
                    worker_wg_public_key,
                    worker_endpoint,
                    &mut out,
                );
            }
            NamespaceInput::Disconnect {
                client_id,
                client_public_key,
            } => {
                self.handle_disconnect(client_id, client_public_key, &mut out);
            }
        }

        out
    }

    // --- Event Handlers ---

    fn handle_worker_event(
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
                            trigger: "traffic".to_string(),
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
                                        reason: "new traffic".to_string(),
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
                let pod_info = match self.pods.remove(&pod_id) {
                    Some(info) => info,
                    None => return,
                };
                if let Some(ws) = self.workers.get_mut(&pod_info.worker_id) {
                    ws.pods.remove(&pod_id);
                }
                let wl_id = pod_info.workload_id.clone();
                out.events.push(SmNamespaceEvent::Workload {
                    workload_id: wl_id.clone(),
                    event: SmWorkloadEvent::PodStopped {
                        reason: format!("exited with code {}", exit_code),
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
                let pod_info = match self.pods.remove(&pod_id) {
                    Some(info) => info,
                    None => return,
                };
                if let Some(ws) = self.workers.get_mut(&pod_info.worker_id) {
                    ws.pods.remove(&pod_id);
                }
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
        }
    }

    fn handle_worker_lost(&mut self, worker_id: &WorkerId, out: &mut NamespaceOutput) {
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

    fn handle_timer_fired(&mut self, timer_key: &TimerKey, out: &mut NamespaceOutput) {
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
                                    reason: "idle timeout".to_string(),
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
                    if let Some(pod_info) = self.pods.remove(pod_id) {
                        if let Some(ws) = self.workers.get_mut(&pod_info.worker_id) {
                            ws.pods.remove(pod_id);
                        }
                    }
                }
                self.forward_workload_outputs(&workload_id, wl_outputs, out);
            }
        }
    }

    fn handle_update_spec(
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
        for (wl_id, _wl_spec) in &spec.workloads {
            if !self.workloads.contains_key(wl_id) {
                self.workloads
                    .insert(wl_id.clone(), WorkloadStateMachine::new(wl_id.clone()));
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
                        if let Some(ws) = self.workers.get_mut(worker_id) {
                            ws.pods.remove(pod_id);
                        }
                        self.pods.remove(pod_id);
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
                        if let Some(ws) = self.workers.get_mut(worker_id) {
                            ws.pods.remove(pod_id);
                        }
                        self.pods.remove(pod_id);
                    }
                    WorkloadState::Dormant | WorkloadState::WaitingForCapacity => {}
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
            for wid in self.active_worker_ids() {
                out.worker_commands.push((
                    wid,
                    WorkerCommand::DestroyService {
                        namespace_id: self.namespace_id.clone(),
                        service_id: svc_id.clone(),
                    },
                ));
            }
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

    fn handle_delete(&mut self, client_id: ClientId, out: &mut NamespaceOutput) {
        self.status = NamespaceStatus::Destroying;
        let ns_id = self.namespace_id.clone();

        // Cancel all active timers.
        for wl in self.workloads.values() {
            if let WorkloadState::Launching {
                launch_timeout, ..
            } = &wl.state
            {
                out.timers_cancel.push(launch_timeout.clone());
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
        self.pods.clear();

        // Send DestroyNamespace to each worker, set fabric_status to Destroying.
        // DestroyNamespace handles stopping pods on the worker side.
        for (wid, ws) in &mut self.workers {
            ws.fabric_status = FabricStatus::Destroying;
            ws.pods.clear();
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

    fn handle_get_status(&self, client_id: ClientId, out: &mut NamespaceOutput) {
        let mut services = HashMap::new();
        for (svc_id, svc) in &self.services {
            let wl_id = &svc.workload_id;
            let wl = self.workloads.get(wl_id);

            let (wl_state_str, pod_id, worker_id) = match wl.map(|w| &w.state) {
                Some(WorkloadState::Dormant) | None => {
                    ("dormant".to_string(), None, None)
                }
                Some(WorkloadState::WaitingForCapacity) => {
                    ("waiting_for_capacity".to_string(), None, None)
                }
                Some(WorkloadState::Launching {
                    pod_id, worker_id, ..
                }) => (
                    "launching".to_string(),
                    Some(pod_id.clone()),
                    Some(worker_id.clone()),
                ),
                Some(WorkloadState::Running {
                    pod_id, worker_id, ..
                }) => (
                    "running".to_string(),
                    Some(pod_id.clone()),
                    Some(worker_id.clone()),
                ),
            };

            let svc_state_str = match &svc.state {
                ServiceState::Pending => "pending".to_string(),
                ServiceState::Idle => "idle".to_string(),
                ServiceState::NeedBackend => "need_backend".to_string(),
                ServiceState::Active { .. } => "active".to_string(),
            };

            let backend_need = match &svc.state {
                ServiceState::Active { backend_need, .. } => Some(backend_need.clone()),
                _ => None,
            };

            services.insert(
                svc_id.clone(),
                ServiceStatusReport {
                    service_state: svc_state_str,
                    workload_id: wl_id.clone(),
                    workload_state: wl_state_str,
                    pod_id,
                    worker_id,
                    backend_need,
                    activation_enabled: svc.has_activation,
                },
            );
        }

        out.client_events.push((
            client_id,
            ClientEvent::NamespaceStatus {
                namespace_id: self.namespace_id.clone(),
                status: NamespaceStatusReport {
                    namespace_id: self.namespace_id.clone(),
                    status: self.status.clone(),
                    services,
                },
            },
        ));
    }

    fn handle_launch_pod(
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
            !self.pods.contains_key(pod_id),
            "Pod {:?} already exists in pods map — outer-layer bug",
            pod_id
        );
        self.pods.insert(
            pod_id.clone(),
            PodInfo {
                workload_id: workload_id.clone(),
                worker_id: worker_id.clone(),
            },
        );
        if let Some(ws) = self.workers.get_mut(worker_id) {
            ws.pods.insert(pod_id.clone());
        }

        // Send worker commands to create the service and launch the pod.
        let ns_id = self.namespace_id.clone();
        out.worker_commands.push((
            worker_id.clone(),
            WorkerCommand::CreateService {
                namespace_id: ns_id.clone(),
                service_id: svc_id.clone(),
                ip: svc_spec.ip,
                mac: svc_spec.mac,
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

    // --- WireGuard Connect/Disconnect ---

    fn handle_connect(
        &mut self,
        client_id: ClientId,
        client_public_key: [u8; 32],
        worker_wg_public_key: [u8; 32],
        worker_endpoint: String,
        out: &mut NamespaceOutput,
    ) {
        if self.status != NamespaceStatus::Active {
            out.client_events.push((
                client_id,
                ClientEvent::Error {
                    message: "namespace is not active".to_string(),
                },
            ));
            return;
        }

        match self.wg_peer_manager.connect(client_public_key) {
            ConnectResult::Ok { client_ip, outputs } => {
                self.apply_wg_outputs(outputs, out);
                let subnet_cidr = self.wg_peer_manager.subnet_cidr();
                out.client_events.push((
                    client_id,
                    ClientEvent::ConnectResult {
                        server_public_key: worker_wg_public_key,
                        endpoint: worker_endpoint,
                        client_ip: client_ip.to_string(),
                        subnet: subnet_cidr,
                    },
                ));
            }
            ConnectResult::Error { message } => {
                out.client_events.push((client_id, ClientEvent::Error { message }));
            }
        }
    }

    fn handle_disconnect(
        &mut self,
        client_id: ClientId,
        client_public_key: [u8; 32],
        out: &mut NamespaceOutput,
    ) {
        let outputs = self.wg_peer_manager.disconnect(client_public_key);
        self.apply_wg_outputs(outputs, out);
        out.client_events.push((client_id, ClientEvent::Ok));
    }

    fn apply_wg_outputs(&self, outputs: Vec<WgPeerOutput>, out: &mut NamespaceOutput) {
        for wg_out in outputs {
            match wg_out {
                WgPeerOutput::AddPeer { peer_public_key, peer_ip } => {
                    if let Some(worker_id) = self.active_worker_ids().into_iter().next() {
                        out.worker_commands.push((
                            worker_id,
                            WorkerCommand::AddWireGuardPeer {
                                namespace_id: self.namespace_id.clone(),
                                peer_public_key,
                                peer_ip,
                                preshared_key: None,
                            },
                        ));
                    }
                }
                WgPeerOutput::RemovePeer { peer_public_key } => {
                    for worker_id in self.active_worker_ids() {
                        out.worker_commands.push((
                            worker_id,
                            WorkerCommand::RemoveWireGuardPeer {
                                peer_public_key,
                            },
                        ));
                    }
                }
            }
        }
    }

    // --- Reconciliation ---

    fn reconcile_all_services(&mut self, out: &mut NamespaceOutput) {
        if self.status == NamespaceStatus::Destroying {
            return;
        }
        let svc_ids: Vec<ServiceId> = self.spec.services.keys().cloned().collect();
        for svc_id in svc_ids {
            self.reconcile_service(&svc_id, out);
        }
    }

    fn reconcile_service(&mut self, svc_id: &ServiceId, out: &mut NamespaceOutput) {
        let svc = match self.services.get(svc_id) {
            Some(s) => s,
            None => return,
        };
        let wl_id = svc.workload_id.clone();
        let has_activation = svc.has_activation;

        let wl = match self.workloads.get(&wl_id) {
            Some(w) => w,
            None => return,
        };

        let svc_state = &svc.state;
        let wl_state = &wl.state;

        match (svc_state, wl_state) {
            (ServiceState::Pending, WorkloadState::Dormant) => {
                if has_activation {
                    // Create the service on workers and move to Idle.
                    let ns_id = self.namespace_id.clone();
                    let svc_spec = self.spec.services.get(svc_id).cloned().unwrap();
                    for wid in self.active_worker_ids() {
                        out.worker_commands.push((
                            wid,
                            WorkerCommand::CreateService {
                                namespace_id: ns_id.clone(),
                                service_id: svc_id.clone(),
                                ip: svc_spec.ip,
                                mac: svc_spec.mac,
                                policy: svc_spec.policy.clone(),
                            },
                        ));
                    }
                    self.services.get_mut(svc_id).unwrap().state = ServiceState::Idle;
                } else {
                    // Always-on: demand up immediately.
                    self.services.get_mut(svc_id).unwrap().state = ServiceState::NeedBackend;
                    // Step workload with DemandUp.
                    let wl = self.workloads.get_mut(&wl_id).unwrap();
                    let wl_outputs = wl.step(WorkloadInput::DemandUp, &self.namespace_id);
                    self.forward_workload_outputs(&wl_id, wl_outputs, out);
                }
            }
            (ServiceState::NeedBackend, WorkloadState::Dormant) => {
                // Workload is dormant but service needs backend - demand up.
                let wl = self.workloads.get_mut(&wl_id).unwrap();
                let wl_outputs = wl.step(WorkloadInput::DemandUp, &self.namespace_id);
                self.forward_workload_outputs(&wl_id, wl_outputs, out);
            }
            _ => {}
        }
    }

    // --- Output Forwarding ---

    fn forward_service_outputs(
        &mut self,
        _service_id: &ServiceId,
        workload_id: &WorkloadId,
        outputs: Vec<ServiceOutput>,
        out: &mut NamespaceOutput,
    ) {
        for svc_out in outputs {
            match svc_out {
                ServiceOutput::DemandUp => {
                    let wl_outputs = if let Some(wl) = self.workloads.get_mut(workload_id) {
                        wl.step(WorkloadInput::DemandUp, &self.namespace_id)
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
                    self.forward_workload_outputs(workload_id, wl_outputs, out);
                }
                ServiceOutput::DemandDown => {
                    let wl_outputs = if let Some(wl) = self.workloads.get_mut(workload_id) {
                        wl.step(WorkloadInput::DemandDown, &self.namespace_id)
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
                    self.forward_workload_outputs(workload_id, wl_outputs, out);
                }
                ServiceOutput::WorkerCommand(wid, cmd) => {
                    out.worker_commands.push((wid, cmd));
                }
                ServiceOutput::BroadcastWorkerCommand(cmd) => {
                    for wid in self.active_worker_ids() {
                        out.worker_commands.push((wid, cmd.clone()));
                    }
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

    fn forward_workload_outputs(
        &mut self,
        workload_id: &WorkloadId,
        outputs: Vec<WorkloadOutput>,
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
                            pod_mac: wl_spec.network.mac,
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
                            self.forward_service_outputs(&sid, &wl_id, svc_outputs, out);
                        }
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
            }
        }
    }
}
