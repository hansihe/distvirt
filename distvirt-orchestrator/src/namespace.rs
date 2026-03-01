use std::collections::HashMap;
use std::time::Duration;

use crate::types::*;

pub struct NamespaceStateMachine {
    pub namespace_id: NamespaceId,
    pub spec: NamespaceSpec,
    pub status: NamespaceStatus,
    pub services: HashMap<ServiceId, ServiceState>,
    pub pods: HashMap<PodId, PodInfo>,
    pub workers: HashMap<WorkerId, NamespaceWorkerState>,
    pub next_pod_id: u64,
}

impl NamespaceStateMachine {
    pub fn new(namespace_id: NamespaceId, spec: NamespaceSpec) -> Self {
        let services = spec
            .services
            .keys()
            .map(|id| (id.clone(), ServiceState::Pending))
            .collect();

        NamespaceStateMachine {
            namespace_id,
            spec,
            status: NamespaceStatus::Creating,
            services,
            pods: HashMap::new(),
            workers: HashMap::new(),
            next_pod_id: 0,
        }
    }

    fn gen_pod_id(&mut self) -> PodId {
        let id = self.next_pod_id;
        self.next_pod_id += 1;
        PodId(format!("pod-{}", id))
    }

    fn select_worker_for_pod(&self) -> Option<WorkerId> {
        self.workers
            .iter()
            .find(|(_, ws)| ws.fabric_status == FabricStatus::Active)
            .map(|(wid, _)| wid.clone())
    }

    fn has_activation(&self, service_id: &ServiceId) -> bool {
        self.spec
            .services
            .get(service_id)
            .and_then(|s| s.activation.as_ref())
            .is_some()
    }

    fn idle_timeout_duration(&self, service_id: &ServiceId) -> Duration {
        self.spec
            .services
            .get(service_id)
            .and_then(|s| s.activation.as_ref())
            .map(|a| a.idle_timeout)
            .unwrap_or(Duration::from_secs(30))
    }

    fn active_worker_ids(&self) -> Vec<WorkerId> {
        self.workers
            .iter()
            .filter(|(_, ws)| ws.fabric_status == FabricStatus::Active)
            .map(|(wid, _)| wid.clone())
            .collect()
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
                service_id,
                worker_id,
            } => {
                self.handle_splice(client_id, service_id, worker_id, &mut out);
            }
            NamespaceInput::Unsplice {
                client_id,
                service_id,
            } => {
                self.handle_unsplice(client_id, service_id, &mut out);
            }
            NamespaceInput::StreamLogs {
                client_id,
                service_id,
            } => {
                self.handle_stream_logs(client_id, service_id, &mut out);
            }
            NamespaceInput::CapacityAvailable => {
                self.handle_capacity_available(&mut out);
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
                        self.reconcile_all_services(out);
                    }
                }
            }
            WorkerEvent::ServiceCreated { .. } => {
                // No-op: service creation is fire-and-forget.
            }
            WorkerEvent::ServiceActivation { service_id } => {
                if matches!(self.services.get(&service_id), Some(ServiceState::Idle)) {
                    self.try_launch_pod(&service_id, out);
                }
            }
            WorkerEvent::ServiceBackendNeed { service_id, need } => {
                let is_active = matches!(
                    self.services.get(&service_id),
                    Some(ServiceState::Active { .. })
                );
                if !is_active {
                    return;
                }

                let has_activation = self.has_activation(&service_id);
                let idle_duration = self.idle_timeout_duration(&service_id);

                if let Some(ServiceState::Active {
                    backend_need,
                    idle_timer,
                    ..
                }) = self.services.get_mut(&service_id)
                {
                    *backend_need = need.clone();
                    match need {
                        BackendNeed::None => {
                            if has_activation && idle_timer.is_none() {
                                let timer_key = TimerKey::IdleTimeout {
                                    service_id: service_id.clone(),
                                };
                                out.timers_set.push((timer_key.clone(), idle_duration));
                                *idle_timer = Some(timer_key);
                            }
                        }
                        BackendNeed::Traffic | BackendNeed::Active => {
                            if let Some(timer_key) = idle_timer.take() {
                                out.timers_cancel.push(timer_key);
                            }
                        }
                    }
                }
            }
            WorkerEvent::PodRunning { pod_id } => {
                let pod_info = match self.pods.get(&pod_id) {
                    Some(info) => info.clone(),
                    None => return,
                };
                let service_id = pod_info.service_id.clone();

                let is_launching = matches!(
                    self.services.get(&service_id),
                    Some(ServiceState::Launching { pod_id: pid, .. }) if *pid == pod_id
                );

                if is_launching {
                    // Cancel launch timeout.
                    if let Some(ServiceState::Launching {
                        launch_timeout, ..
                    }) = self.services.get(&service_id)
                    {
                        out.timers_cancel.push(launch_timeout.clone());
                    }

                    self.services.insert(
                        service_id.clone(),
                        ServiceState::Active {
                            pod_id: pod_id.clone(),
                            worker_id: pod_info.worker_id.clone(),
                            hosting: ServiceHosting::Normal,
                            backend_need: BackendNeed::Active,
                            idle_timer: None,
                        },
                    );

                    let ns_id = self.namespace_id.clone();
                    for wid in self.active_worker_ids() {
                        out.worker_commands.push((
                            wid.clone(),
                            WorkerCommand::UpdateServiceBackend {
                                namespace_id: ns_id.clone(),
                                service_id: service_id.clone(),
                                backend: Some(pod_id.clone()),
                            },
                        ));
                        out.worker_commands.push((
                            wid,
                            WorkerCommand::ServiceReady {
                                namespace_id: ns_id.clone(),
                                service_id: service_id.clone(),
                            },
                        ));
                    }
                }
            }
            WorkerEvent::PodExited { pod_id } | WorkerEvent::PodFailed { pod_id, .. } => {
                self.handle_pod_gone(&pod_id, out);
            }
        }
    }

    fn handle_pod_gone(&mut self, pod_id: &PodId, out: &mut NamespaceOutput) {
        let pod_info = match self.pods.remove(pod_id) {
            Some(info) => info,
            None => return,
        };

        if let Some(ws) = self.workers.get_mut(&pod_info.worker_id) {
            ws.pods.remove(pod_id);
        }

        let service_id = pod_info.service_id.clone();
        let service = self.services.get(&service_id).cloned();

        match service {
            Some(ServiceState::Launching {
                pod_id: pid,
                launch_timeout,
                ..
            }) if pid == *pod_id => {
                out.timers_cancel.push(launch_timeout);
                if self.has_activation(&service_id) {
                    self.services.insert(service_id, ServiceState::Idle);
                } else {
                    self.try_launch_pod(&service_id, out);
                }
            }
            Some(ServiceState::Active {
                pod_id: pid,
                idle_timer,
                ..
            }) if pid == *pod_id => {
                if let Some(timer_key) = idle_timer {
                    out.timers_cancel.push(timer_key);
                }
                let ns_id = self.namespace_id.clone();
                for wid in self.active_worker_ids() {
                    out.worker_commands.push((
                        wid,
                        WorkerCommand::UpdateServiceBackend {
                            namespace_id: ns_id.clone(),
                            service_id: service_id.clone(),
                            backend: None,
                        },
                    ));
                }
                if self.has_activation(&service_id) {
                    self.services.insert(service_id, ServiceState::Idle);
                } else {
                    self.try_launch_pod(&service_id, out);
                }
            }
            _ => {}
        }
    }

    fn reconcile_all_services(&mut self, out: &mut NamespaceOutput) {
        let service_ids: Vec<ServiceId> = self.spec.services.keys().cloned().collect();
        for svc_id in service_ids {
            self.reconcile_service(&svc_id, out);
        }
    }

    fn reconcile_service(&mut self, svc_id: &ServiceId, out: &mut NamespaceOutput) {
        let state = match self.services.get(svc_id) {
            Some(s) => s.clone(),
            None => return,
        };

        match state {
            ServiceState::Pending => {
                if self.has_activation(svc_id) {
                    let ns_id = self.namespace_id.clone();
                    let spec = self.spec.services.get(svc_id).cloned().unwrap();
                    for wid in self.active_worker_ids() {
                        out.worker_commands.push((
                            wid,
                            WorkerCommand::CreateService {
                                namespace_id: ns_id.clone(),
                                service_id: svc_id.clone(),
                                spec: spec.clone(),
                            },
                        ));
                    }
                    self.services.insert(svc_id.clone(), ServiceState::Idle);
                } else {
                    self.try_launch_pod(svc_id, out);
                }
            }
            ServiceState::WaitingForCapacity => {
                self.try_launch_pod(svc_id, out);
            }
            _ => {}
        }
    }

    fn try_launch_pod(&mut self, svc_id: &ServiceId, out: &mut NamespaceOutput) {
        let worker_id = match self.select_worker_for_pod() {
            Some(wid) => wid,
            None => {
                self.services
                    .insert(svc_id.clone(), ServiceState::WaitingForCapacity);
                out.capacity_requests.push(CapacityRequest {
                    service_id: svc_id.clone(),
                    memory_mb: 128,
                });
                return;
            }
        };

        let pod_id = self.gen_pod_id();
        let ns_id = self.namespace_id.clone();
        let spec = self.spec.services.get(svc_id).cloned().unwrap();

        self.pods.insert(
            pod_id.clone(),
            PodInfo {
                service_id: svc_id.clone(),
                worker_id: worker_id.clone(),
            },
        );
        if let Some(ws) = self.workers.get_mut(&worker_id) {
            ws.pods.insert(pod_id.clone());
        }

        out.worker_commands.push((
            worker_id.clone(),
            WorkerCommand::CreateService {
                namespace_id: ns_id.clone(),
                service_id: svc_id.clone(),
                spec,
            },
        ));
        out.worker_commands.push((
            worker_id.clone(),
            WorkerCommand::LaunchPod {
                namespace_id: ns_id.clone(),
                pod_id: pod_id.clone(),
                service_id: svc_id.clone(),
            },
        ));

        let launch_timeout = TimerKey::LaunchTimeout {
            service_id: svc_id.clone(),
            pod_id: pod_id.clone(),
        };
        out.timers_set
            .push((launch_timeout.clone(), Duration::from_secs(60)));

        self.services.insert(
            svc_id.clone(),
            ServiceState::Launching {
                pod_id,
                worker_id,
                launch_timeout,
            },
        );
    }

    fn handle_worker_lost(&mut self, worker_id: &WorkerId, out: &mut NamespaceOutput) {
        let lost_pods: Vec<(PodId, PodInfo)> = self
            .pods
            .iter()
            .filter(|(_, info)| info.worker_id == *worker_id)
            .map(|(pid, info)| (pid.clone(), info.clone()))
            .collect();

        for (pod_id, pod_info) in &lost_pods {
            let service_id = &pod_info.service_id;
            let service = self.services.get(service_id).cloned();

            match service {
                Some(ServiceState::Launching {
                    pod_id: ref pid,
                    launch_timeout,
                    worker_id: ref wid,
                }) if pid == pod_id && wid == worker_id => {
                    out.timers_cancel.push(launch_timeout);
                    if self.has_activation(service_id) {
                        self.services.insert(service_id.clone(), ServiceState::Idle);
                    } else {
                        self.services
                            .insert(service_id.clone(), ServiceState::Pending);
                    }
                }
                Some(ServiceState::Active {
                    pod_id: ref pid,
                    idle_timer,
                    worker_id: ref wid,
                    ..
                }) if pid == pod_id && wid == worker_id => {
                    if let Some(timer_key) = idle_timer {
                        out.timers_cancel.push(timer_key);
                    }
                    if self.has_activation(service_id) {
                        self.services.insert(service_id.clone(), ServiceState::Idle);
                    } else {
                        self.services
                            .insert(service_id.clone(), ServiceState::Pending);
                    }
                }
                _ => {}
            }
        }

        for (pod_id, _) in &lost_pods {
            self.pods.remove(pod_id);
        }

        self.workers.remove(worker_id);

        if self.workers.is_empty() && self.status == NamespaceStatus::Active {
            self.status = NamespaceStatus::Creating;
        } else if self.status == NamespaceStatus::Active {
            self.reconcile_all_services(out);
        }
    }

    fn handle_timer_fired(&mut self, timer_key: &TimerKey, out: &mut NamespaceOutput) {
        match timer_key {
            TimerKey::IdleTimeout { service_id } => {
                let service = self.services.get(service_id).cloned();
                if let Some(ServiceState::Active {
                    pod_id,
                    worker_id,
                    backend_need: BackendNeed::None,
                    idle_timer: Some(ref tk),
                    ..
                }) = service
                    && tk == timer_key
                    && self.has_activation(service_id)
                {
                    let ns_id = self.namespace_id.clone();
                    out.worker_commands.push((
                        worker_id.clone(),
                        WorkerCommand::StopPod {
                            namespace_id: ns_id.clone(),
                            pod_id: pod_id.clone(),
                        },
                    ));

                    for wid in self.active_worker_ids() {
                        out.worker_commands.push((
                            wid,
                            WorkerCommand::UpdateServiceBackend {
                                namespace_id: ns_id.clone(),
                                service_id: service_id.clone(),
                                backend: None,
                            },
                        ));
                    }

                    self.pods.remove(&pod_id);
                    if let Some(ws) = self.workers.get_mut(&worker_id) {
                        ws.pods.remove(&pod_id);
                    }

                    self.services.insert(service_id.clone(), ServiceState::Idle);
                }
            }
            TimerKey::LaunchTimeout {
                service_id,
                pod_id,
            } => {
                let service = self.services.get(service_id).cloned();
                if let Some(ServiceState::Launching {
                    pod_id: pid,
                    worker_id,
                    launch_timeout: tk,
                }) = service
                    && pid == *pod_id
                    && tk == *timer_key
                {
                    let ns_id = self.namespace_id.clone();
                    out.worker_commands.push((
                        worker_id.clone(),
                        WorkerCommand::StopPod {
                            namespace_id: ns_id.clone(),
                            pod_id: pod_id.clone(),
                        },
                    ));

                    self.pods.remove(pod_id);
                    if let Some(ws) = self.workers.get_mut(&worker_id) {
                        ws.pods.remove(pod_id);
                    }

                    if self.has_activation(service_id) {
                        self.services.insert(service_id.clone(), ServiceState::Idle);
                    } else {
                        self.try_launch_pod(service_id, out);
                    }
                }
            }
        }
    }

    fn handle_update_spec(
        &mut self,
        client_id: ClientId,
        spec: NamespaceSpec,
        out: &mut NamespaceOutput,
    ) {
        for svc_id in spec.services.keys() {
            if !self.services.contains_key(svc_id) {
                self.services.insert(svc_id.clone(), ServiceState::Pending);
            }
        }

        self.spec = spec;

        if self.status == NamespaceStatus::Active {
            self.reconcile_all_services(out);
        }

        out.client_events.push((client_id, ClientEvent::Ok));
    }

    fn handle_delete(&mut self, client_id: ClientId, out: &mut NamespaceOutput) {
        self.status = NamespaceStatus::Destroying;
        let ns_id = self.namespace_id.clone();

        for (pod_id, pod_info) in &self.pods {
            out.worker_commands.push((
                pod_info.worker_id.clone(),
                WorkerCommand::StopPod {
                    namespace_id: ns_id.clone(),
                    pod_id: pod_id.clone(),
                },
            ));
        }

        for wid in self.workers.keys() {
            out.worker_commands.push((
                wid.clone(),
                WorkerCommand::DestroyNamespace {
                    namespace_id: ns_id.clone(),
                },
            ));
        }

        for svc in self.services.values() {
            match svc {
                ServiceState::Launching {
                    launch_timeout, ..
                } => {
                    out.timers_cancel.push(launch_timeout.clone());
                }
                ServiceState::Active {
                    idle_timer: Some(tk),
                    ..
                } => {
                    out.timers_cancel.push(tk.clone());
                }
                _ => {}
            }
        }

        out.client_events.push((client_id, ClientEvent::Ok));
    }

    fn handle_get_status(&self, client_id: ClientId, out: &mut NamespaceOutput) {
        let mut services = HashMap::new();
        for (svc_id, svc_state) in &self.services {
            let (state_str, pod_id, worker_id, backend_need) = match svc_state {
                ServiceState::Pending => ("pending".to_string(), None, None, None),
                ServiceState::Idle => ("idle".to_string(), None, None, None),
                ServiceState::WaitingForCapacity => {
                    ("waiting_for_capacity".to_string(), None, None, None)
                }
                ServiceState::Launching {
                    pod_id, worker_id, ..
                } => (
                    "launching".to_string(),
                    Some(pod_id.clone()),
                    Some(worker_id.clone()),
                    None,
                ),
                ServiceState::Active {
                    pod_id,
                    worker_id,
                    backend_need,
                    ..
                } => (
                    "active".to_string(),
                    Some(pod_id.clone()),
                    Some(worker_id.clone()),
                    Some(backend_need.clone()),
                ),
            };

            let activation_enabled = self.has_activation(svc_id);
            let spliced = matches!(
                svc_state,
                ServiceState::Active {
                    hosting: ServiceHosting::Spliced { .. },
                    ..
                }
            );

            services.insert(
                svc_id.clone(),
                ServiceStatusReport {
                    state: state_str,
                    pod_id,
                    worker_id,
                    backend_need,
                    activation_enabled,
                    spliced,
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

    fn handle_splice(
        &mut self,
        client_id: ClientId,
        _service_id: ServiceId,
        _worker_id: WorkerId,
        out: &mut NamespaceOutput,
    ) {
        // TODO: implement splice flow
        out.client_events.push((client_id, ClientEvent::Ok));
    }

    fn handle_unsplice(
        &mut self,
        client_id: ClientId,
        _service_id: ServiceId,
        out: &mut NamespaceOutput,
    ) {
        // TODO: implement unsplice flow
        out.client_events.push((client_id, ClientEvent::Ok));
    }

    fn handle_stream_logs(
        &self,
        client_id: ClientId,
        _service_id: Option<ServiceId>,
        out: &mut NamespaceOutput,
    ) {
        // TODO: set up log streaming
        out.client_events.push((client_id, ClientEvent::Ok));
    }

    fn handle_capacity_available(&mut self, out: &mut NamespaceOutput) {
        if self.status == NamespaceStatus::Active {
            self.reconcile_all_services(out);
        }
    }
}
