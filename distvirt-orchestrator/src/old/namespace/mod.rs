mod commands;
mod events;
mod output;
mod reconciliation;
mod wireguard;

use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;

use crate::pod_map::PodMap;
use crate::sm::service::ServiceStateMachine;
use crate::sm::workload::WorkloadStateMachine;
use crate::types::*;
use crate::wg_peers::WireGuardPeerManager;

/// Cached readiness info per workload, updated from BecameReady/BecameUnready outputs.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WorkloadReadyInfo {
    pub pod_id: PodId,
    pub worker_id: WorkerId,
}

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
    pub segment_id: u16,
    pub workloads: BTreeMap<WorkloadId, WorkloadStateMachine>,
    pub services: BTreeMap<ServiceId, ServiceStateMachine>,
    pub service_workload: BTreeMap<ServiceId, WorkloadId>,
    pub pod_map: PodMap,
    pub workers: BTreeMap<WorkerId, NamespaceWorkerState>,
    /// WireGuard peer IP allocation and tracking.
    pub wg_peer_manager: WireGuardPeerManager,
    /// Cached readiness info per workload, updated from BecameReady/BecameUnready outputs.
    pub workload_readiness: BTreeMap<WorkloadId, WorkloadReadyInfo>,
    /// Workloads with active network flows (demand signal from fabric).
    pub active_flows: BTreeSet<WorkloadId>,
}

impl NamespaceStateMachine {
    pub fn new(namespace_id: NamespaceId, spec: NamespaceSpec, segment_id: u16) -> Self {
        let mut workloads = BTreeMap::new();
        let mut services = BTreeMap::new();
        let mut service_workload = BTreeMap::new();

        for (wl_id, wl_spec) in &spec.workloads {
            // has_activation: true if workload has activation, or if it has services
            // (services drive demand — workload responds to SetDemand).
            // Only serviceless workloads without activation auto-start from construction.
            let has_services = spec.services.values().any(|s| s.workload_id == *wl_id);
            let has_activation = wl_spec.activation.is_some() || has_services;
            let (wl_sm, _init_outputs) =
                WorkloadStateMachine::new(wl_id.clone(), wl_spec.suspend_on_idle, has_activation);
            // Note: init_outputs are discarded here because the namespace is in
            // Creating state — no workers are connected yet to handle PodRequests.
            // The reconciliation pass after first worker join will re-drive demand.
            workloads.insert(wl_id.clone(), wl_sm);
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

        // Sync initial demand: services that start in NeedBackend contribute demand.
        // Step the workload SMs with SetDemand so their state is consistent.
        let ns_id_ref = &namespace_id;
        for (wl_id, wl) in workloads.iter_mut() {
            let service_demand: u32 = services
                .values()
                .filter(|svc| svc.workload_id == *wl_id && svc.wants_backend())
                .count() as u32;
            if service_demand > 0 {
                // Outputs (PodRequest) are discarded — no workers connected yet.
                let _ = wl.step(
                    crate::sm::workload::WorkloadInput::SetDemand {
                        count: service_demand,
                    },
                    ns_id_ref,
                );
            }
        }

        let wg_peer_manager =
            WireGuardPeerManager::new(spec.network.subnet, spec.network.prefix_len);

        NamespaceStateMachine {
            namespace_id,
            spec,
            status: NamespaceStatus::Creating,
            segment_id,
            workloads,
            services,
            service_workload,
            pod_map: PodMap::new(),
            workers: BTreeMap::new(),
            wg_peer_manager,
            workload_readiness: BTreeMap::new(),
            active_flows: BTreeSet::new(),
        }
    }

    pub(crate) fn active_worker_ids(&self) -> Vec<WorkerId> {
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

    pub fn status_report(&self) -> NamespaceStatusReport {
        // Workloads
        let mut workloads = BTreeMap::new();
        for (wl_id, wl) in &self.workloads {
            workloads.insert(
                wl_id.clone(),
                WorkloadStatusReport {
                    state: wl.state.as_str().to_string(),
                    pod_id: wl.state.pod_id().cloned(),
                    conditions: wl.conditions.clone(),
                },
            );
        }

        // Services
        let mut services = BTreeMap::new();
        for (svc_id, svc) in &self.services {
            let backend_need = match &svc.state {
                ServiceState::Active { backend_need, .. } => Some(backend_need.clone()),
                _ => None,
            };

            let ip = self
                .spec
                .services
                .get(svc_id)
                .map(|spec| spec.ip.to_string())
                .unwrap_or_default();

            services.insert(
                svc_id.clone(),
                ServiceStatusReport {
                    workload_id: svc.workload_id.clone(),
                    service_state: svc.state.as_str().to_string(),
                    backend_need,
                    activation_enabled: svc.has_activation,
                    ip,
                    conditions: svc.conditions.clone(),
                },
            );
        }

        // Pods
        let mut pods = BTreeMap::new();
        for (pod_id, info) in self.pod_map.iter() {
            let is_running =
                self.workloads
                    .get(&info.workload_id)
                    .map_or(false, |wl| match &wl.state {
                        WorkloadState::Active {
                            pod:
                                crate::sm::pod::PodSlot {
                                    pod_id: running_pod,
                                    pod_state: crate::sm::pod::PodState::Running,
                                    ..
                                },
                            ..
                        } => running_pod == pod_id,
                        _ => false,
                    });
            let state = if is_running {
                PodStatus::Running
            } else {
                PodStatus::Launching
            };
            let ip = self
                .spec
                .workloads
                .get(&info.workload_id)
                .map(|wl| wl.network.ip.to_string())
                .unwrap_or_default();
            pods.insert(
                pod_id.clone(),
                PodStatusReport {
                    pod_id: pod_id.clone(),
                    workload_id: info.workload_id.clone(),
                    worker_id: info.worker_id.clone(),
                    ip,
                    state,
                },
            );
        }

        NamespaceStatusReport {
            namespace_id: self.namespace_id.clone(),
            status: self.status.clone(),
            workloads,
            services,
            pods,
        }
    }

    /// Build the full set of endpoint specs for all workloads and services.
    pub(crate) fn build_endpoint_specs(&self) -> Vec<EndpointSpec> {
        let mut specs = Vec::new();

        // Pod endpoints: one per workload.
        for (wl_id, wl_spec) in &self.spec.workloads {
            let placement = self
                .pod_map
                .iter()
                .find(|(_, info)| info.workload_id == *wl_id)
                .map(|(_, info)| EndpointPlacement {
                    worker_id: info.worker_id.clone(),
                });

            specs.push(EndpointSpec {
                ip: wl_spec.network.ip,
                kind: EndpointKind::Pod { placement },
            });
        }

        // Service endpoints.
        for (svc_id, svc_spec) in &self.spec.services {
            let backend = self.build_service_backend(svc_id);
            specs.push(EndpointSpec {
                ip: svc_spec.ip,
                kind: EndpointKind::Service {
                    service_id: svc_id.clone(),
                    policy: svc_spec.policy.clone(),
                    backend,
                },
            });
        }

        // WireGuard peer endpoints.
        // All peers are placed on the first active worker (the one hosting
        // the WireGuard adapter). When no worker is active, peers are unplaced
        // and traffic will be buffered.
        let wg_worker = self.active_worker_ids().into_iter().next();
        for (_pubkey, peer_info) in &self.wg_peer_manager.peers {
            let placement = wg_worker.as_ref().map(|wid| EndpointPlacement {
                worker_id: wid.clone(),
            });
            specs.push(EndpointSpec {
                ip: peer_info.client_ip,
                kind: EndpointKind::WireGuardPeer { placement },
            });
        }

        specs
    }

    /// Derive the endpoint backend for a service from its current state.
    fn build_service_backend(&self, svc_id: &ServiceId) -> Option<EndpointPodBackend> {
        let svc = self.services.get(svc_id)?;
        match &svc.state {
            ServiceState::Active { worker_id, .. } => {
                let wl_spec = self.spec.workloads.get(&svc.workload_id)?;
                Some(EndpointPodBackend {
                    pod_ip: wl_spec.network.ip,
                    placement: Some(EndpointPlacement {
                        worker_id: worker_id.clone(),
                    }),
                    ready: true,
                })
            }
            _ => None,
        }
    }

    /// Broadcast a full EndpointSync to all active workers.
    pub(crate) fn emit_endpoint_sync(&self, out: &mut NamespaceOutput) {
        use crate::broadcast::broadcast_to_active_workers;
        let specs = self.build_endpoint_specs();
        let ns_id = self.namespace_id.clone();
        broadcast_to_active_workers(&self.workers, out, |_| WorkerCommand::EndpointSync {
            namespace_id: ns_id.clone(),
            endpoints: specs.clone(),
        });
    }

    /// Send a full EndpointSync to a single worker.
    pub(crate) fn emit_endpoint_sync_to_worker(
        &self,
        worker_id: &WorkerId,
        out: &mut NamespaceOutput,
    ) {
        use crate::broadcast::send_to_worker;
        let specs = self.build_endpoint_specs();
        send_to_worker(
            worker_id,
            out,
            WorkerCommand::EndpointSync {
                namespace_id: self.namespace_id.clone(),
                endpoints: specs,
            },
        );
    }

    /// Broadcast an incremental endpoint update for a workload's pod endpoint.
    pub(crate) fn emit_endpoint_update_for_workload(
        &self,
        workload_id: &WorkloadId,
        out: &mut NamespaceOutput,
    ) {
        if let Some(wl_spec) = self.spec.workloads.get(workload_id) {
            let placement = self
                .pod_map
                .iter()
                .find(|(_, info)| info.workload_id == *workload_id)
                .map(|(_, info)| EndpointPlacement {
                    worker_id: info.worker_id.clone(),
                });
            let spec = EndpointSpec {
                ip: wl_spec.network.ip,
                kind: EndpointKind::Pod { placement },
            };
            let ns_id = self.namespace_id.clone();
            use crate::broadcast::broadcast_to_active_workers;
            broadcast_to_active_workers(&self.workers, out, |_| WorkerCommand::EndpointUpdate {
                namespace_id: ns_id.clone(),
                upserted: vec![spec.clone()],
                removed_ips: vec![],
            });
        }
    }

    /// Broadcast an incremental endpoint update for a service endpoint.
    pub(crate) fn emit_endpoint_update_for_service(
        &self,
        service_id: &ServiceId,
        out: &mut NamespaceOutput,
    ) {
        if let Some(svc_spec) = self.spec.services.get(service_id) {
            let backend = self.build_service_backend(service_id);
            let spec = EndpointSpec {
                ip: svc_spec.ip,
                kind: EndpointKind::Service {
                    service_id: service_id.clone(),
                    policy: svc_spec.policy.clone(),
                    backend,
                },
            };
            let ns_id = self.namespace_id.clone();
            use crate::broadcast::broadcast_to_active_workers;
            broadcast_to_active_workers(&self.workers, out, |_| WorkerCommand::EndpointUpdate {
                namespace_id: ns_id.clone(),
                upserted: vec![spec.clone()],
                removed_ips: vec![],
            });
        }
    }

    pub(crate) fn emit_registry_sync(&self, out: &mut NamespaceOutput) {
        use crate::broadcast::broadcast_to_active_workers;
        let entries = self.build_registry_entries();
        let ns_id = self.namespace_id.clone();
        broadcast_to_active_workers(&self.workers, out, |_| WorkerCommand::RegistrySync {
            namespace_id: ns_id.clone(),
            entries: entries.clone(),
        });
    }

    pub(crate) fn emit_registry_sync_to_worker(
        &self,
        worker_id: &WorkerId,
        out: &mut NamespaceOutput,
    ) {
        use crate::broadcast::send_to_worker;
        let entries = self.build_registry_entries();
        send_to_worker(
            worker_id,
            out,
            WorkerCommand::RegistrySync {
                namespace_id: self.namespace_id.clone(),
                entries,
            },
        );
    }

    /// Pure state transition. No I/O.
    pub fn step(
        &mut self,
        input: NamespaceInput,
        placement_table: &mut PlacementTable,
    ) -> NamespaceOutput {
        let mut out = NamespaceOutput::default();

        match input {
            NamespaceInput::WorkerEvent { worker_id, event } => {
                self.handle_worker_event(&worker_id, event, placement_table, &mut out);
            }
            NamespaceInput::WorkerLost { worker_id } => {
                self.handle_worker_lost(&worker_id, placement_table, &mut out);
            }
            NamespaceInput::TimerFired { timer_key } => {
                self.handle_timer_fired(&timer_key, placement_table, &mut out);
            }
            NamespaceInput::UpdateSpec { client_id, spec } => {
                self.handle_update_spec(client_id, spec, placement_table, &mut out);
            }
            NamespaceInput::Delete { client_id } => {
                self.handle_delete(client_id, placement_table, &mut out);
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
                self.handle_launch_pod(
                    &workload_id,
                    &worker_id,
                    &pod_id,
                    placement_table,
                    &mut out,
                );
            }
            NamespaceInput::ResumePod {
                workload_id,
                worker_id,
                pod_id,
                artifact_id,
            } => {
                self.handle_resume_pod(
                    &workload_id,
                    &worker_id,
                    &pod_id,
                    &artifact_id,
                    placement_table,
                    &mut out,
                );
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
            NamespaceInput::DeactivateWorkload {
                client_id,
                workload_id,
            } => {
                self.handle_deactivate_workload(client_id, workload_id, placement_table, &mut out);
            }
            NamespaceInput::PreemptWorkload { workload_id } => {
                self.handle_preempt_workload(workload_id, placement_table, &mut out);
            }
        }

        out
    }
}
