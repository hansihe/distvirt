mod commands;
mod events;
mod output;
mod reconciliation;
mod wireguard;

use std::collections::BTreeMap;
use std::net::Ipv4Addr;

use crate::pod_map::PodMap;
use crate::service::ServiceStateMachine;
use crate::types::*;
use crate::wg_peers::WireGuardPeerManager;
use crate::workload::WorkloadStateMachine;

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
}

impl NamespaceStateMachine {
    pub fn new(namespace_id: NamespaceId, spec: NamespaceSpec, segment_id: u16) -> Self {
        let mut workloads = BTreeMap::new();
        let mut services = BTreeMap::new();
        let mut service_workload = BTreeMap::new();

        for (wl_id, wl_spec) in &spec.workloads {
            workloads.insert(
                wl_id.clone(),
                WorkloadStateMachine::new(wl_id.clone(), wl_spec.suspend_on_idle),
            );
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
            segment_id,
            workloads,
            services,
            service_workload,
            pod_map: PodMap::new(),
            workers: BTreeMap::new(),
            wg_peer_manager,
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
        let mut services = BTreeMap::new();
        for (svc_id, svc) in &self.services {
            let wl_id = &svc.workload_id;
            let wl = self.workloads.get(wl_id);

            let wl_state = wl.map(|w| &w.state);
            let wl_state_str = wl_state.map_or("dormant", |s| s.as_str()).to_string();
            let pod_id = wl_state.and_then(|s| s.pod_id()).cloned();
            let worker_id = wl_state.and_then(|s| s.worker_id()).cloned();

            let backend_need = match &svc.state {
                ServiceState::Active { backend_need, .. } => Some(backend_need.clone()),
                _ => None,
            };

            let ip = self.spec.services.get(svc_id)
                .map(|spec| spec.ip.to_string())
                .unwrap_or_default();

            let workload_conditions = wl.map(|w| w.conditions.clone()).unwrap_or_default();
            let service_conditions = svc.conditions.clone();

            services.insert(
                svc_id.clone(),
                ServiceStatusReport {
                    service_state: svc.state.as_str().to_string(),
                    workload_id: wl_id.clone(),
                    workload_state: wl_state_str,
                    pod_id,
                    worker_id,
                    backend_need,
                    activation_enabled: svc.has_activation,
                    ip,
                    workload_conditions,
                    service_conditions,
                },
            );
        }

        NamespaceStatusReport {
            namespace_id: self.namespace_id.clone(),
            status: self.status.clone(),
            services,
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

        specs
    }

    /// Derive the endpoint backend for a service from its current state.
    fn build_service_backend(&self, svc_id: &ServiceId) -> Option<EndpointPodBackend> {
        let svc = self.services.get(svc_id)?;
        match &svc.state {
            ServiceState::Active {
                worker_id, ..
            } => {
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
        broadcast_to_active_workers(&self.workers, out, |_| {
            WorkerCommand::EndpointSync {
                namespace_id: ns_id.clone(),
                endpoints: specs.clone(),
            }
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
            broadcast_to_active_workers(&self.workers, out, |_| {
                WorkerCommand::EndpointUpdate {
                    namespace_id: ns_id.clone(),
                    upserted: vec![spec.clone()],
                    removed_ips: vec![],
                }
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
            broadcast_to_active_workers(&self.workers, out, |_| {
                WorkerCommand::EndpointUpdate {
                    namespace_id: ns_id.clone(),
                    upserted: vec![spec.clone()],
                    removed_ips: vec![],
                }
            });
        }
    }

    pub(crate) fn emit_registry_sync(&self, out: &mut NamespaceOutput) {
        use crate::broadcast::broadcast_to_active_workers;
        let entries = self.build_registry_entries();
        let ns_id = self.namespace_id.clone();
        broadcast_to_active_workers(&self.workers, out, |_| {
            WorkerCommand::RegistrySync {
                namespace_id: ns_id.clone(),
                entries: entries.clone(),
            }
        });
    }

    pub(crate) fn emit_registry_sync_to_worker(
        &self,
        worker_id: &WorkerId,
        out: &mut NamespaceOutput,
    ) {
        use crate::broadcast::send_to_worker;
        let entries = self.build_registry_entries();
        send_to_worker(worker_id, out, WorkerCommand::RegistrySync {
            namespace_id: self.namespace_id.clone(),
            entries,
        });
    }

    /// Pure state transition. No I/O.
    pub fn step(&mut self, input: NamespaceInput, placement_table: &mut PlacementTable) -> NamespaceOutput {
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
                self.handle_launch_pod(&workload_id, &worker_id, &pod_id, placement_table, &mut out);
            }
            NamespaceInput::ResumePod {
                workload_id,
                worker_id,
                pod_id,
                artifact_id,
            } => {
                self.handle_resume_pod(&workload_id, &worker_id, &pod_id, &artifact_id, placement_table, &mut out);
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
        }

        out
    }
}
