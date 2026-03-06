mod commands;
mod events;
mod output;
mod reconciliation;
mod wireguard;

use std::collections::HashMap;
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
    pub workloads: HashMap<WorkloadId, WorkloadStateMachine>,
    pub services: HashMap<ServiceId, ServiceStateMachine>,
    pub service_workload: HashMap<ServiceId, WorkloadId>,
    pub pod_map: PodMap,
    pub workers: HashMap<WorkerId, NamespaceWorkerState>,
    /// WireGuard peer IP allocation and tracking.
    pub wg_peer_manager: WireGuardPeerManager,
}

impl NamespaceStateMachine {
    pub fn new(namespace_id: NamespaceId, spec: NamespaceSpec, segment_id: u16) -> Self {
        let mut workloads = HashMap::new();
        let mut services = HashMap::new();
        let mut service_workload = HashMap::new();

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
            workers: HashMap::new(),
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
        let mut services = HashMap::new();
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

    pub(crate) fn build_fabric_routes_for_worker(
        &self,
        target_worker_id: &WorkerId,
    ) -> Vec<distvirt_worker_protocol::FabricRouteEntry> {
        use distvirt_worker_protocol::{FabricRouteEntry, RouteDestination};
        self.pod_map
            .iter()
            .filter(|(_, info)| info.worker_id != *target_worker_id)
            .filter_map(|(_, info)| {
                let wl_spec = self.spec.workloads.get(&info.workload_id)?;
                Some(FabricRouteEntry {
                    ip: wl_spec.network.ip,
                    destination: RouteDestination::RemoteWorker {
                        worker_id: info.worker_id.clone(),
                    },
                })
            })
            .collect()
    }

    pub(crate) fn emit_fabric_route_sync(&self, out: &mut NamespaceOutput) {
        use crate::broadcast::broadcast_to_active_workers;
        let ns_id = self.namespace_id.clone();
        broadcast_to_active_workers(&self.workers, out, |wid| {
            let routes = self.build_fabric_routes_for_worker(wid);
            WorkerCommand::FabricRouteSync {
                namespace_id: ns_id.clone(),
                routes,
            }
        });
    }

    pub(crate) fn emit_fabric_route_sync_to_worker(
        &self,
        worker_id: &WorkerId,
        out: &mut NamespaceOutput,
    ) {
        use crate::broadcast::send_to_worker;
        let routes = self.build_fabric_routes_for_worker(worker_id);
        send_to_worker(worker_id, out, WorkerCommand::FabricRouteSync {
            namespace_id: self.namespace_id.clone(),
            routes,
        });
    }

    pub(crate) fn emit_fabric_route_add(
        &self,
        pod_ip: Ipv4Addr,
        pod_worker_id: &WorkerId,
        out: &mut NamespaceOutput,
    ) {
        use crate::broadcast::broadcast_to_active_workers_except;
        use distvirt_worker_protocol::{FabricRouteEntry, RouteDestination};
        let entry = FabricRouteEntry {
            ip: pod_ip,
            destination: RouteDestination::RemoteWorker {
                worker_id: pod_worker_id.clone(),
            },
        };
        let ns_id = self.namespace_id.clone();
        broadcast_to_active_workers_except(&self.workers, pod_worker_id, out, |_| {
            WorkerCommand::FabricRouteUpdate {
                namespace_id: ns_id.clone(),
                added: vec![entry.clone()],
                removed_ips: vec![],
            }
        });
    }

    /// Remove fabric route for a workload's IP when multi-worker.
    /// No-op if single-worker or workload not in spec.
    pub(crate) fn maybe_remove_fabric_route(&self, workload_id: &WorkloadId, out: &mut NamespaceOutput) {
        if self.workers.len() > 1 {
            if let Some(wl_spec) = self.spec.workloads.get(workload_id) {
                self.emit_fabric_route_remove(wl_spec.network.ip, out);
            }
        }
    }

    pub(crate) fn emit_fabric_route_remove(
        &self,
        pod_ip: Ipv4Addr,
        out: &mut NamespaceOutput,
    ) {
        use crate::broadcast::broadcast_to_active_workers;
        let ns_id = self.namespace_id.clone();
        broadcast_to_active_workers(&self.workers, out, |_| {
            WorkerCommand::FabricRouteUpdate {
                namespace_id: ns_id.clone(),
                added: vec![],
                removed_ips: vec![pod_ip],
            }
        });
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
