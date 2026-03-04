mod commands;
mod events;
mod output;
mod reconciliation;
mod wireguard;

use std::collections::HashMap;
use std::net::Ipv4Addr;

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
            workloads,
            services,
            service_workload,
            pods: HashMap::new(),
            workers: HashMap::new(),
            wg_peer_manager,
        }
    }

    /// Remove a pod from the pods map and the owning worker's pod set.
    /// Returns the PodInfo if the pod existed.
    pub(crate) fn remove_pod(&mut self, pod_id: &PodId) -> Option<PodInfo> {
        let pod_info = self.pods.remove(pod_id)?;
        if let Some(ws) = self.workers.get_mut(&pod_info.worker_id) {
            ws.pods.remove(pod_id);
        }
        Some(pod_info)
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
                },
            );
        }

        NamespaceStatusReport {
            namespace_id: self.namespace_id.clone(),
            status: self.status.clone(),
            services,
        }
    }

    pub(crate) fn emit_registry_sync(&self, out: &mut NamespaceOutput) {
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
            NamespaceInput::ResumePod {
                workload_id,
                worker_id,
                pod_id,
                snapshot_id,
            } => {
                self.handle_resume_pod(&workload_id, &worker_id, &pod_id, &snapshot_id, &mut out);
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
                self.handle_deactivate_workload(client_id, workload_id, &mut out);
            }
        }

        out
    }
}
