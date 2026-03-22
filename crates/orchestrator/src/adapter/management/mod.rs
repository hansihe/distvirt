use std::collections::HashMap;

use crate::id_registry::IdRegistry;
use crate::sm::{
    AdminCmd, DRouter, ManagementId, ServiceSm, ServiceSpec as SmServiceSpec, WorkloadId,
    WorkloadSm, WorkloadSpec as SmWorkloadSpec,
};

#[cfg(test)]
mod tests;

/// Pure adapter: processes client-provided namespace specs and mutates the router.
/// Maintains bidirectional maps between protocol string names and router numeric IDs.
pub struct ManagementAdapter {
    next_workload_id: u64,
    next_service_id: u64,

    /// Protocol name → router ID
    proto_to_router_wl: HashMap<String, WorkloadId>,
    /// Router ID → protocol name
    router_to_proto_wl: HashMap<WorkloadId, String>,
    /// Protocol name → router ID
    proto_to_router_svc: HashMap<String, crate::sm::ServiceId>,
    /// Router ID → protocol name
    router_to_proto_svc: HashMap<crate::sm::ServiceId, String>,

    /// Management port per workload
    workload_mgmt: HashMap<WorkloadId, ManagementId>,
    /// Management port per service
    service_mgmt: HashMap<crate::sm::ServiceId, ManagementId>,

    /// Shared ID registry for external consumers.
    id_registry: IdRegistry,
}

impl ManagementAdapter {
    pub(crate) fn new(id_registry: IdRegistry) -> Self {
        ManagementAdapter {
            next_workload_id: 1,
            next_service_id: 1,
            proto_to_router_wl: HashMap::new(),
            router_to_proto_wl: HashMap::new(),
            proto_to_router_svc: HashMap::new(),
            router_to_proto_svc: HashMap::new(),
            workload_mgmt: HashMap::new(),
            service_mgmt: HashMap::new(),
            id_registry,
        }
    }

    /// Apply a new namespace spec, diffing against the previous state.
    /// Creates/updates/removes workloads and services in the router.
    pub(crate) fn apply_namespace_spec(
        &mut self,
        router: &mut DRouter,
        old: Option<&crate::types::NamespaceSpec>,
        new: &crate::types::NamespaceSpec,
    ) {
        let empty_wl = std::collections::BTreeMap::new();
        let empty_svc = std::collections::BTreeMap::new();
        let old_workloads = old.map(|s| &s.workloads).unwrap_or(&empty_wl);
        let old_services = old.map(|s| &s.services).unwrap_or(&empty_svc);

        // --- Workloads ---

        // New or updated workloads
        for (name, spec) in &new.workloads {
            let name_str = &name.0;
            if let Some(&router_id) = self.proto_to_router_wl.get(name_str) {
                // Update: re-set the spec signal
                let mgmt_id = self.workload_mgmt[&router_id];
                router.set_management_wl_spec(mgmt_id, Self::to_sm_workload_spec(spec));
            } else {
                // Create new workload
                let router_id = WorkloadId(self.next_workload_id);
                self.next_workload_id += 1;

                let mgmt_id = router.create_management();
                router.create_workload(router_id, WorkloadSm::new());
                router.set_workload_config_edges(mgmt_id, vec![router_id]);
                router.set_management_wl_spec(mgmt_id, Self::to_sm_workload_spec(spec));

                self.proto_to_router_wl.insert(name_str.clone(), router_id);
                self.router_to_proto_wl.insert(router_id, name_str.clone());
                self.workload_mgmt.insert(router_id, mgmt_id);
                self.id_registry.register_workload(router_id, name_str.clone());
            }
        }

        // Removed workloads
        for name in old_workloads.keys() {
            let name_str = &name.0;
            if !new.workloads.contains_key(name) {
                if let Some(router_id) = self.proto_to_router_wl.remove(name_str) {
                    self.router_to_proto_wl.remove(&router_id);
                    self.id_registry.unregister_workload(&router_id);
                    if let Some(mgmt_id) = self.workload_mgmt.remove(&router_id) {
                        // Destroying the management port removes the spec signal,
                        // which causes the WorkloadSm to self-destruct.
                        router.destroy_management(mgmt_id);
                    }
                }
            }
        }

        // --- Services ---

        // New or updated services
        for (name, spec) in &new.services {
            let name_str = name.as_str();
            if let Some(&router_id) = self.proto_to_router_svc.get(name_str) {
                // Update: re-set the spec signal
                let mgmt_id = self.service_mgmt[&router_id];
                router.set_management_svc_spec(mgmt_id, self.to_sm_service_spec(name_str, spec));
            } else {
                // Create new service
                let router_id = crate::sm::ServiceId(self.next_service_id);
                self.next_service_id += 1;

                let mgmt_id = router.create_management();
                let _has_activation = spec.has_activation;
                router.create_service(router_id, ServiceSm::new());
                router.set_service_config_edges(mgmt_id, vec![router_id]);
                router.set_management_svc_spec(mgmt_id, self.to_sm_service_spec(name_str, spec));

                self.proto_to_router_svc
                    .insert(name_str.to_owned(), router_id);
                self.router_to_proto_svc
                    .insert(router_id, name_str.to_owned());
                self.service_mgmt.insert(router_id, mgmt_id);
                self.id_registry.register_service(router_id, name_str.to_owned());
            }
        }

        // Removed services
        for name in old_services.keys() {
            let name_str = name.as_str();
            if !new.services.contains_key(name) {
                if let Some(router_id) = self.proto_to_router_svc.remove(name_str) {
                    self.router_to_proto_svc.remove(&router_id);
                    self.id_registry.unregister_service(&router_id);
                    if let Some(mgmt_id) = self.service_mgmt.remove(&router_id) {
                        router.destroy_management(mgmt_id);
                    }
                }
            }
        }
    }

    /// Look up a workload's router ID by protocol name.
    pub fn lookup_workload(&self, proto_name: &str) -> Option<WorkloadId> {
        self.proto_to_router_wl.get(proto_name).copied()
    }

    /// Look up a service's router ID by protocol name.
    pub fn lookup_service(&self, proto_name: &str) -> Option<crate::sm::ServiceId> {
        self.proto_to_router_svc.get(proto_name).copied()
    }

    /// Iterate over all workloads: (protocol name, router ID).
    pub fn iter_workloads(&self) -> impl Iterator<Item = (&str, WorkloadId)> + '_ {
        self.proto_to_router_wl.iter().map(|(name, &id)| (name.as_str(), id))
    }

    /// Iterate over all services: (protocol name, router ID).
    pub fn iter_services(&self) -> impl Iterator<Item = (&str, crate::sm::ServiceId)> + '_ {
        self.proto_to_router_svc.iter().map(|(name, &id)| (name.as_str(), id))
    }

    /// Look up a workload's protocol name by router ID.
    pub(crate) fn workload_proto_name(&self, id: &WorkloadId) -> Option<&str> {
        self.router_to_proto_wl.get(id).map(|s| s.as_str())
    }

    /// Look up a service's protocol name by router ID.
    #[allow(dead_code)]
    pub(crate) fn service_proto_name(&self, id: &crate::sm::ServiceId) -> Option<&str> {
        self.router_to_proto_svc.get(id).map(|s| s.as_str())
    }

    /// Send an admin command to a workload by protocol name.
    pub(crate) fn send_admin_command(
        &self,
        router: &mut DRouter,
        workload_name: &str,
        cmd: AdminCmd,
    ) {
        if let Some(&router_id) = self.proto_to_router_wl.get(workload_name) {
            if let Some(&mgmt_id) = self.workload_mgmt.get(&router_id) {
                router.send_admin_command(mgmt_id, router_id, cmd);
            }
        }
    }

    /// Send an activate/deactivate command to a service by protocol name.
    pub(crate) fn send_activate_service(
        &self,
        router: &mut DRouter,
        service_name: &str,
        active: bool,
    ) {
        if let Some(&router_id) = self.proto_to_router_svc.get(service_name) {
            if let Some(&mgmt_id) = self.service_mgmt.get(&router_id) {
                router.send_activate_service(mgmt_id, router_id, active);
            }
        }
    }

    /// Update the ID registry with dynamic endpoint→owner and pod→workload mappings.
    /// Called after each reconcile cycle once the router has converged.
    pub(crate) fn sync_dynamic_ids(&self, router: &DRouter) {
        // Service → Endpoint: each ServiceSm stores its endpoint_id.
        for (_name, &svc_id) in &self.proto_to_router_svc {
            if let Some(svc_sm) = router.get_service(&svc_id) {
                if let Some(ep_id) = svc_sm.endpoint_id {
                    self.id_registry.register_service_endpoint(ep_id, svc_id);
                }
            }
        }

        // Workload → Endpoint: each WorkloadSm stores its endpoint_id.
        for (_name, &wl_id) in &self.proto_to_router_wl {
            if let Some(wl_sm) = router.get_workload(&wl_id) {
                if let Some(ep_id) = wl_sm.endpoint_id {
                    self.id_registry.register_workload_endpoint(ep_id, wl_id);
                }
            }
        }

        // Workload → Pod: each WorkloadSm stores its pod_id.
        for (_name, &wl_id) in &self.proto_to_router_wl {
            if let Some(wl_sm) = router.get_workload(&wl_id) {
                if let Some(pod_id) = wl_sm.pod_id {
                    self.id_registry.register_pod(pod_id, wl_id);
                }
            }
        }
    }

    pub(crate) fn to_sm_workload_spec(spec: &crate::types::WorkloadSpec) -> SmWorkloadSpec {
        SmWorkloadSpec {
            pod_spec: crate::sm::PodSpec {
                image: spec
                    .containers
                    .first()
                    .map(|c| c.image_ref.clone())
                    .unwrap_or_default(),
                network: Some(spec.network.clone()),
                containers: spec.containers.clone(),
                resources: spec.resources.as_ref().map(|r| {
                    distvirt_worker_protocol::ResourceRequirements {
                        requests: r.requests.as_ref().map(|v| {
                            distvirt_worker_protocol::ResourceValues {
                                memory_mib: v.memory_mib,
                                vcpus: v.vcpus,
                            }
                        }),
                        limits: r.limits.as_ref().map(|v| {
                            distvirt_worker_protocol::ResourceValues {
                                memory_mib: v.memory_mib,
                                vcpus: v.vcpus,
                            }
                        }),
                    }
                }),
                volumes: spec.volumes.clone(),
            },
            config: crate::sm::WorkloadConfig {
                suspend_on_idle: spec.suspend_on_idle,
                run_policy: spec.run_policy.clone(),
                respects_demand: spec.respects_demand,
                activation: spec.activation.clone(),
            },
        }
    }

    fn to_sm_service_spec(&self, name: &str, spec: &crate::types::ServiceSpec) -> SmServiceSpec {
        let workload_router_id = self
            .proto_to_router_wl
            .get(&spec.workload_id.0)
            .copied()
            .unwrap_or(WorkloadId(0));

        // Build worker-protocol ServicePolicy from orchestrator port config.
        let worker_ports = spec
            .ports
            .iter()
            .map(|p| distvirt_worker_protocol::PortConfig {
                port: p.port,
                target_port: p.target_port,
                activator: p.activator.as_ref().map(|a| match a {
                    crate::types::ActivatorKind::Tcp { max_flows } => {
                        distvirt_worker_protocol::ActivatorConfig::Tcp {
                            max_flows: *max_flows,
                        }
                    }
                    crate::types::ActivatorKind::Http2 => {
                        distvirt_worker_protocol::ActivatorConfig::Http2
                    }
                }),
            })
            .collect();

        let policy = distvirt_worker_protocol::ServicePolicy {
            ports: worker_ports,
            buffer_frames: spec.buffer_frames,
            timeout_ms: spec.buffer_timeout_ms,
        };

        SmServiceSpec {
            workload: workload_router_id,
            has_activation: spec.has_activation,
            idle_timeout: spec.idle_timeout,
            dns_name: Some(name.to_owned()),
            dns_ip: Some(spec.ip),
            ip: spec.ip,
            policy,
        }
    }
}
