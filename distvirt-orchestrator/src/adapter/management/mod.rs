use std::collections::HashMap;

use crate::sm_new::{
    AdminCmd, ManagementId, Router, ServiceSm, ServiceSpec as SmServiceSpec, WorkloadId,
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
    proto_to_router_svc: HashMap<String, crate::sm_new::ServiceId>,
    /// Router ID → protocol name
    router_to_proto_svc: HashMap<crate::sm_new::ServiceId, String>,

    /// Management port per workload
    workload_mgmt: HashMap<WorkloadId, ManagementId>,
    /// Management port per service
    service_mgmt: HashMap<crate::sm_new::ServiceId, ManagementId>,

}

impl ManagementAdapter {
    pub(crate) fn new() -> Self {
        ManagementAdapter {
            next_workload_id: 1,
            next_service_id: 1,
            proto_to_router_wl: HashMap::new(),
            router_to_proto_wl: HashMap::new(),
            proto_to_router_svc: HashMap::new(),
            router_to_proto_svc: HashMap::new(),
            workload_mgmt: HashMap::new(),
            service_mgmt: HashMap::new(),
        }
    }

    /// Apply a new namespace spec, diffing against the previous state.
    /// Creates/updates/removes workloads and services in the router.
    pub(crate) fn apply_namespace_spec(
        &mut self,
        router: &mut Router,
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
                router.set_management_to_workload_edges(mgmt_id, vec![router_id]);
                router.set_management_wl_spec(mgmt_id, Self::to_sm_workload_spec(spec));

                self.proto_to_router_wl.insert(name_str.clone(), router_id);
                self.router_to_proto_wl.insert(router_id, name_str.clone());
                self.workload_mgmt.insert(router_id, mgmt_id);
            }
        }

        // Removed workloads
        for name in old_workloads.keys() {
            let name_str = &name.0;
            if !new.workloads.contains_key(name) {
                if let Some(router_id) = self.proto_to_router_wl.remove(name_str) {
                    self.router_to_proto_wl.remove(&router_id);
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
            let name_str = name.as_ref();
            if let Some(&router_id) = self.proto_to_router_svc.get(name_str) {
                // Update: re-set the spec signal
                let mgmt_id = self.service_mgmt[&router_id];
                router.set_management_svc_spec(
                    mgmt_id,
                    self.to_sm_service_spec(spec),
                );
            } else {
                // Create new service
                let router_id = crate::sm_new::ServiceId(self.next_service_id);
                self.next_service_id += 1;

                let mgmt_id = router.create_management();
                let has_activation = spec.activation.is_some();
                router.create_service(router_id, ServiceSm::new(has_activation));
                router.set_management_to_service_edges(mgmt_id, vec![router_id]);
                router.set_management_svc_spec(
                    mgmt_id,
                    self.to_sm_service_spec(spec),
                );

                self.proto_to_router_svc.insert(name_str.to_owned(), router_id);
                self.router_to_proto_svc.insert(router_id, name_str.to_owned());
                self.service_mgmt.insert(router_id, mgmt_id);
            }
        }

        // Removed services
        for name in old_services.keys() {
            let name_str = name.as_ref();
            if !new.services.contains_key(name) {
                if let Some(router_id) = self.proto_to_router_svc.remove(name_str) {
                    self.router_to_proto_svc.remove(&router_id);
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
    pub fn lookup_service(&self, proto_name: &str) -> Option<crate::sm_new::ServiceId> {
        self.proto_to_router_svc.get(proto_name).copied()
    }

    /// Look up a workload's protocol name by router ID.
    pub(crate) fn workload_proto_name(&self, id: &WorkloadId) -> Option<&str> {
        self.router_to_proto_wl.get(id).map(|s| s.as_str())
    }

    /// Look up a service's protocol name by router ID.
    #[allow(dead_code)]
    pub(crate) fn service_proto_name(&self, id: &crate::sm_new::ServiceId) -> Option<&str> {
        self.router_to_proto_svc.get(id).map(|s| s.as_str())
    }

    /// Send an admin command to a workload by protocol name.
    pub(crate) fn send_admin_command(
        &self,
        router: &mut Router,
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
        router: &mut Router,
        service_name: &str,
        active: bool,
    ) {
        if let Some(&router_id) = self.proto_to_router_svc.get(service_name) {
            if let Some(&mgmt_id) = self.service_mgmt.get(&router_id) {
                router.send_activate_service(mgmt_id, router_id, active);
            }
        }
    }

    fn to_sm_workload_spec(spec: &crate::types::WorkloadSpec) -> SmWorkloadSpec {
        SmWorkloadSpec {
            image: spec
                .containers
                .first()
                .map(|c| c.image_ref.clone())
                .unwrap_or_default(),
        }
    }

    fn to_sm_service_spec(&self, spec: &crate::types::ServiceSpec) -> SmServiceSpec {
        let workload_router_id = self
            .proto_to_router_wl
            .get(&spec.workload_id.0)
            .copied()
            .unwrap_or(WorkloadId(0));
        SmServiceSpec {
            workload: workload_router_id,
            has_activation: spec.activation.is_some(),
        }
    }
}
