//! Shared ID registry: maps router-internal numeric IDs to protocol-level string names.
//!
//! Wrapped in `Arc<RwLock<..>>` so that any consumer (gRPC, EventBus subscribers,
//! CLI formatters) can resolve IDs without going through the core or boundary layer.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::sm::{EndpointId, PodId, ServiceId, WorkloadId};
use crate::types::NamespaceId;

// =============================================================================
// Per-namespace ID registry map
// =============================================================================

/// Shared map of per-namespace ID registries.
/// Cheap to clone (Arc wrapper). Accessible from gRPC, event conversion, etc.
#[derive(Clone, Debug)]
pub struct IdRegistryMap {
    inner: Arc<RwLock<HashMap<NamespaceId, IdRegistry>>>,
}

impl IdRegistryMap {
    pub fn new() -> Self {
        IdRegistryMap {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get (or create) the registry for a namespace.
    pub fn get_or_create(&self, namespace_id: &NamespaceId) -> IdRegistry {
        let mut map = self.inner.write().unwrap();
        map.entry(namespace_id.clone())
            .or_insert_with(IdRegistry::new)
            .clone()
    }

    /// Get the registry for a namespace, if it exists.
    pub fn get(&self, namespace_id: &NamespaceId) -> Option<IdRegistry> {
        self.inner.read().unwrap().get(namespace_id).cloned()
    }

    /// Remove the registry for a namespace.
    pub fn remove(&self, namespace_id: &NamespaceId) {
        self.inner.write().unwrap().remove(namespace_id);
    }
}

// =============================================================================
// Per-namespace ID registry
// =============================================================================

/// The entity that owns an endpoint.
#[derive(Clone, Debug, PartialEq)]
pub enum EndpointOwner {
    Service(ServiceId),
    Workload(WorkloadId),
}

/// Interior state of the registry.
#[derive(Debug, Default)]
struct Inner {
    /// Router WorkloadId → protocol workload name.
    workload_names: HashMap<WorkloadId, String>,
    /// Router ServiceId → protocol service name.
    service_names: HashMap<ServiceId, String>,
    /// Router EndpointId → owning entity (service or workload).
    endpoint_owner: HashMap<EndpointId, EndpointOwner>,
    /// Router PodId → owning WorkloadId.
    pod_to_workload: HashMap<PodId, WorkloadId>,
}

/// Cheap-to-clone handle to the shared ID registry.
#[derive(Clone, Debug)]
pub struct IdRegistry {
    inner: Arc<RwLock<Inner>>,
}

impl IdRegistry {
    pub fn new() -> Self {
        IdRegistry {
            inner: Arc::new(RwLock::new(Inner::default())),
        }
    }

    // =========================================================================
    // Write API (called by management adapter / namespace core)
    // =========================================================================

    /// Register a workload name mapping.
    pub fn register_workload(&self, id: WorkloadId, name: String) {
        self.inner.write().unwrap().workload_names.insert(id, name);
    }

    /// Remove a workload name mapping.
    pub fn unregister_workload(&self, id: &WorkloadId) {
        let mut inner = self.inner.write().unwrap();
        inner.workload_names.remove(id);
        // Also remove any pods owned by this workload.
        inner.pod_to_workload.retain(|_, wl| wl != id);
        // Also remove any endpoints owned by this workload.
        inner
            .endpoint_owner
            .retain(|_, owner| !matches!(owner, EndpointOwner::Workload(w) if w == id));
    }

    /// Register a service name mapping.
    pub fn register_service(&self, id: ServiceId, name: String) {
        self.inner.write().unwrap().service_names.insert(id, name);
    }

    /// Remove a service name mapping.
    pub fn unregister_service(&self, id: &ServiceId) {
        let mut inner = self.inner.write().unwrap();
        inner.service_names.remove(id);
        // Also remove any endpoints owned by this service.
        inner
            .endpoint_owner
            .retain(|_, owner| !matches!(owner, EndpointOwner::Service(s) if s == id));
    }

    /// Set the endpoint → service mapping for an endpoint.
    pub fn register_service_endpoint(&self, endpoint_id: EndpointId, service_id: ServiceId) {
        self.inner
            .write()
            .unwrap()
            .endpoint_owner
            .insert(endpoint_id, EndpointOwner::Service(service_id));
    }

    /// Set the endpoint → workload mapping for an endpoint.
    pub fn register_workload_endpoint(&self, endpoint_id: EndpointId, workload_id: WorkloadId) {
        self.inner
            .write()
            .unwrap()
            .endpoint_owner
            .insert(endpoint_id, EndpointOwner::Workload(workload_id));
    }

    /// Set the pod → workload mapping for a pod.
    pub fn register_pod(&self, pod_id: PodId, workload_id: WorkloadId) {
        self.inner
            .write()
            .unwrap()
            .pod_to_workload
            .insert(pod_id, workload_id);
    }

    /// Remove a pod mapping.
    pub fn unregister_pod(&self, pod_id: &PodId) {
        self.inner.write().unwrap().pod_to_workload.remove(pod_id);
    }

    /// Remove an endpoint mapping.
    pub fn unregister_endpoint(&self, endpoint_id: &EndpointId) {
        self.inner.write().unwrap().endpoint_owner.remove(endpoint_id);
    }

    // =========================================================================
    // Read API (called by gRPC, event conversion, etc.)
    // =========================================================================

    /// Resolve a workload ID to its protocol name.
    pub fn workload_name(&self, id: &WorkloadId) -> Option<String> {
        self.inner.read().unwrap().workload_names.get(id).cloned()
    }

    /// Resolve a service ID to its protocol name.
    pub fn service_name(&self, id: &ServiceId) -> Option<String> {
        self.inner.read().unwrap().service_names.get(id).cloned()
    }

    /// Resolve an endpoint ID to its owner.
    pub fn endpoint_owner(&self, endpoint_id: &EndpointId) -> Option<EndpointOwner> {
        self.inner
            .read()
            .unwrap()
            .endpoint_owner
            .get(endpoint_id)
            .cloned()
    }

    /// Resolve an endpoint ID to its owning service's protocol name (if service-owned).
    pub fn endpoint_service_name(&self, endpoint_id: &EndpointId) -> Option<String> {
        let inner = self.inner.read().unwrap();
        match inner.endpoint_owner.get(endpoint_id) {
            Some(EndpointOwner::Service(svc_id)) => inner.service_names.get(svc_id).cloned(),
            _ => None,
        }
    }

    /// Resolve an endpoint ID to its owning workload's protocol name (if workload-owned).
    pub fn endpoint_workload_name(&self, endpoint_id: &EndpointId) -> Option<String> {
        let inner = self.inner.read().unwrap();
        match inner.endpoint_owner.get(endpoint_id) {
            Some(EndpointOwner::Workload(wl_id)) => inner.workload_names.get(wl_id).cloned(),
            _ => None,
        }
    }

    /// Resolve a pod ID to its owning workload's protocol name.
    pub fn pod_workload_name(&self, pod_id: &PodId) -> Option<String> {
        let inner = self.inner.read().unwrap();
        inner
            .pod_to_workload
            .get(pod_id)
            .and_then(|wl_id| inner.workload_names.get(wl_id))
            .cloned()
    }

    /// Resolve a pod ID to its owning workload ID.
    pub fn pod_workload_id(&self, pod_id: &PodId) -> Option<WorkloadId> {
        self.inner
            .read()
            .unwrap()
            .pod_to_workload
            .get(pod_id)
            .copied()
    }

    /// Resolve an endpoint ID to its owning service ID (if service-owned).
    pub fn endpoint_service_id(&self, endpoint_id: &EndpointId) -> Option<ServiceId> {
        match self.inner.read().unwrap().endpoint_owner.get(endpoint_id) {
            Some(EndpointOwner::Service(svc_id)) => Some(*svc_id),
            _ => None,
        }
    }
}
