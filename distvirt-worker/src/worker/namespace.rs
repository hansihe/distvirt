use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};

use distvirt_activator::{ActivatorInstance, ActivatorRuntime, FlowTracker, StreamManager, StreamManagerConfig};
use crate::fabric::ServiceProcessor;
use distvirt_worker_protocol::{
    ActivatorConfig, NamespaceId, PodId, RegistryEntry, ServiceId, ServicePolicy, WorkerEvent,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::adapter::{AdapterManager, AdapterPortHandle};
use crate::fabric::{Fabric, FabricContextInner, FabricEvent, FabricPort, DnsRegistry, FabricGateway};
use super::supervisor::{send_event, PodState, STOP_POD_TIMEOUT};
use crate::task_handle::TaskHandle;

/// Errors that should kill the entire worker.
/// INTENTIONALLY no From<anyhow::Error> — forces explicit construction.
#[derive(Debug)]
pub(crate) enum FatalError {
    InternalInvariant(String),
}

impl fmt::Display for FatalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FatalError::InternalInvariant(msg) => {
                write!(f, "internal invariant violated: {}", msg)
            }
        }
    }
}

/// Per-namespace state: fabric, gateway, registry, pods, and cancellation token.
///
/// If we add more namespace-level tasks beyond the gateway (e.g. health checks,
/// metrics collection), consider extracting a `NamespaceSupervisor` to formalize
/// the one-for-all supervision pattern instead of growing this struct.
pub(crate) struct NamespaceState {
    pub(crate) fabric: Arc<Fabric<FabricPort>>,
    pub(crate) tables: Arc<FabricContextInner<FabricPort>>,
    _gateway_task: TaskHandle<()>,
    _event_bridge_task: TaskHandle<()>,
    _adapter_tasks: Vec<TaskHandle<()>>,
    _adapter_ports: Vec<AdapterPortHandle>,
    pub(crate) registry: DnsRegistry,
    pub(crate) pods: HashMap<PodId, PodState>,
    pub(crate) token: CancellationToken,
}

impl NamespaceState {
    /// Test-only constructor that builds a NamespaceState from pre-built parts.
    #[cfg(test)]
    pub(crate) fn new_for_test(
        fabric: Arc<Fabric<FabricPort>>,
        tables: Arc<FabricContextInner<FabricPort>>,
        gateway_task: TaskHandle<()>,
        event_bridge_task: TaskHandle<()>,
        adapter_tasks: Vec<TaskHandle<()>>,
        adapter_ports: Vec<AdapterPortHandle>,
        registry: DnsRegistry,
        pods: HashMap<PodId, PodState>,
        token: CancellationToken,
    ) -> Self {
        NamespaceState {
            fabric,
            tables,
            _gateway_task: gateway_task,
            _event_bridge_task: event_bridge_task,
            _adapter_tasks: adapter_tasks,
            _adapter_ports: adapter_ports,
            registry,
            pods,
            token,
        }
    }

    /// Create a new namespace with fabric, gateway, event bridge, and adapter ports.
    ///
    /// Returns the namespace state and a `NamespaceCreated` event to send.
    pub(crate) fn new(
        worker_token: &CancellationToken,
        bg_event_tx: &mpsc::Sender<WorkerEvent>,
        adapter_manager: &AdapterManager,
        namespace_id: &NamespaceId,
        network: distvirt_worker_protocol::NetworkConfig,
    ) -> Result<(NamespaceState, WorkerEvent), FatalError> {
        let fabric = Fabric::<FabricPort>::new();

        let registry: DnsRegistry = Arc::new(RwLock::new(HashMap::new()));

        // Set up fabric event channel for route miss reporting.
        let (fabric_event_tx, mut fabric_event_rx) = mpsc::channel::<FabricEvent>(64);
        fabric.set_event_channel(fabric_event_tx);

        let tables = fabric.tables();

        let pod_gateway_ip = network.gateway.octets();
        let (gateway, egress_tx, ingress_rx) =
            FabricGateway::new(Arc::clone(&registry), pod_gateway_ip, network.prefix_len)
                .map_err(|e| {
                    FatalError::InternalInvariant(format!("create fabric gateway: {:#}", e))
                })?;
        fabric.set_gateway(egress_tx, ingress_rx);

        let ns_token = worker_token.child_token();
        let ns_cancel = ns_token.clone();
        let gateway_ns_id = namespace_id.clone();

        let gateway_event_tx = bg_event_tx.clone();
        let gateway_task = TaskHandle::spawn(async move {
            gateway.run().await;
            log::error!(
                "namespace '{}': gateway exited, cancelling all pods",
                gateway_ns_id
            );
            send_event(
                &gateway_event_tx,
                WorkerEvent::NamespaceFailed {
                    namespace_id: gateway_ns_id.clone(),
                    error: "gateway exited unexpectedly".to_string(),
                },
            )
            .await;
            ns_cancel.cancel();
        });

        // Bridge task: map FabricEvents to WorkerEvents.
        let bridge_event_tx = bg_event_tx.clone();
        let bridge_ns_id = namespace_id.clone();
        let event_bridge_task = TaskHandle::spawn(async move {
            while let Some(event) = fabric_event_rx.recv().await {
                match event {
                    FabricEvent::RouteMiss { dst_ip, dst_mac } => {
                        if let Err(e) = bridge_event_tx
                            .try_send(WorkerEvent::FabricRouteMiss {
                                namespace_id: bridge_ns_id.clone(),
                                dst_ip,
                                dst_mac,
                            })
                        {
                            log::warn!("worker: dropped FabricRouteMiss event: {}", e);
                        }
                    }
                    FabricEvent::ServiceActivation { service_id, dst_ip } => {
                        if let Err(e) = bridge_event_tx
                            .try_send(WorkerEvent::ServiceActivation {
                                namespace_id: bridge_ns_id.clone(),
                                service_id: ServiceId::from(service_id),
                                dst_ip,
                            })
                        {
                            log::warn!("worker: dropped ServiceActivation event: {}", e);
                        }
                    }
                    FabricEvent::ServiceBackendNeed { service_id, dst_ip: _, need } => {
                        if let Err(e) = bridge_event_tx
                            .send(WorkerEvent::ServiceBackendNeed {
                                namespace_id: bridge_ns_id.clone(),
                                service_id: ServiceId::from(service_id),
                                need,
                            }).await
                        {
                            log::warn!("worker: failed to send ServiceBackendNeed event: {}", e);
                        }
                    }
                }
            }
        });

        // Create adapter virtual ports and plug them into the fabric.
        let adapter_ports_result = adapter_manager
            .create_namespace_ports(namespace_id.as_ref());
        let mut adapter_handles = Vec::new();
        let mut adapter_tasks = Vec::new();
        for (channel_port, handle) in adapter_ports_result {
            let (_port_id, task) = fabric.add_port_raw(FabricPort::Virtual(channel_port));
            adapter_handles.push(handle);
            adapter_tasks.push(task);
        }

        log::info!(
            "worker: created namespace '{}' with fabric + gateway",
            namespace_id
        );

        let ns = NamespaceState {
            fabric: Arc::new(fabric),
            tables,
            _gateway_task: gateway_task,
            _event_bridge_task: event_bridge_task,
            _adapter_tasks: adapter_tasks,
            _adapter_ports: adapter_handles,
            registry,
            pods: HashMap::new(),
            token: ns_token,
        };

        let event = WorkerEvent::NamespaceCreated {
            namespace_id: namespace_id.clone(),
        };

        Ok((ns, event))
    }

    /// Destroy this namespace: cancel all pods and await their supervisors.
    pub(crate) async fn destroy(
        self,
        bg_event_tx: &mpsc::Sender<WorkerEvent>,
        namespace_id: &NamespaceId,
    ) {
        // Cancel the namespace token, cascading to all pods.
        self.token.cancel();

        // Await all pod supervisors with timeout.
        for (pod_id, pod) in self.pods {
            log::info!(
                "worker: awaiting pod '{}' for namespace '{}' destruction",
                pod_id,
                namespace_id
            );
            match tokio::time::timeout(STOP_POD_TIMEOUT, pod.supervisor).await {
                Ok(Ok(())) => { /* clean exit */ }
                Ok(Err(join_error)) => {
                    log::error!(
                        "worker: pod '{}' supervisor panicked: {}",
                        pod_id,
                        join_error
                    );
                }
                Err(_) => {
                    log::warn!(
                        "worker: pod '{}' supervisor timed out during namespace destroy, aborting",
                        pod_id
                    );
                    // pod.supervisor (TaskHandle) drops here, automatically aborting.
                }
            }
        }
        // _gateway_task (TaskHandle) drops here, automatically aborting.
        log::info!("worker: destroyed namespace '{}'", namespace_id);

        send_event(
            bg_event_tx,
            WorkerEvent::NamespaceDestroyed {
                namespace_id: namespace_id.clone(),
            },
        )
        .await;
    }

    // -----------------------------------------------------------------------
    // Registry
    // -----------------------------------------------------------------------

    pub(crate) fn registry_sync(
        &mut self,
        namespace_id: &NamespaceId,
        entries: Vec<RegistryEntry>,
    ) -> Result<(), FatalError> {
        let mut map = self.registry.write().map_err(|e| {
            FatalError::InternalInvariant(format!("registry lock poisoned: {}", e))
        })?;
        map.clear();
        for entry in entries {
            map.insert(entry.name, entry.ip);
        }

        log::info!("worker: synced registry for namespace '{}'", namespace_id);
        Ok(())
    }

    pub(crate) fn registry_update(
        &mut self,
        namespace_id: &NamespaceId,
        added: Vec<RegistryEntry>,
        removed: Vec<String>,
    ) -> Result<(), FatalError> {
        let mut map = self.registry.write().map_err(|e| {
            FatalError::InternalInvariant(format!("registry lock poisoned: {}", e))
        })?;
        for name in &removed {
            map.remove(name);
        }
        for entry in added {
            map.insert(entry.name, entry.ip);
        }

        log::info!(
            "worker: updated registry for namespace '{}' ({} removed)",
            namespace_id,
            removed.len()
        );
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Fabric routes
    // -----------------------------------------------------------------------

    pub(crate) fn route_sync(
        &mut self,
        namespace_id: &NamespaceId,
        routes: Vec<distvirt_worker_protocol::FabricRouteEntry>,
    ) -> Result<(), FatalError> {
        let mut rt = self.tables.route_table.lock().map_err(|e| {
            FatalError::InternalInvariant(format!("route table lock poisoned: {}", e))
        })?;
        rt.sync(routes);

        log::info!(
            "worker: synced fabric routes for namespace '{}'",
            namespace_id
        );
        Ok(())
    }

    pub(crate) fn route_update(
        &mut self,
        namespace_id: &NamespaceId,
        added: Vec<distvirt_worker_protocol::FabricRouteEntry>,
        removed_ips: Vec<std::net::Ipv4Addr>,
    ) -> Result<(), FatalError> {
        let mut rt = self.tables.route_table.lock().map_err(|e| {
            FatalError::InternalInvariant(format!("route table lock poisoned: {}", e))
        })?;
        rt.update(added, removed_ips);

        log::info!(
            "worker: updated fabric routes for namespace '{}'",
            namespace_id
        );
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Services
    // -----------------------------------------------------------------------

    pub(crate) fn create_service(
        &mut self,
        namespace_id: &NamespaceId,
        service_id: &ServiceId,
        ip: std::net::Ipv4Addr,
        mac: [u8; 6],
        policy: ServicePolicy,
        activator_runtime: Option<&ActivatorRuntime>,
    ) -> Result<(), FatalError> {
        let activator = if policy.activator.is_some() {
            if let Some(runtime) = activator_runtime {
                let component_name = match &policy.activator {
                    Some(ActivatorConfig::Tcp { .. }) => "tcp",
                    Some(ActivatorConfig::Http2 { .. }) => "http2",
                    None => unreachable!(),
                };
                match runtime.get_component(component_name) {
                    Some(component) => {
                        match ActivatorInstance::new(runtime.engine(), component) {
                            Ok(instance) => Some(instance),
                            Err(e) => {
                                log::error!("failed to instantiate activator: {:#}", e);
                                None
                            }
                        }
                    }
                    None => {
                        log::warn!("activator component '{}' not found", component_name);
                        None
                    }
                }
            } else {
                log::warn!("activator requested but runtime not available");
                None
            }
        } else {
            None
        };

        // Build the processor variant based on activator config.
        let processor = match (&policy.activator, activator) {
            (Some(ActivatorConfig::Http2 { .. }), act) => {
                let sm = StreamManager::new(StreamManagerConfig {
                    service_ip: ip,
                    service_mac: mac,
                    listen_ports: vec![80],
                    ..StreamManagerConfig::default()
                });
                ServiceProcessor::L4 { activator: act, stream_manager: sm }
            }
            (_, Some(act)) => {
                ServiceProcessor::L3 { activator: act, flow_tracker: FlowTracker::new() }
            }
            _ => ServiceProcessor::Passthrough,
        };

        let mut st = self.tables.service_table.lock().map_err(|e| {
            FatalError::InternalInvariant(format!("service table lock poisoned: {}", e))
        })?;
        st.create(service_id.0.clone(), ip, mac, policy, processor);

        log::info!(
            "worker: created service '{}' with ip {} in namespace '{}'",
            service_id, ip, namespace_id
        );
        Ok(())
    }

    pub(crate) fn update_service_backend(
        &mut self,
        namespace_id: &NamespaceId,
        service_id: &ServiceId,
        backend: Option<distvirt_worker_protocol::ServiceBackend>,
    ) -> Result<(), FatalError> {
        let mut st = self.tables.service_table.lock().map_err(|e| {
            FatalError::InternalInvariant(format!("service table lock poisoned: {}", e))
        })?;
        let backend_tuple = backend.map(|b| (b.pod_ip, b.pod_mac));
        st.update_backend(service_id.as_ref(), backend_tuple);

        log::info!(
            "worker: updated service backend '{}' in namespace '{}'",
            service_id, namespace_id
        );
        Ok(())
    }

    pub(crate) async fn service_ready(
        &mut self,
        namespace_id: &NamespaceId,
        service_id: &ServiceId,
    ) -> Result<(), FatalError> {
        let flush_data = {
            let mut st = self.tables.service_table.lock().map_err(|e| {
                FatalError::InternalInvariant(format!("service table lock poisoned: {}", e))
            })?;
            st.mark_ready(service_id.as_ref())
        };

        if let Some(result) = flush_data {
            use crate::fabric::{MarkReadyResult, ServiceAction};
            match &result {
                MarkReadyResult::Passthrough { frames, actions, backend_mac, .. } => {
                    log::info!(
                        "worker: service '{}' mark_ready returned Passthrough: {} buffered frames, {} actions, backend_mac={:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
                        service_id, frames.len(), actions.len(),
                        backend_mac[0], backend_mac[1], backend_mac[2],
                        backend_mac[3], backend_mac[4], backend_mac[5],
                    );
                }
                MarkReadyResult::L4(action) => {
                    log::info!(
                        "worker: service '{}' mark_ready returned L4: {:?}",
                        service_id, std::mem::discriminant(action)
                    );
                }
            }
            match result {
                MarkReadyResult::Passthrough { frames, backend_mac, backend_ip, service_ip, service_mac, actions } => {
                    if !frames.is_empty() {
                        self.fabric.flush_service_frames(frames, backend_mac, backend_ip, service_ip, service_mac);
                    }
                    self.fabric.dispatch_actions(&actions, service_id.as_ref()).await;
                }
                MarkReadyResult::L4(ServiceAction::L4Result { actions, frames, .. }) => {
                    self.fabric.send_l4_frames(frames);
                    self.fabric.dispatch_actions(&actions, service_id.as_ref()).await;
                }
                _ => {}
            }
        }

        log::info!(
            "worker: service '{}' marked ready in namespace '{}'",
            service_id, namespace_id
        );
        Ok(())
    }

    pub(crate) fn destroy_service(
        &mut self,
        namespace_id: &NamespaceId,
        service_id: &ServiceId,
    ) -> Result<(), FatalError> {
        let mut st = self.tables.service_table.lock().map_err(|e| {
            FatalError::InternalInvariant(format!("service table lock poisoned: {}", e))
        })?;
        st.destroy(service_id.as_ref());

        log::info!(
            "worker: destroyed service '{}' in namespace '{}'",
            service_id, namespace_id
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    use distvirt_worker_protocol::{
        BufferPolicy, FabricRouteEntry, RegistryEntry, RouteDestination,
        ServiceBackend, ServicePolicy,
    };

    use crate::fabric::{Fabric, FabricPort};

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn default_policy() -> ServicePolicy {
        ServicePolicy {
            buffer_frames: 10,
            timeout_ms: 5000,
            activator: None,
        }
    }

    /// Create a minimal NamespaceState for testing (no gateway, no adapter ports).
    fn make_namespace() -> (NamespaceState, CancellationToken) {
        let worker_token = CancellationToken::new();
        let fabric = Fabric::<FabricPort>::new();
        let tables = fabric.tables();
        let ns_token = worker_token.child_token();

        let ns = NamespaceState {
            fabric: Arc::new(fabric),
            tables,
            _gateway_task: TaskHandle::spawn(std::future::pending::<()>()),
            _event_bridge_task: TaskHandle::spawn(std::future::pending::<()>()),
            _adapter_tasks: Vec::new(),
            _adapter_ports: Vec::new(),
            registry: Arc::new(RwLock::new(HashMap::new())),
            pods: HashMap::new(),
            token: ns_token,
        };
        (ns, worker_token)
    }

    // -----------------------------------------------------------------------
    // Registry tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn registry_sync_populates_entries() {
        let (mut ns, _token) = make_namespace();
        let ns_id = NamespaceId::from("ns1");

        let entries = vec![
            RegistryEntry {
                name: "api".to_string(),
                ip: Ipv4Addr::new(172, 16, 0, 10),
            },
            RegistryEntry {
                name: "db".to_string(),
                ip: Ipv4Addr::new(172, 16, 0, 11),
            },
        ];

        ns.registry_sync(&ns_id, entries).unwrap();

        let map = ns.registry.read().unwrap();
        assert_eq!(map.len(), 2);
        assert_eq!(map.get("api"), Some(&Ipv4Addr::new(172, 16, 0, 10)));
        assert_eq!(map.get("db"), Some(&Ipv4Addr::new(172, 16, 0, 11)));
    }

    #[tokio::test]
    async fn registry_sync_replaces_on_resync() {
        let (mut ns, _token) = make_namespace();
        let ns_id = NamespaceId::from("ns1");

        let entries1 = vec![RegistryEntry {
            name: "api".to_string(),
            ip: Ipv4Addr::new(172, 16, 0, 10),
        }];
        ns.registry_sync(&ns_id, entries1).unwrap();

        // Re-sync with different entries.
        let entries2 = vec![RegistryEntry {
            name: "web".to_string(),
            ip: Ipv4Addr::new(172, 16, 0, 20),
        }];
        ns.registry_sync(&ns_id, entries2).unwrap();

        let map = ns.registry.read().unwrap();
        assert_eq!(map.len(), 1);
        assert!(map.get("api").is_none());
        assert_eq!(map.get("web"), Some(&Ipv4Addr::new(172, 16, 0, 20)));
    }

    // -----------------------------------------------------------------------
    // Fabric route tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn fabric_route_sync_populates_routes() {
        let (mut ns, _token) = make_namespace();
        let ns_id = NamespaceId::from("ns1");

        let routes = vec![FabricRouteEntry {
            ip: Ipv4Addr::new(172, 16, 0, 10),
            mac: [0x02, 0, 0, 0, 0, 0x10],
            destination: RouteDestination::Placeholder {
                buffer_policy: BufferPolicy {
                    buffer_frames: 10,
                    timeout_ms: 5000,
                },
            },
        }];

        ns.route_sync(&ns_id, routes).unwrap();

        // Verify via lookup_and_buffer (requires &mut).
        let mut rt = ns.tables.route_table.lock().unwrap();
        let (action, _) = rt.lookup_and_buffer(Ipv4Addr::new(172, 16, 0, 10), &[0xDE, 0xAD]);
        assert!(
            !matches!(action, crate::fabric::route::RouteAction::NoRoute),
            "expected a route entry, got NoRoute"
        );
    }

    #[tokio::test]
    async fn fabric_route_update_adds_and_removes() {
        let (mut ns, _token) = make_namespace();
        let ns_id = NamespaceId::from("ns1");

        // Sync initial route.
        let routes = vec![FabricRouteEntry {
            ip: Ipv4Addr::new(172, 16, 0, 10),
            mac: [0x02, 0, 0, 0, 0, 0x10],
            destination: RouteDestination::Placeholder {
                buffer_policy: BufferPolicy {
                    buffer_frames: 5,
                    timeout_ms: 5000,
                },
            },
        }];
        ns.route_sync(&ns_id, routes).unwrap();

        // Update: add a new route, remove the old one.
        let added = vec![FabricRouteEntry {
            ip: Ipv4Addr::new(172, 16, 0, 20),
            mac: [0x02, 0, 0, 0, 0, 0x20],
            destination: RouteDestination::RemoteWorker {
                worker_id: "w2".to_string().into(),
            },
        }];
        ns.route_update(
            &ns_id,
            added,
            vec![Ipv4Addr::new(172, 16, 0, 10)],
        )
        .unwrap();
    }

    // -----------------------------------------------------------------------
    // Service lifecycle tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn service_create_and_destroy() {
        let (mut ns, _token) = make_namespace();
        let ns_id = NamespaceId::from("ns1");
        let svc_id = ServiceId::from("svc1");

        ns.create_service(
            &ns_id,
            &svc_id,
            Ipv4Addr::new(172, 16, 0, 100),
            [0x02, 0, 0, 0, 0, 0xAA],
            default_policy(),
            None,
        )
        .unwrap();

        // Verify service exists by checking the service table.
        {
            let st = ns.tables.service_table.lock().unwrap();
            assert_eq!(
                st.get_ip_by_id("svc1"),
                Some(Ipv4Addr::new(172, 16, 0, 100))
            );
        }

        // Destroy.
        ns.destroy_service(&ns_id, &svc_id).unwrap();

        {
            let st = ns.tables.service_table.lock().unwrap();
            assert_eq!(st.get_ip_by_id("svc1"), None);
        }
    }

    #[tokio::test]
    async fn service_update_backend() {
        let (mut ns, _token) = make_namespace();
        let ns_id = NamespaceId::from("ns1");
        let svc_id = ServiceId::from("svc1");

        ns.create_service(
            &ns_id,
            &svc_id,
            Ipv4Addr::new(172, 16, 0, 100),
            [0x02, 0, 0, 0, 0, 0xAA],
            default_policy(),
            None,
        )
        .unwrap();

        // Assign backend.
        ns.update_service_backend(
            &ns_id,
            &svc_id,
            Some(ServiceBackend {
                pod_ip: Ipv4Addr::new(172, 16, 0, 10),
                pod_mac: [0x02, 0, 0, 0, 0, 0x10],
            }),
        )
        .unwrap();

        // Remove backend.
        ns.update_service_backend(&ns_id, &svc_id, None)
            .unwrap();
    }

    #[tokio::test]
    async fn service_ready_on_existing_service() {
        let (mut ns, _token) = make_namespace();
        let ns_id = NamespaceId::from("ns1");
        let svc_id = ServiceId::from("svc1");

        ns.create_service(
            &ns_id,
            &svc_id,
            Ipv4Addr::new(172, 16, 0, 100),
            [0x02, 0, 0, 0, 0, 0xAA],
            default_policy(),
            None,
        )
        .unwrap();

        ns.update_service_backend(
            &ns_id,
            &svc_id,
            Some(ServiceBackend {
                pod_ip: Ipv4Addr::new(172, 16, 0, 10),
                pod_mac: [0x02, 0, 0, 0, 0, 0x10],
            }),
        )
        .unwrap();

        // ServiceReady should succeed (no buffered frames, so no flush).
        ns.service_ready(&ns_id, &svc_id).await.unwrap();
    }
}
