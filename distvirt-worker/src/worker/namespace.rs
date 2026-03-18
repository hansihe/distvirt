use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};

use crate::fabric::ServiceProcessor;
use distvirt_activator::{
    ActivatorInstance, ActivatorRuntime, FlowTracker, StreamManager, StreamManagerConfig,
};
use distvirt_worker_protocol::{
    ActivatorConfig, NamespaceId, PodId, RegistryEntry, ServiceId, ServicePolicy, WorkerEvent,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::supervisor::{PodState, STOP_POD_TIMEOUT, send_event};
use crate::adapter::{AdapterManager, AdapterPortHandle};
use crate::fabric::gateway::GatewayProvider;
use crate::fabric::port::FramePort;
use crate::fabric::{
    DnsRegistry, Fabric, FabricContextInner, FabricEvent, FabricGateway, FabricPort,
};
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
    pub(crate) segment_id: Option<u16>,
    /// Port ID of the adapter channel port (WireGuard, etc.) on this fabric.
    /// Used to create LocalAdapter endpoints for WireGuard peers.
    pub(crate) adapter_port_id: Option<usize>,
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
            segment_id: None,
            adapter_port_id: None,
            token,
        }
    }

    /// Create a new namespace with fabric, gateway, event bridge, and adapter ports.
    ///
    /// Returns the namespace state and a `NamespaceCreated` event to send.
    pub(crate) async fn new<G: GatewayProvider>(
        worker_token: &CancellationToken,
        bg_event_tx: &mpsc::Sender<WorkerEvent>,
        adapter_manager: &AdapterManager,
        gateway_provider: &G,
        namespace_id: &NamespaceId,
        network: distvirt_worker_protocol::NetworkConfig,
    ) -> Result<(NamespaceState, WorkerEvent), FatalError> {
        let fabric = Fabric::<FabricPort>::new(network.gateway, network.prefix_len);

        let registry: DnsRegistry = Arc::new(RwLock::new(HashMap::new()));

        // Set up fabric event channel for route miss reporting.
        let (fabric_event_tx, mut fabric_event_rx) = mpsc::channel::<FabricEvent>(64);
        fabric.set_event_channel(fabric_event_tx);

        let tables = fabric.tables();

        let pod_gateway_ip = network.gateway.octets();

        // Create gateway via provider — one path for all implementations.
        let egress = gateway_provider
            .create_egress(namespace_id, pod_gateway_ip, network.prefix_len)
            .map_err(|e| FatalError::InternalInvariant(format!("create gateway: {e:#}")))?;
        let (gateway, egress_tx, ingress_rx) = FabricGateway::new_with_egress(
            egress,
            Arc::clone(&registry),
            pod_gateway_ip,
            network.prefix_len,
        )
        .map_err(|e| FatalError::InternalInvariant(format!("create fabric gateway: {:#}", e)))?;
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
        //
        // Events use try_send (non-blocking, lossy) or send (blocking, reliable)
        // depending on their semantics:
        //   - RouteMiss, ServiceActivation: use try_send because these are
        //     high-frequency, best-effort signals. Dropping one just means a
        //     slight delay before re-detection on the next packet.
        //   - ServiceBackendNeed: uses send (blocking) because this controls
        //     scale-up decisions — losing it could leave a service stuck without
        //     a backend.
        let bridge_event_tx = bg_event_tx.clone();
        let bridge_ns_id = namespace_id.clone();
        let event_bridge_task = TaskHandle::spawn(async move {
            while let Some(event) = fabric_event_rx.recv().await {
                match event {
                    FabricEvent::EndpointActivation { dst_ip, service_id } => {
                        let svc_id = service_id.map(ServiceId::from);
                        if let Err(e) =
                            bridge_event_tx.try_send(WorkerEvent::EndpointDemandTraffic {
                                namespace_id: bridge_ns_id.clone(),
                                ip: dst_ip,
                                service_id: svc_id,
                            })
                        {
                            log::warn!("worker: dropped EndpointActivation event: {}", e);
                        }
                    }
                    FabricEvent::EndpointDemand {
                        ip,
                        service_id,
                        active,
                    } => {
                        let svc_id = service_id.map(ServiceId::from);
                        if let Err(e) =
                            bridge_event_tx.try_send(WorkerEvent::EndpointDemandActive {
                                namespace_id: bridge_ns_id.clone(),
                                ip,
                                service_id: svc_id,
                                active,
                            })
                        {
                            log::warn!("worker: dropped EndpointDemand event: {}", e);
                        }
                    }
                }
            }
        });

        // Create adapter virtual ports and plug them into the fabric.
        let adapter_ports_result = adapter_manager
            .create_namespace_ports(namespace_id.as_ref())
            .await;
        let mut adapter_handles = Vec::new();
        let mut adapter_tasks = Vec::new();
        let mut adapter_port_id = None;
        for (channel_port, handle) in adapter_ports_result {
            let (port_id, task) = fabric.add_port_raw(FabricPort::Virtual(channel_port));
            // Store the first adapter port ID for LocalAdapter endpoint routing.
            if adapter_port_id.is_none() {
                adapter_port_id = Some(port_id);
            }
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
            segment_id: network.segment_id,
            adapter_port_id,
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
        let mut map = self
            .registry
            .write()
            .map_err(|e| FatalError::InternalInvariant(format!("registry lock poisoned: {}", e)))?;
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
        let mut map = self
            .registry
            .write()
            .map_err(|e| FatalError::InternalInvariant(format!("registry lock poisoned: {}", e)))?;
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
    // Endpoint protocol (unified)
    // -----------------------------------------------------------------------

    /// Shared helper to process endpoint sync effects (ServiceReady / FlushPodBuffer).
    /// Returns any WorkerEvents that need to be sent (e.g. FlowStatusChange).
    fn handle_endpoint_effects(
        &self,
        namespace_id: &NamespaceId,
        effects: Vec<crate::fabric::endpoint::EndpointSyncEffect>,
    ) -> Result<Vec<WorkerEvent>, FatalError> {
        use crate::fabric::MarkReadyResult;
        use crate::fabric::endpoint::{EndpointAction, EndpointSyncEffect};
        let mut pending_events = Vec::new();

        for effect in effects {
            match effect {
                EndpointSyncEffect::ServiceReady { service_id } => {
                    let flush_data = {
                        let mut st = self.tables.endpoint_table.lock().map_err(|e| {
                            FatalError::InternalInvariant(format!(
                                "endpoint table lock poisoned: {}",
                                e
                            ))
                        })?;
                        st.mark_service_ready(service_id)
                    };
                    if let Some(result) = flush_data {
                        match result {
                            MarkReadyResult::Passthrough {
                                frames,
                                backend_ip,
                                service_ip,
                                actions,
                            } => {
                                if !frames.is_empty() {
                                    self.fabric
                                        .flush_service_frames(frames, backend_ip, service_ip);
                                }
                                let fabric = Arc::clone(&self.fabric);
                                tokio::spawn(async move {
                                    fabric.dispatch_actions(&actions, service_id).await;
                                });
                            }
                            MarkReadyResult::L4(EndpointAction::L4Result {
                                actions,
                                frames,
                                ..
                            }) => {
                                self.fabric.send_l4_frames(frames);
                                let fabric = Arc::clone(&self.fabric);
                                tokio::spawn(async move {
                                    fabric.dispatch_actions(&actions, service_id).await;
                                });
                            }
                            other => {
                                log::debug!(
                                    "worker: unexpected MarkReadyResult variant {:?} for service '{}' in namespace '{}'",
                                    other,
                                    service_id,
                                    namespace_id
                                );
                            }
                        }
                    }
                }
                EndpointSyncEffect::FlushPodBuffer { ip } => {
                    let buffered = {
                        let mut et = self.tables.endpoint_table.lock().map_err(|e| {
                            FatalError::InternalInvariant(format!(
                                "endpoint table lock poisoned: {}",
                                e
                            ))
                        })?;
                        et.flush_pod_buffer(ip)
                    };
                    if !buffered.is_empty() {
                        log::info!(
                            "worker: flushing {} buffered pod frames for {} in namespace '{}'",
                            buffered.len(),
                            ip,
                            namespace_id
                        );
                        let port = self.tables.resolve_ip(&ip);
                        if let Some(port) = port {
                            let count = buffered.len();
                            tokio::spawn(async move {
                                for frame in buffered {
                                    if let Err(e) = port.send_frame(&frame).await {
                                        log::warn!("fabric: flush buffered frame error: {}", e);
                                        break;
                                    }
                                }
                                log::info!("fabric: flushed {} buffered pod frames", count);
                            });
                        } else {
                            log::warn!(
                                "fabric: could not resolve port for buffered frames at {}",
                                ip
                            );
                        }
                    }
                }
                EndpointSyncEffect::FlowStatusChange {
                    ip,
                    service_id,
                    active,
                } => {
                    pending_events.push(WorkerEvent::EndpointDemandActive {
                        namespace_id: namespace_id.clone(),
                        ip,
                        service_id,
                        active,
                    });
                }
                EndpointSyncEffect::FlushAdapterBuffer {
                    ip,
                    port_id,
                    frames,
                } => {
                    if !frames.is_empty() {
                        log::info!(
                            "worker: flushing {} buffered adapter frames for {} to port {} in namespace '{}'",
                            frames.len(),
                            ip,
                            port_id,
                            namespace_id
                        );
                        let port = {
                            self.tables
                                .ports
                                .lock()
                                .expect("poisoned")
                                .get(&port_id)
                                .cloned()
                        };
                        if let Some(port) = port {
                            let count = frames.len();
                            tokio::spawn(async move {
                                for frame in frames {
                                    if let Err(e) = port.send_frame(&frame).await {
                                        log::warn!("fabric: flush adapter frame error: {}", e);
                                        break;
                                    }
                                }
                                log::info!(
                                    "fabric: flushed {} adapter frames to port {}",
                                    count,
                                    port_id
                                );
                            });
                        } else {
                            log::warn!(
                                "fabric: adapter port {} not found for flush at {}",
                                port_id,
                                ip
                            );
                        }
                    }
                }
            }
        }

        Ok(pending_events)
    }

    pub(crate) fn endpoint_sync(
        &mut self,
        namespace_id: &NamespaceId,
        endpoints: Vec<distvirt_worker_protocol::EndpointSpec>,
        my_worker_id: distvirt_worker_protocol::WorkerId,
        activator_runtime: Option<&ActivatorRuntime>,
    ) -> Result<Vec<WorkerEvent>, FatalError> {
        let effects = {
            let mut et = self.tables.endpoint_table.lock().map_err(|e| {
                FatalError::InternalInvariant(format!("endpoint table lock poisoned: {}", e))
            })?;

            let mut make_processor = |_svc_id: ServiceId,
                                      policy: &ServicePolicy,
                                      ip: std::net::Ipv4Addr|
             -> ServiceProcessor {
                Self::build_processor(policy, ip, activator_runtime)
            };

            et.apply_endpoint_sync(
                endpoints,
                my_worker_id,
                &mut make_processor,
                self.adapter_port_id,
            )
        };

        let pending_events = self.handle_endpoint_effects(namespace_id, effects)?;

        log::info!("worker: endpoint sync for namespace '{}'", namespace_id);
        Ok(pending_events)
    }

    pub(crate) fn endpoint_update(
        &mut self,
        namespace_id: &NamespaceId,
        upserted: Vec<distvirt_worker_protocol::EndpointSpec>,
        removed_ips: Vec<std::net::Ipv4Addr>,
        my_worker_id: distvirt_worker_protocol::WorkerId,
        activator_runtime: Option<&ActivatorRuntime>,
    ) -> Result<Vec<WorkerEvent>, FatalError> {
        let effects = {
            let mut et = self.tables.endpoint_table.lock().map_err(|e| {
                FatalError::InternalInvariant(format!("endpoint table lock poisoned: {}", e))
            })?;

            let mut make_processor = |_svc_id: ServiceId,
                                      policy: &ServicePolicy,
                                      ip: std::net::Ipv4Addr|
             -> ServiceProcessor {
                Self::build_processor(policy, ip, activator_runtime)
            };

            et.apply_endpoint_update(
                upserted,
                removed_ips,
                my_worker_id,
                &mut make_processor,
                self.adapter_port_id,
            )
        };

        let pending_events = self.handle_endpoint_effects(namespace_id, effects)?;

        log::info!("worker: endpoint update for namespace '{}'", namespace_id);
        Ok(pending_events)
    }

    /// Build a ServiceProcessor from a ServicePolicy.
    fn build_processor(
        policy: &ServicePolicy,
        ip: std::net::Ipv4Addr,
        activator_runtime: Option<&ActivatorRuntime>,
    ) -> ServiceProcessor {
        let activator = if policy.activator.is_some() {
            if let Some(runtime) = activator_runtime {
                let component_name = match &policy.activator {
                    Some(ActivatorConfig::Tcp { .. }) => "tcp",
                    Some(ActivatorConfig::Http2 { .. }) => "http2",
                    None => unreachable!(),
                };
                match runtime.get_component(component_name) {
                    Some(component) => match ActivatorInstance::new(runtime.engine(), component) {
                        Ok(instance) => Some(instance),
                        Err(e) => {
                            log::error!("failed to instantiate activator: {:#}", e);
                            None
                        }
                    },
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

        match (&policy.activator, activator) {
            (Some(ActivatorConfig::Http2 { .. }), act) => {
                let sm = StreamManager::new(StreamManagerConfig {
                    service_ip: ip,
                    listen_ports: vec![80],
                    ..StreamManagerConfig::default()
                });
                ServiceProcessor::L4 {
                    activator: act,
                    stream_manager: sm,
                }
            }
            (_, Some(act)) => ServiceProcessor::L3 {
                activator: act,
                flow_tracker: FlowTracker::new(),
            },
            _ => ServiceProcessor::Passthrough,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    use distvirt_worker_protocol::RegistryEntry;

    use crate::fabric::{Fabric, FabricPort};

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /// Create a minimal NamespaceState for testing (no gateway, no adapter ports).
    fn make_namespace() -> (NamespaceState, CancellationToken) {
        let worker_token = CancellationToken::new();
        let fabric = Fabric::<FabricPort>::new(Ipv4Addr::new(172, 16, 0, 0), 24);
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
            segment_id: None,
            adapter_port_id: None,
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
}
