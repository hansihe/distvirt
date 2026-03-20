//! Per-namespace state machine — no async, no channels, no I/O.
//!
//! `Namespace` is the single pure-logic layer for one namespace. It owns the
//! router, all adapters, worker lifecycle (pending → active), protocol command
//! building, and WireGuard peer management. It consumes
//! `OrchestratorToNamespace` messages and produces `NamespaceEffects`.

use std::collections::{HashMap, HashSet};

use crate::adapter::artifact::{ArtifactAction, ArtifactAdapter};
use crate::adapter::dns_registry::{DnsRegistryAction, DnsRegistryAdapter};
use crate::adapter::endpoint::{EndpointAction, EndpointAdapter};
use crate::adapter::endpoint_demand::EndpointDemandAdapter;
use crate::adapter::management::ManagementAdapter;
use crate::adapter::observability::{ObservabilityAdapter, ObservabilityEvent};
use crate::adapter::pod_assignment::{PodAssignmentAction, PodAssignmentAdapter};
use crate::adapter::schedule_request::{ScheduleRequestAdapter, ScheduleRequestDelta};
use crate::adapter::timer::{TimerAction, TimerAdapter, TimerConfig, TimerIdentity};
use crate::core::namespace::wg_peers::{WgPeerOutput, WireGuardPeerManager};
use crate::core::types::{NamespaceEffects, OrchestratorToNamespace, SchedulerMessage};
use crate::core::{
    ClientCommand, GlobalWorkerId, SchedulerDecision,
    WorkerNamespaceEventKind,
};
use crate::id_registry::IdRegistry;
#[cfg(feature = "test-trace")]
use distvirt_sm_router::trace::PanicTracer;
use crate::sm::{
    AdminCmd, ArtifactId, ArtifactPortId, DNS_REGISTRY, DRouter, FABRIC_ENDPOINT, LeaseInfo,
    OBSERVABILITY, PodId, PodStatus, Router, SCHEDULE_REQUEST, ScheduleLeaseId, TIMER,
    WireGuardPeerEndpointInfo, WireGuardPeerId, WlStatus, WorkerId,
    endpoint::EndpointStatus,
};
use crate::types::{NamespaceId, NamespaceSpec, NamespaceStatusReport, WorkloadName};

// =============================================================================
// ID conversion helpers (same u64 value, different newtypes)
// =============================================================================

fn proto_pod_id(router_id: PodId) -> distvirt_worker_protocol::PodId {
    distvirt_worker_protocol::PodId(router_id.0)
}

fn router_pod_id(proto_id: &distvirt_worker_protocol::PodId) -> PodId {
    PodId(proto_id.0)
}

// =============================================================================
// Helper types
// =============================================================================

/// Worker that has connected but not yet confirmed namespace creation.
struct PendingWorker {
    info: crate::sm::WorkerInfo,
}

/// Per-worker info tracked after namespace creation is confirmed.
struct ActiveWorkerInfo {
    default_pool: Option<distvirt_worker_protocol::PoolId>,
}

/// All pure adapters owned by the namespace.
pub(crate) struct Adapters {
    timer: TimerAdapter,
    pod_assignment: PodAssignmentAdapter,
    schedule_request: ScheduleRequestAdapter,
    pub(crate) management: ManagementAdapter,
    endpoint_demand: EndpointDemandAdapter,
    pub(crate) endpoint: EndpointAdapter,
    pub(crate) dns_registry: DnsRegistryAdapter,
    artifact: ArtifactAdapter,
    observability: ObservabilityAdapter,
}

/// Sync output of the reconcile phase.
struct ReconcileActions {
    timer_actions: Vec<TimerAction>,
    schedule_deltas: Vec<ScheduleRequestDelta>,
    pod_actions: Vec<PodAssignmentAction>,
    endpoint_actions: Vec<EndpointAction>,
    dns_registry_actions: Vec<DnsRegistryAction>,
    observability_events: Vec<ObservabilityEvent>,
}

// =============================================================================
// Namespace
// =============================================================================

pub struct Namespace {
    namespace_id: NamespaceId,
    router: DRouter,
    pub(crate) adapters: Adapters,

    leases: HashMap<PodId, ScheduleLeaseId>,

    /// Tracks which pods are assigned to each worker.
    worker_pod_edges: HashMap<WorkerId, HashSet<PodId>>,
    /// Reverse lookup: pod → assigned worker.
    pod_worker: HashMap<PodId, WorkerId>,

    pub(crate) current_spec: Option<NamespaceSpec>,

    /// WireGuard peer IP allocation and tracking.
    wg_peer_mgr: WireGuardPeerManager,
    /// Maps client public key → WireGuardPeerId (router port).
    wg_peer_ports: HashMap<[u8; 32], WireGuardPeerId>,

    // --- Worker lifecycle (formerly in boundary) ---
    pending_workers: HashMap<GlobalWorkerId, PendingWorker>,
    active_workers: HashMap<GlobalWorkerId, ActiveWorkerInfo>,
    deferred_grants: Vec<(PodId, GlobalWorkerId)>,
    /// Artifact ID allocator (for suspend operations).
    next_artifact_counter: u64,
}

impl Namespace {
    pub fn new(
        namespace_id: NamespaceId,
        timer_config: TimerConfig,
        network: &distvirt_worker_protocol::NetworkConfig,
        id_registry: IdRegistry,
    ) -> Self {
        #[cfg(feature = "test-trace")]
        let mut router = Router::new_traced(16, PanicTracer::new());
        #[cfg(not(feature = "test-trace"))]
        let mut router = Router::new(16);
        router.create_timer(TIMER);
        router.create_schedule_request(SCHEDULE_REQUEST);
        router.create_fabric_endpoint(FABRIC_ENDPOINT);
        router.create_dns_registry(DNS_REGISTRY);
        router.create_observability(OBSERVABILITY);

        Namespace {
            namespace_id,
            router,
            adapters: Adapters {
                timer: TimerAdapter::new(timer_config),
                pod_assignment: PodAssignmentAdapter::new(),
                schedule_request: ScheduleRequestAdapter::new(SCHEDULE_REQUEST),
                management: ManagementAdapter::new(id_registry),
                endpoint_demand: EndpointDemandAdapter::new(),
                endpoint: EndpointAdapter::new(FABRIC_ENDPOINT),
                dns_registry: DnsRegistryAdapter::new(DNS_REGISTRY),
                artifact: ArtifactAdapter::new(),
                observability: ObservabilityAdapter::new(OBSERVABILITY),
            },
            leases: HashMap::new(),
            worker_pod_edges: HashMap::new(),
            pod_worker: HashMap::new(),
            current_spec: None,
            wg_peer_mgr: WireGuardPeerManager::new(network.subnet, network.prefix_len),
            wg_peer_ports: HashMap::new(),
            pending_workers: HashMap::new(),
            active_workers: HashMap::new(),
            deferred_grants: Vec::new(),
            next_artifact_counter: 0,
        }
    }

    // =========================================================================
    // Public entry points
    // =========================================================================

    /// Process an orchestrator-to-namespace message.
    /// Returns all effects to be executed by the shell.
    pub fn process_event(&mut self, event: OrchestratorToNamespace) -> NamespaceEffects {
        let mut effects = NamespaceEffects::default();
        self.push_event(event, &mut effects);
        self.run_cycle(&mut effects);
        effects
    }

    /// Fire a timer by identity. Called by NamespaceUnit when the timer wheel
    /// reports an expired timer.
    pub fn fire_timer(&mut self, identity: &TimerIdentity) -> NamespaceEffects {
        let mut effects = NamespaceEffects::default();
        self.adapters.timer.fire(&mut self.router, identity);
        self.run_cycle(&mut effects);
        effects
    }

    // =========================================================================
    // Core processing pipeline
    // =========================================================================

    /// Propagate + reconcile loop + artifact finalization.
    fn run_cycle(&mut self, effects: &mut NamespaceEffects) {
        self.router.propagate();

        loop {
            let (actions, mutated_router) = self.reconcile();
            self.collect_effects(actions, effects);
            if !mutated_router {
                break;
            }
            self.router.propagate();
        }

        // Drain deduped artifact actions and emit scheduler messages.
        for action in self.adapters.artifact.finalize(&mut self.router) {
            match action {
                ArtifactAction::Referenced { port_id } => {
                    effects.scheduler_messages.push(SchedulerMessage::ArtifactReferenced {
                        namespace_id: self.namespace_id.clone(),
                        proto_artifact_id: ArtifactId(port_id.0.to_string()),
                    });
                }
                ArtifactAction::Released { port_id } => {
                    effects.scheduler_messages.push(SchedulerMessage::ArtifactReleased {
                        namespace_id: self.namespace_id.clone(),
                        proto_artifact_id: ArtifactId(port_id.0.to_string()),
                    });
                }
            }
        }
    }

    // =========================================================================
    // Event dispatch
    // =========================================================================

    fn push_event(&mut self, event: OrchestratorToNamespace, effects: &mut NamespaceEffects) {
        match event {
            OrchestratorToNamespace::WorkerConnected {
                worker_id,
                proto_worker_id: _,
                info,
            } => {
                // Stage as pending — no router changes yet.
                self.pending_workers
                    .insert(worker_id, PendingWorker { info });
            }

            OrchestratorToNamespace::WorkerDisconnected { worker_id } => {
                let was_active = self.active_workers.remove(&worker_id).is_some();
                self.deferred_grants.retain(|(_, w)| *w != worker_id);

                if was_active {
                    // Clean up WorkerToPod edge tracking.
                    if let Some(pods) = self.worker_pod_edges.remove(&worker_id) {
                        for pod_id in pods {
                            self.pod_worker.remove(&pod_id);
                        }
                    }
                    self.adapters
                        .endpoint_demand
                        .remove_worker(&mut self.router, &worker_id);
                    self.router.destroy_worker(worker_id);
                }
                self.pending_workers.remove(&worker_id);
            }

            OrchestratorToNamespace::WorkerEvent(wne) => {
                self.handle_worker_event(wne.worker_id, wne.event, effects);
            }

            OrchestratorToNamespace::SchedulerDecision(decision) => match decision {
                SchedulerDecision::Grant {
                    namespace_id: _,
                    pod_id,
                    worker_id,
                } => {
                    if self.active_workers.contains_key(&worker_id) {
                        self.apply_grant(worker_id, pod_id);
                    } else {
                        self.deferred_grants.push((pod_id, worker_id));
                    }
                }
                SchedulerDecision::Revoke {
                    namespace_id: _,
                    pod_id,
                    ..
                } => {
                    if let Some(lease_id) = self.leases.remove(&pod_id) {
                        self.router.destroy_schedule_lease(lease_id);
                    }
                    self.remove_pod_from_worker(pod_id);
                }
            },

            OrchestratorToNamespace::ClientCommand(cmd) => {
                self.handle_client_command(cmd);
            }

            OrchestratorToNamespace::ArtifactInvalidated { artifact_port_id } => {
                self.router.destroy_artifact(artifact_port_id);
            }
        }
    }

    // =========================================================================
    // Worker event handling
    // =========================================================================

    fn handle_worker_event(
        &mut self,
        worker_id: GlobalWorkerId,
        event: WorkerNamespaceEventKind,
        effects: &mut NamespaceEffects,
    ) {
        match event {
            WorkerNamespaceEventKind::NamespaceCreated => {
                self.handle_namespace_created(worker_id, effects);
            }
            WorkerNamespaceEventKind::NamespaceFailed { error } => {
                if self.pending_workers.remove(&worker_id).is_some() {
                    eprintln!(
                        "namespace {:?}: worker {:?} fabric creation failed: {}",
                        self.namespace_id, worker_id, error
                    );
                }
                self.deferred_grants.retain(|(_, w)| *w != worker_id);
            }
            _ => {
                // Worker must be active to process remaining events.
                if !self.active_workers.contains_key(&worker_id) {
                    eprintln!(
                        "warning: unknown global worker {:?}, dropping event",
                        worker_id
                    );
                    return;
                }
                self.dispatch_active_worker_event(worker_id, event);
            }
        }
    }

    /// Promote a pending worker to active after namespace creation confirmed.
    fn handle_namespace_created(
        &mut self,
        worker_id: GlobalWorkerId,
        effects: &mut NamespaceEffects,
    ) {
        let pending = match self.pending_workers.remove(&worker_id) {
            Some(p) => p,
            None => return,
        };

        // Create router worker port.
        self.router.create_worker(worker_id);
        self.active_workers.insert(
            worker_id,
            ActiveWorkerInfo {
                default_pool: pending.info.default_pool.clone(),
            },
        );

        // Send initial DNS registry to the new worker.
        let sync_entries = self.adapters.dns_registry.build_sync();
        if !sync_entries.is_empty() {
            let cmd = distvirt_worker_protocol::WorkerCommand::RegistrySync {
                namespace_id: self.namespace_id.clone(),
                entries: sync_entries
                    .into_iter()
                    .map(|(name, ip)| distvirt_worker_protocol::RegistryEntry { name, ip })
                    .collect(),
            };
            effects.worker_commands.push((worker_id, cmd));
        }

        // Send initial endpoint state to the new worker.
        {
            let mut endpoints: Vec<distvirt_worker_protocol::EndpointSpec> = Vec::new();

            for (_endpoint_id, info) in self.adapters.endpoint.build_endpoint_sync() {
                endpoints.push(Self::build_endpoint_spec_from_info(&info));
            }

            for (_peer_id, info) in self.adapters.endpoint.build_wg_peer_sync() {
                endpoints.push(distvirt_worker_protocol::EndpointSpec {
                    ip: info.peer_ip,
                    kind: distvirt_worker_protocol::EndpointKind::WireGuardPeer {
                        placement: Some(distvirt_worker_protocol::EndpointPlacement {
                            worker_id: info.worker_id,
                        }),
                    },
                });
            }

            if !endpoints.is_empty() {
                let cmd = distvirt_worker_protocol::WorkerCommand::EndpointSync {
                    namespace_id: self.namespace_id.clone(),
                    endpoints,
                };
                effects.worker_commands.push((worker_id, cmd));
            }
        }

        // Send AddWireGuardPeer commands for existing peers placed on this worker.
        for (_peer_id, info) in self.adapters.endpoint.build_wg_peer_sync() {
            if info.worker_id == worker_id {
                effects.worker_commands.push((
                    worker_id,
                    distvirt_worker_protocol::WorkerCommand::AddWireGuardPeer {
                        namespace_id: self.namespace_id.clone(),
                        peer_public_key: info.peer_public_key,
                        peer_ip: info.peer_ip,
                        preshared_key: None,
                    },
                ));
            }
        }

        // Activate worker in router.
        self.router.set_worker_info(worker_id, pending.info);

        // Apply any scheduler grants that arrived before this worker was registered.
        let deferred: Vec<PodId> = self
            .deferred_grants
            .iter()
            .filter(|(_, w)| *w == worker_id)
            .map(|(p, _)| *p)
            .collect();
        self.deferred_grants.retain(|(_, w)| *w != worker_id);
        for pod_id in deferred {
            self.apply_grant(worker_id, pod_id);
        }
    }

    /// Dispatch a worker event from an active worker into the router.
    fn dispatch_active_worker_event(
        &mut self,
        worker_id: WorkerId,
        event: WorkerNamespaceEventKind,
    ) {
        match event {
            WorkerNamespaceEventKind::PodRunning { pod_id } => {
                let pod_id = router_pod_id(&pod_id);
                if self.router.get_pod(&pod_id).is_some() {
                    self.router
                        .send_notify_pod_status(worker_id, pod_id, PodStatus::Running);
                }
            }
            WorkerNamespaceEventKind::PodExited { pod_id, exit_code } => {
                let pod_id = router_pod_id(&pod_id);
                if self.router.get_pod(&pod_id).is_some() {
                    let status = if exit_code == 0 {
                        PodStatus::Finished
                    } else {
                        PodStatus::Failed
                    };
                    self.router
                        .send_notify_pod_status(worker_id, pod_id, status);
                }
            }
            WorkerNamespaceEventKind::PodFailed { pod_id } => {
                let pod_id = router_pod_id(&pod_id);
                if self.router.get_pod(&pod_id).is_some() {
                    self.router
                        .send_notify_pod_status(worker_id, pod_id, PodStatus::Failed);
                }
            }
            WorkerNamespaceEventKind::PodSuspended {
                pod_id,
                artifact_id,
            } => {
                let pod_id = router_pod_id(&pod_id);
                if self.router.get_pod(&pod_id).is_some() {
                    let artifact_port_id = ArtifactPortId(
                        artifact_id
                            .0
                            .parse::<u64>()
                            .expect("artifact ID must be numeric"),
                    );
                    self.router.create_artifact(artifact_port_id);
                    self.adapters.artifact.register_pending(artifact_port_id);
                    self.router
                        .send_notify_pod_suspended(worker_id, pod_id, artifact_port_id);
                }
            }
            WorkerNamespaceEventKind::PodSuspendFailed { pod_id } => {
                let pod_id = router_pod_id(&pod_id);
                if self.router.get_pod(&pod_id).is_some() {
                    self.router
                        .send_notify_pod_status(worker_id, pod_id, PodStatus::Failed);
                }
            }
            WorkerNamespaceEventKind::EndpointDemand {
                ip, signal, ..
            } => {
                let endpoint_id = self
                    .router
                    .iter_endpoint()
                    .find(|(_, ep)| ep.ip == ip)
                    .map(|(id, _)| *id);
                if let Some(endpoint_id) = endpoint_id {
                    self.adapters.endpoint_demand.push_demand(
                        &mut self.router,
                        worker_id,
                        endpoint_id,
                        signal,
                    );
                }
            }
            WorkerNamespaceEventKind::NamespaceCreated
            | WorkerNamespaceEventKind::NamespaceFailed { .. } => {
                unreachable!("handled in handle_worker_event")
            }
        }
    }

    // =========================================================================
    // Client command handling
    // =========================================================================

    fn handle_client_command(&mut self, cmd: ClientCommand) {
        match cmd {
            ClientCommand::UpdateSpec(new_spec) => {
                self.adapters.management.apply_namespace_spec(
                    &mut self.router,
                    self.current_spec.as_ref(),
                    &new_spec,
                );
                self.current_spec = Some(new_spec);
            }
            ClientCommand::PatchSpec(patch) => {
                if let Some(current) = self.current_spec.as_ref() {
                    let mut new_spec = current.clone();

                    for name in &patch.remove_workloads {
                        new_spec.workloads.remove(name);
                    }
                    for name in &patch.remove_services {
                        new_spec.services.remove(name);
                    }
                    for (name, spec) in patch.workloads {
                        new_spec.workloads.insert(name, spec);
                    }
                    for (name, spec) in patch.services {
                        new_spec.services.insert(name, spec);
                    }

                    self.adapters.management.apply_namespace_spec(
                        &mut self.router,
                        self.current_spec.as_ref(),
                        &new_spec,
                    );
                    self.current_spec = Some(new_spec);
                }
            }
            ClientCommand::AdminRestart { workload_name } => {
                self.adapters.management.send_admin_command(
                    &mut self.router,
                    &workload_name,
                    AdminCmd::Restart,
                );
            }
            ClientCommand::Scavenge { workload_name } => {
                self.adapters.management.send_admin_command(
                    &mut self.router,
                    &workload_name,
                    AdminCmd::Scavenge,
                );
            }
            ClientCommand::ActivateService {
                service_name,
                active,
            } => {
                self.adapters.management.send_activate_service(
                    &mut self.router,
                    &service_name,
                    active,
                );
            }
            ClientCommand::Connect {
                client_public_key,
                worker_id,
            } => {
                self.handle_wg_connect(client_public_key, worker_id);
            }
            ClientCommand::Disconnect { client_public_key } => {
                self.handle_wg_disconnect(client_public_key);
            }
        }
    }

    // =========================================================================
    // WireGuard peer management
    // =========================================================================

    fn handle_wg_connect(&mut self, client_public_key: [u8; 32], worker_id: WorkerId) {
        let result = self.wg_peer_mgr.connect(client_public_key);
        match result {
            crate::core::namespace::wg_peers::ConnectResult::Ok {
                client_ip: _,
                outputs,
            } => {
                for output in outputs {
                    match output {
                        WgPeerOutput::AddPeer {
                            peer_public_key,
                            peer_ip,
                        } => {
                            let peer_port_id = self.router.create_wire_guard_peer();
                            self.wg_peer_ports.insert(peer_public_key, peer_port_id);

                            self.router.set_wire_guard_peer_endpoint_info(
                                peer_port_id,
                                Some(WireGuardPeerEndpointInfo {
                                    peer_ip,
                                    worker_id,
                                    peer_public_key,
                                }),
                            );

                            self.router.set_wire_guard_peer_endpoints_edges(
                                peer_port_id,
                                vec![FABRIC_ENDPOINT],
                            );
                        }
                        WgPeerOutput::RemovePeer { .. } => {}
                    }
                }
            }
            crate::core::namespace::wg_peers::ConnectResult::Error { .. } => {}
        }
    }

    fn handle_wg_disconnect(&mut self, client_public_key: [u8; 32]) {
        let outputs = self.wg_peer_mgr.disconnect(client_public_key);
        for output in outputs {
            match output {
                WgPeerOutput::RemovePeer { peer_public_key } => {
                    if let Some(peer_port_id) = self.wg_peer_ports.remove(&peer_public_key) {
                        self.router.destroy_wire_guard_peer(peer_port_id);
                    }
                }
                WgPeerOutput::AddPeer { .. } => {}
            }
        }
    }

    // =========================================================================
    // WorkerToPod edge management
    // =========================================================================

    /// Apply a scheduler grant: create a lease for the pod on the given worker.
    fn apply_grant(&mut self, worker_id: WorkerId, pod_id: PodId) -> bool {
        if self.router.get_pod(&pod_id).is_none() {
            return false;
        }
        let lease_id = self.router.create_schedule_lease();
        self.router.set_schedule_lease_lease(
            lease_id,
            LeaseInfo {
                worker_id,
            },
        );
        self.router.set_pod_lease_edges(lease_id, vec![pod_id]);
        self.leases.insert(pod_id, lease_id);
        self.add_pod_to_worker(worker_id, pod_id);
        true
    }

    fn add_pod_to_worker(&mut self, worker_id: WorkerId, pod_id: PodId) {
        self.pod_worker.insert(pod_id, worker_id);
        let pods = self.worker_pod_edges.entry(worker_id).or_default();
        pods.insert(pod_id);
        self.router
            .set_worker_assignment_edges(worker_id, pods.iter().copied().collect::<Vec<_>>());
    }

    fn remove_pod_from_worker(&mut self, pod_id: PodId) {
        if let Some(worker_id) = self.pod_worker.remove(&pod_id) {
            if let Some(pods) = self.worker_pod_edges.get_mut(&worker_id) {
                pods.remove(&pod_id);
                self.router.set_worker_assignment_edges(
                    worker_id,
                    pods.iter().copied().collect::<Vec<_>>(),
                );
            }
        }
    }

    // =========================================================================
    // Reconcile + effect collection
    // =========================================================================

    /// Reconcile all adapters. Returns `(actions, mutated_router)`.
    fn reconcile(&mut self) -> (ReconcileActions, bool) {
        let (timer_actions, timer_mut) = self.adapters.timer.reconcile(&mut self.router);
        let (schedule_deltas, sched_mut) =
            self.adapters.schedule_request.reconcile(&mut self.router);
        let (pod_actions, pod_mut) = self.adapters.pod_assignment.reconcile(&mut self.router);
        let (endpoint_actions, ep_mut) = self.adapters.endpoint.reconcile(&mut self.router);
        let (dns_registry_actions, dns_mut) =
            self.adapters.dns_registry.reconcile(&mut self.router);
        let artifact_mut = self.adapters.artifact.reconcile(&mut self.router);

        let observability_events = self.adapters.observability.reconcile(&mut self.router);

        self.adapters.management.sync_dynamic_ids(&self.router);

        let mutated = timer_mut || sched_mut || pod_mut || ep_mut || dns_mut || artifact_mut;

        (
            ReconcileActions {
                timer_actions,
                schedule_deltas,
                pod_actions,
                endpoint_actions,
                dns_registry_actions,
                observability_events,
            },
            mutated,
        )
    }

    /// Translate reconcile actions directly into external effects.
    fn collect_effects(&mut self, actions: ReconcileActions, effects: &mut NamespaceEffects) {
        // Timer actions pass through.
        effects.timer_actions.extend(actions.timer_actions);

        // Schedule request deltas → scheduler messages.
        for delta in actions.schedule_deltas {
            match delta {
                ScheduleRequestDelta::Request { pod_id, request } => {
                    let proto_resume_artifact = request
                        .resume_artifact
                        .map(|port_id| ArtifactId(port_id.0.to_string()));
                    effects
                        .scheduler_messages
                        .push(SchedulerMessage::RequestLease {
                            namespace_id: self.namespace_id.clone(),
                            pod_id,
                            proto_resume_artifact,
                        });
                }
                ScheduleRequestDelta::Drop { pod_id } => {
                    effects
                        .scheduler_messages
                        .push(SchedulerMessage::DropRequest {
                            namespace_id: self.namespace_id.clone(),
                            pod_id,
                        });
                }
            }
        }

        // Pod assignment actions → worker commands.
        for action in actions.pod_actions {
            match action {
                PodAssignmentAction::Launch {
                    worker_id,
                    pod_id,
                    spec,
                    ..
                } => {
                    let cmd = self.build_launch_command(&proto_pod_id(pod_id), &spec);
                    effects.worker_commands.push((worker_id, cmd));
                }
                PodAssignmentAction::Resume {
                    worker_id,
                    pod_id,
                    artifact_id,
                    spec,
                } => {
                    let proto_artifact_id = ArtifactId(artifact_id.0.to_string());
                    let cmd = self.build_resume_command(
                        worker_id,
                        &proto_pod_id(pod_id),
                        &proto_artifact_id,
                        &spec,
                    );
                    effects.worker_commands.push((worker_id, cmd));
                }
                PodAssignmentAction::Stop { worker_id, pod_id } => {
                    let cmd = distvirt_worker_protocol::WorkerCommand::StopPod {
                        namespace_id: self.namespace_id.clone(),
                        pod_id: proto_pod_id(pod_id),
                        graceful: true,
                    };
                    effects.worker_commands.push((worker_id, cmd));
                }
                PodAssignmentAction::Suspend { worker_id, pod_id } => {
                    let pool_id = self
                        .active_workers
                        .get(&worker_id)
                        .and_then(|w| w.default_pool.clone())
                        .expect("suspend target worker must have a pool");
                    let artifact_port_id = self.alloc_artifact_id();
                    let proto_artifact_id = ArtifactId(artifact_port_id.0.to_string());
                    let cmd = distvirt_worker_protocol::WorkerCommand::SuspendPod {
                        namespace_id: self.namespace_id.clone(),
                        pod_id: proto_pod_id(pod_id),
                        artifact_id: proto_artifact_id,
                        pool_id,
                    };
                    effects.worker_commands.push((worker_id, cmd));
                }
            }
        }

        // Endpoint actions → broadcast commands + WG worker commands.
        for action in actions.endpoint_actions {
            let cmd = self.build_endpoint_command(&action);
            effects.broadcast_commands.push(cmd);

            match &action {
                EndpointAction::WireGuardPeerUpdate { info, .. } => {
                    effects.worker_commands.push((
                        info.worker_id,
                        distvirt_worker_protocol::WorkerCommand::AddWireGuardPeer {
                            namespace_id: self.namespace_id.clone(),
                            peer_public_key: info.peer_public_key,
                            peer_ip: info.peer_ip,
                            preshared_key: None,
                        },
                    ));
                }
                EndpointAction::WireGuardPeerRemove { old_info, .. } => {
                    effects.broadcast_commands.push(
                        distvirt_worker_protocol::WorkerCommand::RemoveWireGuardPeer {
                            peer_public_key: old_info.peer_public_key,
                        },
                    );
                }
                _ => {}
            }
        }

        // DNS registry actions → broadcast commands.
        if !actions.dns_registry_actions.is_empty() {
            let mut added = Vec::new();
            let mut removed = Vec::new();
            for action in actions.dns_registry_actions {
                match action {
                    DnsRegistryAction::Add { name, ip } => {
                        added.push(distvirt_worker_protocol::RegistryEntry { name, ip });
                    }
                    DnsRegistryAction::Remove { name } => {
                        removed.push(name);
                    }
                }
            }
            if !added.is_empty() || !removed.is_empty() {
                effects.broadcast_commands.push(
                    distvirt_worker_protocol::WorkerCommand::RegistryUpdate {
                        namespace_id: self.namespace_id.clone(),
                        added,
                        removed,
                    },
                );
            }
        }

        // Observability events pass through.
        effects
            .observability_events
            .extend(actions.observability_events);
    }

    /// Allocate a new artifact port ID for suspend operations.
    fn alloc_artifact_id(&mut self) -> ArtifactPortId {
        self.next_artifact_counter += 1;
        ArtifactPortId(self.next_artifact_counter)
    }

    // =========================================================================
    // Command building
    // =========================================================================

    fn build_launch_command(
        &self,
        proto_pod_id: &distvirt_worker_protocol::PodId,
        spec: &Option<crate::sm::WorkloadSpec>,
    ) -> distvirt_worker_protocol::WorkerCommand {
        let mut network = spec
            .as_ref()
            .and_then(|s| s.pod_spec.network.clone())
            .unwrap_or_else(default_pod_network);
        self.fill_network_from_namespace(&mut network);
        let containers = spec
            .as_ref()
            .map(|s| s.pod_spec.containers.clone())
            .unwrap_or_default();
        let resources = spec.as_ref().and_then(|s| s.pod_spec.resources.clone());
        let volumes = spec
            .as_ref()
            .map(|s| s.pod_spec.volumes.clone())
            .unwrap_or_default();

        distvirt_worker_protocol::WorkerCommand::LaunchPod {
            namespace_id: self.namespace_id.clone(),
            pod_id: *proto_pod_id,
            network,
            containers,
            resources,
            volumes,
        }
    }

    fn build_resume_command(
        &self,
        worker_id: GlobalWorkerId,
        proto_pod_id: &distvirt_worker_protocol::PodId,
        proto_artifact_id: &distvirt_worker_protocol::ArtifactId,
        spec: &Option<crate::sm::WorkloadSpec>,
    ) -> distvirt_worker_protocol::WorkerCommand {
        let pool_id = self
            .active_workers
            .get(&worker_id)
            .and_then(|w| w.default_pool.clone())
            .expect("resume target worker must have a pool");
        let mut network = spec
            .as_ref()
            .and_then(|s| s.pod_spec.network.clone())
            .unwrap_or_else(default_pod_network);
        self.fill_network_from_namespace(&mut network);

        distvirt_worker_protocol::WorkerCommand::ResumePod {
            namespace_id: self.namespace_id.clone(),
            pod_id: *proto_pod_id,
            artifact_id: proto_artifact_id.clone(),
            network,
            pool_id,
        }
    }

    fn fill_network_from_namespace(&self, network: &mut distvirt_worker_protocol::PodNetworkConfig) {
        if let Some(spec) = self.current_spec.as_ref() {
            network.gateway = spec.network.gateway;
            network.netmask = prefix_len_to_netmask(spec.network.prefix_len);
        }
    }

    fn build_endpoint_spec_from_info(
        info: &crate::sm::EndpointInfo,
    ) -> distvirt_worker_protocol::EndpointSpec {
        use crate::sm::endpoint::EndpointKind;
        let proto_kind = match &info.kind {
            EndpointKind::Service {
                service_id,
                policy,
            } => distvirt_worker_protocol::EndpointKind::Service {
                service_id: *service_id,
                policy: policy.clone(),
                backend: info.backend.as_ref().map(|b| {
                    distvirt_worker_protocol::EndpointPodBackend {
                        pod_ip: b.ip.unwrap(),
                        placement: Some(distvirt_worker_protocol::EndpointPlacement {
                            worker_id: b.worker_id,
                        }),
                        ready: true,
                    }
                }),
            },
            EndpointKind::Workload => distvirt_worker_protocol::EndpointKind::Pod {
                placement: info.backend.as_ref().map(|b| {
                    distvirt_worker_protocol::EndpointPlacement {
                        worker_id: b.worker_id,
                    }
                }),
            },
        };
        distvirt_worker_protocol::EndpointSpec {
            ip: info.ip,
            kind: proto_kind,
        }
    }

    fn build_endpoint_command(
        &self,
        action: &EndpointAction,
    ) -> distvirt_worker_protocol::WorkerCommand {
        match action {
            EndpointAction::Update {
                endpoint_id: _,
                info,
            } => {
                let endpoint_spec = Self::build_endpoint_spec_from_info(info);
                distvirt_worker_protocol::WorkerCommand::EndpointUpdate {
                    namespace_id: self.namespace_id.clone(),
                    upserted: vec![endpoint_spec],
                    removed_ips: vec![],
                }
            }
            EndpointAction::Remove {
                endpoint_id: _,
                old_info,
            } => distvirt_worker_protocol::WorkerCommand::EndpointUpdate {
                namespace_id: self.namespace_id.clone(),
                upserted: vec![],
                removed_ips: vec![old_info.ip],
            },
            EndpointAction::WireGuardPeerUpdate { peer_id: _, info } => {
                let endpoint_spec = distvirt_worker_protocol::EndpointSpec {
                    ip: info.peer_ip,
                    kind: distvirt_worker_protocol::EndpointKind::WireGuardPeer {
                        placement: Some(distvirt_worker_protocol::EndpointPlacement {
                            worker_id: info.worker_id,
                        }),
                    },
                };
                distvirt_worker_protocol::WorkerCommand::EndpointUpdate {
                    namespace_id: self.namespace_id.clone(),
                    upserted: vec![endpoint_spec],
                    removed_ips: vec![],
                }
            }
            EndpointAction::WireGuardPeerRemove {
                peer_id: _,
                old_info,
            } => distvirt_worker_protocol::WorkerCommand::EndpointUpdate {
                namespace_id: self.namespace_id.clone(),
                upserted: vec![],
                removed_ips: vec![old_info.peer_ip],
            },
        }
    }

    // =========================================================================
    // Accessors
    // =========================================================================

    /// Get the set of active worker IDs.
    pub fn active_worker_ids(&self) -> impl Iterator<Item = GlobalWorkerId> + '_ {
        self.active_workers.keys().copied()
    }

    /// Access the router.
    pub fn router(&self) -> &DRouter {
        &self.router
    }

    /// Mutable access to the router (for test setup).
    #[allow(dead_code)]
    pub(crate) fn router_mut(&mut self) -> &mut DRouter {
        &mut self.router
    }

    /// Access the management adapter.
    pub fn management(&self) -> &ManagementAdapter {
        &self.adapters.management
    }

    /// Access the WireGuard peer manager.
    pub fn wg_peers(&self) -> &WireGuardPeerManager {
        &self.wg_peer_mgr
    }

    /// Access the current namespace spec.
    pub fn current_spec(&self) -> Option<&NamespaceSpec> {
        self.current_spec.as_ref()
    }

    /// Get the namespace ID.
    pub(crate) fn namespace_id(&self) -> &NamespaceId {
        &self.namespace_id
    }

    /// Build a status report for this namespace.
    pub fn status_report(&self) -> NamespaceStatusReport {
        use std::collections::BTreeMap;

        let mut workloads = BTreeMap::new();
        let mut services = BTreeMap::new();
        let mut pods = BTreeMap::new();

        // Workloads
        for (name, router_id) in self.adapters.management.iter_workloads() {
            let state = self
                .router
                .signal_workload_status(router_id)
                .map(|s| sm_wl_status_to_client(s))
                .unwrap_or(crate::types::WorkloadStatus::Dormant);
            let wl_sm = self.router.get_workload(&router_id);
            let pod_id = wl_sm
                .as_ref()
                .and_then(|wl| wl.pod_id)
                .map(|pid| crate::types::PodId(pid.0));
            let ip = wl_sm
                .as_ref()
                .and_then(|wl| wl.endpoint_id)
                .and_then(|ep_id| self.router.get_endpoint(&ep_id))
                .map(|ep| ep.ip.to_string())
                .unwrap_or_default();

            workloads.insert(
                WorkloadName(name.to_string()),
                crate::types::WorkloadStatusReport {
                    state,
                    pod_id,
                    ip,
                    conditions: BTreeMap::new(),
                },
            );
        }

        // Services
        for (name, router_id) in self.adapters.management.iter_services() {
            let svc_sm = self.router.get_service(&router_id);
            let ep_id = svc_sm.and_then(|s| s.endpoint_id);

            let service_state = ep_id
                .and_then(|id| self.router.signal_endpoint_status(id))
                .map(|s| sm_endpoint_status_to_client(s))
                .unwrap_or(crate::types::ServiceStatus::Pending);
            let backend_need = ep_id
                .and_then(|id| self.router.signal_endpoint_current_backend_need(id))
                .cloned();
            let ep_sm = ep_id.as_ref().and_then(|id| self.router.get_endpoint(id));
            let has_activation = ep_sm.map(|s| s.has_activation).unwrap_or(false);
            let service_ip = ep_sm
                .map(|s| s.ip)
                .unwrap_or(std::net::Ipv4Addr::UNSPECIFIED);

            let workload_name = self
                .current_spec
                .as_ref()
                .and_then(|spec| spec.services.get(name))
                .map(|svc_spec| svc_spec.workload_id.clone())
                .unwrap_or_else(|| WorkloadName(String::new()));

            services.insert(
                name.to_string(),
                crate::types::ServiceStatusReport {
                    workload_id: workload_name,
                    service_state,
                    backend_need,
                    activation_enabled: has_activation,
                    ip: service_ip.to_string(),
                    conditions: BTreeMap::new(),
                },
            );
        }

        // Pods
        for (pod_id, pod_sm) in self.router.iter_pod() {
            if pod_sm.workload_id.is_none() {
                continue;
            }
            let workload_router_id = pod_sm.workload_id.unwrap();
            let workload_name = self
                .adapters
                .management
                .workload_proto_name(&workload_router_id)
                .map(|n| WorkloadName(n.to_string()))
                .unwrap_or_else(|| WorkloadName(String::new()));

            let proto_pod_id = crate::types::PodId(pod_id.0);
            let worker_id = pod_sm
                .worker_id
                .map(|w| crate::types::WorkerId(w.0))
                .unwrap_or(crate::types::WorkerId(0));
            let ip = pod_sm
                .launch_spec
                .as_ref()
                .and_then(|s| s.pod_spec.network.as_ref())
                .map(|n| n.ip.to_string())
                .unwrap_or_default();
            let state = sm_pod_status_to_client(&pod_sm.status);

            pods.insert(
                proto_pod_id.clone(),
                crate::types::PodStatusReport {
                    pod_id: proto_pod_id,
                    workload_id: workload_name,
                    worker_id,
                    ip,
                    state,
                },
            );
        }

        NamespaceStatusReport {
            namespace_id: self.namespace_id.clone(),
            status: crate::types::NamespaceStatus::Active,
            workloads,
            services,
            pods,
        }
    }

    /// Map a router-internal PodId to a protocol PodId.
    pub fn router_pod_to_proto(
        &self,
        router_pid: &PodId,
    ) -> Option<distvirt_worker_protocol::PodId> {
        Some(proto_pod_id(*router_pid))
    }
}

// =============================================================================
// Standalone helpers
// =============================================================================

fn default_pod_network() -> distvirt_worker_protocol::PodNetworkConfig {
    distvirt_worker_protocol::PodNetworkConfig {
        ip: std::net::Ipv4Addr::new(0, 0, 0, 0),
        mac: [0; 6],
        gateway: std::net::Ipv4Addr::new(0, 0, 0, 0),
        netmask: String::new(),
    }
}

fn prefix_len_to_netmask(prefix_len: u8) -> String {
    let mask = if prefix_len == 0 {
        0u32
    } else {
        !0u32 << (32 - prefix_len)
    };
    std::net::Ipv4Addr::from(mask).to_string()
}

fn sm_wl_status_to_client(status: &WlStatus) -> crate::types::WorkloadStatus {
    match status {
        WlStatus::Dormant => crate::types::WorkloadStatus::Dormant,
        WlStatus::WaitingForSpec => crate::types::WorkloadStatus::WaitingForSpec,
        WlStatus::Launching => crate::types::WorkloadStatus::Launching,
        WlStatus::Running => crate::types::WorkloadStatus::Running,
        WlStatus::Suspending => crate::types::WorkloadStatus::Suspending,
        WlStatus::Suspended => crate::types::WorkloadStatus::Suspended,
        WlStatus::RetryBackoff => crate::types::WorkloadStatus::RetryBackoff,
        WlStatus::Failed => crate::types::WorkloadStatus::Failed,
        WlStatus::Completed => crate::types::WorkloadStatus::Completed,
    }
}

fn sm_endpoint_status_to_client(status: &EndpointStatus) -> crate::types::ServiceStatus {
    match status {
        EndpointStatus::Idle => crate::types::ServiceStatus::Idle,
        EndpointStatus::NeedBackend => crate::types::ServiceStatus::NeedBackend,
        EndpointStatus::Active => crate::types::ServiceStatus::Active,
    }
}

fn sm_pod_status_to_client(status: &PodStatus) -> crate::types::PodStatus {
    match status {
        PodStatus::Pending => crate::types::PodStatus::Launching,
        PodStatus::Running => crate::types::PodStatus::Running,
        PodStatus::Suspending => crate::types::PodStatus::Suspending,
        PodStatus::Suspended { .. } => crate::types::PodStatus::Suspended,
        PodStatus::Finished | PodStatus::Failed | PodStatus::Displaced => {
            crate::types::PodStatus::Launching // terminal pods are filtered out
        }
    }
}
