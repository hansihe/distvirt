//! Pure namespace core — no async, no channels.
//!
//! Extracted from `task/namespace/mod.rs`. This module contains all the
//! pure state and logic for a single namespace, producing effects instead
//! of performing I/O directly.
//!
//! `NamespaceCore` operates exclusively with router-internal IDs. All
//! protocol ↔ router ID translation is handled by `NamespaceWithBoundary`
//! in `namespace_boundary.rs`.

use std::collections::{HashMap, HashSet};

use crate::adapter::artifact::{ArtifactAction, ArtifactAdapter};
use crate::adapter::dns_registry::{DnsRegistryAction, DnsRegistryAdapter};
use crate::adapter::endpoint::{EndpointAction, EndpointAdapter};
use crate::adapter::endpoint_demand::EndpointDemandAdapter;
use crate::adapter::management::ManagementAdapter;
use crate::adapter::observability::{ObservabilityAdapter, ObservabilityEvent};
use crate::id_registry::IdRegistry;
use crate::adapter::pod_assignment::{PodAssignmentAction, PodAssignmentAdapter};
use crate::adapter::schedule_request::{ScheduleRequestAdapter, ScheduleRequestDelta};
use crate::adapter::timer::{TimerAction, TimerAdapter, TimerConfig};
use crate::core::ClientCommand;
use distvirt_sm_router::trace::PanicTracer;

use crate::core::wg_peers::{WgPeerOutput, WireGuardPeerManager};
use crate::sm::{
    AdminCmd, DNS_REGISTRY, DRouter, FABRIC_ENDPOINT, LeaseInfo, OBSERVABILITY, PodId, PodStatus,
    Router, SCHEDULE_REQUEST, ScheduleLeaseId, TIMER, WireGuardPeerEndpointInfo, WireGuardPeerId,
    WlStatus, WorkerId, endpoint::EndpointStatus,
};
use crate::types::{NamespaceId, NamespaceSpec, NamespaceStatusReport, WorkloadName};

use super::types::{
    InternalNamespaceEffects, InternalNamespaceEvent, InternalSchedulerMessage, InternalWorkerEvent,
};

// TODO: update for EndpointSm refactor
// #[cfg(test)]
// mod tests;

// =============================================================================
// Grouped state
// =============================================================================

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

// =============================================================================
// Reconcile actions (sync output of reconcile phase)
// =============================================================================

struct ReconcileActions {
    timer_actions: Vec<TimerAction>,
    schedule_deltas: Vec<ScheduleRequestDelta>,
    pod_actions: Vec<PodAssignmentAction>,
    endpoint_actions: Vec<EndpointAction>,
    dns_registry_actions: Vec<DnsRegistryAction>,
    observability_events: Vec<ObservabilityEvent>,
}

// =============================================================================
// NamespaceCore
// =============================================================================

pub struct NamespaceCore {
    namespace_id: NamespaceId,
    router: DRouter,
    pub(crate) adapters: Adapters,

    leases: HashMap<PodId, ScheduleLeaseId>,

    /// Tracks which pods are assigned to each worker (for WorkerToPod edge management).
    worker_pod_edges: HashMap<WorkerId, HashSet<PodId>>,
    /// Reverse lookup: pod → assigned worker.
    pod_worker: HashMap<PodId, WorkerId>,

    pub(crate) current_spec: Option<NamespaceSpec>,

    /// WireGuard peer IP allocation and tracking.
    wg_peer_mgr: WireGuardPeerManager,
    /// Maps client public key → WireGuardPeerId (router port).
    wg_peer_ports: HashMap<[u8; 32], WireGuardPeerId>,
}

impl NamespaceCore {
    pub fn new(
        namespace_id: NamespaceId,
        timer_config: TimerConfig,
        network: &distvirt_worker_protocol::NetworkConfig,
        id_registry: IdRegistry,
    ) -> Self {
        let mut router = Router::new_traced(16, PanicTracer::new());
        router.create_timer(TIMER);
        router.create_schedule_request(SCHEDULE_REQUEST);
        router.create_fabric_endpoint(FABRIC_ENDPOINT);
        router.create_dns_registry(DNS_REGISTRY);
        router.create_observability(OBSERVABILITY);

        NamespaceCore {
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
        }
    }

    /// Top-level event processing: push event, propagate, reconcile loop.
    /// Returns all effects to be executed by the boundary layer.
    /// All IDs are router-internal.
    pub(crate) fn process_event(
        &mut self,
        event: InternalNamespaceEvent,
    ) -> InternalNamespaceEffects {
        let mut effects = InternalNamespaceEffects::default();

        // Phase 1: Push external event into router
        self.push_event(event, &mut effects);

        // Phase 2: Propagate
        self.router.propagate();

        // Phase 3+4: Reconcile and collect effects in a loop until stable.
        // The loop re-propagates only when an adapter signals that it wrote
        // back into the router (mutated_router == true).
        loop {
            let (actions, mutated_router) = self.reconcile();
            self.collect_effects(actions, &mut effects);
            if !mutated_router {
                break;
            }
            self.router.propagate();
        }

        // Phase 5: Drain deduped artifact actions and emit scheduler messages.
        // Also cleans up orphaned ports.
        for action in self.adapters.artifact.finalize(&mut self.router) {
            match action {
                ArtifactAction::Referenced { port_id } => {
                    effects
                        .scheduler_messages
                        .push(InternalSchedulerMessage::ArtifactReferenced {
                            namespace_id: self.namespace_id.clone(),
                            artifact_port_id: port_id,
                        });
                }
                ArtifactAction::Released { port_id } => {
                    effects
                        .scheduler_messages
                        .push(InternalSchedulerMessage::ArtifactReleased {
                            namespace_id: self.namespace_id.clone(),
                            artifact_port_id: port_id,
                        });
                }
            }
        }

        effects
    }

    fn push_event(
        &mut self,
        event: InternalNamespaceEvent,
        _effects: &mut InternalNamespaceEffects,
    ) {
        match event {
            InternalNamespaceEvent::WorkerEvent { worker_id, event } => {
                match event {
                    InternalWorkerEvent::PodRunning { pod_id } => {
                        if self.router.get_pod(&pod_id).is_some() {
                            self.router.send_notify_pod_status(
                                worker_id,
                                pod_id,
                                PodStatus::Running,
                            );
                        }
                    }
                    InternalWorkerEvent::PodExited { pod_id, exit_code } => {
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
                    InternalWorkerEvent::PodFailed { pod_id } => {
                        if self.router.get_pod(&pod_id).is_some() {
                            self.router.send_notify_pod_status(
                                worker_id,
                                pod_id,
                                PodStatus::Failed,
                            );
                        }
                    }
                    InternalWorkerEvent::PodSuspended {
                        pod_id,
                        artifact_id,
                    } => {
                        if self.router.get_pod(&pod_id).is_some() {
                            // Create artifact port so the workload can reference it.
                            self.router.create_artifact(artifact_id);
                            self.adapters.artifact.register_pending(artifact_id);
                            self.router
                                .send_notify_pod_suspended(worker_id, pod_id, artifact_id);
                        }
                    }
                    InternalWorkerEvent::PodSuspendFailed { pod_id } => {
                        if self.router.get_pod(&pod_id).is_some() {
                            self.router.send_notify_pod_status(
                                worker_id,
                                pod_id,
                                PodStatus::Failed,
                            );
                        }
                    }
                    InternalWorkerEvent::EndpointDemand { ip, signal } => {
                        // Look up the endpoint by IP.
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
                }
            }
            InternalNamespaceEvent::SchedulerGrant { worker_id, pod_id } => {
                self.apply_grant(worker_id, pod_id);
            }
            InternalNamespaceEvent::SchedulerRevoke { pod_id } => {
                if let Some(lease_id) = self.leases.remove(&pod_id) {
                    self.router.destroy_schedule_lease(lease_id);
                }
                self.remove_pod_from_worker(pod_id);
            }
            InternalNamespaceEvent::TimerFired { identity } => {
                self.adapters.timer.fire(&mut self.router, &identity);
            }
            InternalNamespaceEvent::WorkerActivated { worker_id, info } => {
                self.router.set_worker_info(worker_id, info);
            }
            InternalNamespaceEvent::WorkerDeactivated { worker_id } => {
                // Clean up WorkerToPod edge tracking for this worker.
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
            InternalNamespaceEvent::ClientCommand(cmd) => {
                self.handle_client_command(cmd);
            }
            InternalNamespaceEvent::ArtifactInvalidated { artifact_port_id } => {
                self.router.destroy_artifact(artifact_port_id);
            }
        }
    }

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

                    // Removals first
                    for name in &patch.remove_workloads {
                        new_spec.workloads.remove(name);
                    }
                    for name in &patch.remove_services {
                        new_spec.services.remove(name);
                    }

                    // Upserts
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

    fn handle_wg_connect(&mut self, client_public_key: [u8; 32], worker_id: WorkerId) {
        let result = self.wg_peer_mgr.connect(client_public_key);
        match result {
            crate::core::wg_peers::ConnectResult::Ok { client_ip, outputs } => {
                for output in outputs {
                    match output {
                        WgPeerOutput::AddPeer {
                            peer_public_key,
                            peer_ip,
                        } => {
                            // Create router port for this WG peer.
                            let peer_port_id = self.router.create_wire_guard_peer();
                            self.wg_peer_ports.insert(peer_public_key, peer_port_id);

                            // Set endpoint info signal (includes public key so the
                            // boundary can derive AddWireGuardPeer commands from
                            // endpoint actions).
                            self.router.set_wire_guard_peer_endpoint_info(
                                peer_port_id,
                                Some(WireGuardPeerEndpointInfo {
                                    peer_ip,
                                    worker_id,
                                    peer_public_key,
                                }),
                            );

                            // Connect WG peer port to fabric endpoint port.
                            self.router.set_wire_guard_peer_endpoints_edges(
                                peer_port_id,
                                vec![FABRIC_ENDPOINT],
                            );
                        }
                        WgPeerOutput::RemovePeer { .. } => {
                            // Shouldn't happen on connect.
                        }
                    }
                }
                let _ = client_ip; // IP is returned to caller via ConnectResult at orchestrator level.
            }
            crate::core::wg_peers::ConnectResult::Error { .. } => {
                // Error is returned to caller at orchestrator level.
            }
        }
    }

    fn handle_wg_disconnect(&mut self, client_public_key: [u8; 32]) {
        let outputs = self.wg_peer_mgr.disconnect(client_public_key);
        for output in outputs {
            match output {
                WgPeerOutput::RemovePeer { peer_public_key } => {
                    // Destroy the router port (clears signals/edges automatically).
                    // This produces a WireGuardPeerRemove endpoint action via the
                    // incremental aggregator, which the boundary translates into
                    // both EndpointUpdate and RemoveWireGuardPeer commands.
                    if let Some(peer_port_id) = self.wg_peer_ports.remove(&peer_public_key) {
                        self.router.destroy_wire_guard_peer(peer_port_id);
                    }
                }
                WgPeerOutput::AddPeer { .. } => {
                    // Shouldn't happen on disconnect.
                }
            }
        }
    }

    // =========================================================================
    // WorkerToPod edge management
    // =========================================================================

    /// Apply a scheduler grant: create a lease for the pod on the given worker.
    /// Returns false if the pod no longer exists in the router (stale grant).
    fn apply_grant(&mut self, router_worker_id: WorkerId, pod_id: PodId) -> bool {
        if self.router.get_pod(&pod_id).is_none() {
            return false;
        }
        let lease_id = self.router.create_schedule_lease();
        self.router.set_schedule_lease_lease(
            lease_id,
            LeaseInfo {
                worker_id: router_worker_id,
            },
        );
        self.router.set_pod_lease_edges(lease_id, vec![pod_id]);
        self.leases.insert(pod_id, lease_id);
        self.add_pod_to_worker(router_worker_id, pod_id);
        true
    }

    /// Add a pod to a worker's WorkerToPod edge set and update the router.
    fn add_pod_to_worker(&mut self, worker_id: WorkerId, pod_id: PodId) {
        self.pod_worker.insert(pod_id, worker_id);
        let pods = self.worker_pod_edges.entry(worker_id).or_default();
        pods.insert(pod_id);
        self.router
            .set_worker_assignment_edges(worker_id, pods.iter().copied().collect::<Vec<_>>());
    }

    /// Remove a pod from its assigned worker's WorkerToPod edge set and update the router.
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

    /// Phase 3: Reconcile all adapters. Pure/sync — no I/O.
    /// Returns `(actions, mutated_router)`.
    /// `mutated_router` is `true` when any adapter wrote back into the router.
    fn reconcile(&mut self) -> (ReconcileActions, bool) {
        let (timer_actions, timer_mut) = self.adapters.timer.reconcile(&mut self.router);
        let (schedule_deltas, sched_mut) =
            self.adapters.schedule_request.reconcile(&mut self.router);
        let (pod_actions, pod_mut) = self.adapters.pod_assignment.reconcile(&mut self.router);
        let (endpoint_actions, ep_mut) = self.adapters.endpoint.reconcile(&mut self.router);
        let (dns_registry_actions, dns_mut) =
            self.adapters.dns_registry.reconcile(&mut self.router);
        let artifact_mut = self.adapters.artifact.reconcile(&mut self.router);

        // Observability runs last — read-only, never mutates router.
        let observability_events = self.adapters.observability.reconcile(&mut self.router);

        // Sync dynamic ID mappings (endpoint→service, pod→workload) now that
        // the router has converged and SMs have their current endpoint/pod IDs.
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

    /// Phase 4: Translate reconcile actions into internal effects.
    fn collect_effects(
        &mut self,
        actions: ReconcileActions,
        effects: &mut InternalNamespaceEffects,
    ) {
        // Timer actions pass through directly.
        effects.timer_actions.extend(actions.timer_actions);

        // Schedule request deltas → internal scheduler messages.
        for delta in actions.schedule_deltas {
            match delta {
                ScheduleRequestDelta::Request { pod_id, request } => {
                    effects
                        .scheduler_messages
                        .push(InternalSchedulerMessage::RequestLease {
                            namespace_id: self.namespace_id.clone(),
                            pod_id,
                            resume_artifact: request.resume_artifact,
                        });
                }
                ScheduleRequestDelta::Drop { pod_id } => {
                    effects
                        .scheduler_messages
                        .push(InternalSchedulerMessage::DropRequest {
                            namespace_id: self.namespace_id.clone(),
                            pod_id,
                        });
                }
            }
        }

        // Pod assignment actions — spec now flows through the signal graph
        // (Workload::PodLaunchSpec → Pod::LaunchSpecInput → PodScheduleRequest → Worker port).
        effects.pod_actions.extend(actions.pod_actions);

        // Endpoint actions pass through directly (already router-level).
        effects.endpoint_actions.extend(actions.endpoint_actions);

        // DNS registry actions pass through directly.
        effects
            .dns_registry_actions
            .extend(actions.dns_registry_actions);

        // Observability events pass through directly.
        effects
            .observability_events
            .extend(actions.observability_events);
    }

    /// Create a new worker port in the router with the given ID.
    pub(crate) fn create_worker_port(&mut self, id: WorkerId) {
        self.router.create_worker(id);
    }

    // =========================================================================
    // Accessors
    // =========================================================================

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
                .map(|s| wl_status_str(s))
                .unwrap_or("unknown");
            let pod_id = self
                .router
                .get_workload(&router_id)
                .and_then(|wl| wl.pod_id)
                .map(|pid| crate::types::PodId(pid.0));

            workloads.insert(
                WorkloadName(name.to_string()),
                crate::types::WorkloadStatusReport {
                    state: state.to_string(),
                    pod_id,
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
                .map(|s| endpoint_status_str(s))
                .unwrap_or("unknown");
            let backend_need = ep_id
                .and_then(|id| self.router.signal_endpoint_current_backend_need(id))
                .cloned();
            let ep_sm = ep_id.as_ref().and_then(|id| self.router.get_endpoint(id));
            let has_activation = ep_sm.map(|s| s.has_activation).unwrap_or(false);
            let service_ip = ep_sm
                .map(|s| s.ip)
                .unwrap_or(std::net::Ipv4Addr::UNSPECIFIED);

            // Look up the workload name from the spec.
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
                    service_state: service_state.to_string(),
                    backend_need,
                    activation_enabled: has_activation,
                    ip: service_ip.to_string(),
                    conditions: BTreeMap::new(),
                },
            );
        }

        // Pods
        for (pod_id, pod_sm) in self.router.iter_pod() {
            // Skip terminal pods with no workload (being reaped).
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
                .and_then(|s| s.network.as_ref())
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

    /// Access the router (for inspecting workload/service/pod state).
    pub fn router(&self) -> &DRouter {
        &self.router
    }

    /// Mutable access to the router (for test setup).
    pub(crate) fn router_mut(&mut self) -> &mut DRouter {
        &mut self.router
    }

    /// Access the management adapter (for looking up workloads/services by name).
    pub fn management(&self) -> &ManagementAdapter {
        &self.adapters.management
    }

    /// Access the current namespace spec.
    pub fn current_spec(&self) -> Option<&NamespaceSpec> {
        self.current_spec.as_ref()
    }

    /// Get the namespace ID.
    pub(crate) fn namespace_id(&self) -> &NamespaceId {
        &self.namespace_id
    }

    /// Access the WireGuard peer manager (for reading subnet info etc.).
    pub fn wg_peers(&self) -> &WireGuardPeerManager {
        &self.wg_peer_mgr
    }
}

fn wl_status_str(status: &WlStatus) -> &'static str {
    match status {
        WlStatus::Dormant => "dormant",
        WlStatus::WaitingForSpec => "waiting_for_spec",
        WlStatus::Launching => "launching",
        WlStatus::Running => "running",
        WlStatus::Suspending => "suspending",
        WlStatus::Suspended => "suspended",
        WlStatus::RetryBackoff => "retry_backoff",
        WlStatus::Failed => "failed",
        WlStatus::Completed => "completed",
    }
}

fn endpoint_status_str(status: &EndpointStatus) -> &'static str {
    match status {
        EndpointStatus::Idle => "idle",
        EndpointStatus::NeedBackend => "need_backend",
        EndpointStatus::Active => "active",
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
