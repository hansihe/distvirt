//! Pure namespace core — no async, no channels.
//!
//! Extracted from `task/namespace/mod.rs`. This module contains all the
//! pure state and logic for a single namespace, producing effects instead
//! of performing I/O directly.

use std::collections::{HashMap, HashSet};
use std::net::Ipv4Addr;

use crate::adapter::backend_need::BackendNeedAdapter;
use crate::adapter::endpoint::{EndpointAction, EndpointAdapter, RegistryAction};
use crate::adapter::flow_demand::FlowDemandAdapter;
use crate::adapter::management::ManagementAdapter;
use crate::adapter::pod_assignment::{PodAssignmentAction, PodAssignmentAdapter};
use crate::adapter::schedule_request::{ScheduleRequestAdapter, ScheduleRequestDelta};
use crate::adapter::timer::{TimerAction, TimerAdapter, TimerConfig};
use crate::core::{ClientCommand, GlobalWorkerId, SchedulerDecision, WorkerNamespaceEventKind};
use distvirt_sm_router::trace::PanicTracer;

use crate::sm_new::{
    AdminCmd, DRouter, ENDPOINT, LeaseInfo, PodId, PodStatus, Router, SCHEDULE_REQUEST,
    ScheduleLeaseId, TIMER, WorkerId,
};
use crate::types::{NamespaceId, NamespaceSpec};

use super::types::{NamespaceCoreEvent, NamespaceEffects, SchedulerMessage};

#[cfg(test)]
mod tests;

// =============================================================================
// Grouped state
// =============================================================================

/// All pure adapters owned by the namespace.
struct Adapters {
    timer: TimerAdapter,
    pod_assignment: PodAssignmentAdapter,
    schedule_request: ScheduleRequestAdapter,
    management: ManagementAdapter,
    backend_need: BackendNeedAdapter,
    flow_demand: FlowDemandAdapter,
    endpoint: EndpointAdapter,
}

/// Bidirectional ID mappings between external (global/protocol) and router-internal IDs.
struct IdMaps {
    // Worker: global ↔ router
    global_to_router_worker: HashMap<GlobalWorkerId, WorkerId>,
    router_to_global_worker: HashMap<WorkerId, GlobalWorkerId>,
    // Pod: protocol ↔ router
    proto_to_router_pod: HashMap<distvirt_worker_protocol::PodId, PodId>,
    router_to_proto_pod: HashMap<PodId, distvirt_worker_protocol::PodId>,
    // Artifact: protocol ↔ router
    proto_to_router_artifact:
        HashMap<distvirt_worker_protocol::ArtifactId, crate::sm_new::ArtifactId>,
    router_to_proto_artifact:
        HashMap<crate::sm_new::ArtifactId, distvirt_worker_protocol::ArtifactId>,
    next_artifact_counter: u64,
}

impl IdMaps {
    fn new() -> Self {
        IdMaps {
            global_to_router_worker: HashMap::new(),
            router_to_global_worker: HashMap::new(),
            proto_to_router_pod: HashMap::new(),
            router_to_proto_pod: HashMap::new(),
            proto_to_router_artifact: HashMap::new(),
            router_to_proto_artifact: HashMap::new(),
            next_artifact_counter: 0,
        }
    }

    fn insert_worker(&mut self, global: GlobalWorkerId, router: WorkerId) {
        self.global_to_router_worker.insert(global, router);
        self.router_to_global_worker.insert(router, global);
    }

    fn remove_worker_by_global(&mut self, global: &GlobalWorkerId) -> Option<WorkerId> {
        if let Some(router) = self.global_to_router_worker.remove(global) {
            self.router_to_global_worker.remove(&router);
            Some(router)
        } else {
            None
        }
    }

    fn insert_pod(&mut self, proto: distvirt_worker_protocol::PodId, router: PodId) {
        self.proto_to_router_pod.insert(proto.clone(), router);
        self.router_to_proto_pod.insert(router, proto);
    }

    fn remove_pod_by_router(&mut self, router: &PodId) -> Option<distvirt_worker_protocol::PodId> {
        if let Some(proto) = self.router_to_proto_pod.remove(router) {
            self.proto_to_router_pod.remove(&proto);
            Some(proto)
        } else {
            None
        }
    }

    fn assign_proto_pod_id(&mut self, router_pod_id: PodId) -> distvirt_worker_protocol::PodId {
        if let Some(existing) = self.router_to_proto_pod.get(&router_pod_id) {
            return existing.clone();
        }
        let proto_id = distvirt_worker_protocol::PodId::from(format!("{:?}", router_pod_id));
        self.insert_pod(proto_id.clone(), router_pod_id);
        proto_id
    }

    fn get_or_create_router_artifact(
        &mut self,
        proto: &distvirt_worker_protocol::ArtifactId,
    ) -> crate::sm_new::ArtifactId {
        if let Some(&router_id) = self.proto_to_router_artifact.get(proto) {
            return router_id;
        }
        let router_id = crate::sm_new::ArtifactId(self.next_artifact_counter);
        self.next_artifact_counter += 1;
        self.proto_to_router_artifact
            .insert(proto.clone(), router_id);
        self.router_to_proto_artifact
            .insert(router_id, proto.clone());
        router_id
    }

    fn get_proto_artifact(
        &self,
        router: &crate::sm_new::ArtifactId,
    ) -> &distvirt_worker_protocol::ArtifactId {
        self.router_to_proto_artifact.get(router).expect(
            "router ArtifactId has no protocol mapping — artifact was never registered at the namespace boundary",
        )
    }

    /// Allocate a new artifact ID pair (proto + router) for a suspend operation.
    fn create_artifact_id(
        &mut self,
    ) -> (
        distvirt_worker_protocol::ArtifactId,
        crate::sm_new::ArtifactId,
    ) {
        let router_id = crate::sm_new::ArtifactId(self.next_artifact_counter);
        self.next_artifact_counter += 1;
        let proto_id =
            distvirt_worker_protocol::ArtifactId::from(format!("artifact-{}", router_id.0));
        self.proto_to_router_artifact
            .insert(proto_id.clone(), router_id);
        self.router_to_proto_artifact
            .insert(router_id, proto_id.clone());
        (proto_id, router_id)
    }
}

// =============================================================================
// Reconcile actions (sync output of reconcile phase)
// =============================================================================

struct ReconcileActions {
    timer_actions: Vec<TimerAction>,
    schedule_deltas: Vec<ScheduleRequestDelta>,
    pod_actions: Vec<PodAssignmentAction>,
    endpoint_actions: Vec<EndpointAction>,
}

// =============================================================================
// Pending worker (pure — no writer handle)
// =============================================================================

struct PendingWorkerCore {
    proto_worker_id: distvirt_worker_protocol::WorkerId,
    info: crate::sm_new::WorkerInfo,
}

// =============================================================================
// NamespaceCore
// =============================================================================

pub struct NamespaceCore {
    namespace_id: NamespaceId,
    router: DRouter,
    adapters: Adapters,
    ids: IdMaps,

    pending_workers: HashMap<GlobalWorkerId, PendingWorkerCore>,

    leases: HashMap<PodId, ScheduleLeaseId>,

    /// Tracks which pods are assigned to each worker (for WorkerToPod edge management).
    worker_pod_edges: HashMap<WorkerId, HashSet<PodId>>,
    /// Reverse lookup: pod → assigned worker.
    pod_worker: HashMap<PodId, WorkerId>,

    /// Active workers (pure set — writer handles live in the async shell).
    active_workers: HashSet<GlobalWorkerId>,

    /// Grants received before the target worker was registered in the namespace.
    /// Applied once NamespaceCreated confirms the worker.
    deferred_grants: Vec<(PodId, GlobalWorkerId)>,

    proto_worker_ids: HashMap<GlobalWorkerId, distvirt_worker_protocol::WorkerId>,

    current_spec: Option<NamespaceSpec>,

    workload_specs: HashMap<crate::sm_new::WorkloadId, crate::types::WorkloadSpec>,
}

impl NamespaceCore {
    pub fn new(namespace_id: NamespaceId, timer_config: TimerConfig) -> Self {
        let mut router = Router::new_traced(16, PanicTracer::new());
        router.create_timer(TIMER);
        router.create_schedule_request(SCHEDULE_REQUEST);
        router.create_endpoint(ENDPOINT);

        NamespaceCore {
            namespace_id,
            router,
            adapters: Adapters {
                timer: TimerAdapter::new(timer_config),
                pod_assignment: PodAssignmentAdapter::new(),
                schedule_request: ScheduleRequestAdapter::new(SCHEDULE_REQUEST),
                management: ManagementAdapter::new(),
                backend_need: BackendNeedAdapter::new(),
                flow_demand: FlowDemandAdapter::new(),
                endpoint: EndpointAdapter::new(ENDPOINT),
            },
            ids: IdMaps::new(),
            pending_workers: HashMap::new(),
            leases: HashMap::new(),
            worker_pod_edges: HashMap::new(),
            pod_worker: HashMap::new(),
            active_workers: HashSet::new(),
            deferred_grants: Vec::new(),
            proto_worker_ids: HashMap::new(),
            current_spec: None,
            workload_specs: HashMap::new(),
        }
    }

    /// Top-level event processing: push event, propagate, reconcile loop.
    /// Returns all effects to be executed by the async shell.
    pub fn process_event(&mut self, event: NamespaceCoreEvent) -> NamespaceEffects {
        let mut effects = NamespaceEffects::default();

        // Phase 1: Push external event into router
        self.push_event(event, &mut effects);

        // Phase 2: Propagate
        self.router.propagate();

        // Phase 3+4: Reconcile and collect effects in a loop until stable
        loop {
            let actions = self.reconcile();
            let has_actions = !actions.timer_actions.is_empty()
                || !actions.schedule_deltas.is_empty()
                || !actions.pod_actions.is_empty()
                || !actions.endpoint_actions.is_empty();
            self.collect_effects(actions, &mut effects);
            if !has_actions {
                break;
            }
            self.router.propagate();
        }

        effects
    }

    fn push_event(&mut self, event: NamespaceCoreEvent, effects: &mut NamespaceEffects) {
        match event {
            NamespaceCoreEvent::WorkerEvent(wne) => {
                match &wne.event {
                    WorkerNamespaceEventKind::NamespaceCreated => {
                        if let Some(pending) = self.pending_workers.remove(&wne.worker_id) {
                            let router_worker_id = self.router.create_worker();
                            self.ids.insert_worker(wne.worker_id, router_worker_id);
                            self.router.set_worker_info(router_worker_id, pending.info);
                            self.active_workers.insert(wne.worker_id);
                            self.proto_worker_ids
                                .insert(wne.worker_id, pending.proto_worker_id);

                            // Send initial service registry to the new worker.
                            let sync_action = self.adapters.endpoint.build_registry_sync();
                            if let Some(cmd) = self.build_registry_command(&sync_action) {
                                effects.worker_commands.push((wne.worker_id, cmd));
                            }

                            // Apply any scheduler grants that arrived before this
                            // worker was registered. See SchedulerDecision::Grant.
                            let deferred: Vec<PodId> = self
                                .deferred_grants
                                .iter()
                                .filter(|(_, w)| *w == wne.worker_id)
                                .map(|(p, _)| *p)
                                .collect();
                            self.deferred_grants.retain(|(_, w)| *w != wne.worker_id);
                            for pod_id in deferred {
                                self.apply_grant(router_worker_id, pod_id);
                            }
                        }
                        return;
                    }
                    WorkerNamespaceEventKind::NamespaceFailed { error } => {
                        if self.pending_workers.remove(&wne.worker_id).is_some() {
                            eprintln!(
                                "namespace {:?}: worker {:?} fabric creation failed: {}",
                                self.namespace_id, wne.worker_id, error
                            );
                        }
                        // Discard any deferred grants for this worker.
                        self.deferred_grants.retain(|(_, w)| *w != wne.worker_id);
                        return;
                    }
                    _ => {}
                }

                let router_worker_id = match self.ids.global_to_router_worker.get(&wne.worker_id) {
                    Some(&id) => id,
                    None => {
                        eprintln!(
                            "warning: unknown global worker {:?}, dropping event",
                            wne.worker_id
                        );
                        return;
                    }
                };
                match wne.event {
                    WorkerNamespaceEventKind::PodRunning {
                        pod_id: ref proto_id,
                    } => {
                        if let Some(&router_id) = self.ids.proto_to_router_pod.get(proto_id) {
                            self.router.send_notify_pod_status(
                                router_worker_id,
                                router_id,
                                PodStatus::Running,
                            );
                        }
                    }
                    WorkerNamespaceEventKind::PodExited {
                        pod_id: ref proto_id,
                        exit_code,
                    } => {
                        if let Some(&router_id) = self.ids.proto_to_router_pod.get(proto_id) {
                            let status = if exit_code == 0 {
                                PodStatus::Finished
                            } else {
                                PodStatus::Failed
                            };
                            self.router
                                .send_notify_pod_status(router_worker_id, router_id, status);
                        }
                    }
                    WorkerNamespaceEventKind::PodFailed {
                        pod_id: ref proto_id,
                    } => {
                        if let Some(&router_id) = self.ids.proto_to_router_pod.get(proto_id) {
                            self.router.send_notify_pod_status(
                                router_worker_id,
                                router_id,
                                PodStatus::Failed,
                            );
                        }
                    }
                    WorkerNamespaceEventKind::PodSuspended {
                        pod_id: ref proto_id,
                        ref artifact_id,
                    } => {
                        if let Some(&router_id) = self.ids.proto_to_router_pod.get(proto_id) {
                            let router_artifact =
                                self.ids.get_or_create_router_artifact(artifact_id);
                            self.router.send_notify_pod_suspended(
                                router_worker_id,
                                router_id,
                                router_artifact,
                            );
                        }
                    }
                    WorkerNamespaceEventKind::PodSuspendFailed {
                        pod_id: ref proto_id,
                    } => {
                        if let Some(&router_id) = self.ids.proto_to_router_pod.get(proto_id) {
                            self.router.send_notify_pod_status(
                                router_worker_id,
                                router_id,
                                PodStatus::Failed,
                            );
                        }
                    }
                    WorkerNamespaceEventKind::ServiceBackendNeed {
                        ref service_id,
                        ref need,
                    } => {
                        if let Some(router_svc_id) =
                            self.adapters.management.lookup_service(service_id.as_ref())
                        {
                            let sm_need = match need {
                                distvirt_worker_protocol::BackendNeed::None => {
                                    crate::sm_new::BackendNeed::None
                                }
                                distvirt_worker_protocol::BackendNeed::Traffic => {
                                    crate::sm_new::BackendNeed::Traffic
                                }
                                distvirt_worker_protocol::BackendNeed::Active => {
                                    crate::sm_new::BackendNeed::Active
                                }
                            };
                            self.adapters.backend_need.push_need(
                                &mut self.router,
                                router_worker_id,
                                router_svc_id,
                                sm_need,
                            );
                        }
                    }
                    WorkerNamespaceEventKind::EndpointActivation {
                        service_id: Some(ref proto_svc_id),
                        ..
                    } => {
                        if let Some(_router_svc_id) = self
                            .adapters
                            .management
                            .lookup_service(proto_svc_id.as_ref())
                        {
                            self.adapters.management.send_activate_service(
                                &mut self.router,
                                proto_svc_id.as_ref(),
                                true,
                            );
                        }
                    }
                    WorkerNamespaceEventKind::EndpointActivation {
                        service_id: None, ..
                    } => {}
                    WorkerNamespaceEventKind::EndpointFlowStatus {
                        service_id: Some(ref proto_svc_id),
                        has_active_flows,
                        ..
                    } => {
                        if let Some(router_svc_id) = self
                            .adapters
                            .management
                            .lookup_service(proto_svc_id.as_ref())
                        {
                            if has_active_flows {
                                self.adapters.flow_demand.set_active(
                                    &mut self.router,
                                    router_worker_id,
                                    router_svc_id,
                                );
                            } else {
                                self.adapters.flow_demand.set_inactive(
                                    &mut self.router,
                                    router_worker_id,
                                    router_svc_id,
                                );
                            }
                        }
                    }
                    WorkerNamespaceEventKind::EndpointFlowStatus {
                        service_id: None, ..
                    } => {}
                    WorkerNamespaceEventKind::NamespaceCreated
                    | WorkerNamespaceEventKind::NamespaceFailed { .. } => unreachable!(),
                }
            }
            NamespaceCoreEvent::SchedulerDecision(decision) => match decision {
                SchedulerDecision::Grant {
                    namespace_id: _,
                    pod_id,
                    worker_id,
                } => {
                    match self.ids.global_to_router_worker.get(&worker_id) {
                        Some(&router_worker_id) => {
                            self.apply_grant(router_worker_id, pod_id);
                        }
                        None => {
                            // Worker not registered yet (NamespaceCreated pending).
                            // Defer until the worker completes registration.
                            self.deferred_grants.push((pod_id, worker_id));
                        }
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
            NamespaceCoreEvent::TimerFired {
                identity,
                generation: _,
            } => {
                // Generation check is done by the async shell before forwarding.
                // Core always fires valid timers.
                self.adapters.timer.fire(&mut self.router, &identity);
            }
            NamespaceCoreEvent::WorkerConnected {
                worker_id,
                proto_worker_id,
                info,
            } => {
                self.pending_workers.insert(
                    worker_id,
                    PendingWorkerCore {
                        proto_worker_id,
                        info,
                    },
                );
            }
            NamespaceCoreEvent::WorkerDisconnected { worker_id } => {
                self.active_workers.remove(&worker_id);
                self.proto_worker_ids.remove(&worker_id);
                // Discard any deferred grants for this worker.
                self.deferred_grants.retain(|(_, w)| *w != worker_id);
                if let Some(router_worker_id) = self.ids.remove_worker_by_global(&worker_id) {
                    // Clean up WorkerToPod edge tracking for this worker.
                    if let Some(pods) = self.worker_pod_edges.remove(&router_worker_id) {
                        for pod_id in pods {
                            self.pod_worker.remove(&pod_id);
                        }
                    }

                    self.adapters
                        .pod_assignment
                        .remove_worker(&router_worker_id);
                    self.adapters
                        .backend_need
                        .remove_worker(&mut self.router, &router_worker_id);
                    self.adapters
                        .flow_demand
                        .remove_worker(&mut self.router, &router_worker_id);
                    self.router.destroy_worker(router_worker_id);
                }
                self.pending_workers.remove(&worker_id);
            }
            NamespaceCoreEvent::ClientCommand(cmd) => {
                self.handle_client_command(cmd, effects);
            }
        }
    }

    fn handle_client_command(&mut self, cmd: ClientCommand, effects: &mut NamespaceEffects) {
        match cmd {
            ClientCommand::UpdateSpec(new_spec) => {
                self.adapters.management.apply_namespace_spec(
                    &mut self.router,
                    self.current_spec.as_ref(),
                    &new_spec,
                );

                self.workload_specs.clear();
                for (name, wl_spec) in &new_spec.workloads {
                    if let Some(router_id) = self.adapters.management.lookup_workload(&name.0) {
                        self.workload_specs.insert(router_id, wl_spec.clone());
                    }
                }

                let registry_action = self.adapters.endpoint.update_registry(
                    new_spec
                        .services
                        .iter()
                        .map(|(name, spec)| (name.as_ref().to_owned(), spec.ip)),
                );
                if let Some(action) = registry_action {
                    if let Some(cmd) = self.build_registry_command(&action) {
                        effects.broadcast_commands.push(cmd);
                    }
                }

                self.current_spec = Some(new_spec);
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
        self.router
            .set_schedule_lease_to_pod_edges(lease_id, vec![pod_id]);
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
            .set_worker_to_pod_edges(worker_id, pods.iter().copied().collect::<Vec<_>>());
    }

    /// Remove a pod from its assigned worker's WorkerToPod edge set and update the router.
    fn remove_pod_from_worker(&mut self, pod_id: PodId) {
        if let Some(worker_id) = self.pod_worker.remove(&pod_id) {
            if let Some(pods) = self.worker_pod_edges.get_mut(&worker_id) {
                pods.remove(&pod_id);
                self.router
                    .set_worker_to_pod_edges(worker_id, pods.iter().copied().collect::<Vec<_>>());
            }
        }
    }

    /// Phase 3: Reconcile all adapters. Pure/sync — no I/O.
    fn reconcile(&mut self) -> ReconcileActions {
        ReconcileActions {
            timer_actions: self.adapters.timer.reconcile(&mut self.router),
            schedule_deltas: self.adapters.schedule_request.reconcile(&mut self.router),
            pod_actions: self.adapters.pod_assignment.reconcile(&mut self.router),
            endpoint_actions: self.adapters.endpoint.reconcile(&mut self.router),
        }
    }

    /// Phase 4: Translate reconcile actions into effects (replaces async execute).
    fn collect_effects(&mut self, actions: ReconcileActions, effects: &mut NamespaceEffects) {
        // Timer actions pass through directly.
        effects.timer_actions.extend(actions.timer_actions);

        // Schedule request deltas → scheduler messages.
        for delta in actions.schedule_deltas {
            match delta {
                ScheduleRequestDelta::Request { pod_id, request } => {
                    let proto_resume_artifact = request
                        .resume_artifact
                        .as_ref()
                        .map(|art_id| self.ids.get_proto_artifact(art_id).clone());
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
        let mut pods_to_clean: Vec<PodId> = Vec::new();

        for action in actions.pod_actions {
            match action {
                PodAssignmentAction::Launch {
                    worker_id,
                    pod_id,
                    request,
                } => {
                    if let Some(&global_id) = self.ids.router_to_global_worker.get(&worker_id) {
                        let proto_pod_id = self.ids.assign_proto_pod_id(pod_id);
                        let cmd = self.build_launch_command(pod_id, &proto_pod_id, &request);
                        effects.worker_commands.push((global_id, cmd));
                    }
                }
                PodAssignmentAction::Resume {
                    worker_id,
                    pod_id,
                    artifact_id,
                } => {
                    if let Some(&global_id) = self.ids.router_to_global_worker.get(&worker_id) {
                        let proto_pod_id = self.ids.assign_proto_pod_id(pod_id);
                        let proto_artifact_id = self.ids.get_proto_artifact(&artifact_id).clone();
                        let cmd =
                            self.build_resume_command(pod_id, &proto_pod_id, &proto_artifact_id);
                        effects.worker_commands.push((global_id, cmd));
                    }
                }
                PodAssignmentAction::Stop { worker_id, pod_id } => {
                    if let Some(&global_id) = self.ids.router_to_global_worker.get(&worker_id) {
                        if let Some(proto_pod_id) = self.ids.router_to_proto_pod.get(&pod_id) {
                            let cmd = distvirt_worker_protocol::WorkerCommand::StopPod {
                                namespace_id: self.namespace_id.clone(),
                                pod_id: proto_pod_id.clone(),
                                graceful: true,
                            };
                            effects.worker_commands.push((global_id, cmd));
                        }
                    }
                    self.remove_pod_from_worker(pod_id);
                    pods_to_clean.push(pod_id);
                }
                PodAssignmentAction::Suspend { worker_id, pod_id } => {
                    if let Some(&global_id) = self.ids.router_to_global_worker.get(&worker_id) {
                        let proto_pod_id = self.ids.router_to_proto_pod.get(&pod_id).cloned();
                        if let Some(proto_pod_id) = proto_pod_id {
                            let (proto_artifact_id, _router_artifact_id) =
                                self.ids.create_artifact_id();
                            let cmd = distvirt_worker_protocol::WorkerCommand::SuspendPod {
                                namespace_id: self.namespace_id.clone(),
                                pod_id: proto_pod_id,
                                artifact_id: proto_artifact_id,
                                pool_id: distvirt_worker_protocol::PoolId::from("default"),
                            };
                            effects.worker_commands.push((global_id, cmd));
                        }
                    }
                }
            }
        }

        for pod_id in pods_to_clean {
            self.ids.remove_pod_by_router(&pod_id);
        }

        // Endpoint actions → broadcast commands.
        for action in actions.endpoint_actions {
            if let Some(cmd) = self.build_endpoint_command(&action) {
                effects.broadcast_commands.push(cmd);
            }
        }
    }

    /// Build a LaunchPod protocol command from the cached workload spec.
    fn build_launch_command(
        &self,
        router_pod_id: PodId,
        proto_pod_id: &distvirt_worker_protocol::PodId,
        _request: &crate::sm_new::PodScheduleRequest,
    ) -> distvirt_worker_protocol::WorkerCommand {
        let workload_id = self
            .router
            .get_pod(&router_pod_id)
            .and_then(|pod| pod.workload_id);

        let spec = workload_id.and_then(|wid| self.workload_specs.get(&wid));

        let network = spec
            .map(|s| s.network.clone())
            .unwrap_or_else(default_pod_network);
        let containers = spec.map(|s| s.containers.clone()).unwrap_or_default();
        let resources = spec
            .and_then(|s| s.resources.as_ref())
            .map(convert_resources);

        distvirt_worker_protocol::WorkerCommand::LaunchPod {
            namespace_id: self.namespace_id.clone(),
            pod_id: proto_pod_id.clone(),
            network,
            containers,
            resources,
        }
    }

    fn lookup_service_spec(
        &self,
        service_id: &crate::sm_new::ServiceId,
    ) -> Option<(&str, &crate::types::ServiceSpec)> {
        let proto_name = self.adapters.management.service_proto_name(service_id)?;
        let spec = self
            .current_spec
            .as_ref()?
            .services
            .iter()
            .find(|(k, _)| k.as_ref() == proto_name)
            .map(|(_, spec)| spec)?;
        Some((proto_name, spec))
    }

    fn build_endpoint_command(
        &self,
        action: &EndpointAction,
    ) -> Option<distvirt_worker_protocol::WorkerCommand> {
        match action {
            EndpointAction::Update { service_id, ready } => {
                let (proto_name, svc_spec) = self.lookup_service_spec(service_id)?;

                let pod_ip = self
                    .current_spec
                    .as_ref()
                    .and_then(|ns| ns.workloads.get(&svc_spec.workload_id))
                    .map(|wl| wl.network.ip)
                    .unwrap_or(Ipv4Addr::UNSPECIFIED);

                let global_wid = self.ids.router_to_global_worker.get(&ready.worker_id)?;
                let proto_wid = self.proto_worker_ids.get(global_wid)?;

                let endpoint_spec = distvirt_worker_protocol::EndpointSpec {
                    ip: svc_spec.ip,
                    kind: distvirt_worker_protocol::EndpointKind::Service {
                        service_id: distvirt_worker_protocol::ServiceId::from(proto_name),
                        policy: svc_spec.policy.clone(),
                        backend: Some(distvirt_worker_protocol::EndpointPodBackend {
                            pod_ip,
                            placement: Some(distvirt_worker_protocol::EndpointPlacement {
                                worker_id: proto_wid.clone(),
                            }),
                            ready: true,
                        }),
                    },
                };

                Some(distvirt_worker_protocol::WorkerCommand::EndpointUpdate {
                    namespace_id: self.namespace_id.clone(),
                    upserted: vec![endpoint_spec],
                    removed_ips: vec![],
                })
            }
            EndpointAction::Remove { service_id } => {
                let (proto_name, svc_spec) = self.lookup_service_spec(service_id)?;

                let endpoint_spec = distvirt_worker_protocol::EndpointSpec {
                    ip: svc_spec.ip,
                    kind: distvirt_worker_protocol::EndpointKind::Service {
                        service_id: distvirt_worker_protocol::ServiceId::from(proto_name),
                        policy: svc_spec.policy.clone(),
                        backend: None,
                    },
                };

                Some(distvirt_worker_protocol::WorkerCommand::EndpointUpdate {
                    namespace_id: self.namespace_id.clone(),
                    upserted: vec![endpoint_spec],
                    removed_ips: vec![],
                })
            }
        }
    }

    fn build_registry_command(
        &self,
        action: &RegistryAction,
    ) -> Option<distvirt_worker_protocol::WorkerCommand> {
        match action {
            RegistryAction::Sync { entries } => {
                Some(distvirt_worker_protocol::WorkerCommand::RegistrySync {
                    namespace_id: self.namespace_id.clone(),
                    entries: entries
                        .iter()
                        .map(|e| distvirt_worker_protocol::RegistryEntry {
                            name: e.name.clone(),
                            ip: e.ip,
                        })
                        .collect(),
                })
            }
            RegistryAction::Update { added, removed } => {
                if added.is_empty() && removed.is_empty() {
                    return None;
                }
                Some(distvirt_worker_protocol::WorkerCommand::RegistryUpdate {
                    namespace_id: self.namespace_id.clone(),
                    added: added
                        .iter()
                        .map(|e| distvirt_worker_protocol::RegistryEntry {
                            name: e.name.clone(),
                            ip: e.ip,
                        })
                        .collect(),
                    removed: removed.clone(),
                })
            }
        }
    }

    fn build_resume_command(
        &self,
        router_pod_id: PodId,
        proto_pod_id: &distvirt_worker_protocol::PodId,
        proto_artifact_id: &distvirt_worker_protocol::ArtifactId,
    ) -> distvirt_worker_protocol::WorkerCommand {
        let workload_id = self
            .router
            .get_pod(&router_pod_id)
            .and_then(|pod| pod.workload_id);

        let network = workload_id
            .and_then(|wid| self.workload_specs.get(&wid))
            .map(|spec| spec.network.clone())
            .unwrap_or_else(default_pod_network);

        distvirt_worker_protocol::WorkerCommand::ResumePod {
            namespace_id: self.namespace_id.clone(),
            pod_id: proto_pod_id.clone(),
            artifact_id: proto_artifact_id.clone(),
            network,
            pool_id: distvirt_worker_protocol::PoolId::from("default"),
        }
    }

    /// Get the set of active worker IDs (for the async shell to know whom to broadcast to).
    pub fn active_workers(&self) -> &HashSet<GlobalWorkerId> {
        &self.active_workers
    }

    /// Access the router (for inspecting workload/service/pod state in tests).
    pub fn router(&self) -> &DRouter {
        &self.router
    }

    /// Access the management adapter (for looking up workloads/services by name).
    pub fn management(&self) -> &ManagementAdapter {
        &self.adapters.management
    }

    /// Access the current namespace spec (for reading service/workload specs in tests).
    pub fn current_spec(&self) -> Option<&NamespaceSpec> {
        self.current_spec.as_ref()
    }

    /// Map a router-internal WorkerId to a GlobalWorkerId (for test use).
    pub fn router_worker_to_global(
        &self,
        router_wid: &crate::sm_new::WorkerId,
    ) -> Option<GlobalWorkerId> {
        self.ids.router_to_global_worker.get(router_wid).copied()
    }

    /// Map a router-internal PodId to a protocol PodId (for test use).
    pub fn router_pod_to_proto(
        &self,
        router_pid: &crate::sm_new::PodId,
    ) -> Option<&distvirt_worker_protocol::PodId> {
        self.ids.router_to_proto_pod.get(router_pid)
    }
}

fn convert_resources(
    r: &crate::types::ResourceRequirements,
) -> distvirt_worker_protocol::ResourceRequirements {
    distvirt_worker_protocol::ResourceRequirements {
        requests: r
            .requests
            .as_ref()
            .map(|v| distvirt_worker_protocol::ResourceValues {
                memory_mib: v.memory_mb,
                vcpus: v.vcpus,
            }),
        limits: r
            .limits
            .as_ref()
            .map(|v| distvirt_worker_protocol::ResourceValues {
                memory_mib: v.memory_mb,
                vcpus: v.vcpus,
            }),
    }
}

fn default_pod_network() -> distvirt_worker_protocol::PodNetworkConfig {
    distvirt_worker_protocol::PodNetworkConfig {
        ip: std::net::Ipv4Addr::new(0, 0, 0, 0),
        mac: [0; 6],
        gateway: std::net::Ipv4Addr::new(0, 0, 0, 0),
        netmask: String::new(),
    }
}
