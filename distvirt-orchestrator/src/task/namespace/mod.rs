use std::collections::HashMap;
use std::net::Ipv4Addr;

use tokio::sync::mpsc;

use crate::adapter::backend_need::BackendNeedAdapter;
use crate::adapter::flow_demand::FlowDemandAdapter;
use crate::adapter::endpoint::{EndpointAction, EndpointAdapter, RegistryAction};
use crate::adapter::management::ManagementAdapter;
use crate::adapter::pod_assignment::{PodAssignmentAction, PodAssignmentAdapter};
use crate::adapter::schedule_request::{ScheduleRequestAdapter, ScheduleRequestDelta};
use crate::adapter::timer::{TimerAction, TimerAdapter, TimerConfig, TimerIdentity};
use crate::sm_new::{
    AdminCmd, LeaseInfo, PodId, PodStatus, Router, ScheduleLeaseId, WorkerId, ENDPOINT,
    SCHEDULE_REQUEST, TIMER,
};
use crate::types::{NamespaceId, NamespaceSpec};

use super::{
    ClientCommand, GlobalWorkerId, NamespaceEvent, SchedulerDecision, SchedulerInput,
    WorkerNamespaceEventKind, WorkerWriterHandle,
};

#[cfg(test)]
mod tests;

// =============================================================================
// Grouped state
// =============================================================================

/// All pure adapters owned by the namespace task.
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
    proto_to_router_artifact: HashMap<distvirt_worker_protocol::ArtifactId, crate::sm_new::ArtifactId>,
    router_to_proto_artifact: HashMap<crate::sm_new::ArtifactId, distvirt_worker_protocol::ArtifactId>,
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

    /// Assign a protocol PodId for a router PodId. If one already exists, return it.
    fn assign_proto_pod_id(&mut self, router_pod_id: PodId) -> distvirt_worker_protocol::PodId {
        if let Some(existing) = self.router_to_proto_pod.get(&router_pod_id) {
            return existing.clone();
        }
        let proto_id = distvirt_worker_protocol::PodId::from(format!("{:?}", router_pod_id));
        self.insert_pod(proto_id.clone(), router_pod_id);
        proto_id
    }

    /// Get or create a router ArtifactId for a protocol ArtifactId.
    /// Used when receiving artifact events from workers (PodSuspended).
    fn get_or_create_router_artifact(
        &mut self,
        proto: &distvirt_worker_protocol::ArtifactId,
    ) -> crate::sm_new::ArtifactId {
        if let Some(&router_id) = self.proto_to_router_artifact.get(proto) {
            return router_id;
        }
        let router_id = crate::sm_new::ArtifactId(self.next_artifact_counter);
        self.next_artifact_counter += 1;
        self.proto_to_router_artifact.insert(proto.clone(), router_id);
        self.router_to_proto_artifact.insert(router_id, proto.clone());
        router_id
    }

    /// Look up the protocol ArtifactId for a router ArtifactId.
    /// Panics if the mapping doesn't exist — this indicates a bug, since every
    /// router ArtifactId must have been created through get_or_create_router_artifact.
    fn get_proto_artifact(
        &self,
        router: &crate::sm_new::ArtifactId,
    ) -> &distvirt_worker_protocol::ArtifactId {
        self.router_to_proto_artifact
            .get(router)
            .expect("router ArtifactId has no protocol mapping — artifact was never registered at the namespace boundary")
    }
}

// =============================================================================
// Reconcile actions (sync output of reconcile phase)
// =============================================================================

/// All actions collected from adapter reconciliation (Phase 3).
/// Produced synchronously, executed asynchronously.
struct ReconcileActions {
    timer_actions: Vec<TimerAction>,
    schedule_deltas: Vec<ScheduleRequestDelta>,
    pod_actions: Vec<PodAssignmentAction>,
    endpoint_actions: Vec<EndpointAction>,
}

// =============================================================================
// Namespace task
// =============================================================================

/// Worker waiting for NamespaceCreated confirmation before being added to the router.
struct PendingWorker {
    proto_worker_id: distvirt_worker_protocol::WorkerId,
    info: crate::sm_new::WorkerInfo,
    writer: WorkerWriterHandle,
}

struct NamespaceTask {
    namespace_id: NamespaceId,
    router: Router,
    adapters: Adapters,
    ids: IdMaps,

    // Workers pending fabric creation (waiting for NamespaceCreated)
    pending_workers: HashMap<GlobalWorkerId, PendingWorker>,

    // Lease tracking: pod_id → lease_id
    leases: HashMap<PodId, ScheduleLeaseId>,

    // Worker handles: global_worker_id → writer
    workers: HashMap<GlobalWorkerId, WorkerWriterHandle>,

    // Protocol worker IDs: global_worker_id → protocol WorkerId
    proto_worker_ids: HashMap<GlobalWorkerId, distvirt_worker_protocol::WorkerId>,

    // Cached namespace spec (for diffing on update)
    current_spec: Option<NamespaceSpec>,

    // Cached full workload specs: router WorkloadId → types::WorkloadSpec
    // Used to build protocol LaunchPod commands.
    workload_specs: HashMap<crate::sm_new::WorkloadId, crate::types::WorkloadSpec>,

    // Timer handles: identity → (generation, JoinHandle)
    timer_handles: HashMap<TimerIdentity, (u64, tokio::task::JoinHandle<()>)>,

    // Channels
    event_rx: mpsc::Receiver<NamespaceEvent>,
    scheduler_tx: mpsc::Sender<SchedulerInput>,
    scheduler_reply_rx: mpsc::Receiver<SchedulerDecision>,
    /// Self-sender for feeding timer fires back into the event loop.
    self_tx: mpsc::Sender<NamespaceEvent>,
}

impl NamespaceTask {
    async fn run(mut self) {
        loop {
            let event = tokio::select! {
                event = self.event_rx.recv() => {
                    match event {
                        Some(e) => e,
                        None => break,
                    }
                }
                decision = self.scheduler_reply_rx.recv() => {
                    match decision {
                        Some(d) => NamespaceEvent::SchedulerDecision(d),
                        None => continue,
                    }
                }
            };

            // Phase 1: Push external event into router
            self.push_event(event);

            // Phase 2: Propagate
            self.router.propagate();

            // Phase 3+4: Reconcile and execute in a loop until stable
            loop {
                let actions = self.reconcile();
                let has_actions = !actions.timer_actions.is_empty()
                    || !actions.schedule_deltas.is_empty()
                    || !actions.pod_actions.is_empty()
                    || !actions.endpoint_actions.is_empty();
                self.execute(actions).await;
                if !has_actions {
                    break;
                }
                self.router.propagate();
            }
        }

        // Clean up timer tasks
        for (_, (_, handle)) in self.timer_handles.drain() {
            handle.abort();
        }
    }

    fn push_event(&mut self, event: NamespaceEvent) {
        match event {
            NamespaceEvent::WorkerEvent(wne) => {
                // Handle events that don't require a router worker ID first.
                match &wne.event {
                    WorkerNamespaceEventKind::NamespaceCreated => {
                        // Namespace fabric is ready on this worker.
                        // Promote from pending to active in the router.
                        if let Some(pending) = self.pending_workers.remove(&wne.worker_id) {
                            let router_worker_id = self.router.create_worker();
                            self.ids.insert_worker(wne.worker_id, router_worker_id);
                            self.router.set_worker_info(router_worker_id, pending.info);
                            self.workers.insert(wne.worker_id, pending.writer.clone());
                            self.proto_worker_ids.insert(wne.worker_id, pending.proto_worker_id);

                            // Send initial service registry to the new worker.
                            let sync_action = self.adapters.endpoint.build_registry_sync();
                            if let Some(cmd) = self.build_registry_command(&sync_action) {
                                let w = pending.writer;
                                tokio::spawn(async move { w.send(cmd).await });
                            }
                        }
                        return;
                    }
                    WorkerNamespaceEventKind::NamespaceFailed { error } => {
                        // Namespace creation failed on this worker — remove from pending.
                        if self.pending_workers.remove(&wne.worker_id).is_some() {
                            eprintln!(
                                "namespace {:?}: worker {:?} fabric creation failed: {}",
                                self.namespace_id, wne.worker_id, error
                            );
                        }
                        return;
                    }
                    _ => {}
                }

                let router_worker_id = match self.ids.global_to_router_worker.get(&wne.worker_id) {
                    Some(&id) => id,
                    None => {
                        eprintln!("warning: unknown global worker {:?}, dropping event", wne.worker_id);
                        return;
                    }
                };
                match wne.event {
                    WorkerNamespaceEventKind::PodRunning { pod_id: ref proto_id } => {
                        if let Some(&router_id) = self.ids.proto_to_router_pod.get(proto_id) {
                            self.router.send_notify_pod_status(
                                router_worker_id,
                                router_id,
                                PodStatus::Running,
                            );
                        }
                    }
                    WorkerNamespaceEventKind::PodExited { pod_id: ref proto_id, exit_code } => {
                        if let Some(&router_id) = self.ids.proto_to_router_pod.get(proto_id) {
                            let status = if exit_code == 0 {
                                PodStatus::Finished
                            } else {
                                PodStatus::Failed
                            };
                            self.router.send_notify_pod_status(
                                router_worker_id,
                                router_id,
                                status,
                            );
                        }
                    }
                    WorkerNamespaceEventKind::PodFailed { pod_id: ref proto_id } => {
                        if let Some(&router_id) = self.ids.proto_to_router_pod.get(proto_id) {
                            self.router.send_notify_pod_status(
                                router_worker_id,
                                router_id,
                                PodStatus::Failed,
                            );
                        }
                    }
                    WorkerNamespaceEventKind::PodSuspended { pod_id: ref proto_id, ref artifact_id } => {
                        if let Some(&router_id) = self.ids.proto_to_router_pod.get(proto_id) {
                            let router_artifact = self.ids.get_or_create_router_artifact(artifact_id);
                            self.router.send_notify_pod_suspended(
                                router_worker_id,
                                router_id,
                                router_artifact,
                            );
                        }
                    }
                    WorkerNamespaceEventKind::PodSuspendFailed { pod_id: ref proto_id } => {
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
                        // Activate the service — same as client-initiated activation.
                        if let Some(_router_svc_id) =
                            self.adapters.management.lookup_service(proto_svc_id.as_ref())
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
                    } => {
                        // Direct IP activation without service — not yet supported.
                    }
                    WorkerNamespaceEventKind::EndpointFlowStatus {
                        service_id: Some(ref proto_svc_id),
                        has_active_flows,
                        ..
                    } => {
                        if let Some(router_svc_id) =
                            self.adapters.management.lookup_service(proto_svc_id.as_ref())
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
                    } => {
                        // Direct IP flow status without service — not yet supported.
                    }
                    // Already handled above with early return.
                    WorkerNamespaceEventKind::NamespaceCreated
                    | WorkerNamespaceEventKind::NamespaceFailed { .. } => unreachable!(),
                }
            }
            NamespaceEvent::SchedulerDecision(decision) => match decision {
                SchedulerDecision::Grant { namespace_id: _, pod_id, worker_id } => {
                    let router_worker_id = match self.ids.global_to_router_worker.get(&worker_id) {
                        Some(&id) => id,
                        None => {
                            eprintln!(
                                "warning: scheduler grant for unknown worker {:?}, ignoring",
                                worker_id
                            );
                            return;
                        }
                    };
                    let lease_id = self.router.create_schedule_lease();
                    self.router.set_schedule_lease_lease(
                        lease_id,
                        LeaseInfo { worker_id: router_worker_id },
                    );
                    self.router
                        .set_schedule_lease_to_pod_edges(lease_id, vec![pod_id]);
                    self.leases.insert(pod_id, lease_id);
                }
                SchedulerDecision::Revoke { namespace_id: _, pod_id } => {
                    if let Some(lease_id) = self.leases.remove(&pod_id) {
                        self.router.destroy_schedule_lease(lease_id);
                    }
                }
            },
            NamespaceEvent::TimerFired {
                identity,
                generation,
            } => {
                if let Some(&(active_gen, _)) = self.timer_handles.get(&identity) {
                    if generation == active_gen {
                        self.adapters.timer.fire(&mut self.router, &identity);
                        self.timer_handles.remove(&identity);
                    }
                }
            }
            NamespaceEvent::WorkerConnected {
                worker_id,
                proto_worker_id,
                info,
                writer,
            } => {
                // Stage as pending — the worker is added to the router only
                // after NamespaceCreated confirms fabric readiness.
                self.pending_workers.insert(worker_id, PendingWorker {
                    proto_worker_id,
                    info,
                    writer,
                });
            }
            NamespaceEvent::WorkerDisconnected { worker_id } => {
                self.workers.remove(&worker_id);
                self.proto_worker_ids.remove(&worker_id);
                if let Some(router_worker_id) = self.ids.remove_worker_by_global(&worker_id) {
                    self.adapters.pod_assignment.remove_worker(&router_worker_id);
                    self.adapters.backend_need.remove_worker(&mut self.router, &router_worker_id);
                    self.adapters.flow_demand.remove_worker(&mut self.router, &router_worker_id);
                    self.router.destroy_worker(router_worker_id);
                }
                // Also clean up any pending (not yet fabric-ready) worker.
                self.pending_workers.remove(&worker_id);
            }
            NamespaceEvent::ClientCommand(cmd) => {
                self.handle_client_command(cmd);
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

                // Cache full workload specs for building protocol commands later.
                self.workload_specs.clear();
                for (name, wl_spec) in &new_spec.workloads {
                    if let Some(router_id) = self.adapters.management.lookup_workload(&name.0) {
                        self.workload_specs.insert(router_id, wl_spec.clone());
                    }
                }

                // Update service registry and broadcast changes to workers.
                let registry_action = self.adapters.endpoint.update_registry(
                    new_spec.services.iter().map(|(name, spec)| {
                        (name.as_ref().to_owned(), spec.ip)
                    }),
                );
                if let Some(action) = registry_action {
                    if let Some(cmd) = self.build_registry_command(&action) {
                        // Broadcast to all workers. handle_client_command is sync,
                        // so spawn fire-and-forget tasks.
                        for writer in self.workers.values() {
                            let cmd = cmd.clone();
                            let w = writer.clone();
                            tokio::spawn(async move { w.send(cmd).await });
                        }
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
            ClientCommand::ActivateService { service_name, active } => {
                self.adapters.management.send_activate_service(
                    &mut self.router,
                    &service_name,
                    active,
                );
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

    /// Phase 4: Execute reconcile actions. Async — sends to channels, spawns timers.
    async fn execute(&mut self, actions: ReconcileActions) {
        // Timer actions
        for action in actions.timer_actions {
            match action {
                TimerAction::Start {
                    identity,
                    generation,
                    duration,
                } => {
                    if let Some((_, handle)) = self.timer_handles.remove(&identity) {
                        handle.abort();
                    }
                    let self_tx = self.self_tx.clone();
                    let ident = identity.clone();
                    let handle = tokio::spawn(async move {
                        tokio::time::sleep(duration).await;
                        let _ = self_tx
                            .send(NamespaceEvent::TimerFired {
                                identity: ident,
                                generation,
                            })
                            .await;
                    });
                    self.timer_handles.insert(identity, (generation, handle));
                }
                TimerAction::Cancel { identity } => {
                    if let Some((_, handle)) = self.timer_handles.remove(&identity) {
                        handle.abort();
                    }
                }
            }
        }

        // Schedule request deltas
        for delta in actions.schedule_deltas {
            match delta {
                ScheduleRequestDelta::Request { pod_id, request } => {
                    // Convert router artifact ID to protocol artifact ID at the boundary.
                    let proto_resume_artifact = request.resume_artifact.as_ref().map(|art_id| {
                        self.ids.get_proto_artifact(art_id).clone()
                    });
                    let _ = self
                        .scheduler_tx
                        .send(SchedulerInput::RequestLease {
                            namespace_id: self.namespace_id.clone(),
                            pod_id,
                            proto_resume_artifact,
                        })
                        .await;
                }
                ScheduleRequestDelta::Drop { pod_id } => {
                    let _ = self
                        .scheduler_tx
                        .send(SchedulerInput::DropRequest {
                            namespace_id: self.namespace_id.clone(),
                            pod_id,
                        })
                        .await;
                }
            }
        }

        // Pod assignment actions — collect commands first, then send.
        let mut commands: Vec<(GlobalWorkerId, distvirt_worker_protocol::WorkerCommand)> = Vec::new();
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
                        commands.push((global_id, cmd));
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
                        let cmd = self.build_resume_command(
                            pod_id, &proto_pod_id, &proto_artifact_id,
                        );
                        commands.push((global_id, cmd));
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
                            commands.push((global_id, cmd));
                        }
                    }
                    pods_to_clean.push(pod_id);
                }
            }
        }

        // Send all commands
        for (global_id, cmd) in commands {
            if let Some(writer) = self.workers.get(&global_id) {
                writer.send(cmd).await;
            }
        }

        // Clean up stopped pod mappings
        for pod_id in pods_to_clean {
            self.ids.remove_pod_by_router(&pod_id);
        }

        // Endpoint actions — broadcast service endpoint changes to all workers.
        for action in actions.endpoint_actions {
            if let Some(cmd) = self.build_endpoint_command(&action) {
                for writer in self.workers.values() {
                    writer.send(cmd.clone()).await;
                }
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
        let workload_id = self.router.get_pod(&router_pod_id)
            .and_then(|pod| pod.workload_id);

        let spec = workload_id.and_then(|wid| self.workload_specs.get(&wid));

        let network = spec.map(|s| s.network.clone())
            .unwrap_or_else(default_pod_network);
        let containers = spec.map(|s| s.containers.clone()).unwrap_or_default();
        let resources = spec.and_then(|s| s.resources.as_ref()).map(convert_resources);

        distvirt_worker_protocol::WorkerCommand::LaunchPod {
            namespace_id: self.namespace_id.clone(),
            pod_id: proto_pod_id.clone(),
            network,
            containers,
            resources,
        }
    }

    /// Look up a service's spec from the current namespace spec using a router ServiceId.
    fn lookup_service_spec(
        &self,
        service_id: &crate::sm_new::ServiceId,
    ) -> Option<(&str, &crate::types::ServiceSpec)> {
        let proto_name = self.adapters.management.service_proto_name(service_id)?;
        let spec = self.current_spec.as_ref()?
            .services
            .iter()
            .find(|(k, _)| k.as_ref() == proto_name)
            .map(|(_, spec)| spec)?;
        Some((proto_name, spec))
    }

    /// Build a WorkerCommand::EndpointUpdate from an EndpointAction.
    fn build_endpoint_command(
        &self,
        action: &EndpointAction,
    ) -> Option<distvirt_worker_protocol::WorkerCommand> {
        match action {
            EndpointAction::Update { service_id, ready } => {
                let (proto_name, svc_spec) = self.lookup_service_spec(service_id)?;

                // Get pod IP from workload spec.
                let pod_ip = self.current_spec.as_ref()
                    .and_then(|ns| ns.workloads.get(&svc_spec.workload_id))
                    .map(|wl| wl.network.ip)
                    .unwrap_or(Ipv4Addr::UNSPECIFIED);

                // Get protocol worker ID for placement.
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

                // Send an update with backend: None so the worker knows the service
                // exists but has no active backend (allows traffic buffering).
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

    /// Build a WorkerCommand from a RegistryAction.
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

    /// Build a ResumePod protocol command.
    fn build_resume_command(
        &self,
        router_pod_id: PodId,
        proto_pod_id: &distvirt_worker_protocol::PodId,
        proto_artifact_id: &distvirt_worker_protocol::ArtifactId,
    ) -> distvirt_worker_protocol::WorkerCommand {
        let workload_id = self.router.get_pod(&router_pod_id)
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
}

/// Convert orchestrator-side ResourceRequirements to protocol ResourceRequirements.
fn convert_resources(r: &crate::types::ResourceRequirements) -> distvirt_worker_protocol::ResourceRequirements {
    distvirt_worker_protocol::ResourceRequirements {
        requests: r.requests.as_ref().map(|v| distvirt_worker_protocol::ResourceValues {
            memory_mib: v.memory_mb,
            vcpus: v.vcpus,
        }),
        limits: r.limits.as_ref().map(|v| distvirt_worker_protocol::ResourceValues {
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

pub(crate) fn spawn(
    namespace_id: NamespaceId,
    scheduler_tx: mpsc::Sender<SchedulerInput>,
    timer_config: TimerConfig,
) -> (mpsc::Sender<NamespaceEvent>, tokio::task::JoinHandle<()>) {
    let (event_tx, event_rx) = mpsc::channel(256);
    let (scheduler_reply_tx, scheduler_reply_rx) = mpsc::channel(64);

    // Register this namespace with the scheduler so it can route decisions back.
    let _ = scheduler_tx.try_send(SchedulerInput::RegisterNamespace {
        namespace_id: namespace_id.clone(),
        reply_tx: scheduler_reply_tx,
    });

    let mut router = Router::new(16);
    router.create_timer(TIMER);
    router.create_schedule_request(SCHEDULE_REQUEST);
    router.create_endpoint(ENDPOINT);

    let task = NamespaceTask {
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
        workers: HashMap::new(),
        proto_worker_ids: HashMap::new(),
        current_spec: None,
        workload_specs: HashMap::new(),
        timer_handles: HashMap::new(),
        event_rx,
        scheduler_tx,
        scheduler_reply_rx,
        self_tx: event_tx.clone(),
    };

    let handle = tokio::spawn(task.run());
    (event_tx, handle)
}
