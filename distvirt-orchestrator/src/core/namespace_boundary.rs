//! Boundary adapter: translates between protocol string IDs and router-internal u64 IDs.
//!
//! `NamespaceWithBoundary` wraps `NamespaceCore` + `IdMaps` and presents
//! the same external API (proto IDs in, proto IDs out) while the core only
//! sees router IDs internally.

use std::collections::{HashMap, HashSet};
use std::net::Ipv4Addr;

use crate::adapter::endpoint::{EndpointAction, RegistryAction};
use crate::adapter::pod_assignment::PodAssignmentAction;
use crate::adapter::timer::TimerConfig;
use crate::core::{ClientCommand, GlobalWorkerId, SchedulerDecision, WorkerNamespaceEventKind};
use crate::sm::{ArtifactId, DRouter, PodId, WorkerId};
use crate::types::{NamespaceId, NamespaceSpec};

use super::namespace::NamespaceCore;
use super::types::{
    InternalNamespaceEvent, InternalSchedulerMessage, InternalWorkerEvent, NamespaceCoreEvent,
    NamespaceEffects, SchedulerMessage,
};

// =============================================================================
// Bidirectional ID mappings
// =============================================================================

struct IdMaps {
    // Worker: global ↔ router
    global_to_router_worker: HashMap<GlobalWorkerId, WorkerId>,
    router_to_global_worker: HashMap<WorkerId, GlobalWorkerId>,
    // Pod: protocol ↔ router
    proto_to_router_pod: HashMap<distvirt_worker_protocol::PodId, PodId>,
    router_to_proto_pod: HashMap<PodId, distvirt_worker_protocol::PodId>,
    // Artifact: protocol ↔ router
    proto_to_router_artifact: HashMap<distvirt_worker_protocol::ArtifactId, ArtifactId>,
    router_to_proto_artifact: HashMap<ArtifactId, distvirt_worker_protocol::ArtifactId>,
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
    ) -> ArtifactId {
        if let Some(&router_id) = self.proto_to_router_artifact.get(proto) {
            return router_id;
        }
        let router_id = ArtifactId(self.next_artifact_counter);
        self.next_artifact_counter += 1;
        self.proto_to_router_artifact
            .insert(proto.clone(), router_id);
        self.router_to_proto_artifact
            .insert(router_id, proto.clone());
        router_id
    }

    fn get_proto_artifact(
        &self,
        router: &ArtifactId,
    ) -> &distvirt_worker_protocol::ArtifactId {
        self.router_to_proto_artifact.get(router).expect(
            "router ArtifactId has no protocol mapping — artifact was never registered at the namespace boundary",
        )
    }

    /// Allocate a new artifact ID pair (proto + router) for a suspend operation.
    fn create_artifact_id(&mut self) -> (distvirt_worker_protocol::ArtifactId, ArtifactId) {
        let router_id = ArtifactId(self.next_artifact_counter);
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
// Pending worker (pure — no writer handle)
// =============================================================================

struct PendingWorkerCore {
    proto_worker_id: distvirt_worker_protocol::WorkerId,
    info: crate::sm::WorkerInfo,
}

// =============================================================================
// NamespaceWithBoundary
// =============================================================================

pub struct NamespaceWithBoundary {
    core: NamespaceCore,
    ids: IdMaps,
    pending_workers: HashMap<GlobalWorkerId, PendingWorkerCore>,
    active_workers: HashSet<GlobalWorkerId>,
    proto_worker_ids: HashMap<GlobalWorkerId, distvirt_worker_protocol::WorkerId>,
    deferred_grants: Vec<(PodId, GlobalWorkerId)>,
}

impl NamespaceWithBoundary {
    pub fn new(namespace_id: NamespaceId, timer_config: TimerConfig) -> Self {
        NamespaceWithBoundary {
            core: NamespaceCore::new(namespace_id, timer_config),
            ids: IdMaps::new(),
            pending_workers: HashMap::new(),
            active_workers: HashSet::new(),
            proto_worker_ids: HashMap::new(),
            deferred_grants: Vec::new(),
        }
    }

    /// Construct from a pre-configured NamespaceCore (for test setup).
    pub(crate) fn from_core(core: NamespaceCore) -> Self {
        NamespaceWithBoundary {
            core,
            ids: IdMaps::new(),
            pending_workers: HashMap::new(),
            active_workers: HashSet::new(),
            proto_worker_ids: HashMap::new(),
            deferred_grants: Vec::new(),
        }
    }

    /// Top-level event processing: translate proto IDs → router IDs,
    /// process through core, translate effects back.
    pub fn process_event(&mut self, event: NamespaceCoreEvent) -> NamespaceEffects {
        let mut effects = NamespaceEffects::default();

        // Translate and dispatch. Some events are handled entirely at the
        // boundary (e.g. WorkerConnected, NamespaceCreated).
        self.translate_and_process(event, &mut effects);

        effects
    }

    fn translate_and_process(&mut self, event: NamespaceCoreEvent, effects: &mut NamespaceEffects) {
        match event {
            NamespaceCoreEvent::WorkerConnected {
                worker_id,
                proto_worker_id,
                info,
            } => {
                // Stage as pending — no core event yet.
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
                    let internal_effects = self.core.process_event(
                        InternalNamespaceEvent::WorkerDeactivated {
                            worker_id: router_worker_id,
                        },
                    );
                    self.translate_effects(internal_effects, effects);
                }
                self.pending_workers.remove(&worker_id);
            }
            NamespaceCoreEvent::WorkerEvent(wne) => {
                match &wne.event {
                    WorkerNamespaceEventKind::NamespaceCreated => {
                        if let Some(pending) = self.pending_workers.remove(&wne.worker_id) {
                            // Create router worker port and register in IdMaps.
                            let router_worker_id = self.core.create_worker_port();
                            self.ids.insert_worker(wne.worker_id, router_worker_id);
                            self.active_workers.insert(wne.worker_id);
                            self.proto_worker_ids
                                .insert(wne.worker_id, pending.proto_worker_id);

                            // Send initial service registry to the new worker.
                            let sync_action = self.core.adapters.endpoint.build_registry_sync();
                            if let Some(cmd) = self.build_registry_command(&sync_action) {
                                effects.worker_commands.push((wne.worker_id, cmd));
                            }

                            // Activate worker in core.
                            let internal_effects = self.core.process_event(
                                InternalNamespaceEvent::WorkerActivated {
                                    worker_id: router_worker_id,
                                    info: pending.info,
                                },
                            );
                            self.translate_effects(internal_effects, effects);

                            // Apply any scheduler grants that arrived before this
                            // worker was registered.
                            let deferred: Vec<PodId> = self
                                .deferred_grants
                                .iter()
                                .filter(|(_, w)| *w == wne.worker_id)
                                .map(|(p, _)| *p)
                                .collect();
                            self.deferred_grants.retain(|(_, w)| *w != wne.worker_id);
                            if !deferred.is_empty() {
                                let internal_effects = self.process_deferred_grants(
                                    router_worker_id,
                                    deferred,
                                );
                                self.translate_effects(internal_effects, effects);
                            }
                        }
                        return;
                    }
                    WorkerNamespaceEventKind::NamespaceFailed { error } => {
                        if self.pending_workers.remove(&wne.worker_id).is_some() {
                            eprintln!(
                                "namespace {:?}: worker {:?} fabric creation failed: {}",
                                self.core.namespace_id(), wne.worker_id, error
                            );
                        }
                        self.deferred_grants.retain(|(_, w)| *w != wne.worker_id);
                        return;
                    }
                    _ => {}
                }

                // Translate worker event to internal form.
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

                if let Some(internal_event) = self.translate_worker_event(router_worker_id, wne.event) {
                    let internal_effects = self.core.process_event(
                        InternalNamespaceEvent::WorkerEvent {
                            worker_id: router_worker_id,
                            event: internal_event,
                        },
                    );
                    self.translate_effects(internal_effects, effects);
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
                            let internal_effects = self.core.process_event(
                                InternalNamespaceEvent::SchedulerGrant {
                                    worker_id: router_worker_id,
                                    pod_id,
                                },
                            );
                            self.translate_effects(internal_effects, effects);
                        }
                        None => {
                            // Worker not registered yet (NamespaceCreated pending).
                            self.deferred_grants.push((pod_id, worker_id));
                        }
                    }
                }
                SchedulerDecision::Revoke {
                    namespace_id: _,
                    pod_id,
                    ..
                } => {
                    let internal_effects = self.core.process_event(
                        InternalNamespaceEvent::SchedulerRevoke { pod_id },
                    );
                    self.translate_effects(internal_effects, effects);
                }
            },
            NamespaceCoreEvent::TimerFired {
                identity,
                generation: _,
            } => {
                let internal_effects = self.core.process_event(
                    InternalNamespaceEvent::TimerFired { identity },
                );
                self.translate_effects(internal_effects, effects);
            }
            NamespaceCoreEvent::ClientCommand(cmd) => {
                // Registry update from UpdateSpec needs boundary handling.
                if let ClientCommand::UpdateSpec(ref new_spec) = cmd {
                    let registry_action = self.core.adapters.endpoint.update_registry(
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
                }

                let internal_effects = self.core.process_event(
                    InternalNamespaceEvent::ClientCommand(cmd),
                );
                self.translate_effects(internal_effects, effects);
            }
        }
    }

    /// Translate a protocol-level worker event to an internal worker event.
    fn translate_worker_event(
        &mut self,
        router_worker_id: WorkerId,
        event: WorkerNamespaceEventKind,
    ) -> Option<InternalWorkerEvent> {
        match event {
            WorkerNamespaceEventKind::PodRunning { pod_id: ref proto_id } => {
                let &router_id = self.ids.proto_to_router_pod.get(proto_id)?;
                Some(InternalWorkerEvent::PodRunning { pod_id: router_id })
            }
            WorkerNamespaceEventKind::PodExited {
                pod_id: ref proto_id,
                exit_code,
            } => {
                let &router_id = self.ids.proto_to_router_pod.get(proto_id)?;
                Some(InternalWorkerEvent::PodExited {
                    pod_id: router_id,
                    exit_code,
                })
            }
            WorkerNamespaceEventKind::PodFailed { pod_id: ref proto_id } => {
                let &router_id = self.ids.proto_to_router_pod.get(proto_id)?;
                Some(InternalWorkerEvent::PodFailed { pod_id: router_id })
            }
            WorkerNamespaceEventKind::PodSuspended {
                pod_id: ref proto_id,
                ref artifact_id,
            } => {
                let &router_id = self.ids.proto_to_router_pod.get(proto_id)?;
                let router_artifact = self.ids.get_or_create_router_artifact(artifact_id);
                Some(InternalWorkerEvent::PodSuspended {
                    pod_id: router_id,
                    artifact_id: router_artifact,
                })
            }
            WorkerNamespaceEventKind::PodSuspendFailed { pod_id: ref proto_id } => {
                let &router_id = self.ids.proto_to_router_pod.get(proto_id)?;
                Some(InternalWorkerEvent::PodSuspendFailed { pod_id: router_id })
            }
            WorkerNamespaceEventKind::ServiceBackendNeed {
                ref service_id,
                ref need,
            } => {
                let router_svc_id =
                    self.core.management().lookup_service(service_id.as_ref())?;
                let sm_need = match need {
                    distvirt_worker_protocol::BackendNeed::None => crate::sm::BackendNeed::None,
                    distvirt_worker_protocol::BackendNeed::Traffic => {
                        crate::sm::BackendNeed::Traffic
                    }
                    distvirt_worker_protocol::BackendNeed::Active => {
                        crate::sm::BackendNeed::Active
                    }
                };
                Some(InternalWorkerEvent::ServiceBackendNeed {
                    service_id: router_svc_id,
                    need: sm_need,
                })
            }
            WorkerNamespaceEventKind::EndpointActivation {
                service_id: Some(ref proto_svc_id),
                ..
            } => {
                if self
                    .core
                    .management()
                    .lookup_service(proto_svc_id.as_ref())
                    .is_some()
                {
                    Some(InternalWorkerEvent::EndpointActivation {
                        service_name: proto_svc_id.as_ref().to_string(),
                    })
                } else {
                    None
                }
            }
            WorkerNamespaceEventKind::EndpointActivation {
                service_id: None, ..
            } => None,
            WorkerNamespaceEventKind::EndpointFlowStatus {
                service_id: Some(ref proto_svc_id),
                has_active_flows,
                ..
            } => {
                let router_svc_id =
                    self.core.management().lookup_service(proto_svc_id.as_ref())?;
                Some(InternalWorkerEvent::EndpointFlowStatus {
                    worker_id: router_worker_id,
                    service_id: router_svc_id,
                    has_active_flows,
                })
            }
            WorkerNamespaceEventKind::EndpointFlowStatus {
                service_id: None, ..
            } => None,
            WorkerNamespaceEventKind::NamespaceCreated
            | WorkerNamespaceEventKind::NamespaceFailed { .. } => unreachable!(),
        }
    }

    /// Process deferred grants for a newly activated worker.
    fn process_deferred_grants(
        &mut self,
        router_worker_id: WorkerId,
        pod_ids: Vec<PodId>,
    ) -> super::types::InternalNamespaceEffects {
        let mut combined = super::types::InternalNamespaceEffects::default();
        for pod_id in pod_ids {
            let effects = self.core.process_event(
                InternalNamespaceEvent::SchedulerGrant {
                    worker_id: router_worker_id,
                    pod_id,
                },
            );
            combined.timer_actions.extend(effects.timer_actions);
            combined.scheduler_messages.extend(effects.scheduler_messages);
            combined.pod_actions.extend(effects.pod_actions);
            combined.endpoint_actions.extend(effects.endpoint_actions);
        }
        combined
    }

    // =========================================================================
    // Outbound translation: internal effects → external effects
    // =========================================================================

    fn translate_effects(
        &mut self,
        internal: super::types::InternalNamespaceEffects,
        effects: &mut NamespaceEffects,
    ) {
        // Timer actions pass through.
        effects.timer_actions.extend(internal.timer_actions);

        // Translate scheduler messages.
        for msg in internal.scheduler_messages {
            match msg {
                InternalSchedulerMessage::RequestLease {
                    namespace_id,
                    pod_id,
                    resume_artifact,
                } => {
                    let proto_resume_artifact = resume_artifact
                        .as_ref()
                        .map(|art_id| self.ids.get_proto_artifact(art_id).clone());
                    effects
                        .scheduler_messages
                        .push(SchedulerMessage::RequestLease {
                            namespace_id,
                            pod_id,
                            proto_resume_artifact,
                        });
                }
                InternalSchedulerMessage::DropRequest {
                    namespace_id,
                    pod_id,
                } => {
                    effects
                        .scheduler_messages
                        .push(SchedulerMessage::DropRequest {
                            namespace_id,
                            pod_id,
                        });
                }
            }
        }

        // Translate pod assignment actions → worker commands.
        let mut pods_to_clean: Vec<PodId> = Vec::new();

        for action in internal.pod_actions {
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
                                namespace_id: self.core.namespace_id().clone(),
                                pod_id: proto_pod_id.clone(),
                                graceful: true,
                            };
                            effects.worker_commands.push((global_id, cmd));
                        }
                    }
                    pods_to_clean.push(pod_id);
                }
                PodAssignmentAction::Suspend { worker_id, pod_id } => {
                    if let Some(&global_id) = self.ids.router_to_global_worker.get(&worker_id) {
                        let proto_pod_id = self.ids.router_to_proto_pod.get(&pod_id).cloned();
                        if let Some(proto_pod_id) = proto_pod_id {
                            let (proto_artifact_id, _router_artifact_id) =
                                self.ids.create_artifact_id();
                            let cmd = distvirt_worker_protocol::WorkerCommand::SuspendPod {
                                namespace_id: self.core.namespace_id().clone(),
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

        // Translate endpoint actions → broadcast commands.
        for action in internal.endpoint_actions {
            if let Some(cmd) = self.build_endpoint_command(&action) {
                effects.broadcast_commands.push(cmd);
            }
        }
    }

    // =========================================================================
    // Command building (moved from NamespaceCore)
    // =========================================================================

    fn build_launch_command(
        &self,
        router_pod_id: PodId,
        proto_pod_id: &distvirt_worker_protocol::PodId,
        _request: &crate::sm::PodScheduleRequest,
    ) -> distvirt_worker_protocol::WorkerCommand {
        let workload_id = self
            .core
            .router()
            .get_pod(&router_pod_id)
            .and_then(|pod| pod.workload_id);

        let spec = workload_id.and_then(|wid| self.core.workload_specs().get(&wid));

        let network = spec
            .map(|s| s.network.clone())
            .unwrap_or_else(default_pod_network);
        let containers = spec.map(|s| s.containers.clone()).unwrap_or_default();
        let resources = spec
            .and_then(|s| s.resources.as_ref())
            .map(convert_resources);

        distvirt_worker_protocol::WorkerCommand::LaunchPod {
            namespace_id: self.core.namespace_id().clone(),
            pod_id: proto_pod_id.clone(),
            network,
            containers,
            resources,
        }
    }

    fn build_resume_command(
        &self,
        router_pod_id: PodId,
        proto_pod_id: &distvirt_worker_protocol::PodId,
        proto_artifact_id: &distvirt_worker_protocol::ArtifactId,
    ) -> distvirt_worker_protocol::WorkerCommand {
        let workload_id = self
            .core
            .router()
            .get_pod(&router_pod_id)
            .and_then(|pod| pod.workload_id);

        let network = workload_id
            .and_then(|wid| self.core.workload_specs().get(&wid))
            .map(|spec| spec.network.clone())
            .unwrap_or_else(default_pod_network);

        distvirt_worker_protocol::WorkerCommand::ResumePod {
            namespace_id: self.core.namespace_id().clone(),
            pod_id: proto_pod_id.clone(),
            artifact_id: proto_artifact_id.clone(),
            network,
            pool_id: distvirt_worker_protocol::PoolId::from("default"),
        }
    }

    fn lookup_service_spec(
        &self,
        service_id: &crate::sm::ServiceId,
    ) -> Option<(&str, &crate::types::ServiceSpec)> {
        let proto_name = self.core.management().service_proto_name(service_id)?;
        let spec = self
            .core
            .current_spec()
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
                    .core
                    .current_spec()
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
                    namespace_id: self.core.namespace_id().clone(),
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
                    namespace_id: self.core.namespace_id().clone(),
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
                    namespace_id: self.core.namespace_id().clone(),
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
                    namespace_id: self.core.namespace_id().clone(),
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

    // =========================================================================
    // Accessors (delegate to core)
    // =========================================================================

    /// Get the set of active worker IDs (for the async shell to know whom to broadcast to).
    pub fn active_workers(&self) -> &HashSet<GlobalWorkerId> {
        &self.active_workers
    }

    /// Access the router (for inspecting workload/service/pod state in tests).
    pub fn router(&self) -> &DRouter {
        self.core.router()
    }

    /// Access the management adapter (for looking up workloads/services by name).
    pub fn management(&self) -> &crate::adapter::management::ManagementAdapter {
        self.core.management()
    }

    /// Access the current namespace spec.
    pub fn current_spec(&self) -> Option<&NamespaceSpec> {
        self.core.current_spec()
    }

    /// Map a router-internal WorkerId to a GlobalWorkerId (for test use).
    pub fn router_worker_to_global(
        &self,
        router_wid: &WorkerId,
    ) -> Option<GlobalWorkerId> {
        self.ids.router_to_global_worker.get(router_wid).copied()
    }

    /// Map a router-internal PodId to a protocol PodId (for test use).
    pub fn router_pod_to_proto(
        &self,
        router_pid: &PodId,
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
