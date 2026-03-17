//! Boundary adapter: translates between protocol IDs and router-internal IDs.
//!
//! With unified worker IDs (GlobalWorkerId = sm::WorkerId), the boundary layer
//! is very thin:
//! - Pod, Artifact, and Worker IDs all pass through directly
//! - Pending worker lifecycle (before NamespaceCreated) still matters
//! - Protocol command building (LaunchPod, StopPod, etc.) still lives here

use std::collections::{HashMap, HashSet};

use crate::adapter::dns_registry::DnsRegistryAction;
use crate::adapter::endpoint::EndpointAction;
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
// ID conversion helpers (same u64 value, different newtypes)
// =============================================================================

fn proto_pod_id(router_id: PodId) -> distvirt_worker_protocol::PodId {
    distvirt_worker_protocol::PodId(router_id.0)
}

fn router_pod_id(proto_id: &distvirt_worker_protocol::PodId) -> PodId {
    PodId(proto_id.0)
}



// =============================================================================
// Pending worker (pure — no writer handle)
// =============================================================================

struct PendingWorkerCore {
    info: crate::sm::WorkerInfo,
}

// =============================================================================
// NamespaceWithBoundary
// =============================================================================

pub struct NamespaceWithBoundary {
    core: NamespaceCore,
    pending_workers: HashMap<GlobalWorkerId, PendingWorkerCore>,
    active_workers: HashSet<GlobalWorkerId>,
    deferred_grants: Vec<(PodId, GlobalWorkerId)>,
    /// Artifact ID allocator (for suspend operations that create new artifacts).
    next_artifact_counter: u64,
}

impl NamespaceWithBoundary {
    pub fn new(namespace_id: NamespaceId, timer_config: TimerConfig) -> Self {
        NamespaceWithBoundary {
            core: NamespaceCore::new(namespace_id, timer_config),
            pending_workers: HashMap::new(),
            active_workers: HashSet::new(),
            deferred_grants: Vec::new(),
            next_artifact_counter: 0,
        }
    }

    /// Construct from a pre-configured NamespaceCore (for test setup).
    pub(crate) fn from_core(core: NamespaceCore) -> Self {
        NamespaceWithBoundary {
            core,
            pending_workers: HashMap::new(),
            active_workers: HashSet::new(),
            deferred_grants: Vec::new(),
            next_artifact_counter: 0,
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
                proto_worker_id: _,
                info,
            } => {
                // Stage as pending — no core event yet.
                self.pending_workers.insert(
                    worker_id,
                    PendingWorkerCore {
                        info,
                    },
                );
            }
            NamespaceCoreEvent::WorkerDisconnected { worker_id } => {
                let was_active = self.active_workers.remove(&worker_id);
                // Discard any deferred grants for this worker.
                self.deferred_grants.retain(|(_, w)| *w != worker_id);

                if was_active {
                    let internal_effects = self.core.process_event(
                        InternalNamespaceEvent::WorkerDeactivated {
                            worker_id,
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
                            // Create router worker port with the same ID.
                            self.core.create_worker_port(wne.worker_id);
                            self.active_workers.insert(wne.worker_id);

                            // Send initial DNS registry to the new worker.
                            let sync_entries = self.core.adapters.dns_registry.build_sync();
                            if !sync_entries.is_empty() {
                                let cmd = distvirt_worker_protocol::WorkerCommand::RegistrySync {
                                    namespace_id: self.core.namespace_id().clone(),
                                    entries: sync_entries
                                        .into_iter()
                                        .map(|(name, ip)| distvirt_worker_protocol::RegistryEntry {
                                            name,
                                            ip,
                                        })
                                        .collect(),
                                };
                                effects.worker_commands.push((wne.worker_id, cmd));
                            }

                            // Send initial endpoint state to the new worker.
                            let endpoint_entries = self.core.adapters.endpoint.build_sync();
                            if !endpoint_entries.is_empty() {
                                let endpoints: Vec<_> = endpoint_entries
                                    .iter()
                                    .map(|(service_id, info)| {
                                        Self::build_endpoint_spec_from_info(service_id, info)
                                    })
                                    .collect();
                                if !endpoints.is_empty() {
                                    let cmd = distvirt_worker_protocol::WorkerCommand::EndpointSync {
                                        namespace_id: self.core.namespace_id().clone(),
                                        endpoints,
                                    };
                                    effects.worker_commands.push((wne.worker_id, cmd));
                                }
                            }

                            // Activate worker in core.
                            let internal_effects = self.core.process_event(
                                InternalNamespaceEvent::WorkerActivated {
                                    worker_id: wne.worker_id,
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
                                    wne.worker_id,
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

                // Worker must be active to process events.
                if !self.active_workers.contains(&wne.worker_id) {
                    eprintln!(
                        "warning: unknown global worker {:?}, dropping event",
                        wne.worker_id
                    );
                    return;
                }

                if let Some(internal_event) = self.translate_worker_event(wne.worker_id, wne.event) {
                    let internal_effects = self.core.process_event(
                        InternalNamespaceEvent::WorkerEvent {
                            worker_id: wne.worker_id,
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
                    if self.active_workers.contains(&worker_id) {
                        let internal_effects = self.core.process_event(
                            InternalNamespaceEvent::SchedulerGrant {
                                worker_id,
                                pod_id,
                            },
                        );
                        self.translate_effects(internal_effects, effects);
                    } else {
                        // Worker not registered yet (NamespaceCreated pending).
                        self.deferred_grants.push((pod_id, worker_id));
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
                // DNS registry is now handled by the router — no boundary-level
                // registry update needed. Service SMs signal their DNS entries
                // to the DnsRegistry port, and the adapter produces incremental
                // actions that flow through translate_effects.
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
        worker_id: WorkerId,
        event: WorkerNamespaceEventKind,
    ) -> Option<InternalWorkerEvent> {
        match event {
            WorkerNamespaceEventKind::PodRunning { pod_id } => {
                Some(InternalWorkerEvent::PodRunning { pod_id: router_pod_id(&pod_id) })
            }
            WorkerNamespaceEventKind::PodExited { pod_id, exit_code } => {
                Some(InternalWorkerEvent::PodExited {
                    pod_id: router_pod_id(&pod_id),
                    exit_code,
                })
            }
            WorkerNamespaceEventKind::PodFailed { pod_id } => {
                Some(InternalWorkerEvent::PodFailed { pod_id: router_pod_id(&pod_id) })
            }
            WorkerNamespaceEventKind::PodSuspended { pod_id, artifact_id } => {
                Some(InternalWorkerEvent::PodSuspended {
                    pod_id: router_pod_id(&pod_id),
                    artifact_id,
                })
            }
            WorkerNamespaceEventKind::PodSuspendFailed { pod_id } => {
                Some(InternalWorkerEvent::PodSuspendFailed { pod_id: router_pod_id(&pod_id) })
            }
            WorkerNamespaceEventKind::EndpointActivation {
                service_id: Some(ref proto_svc_id),
                ..
            } => {
                // Look up the service name from the router ID for the core.
                if let Some(svc_name) = self.core.management().service_proto_name(proto_svc_id) {
                    Some(InternalWorkerEvent::EndpointActivation {
                        service_name: svc_name.to_string(),
                    })
                } else {
                    None
                }
            }
            WorkerNamespaceEventKind::EndpointActivation {
                service_id: None, ..
            } => None,
            WorkerNamespaceEventKind::EndpointDemand {
                service_id: Some(ref proto_svc_id),
                active,
                ..
            } => {
                Some(InternalWorkerEvent::EndpointDemand {
                    service_id: *proto_svc_id,
                    active,
                })
            }
            WorkerNamespaceEventKind::EndpointDemand {
                service_id: None, ..
            } => None,
            WorkerNamespaceEventKind::NamespaceCreated
            | WorkerNamespaceEventKind::NamespaceFailed { .. } => unreachable!(),
        }
    }

    /// Process deferred grants for a newly activated worker.
    fn process_deferred_grants(
        &mut self,
        worker_id: WorkerId,
        pod_ids: Vec<PodId>,
    ) -> super::types::InternalNamespaceEffects {
        let mut combined = super::types::InternalNamespaceEffects::default();
        for pod_id in pod_ids {
            let effects = self.core.process_event(
                InternalNamespaceEvent::SchedulerGrant {
                    worker_id,
                    pod_id,
                },
            );
            combined.timer_actions.extend(effects.timer_actions);
            combined.scheduler_messages.extend(effects.scheduler_messages);
            combined.pod_actions.extend(effects.pod_actions);
            combined.endpoint_actions.extend(effects.endpoint_actions);
            combined.dns_registry_actions.extend(effects.dns_registry_actions);
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
                    let proto_resume_artifact = resume_artifact;
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
        // Worker IDs are now unified — no mapping needed.
        for action in internal.pod_actions {
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
                    let cmd =
                        self.build_resume_command(&proto_pod_id(pod_id), &artifact_id, &spec);
                    effects.worker_commands.push((worker_id, cmd));
                }
                PodAssignmentAction::Stop { worker_id, pod_id } => {
                    let cmd = distvirt_worker_protocol::WorkerCommand::StopPod {
                        namespace_id: self.core.namespace_id().clone(),
                        pod_id: proto_pod_id(pod_id),
                        graceful: true,
                    };
                    effects.worker_commands.push((worker_id, cmd));
                }
                PodAssignmentAction::Suspend { worker_id, pod_id } => {
                    let artifact_id = self.alloc_artifact_id();
                    let cmd = distvirt_worker_protocol::WorkerCommand::SuspendPod {
                        namespace_id: self.core.namespace_id().clone(),
                        pod_id: proto_pod_id(pod_id),
                        artifact_id,
                        pool_id: distvirt_worker_protocol::PoolId::from("default"),
                    };
                    effects.worker_commands.push((worker_id, cmd));
                }
            }
        }

        // Translate endpoint actions → broadcast commands.
        for action in internal.endpoint_actions {
            let cmd = self.build_endpoint_command(&action);
            effects.broadcast_commands.push(cmd);
        }

        // Translate DNS registry actions → broadcast commands.
        if !internal.dns_registry_actions.is_empty() {
            let mut added = Vec::new();
            let mut removed = Vec::new();
            for action in internal.dns_registry_actions {
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
                        namespace_id: self.core.namespace_id().clone(),
                        added,
                        removed,
                    },
                );
            }
        }
    }

    /// Allocate a new artifact ID for suspend operations.
    fn alloc_artifact_id(&mut self) -> ArtifactId {
        let id = ArtifactId(self.next_artifact_counter.to_string());
        self.next_artifact_counter += 1;
        id
    }

    // =========================================================================
    // Command building
    // =========================================================================

    fn build_launch_command(
        &self,
        proto_pod_id: &distvirt_worker_protocol::PodId,
        spec: &Option<crate::sm::WorkloadSpec>,
    ) -> distvirt_worker_protocol::WorkerCommand {
        let network = spec
            .as_ref()
            .and_then(|s| s.network.clone())
            .unwrap_or_else(default_pod_network);
        let containers = spec
            .as_ref()
            .map(|s| s.containers.clone())
            .unwrap_or_default();
        let resources = spec.as_ref().and_then(|s| s.resources.clone());

        distvirt_worker_protocol::WorkerCommand::LaunchPod {
            namespace_id: self.core.namespace_id().clone(),
            pod_id: *proto_pod_id,
            network,
            containers,
            resources,
        }
    }

    fn build_resume_command(
        &self,
        proto_pod_id: &distvirt_worker_protocol::PodId,
        proto_artifact_id: &distvirt_worker_protocol::ArtifactId,
        spec: &Option<crate::sm::WorkloadSpec>,
    ) -> distvirt_worker_protocol::WorkerCommand {
        let network = spec
            .as_ref()
            .and_then(|s| s.network.clone())
            .unwrap_or_else(default_pod_network);

        distvirt_worker_protocol::WorkerCommand::ResumePod {
            namespace_id: self.core.namespace_id().clone(),
            pod_id: *proto_pod_id,
            artifact_id: proto_artifact_id.clone(),
            network,
            pool_id: distvirt_worker_protocol::PoolId::from("default"),
        }
    }

    /// Build an EndpointSpec from self-contained ServiceEndpointInfo.
    fn build_endpoint_spec_from_info(
        service_id: &crate::sm::ServiceId,
        info: &crate::sm::ServiceEndpointInfo,
    ) -> distvirt_worker_protocol::EndpointSpec {
        distvirt_worker_protocol::EndpointSpec {
            ip: info.service_ip,
            kind: distvirt_worker_protocol::EndpointKind::Service {
                service_id: *service_id,
                policy: info.policy.clone(),
                backend: Some(distvirt_worker_protocol::EndpointPodBackend {
                    pod_ip: info.pod_ip,
                    placement: Some(distvirt_worker_protocol::EndpointPlacement {
                        worker_id: info.worker_id,
                    }),
                    ready: true,
                }),
            },
        }
    }

    fn build_endpoint_command(
        &self,
        action: &EndpointAction,
    ) -> distvirt_worker_protocol::WorkerCommand {
        match action {
            EndpointAction::Update { service_id, info } => {
                let endpoint_spec = Self::build_endpoint_spec_from_info(service_id, info);
                distvirt_worker_protocol::WorkerCommand::EndpointUpdate {
                    namespace_id: self.core.namespace_id().clone(),
                    upserted: vec![endpoint_spec],
                    removed_ips: vec![],
                }
            }
            EndpointAction::Remove {
                service_id: _,
                old_info,
            } => {
                distvirt_worker_protocol::WorkerCommand::EndpointUpdate {
                    namespace_id: self.core.namespace_id().clone(),
                    upserted: vec![],
                    removed_ips: vec![old_info.service_ip],
                }
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

    /// Map a router-internal PodId to a protocol PodId.
    pub fn router_pod_to_proto(
        &self,
        router_pid: &PodId,
    ) -> Option<distvirt_worker_protocol::PodId> {
        // With u64 IDs, this is a trivial conversion — the pod always has a protocol ID.
        Some(proto_pod_id(*router_pid))
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
