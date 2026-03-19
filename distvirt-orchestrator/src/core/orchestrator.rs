//! Top-level orchestrator core — composes sub-cores and routes effects.
//!
//! `OrchestratorCore` owns all core state machines and handles the internal
//! effect routing (namespace ↔ scheduler ↔ worker state). It exposes
//! high-level lifecycle methods (worker connect/disconnect, namespace
//! create/destroy) and a low-level `process()` for individual events.
//!
//! Timer actions from namespaces are absorbed internally by a `TimerWheel`.
//! Shells drive time via `advance_to()` and query `next_deadline()` — they
//! never see `TimerAction`s directly.
//!
//! No async, no channels — pure, deterministic logic.

use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use super::namespace_boundary::NamespaceWithBoundary;
use super::scheduler::{SchedulerCore, SchedulerEffects};
use super::timer_wheel::TimerWheel;
use super::types::{
    CreateNamespaceInfo, DirectWorkerCommand, NamespaceCoreEvent, OrchestratorEffects,
    OrchestratorInput, SchedulerCoreInput, SchedulerMessage, WorkerConnectedInfo,
    WorkerStateCoreEvent,
};
use super::worker_state::WorkerStateCore;
use crate::adapter::timer::TimerConfig;
use crate::core::{ClientCommand, ClientError, GlobalWorkerId, SchedulerDecision};
use crate::id_registry::IdRegistryMap;
use crate::sm::{ArtifactPortId, WorkerInfo};
use crate::types::{NamespaceId, NamespaceSpec};

pub struct OrchestratorCore {
    namespaces: HashMap<NamespaceId, NamespaceWithBoundary>,
    scheduler: SchedulerCore,
    worker_state: WorkerStateCore,
    timer_config: TimerConfig,
    timer_wheel: TimerWheel,

    /// Connected workers tracked at orchestrator level (for lifecycle fan-out).
    connected_workers: HashMap<GlobalWorkerId, ConnectedWorkerInfo>,

    /// Segment ID allocator.
    next_segment_id: u16,
    active_segment_ids: BTreeSet<u16>,
    /// Namespace → segment_id mapping.
    namespace_segments: HashMap<NamespaceId, u16>,
    /// Namespace → network config (for sending CreateNamespace to new workers).
    namespace_networks: HashMap<NamespaceId, distvirt_worker_protocol::NetworkConfig>,

    /// Shared per-namespace ID registries.
    id_registry_map: IdRegistryMap,
}

struct ConnectedWorkerInfo {
    proto_worker_id: distvirt_worker_protocol::WorkerId,
    max_pods: u32,
    default_pool: Option<distvirt_worker_protocol::PoolId>,
}

impl OrchestratorCore {
    pub fn new(timer_config: TimerConfig, id_registry_map: IdRegistryMap) -> Self {
        OrchestratorCore {
            namespaces: HashMap::new(),
            scheduler: SchedulerCore::new(),
            worker_state: WorkerStateCore::new(),
            timer_config,
            timer_wheel: TimerWheel::new(),
            connected_workers: HashMap::new(),
            next_segment_id: 1, // segment 0 is reserved
            active_segment_ids: BTreeSet::new(),
            namespace_segments: HashMap::new(),
            namespace_networks: HashMap::new(),
            id_registry_map,
        }
    }

    /// Process a single top-level input, routing effects between internal cores.
    ///
    /// `now` is the current logical time — used to compute absolute deadlines
    /// for any timers started as a result of processing this input.
    pub fn process(&mut self, input: OrchestratorInput, now: Duration) -> OrchestratorEffects {
        let mut effects = OrchestratorEffects::default();

        match input {
            OrchestratorInput::NamespaceEvent {
                namespace_id,
                event,
            } => {
                if let Some(ns) = self.namespaces.get_mut(&namespace_id) {
                    let ns_effects = ns.process_event(event);
                    self.route_namespace_effects(&namespace_id, ns_effects, &mut effects, now);
                }
            }
            OrchestratorInput::WorkerStateEvent(event) => {
                let ws_effects = self.worker_state.process(event);
                self.route_worker_state_effects(ws_effects, &mut effects, now);
            }
            OrchestratorInput::SchedulerEvent(input) => {
                let sched_effects = self.scheduler.process(input);
                self.route_scheduler_effects(sched_effects, &mut effects, now);
            }
            OrchestratorInput::CreateNamespace { namespace_id, network } => {
                if !self.namespaces.contains_key(&namespace_id) {
                    let registry = self.id_registry_map.get_or_create(&namespace_id);
                    let ns = NamespaceWithBoundary::new(namespace_id.clone(), self.timer_config.clone(), &network, registry);
                    self.namespaces.insert(namespace_id, ns);
                }
            }
            OrchestratorInput::DestroyNamespace { namespace_id } => {
                self.namespaces.remove(&namespace_id);
                self.timer_wheel.remove_namespace(&namespace_id);
            }
        }

        effects
    }

    // =========================================================================
    // Timer interface
    // =========================================================================

    /// Fire all timers whose deadline ≤ `now`, processing their effects
    /// internally. Loops until no more timers are expired (a timer fire
    /// can start new timers, but those will have a future deadline).
    ///
    /// Returns the accumulated non-timer effects.
    pub fn advance_to(&mut self, now: Duration) -> OrchestratorEffects {
        let mut effects = OrchestratorEffects::default();

        loop {
            let expired = self.timer_wheel.fire_expired(now);
            if expired.is_empty() {
                break;
            }

            for fired in expired {
                let input = OrchestratorInput::NamespaceEvent {
                    namespace_id: fired.namespace_id,
                    event: NamespaceCoreEvent::TimerFired {
                        identity: fired.identity,
                        generation: fired.generation,
                    },
                };
                let new_effects = self.process(input, now);
                effects.merge(new_effects);
            }
        }

        effects
    }

    /// Returns the earliest deadline across all pending timers, or `None`
    /// if no timers are active.
    pub fn next_deadline(&self) -> Option<Duration> {
        self.timer_wheel.next_deadline()
    }

    // =========================================================================
    // High-level lifecycle methods
    // =========================================================================

    /// Register a newly connected worker.
    ///
    /// Handles: worker state registration, plus for each namespace:
    /// CreateNamespace wire command + WorkerConnected + NamespaceAssigned.
    pub fn worker_connected(
        &mut self,
        info: WorkerConnectedInfo,
        now: Duration,
    ) -> OrchestratorEffects {
        let mut effects = OrchestratorEffects::default();

        let worker_id = info.worker_id;
        let proto_worker_id = info.proto_worker_id;
        let max_pods = info.capabilities.max_pods;
        let default_pool = info.capabilities.pools.first().map(|p| p.pool_id.clone());

        self.connected_workers.insert(
            worker_id,
            ConnectedWorkerInfo {
                proto_worker_id: proto_worker_id.clone(),
                max_pods,
                default_pool: default_pool.clone(),
            },
        );

        let ws_effects = self.worker_state.process(WorkerStateCoreEvent::Connected {
            worker_id,
            capabilities: info.capabilities,
            tunnel_info: info.tunnel_info,
            proto_worker_id: proto_worker_id.clone(),
        });
        self.route_worker_state_effects(ws_effects, &mut effects, now);

        let ns_ids: Vec<_> = self.namespaces.keys().cloned().collect();
        for ns_id in ns_ids {
            if let Some(network) = self.namespace_networks.get(&ns_id) {
                effects.direct_worker_commands.push(DirectWorkerCommand {
                    worker_id,
                    command: distvirt_worker_protocol::WorkerCommand::CreateNamespace {
                        namespace_id: ns_id.clone(),
                        network: network.clone(),
                    },
                });
            }

            if let Some(ns) = self.namespaces.get_mut(&ns_id) {
                let ns_effects = ns.process_event(NamespaceCoreEvent::WorkerConnected {
                    worker_id,
                    proto_worker_id: proto_worker_id.clone(),
                    info: WorkerInfo { capacity: max_pods, default_pool: default_pool.clone() },
                });
                self.route_namespace_effects(&ns_id, ns_effects, &mut effects, now);
            }

            let ws_effects = self
                .worker_state
                .process(WorkerStateCoreEvent::NamespaceAssigned {
                    worker_id,
                    namespace_id: ns_id,
                });
            self.route_worker_state_effects(ws_effects, &mut effects, now);
        }

        effects
    }

    /// Unregister a disconnected worker.
    ///
    /// Handles: for each namespace WorkerDisconnected + NamespaceUnassigned,
    /// then worker state Disconnected.
    pub fn worker_disconnected(
        &mut self,
        worker_id: GlobalWorkerId,
        now: Duration,
    ) -> OrchestratorEffects {
        let mut effects = OrchestratorEffects::default();

        self.connected_workers.remove(&worker_id);

        // Remove the worker from the scheduler FIRST, before processing namespace
        // events. Namespace processing may create new pods that emit schedule
        // requests; those must not be granted to the disconnecting worker.
        let ws_effects = self
            .worker_state
            .process(WorkerStateCoreEvent::Disconnected { worker_id });
        self.route_worker_state_effects(ws_effects, &mut effects, now);

        let ns_ids: Vec<_> = self.namespaces.keys().cloned().collect();
        for ns_id in ns_ids {
            if let Some(ns) = self.namespaces.get_mut(&ns_id) {
                let ns_effects =
                    ns.process_event(NamespaceCoreEvent::WorkerDisconnected { worker_id });
                self.route_namespace_effects(&ns_id, ns_effects, &mut effects, now);
            }

            let ws_effects = self
                .worker_state
                .process(WorkerStateCoreEvent::NamespaceUnassigned {
                    worker_id,
                    namespace_id: ns_id,
                });
            self.route_worker_state_effects(ws_effects, &mut effects, now);
        }

        effects
    }

    /// Create a namespace.
    ///
    /// Allocates segment, registers with worker state, creates namespace core,
    /// fans out to all connected workers.
    pub fn create_namespace(
        &mut self,
        info: CreateNamespaceInfo,
        now: Duration,
    ) -> (Result<(), ClientError>, OrchestratorEffects) {
        if self.namespaces.contains_key(&info.namespace_id) {
            return (Err(ClientError::NamespaceAlreadyExists), OrchestratorEffects::default());
        }

        let mut effects = OrchestratorEffects::default();

        let segment_id = self.alloc_segment_id();
        let mut network = info.network;
        network.segment_id = Some(segment_id);
        self.namespace_segments
            .insert(info.namespace_id.clone(), segment_id);
        self.namespace_networks
            .insert(info.namespace_id.clone(), network.clone());

        let ws_effects =
            self.worker_state
                .process(WorkerStateCoreEvent::RegisterNamespaceSegment {
                    namespace_id: info.namespace_id.clone(),
                    segment_id,
                });
        self.route_worker_state_effects(ws_effects, &mut effects, now);

        let registry = self.id_registry_map.get_or_create(&info.namespace_id);
        let ns = NamespaceWithBoundary::new(info.namespace_id.clone(), self.timer_config.clone(), &network, registry);
        self.namespaces.insert(info.namespace_id.clone(), ns);

        let workers: Vec<_> = self
            .connected_workers
            .iter()
            .map(|(&wid, winfo)| (wid, winfo.proto_worker_id.clone(), winfo.max_pods, winfo.default_pool.clone()))
            .collect();
        for (worker_id, proto_wid, max_pods, default_pool) in workers {
            effects.direct_worker_commands.push(DirectWorkerCommand {
                worker_id,
                command: distvirt_worker_protocol::WorkerCommand::CreateNamespace {
                    namespace_id: info.namespace_id.clone(),
                    network: network.clone(),
                },
            });

            if let Some(ns) = self.namespaces.get_mut(&info.namespace_id) {
                let ns_effects = ns.process_event(NamespaceCoreEvent::WorkerConnected {
                    worker_id,
                    proto_worker_id: proto_wid,
                    info: WorkerInfo { capacity: max_pods, default_pool },
                });
                self.route_namespace_effects(&info.namespace_id, ns_effects, &mut effects, now);
            }

            let ws_effects = self
                .worker_state
                .process(WorkerStateCoreEvent::NamespaceAssigned {
                    worker_id,
                    namespace_id: info.namespace_id.clone(),
                });
            self.route_worker_state_effects(ws_effects, &mut effects, now);
        }

        (Ok(()), effects)
    }

    /// Destroy a namespace.
    ///
    /// Fans out NamespaceUnassigned, unregisters segment, removes namespace core.
    pub fn destroy_namespace(&mut self, namespace_id: &NamespaceId) -> (Result<(), ClientError>, OrchestratorEffects) {
        let mut effects = OrchestratorEffects::default();

        if self.namespaces.remove(namespace_id).is_none() {
            return (Err(ClientError::NamespaceNotFound), effects);
        }

        self.timer_wheel.remove_namespace(namespace_id);

        if let Some(segment_id) = self.namespace_segments.remove(namespace_id) {
            self.free_segment_id(segment_id);
        }
        self.namespace_networks.remove(namespace_id);

        let worker_ids: Vec<_> = self.connected_workers.keys().copied().collect();
        for worker_id in worker_ids {
            let ws_effects = self
                .worker_state
                .process(WorkerStateCoreEvent::NamespaceUnassigned {
                    worker_id,
                    namespace_id: namespace_id.clone(),
                });
            // destroy_namespace doesn't need `now` since it can't produce new timers
            // (the namespace is already removed).
            self.route_worker_state_effects(ws_effects, &mut effects, Duration::ZERO);
        }

        let ws_effects =
            self.worker_state
                .process(WorkerStateCoreEvent::UnregisterNamespaceSegment {
                    namespace_id: namespace_id.clone(),
                });
        self.route_worker_state_effects(ws_effects, &mut effects, Duration::ZERO);

        (Ok(()), effects)
    }

    /// Update a namespace's spec.
    ///
    /// Routes `ClientCommand::UpdateSpec` to the target namespace.
    pub fn update_namespace(
        &mut self,
        namespace_id: &NamespaceId,
        spec: NamespaceSpec,
        now: Duration,
    ) -> (Result<(), ClientError>, OrchestratorEffects) {
        let Some(ns) = self.namespaces.get_mut(namespace_id) else {
            return (Err(ClientError::NamespaceNotFound), OrchestratorEffects::default());
        };
        let ns_effects = ns.process_event(NamespaceCoreEvent::ClientCommand(
            ClientCommand::UpdateSpec(spec),
        ));
        let mut effects = OrchestratorEffects::default();
        self.route_namespace_effects(namespace_id, ns_effects, &mut effects, now);
        (Ok(()), effects)
    }

    /// Partially update a namespace's spec (upsert/remove individual resources).
    ///
    /// Routes `ClientCommand::PatchSpec` to the target namespace.
    pub fn patch_namespace(
        &mut self,
        namespace_id: &NamespaceId,
        patch: crate::types::NamespacePatch,
        now: Duration,
    ) -> (Result<(), ClientError>, OrchestratorEffects) {
        let Some(ns) = self.namespaces.get_mut(namespace_id) else {
            return (Err(ClientError::NamespaceNotFound), OrchestratorEffects::default());
        };
        let ns_effects = ns.process_event(NamespaceCoreEvent::ClientCommand(
            ClientCommand::PatchSpec(patch),
        ));
        let mut effects = OrchestratorEffects::default();
        self.route_namespace_effects(namespace_id, ns_effects, &mut effects, now);
        (Ok(()), effects)
    }

    /// Connect a WireGuard peer to a namespace's network.
    ///
    /// Picks a worker with tunnel info, routes `ClientCommand::Connect` to the
    /// target namespace, and returns connection details.
    pub fn connect_network(
        &mut self,
        namespace_id: &NamespaceId,
        client_public_key: [u8; 32],
        now: Duration,
    ) -> (Result<super::ConnectResult, ClientError>, OrchestratorEffects) {
        // Look up namespace.
        if !self.namespaces.contains_key(namespace_id) {
            return (Err(ClientError::NamespaceNotFound), OrchestratorEffects::default());
        }

        // Find a worker with tunnel capabilities.
        let (worker_id, tunnel_info, public_endpoint) = match self.worker_state.find_tunnel_worker() {
            Some((wid, ti, ep)) => (wid, ti.clone(), ep.to_string()),
            None => return (Err(ClientError::NoTunnelWorker), OrchestratorEffects::default()),
        };

        // Get network config for building the result.
        let network = self.namespace_networks.get(namespace_id).cloned();

        // Route connect command to namespace.
        let ns = self.namespaces.get_mut(namespace_id).unwrap();
        let ns_effects = ns.process_event(NamespaceCoreEvent::ClientCommand(
            ClientCommand::Connect {
                client_public_key,
                worker_id,
            },
        ));

        let mut effects = OrchestratorEffects::default();
        self.route_namespace_effects(namespace_id, ns_effects, &mut effects, now);

        // Look up the allocated client IP from the namespace's WG peer manager.
        // The namespace core handles the IP allocation internally; we need to
        // read it back from the WG peer manager.
        let ns = self.namespaces.get(namespace_id).unwrap();
        let wg_peers = ns.wg_peers();
        let client_ip = match wg_peers.peers.get(&client_public_key) {
            Some(info) => info.client_ip,
            None => return (Err(ClientError::IpExhausted), effects),
        };

        let subnet_cidr = wg_peers.subnet_cidr();
        let endpoint = format!("{}:{}", public_endpoint, tunnel_info.listen_port);

        let result = super::ConnectResult {
            server_public_key: tunnel_info.public_key,
            endpoint,
            client_ip,
            subnet: subnet_cidr,
        };

        (Ok(result), effects)
    }

    /// Disconnect a WireGuard peer from a namespace's network.
    pub fn disconnect_network(
        &mut self,
        namespace_id: &NamespaceId,
        client_public_key: [u8; 32],
        now: Duration,
    ) -> (Result<(), ClientError>, OrchestratorEffects) {
        let Some(ns) = self.namespaces.get_mut(namespace_id) else {
            return (Err(ClientError::NamespaceNotFound), OrchestratorEffects::default());
        };

        let ns_effects = ns.process_event(NamespaceCoreEvent::ClientCommand(
            ClientCommand::Disconnect { client_public_key },
        ));

        let mut effects = OrchestratorEffects::default();
        self.route_namespace_effects(namespace_id, ns_effects, &mut effects, now);
        (Ok(()), effects)
    }

    // =========================================================================
    // Internal routing
    // =========================================================================

    fn route_namespace_effects(
        &mut self,
        namespace_id: &NamespaceId,
        ns_effects: super::types::NamespaceEffects,
        effects: &mut OrchestratorEffects,
        now: Duration,
    ) {
        if !ns_effects.timer_actions.is_empty() {
            self.timer_wheel
                .absorb(namespace_id, ns_effects.timer_actions, now);
        }

        effects.worker_commands.extend(ns_effects.worker_commands);

        for cmd in ns_effects.broadcast_commands {
            effects.broadcast_commands.push((namespace_id.clone(), cmd));
        }

        if !ns_effects.observability_events.is_empty() {
            effects
                .observability_events
                .push((namespace_id.clone(), ns_effects.observability_events));
        }

        for msg in ns_effects.scheduler_messages {
            let scheduler_input = match msg {
                SchedulerMessage::RequestLease {
                    namespace_id,
                    pod_id,
                    proto_resume_artifact,
                } => SchedulerCoreInput::RequestLease {
                    namespace_id,
                    pod_id,
                    proto_resume_artifact,
                },
                SchedulerMessage::DropRequest {
                    namespace_id,
                    pod_id,
                } => SchedulerCoreInput::DropRequest {
                    namespace_id,
                    pod_id,
                },
                SchedulerMessage::ArtifactReferenced {
                    namespace_id,
                    proto_artifact_id,
                } => SchedulerCoreInput::ArtifactReferenced {
                    proto_artifact_id,
                    namespace_id,
                },
                SchedulerMessage::ArtifactReleased {
                    namespace_id,
                    proto_artifact_id,
                } => SchedulerCoreInput::ArtifactReleased {
                    proto_artifact_id,
                    namespace_id,
                },
            };

            let sched_effects = self.scheduler.process(scheduler_input);
            self.route_scheduler_effects(sched_effects, effects, now);
        }
    }

    fn route_worker_state_effects(
        &mut self,
        ws_effects: super::types::WorkerStateEffects,
        effects: &mut OrchestratorEffects,
        now: Duration,
    ) {
        for update in ws_effects.scheduler_updates {
            let sched_effects = self.scheduler.process(update);
            self.route_scheduler_effects(sched_effects, effects, now);
        }

        if let Some(cmd) = ws_effects.worker_registry_broadcast {
            effects.global_broadcasts.push(cmd);
        }
    }

    fn route_scheduler_effects(
        &mut self,
        sched_effects: SchedulerEffects,
        effects: &mut OrchestratorEffects,
        now: Duration,
    ) {
        // Route scheduling decisions to namespaces.
        for decision in sched_effects.decisions {
            let target_ns_id = match &decision {
                SchedulerDecision::Grant {
                    namespace_id,
                    worker_id,
                    ..
                } => {
                    let ws_effects =
                        self.worker_state
                            .process(WorkerStateCoreEvent::PodCountChange {
                                worker_id: *worker_id,
                                delta: 1,
                            });
                    self.route_worker_state_effects(ws_effects, effects, now);
                    namespace_id.clone()
                }
                SchedulerDecision::Revoke {
                    namespace_id,
                    worker_id,
                    ..
                } => {
                    let ws_effects =
                        self.worker_state
                            .process(WorkerStateCoreEvent::PodCountChange {
                                worker_id: *worker_id,
                                delta: -1,
                            });
                    self.route_worker_state_effects(ws_effects, effects, now);
                    namespace_id.clone()
                }
            };

            if let Some(ns) = self.namespaces.get_mut(&target_ns_id) {
                let ns_effects = ns.process_event(NamespaceCoreEvent::SchedulerDecision(decision));
                if !ns_effects.timer_actions.is_empty() {
                    self.timer_wheel
                        .absorb(&target_ns_id, ns_effects.timer_actions, now);
                }
                effects.worker_commands.extend(ns_effects.worker_commands);
                for cmd in ns_effects.broadcast_commands {
                    effects.broadcast_commands.push((target_ns_id.clone(), cmd));
                }
                debug_assert!(
                    ns_effects.scheduler_messages.is_empty(),
                    "scheduler decisions should not produce new scheduler messages"
                );
            }
        }

        // Broadcast artifact invalidations to ALL namespaces.
        for artifact_port_id in sched_effects.artifact_invalidations {
            let ns_ids: Vec<_> = self.namespaces.keys().cloned().collect();
            for ns_id in ns_ids {
                if let Some(ns) = self.namespaces.get_mut(&ns_id) {
                    let ns_effects = ns.process_event(NamespaceCoreEvent::ArtifactInvalidated {
                        artifact_port_id: ArtifactPortId(artifact_port_id),
                    });
                    self.route_namespace_effects(&ns_id, ns_effects, effects, now);
                }
            }
        }

        // Route DeleteArtifact commands to workers.
        for cmd in sched_effects.delete_commands {
            effects.worker_commands.push((
                cmd.worker_id,
                distvirt_worker_protocol::WorkerCommand::DeleteArtifact {
                    artifact_id: cmd.artifact_id,
                    pool_id: cmd.pool_id,
                },
            ));
        }
    }

    // =========================================================================
    // Segment ID allocation
    // =========================================================================

    fn alloc_segment_id(&mut self) -> u16 {
        loop {
            let id = self.next_segment_id;
            self.next_segment_id = self.next_segment_id.wrapping_add(1);
            if id == 0 {
                continue;
            }
            if !self.active_segment_ids.contains(&id) {
                self.active_segment_ids.insert(id);
                return id;
            }
        }
    }

    fn free_segment_id(&mut self, id: u16) {
        self.active_segment_ids.remove(&id);
    }

    // =========================================================================
    // Accessors
    // =========================================================================

    pub fn list_workers(&self) -> Vec<super::worker_state::WorkerQueryInfo> {
        self.worker_state.query_all_workers()
    }

    pub fn get_worker(&self, worker_id: GlobalWorkerId) -> Result<super::worker_state::WorkerQueryInfo, ClientError> {
        self.worker_state.query_worker(worker_id).ok_or(ClientError::WorkerNotFound)
    }

    pub fn list_pods(&self, namespace_id: &NamespaceId) -> Result<Vec<crate::types::PodStatusReport>, ClientError> {
        let ns = self.namespaces.get(namespace_id).ok_or(ClientError::NamespaceNotFound)?;
        let report = ns.status_report();
        Ok(report.pods.into_values().collect())
    }

    pub fn namespace(&self, id: &NamespaceId) -> Option<&NamespaceWithBoundary> {
        self.namespaces.get(id)
    }

    pub fn namespace_ids(&self) -> impl Iterator<Item = &NamespaceId> {
        self.namespaces.keys()
    }

    /// Get the status report for a single namespace. Pure read — no effects.
    pub fn get_namespace_status(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<crate::types::NamespaceStatusReport, ClientError> {
        let ns = self.namespaces.get(namespace_id)
            .ok_or(ClientError::NamespaceNotFound)?;
        Ok(ns.status_report())
    }

    /// List all namespaces with their status reports. Pure read — no effects.
    pub fn list_namespaces(&self) -> Vec<crate::types::NamespaceStatusReport> {
        self.namespaces.values().map(|ns| ns.status_report()).collect()
    }
}

// TODO: update for EndpointSm refactor
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::{
//         adapter::timer::TimerConfig,
//         core::{WorkerNamespaceEvent, WorkerNamespaceEventKind},
//     };
//     use std::time::Duration;
//
//     fn test_caps() -> distvirt_worker_protocol::WorkerCapabilities {
//         distvirt_worker_protocol::WorkerCapabilities {
//             has_kvm: false,
//             has_containerd: false,
//             available_adapters: vec![],
//             max_pods: 10,
//             available_memory_mb: 1024,
//             public_endpoint: String::new(),
//             pools: vec![],
//         }
//     }
//
//     fn test_timer_config() -> TimerConfig {
//         TimerConfig {
//             retry_backoff: Duration::from_millis(100),
//             launch_timeout: Duration::from_millis(100),
//             suspend_timeout: Duration::from_millis(100),
//             idle_timeout: Duration::from_millis(100),
//         }
//     }
//
//     fn test_network() -> distvirt_worker_protocol::NetworkConfig {
//         distvirt_worker_protocol::NetworkConfig {
//             segment_id: None,
//             subnet: std::net::Ipv4Addr::new(10, 0, 0, 0),
//             gateway: std::net::Ipv4Addr::new(10, 0, 0, 1),
//             prefix_len: 24,
//         }
//     }
//
//     fn ns(name: &str) -> NamespaceId {
//         NamespaceId::from(name)
//     }
//
//     #[test]
//     fn create_and_destroy_namespace() {
//         let mut orch = OrchestratorCore::new(test_timer_config());
//
//         let effects = orch.process(
//             OrchestratorInput::CreateNamespace {
//                 namespace_id: ns("test"),
//                 network: test_network(),
//             },
//             Duration::ZERO,
//         );
//         assert!(orch.namespace(&ns("test")).is_some());
//         let _ = effects;
//
//         let effects = orch.process(
//             OrchestratorInput::DestroyNamespace {
//                 namespace_id: ns("test"),
//             },
//             Duration::ZERO,
//         );
//         assert!(orch.namespace(&ns("test")).is_none());
//         let _ = effects;
//     }
//
//     #[test]
//     fn worker_connected_lifecycle() {
//         let mut orch = OrchestratorCore::new(test_timer_config());
//
//         let effects = orch.worker_connected(
//             WorkerConnectedInfo {
//                 worker_id: GlobalWorkerId::from(1),
//                 capabilities: test_caps(),
//                 tunnel_info: None,
//                 proto_worker_id: distvirt_worker_protocol::WorkerId::from(1u64),
//             },
//             Duration::ZERO,
//         );
//
//         assert!(!effects.global_broadcasts.is_empty());
//     }
//
//     #[test]
//     fn worker_connected_fans_out_to_namespaces() {
//         let mut orch = OrchestratorCore::new(test_timer_config());
//
//         let _ = orch.create_namespace(
//             CreateNamespaceInfo {
//                 namespace_id: ns("test"),
//                 network: test_network(),
//             },
//             Duration::ZERO,
//         );
//
//         let effects = orch.worker_connected(
//             WorkerConnectedInfo {
//                 worker_id: GlobalWorkerId::from(1),
//                 capabilities: test_caps(),
//                 tunnel_info: None,
//                 proto_worker_id: distvirt_worker_protocol::WorkerId::from(1u64),
//             },
//             Duration::ZERO,
//         );
//
//         let has_create_ns = effects.direct_worker_commands.iter().any(|d| {
//             matches!(
//                 &d.command,
//                 distvirt_worker_protocol::WorkerCommand::CreateNamespace { .. }
//             )
//         });
//         assert!(
//             has_create_ns,
//             "worker should receive CreateNamespace for existing namespace"
//         );
//     }
//
//     #[test]
//     fn create_namespace_fans_out_to_workers() {
//         let mut orch = OrchestratorCore::new(test_timer_config());
//
//         orch.worker_connected(
//             WorkerConnectedInfo {
//                 worker_id: GlobalWorkerId::from(1),
//                 capabilities: test_caps(),
//                 tunnel_info: None,
//                 proto_worker_id: distvirt_worker_protocol::WorkerId::from(1u64),
//             },
//             Duration::ZERO,
//         );
//
//         let (_result, effects) = orch.create_namespace(
//             CreateNamespaceInfo {
//                 namespace_id: ns("test"),
//                 network: test_network(),
//             },
//             Duration::ZERO,
//         );
//
//         let has_create_ns = effects.direct_worker_commands.iter().any(|d| {
//             d.worker_id == GlobalWorkerId::from(1)
//                 && matches!(
//                     &d.command,
//                     distvirt_worker_protocol::WorkerCommand::CreateNamespace { .. }
//                 )
//         });
//         assert!(
//             has_create_ns,
//             "existing worker should receive CreateNamespace"
//         );
//     }
//
//     #[test]
//     fn full_sync_flow() {
//         let mut orch = OrchestratorCore::new(test_timer_config());
//
//         let _ = orch.create_namespace(
//             CreateNamespaceInfo {
//                 namespace_id: ns("test"),
//                 network: test_network(),
//             },
//             Duration::ZERO,
//         );
//
//         orch.worker_connected(
//             WorkerConnectedInfo {
//                 worker_id: GlobalWorkerId::from(1),
//                 capabilities: test_caps(),
//                 tunnel_info: None,
//                 proto_worker_id: distvirt_worker_protocol::WorkerId::from(1u64),
//             },
//             Duration::ZERO,
//         );
//
//         let effects = orch.process(
//             OrchestratorInput::NamespaceEvent {
//                 namespace_id: ns("test"),
//                 event: NamespaceCoreEvent::WorkerEvent(WorkerNamespaceEvent {
//                     worker_id: GlobalWorkerId::from(1),
//                     event: WorkerNamespaceEventKind::NamespaceCreated,
//                 }),
//             },
//             Duration::ZERO,
//         );
//         let _ = effects;
//
//         let effects = orch.worker_disconnected(GlobalWorkerId::from(1), Duration::ZERO);
//         let _ = effects;
//
//         let (_result, effects) = orch.destroy_namespace(&ns("test"));
//         let _ = effects;
//
//         assert!(orch.namespace(&ns("test")).is_none());
//     }
// }
