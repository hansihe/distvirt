//! Top-level orchestrator core — composes sub-cores and routes effects.
//!
//! `OrchestratorCore` owns all core state machines and handles the internal
//! effect routing (namespace ↔ scheduler ↔ worker state). It exposes
//! high-level lifecycle methods (worker connect/disconnect, namespace
//! create/destroy) and a low-level `process()` for individual events.
//!
//! No async, no channels — pure, deterministic logic.

use std::collections::{BTreeSet, HashMap};

use crate::adapter::timer::TimerConfig;
use super::namespace::NamespaceCore;
use super::scheduler::SchedulerCore;
use super::types::{
    CreateNamespaceInfo, DirectWorkerCommand, NamespaceCoreEvent, OrchestratorEffects,
    OrchestratorInput, SchedulerCoreInput, SchedulerMessage, WorkerConnectedInfo,
    WorkerStateCoreEvent,
};
use super::worker_state::WorkerStateCore;
use crate::sm_new::WorkerInfo;
use crate::task::GlobalWorkerId;
use crate::types::NamespaceId;

pub(crate) struct OrchestratorCore {
    namespaces: HashMap<NamespaceId, NamespaceCore>,
    scheduler: SchedulerCore,
    worker_state: WorkerStateCore,
    timer_config: TimerConfig,

    /// Connected workers tracked at orchestrator level (for lifecycle fan-out).
    connected_workers: HashMap<GlobalWorkerId, ConnectedWorkerInfo>,

    /// Segment ID allocator.
    next_segment_id: u16,
    active_segment_ids: BTreeSet<u16>,
    /// Namespace → segment_id mapping.
    namespace_segments: HashMap<NamespaceId, u16>,
    /// Namespace → network config (for sending CreateNamespace to new workers).
    namespace_networks: HashMap<NamespaceId, distvirt_worker_protocol::NetworkConfig>,
}

struct ConnectedWorkerInfo {
    proto_worker_id: distvirt_worker_protocol::WorkerId,
    max_pods: u32,
}

impl OrchestratorCore {
    pub fn new(timer_config: TimerConfig) -> Self {
        OrchestratorCore {
            namespaces: HashMap::new(),
            scheduler: SchedulerCore::new(),
            worker_state: WorkerStateCore::new(),
            timer_config,
            connected_workers: HashMap::new(),
            next_segment_id: 1, // segment 0 is reserved
            active_segment_ids: BTreeSet::new(),
            namespace_segments: HashMap::new(),
            namespace_networks: HashMap::new(),
        }
    }

    /// Process a single top-level input, routing effects between internal cores.
    pub fn process(&mut self, input: OrchestratorInput) -> OrchestratorEffects {
        let mut effects = OrchestratorEffects::default();

        match input {
            OrchestratorInput::NamespaceEvent {
                namespace_id,
                event,
            } => {
                if let Some(ns) = self.namespaces.get_mut(&namespace_id) {
                    let ns_effects = ns.process_event(event);
                    self.route_namespace_effects(&namespace_id, ns_effects, &mut effects);
                }
            }
            OrchestratorInput::WorkerStateEvent(event) => {
                let ws_effects = self.worker_state.process(event);
                self.route_worker_state_effects(ws_effects, &mut effects);
            }
            OrchestratorInput::SchedulerEvent(input) => {
                let decisions = self.scheduler.process(input);
                self.route_scheduler_decisions(decisions, &mut effects);
            }
            OrchestratorInput::CreateNamespace { namespace_id } => {
                if !self.namespaces.contains_key(&namespace_id) {
                    let ns = NamespaceCore::new(namespace_id.clone(), self.timer_config.clone());
                    self.namespaces.insert(namespace_id, ns);
                }
            }
            OrchestratorInput::DestroyNamespace { namespace_id } => {
                self.namespaces.remove(&namespace_id);
            }
        }

        effects
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
    ) -> OrchestratorEffects {
        let mut effects = OrchestratorEffects::default();

        let worker_id = info.worker_id;
        let proto_worker_id = info.proto_worker_id;
        let max_pods = info.capabilities.max_pods;

        self.connected_workers.insert(
            worker_id,
            ConnectedWorkerInfo {
                proto_worker_id: proto_worker_id.clone(),
                max_pods,
            },
        );

        let ws_effects = self.worker_state.process(WorkerStateCoreEvent::Connected {
            worker_id,
            capabilities: info.capabilities,
            tunnel_info: info.tunnel_info,
            proto_worker_id: proto_worker_id.clone(),
        });
        self.route_worker_state_effects(ws_effects, &mut effects);

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
                    info: WorkerInfo { capacity: max_pods },
                });
                self.route_namespace_effects(&ns_id, ns_effects, &mut effects);
            }

            let ws_effects =
                self.worker_state
                    .process(WorkerStateCoreEvent::NamespaceAssigned {
                        worker_id,
                        namespace_id: ns_id,
                    });
            self.route_worker_state_effects(ws_effects, &mut effects);
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
    ) -> OrchestratorEffects {
        let mut effects = OrchestratorEffects::default();

        self.connected_workers.remove(&worker_id);

        let ns_ids: Vec<_> = self.namespaces.keys().cloned().collect();
        for ns_id in ns_ids {
            if let Some(ns) = self.namespaces.get_mut(&ns_id) {
                let ns_effects =
                    ns.process_event(NamespaceCoreEvent::WorkerDisconnected { worker_id });
                self.route_namespace_effects(&ns_id, ns_effects, &mut effects);
            }

            let ws_effects =
                self.worker_state
                    .process(WorkerStateCoreEvent::NamespaceUnassigned {
                        worker_id,
                        namespace_id: ns_id,
                    });
            self.route_worker_state_effects(ws_effects, &mut effects);
        }

        let ws_effects = self
            .worker_state
            .process(WorkerStateCoreEvent::Disconnected { worker_id });
        self.route_worker_state_effects(ws_effects, &mut effects);

        effects
    }

    /// Create a namespace.
    ///
    /// Allocates segment, registers with worker state, creates namespace core,
    /// fans out to all connected workers.
    pub fn create_namespace(
        &mut self,
        info: CreateNamespaceInfo,
    ) -> OrchestratorEffects {
        if self.namespaces.contains_key(&info.namespace_id) {
            return OrchestratorEffects::default();
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
        self.route_worker_state_effects(ws_effects, &mut effects);

        let ns = NamespaceCore::new(info.namespace_id.clone(), self.timer_config.clone());
        self.namespaces.insert(info.namespace_id.clone(), ns);

        let workers: Vec<_> = self
            .connected_workers
            .iter()
            .map(|(&wid, winfo)| (wid, winfo.proto_worker_id.clone(), winfo.max_pods))
            .collect();
        for (worker_id, proto_wid, max_pods) in workers {
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
                    info: WorkerInfo { capacity: max_pods },
                });
                self.route_namespace_effects(&info.namespace_id, ns_effects, &mut effects);
            }

            let ws_effects =
                self.worker_state
                    .process(WorkerStateCoreEvent::NamespaceAssigned {
                        worker_id,
                        namespace_id: info.namespace_id.clone(),
                    });
            self.route_worker_state_effects(ws_effects, &mut effects);
        }

        effects
    }

    /// Destroy a namespace.
    ///
    /// Fans out NamespaceUnassigned, unregisters segment, removes namespace core.
    pub fn destroy_namespace(
        &mut self,
        namespace_id: &NamespaceId,
    ) -> OrchestratorEffects {
        let mut effects = OrchestratorEffects::default();

        if self.namespaces.remove(namespace_id).is_none() {
            return effects;
        }

        if let Some(segment_id) = self.namespace_segments.remove(namespace_id) {
            self.free_segment_id(segment_id);
        }
        self.namespace_networks.remove(namespace_id);

        let worker_ids: Vec<_> = self.connected_workers.keys().copied().collect();
        for worker_id in worker_ids {
            let ws_effects =
                self.worker_state
                    .process(WorkerStateCoreEvent::NamespaceUnassigned {
                        worker_id,
                        namespace_id: namespace_id.clone(),
                    });
            self.route_worker_state_effects(ws_effects, &mut effects);
        }

        let ws_effects =
            self.worker_state
                .process(WorkerStateCoreEvent::UnregisterNamespaceSegment {
                    namespace_id: namespace_id.clone(),
                });
        self.route_worker_state_effects(ws_effects, &mut effects);

        effects
    }

    // =========================================================================
    // Internal routing
    // =========================================================================

    fn route_namespace_effects(
        &mut self,
        namespace_id: &NamespaceId,
        ns_effects: super::types::NamespaceEffects,
        effects: &mut OrchestratorEffects,
    ) {
        if !ns_effects.timer_actions.is_empty() {
            effects
                .timer_actions
                .push((namespace_id.clone(), ns_effects.timer_actions));
        }

        effects.worker_commands.extend(ns_effects.worker_commands);

        for cmd in ns_effects.broadcast_commands {
            effects
                .broadcast_commands
                .push((namespace_id.clone(), cmd));
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
            };

            let decisions = self.scheduler.process(scheduler_input);
            self.route_scheduler_decisions(decisions, effects);
        }
    }

    fn route_worker_state_effects(
        &mut self,
        ws_effects: super::types::WorkerStateEffects,
        effects: &mut OrchestratorEffects,
    ) {
        for update in ws_effects.scheduler_updates {
            let decisions = self.scheduler.process(update);
            self.route_scheduler_decisions(decisions, effects);
        }

        if let Some(cmd) = ws_effects.worker_registry_broadcast {
            effects.global_broadcasts.push(cmd);
        }
    }

    fn route_scheduler_decisions(
        &mut self,
        decisions: Vec<crate::task::SchedulerDecision>,
        effects: &mut OrchestratorEffects,
    ) {
        for decision in decisions {
            let target_ns_id = match &decision {
                crate::task::SchedulerDecision::Grant { namespace_id, .. } => {
                    namespace_id.clone()
                }
                crate::task::SchedulerDecision::Revoke { namespace_id, .. } => {
                    namespace_id.clone()
                }
            };

            if let Some(ns) = self.namespaces.get_mut(&target_ns_id) {
                let ns_effects =
                    ns.process_event(NamespaceCoreEvent::SchedulerDecision(decision));
                if !ns_effects.timer_actions.is_empty() {
                    effects
                        .timer_actions
                        .push((target_ns_id.clone(), ns_effects.timer_actions));
                }
                effects.worker_commands.extend(ns_effects.worker_commands);
                for cmd in ns_effects.broadcast_commands {
                    effects
                        .broadcast_commands
                        .push((target_ns_id.clone(), cmd));
                }
                debug_assert!(
                    ns_effects.scheduler_messages.is_empty(),
                    "scheduler decisions should not produce new scheduler messages"
                );
            }
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

    pub fn namespace(&self, id: &NamespaceId) -> Option<&NamespaceCore> {
        self.namespaces.get(id)
    }

    pub fn namespace_ids(&self) -> impl Iterator<Item = &NamespaceId> {
        self.namespaces.keys()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::timer::TimerConfig;
    use std::time::Duration;

    fn test_caps() -> distvirt_worker_protocol::WorkerCapabilities {
        distvirt_worker_protocol::WorkerCapabilities {
            has_kvm: false,
            has_containerd: false,
            available_adapters: vec![],
            max_pods: 10,
            available_memory_mb: 1024,
            public_endpoint: String::new(),
            pools: vec![],
        }
    }

    fn test_timer_config() -> TimerConfig {
        TimerConfig {
            retry_backoff: Duration::from_millis(100),
            launch_timeout: Duration::from_millis(100),
            suspend_timeout: Duration::from_millis(100),
            idle_timeout: Duration::from_millis(100),
        }
    }

    fn test_network() -> distvirt_worker_protocol::NetworkConfig {
        distvirt_worker_protocol::NetworkConfig {
            segment_id: None,
            subnet: std::net::Ipv4Addr::new(10, 0, 0, 0),
            gateway: std::net::Ipv4Addr::new(10, 0, 0, 1),
            prefix_len: 24,
        }
    }

    fn ns(name: &str) -> NamespaceId {
        NamespaceId::from(name)
    }

    #[test]
    fn create_and_destroy_namespace() {
        let mut orch = OrchestratorCore::new(test_timer_config());

        let effects = orch.process(OrchestratorInput::CreateNamespace {
            namespace_id: ns("test"),
        });
        assert!(orch.namespace(&ns("test")).is_some());
        let _ = effects;

        let effects = orch.process(OrchestratorInput::DestroyNamespace {
            namespace_id: ns("test"),
        });
        assert!(orch.namespace(&ns("test")).is_none());
        let _ = effects;
    }

    #[test]
    fn worker_connected_lifecycle() {
        let mut orch = OrchestratorCore::new(test_timer_config());

        let effects = orch.worker_connected(WorkerConnectedInfo {
            worker_id: GlobalWorkerId::test(1),
            capabilities: test_caps(),
            tunnel_info: None,
            proto_worker_id: distvirt_worker_protocol::WorkerId::from("w-1"),
        });

        assert!(!effects.global_broadcasts.is_empty());
    }

    #[test]
    fn worker_connected_fans_out_to_namespaces() {
        let mut orch = OrchestratorCore::new(test_timer_config());

        orch.create_namespace(CreateNamespaceInfo {
            namespace_id: ns("test"),
            network: test_network(),
        });

        let effects = orch.worker_connected(WorkerConnectedInfo {
            worker_id: GlobalWorkerId::test(1),
            capabilities: test_caps(),
            tunnel_info: None,
            proto_worker_id: distvirt_worker_protocol::WorkerId::from("w-1"),
        });

        let has_create_ns = effects.direct_worker_commands.iter().any(|d| {
            matches!(
                &d.command,
                distvirt_worker_protocol::WorkerCommand::CreateNamespace { .. }
            )
        });
        assert!(
            has_create_ns,
            "worker should receive CreateNamespace for existing namespace"
        );
    }

    #[test]
    fn create_namespace_fans_out_to_workers() {
        let mut orch = OrchestratorCore::new(test_timer_config());

        orch.worker_connected(WorkerConnectedInfo {
            worker_id: GlobalWorkerId::test(1),
            capabilities: test_caps(),
            tunnel_info: None,
            proto_worker_id: distvirt_worker_protocol::WorkerId::from("w-1"),
        });

        let effects = orch.create_namespace(CreateNamespaceInfo {
            namespace_id: ns("test"),
            network: test_network(),
        });

        let has_create_ns = effects.direct_worker_commands.iter().any(|d| {
            d.worker_id == GlobalWorkerId::test(1)
                && matches!(
                    &d.command,
                    distvirt_worker_protocol::WorkerCommand::CreateNamespace { .. }
                )
        });
        assert!(has_create_ns, "existing worker should receive CreateNamespace");
    }

    #[test]
    fn full_sync_flow() {
        let mut orch = OrchestratorCore::new(test_timer_config());

        orch.create_namespace(CreateNamespaceInfo {
            namespace_id: ns("test"),
            network: test_network(),
        });

        orch.worker_connected(WorkerConnectedInfo {
            worker_id: GlobalWorkerId::test(1),
            capabilities: test_caps(),
            tunnel_info: None,
            proto_worker_id: distvirt_worker_protocol::WorkerId::from("w-1"),
        });

        let effects = orch.process(OrchestratorInput::NamespaceEvent {
            namespace_id: ns("test"),
            event: NamespaceCoreEvent::WorkerEvent(crate::task::WorkerNamespaceEvent {
                worker_id: GlobalWorkerId::test(1),
                event: crate::task::WorkerNamespaceEventKind::NamespaceCreated,
            }),
        });
        let _ = effects;

        let effects = orch.worker_disconnected(GlobalWorkerId::test(1));
        let _ = effects;

        let effects = orch.destroy_namespace(&ns("test"));
        let _ = effects;

        assert!(orch.namespace(&ns("test")).is_none());
    }
}
