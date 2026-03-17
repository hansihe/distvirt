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

use super::namespace::NamespaceCore;
use super::scheduler::SchedulerCore;
use super::timer_wheel::TimerWheel;
use super::types::{
    CreateNamespaceInfo, DirectWorkerCommand, NamespaceCoreEvent, OrchestratorEffects,
    OrchestratorInput, SchedulerCoreInput, SchedulerMessage, WorkerConnectedInfo,
    WorkerStateCoreEvent,
};
use super::worker_state::WorkerStateCore;
use crate::adapter::timer::TimerConfig;
use crate::core::{GlobalWorkerId, SchedulerDecision};
use crate::sm::WorkerInfo;
use crate::types::NamespaceId;

pub struct OrchestratorCore {
    namespaces: HashMap<NamespaceId, NamespaceCore>,
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
            timer_wheel: TimerWheel::new(),
            connected_workers: HashMap::new(),
            next_segment_id: 1, // segment 0 is reserved
            active_segment_ids: BTreeSet::new(),
            namespace_segments: HashMap::new(),
            namespace_networks: HashMap::new(),
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
                let decisions = self.scheduler.process(input);
                self.route_scheduler_decisions(decisions, &mut effects, now);
            }
            OrchestratorInput::CreateNamespace { namespace_id } => {
                if !self.namespaces.contains_key(&namespace_id) {
                    let ns = NamespaceCore::new(namespace_id.clone(), self.timer_config.clone());
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
                    info: WorkerInfo { capacity: max_pods },
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
        self.route_worker_state_effects(ws_effects, &mut effects, now);

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

        effects
    }

    /// Destroy a namespace.
    ///
    /// Fans out NamespaceUnassigned, unregisters segment, removes namespace core.
    pub fn destroy_namespace(&mut self, namespace_id: &NamespaceId) -> OrchestratorEffects {
        let mut effects = OrchestratorEffects::default();

        if self.namespaces.remove(namespace_id).is_none() {
            return effects;
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
            self.route_scheduler_decisions(decisions, effects, now);
        }
    }

    fn route_worker_state_effects(
        &mut self,
        ws_effects: super::types::WorkerStateEffects,
        effects: &mut OrchestratorEffects,
        now: Duration,
    ) {
        for update in ws_effects.scheduler_updates {
            let decisions = self.scheduler.process(update);
            self.route_scheduler_decisions(decisions, effects, now);
        }

        if let Some(cmd) = ws_effects.worker_registry_broadcast {
            effects.global_broadcasts.push(cmd);
        }
    }

    fn route_scheduler_decisions(
        &mut self,
        decisions: Vec<SchedulerDecision>,
        effects: &mut OrchestratorEffects,
        now: Duration,
    ) {
        for decision in decisions {
            let target_ns_id = match &decision {
                SchedulerDecision::Grant {
                    namespace_id,
                    worker_id,
                    ..
                } => {
                    // Track pod count so the scheduler can tiebreak by load.
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
    use crate::{
        adapter::timer::TimerConfig,
        core::{WorkerNamespaceEvent, WorkerNamespaceEventKind},
    };
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

        let effects = orch.process(
            OrchestratorInput::CreateNamespace {
                namespace_id: ns("test"),
            },
            Duration::ZERO,
        );
        assert!(orch.namespace(&ns("test")).is_some());
        let _ = effects;

        let effects = orch.process(
            OrchestratorInput::DestroyNamespace {
                namespace_id: ns("test"),
            },
            Duration::ZERO,
        );
        assert!(orch.namespace(&ns("test")).is_none());
        let _ = effects;
    }

    #[test]
    fn worker_connected_lifecycle() {
        let mut orch = OrchestratorCore::new(test_timer_config());

        let effects = orch.worker_connected(
            WorkerConnectedInfo {
                worker_id: GlobalWorkerId::test(1),
                capabilities: test_caps(),
                tunnel_info: None,
                proto_worker_id: distvirt_worker_protocol::WorkerId::from("w-1"),
            },
            Duration::ZERO,
        );

        assert!(!effects.global_broadcasts.is_empty());
    }

    #[test]
    fn worker_connected_fans_out_to_namespaces() {
        let mut orch = OrchestratorCore::new(test_timer_config());

        orch.create_namespace(
            CreateNamespaceInfo {
                namespace_id: ns("test"),
                network: test_network(),
            },
            Duration::ZERO,
        );

        let effects = orch.worker_connected(
            WorkerConnectedInfo {
                worker_id: GlobalWorkerId::test(1),
                capabilities: test_caps(),
                tunnel_info: None,
                proto_worker_id: distvirt_worker_protocol::WorkerId::from("w-1"),
            },
            Duration::ZERO,
        );

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

        orch.worker_connected(
            WorkerConnectedInfo {
                worker_id: GlobalWorkerId::test(1),
                capabilities: test_caps(),
                tunnel_info: None,
                proto_worker_id: distvirt_worker_protocol::WorkerId::from("w-1"),
            },
            Duration::ZERO,
        );

        let effects = orch.create_namespace(
            CreateNamespaceInfo {
                namespace_id: ns("test"),
                network: test_network(),
            },
            Duration::ZERO,
        );

        let has_create_ns = effects.direct_worker_commands.iter().any(|d| {
            d.worker_id == GlobalWorkerId::test(1)
                && matches!(
                    &d.command,
                    distvirt_worker_protocol::WorkerCommand::CreateNamespace { .. }
                )
        });
        assert!(
            has_create_ns,
            "existing worker should receive CreateNamespace"
        );
    }

    #[test]
    fn full_sync_flow() {
        let mut orch = OrchestratorCore::new(test_timer_config());

        orch.create_namespace(
            CreateNamespaceInfo {
                namespace_id: ns("test"),
                network: test_network(),
            },
            Duration::ZERO,
        );

        orch.worker_connected(
            WorkerConnectedInfo {
                worker_id: GlobalWorkerId::test(1),
                capabilities: test_caps(),
                tunnel_info: None,
                proto_worker_id: distvirt_worker_protocol::WorkerId::from("w-1"),
            },
            Duration::ZERO,
        );

        let effects = orch.process(
            OrchestratorInput::NamespaceEvent {
                namespace_id: ns("test"),
                event: NamespaceCoreEvent::WorkerEvent(WorkerNamespaceEvent {
                    worker_id: GlobalWorkerId::test(1),
                    event: WorkerNamespaceEventKind::NamespaceCreated,
                }),
            },
            Duration::ZERO,
        );
        let _ = effects;

        let effects = orch.worker_disconnected(GlobalWorkerId::test(1), Duration::ZERO);
        let _ = effects;

        let effects = orch.destroy_namespace(&ns("test"));
        let _ = effects;

        assert!(orch.namespace(&ns("test")).is_none());
    }
}
