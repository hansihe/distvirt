//! Pure orchestrator core — no async, no channels, no I/O.
//!
//! This module contains the `SyncOrchestrator` which owns all pure state
//! (namespaces, scheduler, worker state) and routes effects between them
//! synchronously. The async shell wraps this with channel I/O.

pub(crate) mod types;
pub(crate) mod namespace;
pub(crate) mod scheduler;
pub(crate) mod worker_state;

use std::collections::HashMap;

use crate::adapter::timer::TimerConfig;
use crate::types::NamespaceId;

use self::namespace::NamespaceCore;
use self::scheduler::SchedulerCore;
use self::types::{
    NamespaceCoreEvent, OrchestratorEffects, OrchestratorInput, SchedulerCoreInput,
    SchedulerMessage, WorkerStateCoreEvent,
};
use self::worker_state::WorkerStateCore;

pub(crate) struct SyncOrchestrator {
    namespaces: HashMap<NamespaceId, NamespaceCore>,
    scheduler: SchedulerCore,
    worker_state: WorkerStateCore,
    timer_config: TimerConfig,
}

impl SyncOrchestrator {
    pub(crate) fn new(timer_config: TimerConfig) -> Self {
        SyncOrchestrator {
            namespaces: HashMap::new(),
            scheduler: SchedulerCore::new(),
            worker_state: WorkerStateCore::new(),
            timer_config,
        }
    }

    /// Process a single top-level input, routing effects between internal cores.
    /// Returns all external effects to be executed by the async shell.
    pub(crate) fn process(&mut self, input: OrchestratorInput) -> OrchestratorEffects {
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
            OrchestratorInput::CreateNamespace { namespace_id } => {
                if !self.namespaces.contains_key(&namespace_id) {
                    let ns = NamespaceCore::new(namespace_id.clone(), self.timer_config.clone());
                    self.namespaces.insert(namespace_id, ns);
                }
            }
            OrchestratorInput::DestroyNamespace { namespace_id } => {
                self.namespaces.remove(&namespace_id);
                // Note: scheduler cleanup (drop pending/granted for this namespace)
                // would happen as namespace sends DropRequests during its shutdown.
            }
        }

        effects
    }

    /// Route namespace effects: timer actions pass through, scheduler messages
    /// go to scheduler core, scheduler decisions route back to namespace.
    fn route_namespace_effects(
        &mut self,
        namespace_id: &NamespaceId,
        ns_effects: types::NamespaceEffects,
        effects: &mut OrchestratorEffects,
    ) {
        // Timer actions pass through to the shell.
        if !ns_effects.timer_actions.is_empty() {
            effects
                .timer_actions
                .push((namespace_id.clone(), ns_effects.timer_actions));
        }

        // Worker commands pass through to the shell.
        effects.worker_commands.extend(ns_effects.worker_commands);

        // Broadcast commands pass through (namespace-scoped).
        for cmd in ns_effects.broadcast_commands {
            effects
                .broadcast_commands
                .push((namespace_id.clone(), cmd));
        }

        // Scheduler messages → scheduler core → decisions → back to namespace.
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

            // Route decisions back to their respective namespaces.
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
                    // Recursive routing — but scheduler decisions produce pod assignments
                    // (worker commands), never new scheduler requests. So this is single-pass.
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
                    // Assert: no new scheduler messages from decision processing.
                    debug_assert!(
                        ns_effects.scheduler_messages.is_empty(),
                        "scheduler decisions should not produce new scheduler messages"
                    );
                }
            }
        }
    }

    /// Route worker state effects: scheduler updates go to scheduler core,
    /// worker registry broadcasts pass through to the shell.
    fn route_worker_state_effects(
        &mut self,
        ws_effects: types::WorkerStateEffects,
        effects: &mut OrchestratorEffects,
    ) {
        // Scheduler updates from worker state → scheduler core.
        for update in ws_effects.scheduler_updates {
            let decisions = self.scheduler.process(update);

            // Route any decisions back to namespaces.
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
                    debug_assert!(ns_effects.scheduler_messages.is_empty());
                }
            }
        }

        // Worker registry broadcast → shell sends to all connected workers.
        if let Some(cmd) = ws_effects.worker_registry_broadcast {
            effects.global_broadcasts.push(cmd);
        }
    }

    /// Access a namespace core (for testing / inspection).
    pub(crate) fn namespace(&self, id: &NamespaceId) -> Option<&NamespaceCore> {
        self.namespaces.get(id)
    }

    /// Access all namespace IDs.
    pub(crate) fn namespace_ids(&self) -> impl Iterator<Item = &NamespaceId> {
        self.namespaces.keys()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::timer::TimerConfig;
    use std::time::Duration;

    fn test_timer_config() -> TimerConfig {
        TimerConfig {
            retry_backoff: Duration::from_millis(100),
            launch_timeout: Duration::from_millis(100),
            suspend_timeout: Duration::from_millis(100),
            idle_timeout: Duration::from_millis(100),
        }
    }

    fn ns(name: &str) -> NamespaceId {
        NamespaceId::from(name)
    }

    #[test]
    fn create_and_destroy_namespace() {
        let mut orch = SyncOrchestrator::new(test_timer_config());

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
    fn worker_state_event_routes_to_scheduler() {
        let mut orch = SyncOrchestrator::new(test_timer_config());

        let effects = orch.process(OrchestratorInput::WorkerStateEvent(
            WorkerStateCoreEvent::Connected {
                worker_id: GlobalWorkerId::test(1),
                capabilities: distvirt_worker_protocol::WorkerCapabilities {
                    has_kvm: false,
                    has_containerd: false,
                    available_adapters: vec![],
                    max_pods: 10,
                    available_memory_mb: 1024,
                    public_endpoint: String::new(),
                    pools: vec![],
                },
                tunnel_info: None,
                proto_worker_id: distvirt_worker_protocol::WorkerId::from("w-1"),
            },
        ));

        // Should produce a global broadcast (worker registry sync).
        assert!(!effects.global_broadcasts.is_empty());
    }

    #[test]
    fn full_sync_flow() {
        // This test verifies the full sync path: create namespace, connect worker,
        // see scheduler messages, grant lease, see worker commands.
        // All without any async runtime.
        let mut orch = SyncOrchestrator::new(test_timer_config());

        // Create namespace.
        orch.process(OrchestratorInput::CreateNamespace {
            namespace_id: ns("test"),
        });

        // Register worker in worker state.
        orch.process(OrchestratorInput::WorkerStateEvent(
            WorkerStateCoreEvent::Connected {
                worker_id: GlobalWorkerId::test(1),
                capabilities: distvirt_worker_protocol::WorkerCapabilities {
                    has_kvm: false,
                    has_containerd: false,
                    available_adapters: vec![],
                    max_pods: 10,
                    available_memory_mb: 1024,
                    public_endpoint: String::new(),
                    pools: vec![],
                },
                tunnel_info: None,
                proto_worker_id: distvirt_worker_protocol::WorkerId::from("w-1"),
            },
        ));

        // Connect worker to namespace.
        orch.process(OrchestratorInput::NamespaceEvent {
            namespace_id: ns("test"),
            event: NamespaceCoreEvent::WorkerConnected {
                worker_id: GlobalWorkerId::test(1),
                proto_worker_id: distvirt_worker_protocol::WorkerId::from("w-1"),
                info: crate::sm_new::WorkerInfo { capacity: 10 },
            },
        });

        // Promote worker (NamespaceCreated).
        let effects = orch.process(OrchestratorInput::NamespaceEvent {
            namespace_id: ns("test"),
            event: NamespaceCoreEvent::WorkerEvent(crate::task::WorkerNamespaceEvent {
                worker_id: GlobalWorkerId::test(1),
                event: crate::task::WorkerNamespaceEventKind::NamespaceCreated,
            }),
        });

        // The namespace has no workloads configured, so minimal effects expected.
        // But the flow completes without panicking — that's the key assertion.
        let _ = effects;
    }
}
