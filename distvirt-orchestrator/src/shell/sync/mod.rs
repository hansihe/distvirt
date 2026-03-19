//! Synchronous shell wrapper around `OrchestratorCore` for testing.
//!
//! Provides a step/drain API for driving the orchestrator with fake time,
//! timer management, and mock worker command handling.
//!
//! Time is driven via a logical clock (`Duration` from zero). Call
//! `advance_time()` to move the clock forward and fire any expired timers.
//! No tokio dependency — tests are fully synchronous.
//!
//! # Mock workers
//!
//! Each worker has a `CommandHandler` that maps outgoing `WorkerCommand`s to
//! response `WorkerEvent`s. The default handler simulates a happy-path worker:
//! `LaunchPod` → `PodRunning`, `StopPod` → `PodExited(0)`, etc. Custom handlers
//! can override individual commands (return `Some(events)`) or fall through to
//! the default (return `None`).

#[cfg(test)]
mod tests;

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use crate::adapter::timer::TimerConfig;
use crate::core::namespace_boundary::NamespaceWithBoundary;
use crate::core::orchestrator::OrchestratorCore;
use crate::event_bus::EventBusHandle;
use crate::id_registry::IdRegistryMap;
use crate::core::types::{
    CreateNamespaceInfo, NamespaceCoreEvent, OrchestratorEffects, OrchestratorInput,
    WorkerConnectedInfo, WorkerStateCoreEvent,
};
use crate::core::worker_state::WorkerTunnelInfo;
use crate::core::worker_event::{ClassifiedWorkerEvent, classify};
use crate::core::{ClientCommand, GlobalWorkerId, WorkerNamespaceEvent, WorkerNamespaceEventKind};
use crate::types::NamespaceId;

use distvirt_worker_protocol::{WorkerCommand, WorkerEvent};

// =============================================================================
// Command handler types
// =============================================================================

/// Handler that can override the default command→event mapping.
/// Return `None` to fall through to default, `Some(vec![])` to suppress,
/// `Some(events)` to override.
pub type CommandHandler =
    Box<dyn Fn(&WorkerCommand) -> Option<Vec<WorkerEvent>> + Send + Sync + 'static>;

/// Default happy-path handler: maps commands to expected response events.
fn default_command_handler(cmd: &WorkerCommand) -> Vec<WorkerEvent> {
    match cmd {
        WorkerCommand::CreateNamespace { namespace_id, .. } => {
            vec![WorkerEvent::NamespaceCreated {
                namespace_id: namespace_id.clone(),
            }]
        }
        WorkerCommand::LaunchPod {
            namespace_id,
            pod_id,
            ..
        } => vec![WorkerEvent::PodRunning {
            namespace_id: namespace_id.clone(),
            pod_id: pod_id.clone(),
        }],
        WorkerCommand::StopPod {
            namespace_id,
            pod_id,
            ..
        } => vec![WorkerEvent::PodExited {
            namespace_id: namespace_id.clone(),
            pod_id: pod_id.clone(),
            exit_code: 0,
        }],
        WorkerCommand::DestroyNamespace { namespace_id } => {
            vec![WorkerEvent::NamespaceDestroyed {
                namespace_id: namespace_id.clone(),
            }]
        }
        WorkerCommand::SuspendPod {
            namespace_id,
            pod_id,
            artifact_id,
            pool_id,
            ..
        } => vec![
            WorkerEvent::ArtifactWriteStarted {
                namespace_id: namespace_id.clone(),
                artifact_id: artifact_id.clone(),
                pool_id: pool_id.clone(),
            },
            WorkerEvent::ArtifactWriteCommitted {
                namespace_id: namespace_id.clone(),
                artifact_id: artifact_id.clone(),
                pool_id: pool_id.clone(),
                size_bytes: 1024,
            },
            WorkerEvent::PodSuspended {
                namespace_id: namespace_id.clone(),
                pod_id: pod_id.clone(),
                artifact_id: artifact_id.clone(),
                artifact_size_bytes: 1024,
                pool_id: pool_id.clone(),
            },
        ],
        WorkerCommand::ResumePod {
            namespace_id,
            pod_id,
            ..
        } => vec![WorkerEvent::PodRunning {
            namespace_id: namespace_id.clone(),
            pod_id: pod_id.clone(),
        }],
        // Everything else (EndpointUpdate, RegistrySync, WorkerRegistrySync, etc.): no response.
        _ => vec![],
    }
}

// =============================================================================
// Worker config (mirrors old MockWorkerConfig)
// =============================================================================

/// Configuration for a mock worker in the sync shell.
pub struct MockWorkerConfig {
    pub handler: Option<CommandHandler>,
    pub capabilities: distvirt_worker_protocol::WorkerCapabilities,
    pub tunnel_info: Option<WorkerTunnelInfo>,
}

impl Default for MockWorkerConfig {
    fn default() -> Self {
        MockWorkerConfig {
            handler: None,
            capabilities: distvirt_worker_protocol::WorkerCapabilities {
                has_kvm: true,
                has_containerd: true,
                available_adapters: vec![],
                max_pods: 10,
                available_memory_mb: 1024,
                public_endpoint: String::new(),
                pools: vec![],
            },
            tunnel_info: None,
        }
    }
}

impl MockWorkerConfig {
    /// Set a custom command handler.
    pub fn with_handler(mut self, handler: CommandHandler) -> Self {
        self.handler = Some(handler);
        self
    }

    /// Handler that returns PodFailed on LaunchPod.
    pub fn with_launch_failure() -> Self {
        MockWorkerConfig {
            handler: Some(Box::new(|cmd| match cmd {
                WorkerCommand::LaunchPod {
                    namespace_id,
                    pod_id,
                    ..
                } => Some(vec![WorkerEvent::PodFailed {
                    namespace_id: namespace_id.clone(),
                    pod_id: pod_id.clone(),
                    error: "mock launch failure".to_string(),
                }]),
                _ => None,
            })),
            ..Default::default()
        }
    }

    /// Handler that returns PodSuspendFailed on SuspendPod.
    pub fn with_suspend_failure() -> Self {
        MockWorkerConfig {
            handler: Some(Box::new(|cmd| match cmd {
                WorkerCommand::SuspendPod {
                    namespace_id,
                    pod_id,
                    ..
                } => Some(vec![WorkerEvent::PodSuspendFailed {
                    namespace_id: namespace_id.clone(),
                    pod_id: pod_id.clone(),
                    error: "mock suspend failure".to_string(),
                }]),
                _ => None,
            })),
            ..Default::default()
        }
    }

    /// Handler that returns empty vec on LaunchPod (no response, simulates hang/timeout).
    pub fn with_launch_hang() -> Self {
        MockWorkerConfig {
            handler: Some(Box::new(|cmd| match cmd {
                WorkerCommand::LaunchPod { .. } => Some(vec![]),
                _ => None,
            })),
            ..Default::default()
        }
    }

    /// Handler that returns empty vec on SuspendPod (no response, simulates hang/timeout).
    pub fn with_suspend_hang() -> Self {
        MockWorkerConfig {
            handler: Some(Box::new(|cmd| match cmd {
                WorkerCommand::SuspendPod { .. } => Some(vec![]),
                _ => None,
            })),
            ..Default::default()
        }
    }

    /// Config with a local storage pool (needed for suspend/resume).
    pub fn with_pool() -> Self {
        MockWorkerConfig {
            capabilities: distvirt_worker_protocol::WorkerCapabilities {
                has_kvm: true,
                has_containerd: true,
                available_adapters: vec![],
                max_pods: 10,
                available_memory_mb: 1024,
                public_endpoint: String::new(),
                pools: vec![distvirt_worker_protocol::PoolInfo {
                    pool_id: distvirt_worker_protocol::PoolId::from("local"),
                    path: "/tmp/pool".to_string(),
                    capacity_bytes: 1024 * 1024 * 1024,
                    available_bytes: 1024 * 1024 * 1024,
                }],
            },
            ..Default::default()
        }
    }

    /// Add a local storage pool to an existing config (chainable).
    pub fn add_pool(mut self) -> Self {
        self.capabilities
            .pools
            .push(distvirt_worker_protocol::PoolInfo {
                pool_id: distvirt_worker_protocol::PoolId::from("local"),
                path: "/tmp/pool".to_string(),
                capacity_bytes: 1024 * 1024 * 1024,
                available_bytes: 1024 * 1024 * 1024,
            });
        self
    }

    /// Handler that returns PodFailed on ResumePod.
    pub fn with_resume_failure() -> Self {
        MockWorkerConfig {
            handler: Some(Box::new(|cmd| match cmd {
                WorkerCommand::ResumePod {
                    namespace_id,
                    pod_id,
                    ..
                } => Some(vec![WorkerEvent::PodFailed {
                    namespace_id: namespace_id.clone(),
                    pod_id: pod_id.clone(),
                    error: "mock resume failure".to_string(),
                }]),
                _ => None,
            })),
            ..MockWorkerConfig::with_pool()
        }
    }

    /// Config with a local storage pool and limited memory.
    pub fn with_pool_and_memory(available_memory_mb: u64) -> Self {
        MockWorkerConfig {
            capabilities: distvirt_worker_protocol::WorkerCapabilities {
                has_kvm: true,
                has_containerd: true,
                available_adapters: vec![],
                max_pods: 10,
                available_memory_mb,
                public_endpoint: String::new(),
                pools: vec![distvirt_worker_protocol::PoolInfo {
                    pool_id: distvirt_worker_protocol::PoolId::from("local"),
                    path: "/tmp/pool".to_string(),
                    capacity_bytes: 1024 * 1024 * 1024,
                    available_bytes: 1024 * 1024 * 1024,
                }],
            },
            ..Default::default()
        }
    }

    /// Config with tunnel capabilities.
    pub fn with_tunnel(endpoint: &str, public_key: [u8; 32]) -> Self {
        MockWorkerConfig {
            capabilities: distvirt_worker_protocol::WorkerCapabilities {
                has_kvm: true,
                has_containerd: true,
                available_adapters: vec![],
                max_pods: 10,
                available_memory_mb: 1024,
                public_endpoint: endpoint.to_string(),
                pools: vec![distvirt_worker_protocol::PoolInfo {
                    pool_id: distvirt_worker_protocol::PoolId::from("local"),
                    path: "/tmp/pool".to_string(),
                    capacity_bytes: 1024 * 1024 * 1024,
                    available_bytes: 1024 * 1024 * 1024,
                }],
            },
            tunnel_info: Some(WorkerTunnelInfo {
                listen_port: 51820,
                public_key,
            }),
            ..Default::default()
        }
    }
}

// =============================================================================
// SyncShell
// =============================================================================

struct WorkerState {
    proto_worker_id: distvirt_worker_protocol::WorkerId,
    handler: Option<CommandHandler>,
    /// Commands sent to this worker (for test assertions).
    commands_sent: Vec<WorkerCommand>,
    /// Index into commands_sent: next command to process through handler.
    /// Commands before this index have already had their responses queued.
    commands_handled: usize,
}

pub struct SyncShell {
    core: OrchestratorCore,
    /// Logical clock — starts at zero, advanced explicitly by the caller.
    now: Duration,
    /// Connected workers and their buffered commands.
    workers: HashMap<GlobalWorkerId, WorkerState>,
    /// Worker ID allocator.
    next_worker_id: u64,
    /// Pending events to process.
    pending: VecDeque<OrchestratorInput>,
    /// Observability event bus (same as async shell — tests can subscribe).
    event_bus: EventBusHandle,
    /// Shared ID registries.
    id_registry_map: IdRegistryMap,
}

impl SyncShell {
    /// Create a new sync shell with the given timer config.
    pub fn new(timer_config: TimerConfig) -> Self {
        let id_registry_map = IdRegistryMap::new();
        SyncShell {
            core: OrchestratorCore::new(timer_config, id_registry_map.clone()),
            now: Duration::ZERO,
            workers: HashMap::new(),
            next_worker_id: 1,
            pending: VecDeque::new(),
            event_bus: EventBusHandle::new(1024),
            id_registry_map,
        }
    }

    /// Access the event bus for subscribing to observability events.
    pub fn event_bus(&self) -> &EventBusHandle {
        &self.event_bus
    }

    /// Access the shared ID registry map.
    pub fn id_registry_map(&self) -> &IdRegistryMap {
        &self.id_registry_map
    }

    /// Register a worker with default config.
    pub fn add_worker_default(&mut self) -> GlobalWorkerId {
        self.add_worker(MockWorkerConfig::default())
    }

    /// Register a worker with custom config.
    /// Returns the GlobalWorkerId assigned.
    ///
    /// Immediately connects the worker and queues `NamespaceCreated` events
    /// for all existing namespaces. After `drain()`, the worker will be fully
    /// active in all namespaces.
    pub fn add_worker(&mut self, config: MockWorkerConfig) -> GlobalWorkerId {
        let worker_id = crate::sm::WorkerId(self.next_worker_id);
        self.next_worker_id += 1;

        let proto_worker_id =
            distvirt_worker_protocol::WorkerId::from(worker_id.0);

        self.workers.insert(
            worker_id,
            WorkerState {
                proto_worker_id: proto_worker_id.clone(),
                handler: config.handler,
                commands_sent: Vec::new(),
                commands_handled: 0,
            },
        );

        let effects = self.core.worker_connected(
            WorkerConnectedInfo {
                worker_id,
                capabilities: config.capabilities,
                tunnel_info: config.tunnel_info,
                wireguard_info: None,
                proto_worker_id,
            },
            self.now,
        );
        self.execute_effects(effects);

        // Process any CreateNamespace commands that were just sent to this worker.
        // This generates NamespaceCreated response events which get queued.
        self.process_new_worker_commands();

        worker_id
    }

    /// Remove/disconnect a worker.
    pub fn disconnect_worker(&mut self, worker_id: GlobalWorkerId) {
        self.workers.remove(&worker_id);
        let effects = self.core.worker_disconnected(worker_id, self.now);
        self.execute_effects(effects);
        self.process_new_worker_commands();
    }

    /// Create a namespace. Immediately creates it in the core and fans out
    /// CreateNamespace commands to all connected workers. The mock worker
    /// handlers will respond with NamespaceCreated, which gets queued.
    pub fn create_namespace(
        &mut self,
        namespace_id: NamespaceId,
        network: distvirt_worker_protocol::NetworkConfig,
    ) {
        let (_result, effects) = self.core.create_namespace(
            CreateNamespaceInfo {
                namespace_id,
                network,
            },
            self.now,
        );
        self.execute_effects(effects);

        // Process CreateNamespace commands sent to workers → NamespaceCreated responses.
        self.process_new_worker_commands();
    }

    /// Destroy a namespace.
    pub fn destroy_namespace(&mut self, namespace_id: &NamespaceId) {
        let (_result, effects) = self.core.destroy_namespace(namespace_id);
        self.execute_effects(effects);

        // Process DestroyNamespace commands → NamespaceDestroyed responses.
        self.process_new_worker_commands();
    }

    /// Connect a WireGuard peer to a namespace. Processes immediately and drains.
    pub fn connect_network(
        &mut self,
        namespace_id: &NamespaceId,
        client_public_key: [u8; 32],
    ) -> Result<crate::core::ConnectResult, crate::core::ClientError> {
        let (result, effects) = self.core.connect_network(namespace_id, client_public_key, self.now);
        self.execute_effects(effects);
        self.process_new_worker_commands();
        result
    }

    /// Disconnect a WireGuard peer from a namespace. Processes immediately and drains.
    pub fn disconnect_network(
        &mut self,
        namespace_id: &NamespaceId,
        client_public_key: [u8; 32],
    ) -> Result<(), crate::core::ClientError> {
        let (result, effects) = self.core.disconnect_network(namespace_id, client_public_key, self.now);
        self.execute_effects(effects);
        self.process_new_worker_commands();
        result
    }

    /// Queue a client command to a namespace (processed on next `step()`/`drain()`).
    pub fn client_command(&mut self, namespace_id: &NamespaceId, cmd: ClientCommand) {
        self.pending.push_back(OrchestratorInput::NamespaceEvent {
            namespace_id: namespace_id.clone(),
            event: NamespaceCoreEvent::ClientCommand(cmd),
        });
    }

    /// Queue a worker event for a specific namespace (processed on next `step()`/`drain()`).
    pub fn inject_worker_event(
        &mut self,
        namespace_id: &NamespaceId,
        worker_id: GlobalWorkerId,
        event: WorkerNamespaceEventKind,
    ) {
        self.pending.push_back(OrchestratorInput::NamespaceEvent {
            namespace_id: namespace_id.clone(),
            event: NamespaceCoreEvent::WorkerEvent(WorkerNamespaceEvent { worker_id, event }),
        });
    }

    /// Inject a pressure update for a worker (processed on next `step()`/`drain()`).
    pub fn inject_pressure_update(
        &mut self,
        worker_id: GlobalWorkerId,
        cpu: distvirt_worker_protocol::PsiMetrics,
        memory: distvirt_worker_protocol::PsiMetrics,
        io: distvirt_worker_protocol::PsiMetrics,
    ) {
        self.pending.push_back(OrchestratorInput::WorkerStateEvent(
            WorkerStateCoreEvent::PressureUpdate {
                worker_id,
                cpu,
                memory,
                io,
            },
        ));
    }

    /// Advance the logical clock by `delta` and fire any expired timers.
    /// This processes all timer-triggered effects (which may cascade).
    pub fn advance_time(&mut self, delta: Duration) {
        self.now += delta;
        let effects = self.core.advance_to(self.now);
        self.execute_effects(effects);
        self.process_new_worker_commands();
    }

    /// Process one pending event. Returns true if there was work to do.
    pub fn step(&mut self) -> bool {
        if let Some(input) = self.pending.pop_front() {
            let effects = self.core.process(input, self.now);
            self.execute_effects(effects);
            // After executing effects, process any new worker commands through handlers.
            self.process_new_worker_commands();
            true
        } else {
            false
        }
    }

    /// Process all pending events until quiescent.
    pub fn drain(&mut self) {
        loop {
            if !self.step() {
                break;
            }
        }
    }

    /// Execute effects from OrchestratorCore, converting them to pending events
    /// or buffered commands.
    fn execute_effects(&mut self, effects: OrchestratorEffects) {
        // Targeted worker commands.
        for (worker_id, cmd) in effects.worker_commands {
            if let Some(state) = self.workers.get_mut(&worker_id) {
                state.commands_sent.push(cmd);
            }
        }

        // Broadcast commands scoped to a namespace.
        for (namespace_id, cmd) in effects.broadcast_commands {
            if let Some(ns) = self.core.namespace(&namespace_id) {
                let active: Vec<GlobalWorkerId> = ns.active_worker_ids().collect();
                for worker_id in active {
                    if let Some(state) = self.workers.get_mut(&worker_id) {
                        state.commands_sent.push(cmd.clone());
                    }
                }
            }
        }

        // Global broadcasts to all connected workers.
        for cmd in effects.global_broadcasts {
            let worker_ids: Vec<GlobalWorkerId> = self.workers.keys().copied().collect();
            for worker_id in worker_ids {
                if let Some(state) = self.workers.get_mut(&worker_id) {
                    state.commands_sent.push(cmd.clone());
                }
            }
        }

        // Direct worker commands.
        for dwc in effects.direct_worker_commands {
            if let Some(state) = self.workers.get_mut(&dwc.worker_id) {
                state.commands_sent.push(dwc.command);
            }
        }

        // Publish observability events to the event bus.
        for (namespace_id, events) in effects.observability_events {
            self.event_bus.publish(&namespace_id, events);
        }
    }

    /// Process any new (unhandled) worker commands through their handlers,
    /// queueing response events back into `self.pending`.
    fn process_new_worker_commands(&mut self) {
        // Collect all response events first to avoid borrow conflicts.
        let mut responses: Vec<(GlobalWorkerId, Vec<WorkerEvent>)> = Vec::new();

        for (&worker_id, state) in &mut self.workers {
            while state.commands_handled < state.commands_sent.len() {
                let cmd = &state.commands_sent[state.commands_handled];
                state.commands_handled += 1;

                let events = if let Some(ref handler) = state.handler {
                    match handler(cmd) {
                        Some(evts) => evts,
                        None => default_command_handler(cmd),
                    }
                } else {
                    default_command_handler(cmd)
                };

                if !events.is_empty() {
                    responses.push((worker_id, events));
                }
            }
        }

        // Queue response events.
        for (worker_id, events) in responses {
            for event in events {
                self.queue_worker_event(worker_id, event);
            }
        }
    }

    /// Classify a raw WorkerEvent and queue it as the appropriate OrchestratorInput.
    /// Delegates to `core::worker_event::classify` so both shells share the same mapping.
    pub fn queue_worker_event(&mut self, worker_id: GlobalWorkerId, event: WorkerEvent) {
        let input = match classify(worker_id, event) {
            ClassifiedWorkerEvent::Namespace {
                namespace_id,
                event,
            } => OrchestratorInput::NamespaceEvent {
                namespace_id,
                event,
            },
            ClassifiedWorkerEvent::WorkerState(event) => OrchestratorInput::WorkerStateEvent(event),
            ClassifiedWorkerEvent::Scheduler(input) => OrchestratorInput::SchedulerEvent(input),
            ClassifiedWorkerEvent::Ignored => return,
        };
        self.pending.push_back(input);
    }

    // =========================================================================
    // State access
    // =========================================================================

    /// Access a namespace's core (for reading router state).
    pub fn namespace(&self, id: &NamespaceId) -> Option<&NamespaceWithBoundary> {
        self.core.namespace(id)
    }

    /// Get all namespace IDs.
    pub fn namespace_ids(&self) -> impl Iterator<Item = &NamespaceId> {
        self.core.namespace_ids()
    }

    /// Get commands sent to a specific worker.
    pub fn worker_commands(&self, worker_id: &GlobalWorkerId) -> &[WorkerCommand] {
        self.workers
            .get(worker_id)
            .map(|s| s.commands_sent.as_slice())
            .unwrap_or(&[])
    }

    /// Get the protocol WorkerId for a GlobalWorkerId.
    pub fn proto_worker_id(
        &self,
        worker_id: &GlobalWorkerId,
    ) -> Option<&distvirt_worker_protocol::WorkerId> {
        self.workers.get(worker_id).map(|s| &s.proto_worker_id)
    }

    /// Check if a worker is connected.
    pub fn has_worker(&self, worker_id: &GlobalWorkerId) -> bool {
        self.workers.contains_key(worker_id)
    }

    /// Get all connected worker IDs.
    pub fn worker_ids(&self) -> impl Iterator<Item = &GlobalWorkerId> {
        self.workers.keys()
    }
}
