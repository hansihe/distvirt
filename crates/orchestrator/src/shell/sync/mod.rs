//! Synchronous shell wrapper for testing.
//!
//! Provides a step/drain API for driving the orchestrator with fake time,
//! timer management, and mock worker command handling.
//!
//! The shell owns both the `OrchestratorCore` and a map of `NamespaceUnit`s.
//! It uses a two-queue delivery loop to route messages between them:
//! - `orch_pending`: events for the orchestrator
//! - `ns_pending`: events for namespaces
//!
//! Time is driven via a logical clock. Call `advance_time()` to move the
//! clock forward and fire any expired namespace timers.

#[cfg(test)]
mod tests;

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use crate::adapter::timer::TimerConfig;
use crate::core::namespace::NamespaceUnit;
use crate::core::orchestrator::OrchestratorCore;
use crate::event_bus::EventBusHandle;
use crate::id_registry::IdRegistryMap;
use crate::core::types::{
    CreateNamespaceInfo, NamespaceOutput, OrchestratorInputNew, OrchestratorOutput,
    OrchestratorToNamespace, WorkerConnectedInfo, WorkerStateCoreEvent,
};
use crate::core::orchestrator::worker_state::WorkerTunnelInfo;
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
        _ => vec![],
    }
}

// =============================================================================
// Worker config
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
    pub fn with_handler(mut self, handler: CommandHandler) -> Self {
        self.handler = Some(handler);
        self
    }

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

    pub fn with_launch_hang() -> Self {
        MockWorkerConfig {
            handler: Some(Box::new(|cmd| match cmd {
                WorkerCommand::LaunchPod { .. } => Some(vec![]),
                _ => None,
            })),
            ..Default::default()
        }
    }

    pub fn with_suspend_hang() -> Self {
        MockWorkerConfig {
            handler: Some(Box::new(|cmd| match cmd {
                WorkerCommand::SuspendPod { .. } => Some(vec![]),
                _ => None,
            })),
            ..Default::default()
        }
    }

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
    commands_sent: Vec<WorkerCommand>,
    commands_handled: usize,
}

pub struct SyncShell {
    core: OrchestratorCore,
    /// Namespaces owned by the shell (not the orchestrator).
    namespaces: HashMap<NamespaceId, NamespaceUnit>,
    /// Logical clock.
    now: Duration,
    /// Connected workers and their buffered commands.
    workers: HashMap<GlobalWorkerId, WorkerState>,
    /// Worker ID allocator.
    next_worker_id: u64,
    /// Pending events for the orchestrator.
    orch_pending: VecDeque<OrchestratorInputNew>,
    /// Pending events for namespaces.
    ns_pending: VecDeque<(NamespaceId, OrchestratorToNamespace)>,
    /// Observability event bus.
    event_bus: EventBusHandle,
    /// Shared ID registries.
    id_registry_map: IdRegistryMap,
}

impl SyncShell {
    pub fn new(timer_config: TimerConfig) -> Self {
        let id_registry_map = IdRegistryMap::new();
        SyncShell {
            core: OrchestratorCore::new(timer_config, id_registry_map.clone()),
            namespaces: HashMap::new(),
            now: Duration::ZERO,
            workers: HashMap::new(),
            next_worker_id: 1,
            orch_pending: VecDeque::new(),
            ns_pending: VecDeque::new(),
            event_bus: EventBusHandle::new(1024),
            id_registry_map,
        }
    }

    pub fn event_bus(&self) -> &EventBusHandle {
        &self.event_bus
    }

    pub fn id_registry_map(&self) -> &IdRegistryMap {
        &self.id_registry_map
    }

    pub fn add_worker_default(&mut self) -> GlobalWorkerId {
        self.add_worker(MockWorkerConfig::default())
    }

    pub fn add_worker(&mut self, config: MockWorkerConfig) -> GlobalWorkerId {
        let worker_id = crate::sm::WorkerId(self.next_worker_id);
        self.next_worker_id += 1;

        let proto_worker_id = distvirt_worker_protocol::WorkerId::from(worker_id.0);

        self.workers.insert(
            worker_id,
            WorkerState {
                proto_worker_id: proto_worker_id.clone(),
                handler: config.handler,
                commands_sent: Vec::new(),
                commands_handled: 0,
            },
        );

        let output = self.core.worker_connected(
            WorkerConnectedInfo {
                worker_id,
                capabilities: config.capabilities,
                tunnel_info: config.tunnel_info,
                wireguard_info: None,
                proto_worker_id,
            },
            self.now,
        );
        self.route_orchestrator_output(output);
        self.delivery_loop();

        worker_id
    }

    pub fn disconnect_worker(&mut self, worker_id: GlobalWorkerId) {
        self.workers.remove(&worker_id);
        let output = self.core.worker_disconnected(worker_id, self.now);
        self.route_orchestrator_output(output);
        self.delivery_loop();
    }

    pub fn create_namespace(
        &mut self,
        namespace_id: NamespaceId,
        network: distvirt_worker_protocol::NetworkConfig,
    ) {
        let (result, output) = self.core.create_namespace(CreateNamespaceInfo {
            namespace_id: namespace_id.clone(),
            network,
        });
        self.route_orchestrator_output(output);

        if let Ok(creation_info) = result {
            // Construct the NamespaceUnit.
            let ns = NamespaceUnit::new(
                namespace_id.clone(),
                creation_info.timer_config,
                &creation_info.network,
                creation_info.id_registry,
            );
            self.namespaces.insert(namespace_id.clone(), ns);

            // Queue WorkerConnected messages for all connected workers.
            for summary in creation_info.connected_workers {
                self.ns_pending.push_back((
                    namespace_id.clone(),
                    OrchestratorToNamespace::WorkerConnected {
                        worker_id: summary.worker_id,
                        proto_worker_id: summary.proto_worker_id,
                        info: crate::sm::WorkerInfo {
                            capacity: summary.max_pods,
                            default_pool: summary.default_pool,
                        },
                    },
                ));
            }
        }

        self.delivery_loop();
    }

    pub fn destroy_namespace(&mut self, namespace_id: &NamespaceId) {
        self.namespaces.remove(namespace_id);
        let (_result, output) = self.core.destroy_namespace(namespace_id);
        self.route_orchestrator_output(output);
        // Remove any pending namespace events for this namespace.
        self.ns_pending
            .retain(|(ns_id, _)| ns_id != namespace_id);
        self.delivery_loop();
    }

    pub fn connect_network(
        &mut self,
        namespace_id: &NamespaceId,
        client_public_key: [u8; 32],
    ) -> Result<crate::core::ConnectResult, crate::core::ClientError> {
        // Check namespace exists.
        if !self.namespaces.contains_key(namespace_id) {
            return Err(crate::core::ClientError::NamespaceNotFound);
        }

        // Find a worker with WireGuard.
        let (worker_id, wg_info, public_endpoint) = match self.core.find_wireguard_worker() {
            Some((wid, wg, ep)) => (wid, wg.clone(), ep.to_string()),
            None => return Err(crate::core::ClientError::NoTunnelWorker),
        };

        // Send Connect command to namespace.
        let ns = self.namespaces.get_mut(namespace_id).unwrap();
        let ns_output = ns.process(
            OrchestratorToNamespace::ClientCommand(ClientCommand::Connect {
                client_public_key,
                worker_id,
            }),
            self.now,
        );
        self.route_namespace_output(namespace_id, ns_output);
        self.delivery_loop();

        // Read back the allocated IP.
        let ns = self.namespaces.get(namespace_id).unwrap();
        let wg_peers = ns.wg_peers();
        let client_ip = match wg_peers.peers.get(&client_public_key) {
            Some(info) => info.client_ip,
            None => return Err(crate::core::ClientError::IpExhausted),
        };

        let subnet_cidr = wg_peers.subnet_cidr();
        let endpoint = format!("{}:{}", public_endpoint, wg_info.listen_port);

        Ok(crate::core::ConnectResult {
            server_public_key: wg_info.public_key,
            endpoint,
            client_ip,
            subnet: subnet_cidr,
        })
    }

    pub fn disconnect_network(
        &mut self,
        namespace_id: &NamespaceId,
        client_public_key: [u8; 32],
    ) -> Result<(), crate::core::ClientError> {
        let Some(ns) = self.namespaces.get_mut(namespace_id) else {
            return Err(crate::core::ClientError::NamespaceNotFound);
        };

        let ns_output = ns.process(
            OrchestratorToNamespace::ClientCommand(ClientCommand::Disconnect {
                client_public_key,
            }),
            self.now,
        );
        self.route_namespace_output(namespace_id, ns_output);
        self.delivery_loop();
        Ok(())
    }

    pub fn client_command(&mut self, namespace_id: &NamespaceId, cmd: ClientCommand) {
        self.ns_pending.push_back((
            namespace_id.clone(),
            OrchestratorToNamespace::ClientCommand(cmd),
        ));
    }

    pub fn inject_worker_event(
        &mut self,
        namespace_id: &NamespaceId,
        worker_id: GlobalWorkerId,
        event: WorkerNamespaceEventKind,
    ) {
        self.ns_pending.push_back((
            namespace_id.clone(),
            OrchestratorToNamespace::WorkerEvent(WorkerNamespaceEvent {
                worker_id,
                event,
            }),
        ));
    }

    pub fn inject_pressure_update(
        &mut self,
        worker_id: GlobalWorkerId,
        cpu: distvirt_worker_protocol::PsiMetrics,
        memory: distvirt_worker_protocol::PsiMetrics,
        io: distvirt_worker_protocol::PsiMetrics,
    ) {
        self.orch_pending
            .push_back(OrchestratorInputNew::WorkerStateEvent(
                WorkerStateCoreEvent::PressureUpdate {
                    worker_id,
                    cpu,
                    memory,
                    io,
                },
            ));
    }

    pub fn advance_time(&mut self, delta: Duration) {
        self.now += delta;
        // Fire timers on all namespaces.
        let ns_ids: Vec<_> = self.namespaces.keys().cloned().collect();
        for ns_id in ns_ids {
            if let Some(ns) = self.namespaces.get_mut(&ns_id) {
                let ns_output = ns.advance_to(self.now);
                self.route_namespace_output_no_borrow(&ns_id, ns_output);
            }
        }
        self.delivery_loop();
    }

    pub fn step(&mut self) -> bool {
        self.step_once()
    }

    pub fn drain(&mut self) {
        self.delivery_loop();
    }

    // =========================================================================
    // Delivery loop
    // =========================================================================

    /// The two-queue delivery loop: process orchestrator and namespace events
    /// until both queues are empty and no new worker commands produce events.
    fn delivery_loop(&mut self) {
        loop {
            let mut did_work = false;

            // Process orchestrator events.
            if let Some(input) = self.orch_pending.pop_front() {
                let output = self.core.process(input);
                self.route_orchestrator_output(output);
                did_work = true;
            }

            // Process namespace events.
            if let Some((ns_id, msg)) = self.ns_pending.pop_front() {
                if let Some(ns) = self.namespaces.get_mut(&ns_id) {
                    let ns_output = ns.process(msg, self.now);
                    self.route_namespace_output_no_borrow(&ns_id, ns_output);
                }
                did_work = true;
            }

            // Process new worker commands (generates response events).
            let had_commands = self.process_new_worker_commands();
            if had_commands {
                did_work = true;
            }

            if !did_work {
                break;
            }
        }
    }

    /// Process a single pending event from either queue.
    fn step_once(&mut self) -> bool {
        if let Some(input) = self.orch_pending.pop_front() {
            let output = self.core.process(input);
            self.route_orchestrator_output(output);
            self.process_new_worker_commands();
            return true;
        }
        if let Some((ns_id, msg)) = self.ns_pending.pop_front() {
            if let Some(ns) = self.namespaces.get_mut(&ns_id) {
                let ns_output = ns.process(msg, self.now);
                self.route_namespace_output_no_borrow(&ns_id, ns_output);
            }
            self.process_new_worker_commands();
            return true;
        }
        false
    }

    // =========================================================================
    // Output routing
    // =========================================================================

    /// Route orchestrator output: direct commands to workers, namespace messages
    /// to ns_pending.
    fn route_orchestrator_output(&mut self, output: OrchestratorOutput) {
        // Namespace messages.
        for (ns_id, msg) in output.to_namespaces {
            self.ns_pending.push_back((ns_id, msg));
        }

        // Worker commands (e.g. DeleteArtifact from scheduler).
        for (worker_id, cmd) in output.worker_commands {
            if let Some(state) = self.workers.get_mut(&worker_id) {
                state.commands_sent.push(cmd);
            }
        }

        // Direct worker commands (CreateNamespace, etc.).
        for dwc in output.direct_worker_commands {
            if let Some(state) = self.workers.get_mut(&dwc.worker_id) {
                state.commands_sent.push(dwc.command);
            }
        }

        // Global broadcasts.
        for cmd in output.global_broadcasts {
            let worker_ids: Vec<GlobalWorkerId> = self.workers.keys().copied().collect();
            for worker_id in worker_ids {
                if let Some(state) = self.workers.get_mut(&worker_id) {
                    state.commands_sent.push(cmd.clone());
                }
            }
        }
    }

    /// Route namespace output: orchestrator messages to orch_pending,
    /// worker commands to workers, observability to event bus.
    /// Uses `&NamespaceId` instead of looking up the namespace (avoids borrow issues).
    fn route_namespace_output_no_borrow(&mut self, namespace_id: &NamespaceId, output: NamespaceOutput) {
        // Orchestrator messages.
        for msg in output.to_orchestrator {
            self.orch_pending
                .push_back(OrchestratorInputNew::FromNamespace {
                    namespace_id: namespace_id.clone(),
                    message: msg,
                });
        }

        // Namespace-scoped broadcasts.
        if !output.broadcast_commands.is_empty() {
            if let Some(ns) = self.namespaces.get(namespace_id) {
                let active: Vec<GlobalWorkerId> = ns.active_worker_ids().collect();
                for cmd in &output.broadcast_commands {
                    for &worker_id in &active {
                        if let Some(state) = self.workers.get_mut(&worker_id) {
                            state.commands_sent.push(cmd.clone());
                        }
                    }
                }
            }
        }

        // Targeted worker commands.
        for (worker_id, cmd) in output.worker_commands {
            if let Some(state) = self.workers.get_mut(&worker_id) {
                state.commands_sent.push(cmd);
            }
        }

        // Observability events.
        if !output.observability_events.is_empty() {
            self.event_bus
                .publish(namespace_id, output.observability_events);
        }
    }

    /// Route namespace output when we already have a `&NamespaceId`.
    fn route_namespace_output(&mut self, namespace_id: &NamespaceId, output: NamespaceOutput) {
        self.route_namespace_output_no_borrow(namespace_id, output);
    }

    /// Process new worker commands through handlers, queueing response events.
    fn process_new_worker_commands(&mut self) -> bool {
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

        let had_work = !responses.is_empty();

        for (worker_id, events) in responses {
            for event in events {
                self.queue_worker_event(worker_id, event);
            }
        }

        had_work
    }

    /// Classify a raw WorkerEvent and queue it to the appropriate pending queue.
    pub fn queue_worker_event(&mut self, worker_id: GlobalWorkerId, event: WorkerEvent) {
        match classify(worker_id, event) {
            ClassifiedWorkerEvent::Namespace {
                namespace_id,
                event,
            } => {
                // Route namespace events directly to namespace pending queue.
                self.ns_pending.push_back((namespace_id, event));
            }
            ClassifiedWorkerEvent::WorkerState(event) => {
                self.orch_pending
                    .push_back(OrchestratorInputNew::WorkerStateEvent(event));
            }
            ClassifiedWorkerEvent::Scheduler(input) => {
                self.orch_pending
                    .push_back(OrchestratorInputNew::SchedulerEvent(input));
            }
            ClassifiedWorkerEvent::Ignored => {}
        }
    }

    // =========================================================================
    // State access
    // =========================================================================

    pub fn namespace(&self, id: &NamespaceId) -> Option<&NamespaceUnit> {
        self.namespaces.get(id)
    }

    pub fn namespace_ids(&self) -> impl Iterator<Item = &NamespaceId> {
        self.namespaces.keys()
    }

    pub fn worker_commands(&self, worker_id: &GlobalWorkerId) -> &[WorkerCommand] {
        self.workers
            .get(worker_id)
            .map(|s| s.commands_sent.as_slice())
            .unwrap_or(&[])
    }

    pub fn proto_worker_id(
        &self,
        worker_id: &GlobalWorkerId,
    ) -> Option<&distvirt_worker_protocol::WorkerId> {
        self.workers.get(worker_id).map(|s| &s.proto_worker_id)
    }

    pub fn has_worker(&self, worker_id: &GlobalWorkerId) -> bool {
        self.workers.contains_key(worker_id)
    }

    pub fn worker_ids(&self) -> impl Iterator<Item = &GlobalWorkerId> {
        self.workers.keys()
    }
}

