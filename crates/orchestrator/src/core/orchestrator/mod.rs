//! Top-level orchestrator core — scheduler + worker state, no namespace ownership.
//!
//! `OrchestratorCore` no longer owns namespaces or timers. Those live in
//! `NamespaceUnit` instances managed by the shell. The orchestrator communicates
//! with namespaces via `OrchestratorToNamespace` / `NamespaceToOrchestrator`
//! messages delivered through the shell's routing loop.
//!
//! No async, no channels — pure, deterministic logic.

pub(crate) mod scheduler;
pub mod worker_state;

use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Duration;

use self::scheduler::{SchedulerCore, SchedulerEffects};
use self::worker_state::WorkerStateCore;
use super::types::{
    ConnectedWorkerSummary, CreateNamespaceInfo, DirectWorkerCommand, NamespaceCreationInfo,
    NamespaceToOrchestrator, OrchestratorInputNew, OrchestratorOutput, OrchestratorToNamespace,
    SchedulerCoreInput, SchedulerMessage, WorkerConnectedInfo, WorkerStateCoreEvent,
};
use crate::adapter::timer::TimerConfig;
use crate::core::{ClientError, GlobalWorkerId, SchedulerDecision};
use crate::id_registry::IdRegistryMap;
use crate::sm::{ArtifactPortId, WorkerInfo};
use crate::types::NamespaceId;

pub struct OrchestratorCore {
    scheduler: SchedulerCore,
    worker_state: WorkerStateCore,
    timer_config: TimerConfig,

    /// Connected workers tracked at orchestrator level (for lifecycle fan-out).
    connected_workers: HashMap<GlobalWorkerId, ConnectedWorkerInfo>,

    /// Known namespace IDs (for fan-out on worker connect/disconnect).
    namespace_ids: HashSet<NamespaceId>,

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
            scheduler: SchedulerCore::new(),
            worker_state: WorkerStateCore::new(),
            timer_config,
            connected_workers: HashMap::new(),
            namespace_ids: HashSet::new(),
            next_segment_id: 1, // segment 0 is reserved
            active_segment_ids: BTreeSet::new(),
            namespace_segments: HashMap::new(),
            namespace_networks: HashMap::new(),
            id_registry_map,
        }
    }

    /// Process a single input, returning output for the shell to route.
    pub fn process(&mut self, input: OrchestratorInputNew) -> OrchestratorOutput {
        let mut output = OrchestratorOutput::default();

        match input {
            OrchestratorInputNew::WorkerStateEvent(event) => {
                let ws_effects = self.worker_state.process(event);
                self.route_worker_state_effects(ws_effects, &mut output);
            }
            OrchestratorInputNew::SchedulerEvent(input) => {
                let sched_effects = self.scheduler.process(input);
                self.route_scheduler_effects(sched_effects, &mut output);
            }
            OrchestratorInputNew::FromNamespace {
                namespace_id: _,
                message,
            } => match message {
                NamespaceToOrchestrator::SchedulerMessage(msg) => {
                    let scheduler_input = scheduler_message_to_input(msg);
                    let sched_effects = self.scheduler.process(scheduler_input);
                    self.route_scheduler_effects(sched_effects, &mut output);
                }
            },
        }

        output
    }

    // =========================================================================
    // High-level lifecycle methods
    // =========================================================================

    /// Register a newly connected worker.
    ///
    /// Returns output with `to_namespaces` messages (WorkerConnected for each
    /// known namespace) and `direct_worker_commands` (CreateNamespace for each).
    pub fn worker_connected(
        &mut self,
        info: WorkerConnectedInfo,
        _now: Duration,
    ) -> OrchestratorOutput {
        let mut output = OrchestratorOutput::default();

        let worker_id = info.worker_id;
        let proto_worker_id = info.proto_worker_id;
        let max_pods = info.capabilities.max_pods;
        let default_pool = info
            .capabilities
            .pools
            .first()
            .map(|p| p.pool_id.clone());

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
            wireguard_info: info.wireguard_info,
            proto_worker_id: proto_worker_id.clone(),
        });
        self.route_worker_state_effects(ws_effects, &mut output);

        let ns_ids: Vec<_> = self.namespace_ids.iter().cloned().collect();
        for ns_id in ns_ids {
            if let Some(network) = self.namespace_networks.get(&ns_id) {
                output.direct_worker_commands.push(DirectWorkerCommand {
                    worker_id,
                    command: distvirt_worker_protocol::WorkerCommand::CreateNamespace {
                        namespace_id: ns_id.clone(),
                        network: network.clone(),
                    },
                });
            }

            output.to_namespaces.push((
                ns_id.clone(),
                OrchestratorToNamespace::WorkerConnected {
                    worker_id,
                    proto_worker_id: proto_worker_id.clone(),
                    info: WorkerInfo {
                        capacity: max_pods,
                        default_pool: default_pool.clone(),
                    },
                },
            ));

            let ws_effects =
                self.worker_state
                    .process(WorkerStateCoreEvent::NamespaceAssigned {
                        worker_id,
                        namespace_id: ns_id,
                    });
            self.route_worker_state_effects(ws_effects, &mut output);
        }

        output
    }

    /// Unregister a disconnected worker.
    ///
    /// Returns output with `to_namespaces` messages (WorkerDisconnected for each).
    pub fn worker_disconnected(
        &mut self,
        worker_id: GlobalWorkerId,
        _now: Duration,
    ) -> OrchestratorOutput {
        let mut output = OrchestratorOutput::default();

        self.connected_workers.remove(&worker_id);

        // Remove the worker from the scheduler FIRST, before namespace processing.
        let ws_effects = self
            .worker_state
            .process(WorkerStateCoreEvent::Disconnected { worker_id });
        self.route_worker_state_effects(ws_effects, &mut output);

        let ns_ids: Vec<_> = self.namespace_ids.iter().cloned().collect();
        for ns_id in ns_ids {
            output.to_namespaces.push((
                ns_id.clone(),
                OrchestratorToNamespace::WorkerDisconnected { worker_id },
            ));

            let ws_effects =
                self.worker_state
                    .process(WorkerStateCoreEvent::NamespaceUnassigned {
                        worker_id,
                        namespace_id: ns_id,
                    });
            self.route_worker_state_effects(ws_effects, &mut output);
        }

        output
    }

    /// Create a namespace.
    ///
    /// Allocates segment, registers with worker state. Returns a
    /// `NamespaceCreationInfo` that the shell uses to construct a `NamespaceUnit`,
    /// plus `OrchestratorOutput` with direct worker commands and worker state effects.
    pub fn create_namespace(
        &mut self,
        info: CreateNamespaceInfo,
    ) -> (Result<NamespaceCreationInfo, ClientError>, OrchestratorOutput) {
        if self.namespace_ids.contains(&info.namespace_id) {
            return (
                Err(ClientError::NamespaceAlreadyExists),
                OrchestratorOutput::default(),
            );
        }

        let mut output = OrchestratorOutput::default();

        let segment_id = self.alloc_segment_id();
        let mut network = info.network;
        network.segment_id = Some(segment_id);
        self.namespace_segments
            .insert(info.namespace_id.clone(), segment_id);
        self.namespace_networks
            .insert(info.namespace_id.clone(), network.clone());
        self.namespace_ids.insert(info.namespace_id.clone());

        let ws_effects =
            self.worker_state
                .process(WorkerStateCoreEvent::RegisterNamespaceSegment {
                    namespace_id: info.namespace_id.clone(),
                    segment_id,
                });
        self.route_worker_state_effects(ws_effects, &mut output);

        let registry = self.id_registry_map.get_or_create(&info.namespace_id);

        // Build connected worker summaries and direct CreateNamespace commands.
        let connected_workers: Vec<ConnectedWorkerSummary> = self
            .connected_workers
            .iter()
            .map(|(&wid, winfo)| ConnectedWorkerSummary {
                worker_id: wid,
                proto_worker_id: winfo.proto_worker_id.clone(),
                max_pods: winfo.max_pods,
                default_pool: winfo.default_pool.clone(),
            })
            .collect();

        for summary in &connected_workers {
            output.direct_worker_commands.push(DirectWorkerCommand {
                worker_id: summary.worker_id,
                command: distvirt_worker_protocol::WorkerCommand::CreateNamespace {
                    namespace_id: info.namespace_id.clone(),
                    network: network.clone(),
                },
            });

            let ws_effects =
                self.worker_state
                    .process(WorkerStateCoreEvent::NamespaceAssigned {
                        worker_id: summary.worker_id,
                        namespace_id: info.namespace_id.clone(),
                    });
            self.route_worker_state_effects(ws_effects, &mut output);
        }

        let creation_info = NamespaceCreationInfo {
            network,
            id_registry: registry,
            timer_config: self.timer_config.clone(),
            connected_workers,
        };

        (Ok(creation_info), output)
    }

    /// Destroy a namespace.
    ///
    /// Removes from tracking, unregisters segment. The shell is responsible
    /// for dropping the `NamespaceUnit`.
    pub fn destroy_namespace(
        &mut self,
        namespace_id: &NamespaceId,
    ) -> (Result<(), ClientError>, OrchestratorOutput) {
        let mut output = OrchestratorOutput::default();

        if !self.namespace_ids.remove(namespace_id) {
            return (Err(ClientError::NamespaceNotFound), output);
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
            self.route_worker_state_effects(ws_effects, &mut output);
        }

        let ws_effects =
            self.worker_state
                .process(WorkerStateCoreEvent::UnregisterNamespaceSegment {
                    namespace_id: namespace_id.clone(),
                });
        self.route_worker_state_effects(ws_effects, &mut output);

        (Ok(()), output)
    }

    /// Find a worker with a WireGuard adapter. Used by the shell for `connect_network`.
    pub fn find_wireguard_worker(
        &self,
    ) -> Option<(
        GlobalWorkerId,
        &worker_state::WireguardAdapterInfo,
        &str,
    )> {
        self.worker_state.find_wireguard_worker()
    }

    // =========================================================================
    // Internal routing
    // =========================================================================

    fn route_worker_state_effects(
        &mut self,
        ws_effects: super::types::WorkerStateEffects,
        output: &mut OrchestratorOutput,
    ) {
        for update in ws_effects.scheduler_updates {
            let sched_effects = self.scheduler.process(update);
            self.route_scheduler_effects(sched_effects, output);
        }

        if let Some(cmd) = ws_effects.worker_registry_broadcast {
            output.global_broadcasts.push(cmd);
        }
    }

    fn route_scheduler_effects(
        &mut self,
        sched_effects: SchedulerEffects,
        output: &mut OrchestratorOutput,
    ) {
        // Route scheduling decisions to namespaces (as messages).
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
                    self.route_worker_state_effects(ws_effects, output);
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
                    self.route_worker_state_effects(ws_effects, output);
                    namespace_id.clone()
                }
            };

            output.to_namespaces.push((
                target_ns_id,
                OrchestratorToNamespace::SchedulerDecision(decision),
            ));
        }

        // Broadcast artifact invalidations to ALL namespaces.
        for artifact_port_id in sched_effects.artifact_invalidations {
            let ns_ids: Vec<_> = self.namespace_ids.iter().cloned().collect();
            for ns_id in ns_ids {
                output.to_namespaces.push((
                    ns_id,
                    OrchestratorToNamespace::ArtifactInvalidated {
                        artifact_port_id: ArtifactPortId(artifact_port_id),
                    },
                ));
            }
        }

        // Route DeleteArtifact commands to workers.
        for cmd in sched_effects.delete_commands {
            output.worker_commands.push((
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

    pub fn list_workers(&self) -> Vec<self::worker_state::WorkerQueryInfo> {
        self.worker_state.query_all_workers()
    }

    pub fn get_worker(
        &self,
        worker_id: GlobalWorkerId,
    ) -> Result<self::worker_state::WorkerQueryInfo, ClientError> {
        self.worker_state
            .query_worker(worker_id)
            .ok_or(ClientError::WorkerNotFound)
    }

    pub fn namespace_ids(&self) -> &HashSet<NamespaceId> {
        &self.namespace_ids
    }
}

/// Convert a `SchedulerMessage` (from namespace) into a `SchedulerCoreInput`.
fn scheduler_message_to_input(msg: SchedulerMessage) -> SchedulerCoreInput {
    match msg {
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
    }
}
