//! Pure worker state core — no async, no channels.
//!
//! Extracted from `task/worker_state.rs`. Tracks per-worker state and
//! produces scheduler updates and worker registry broadcasts as effects.

use std::collections::{HashMap, HashSet};

use crate::core::GlobalWorkerId;
use crate::core::scheduler::WorkerCandidate;
use crate::types::{NamespaceId, PressureBands, WorkerPressure, WorkerPsi};

use super::types::{SchedulerCoreInput, WorkerStateCoreEvent, WorkerStateEffects};

// =============================================================================
// Tunnel config (from WorkerReady handshake)
// =============================================================================

/// Tunnel configuration reported by a worker during handshake.
#[derive(Clone, Debug)]
pub struct WorkerTunnelInfo {
    pub listen_port: u16,
    pub public_key: [u8; 32],
}

/// WireGuard adapter info reported by a worker during handshake.
/// Separate from tunnel info — this is for client-facing WireGuard connections.
#[derive(Clone, Debug)]
pub struct WireguardAdapterInfo {
    pub listen_port: u16,
    pub public_key: [u8; 32],
}

// =============================================================================
// Tracked per-worker state (pure — no writer handle)
// =============================================================================

struct TrackedWorkerStateCore {
    pressure_bands: PressureBands,
    pressure: WorkerPressure,
    psi: Option<WorkerPsi>,
    capabilities: distvirt_worker_protocol::WorkerCapabilities,
    conditions: HashMap<String, bool>,
    pod_count: usize,
    tunnel_info: Option<WorkerTunnelInfo>,
    wireguard_info: Option<WireguardAdapterInfo>,
    proto_worker_id: distvirt_worker_protocol::WorkerId,
    segments: HashSet<u16>,
}

impl TrackedWorkerStateCore {
    fn new(
        capabilities: distvirt_worker_protocol::WorkerCapabilities,
        tunnel_info: Option<WorkerTunnelInfo>,
        wireguard_info: Option<WireguardAdapterInfo>,
        proto_worker_id: distvirt_worker_protocol::WorkerId,
    ) -> Self {
        TrackedWorkerStateCore {
            pressure_bands: PressureBands::default(),
            pressure: WorkerPressure {
                compute: 0.0,
                memory: 0.0,
                storage: 0.0,
                network: 0.0,
            },
            psi: None,
            capabilities,
            conditions: HashMap::new(),
            pod_count: 0,
            tunnel_info,
            wireguard_info,
            proto_worker_id,
            segments: HashSet::new(),
        }
    }

    fn to_candidate(&self, worker_id: GlobalWorkerId) -> WorkerCandidate {
        WorkerCandidate {
            worker_id,
            max_pressure_band: self.pressure_bands.max_band(),
            pod_count: self.pod_count,
            draining: self.conditions.get("draining").copied().unwrap_or(false),
            active: true,
        }
    }

    fn recompute_pressure_from_psi(&mut self) {
        if let Some(ref psi) = self.psi {
            let compute = (psi.cpu.some_avg10 as f32 / 100.0).clamp(0.0, 1.0);
            let memory = (psi.memory.some_avg10 as f32 / 100.0).clamp(0.0, 1.0);
            let storage = (psi.io.some_avg10 as f32 / 100.0).clamp(0.0, 1.0);

            self.pressure = WorkerPressure {
                compute,
                memory,
                storage,
                network: 0.0,
            };
            self.pressure_bands = self.pressure.update_bands(&self.pressure_bands);
        }
    }

    fn to_peer_info(&self) -> Option<distvirt_worker_protocol::WorkerPeerInfo> {
        let tunnel = self.tunnel_info.as_ref()?;
        if self.capabilities.public_endpoint.is_empty() {
            return None;
        }
        let endpoint = format!(
            "{}:{}",
            self.capabilities.public_endpoint, tunnel.listen_port
        );
        let segments: Vec<u16> = self.segments.iter().copied().collect();
        Some(distvirt_worker_protocol::WorkerPeerInfo {
            worker_id: self.proto_worker_id.clone(),
            endpoint,
            public_key: tunnel.public_key,
            segments,
        })
    }
}

// =============================================================================
// Worker query info (returned by read-only queries)
// =============================================================================

pub struct WorkerQueryInfo {
    pub worker_id: GlobalWorkerId,
    pub max_pods: u32,
    pub available_memory_mb: u64,
    pub active_pods: u32,
    pub conditions: HashMap<String, bool>,
}

// =============================================================================
// WorkerStateCore
// =============================================================================

pub(crate) struct WorkerStateCore {
    workers: HashMap<GlobalWorkerId, TrackedWorkerStateCore>,
    namespace_segments: HashMap<NamespaceId, u16>,
}

impl WorkerStateCore {
    pub(crate) fn new() -> Self {
        WorkerStateCore {
            workers: HashMap::new(),
            namespace_segments: HashMap::new(),
        }
    }

    /// Process a single event, returning effects (scheduler updates + optional broadcast).
    pub(crate) fn process(&mut self, event: WorkerStateCoreEvent) -> WorkerStateEffects {
        let mut effects = WorkerStateEffects::default();

        match event {
            WorkerStateCoreEvent::PressureUpdate {
                worker_id,
                cpu,
                memory,
                io,
            } => {
                if let Some(state) = self.workers.get_mut(&worker_id) {
                    state.psi = Some(WorkerPsi { cpu, memory, io });
                    state.recompute_pressure_from_psi();
                    let candidate = state.to_candidate(worker_id);
                    effects
                        .scheduler_updates
                        .push(SchedulerCoreInput::WorkerUpdate(worker_id, candidate));
                }
            }
            WorkerStateCoreEvent::PoolCapacityUpdate { worker_id, pools } => {
                if let Some(state) = self.workers.get_mut(&worker_id) {
                    state.capabilities.pools = pools;
                }
            }
            WorkerStateCoreEvent::ConditionUpdate {
                worker_id,
                key,
                active,
                ..
            } => {
                if let Some(state) = self.workers.get_mut(&worker_id) {
                    state.conditions.insert(key, active);
                    let candidate = state.to_candidate(worker_id);
                    effects
                        .scheduler_updates
                        .push(SchedulerCoreInput::WorkerUpdate(worker_id, candidate));
                }
            }
            WorkerStateCoreEvent::Connected {
                worker_id,
                capabilities,
                tunnel_info,
                wireguard_info,
                proto_worker_id,
            } => {
                let state = TrackedWorkerStateCore::new(capabilities, tunnel_info, wireguard_info, proto_worker_id);
                let candidate = state.to_candidate(worker_id);
                self.workers.insert(worker_id, state);
                effects
                    .scheduler_updates
                    .push(SchedulerCoreInput::WorkerUpdate(worker_id, candidate));
                effects.worker_registry_broadcast = Some(self.build_worker_registry_command());
            }
            WorkerStateCoreEvent::Disconnected { worker_id } => {
                self.workers.remove(&worker_id);
                effects
                    .scheduler_updates
                    .push(SchedulerCoreInput::WorkerRemoved(worker_id));
                effects.worker_registry_broadcast = Some(self.build_worker_registry_command());
            }
            WorkerStateCoreEvent::NamespaceAssigned {
                worker_id,
                namespace_id,
            } => {
                if let Some(state) = self.workers.get_mut(&worker_id) {
                    if let Some(&segment) = self.namespace_segments.get(&namespace_id) {
                        if state.segments.insert(segment) {
                            effects.worker_registry_broadcast =
                                Some(self.build_worker_registry_command());
                        }
                    }
                }
            }
            WorkerStateCoreEvent::NamespaceUnassigned {
                worker_id,
                namespace_id,
            } => {
                if let Some(state) = self.workers.get_mut(&worker_id) {
                    if let Some(&segment) = self.namespace_segments.get(&namespace_id) {
                        if state.segments.remove(&segment) {
                            effects.worker_registry_broadcast =
                                Some(self.build_worker_registry_command());
                        }
                    }
                }
            }
            WorkerStateCoreEvent::RegisterNamespaceSegment {
                namespace_id,
                segment_id,
            } => {
                self.namespace_segments.insert(namespace_id, segment_id);
            }
            WorkerStateCoreEvent::UnregisterNamespaceSegment { namespace_id } => {
                self.namespace_segments.remove(&namespace_id);
            }
            WorkerStateCoreEvent::PodCountChange { worker_id, delta } => {
                if let Some(state) = self.workers.get_mut(&worker_id) {
                    state.pod_count = (state.pod_count as i32 + delta).max(0) as usize;
                    state.recompute_pressure_from_psi();
                    let candidate = state.to_candidate(worker_id);
                    effects
                        .scheduler_updates
                        .push(SchedulerCoreInput::WorkerUpdate(worker_id, candidate));
                }
            }
        }

        effects
    }

    /// Find the first worker with tunnel info. Returns (worker_id, tunnel_info, public_endpoint).
    pub(crate) fn find_tunnel_worker(&self) -> Option<(GlobalWorkerId, &WorkerTunnelInfo, &str)> {
        for (&wid, state) in &self.workers {
            if let Some(ref tunnel) = state.tunnel_info {
                if !state.capabilities.public_endpoint.is_empty() {
                    return Some((wid, tunnel, &state.capabilities.public_endpoint));
                }
            }
        }
        None
    }

    /// Find the first worker with a WireGuard adapter. Returns (worker_id, wireguard_info, public_endpoint).
    pub(crate) fn find_wireguard_worker(&self) -> Option<(GlobalWorkerId, &WireguardAdapterInfo, &str)> {
        for (&wid, state) in &self.workers {
            if let Some(ref wg) = state.wireguard_info {
                if !state.capabilities.public_endpoint.is_empty() {
                    return Some((wid, wg, &state.capabilities.public_endpoint));
                }
            }
        }
        None
    }

    pub(crate) fn query_worker(&self, id: GlobalWorkerId) -> Option<WorkerQueryInfo> {
        let state = self.workers.get(&id)?;
        Some(WorkerQueryInfo {
            worker_id: id,
            max_pods: state.capabilities.max_pods,
            available_memory_mb: state.capabilities.available_memory_mb,
            active_pods: state.pod_count as u32,
            conditions: state.conditions.clone(),
        })
    }

    pub(crate) fn query_all_workers(&self) -> Vec<WorkerQueryInfo> {
        self.workers
            .iter()
            .map(|(&id, state)| WorkerQueryInfo {
                worker_id: id,
                max_pods: state.capabilities.max_pods,
                available_memory_mb: state.capabilities.available_memory_mb,
                active_pods: state.pod_count as u32,
                conditions: state.conditions.clone(),
            })
            .collect()
    }

    fn build_worker_registry(&self) -> Vec<distvirt_worker_protocol::WorkerPeerInfo> {
        self.workers
            .values()
            .filter_map(|state| state.to_peer_info())
            .collect()
    }

    fn build_worker_registry_command(&self) -> distvirt_worker_protocol::WorkerCommand {
        let registry = self.build_worker_registry();
        distvirt_worker_protocol::WorkerCommand::WorkerRegistrySync { workers: registry }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ws_connected(id: u64) -> WorkerStateCoreEvent {
        WorkerStateCoreEvent::Connected {
            worker_id: GlobalWorkerId::from(id),
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
            wireguard_info: None,
            proto_worker_id: distvirt_worker_protocol::WorkerId::from(id),
        }
    }

    #[test]
    fn connected_produces_scheduler_update() {
        let mut ws = WorkerStateCore::new();
        let effects = ws.process(ws_connected(1));
        assert_eq!(effects.scheduler_updates.len(), 1);
        assert!(matches!(
            &effects.scheduler_updates[0],
            SchedulerCoreInput::WorkerUpdate(id, _) if *id == GlobalWorkerId::from(1)
        ));
    }

    #[test]
    fn disconnected_produces_scheduler_removed() {
        let mut ws = WorkerStateCore::new();
        ws.process(ws_connected(1));
        let effects = ws.process(WorkerStateCoreEvent::Disconnected {
            worker_id: GlobalWorkerId::from(1),
        });
        assert_eq!(effects.scheduler_updates.len(), 1);
        assert!(matches!(
            &effects.scheduler_updates[0],
            SchedulerCoreInput::WorkerRemoved(id) if *id == GlobalWorkerId::from(1)
        ));
    }

    #[test]
    fn connected_broadcasts_worker_registry() {
        let mut ws = WorkerStateCore::new();
        let effects = ws.process(ws_connected(1));
        assert!(effects.worker_registry_broadcast.is_some());
    }
}
