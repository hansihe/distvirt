use std::collections::{HashMap, HashSet};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::scheduler::WorkerCandidate;
use super::{GlobalWorkerId, SchedulerInput, WorkerStateEvent, WorkerWriterHandle};
use crate::types::{NamespaceId, PressureBands, WorkerPressure, WorkerPsi};

// =============================================================================
// Tunnel config (from WorkerReady handshake)
// =============================================================================

/// Tunnel configuration reported by a worker during handshake.
#[derive(Clone, Debug)]
pub(crate) struct WorkerTunnelInfo {
    pub listen_port: u16,
    pub public_key: [u8; 32],
}

// =============================================================================
// Tracked per-worker state
// =============================================================================

struct TrackedWorkerState {
    pressure_bands: PressureBands,
    pressure: WorkerPressure,
    psi: Option<WorkerPsi>,
    capabilities: distvirt_worker_protocol::WorkerCapabilities,
    conditions: HashMap<String, bool>,
    pod_count: usize,
    /// Tunnel info from handshake (None if worker doesn't support tunnels).
    tunnel_info: Option<WorkerTunnelInfo>,
    /// Protocol worker ID (for WorkerPeerInfo).
    proto_worker_id: distvirt_worker_protocol::WorkerId,
    /// Writer handle for sending commands to this worker.
    writer: WorkerWriterHandle,
    /// Namespace segments this worker participates in.
    segments: HashSet<u16>,
}

impl TrackedWorkerState {
    fn new(
        capabilities: distvirt_worker_protocol::WorkerCapabilities,
        tunnel_info: Option<WorkerTunnelInfo>,
        proto_worker_id: distvirt_worker_protocol::WorkerId,
        writer: WorkerWriterHandle,
    ) -> Self {
        TrackedWorkerState {
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
            proto_worker_id,
            writer,
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

    /// Build a WorkerPeerInfo if this worker is tunnel-capable.
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
// Worker state tracker
// =============================================================================

struct WorkerStateTracker {
    workers: HashMap<GlobalWorkerId, TrackedWorkerState>,
    /// Namespace → segment_id mapping (set by shell).
    namespace_segments: HashMap<NamespaceId, u16>,
    scheduler_tx: mpsc::Sender<SchedulerInput>,
    rx: mpsc::Receiver<WorkerStateEvent>,
}

impl WorkerStateTracker {
    async fn run(mut self) {
        while let Some(event) = self.rx.recv().await {
            match event {
                WorkerStateEvent::PressureUpdate {
                    worker_id,
                    cpu,
                    memory,
                    io,
                } => {
                    if let Some(state) = self.workers.get_mut(&worker_id) {
                        state.psi = Some(WorkerPsi { cpu, memory, io });
                        state.recompute_pressure_from_psi();
                        let candidate = state.to_candidate(worker_id);
                        let _ = self
                            .scheduler_tx
                            .send(SchedulerInput::WorkerUpdate(worker_id, candidate))
                            .await;
                    }
                }
                WorkerStateEvent::PoolCapacityUpdate { worker_id, pools } => {
                    if let Some(state) = self.workers.get_mut(&worker_id) {
                        state.capabilities.pools = pools;
                    }
                }
                WorkerStateEvent::ConditionUpdate {
                    worker_id,
                    key,
                    active,
                    ..
                } => {
                    if let Some(state) = self.workers.get_mut(&worker_id) {
                        state.conditions.insert(key, active);
                        let candidate = state.to_candidate(worker_id);
                        let _ = self
                            .scheduler_tx
                            .send(SchedulerInput::WorkerUpdate(worker_id, candidate))
                            .await;
                    }
                }
                WorkerStateEvent::Connected {
                    worker_id,
                    capabilities,
                    tunnel_info,
                    proto_worker_id,
                    writer,
                } => {
                    let state = TrackedWorkerState::new(
                        capabilities,
                        tunnel_info,
                        proto_worker_id,
                        writer,
                    );
                    let candidate = state.to_candidate(worker_id);
                    self.workers.insert(worker_id, state);
                    let _ = self
                        .scheduler_tx
                        .send(SchedulerInput::WorkerUpdate(worker_id, candidate))
                        .await;
                    self.broadcast_worker_registry().await;
                }
                WorkerStateEvent::Disconnected { worker_id } => {
                    self.workers.remove(&worker_id);
                    let _ = self
                        .scheduler_tx
                        .send(SchedulerInput::WorkerRemoved(worker_id))
                        .await;
                    self.broadcast_worker_registry().await;
                }
                WorkerStateEvent::NamespaceAssigned {
                    worker_id,
                    namespace_id,
                } => {
                    if let Some(state) = self.workers.get_mut(&worker_id) {
                        if let Some(&segment) = self.namespace_segments.get(&namespace_id) {
                            if state.segments.insert(segment) {
                                self.broadcast_worker_registry().await;
                            }
                        }
                    }
                }
                WorkerStateEvent::NamespaceUnassigned {
                    worker_id,
                    namespace_id,
                } => {
                    if let Some(state) = self.workers.get_mut(&worker_id) {
                        if let Some(&segment) = self.namespace_segments.get(&namespace_id) {
                            if state.segments.remove(&segment) {
                                self.broadcast_worker_registry().await;
                            }
                        }
                    }
                }
                WorkerStateEvent::RegisterNamespaceSegment {
                    namespace_id,
                    segment_id,
                } => {
                    self.namespace_segments.insert(namespace_id, segment_id);
                }
                WorkerStateEvent::UnregisterNamespaceSegment { namespace_id } => {
                    self.namespace_segments.remove(&namespace_id);
                }
            }
        }
    }

    /// Build the worker registry and broadcast to all connected workers.
    fn build_worker_registry(&self) -> Vec<distvirt_worker_protocol::WorkerPeerInfo> {
        self.workers
            .values()
            .filter_map(|state| state.to_peer_info())
            .collect()
    }

    async fn broadcast_worker_registry(&self) {
        let registry = self.build_worker_registry();
        let cmd = distvirt_worker_protocol::WorkerCommand::WorkerRegistrySync {
            workers: registry,
        };
        for state in self.workers.values() {
            state.writer.send(cmd.clone()).await;
        }
    }
}

/// Spawn the worker state tracker. Returns (event sender, join handle).
pub(crate) fn spawn(
    scheduler_tx: mpsc::Sender<SchedulerInput>,
) -> (mpsc::Sender<WorkerStateEvent>, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(256);

    let tracker = WorkerStateTracker {
        workers: HashMap::new(),
        namespace_segments: HashMap::new(),
        scheduler_tx,
        rx,
    };

    let handle = tokio::spawn(tracker.run());
    (tx, handle)
}
