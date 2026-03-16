use std::collections::{HashMap, HashSet};

use tokio::sync::mpsc;

use crate::sm_new::PodId;
use crate::types::{NamespaceId, PressureBand};

use super::{ArtifactPlacementEvent, GlobalWorkerId, SchedulerDecision, SchedulerInput};

#[cfg(test)]
mod tests;

// =============================================================================
// Pure scheduling logic (no async, no channels)
// =============================================================================

/// Snapshot of a single worker's scheduling-relevant state.
/// Passed in by the shell — not owned by the scheduler.
pub(crate) struct WorkerCandidate {
    pub worker_id: GlobalWorkerId,
    pub max_pressure_band: PressureBand,
    pub pod_count: usize,
    pub draining: bool,
    pub active: bool,
}

/// Status of an artifact on a specific worker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ArtifactStatus {
    Writing,
    Ready { pool_id: distvirt_worker_protocol::PoolId, size_bytes: u64 },
}

/// Artifact placement table: tracks which artifacts exist on which workers.
#[derive(Default)]
pub(crate) struct PlacementTable {
    /// artifact_id → set of workers that have it, with status.
    entries: HashMap<distvirt_worker_protocol::ArtifactId, HashMap<GlobalWorkerId, ArtifactStatus>>,
}

impl PlacementTable {
    pub(crate) fn apply_event(&mut self, worker_id: GlobalWorkerId, event: ArtifactPlacementEvent) {
        match event {
            ArtifactPlacementEvent::WriteStarted { artifact_id, .. } => {
                self.entries
                    .entry(artifact_id)
                    .or_default()
                    .insert(worker_id, ArtifactStatus::Writing);
            }
            ArtifactPlacementEvent::WriteCommitted { artifact_id, pool_id, size_bytes } => {
                self.entries
                    .entry(artifact_id)
                    .or_default()
                    .insert(worker_id, ArtifactStatus::Ready { pool_id, size_bytes });
            }
            ArtifactPlacementEvent::TransferReceived { artifact_id, pool_id, size_bytes } => {
                self.entries
                    .entry(artifact_id)
                    .or_default()
                    .insert(worker_id, ArtifactStatus::Ready { pool_id, size_bytes });
            }
            ArtifactPlacementEvent::TransferFailed { artifact_id } => {
                // Remove this worker's entry for the artifact (transfer didn't produce a usable copy).
                if let Some(workers) = self.entries.get_mut(&artifact_id) {
                    workers.remove(&worker_id);
                    if workers.is_empty() {
                        self.entries.remove(&artifact_id);
                    }
                }
            }
        }
    }

    pub(crate) fn remove_worker(&mut self, worker_id: GlobalWorkerId) {
        self.entries.retain(|_, workers| {
            workers.remove(&worker_id);
            !workers.is_empty()
        });
    }

    /// Returns the set of workers that have a Ready copy of the given artifact.
    pub(crate) fn workers_with_artifact(
        &self,
        artifact_id: &distvirt_worker_protocol::ArtifactId,
    ) -> HashSet<GlobalWorkerId> {
        self.entries
            .get(artifact_id)
            .map(|workers| {
                workers
                    .iter()
                    .filter(|(_, status)| matches!(status, ArtifactStatus::Ready { .. }))
                    .map(|(&wid, _)| wid)
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// Select the best worker from candidates for a pod.
/// Hard filter: active, not draining, pressure below High.
/// Soft preference: artifact affinity (if resume), then lowest pressure band,
/// then lowest pod count, then lowest worker ID.
///
/// `resume_artifact` is the protocol-level artifact ID, already resolved at the
/// namespace boundary. No type conversion happens here.
pub(crate) fn select_worker<'a>(
    candidates: impl IntoIterator<Item = &'a WorkerCandidate>,
    resume_artifact: Option<&distvirt_worker_protocol::ArtifactId>,
    placements: &PlacementTable,
) -> Option<GlobalWorkerId> {
    let affinity_workers = resume_artifact.and_then(|art_id| {
        let workers = placements.workers_with_artifact(art_id);
        if workers.is_empty() { None } else { Some(workers) }
    });

    candidates
        .into_iter()
        .filter(|c| c.active && !c.draining && c.max_pressure_band < PressureBand::High)
        .min_by_key(|c| {
            // no_affinity = false (0) sorts before true (1), so workers WITH the artifact sort first.
            let no_affinity = affinity_workers
                .as_ref()
                .map(|ws| !ws.contains(&c.worker_id))
                .unwrap_or(false);
            (no_affinity, c.max_pressure_band, c.pod_count, c.worker_id)
        })
        .map(|c| c.worker_id)
}

// =============================================================================
// Scheduler task (async)
// =============================================================================

/// Composite key — PodId is per-Router, not globally unique.
type PodKey = (NamespaceId, PodId);

struct PendingEntry {
    proto_resume_artifact: Option<distvirt_worker_protocol::ArtifactId>,
}

struct GrantedEntry {
    worker_id: GlobalWorkerId,
}

struct SchedulerTask {
    /// Pods waiting for a worker.
    pending: HashMap<PodKey, PendingEntry>,
    /// Pods already granted a worker.
    granted: HashMap<PodKey, GrantedEntry>,
    /// Current worker state.
    workers: HashMap<GlobalWorkerId, WorkerCandidate>,
    /// Artifact placement tracking.
    placements: PlacementTable,
    /// Registered namespace reply channels.
    namespaces: HashMap<NamespaceId, mpsc::Sender<SchedulerDecision>>,
    /// Input channel.
    rx: mpsc::Receiver<SchedulerInput>,
}

impl SchedulerTask {
    async fn run(mut self) {
        while let Some(msg) = self.rx.recv().await {
            match msg {
                SchedulerInput::RegisterNamespace { namespace_id, reply_tx } => {
                    self.namespaces.insert(namespace_id, reply_tx);
                }
                SchedulerInput::UnregisterNamespace { namespace_id } => {
                    self.namespaces.remove(&namespace_id);
                }
                SchedulerInput::RequestLease {
                    namespace_id,
                    pod_id,
                    proto_resume_artifact,
                } => {
                    let key = (namespace_id.clone(), pod_id);
                    if let Some(worker_id) = select_worker(
                        self.workers.values(),
                        proto_resume_artifact.as_ref(),
                        &self.placements,
                    ) {
                        if let Some(reply_tx) = self.namespaces.get(&namespace_id) {
                            let _ = reply_tx
                                .send(SchedulerDecision::Grant {
                                    namespace_id: namespace_id.clone(),
                                    pod_id: key.1,
                                    worker_id,
                                })
                                .await;
                        }
                        self.granted.insert(
                            key,
                            GrantedEntry {
                                worker_id,
                            },
                        );
                    } else {
                        self.pending.insert(key, PendingEntry { proto_resume_artifact });
                    }
                }
                SchedulerInput::DropRequest {
                    namespace_id,
                    pod_id,
                } => {
                    let key = (namespace_id.clone(), pod_id);
                    if let Some(entry) = self.granted.remove(&key) {
                        if let Some(reply_tx) = self.namespaces.get(&namespace_id) {
                            let _ = reply_tx
                                .send(SchedulerDecision::Revoke { namespace_id, pod_id, worker_id: entry.worker_id })
                                .await;
                        }
                    } else {
                        self.pending.remove(&key);
                    }
                }
                SchedulerInput::WorkerUpdate(worker_id, candidate) => {
                    self.workers.insert(worker_id, candidate);
                    self.retry_pending().await;
                }
                SchedulerInput::WorkerRemoved(worker_id) => {
                    self.workers.remove(&worker_id);
                    self.placements.remove_worker(worker_id);
                }
                SchedulerInput::ArtifactEvent { worker_id, event } => {
                    self.placements.apply_event(worker_id, event);
                }
            }
        }
    }

    async fn retry_pending(&mut self) {
        let newly_granted: Vec<PodKey> = self
            .pending
            .iter()
            .filter_map(|(key, entry)| {
                select_worker(
                    self.workers.values(),
                    entry.proto_resume_artifact.as_ref(),
                    &self.placements,
                )
                .map(|_| key.clone())
            })
            .collect();

        for key in newly_granted {
            if let Some(entry) = self.pending.remove(&key) {
                if let Some(worker_id) = select_worker(
                    self.workers.values(),
                    entry.proto_resume_artifact.as_ref(),
                    &self.placements,
                ) {
                    if let Some(reply_tx) = self.namespaces.get(&key.0) {
                        let _ = reply_tx
                            .send(SchedulerDecision::Grant {
                                namespace_id: key.0.clone(),
                                pod_id: key.1,
                                worker_id,
                            })
                            .await;
                    }
                    self.granted.insert(
                        key,
                        GrantedEntry {
                            worker_id,
                        },
                    );
                } else {
                    // Worker state changed between collection and grant, re-pend
                    self.pending.insert(key, entry);
                }
            }
        }
    }
}

pub(crate) fn spawn(rx: mpsc::Receiver<SchedulerInput>) -> tokio::task::JoinHandle<()> {
    let task = SchedulerTask {
        pending: HashMap::new(),
        granted: HashMap::new(),
        workers: HashMap::new(),
        placements: PlacementTable::default(),
        namespaces: HashMap::new(),
        rx,
    };
    tokio::spawn(task.run())
}
