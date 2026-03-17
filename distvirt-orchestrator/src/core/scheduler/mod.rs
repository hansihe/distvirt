//! Pure scheduler core — no async, no channels.
//!
//! Extracted from `task/scheduler/mod.rs`. All scheduling logic lives here.
//! The async wrapper (in task/ or shell_new/) handles channel I/O.

use std::collections::HashMap;

use crate::core::scheduler::placement_table::PlacementTable;
use crate::core::{GlobalWorkerId, SchedulerDecision};
use crate::sm_new::PodId;
use crate::types::{NamespaceId, PressureBand};

use super::types::SchedulerCoreInput;

mod placement_table;

/// Snapshot of a single worker's scheduling-relevant state.
/// Passed in by the shell — not owned by the adapter.
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
    Ready {
        pool_id: distvirt_worker_protocol::PoolId,
        size_bytes: u64,
    },
}

/// Composite key — PodId is per-Router, not globally unique.
type PodKey = (NamespaceId, PodId);

struct PendingEntry {
    proto_resume_artifact: Option<distvirt_worker_protocol::ArtifactId>,
}

struct GrantedEntry {
    worker_id: GlobalWorkerId,
}

pub(crate) struct SchedulerCore {
    pending: HashMap<PodKey, PendingEntry>,
    granted: HashMap<PodKey, GrantedEntry>,
    workers: HashMap<GlobalWorkerId, WorkerCandidate>,
    placements: PlacementTable,
}

impl SchedulerCore {
    pub(crate) fn new() -> Self {
        SchedulerCore {
            pending: HashMap::new(),
            granted: HashMap::new(),
            workers: HashMap::new(),
            placements: PlacementTable::default(),
        }
    }

    /// Process a single scheduler input, returning any decisions produced.
    pub(crate) fn process(&mut self, input: SchedulerCoreInput) -> Vec<SchedulerDecision> {
        match input {
            SchedulerCoreInput::RequestLease {
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
                    self.granted.insert(key, GrantedEntry { worker_id });
                    vec![SchedulerDecision::Grant {
                        namespace_id,
                        pod_id,
                        worker_id,
                    }]
                } else {
                    self.pending.insert(
                        key,
                        PendingEntry {
                            proto_resume_artifact,
                        },
                    );
                    vec![]
                }
            }
            SchedulerCoreInput::DropRequest {
                namespace_id,
                pod_id,
            } => {
                let key = (namespace_id.clone(), pod_id);
                if let Some(entry) = self.granted.remove(&key) {
                    vec![SchedulerDecision::Revoke {
                        namespace_id,
                        pod_id,
                        worker_id: entry.worker_id,
                    }]
                } else {
                    self.pending.remove(&key);
                    vec![]
                }
            }
            SchedulerCoreInput::WorkerUpdate(worker_id, candidate) => {
                self.workers.insert(worker_id, candidate);
                self.retry_pending()
            }
            SchedulerCoreInput::WorkerRemoved(worker_id) => {
                self.workers.remove(&worker_id);
                self.placements.remove_worker(worker_id);
                vec![]
            }
            SchedulerCoreInput::ArtifactEvent { worker_id, event } => {
                self.placements.apply_event(worker_id, event);
                vec![]
            }
        }
    }

    fn retry_pending(&mut self) -> Vec<SchedulerDecision> {
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

        let mut decisions = Vec::new();
        for key in newly_granted {
            if let Some(entry) = self.pending.remove(&key) {
                if let Some(worker_id) = select_worker(
                    self.workers.values(),
                    entry.proto_resume_artifact.as_ref(),
                    &self.placements,
                ) {
                    decisions.push(SchedulerDecision::Grant {
                        namespace_id: key.0.clone(),
                        pod_id: key.1,
                        worker_id,
                    });
                    self.granted.insert(key, GrantedEntry { worker_id });
                } else {
                    self.pending.insert(key, entry);
                }
            }
        }
        decisions
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
        if workers.is_empty() {
            None
        } else {
            Some(workers)
        }
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

#[cfg(test)]
mod tests;
