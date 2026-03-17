//! Pure scheduler core — no async, no channels.
//!
//! Extracted from `task/scheduler/mod.rs`. All scheduling logic lives here.
//! The async wrapper (in task/ or shell_new/) handles channel I/O.

use std::collections::HashMap;

use crate::core::scheduler::placement_table::PlacementTable;
use crate::core::{GlobalWorkerId, SchedulerDecision};
use crate::sm::PodId;
use crate::types::{NamespaceId, PressureBand};

use super::types::SchedulerCoreInput;

mod placement_table;

/// Snapshot of a single worker's scheduling-relevant state.
/// Passed in by the shell — not owned by the adapter.
pub struct WorkerCandidate {
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

/// Output effects from scheduler processing.
pub(crate) struct SchedulerEffects {
    pub decisions: Vec<SchedulerDecision>,
    /// Artifact port IDs that have become unreachable (broadcast to all namespaces).
    pub artifact_invalidations: Vec<u64>,
    /// DeleteArtifact commands to send to specific workers.
    pub delete_commands: Vec<DeleteArtifactCommand>,
}

impl SchedulerEffects {
    fn new() -> Self {
        SchedulerEffects {
            decisions: Vec::new(),
            artifact_invalidations: Vec::new(),
            delete_commands: Vec::new(),
        }
    }

    fn from_decisions(decisions: Vec<SchedulerDecision>) -> Self {
        SchedulerEffects {
            decisions,
            artifact_invalidations: Vec::new(),
            delete_commands: Vec::new(),
        }
    }
}

/// Command to delete an artifact from a worker's storage pool.
pub(crate) struct DeleteArtifactCommand {
    pub worker_id: GlobalWorkerId,
    pub artifact_id: distvirt_worker_protocol::ArtifactId,
    pub pool_id: distvirt_worker_protocol::PoolId,
}

pub(crate) struct SchedulerCore {
    pending: HashMap<PodKey, PendingEntry>,
    granted: HashMap<PodKey, GrantedEntry>,
    workers: HashMap<GlobalWorkerId, WorkerCandidate>,
    placements: PlacementTable,
    /// Active artifact references: proto_artifact_id → namespace that holds the reference.
    /// An artifact is "referenced" when a workload has an edge to its port.
    artifact_refs: HashMap<distvirt_worker_protocol::ArtifactId, NamespaceId>,
}

impl SchedulerCore {
    pub(crate) fn new() -> Self {
        SchedulerCore {
            pending: HashMap::new(),
            granted: HashMap::new(),
            workers: HashMap::new(),
            placements: PlacementTable::default(),
            artifact_refs: HashMap::new(),
        }
    }

    /// Process a single scheduler input, returning effects.
    pub(crate) fn process(&mut self, input: SchedulerCoreInput) -> SchedulerEffects {
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
                    SchedulerEffects::from_decisions(vec![SchedulerDecision::Grant {
                        namespace_id,
                        pod_id,
                        worker_id,
                    }])
                } else {
                    self.pending.insert(
                        key,
                        PendingEntry {
                            proto_resume_artifact,
                        },
                    );
                    SchedulerEffects::new()
                }
            }
            SchedulerCoreInput::DropRequest {
                namespace_id,
                pod_id,
            } => {
                let key = (namespace_id.clone(), pod_id);
                if let Some(entry) = self.granted.remove(&key) {
                    SchedulerEffects::from_decisions(vec![SchedulerDecision::Revoke {
                        namespace_id,
                        pod_id,
                        worker_id: entry.worker_id,
                    }])
                } else {
                    self.pending.remove(&key);
                    SchedulerEffects::new()
                }
            }
            SchedulerCoreInput::WorkerUpdate(worker_id, candidate) => {
                self.workers.insert(worker_id, candidate);
                SchedulerEffects::from_decisions(self.retry_pending())
            }
            SchedulerCoreInput::WorkerRemoved(worker_id) => {
                self.workers.remove(&worker_id);
                self.placements.remove_worker(worker_id);

                // Check if any referenced artifacts became unreachable.
                let mut effects = SchedulerEffects::new();
                let unreachable: Vec<distvirt_worker_protocol::ArtifactId> = self
                    .artifact_refs
                    .keys()
                    .filter(|art_id| self.placements.workers_with_artifact(art_id).is_empty())
                    .cloned()
                    .collect();
                for art_id in unreachable {
                    self.artifact_refs.remove(&art_id);
                    // Parse back to u64 for the artifact port ID.
                    if let Ok(port_id) = art_id.0.parse::<u64>() {
                        effects.artifact_invalidations.push(port_id);
                    }
                }

                effects
            }
            SchedulerCoreInput::ArtifactEvent { worker_id, event } => {
                self.placements.apply_event(worker_id, event);
                SchedulerEffects::new()
            }
            SchedulerCoreInput::ArtifactReferenced {
                proto_artifact_id,
                namespace_id,
            } => {
                self.artifact_refs
                    .insert(proto_artifact_id, namespace_id);
                SchedulerEffects::new()
            }
            SchedulerCoreInput::ArtifactReleased {
                proto_artifact_id,
                namespace_id: _,
            } => {
                self.artifact_refs.remove(&proto_artifact_id);

                // Find a worker that has this artifact and send DeleteArtifact.
                let mut effects = SchedulerEffects::new();
                if let Some((&worker_id, pool_id)) =
                    self.placements.any_worker_with_artifact(&proto_artifact_id)
                {
                    effects.delete_commands.push(DeleteArtifactCommand {
                        worker_id,
                        artifact_id: proto_artifact_id.clone(),
                        pool_id,
                    });
                }
                self.placements.remove_artifact(&proto_artifact_id);

                effects
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
