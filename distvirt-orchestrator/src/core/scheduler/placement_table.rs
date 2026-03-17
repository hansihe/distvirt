use std::collections::{HashMap, HashSet};

use crate::core::{ArtifactPlacementEvent, GlobalWorkerId, scheduler::ArtifactStatus};

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
            ArtifactPlacementEvent::WriteCommitted {
                artifact_id,
                pool_id,
                size_bytes,
            } => {
                self.entries.entry(artifact_id).or_default().insert(
                    worker_id,
                    ArtifactStatus::Ready {
                        pool_id,
                        size_bytes,
                    },
                );
            }
            ArtifactPlacementEvent::TransferReceived {
                artifact_id,
                pool_id,
                size_bytes,
            } => {
                self.entries.entry(artifact_id).or_default().insert(
                    worker_id,
                    ArtifactStatus::Ready {
                        pool_id,
                        size_bytes,
                    },
                );
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
