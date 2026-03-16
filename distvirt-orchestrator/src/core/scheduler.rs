//! Pure scheduler core — no async, no channels.
//!
//! Extracted from `task/scheduler/mod.rs`. All scheduling logic lives here.
//! The async wrapper (in task/ or shell_new/) handles channel I/O.

use std::collections::HashMap;

use crate::sm_new::PodId;
use crate::task::scheduler::{select_worker, PlacementTable, WorkerCandidate};
use crate::task::{GlobalWorkerId, SchedulerDecision};
use crate::types::NamespaceId;

use super::types::SchedulerCoreInput;

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
                    self.pending
                        .insert(key, PendingEntry { proto_resume_artifact });
                    vec![]
                }
            }
            SchedulerCoreInput::DropRequest {
                namespace_id,
                pod_id,
            } => {
                let key = (namespace_id.clone(), pod_id);
                if let Some(_entry) = self.granted.remove(&key) {
                    vec![SchedulerDecision::Revoke {
                        namespace_id,
                        pod_id,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::scheduler::WorkerCandidate;
    use crate::types::PressureBand;

    fn ns(name: &str) -> NamespaceId {
        NamespaceId::from(name)
    }

    #[test]
    fn grant_immediate_when_worker_available() {
        let mut sched = SchedulerCore::new();

        // Add a worker.
        let decisions = sched.process(SchedulerCoreInput::WorkerUpdate(
            GlobalWorkerId::test(1),
            WorkerCandidate {
                worker_id: GlobalWorkerId::test(1),
                max_pressure_band: PressureBand::Normal,
                pod_count: 0,
                draining: false,
                active: true,
            },
        ));
        assert!(decisions.is_empty());

        // Request lease — should be granted immediately.
        let decisions = sched.process(SchedulerCoreInput::RequestLease {
            namespace_id: ns("test"),
            pod_id: PodId::test(1),
            proto_resume_artifact: None,
        });
        assert_eq!(decisions.len(), 1);
        assert!(matches!(
            &decisions[0],
            SchedulerDecision::Grant { worker_id, .. } if *worker_id == GlobalWorkerId::test(1)
        ));
    }

    #[test]
    fn pend_when_no_workers() {
        let mut sched = SchedulerCore::new();

        let decisions = sched.process(SchedulerCoreInput::RequestLease {
            namespace_id: ns("test"),
            pod_id: PodId::test(1),
            proto_resume_artifact: None,
        });
        assert!(decisions.is_empty(), "should pend when no workers");
    }

    #[test]
    fn retry_pending_on_worker_update() {
        let mut sched = SchedulerCore::new();

        // Request lease with no workers — pends.
        sched.process(SchedulerCoreInput::RequestLease {
            namespace_id: ns("test"),
            pod_id: PodId::test(1),
            proto_resume_artifact: None,
        });

        // Add worker — should grant the pending request.
        let decisions = sched.process(SchedulerCoreInput::WorkerUpdate(
            GlobalWorkerId::test(1),
            WorkerCandidate {
                worker_id: GlobalWorkerId::test(1),
                max_pressure_band: PressureBand::Normal,
                pod_count: 0,
                draining: false,
                active: true,
            },
        ));
        assert_eq!(decisions.len(), 1);
        assert!(matches!(&decisions[0], SchedulerDecision::Grant { .. }));
    }

    #[test]
    fn drop_request_revokes_granted() {
        let mut sched = SchedulerCore::new();

        sched.process(SchedulerCoreInput::WorkerUpdate(
            GlobalWorkerId::test(1),
            WorkerCandidate {
                worker_id: GlobalWorkerId::test(1),
                max_pressure_band: PressureBand::Normal,
                pod_count: 0,
                draining: false,
                active: true,
            },
        ));

        sched.process(SchedulerCoreInput::RequestLease {
            namespace_id: ns("test"),
            pod_id: PodId::test(1),
            proto_resume_artifact: None,
        });

        let decisions = sched.process(SchedulerCoreInput::DropRequest {
            namespace_id: ns("test"),
            pod_id: PodId::test(1),
        });
        assert_eq!(decisions.len(), 1);
        assert!(matches!(&decisions[0], SchedulerDecision::Revoke { .. }));
    }
}
