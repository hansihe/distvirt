use std::collections::{HashMap, HashSet};

use distvirt_sm_router::IncrementalAggregator;

use crate::sm::{ArtifactPortId, ArtifactPortInput, DRouter, WorkloadId};

/// Delta produced when a workload adds or removes its reference to an artifact port.
/// The port ID comes from the drain call context, not the aggregator.
#[derive(Clone, Debug, PartialEq)]
pub enum ArtifactRefDelta {
    /// A workload now references this artifact (wants it kept alive).
    Referenced { workload_id: WorkloadId },
    /// A workload released its reference to this artifact.
    Released { workload_id: WorkloadId },
}

/// Action returned by finalize — caller (namespace) translates these into scheduler messages.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ArtifactAction {
    Referenced {
        port_id: ArtifactPortId,
    },
    Released {
        port_id: ArtifactPortId,
    },
}

pub(crate) struct ArtifactAdapter {
    /// Artifact ports that have been created but not yet confirmed as referenced.
    /// Used to detect orphaned ports after propagation settles.
    pending_ports: HashSet<ArtifactPortId>,
    /// Accumulates deduped actions across reconcile iterations (last per port wins).
    settled: HashMap<ArtifactPortId, ArtifactAction>,
}

impl ArtifactAdapter {
    pub(crate) fn new() -> Self {
        ArtifactAdapter {
            pending_ports: HashSet::new(),
            settled: HashMap::new(),
        }
    }

    /// Register a newly created artifact port as pending.
    /// Called when PodSuspended creates the port — before propagation has
    /// a chance to produce a reference delta.
    pub(crate) fn register_pending(&mut self, port_id: ArtifactPortId) {
        self.pending_ports.insert(port_id);
    }

    /// Drain artifact port inputs from the router, set return edges, and
    /// accumulate deduped actions (last per port wins).
    ///
    /// Returns `mutated_router` — `true` when the adapter wrote back into
    /// the router (validity edges / destroy).
    pub(crate) fn reconcile(&mut self, router: &mut DRouter) -> bool {
        let inputs = router.drain_artifact_inputs();

        if inputs.is_empty() {
            return false;
        }

        for (port_id, port_input) in inputs {
            let delta = match port_input {
                ArtifactPortInput::RefsInput(d) => d,
            };
            match delta {
                ArtifactRefDelta::Referenced { workload_id } => {
                    // Set return edge: artifact → workload (confirms validity).
                    router.set_artifact_validity_edges(port_id, vec![workload_id]);
                    router.set_artifact_valid(port_id, true);
                    self.settled.insert(port_id, ArtifactAction::Referenced { port_id });
                }
                ArtifactRefDelta::Released { workload_id: _ } => {
                    // Destroy the port — no more references.
                    router.destroy_artifact(port_id);
                    self.settled.insert(port_id, ArtifactAction::Released { port_id });
                }
            }
        }

        true
    }

    /// Drain accumulated actions, clean up orphaned ports, and return
    /// the final list of actions to emit as scheduler messages.
    ///
    /// Call this once after the reconcile loop has stabilized.
    pub(crate) fn finalize(&mut self, router: &mut DRouter) -> Vec<ArtifactAction> {
        let settled = std::mem::take(&mut self.settled);

        let mut final_actions = Vec::with_capacity(settled.len());

        for (port_id, action) in &settled {
            self.pending_ports.remove(port_id);
            final_actions.push(action.clone());
        }

        // Clean up orphaned artifact ports: created but nobody ever referenced them.
        // This happens when PodSuspended arrives but the workload rejects the
        // artifact (e.g. spec changed during suspend).
        let orphans: Vec<ArtifactPortId> = self
            .pending_ports
            .iter()
            .filter(|port_id| !settled.contains_key(port_id))
            .copied()
            .collect();
        for port_id in orphans {
            self.pending_ports.remove(&port_id);
            router.destroy_artifact(port_id);
            final_actions.push(ArtifactAction::Released { port_id });
        }

        final_actions
    }
}

/// Incremental aggregator for workload references to artifact ports.
/// Produces deltas when workloads add/remove their edges to artifact ports.
///
/// Each artifact port has its own RefsInput. The drain returns
/// `(ArtifactPortId, ArtifactRefDelta)` pairs — the port ID is known
/// from context, so the aggregator only needs to report the workload.
#[derive(Default)]
pub struct ArtifactRefIncrementalAggregator;

impl IncrementalAggregator for ArtifactRefIncrementalAggregator {
    type Input = (WorkloadId, bool);
    type Output = ArtifactRefDelta;

    fn added(&self, (workload_id, _ref_signal): &(WorkloadId, bool)) -> Option<ArtifactRefDelta> {
        Some(ArtifactRefDelta::Referenced {
            workload_id: *workload_id,
        })
    }

    fn removed(
        &self,
        (workload_id, _ref_signal): &(WorkloadId, bool),
    ) -> Option<ArtifactRefDelta> {
        Some(ArtifactRefDelta::Released {
            workload_id: *workload_id,
        })
    }

    fn changed(
        &self,
        _old: &(WorkloadId, bool),
        _new: &(WorkloadId, bool),
    ) -> Option<ArtifactRefDelta> {
        // Signal value changes don't matter — only edge presence matters.
        None
    }
}
