use super::*;
use distvirt_sm_router::SmHandler;

// ---- Pod SM ----
//
// A pod manages the lifecycle of a single "running thing" from creation to
// terminal state. The lifecycle is linear and non-circular:
//
//   Pending → Running → Suspending → Suspended(artifact)  [terminal]
//                     → Failed                             [terminal]
//            → Failed                                      [terminal]
//
// Terminal states wait for reaping: the pod self-destructs only when it is
// in a terminal state AND has no owner. This gives the workload time to
// read the terminal status (e.g. extract artifact_id from Suspended).
//
// Two paths to pod death:
//   Natural:  pod reaches terminal → workload reads status → workload
//             removes edge (reap) → pod self-destructs.
//   Abandon:  workload removes edge → pod drives itself to terminal
//             (owner loss while live = failure) → pod self-destructs.

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PodSm {
    pub status: PodStatus,
    pub workload_id: Option<WorkloadId>,
    pub worker_id: Option<WorkerId>,
    pub intent: PodIntent,
    /// Artifact to resume from (set at creation for resumed pods).
    /// The worker port can read this to know whether to cold-boot or resume.
    pub resume_artifact: Option<ArtifactPortId>,
    /// Generation counter for timer requests.
    pub timer_generation: u64,
    /// Worker assigned via lease signal.
    pub assigned_worker: Option<WorkerId>,
    /// Launch spec received from owner workload via signal graph.
    /// Included in PodScheduleRequest so it flows to the worker port.
    pub launch_spec: Option<WorkloadSpec>,
}

impl PodSm {
    pub(crate) fn new() -> Self {
        PodSm {
            status: PodStatus::Pending,
            workload_id: None,
            worker_id: None,
            intent: PodIntent::None,
            resume_artifact: None,
            timer_generation: 0,
            assigned_worker: None,
            launch_spec: None,
        }
    }

    pub(crate) fn new_from_artifact(artifact_id: ArtifactPortId) -> Self {
        PodSm {
            status: PodStatus::Pending,
            workload_id: None,
            worker_id: None,
            intent: PodIntent::None,
            resume_artifact: Some(artifact_id),
            timer_generation: 0,
            assigned_worker: None,
            launch_spec: None,
        }
    }

    /// Build a PodScheduleRequest with the current state.
    fn make_schedule_request(&self) -> PodScheduleRequest {
        PodScheduleRequest {
            resume_artifact: self.resume_artifact.clone(),
            suspend: matches!(self.status, PodStatus::Suspending),
            spec: self.launch_spec.clone(),
        }
    }

    /// Self-destruct if terminal and no owner (the reaping rule).
    pub(crate) fn maybe_reap(&self, ctx: &mut impl PodCtx) {
        if self.status.is_terminal() && self.workload_id.is_none() {
            ctx.self_destruct();
        }
    }

    /// Update the timer signal based on current pod status.
    pub(crate) fn update_timer_signal(&self, ctx: &mut impl PodCtx) {
        use std::time::Duration;
        match &self.status {
            PodStatus::Pending => {
                ctx.set_wanted_timers(vec![PodTimerRequest {
                    key: PodTimerKey::LaunchTimeout,
                    generation: self.timer_generation,
                    duration: Duration::from_secs(30),
                }]);
            }
            PodStatus::Suspending => {
                ctx.set_wanted_timers(vec![PodTimerRequest {
                    key: PodTimerKey::SuspendTimeout,
                    generation: self.timer_generation,
                    duration: Duration::from_secs(30),
                }]);
            }
            _ => {
                ctx.set_wanted_timers(vec![]);
            }
        }
    }
}

impl<C: PodCtx> SmHandler<C> for PodSm {
    type Input = PodInput;

    fn initialize(&mut self, ctx: &mut C) {
        ctx.set_pod_timers_edges(vec![TIMER]);
        ctx.set_pod_schedule_intent_edges(vec![SCHEDULE_REQUEST]);
        ctx.set_schedule_request(self.make_schedule_request());
        self.update_timer_signal(ctx);
    }

    fn handle(&mut self, input: Self::Input, ctx: &mut C) {
        match input {
            PodInput::LaunchSpecInput(spec) => {
                if self.launch_spec != spec {
                    self.launch_spec = spec;
                    // Re-emit schedule request with updated spec.
                    ctx.set_schedule_request(self.make_schedule_request());
                }
            }
            PodInput::WorkerInput(worker) => {
                // Track assigned worker.
                let new_worker_id = worker.as_ref().map(|(id, _)| *id);
                if new_worker_id != self.worker_id {
                    self.worker_id = new_worker_id;
                    ctx.set_assigned_worker(self.worker_id);
                }

                if worker.is_none() && !self.status.is_terminal() {
                    // Worker lost — pod displaced by infrastructure.
                    self.status = PodStatus::Displaced;
                    ctx.set_status(PodStatus::Displaced);
                    self.update_timer_signal(ctx);
                    self.maybe_reap(ctx);
                }
            }
            PodInput::OwnerInput(owner) => {
                let had_owner = self.workload_id.is_some();
                let (new_wl, new_intent) = match owner {
                    Some((wl, intent)) => (Some(wl), intent),
                    None => (None, PodIntent::None),
                };
                self.workload_id = new_wl;
                self.intent = new_intent;

                let edges: Vec<WorkloadId> = self.workload_id.into_iter().collect();
                ctx.set_pod_report_edges(edges);

                // React to intent: Running + Suspend → begin suspending.
                if matches!(
                    (&self.status, &self.intent),
                    (PodStatus::Running, PodIntent::Suspend)
                ) {
                    self.timer_generation += 1;
                    self.status = PodStatus::Suspending;
                    ctx.set_status(PodStatus::Suspending);
                    ctx.set_schedule_request(self.make_schedule_request());
                    self.update_timer_signal(ctx);
                }

                // Lost owner while in a live state → drive to terminal.
                // (In a real system this would go through a shutdown sequence
                // with worker interaction; simplified to immediate here.)
                if had_owner && self.workload_id.is_none() && !self.status.is_terminal() {
                    self.status = PodStatus::Failed;
                    ctx.set_status(PodStatus::Failed);
                    self.update_timer_signal(ctx);
                }

                self.maybe_reap(ctx);
            }
            PodInput::NotifyPodStatus(new_status) => {
                // Only accept valid worker-reported status transitions.
                // Pending and Suspending are SM-internal states (managed by
                // initialization and OwnerInput respectively), so they are
                // rejected here. This prevents inconsistent state from
                // out-of-order or stale worker notifications.
                let accept = !self.status.is_terminal()
                    && match &new_status {
                        // Worker reports pod is running. Only valid from Pending.
                        PodStatus::Running => matches!(self.status, PodStatus::Pending),
                        // Worker reports failure or graceful exit. Valid from any
                        // non-terminal state.
                        PodStatus::Failed | PodStatus::Finished => true,
                        // All other statuses are SM-internal or use dedicated
                        // inputs (NotifyPodSuspended).
                        _ => false,
                    };
                if accept {
                    self.status = new_status.clone();
                    ctx.set_status(new_status);
                    self.update_timer_signal(ctx);
                    self.maybe_reap(ctx);
                }
            }
            PodInput::NotifyPodSuspended(artifact_id) => {
                if matches!(self.status, PodStatus::Suspending) {
                    self.status = PodStatus::Suspended {
                        artifact_id: artifact_id.clone(),
                    };
                    ctx.set_status(PodStatus::Suspended { artifact_id });
                    self.update_timer_signal(ctx);
                    self.maybe_reap(ctx);
                }
            }
            PodInput::LeaseInput(lease) => {
                let had_lease = self.assigned_worker.is_some();
                match lease {
                    Some(info) if !had_lease && matches!(self.status, PodStatus::Pending) => {
                        // Lease granted — target the assigned worker.
                        self.assigned_worker = Some(info.worker_id);
                        ctx.set_pod_placement_edges(vec![info.worker_id]);
                    }
                    None if had_lease && !self.status.is_terminal() => {
                        // Lease revoked — preemption (infrastructure event).
                        self.assigned_worker = None;
                        self.status = PodStatus::Displaced;
                        ctx.set_status(PodStatus::Displaced);
                        ctx.set_pod_placement_edges(vec![]);
                        self.update_timer_signal(ctx);
                        self.maybe_reap(ctx);
                    }
                    _ => {}
                }
            }
            PodInput::PodTimerFired(key) => match key {
                PodTimerKey::LaunchTimeout => {
                    if matches!(self.status, PodStatus::Pending) {
                        self.status = PodStatus::Failed;
                        ctx.set_status(PodStatus::Failed);
                        self.update_timer_signal(ctx);
                        self.maybe_reap(ctx);
                    }
                }
                PodTimerKey::SuspendTimeout => {
                    if matches!(self.status, PodStatus::Suspending) {
                        self.status = PodStatus::Failed;
                        ctx.set_status(PodStatus::Failed);
                        self.update_timer_signal(ctx);
                        self.maybe_reap(ctx);
                    }
                }
            },
        }
    }
}
