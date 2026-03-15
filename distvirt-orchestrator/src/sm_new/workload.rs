use distvirt_sm_router::SmHandler;
use super::*;

// ---- Workload SM ----

pub(crate) const MAX_RETRIES: u32 = 5;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct WorkloadSm {
    pub(crate) has_spec: bool,
    pub(crate) has_demand: bool,
    pub(crate) pod_running: bool,
    pub(crate) wants_pod: bool,
    pub(crate) pod_id: Option<PodId>,

    /// Set when demand transitions 0→non-zero. Prevents demand fluctuations
    /// from aborting an in-progress pod launch. Cleared when:
    /// - Pod reaches Running (commitment fulfilled)
    /// - Scavenge arrives (explicit override)
    /// - Pod is destroyed with no demand (nothing to commit to)
    pub(crate) committed_to_boot: bool,

    /// Incremented each time the spec signal changes value (Some→Some).
    /// Compared against `launched_with_spec_version` to detect spec changes
    /// during pod launch — replaces PendingIntent::Restart.
    pub(crate) spec_version: u64,
    /// The spec_version when the current pod was created.
    pub(crate) launched_with_spec_version: u64,

    /// Number of consecutive pod failures without a successful Running transition.
    pub(crate) consecutive_failures: u32,
    /// Maximum retries before entering terminal Failed state.
    pub(crate) max_retries: u32,
    /// True while waiting for a retry backoff timer to fire.
    pub(crate) in_backoff: bool,
    /// Incremented each time we enter backoff, used for timer identity.
    pub(crate) backoff_generation: u64,

    /// Timer port ID, passed to pods created by this workload.
    pub(crate) timer_id: TimerId,

    /// Worker ID of the current pod, learned from PodWorkerInput.
    pub(crate) pod_worker_id: Option<WorkerId>,

    /// Whether to suspend the pod instead of destroying it when demand drops.
    pub(crate) suspend_on_idle: bool,
    /// Artifact from a successfully suspended pod. Used to resume on next
    /// demand cycle instead of cold-booting.
    pub(crate) suspended_artifact: Option<ArtifactId>,
    /// True while the pod is in the process of suspending. Prevents reconcile
    /// from touching the pod until it reaches a terminal state.
    pub(crate) awaiting_suspend: bool,
    /// Counter for generating unique artifact IDs.
    pub(crate) artifact_counter: u64,
}

impl WorkloadSm {
    pub(crate) fn new(timer_id: TimerId) -> Self {
        Self::with_max_retries(timer_id, MAX_RETRIES)
    }

    #[allow(dead_code)]
    pub(crate) fn with_max_retries(timer_id: TimerId, max_retries: u32) -> Self {
        WorkloadSm {
            has_spec: false,
            has_demand: false,
            pod_running: false,
            wants_pod: false,
            pod_id: None,
            committed_to_boot: false,
            spec_version: 0,
            launched_with_spec_version: 0,
            consecutive_failures: 0,
            max_retries,
            in_backoff: false,
            backoff_generation: 0,
            timer_id,
            pod_worker_id: None,
            suspend_on_idle: false,
            suspended_artifact: None,
            awaiting_suspend: false,
            artifact_counter: 0,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn new_suspendable(timer_id: TimerId) -> Self {
        WorkloadSm {
            suspend_on_idle: true,
            ..Self::new(timer_id)
        }
    }

    #[allow(dead_code)]
    pub(crate) fn new_suspendable_with_max_retries(timer_id: TimerId, max_retries: u32) -> Self {
        WorkloadSm {
            suspend_on_idle: true,
            ..Self::with_max_retries(timer_id, max_retries)
        }
    }

    #[allow(dead_code)]
    pub(crate) fn next_artifact_id(&mut self) -> ArtifactId {
        self.artifact_counter += 1;
        ArtifactId(self.artifact_counter)
    }
}

impl<C: WorkloadCtx> SmHandler<C> for WorkloadSm {
    type Input = WorkloadInput;

    fn initialize(&mut self, ctx: &mut C) {
        ctx.set_workload_to_timer_edges(vec![self.timer_id]);
        self.update_status_signals(ctx);
    }

    fn handle(&mut self, input: Self::Input, ctx: &mut C) {
        match input {
            WorkloadInput::DemandInput(demand) => {
                let old_demand = self.has_demand;
                self.has_demand = demand.demand_count > 0;

                // Demand appeared: commit to reaching Running.
                if self.has_demand && !old_demand {
                    self.committed_to_boot = true;
                }
                // Demand dropped with no pod in flight: clear commitment and retry state.
                if !self.has_demand && self.pod_id.is_none() {
                    self.committed_to_boot = false;
                    self.consecutive_failures = 0;
                    self.in_backoff = false;
                }

                ctx.set_workload_to_service_edges(demand.service_ids);
                self.reconcile(ctx);
                self.update_timer_signal(ctx);
            }
            WorkloadInput::SpecInput(spec_opt) => {
                let new_has_spec = spec_opt.is_some();

                if self.has_spec && new_has_spec {
                    // Spec value changed (Some→Some). Increment version so we
                    // detect stale launches via on_pod_running.
                    self.spec_version += 1;
                    self.consecutive_failures = 0;
                    self.in_backoff = false;

                    // If pod is already Running, restart immediately.
                    if self.pod_running {
                        self.destroy_current_pod(ctx);
                    }
                }

                if self.has_spec && !new_has_spec {
                    // Spec removed — clean up and self-destruct.
                    self.destroy_current_pod(ctx);
                    ctx.self_destruct();
                    return;
                }

                self.has_spec = new_has_spec;
                self.reconcile(ctx);
                self.update_timer_signal(ctx);
            }
            WorkloadInput::PodStatusInput(statuses) => {
                let was_running = self.pod_running;
                self.pod_running = statuses.iter().any(|s| *s == PodStatus::Running);
                let has_failed = statuses.iter().any(|s| *s == PodStatus::Failed);
                let has_finished = statuses.iter().any(|s| *s == PodStatus::Finished);

                // Pod reached Suspended terminal state — save artifact and reap.
                let suspended_artifact = statuses.iter().find_map(|s| match s {
                    PodStatus::Suspended { artifact_id } => Some(*artifact_id),
                    _ => None,
                });

                // All pods gone — single cleanup path for signal-derived state.
                // Normally pod_id is already cleared by the initiator
                // (destroy_current_pod, on_pod_failed, etc.), but this acts
                // as a safety net for unexpected pod disappearance.
                if statuses.is_empty() && self.pod_id.is_some() {
                    self.pod_id = None;
                    self.pod_worker_id = None;
                    self.awaiting_suspend = false;
                    self.committed_to_boot = false;
                    ctx.set_workload_to_pod_edges(vec![]);
                    ctx.set_pod_intent(PodIntent::None);
                    ctx.set_readiness(None);
                }

                if let Some(artifact_id) = suspended_artifact {
                    // Pod successfully suspended. Save artifact, reap pod.
                    self.suspended_artifact = Some(artifact_id);
                    // pod_running already set to false at top of handler
                    // (Suspended is not Running).
                    // pod_worker_id will be cleared by PodWorkerInput signal propagation.
                    self.awaiting_suspend = false;
                    ctx.set_readiness(None);
                    // Remove edge → pod will self-destruct (terminal + no owner).
                    ctx.set_workload_to_pod_edges(vec![]);
                    ctx.set_pod_intent(PodIntent::None);
                    self.pod_id = None;
                    // Reconcile may create a new pod if demand returned during suspend.
                    self.reconcile(ctx);
                } else if self.pod_running && !was_running {
                    // Pod just became Running — check current signal state
                    // to decide what to do. This replaces PendingIntent.
                    self.on_pod_running(ctx);
                } else if has_failed && self.pod_id.is_some() {
                    self.on_pod_failed(ctx);
                } else if has_finished && self.pod_id.is_some() {
                    self.on_pod_finished(ctx);
                } else if !self.pod_running && was_running {
                    // Pod lost running status.
                    ctx.set_readiness(None);
                    self.reconcile(ctx);
                } else {
                    self.reconcile(ctx);
                }
                self.update_timer_signal(ctx);
            }
            WorkloadInput::PodWorkerInput(workers) => {
                // Track the worker ID of our current pod.
                let new_worker_id = workers.into_iter().next().flatten();
                if new_worker_id != self.pod_worker_id {
                    self.pod_worker_id = new_worker_id;
                    // If pod is running, update readiness with the real worker ID.
                    if self.pod_running {
                        self.update_readiness(ctx);
                    }
                }
            }
            WorkloadInput::AdminCommand(cmd) => {
                match cmd {
                    AdminCmd::Scavenge => {
                        // Safe capacity reclamation. Noop if actively demanded.
                        if self.has_demand {
                            return;
                        }
                        // Not demanded — reclaim: destroy pod, clear commitment and retry state.
                        // Also discard any suspended artifact.
                        self.committed_to_boot = false;
                        self.consecutive_failures = 0;
                        self.in_backoff = false;
                        self.suspended_artifact = None;
                        self.destroy_current_pod(ctx);
                        self.reconcile(ctx);
                    }
                    AdminCmd::Restart => {
                        // Destroy current pod (if any) and let reconcile create
                        // a fresh one. Reset spec version tracking since this is
                        // an intentional restart, not a stale-spec detection.
                        self.consecutive_failures = 0;
                        self.in_backoff = false;
                        self.destroy_current_pod(ctx);
                        self.launched_with_spec_version = self.spec_version;
                        self.reconcile(ctx);
                    }
                }
                self.update_timer_signal(ctx);
            }
            WorkloadInput::WorkloadTimerFired(key) => match key {
                WorkloadTimerKey::RetryBackoff => {
                    if self.in_backoff {
                        self.in_backoff = false;
                        self.reconcile(ctx);
                        self.update_timer_signal(ctx);
                    }
                }
            },
        }
        self.update_status_signals(ctx);
    }
}

impl WorkloadSm {
    /// Called when the pod transitions to Running. Makes decisions based on
    /// current signal state rather than accumulated PendingIntent.
    ///
    /// Priority order:
    /// 1. Spec changed since launch → restart with new spec
    /// 2. No demand → deactivate (committed_to_boot fulfilled)
    /// 3. Otherwise → emit readiness
    pub(crate) fn on_pod_running(&mut self, ctx: &mut impl WorkloadCtx) {
        self.committed_to_boot = false;
        self.consecutive_failures = 0;

        // 1. Spec changed since we launched this pod → restart.
        if self.launched_with_spec_version != self.spec_version {
            self.destroy_current_pod(ctx);
            self.reconcile(ctx);
            return;
        }

        // 2. No demand → let reconcile decide (suspend if enabled, else destroy).
        if !self.has_demand {
            self.reconcile(ctx);
            return;
        }

        // 3. Active — emit readiness with real worker ID.
        self.update_readiness(ctx);
    }

    /// Emit readiness signal with current pod and worker info.
    pub(crate) fn update_readiness(&self, ctx: &mut impl WorkloadCtx) {
        ctx.set_readiness(Some(ReadyInfo {
            pod_id: self.pod_id.unwrap_or(PodId(0)),
            worker_id: self.pod_worker_id.unwrap_or(WorkerId(0)),
        }));
    }

    /// Called when a pod reports Finished status (graceful exit, exit code 0).
    /// Not counted as a failure. Cleans up and reconciles.
    pub(crate) fn on_pod_finished(&mut self, ctx: &mut impl WorkloadCtx) {
        // pod_running is already false — set by PodStatusInput handler at
        // the top (Finished is not Running).
        self.awaiting_suspend = false;
        ctx.set_readiness(None);

        // Remove ownership edge — pod is terminal (Finished),
        // so removing the edge triggers self-destruct.
        ctx.set_workload_to_pod_edges(vec![]);
        ctx.set_pod_intent(PodIntent::None);
        self.pod_id = None;
        // pod_worker_id will be cleared by PodWorkerInput signal propagation.

        // No failure increment — graceful exit is not a failure.
        // Re-evaluate commitment.
        if !self.has_demand {
            self.committed_to_boot = false;
        }

        self.reconcile(ctx);
        self.update_timer_signal(ctx);
    }

    /// Called when a pod reports Failed status. Cleans up tracking and enters
    /// backoff for retry, or gives up if max retries exceeded.
    pub(crate) fn on_pod_failed(&mut self, ctx: &mut impl WorkloadCtx) {
        // pod_running is already false — set by PodStatusInput handler at
        // the top (Failed is not Running).
        self.awaiting_suspend = false;
        ctx.set_readiness(None);

        // Remove ownership edge — pod is already terminal (Failed),
        // so removing the edge triggers self-destruct (terminal + no owner).
        ctx.set_workload_to_pod_edges(vec![]);
        ctx.set_pod_intent(PodIntent::None);
        self.pod_id = None;
        // pod_worker_id will be cleared by PodWorkerInput signal propagation.

        self.consecutive_failures += 1;

        // Re-evaluate commitment: no demand after pod death → no reason to retry.
        if !self.has_demand {
            self.committed_to_boot = false;
        }
        if self.consecutive_failures >= self.max_retries {
            self.committed_to_boot = false;
        }

        // Enter backoff only if we actually want to retry.
        let want_retry = (self.has_demand || self.committed_to_boot)
            && self.consecutive_failures < self.max_retries;
        if want_retry {
            self.in_backoff = true;
            self.backoff_generation += 1;
        } else if !self.has_demand {
            // Going dormant — clear failure tracking.
            self.consecutive_failures = 0;
        }

        self.reconcile(ctx);
        self.update_timer_signal(ctx);
    }

    /// Abandon the current pod by removing the ownership edge.
    /// The pod will drive itself to a terminal state and self-destruct.
    /// Any suspended artifact is discarded (this is a hard kill).
    pub(crate) fn destroy_current_pod(&mut self, ctx: &mut impl WorkloadCtx) {
        if self.pod_id.is_some() {
            ctx.set_workload_to_pod_edges(vec![]);
            ctx.set_pod_intent(PodIntent::None);
            self.pod_id = None;
        }
        // pod_running and pod_worker_id are signal-derived — they will be
        // cleared by PodStatusInput([]) and PodWorkerInput([]) when the
        // abandoned pod removes its reverse edges and self-destructs.
        self.awaiting_suspend = false;
        self.suspended_artifact = None;
        ctx.set_readiness(None);
    }

    pub(crate) fn update_timer_signal(&self, ctx: &mut impl WorkloadCtx) {
        if self.in_backoff {
            ctx.set_wanted_timers(vec![TimerRequest {
                key: WorkloadTimerKey::RetryBackoff,
                generation: self.backoff_generation,
            }]);
        } else {
            ctx.set_wanted_timers(vec![]);
        }
    }

    pub(crate) fn update_status_signals(&self, ctx: &mut impl WorkloadCtx) {
        let is_failed = self.consecutive_failures >= self.max_retries
            && (self.has_demand || self.committed_to_boot);
        let status = if is_failed {
            WlStatus::Failed
        } else if self.in_backoff {
            WlStatus::RetryBackoff
        } else if self.awaiting_suspend {
            WlStatus::Suspending
        } else if self.suspended_artifact.is_some() && self.pod_id.is_none() {
            WlStatus::Suspended
        } else if self.pod_running {
            WlStatus::Running
        } else if self.pod_id.is_some() {
            WlStatus::Launching
        } else if !self.has_spec && (self.has_demand || self.committed_to_boot) {
            WlStatus::WaitingForSpec
        } else {
            WlStatus::Dormant
        };
        ctx.set_wl_status_signal(status);
        ctx.set_consecutive_failures_signal(self.consecutive_failures);
        ctx.set_spec_stale_signal(
            self.pod_id.is_some() && self.launched_with_spec_version != self.spec_version,
        );
    }

    pub(crate) fn reconcile(&mut self, ctx: &mut impl WorkloadCtx) {
        // If we're waiting for a suspend to complete, don't touch the pod.
        if self.awaiting_suspend {
            return;
        }

        let is_failed = self.consecutive_failures >= self.max_retries;
        let want_pod = self.has_spec
            && (self.has_demand || self.committed_to_boot)
            && !self.in_backoff
            && !is_failed;
        self.wants_pod = want_pod;

        if want_pod && self.pod_id.is_none() {
            // Create new pod — resume from artifact if available.
            let pod = if let Some(artifact_id) = self.suspended_artifact.take() {
                PodSm::new_from_artifact(self.timer_id, artifact_id)
            } else {
                PodSm::new(self.timer_id)
            };
            let pod_id = ctx.create_pod(pod);
            self.pod_id = Some(pod_id);
            self.launched_with_spec_version = self.spec_version;
            ctx.set_workload_to_pod_edges(vec![pod_id]);
            ctx.set_pod_intent(PodIntent::Want);
        } else if want_pod && self.pod_id.is_some() {
            ctx.set_pod_intent(PodIntent::Want);
        } else if !want_pod && self.pod_id.is_some() {
            if self.pod_running && self.suspend_on_idle {
                // Signal pod to suspend — keep edge, pod drives itself to
                // Suspended terminal state.
                ctx.set_pod_intent(PodIntent::Suspend);
                self.awaiting_suspend = true;
            } else {
                // Abandon pod (remove edge). Pod will drive itself to
                // terminal and self-destruct.
                ctx.set_workload_to_pod_edges(vec![]);
                ctx.set_pod_intent(PodIntent::None);
                self.pod_id = None;
                ctx.set_readiness(None);
            }
        } else {
            ctx.set_pod_intent(PodIntent::None);
        }
    }
}
