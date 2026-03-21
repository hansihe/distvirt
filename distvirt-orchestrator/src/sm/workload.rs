use super::*;
use distvirt_sm_router::SmHandler;

// ---- Workload SM ----

pub const MAX_RETRIES: u32 = 5;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct WorkloadSm {
    pub has_spec: bool,
    pub has_demand: bool,
    pub pod_running: bool,
    pub wants_pod: bool,
    pub pod_id: Option<PodId>,

    /// Set when demand transitions 0→non-zero. Prevents demand fluctuations
    /// from aborting an in-progress pod launch. Cleared when:
    /// - Pod reaches Running (commitment fulfilled)
    /// - Scavenge arrives (explicit override)
    /// - Pod is destroyed with no demand (nothing to commit to)
    pub committed_to_boot: bool,

    /// Incremented each time the spec signal changes value (Some→Some).
    /// Compared against `launched_with_spec_version` to detect spec changes
    /// during pod launch — replaces PendingIntent::Restart.
    pub spec_version: u64,
    /// The spec_version when the current pod was created.
    pub launched_with_spec_version: u64,

    /// Number of consecutive pod failures without a successful Running transition.
    pub consecutive_failures: u32,
    /// Maximum retries before entering terminal Failed state.
    pub max_retries: u32,
    /// True while waiting for a retry backoff timer to fire.
    pub in_backoff: bool,
    /// Incremented each time we enter backoff, used for timer identity.
    pub backoff_generation: u64,

    /// Worker ID of the current pod, learned from PodWorkerInput.
    pub pod_worker_id: Option<WorkerId>,

    /// Whether to suspend the pod instead of destroying it when demand drops.
    /// Updated from WorkloadSpec on each spec delivery.
    pub suspend_on_idle: bool,
    /// Artifact port this workload references (if suspended with artifact).
    /// Set when a pod successfully suspends. The workload sets an edge to
    /// this port; the port signals back validity once the scheduler confirms.
    pub artifact_port: Option<ArtifactPortId>,
    /// Whether the artifact port has confirmed validity (return edge received).
    pub artifact_confirmed: bool,
    /// Generation counter for artifact confirmation timer.
    pub artifact_confirm_gen: u64,
    /// True while the pod is in the process of suspending. Prevents reconcile
    /// from touching the pod until it reaches a terminal state.
    pub awaiting_suspend: bool,

    /// The pod-affecting spec from the last delivery. Used to detect changes
    /// that require pod recreation (any field change in PodSpec bumps
    /// spec_version and triggers restart).
    pub current_pod_spec: Option<PodSpec>,

    /// Pod IP from spec's network config, included in ReadyInfo for downstream
    /// consumers (endpoint signals).
    pub pod_ip: std::net::Ipv4Addr,

    /// Endpoint SM owned by this workload (if any).
    pub endpoint_id: Option<EndpointId>,

    /// Run policy: Service (restart on completion) or Job (run once).
    pub run_policy: RunPolicy,
    /// True when a Job has finished successfully (exit code 0).
    pub completed: bool,
    /// Exit code from the last completed or failed pod.
    pub last_exit_code: Option<i32>,
    /// Reason string from the last failed pod.
    pub last_failure_reason: Option<String>,
    /// If true, the workload respects demand signals and starts dormant.
    /// If false, the workload is always-on regardless of demand.
    pub respects_demand: bool,
}

impl WorkloadSm {
    pub(crate) fn new() -> Self {
        Self::with_max_retries(MAX_RETRIES)
    }

    #[allow(dead_code)]
    pub(crate) fn with_max_retries(max_retries: u32) -> Self {
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
            pod_worker_id: None,
            suspend_on_idle: false,
            artifact_port: None,
            artifact_confirmed: false,
            artifact_confirm_gen: 0,
            awaiting_suspend: false,
            current_pod_spec: None,
            pod_ip: std::net::Ipv4Addr::UNSPECIFIED,
            endpoint_id: None,
            run_policy: RunPolicy::Service,
            completed: false,
            last_exit_code: None,
            last_failure_reason: None,
            respects_demand: false,
        }
    }

    /// Effective demand: always true for always-on workloads, otherwise
    /// reflects the actual demand signal.
    fn effective_demand(&self) -> bool {
        !self.respects_demand || self.has_demand
    }

    /// Drop the artifact port reference. Called on spec change, scavenge, etc.
    fn clear_artifact_ref(&mut self, ctx: &mut impl WorkloadCtx) {
        if self.artifact_port.is_some() {
            self.artifact_port = None;
            self.artifact_confirmed = false;
            ctx.set_workload_artifact_ref_edges(vec![]);
            ctx.set_artifact_ref(false);
        }
    }
}

impl<C: WorkloadCtx> SmHandler<C> for WorkloadSm {
    type Input = WorkloadInput;

    fn initialize(&mut self, ctx: &mut C) {
        ctx.set_workload_timers_edges(vec![TIMER]);
        ctx.set_workload_observability_edges(vec![OBSERVABILITY]);
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
                    self.completed = false; // allow re-run on next activation
                }
                // Demand dropped while holding a retained failed pod: clear
                // failure tracking so reconcile can reap and go dormant.
                if !self.has_demand
                    && self.pod_id.is_some()
                    && !self.pod_running
                    && (self.in_backoff || self.consecutive_failures >= self.max_retries)
                {
                    self.committed_to_boot = false;
                    self.consecutive_failures = 0;
                    self.in_backoff = false;
                    self.completed = false;
                }

                ctx.set_endpoint_readiness_edges(demand.endpoint_ids);
                self.reconcile(ctx);
                self.update_timer_signal(ctx);
            }
            WorkloadInput::SpecInput(spec_opt) => {
                let new_has_spec = spec_opt.is_some();

                // Forward the full spec to pods via signal graph.
                let launch_spec = spec_opt.as_ref().map(|(_, s)| s.clone());
                ctx.set_pod_launch_spec(launch_spec);

                if let Some((_, ref spec)) = spec_opt {
                    // --- Update pod_ip from spec network config ---
                    self.pod_ip = spec
                        .pod_spec
                        .network
                        .as_ref()
                        .map(|n| n.ip)
                        .unwrap_or(std::net::Ipv4Addr::UNSPECIFIED);

                    // --- Create/update workload-owned endpoint ---
                    if let Some(ref network) = spec.pod_spec.network {
                        if self.endpoint_id.is_none() {
                            let ep_id = ctx.create_endpoint(
                                endpoint::EndpointSm::new(spec.config.respects_demand),
                            );
                            self.endpoint_id = Some(ep_id);
                            ctx.set_workload_endpoint_ownership_edges(vec![ep_id]);
                        }
                        let idle_timeout = spec
                            .config
                            .activation
                            .as_ref()
                            .map(|a| a.idle_timeout)
                            .unwrap_or(std::time::Duration::ZERO);
                        ctx.set_endpoint_config(Some(endpoint::EndpointConfig {
                            kind: endpoint::EndpointKind::Workload,
                            workload: ctx.id(),
                            has_activation: spec.config.respects_demand,
                            idle_timeout,
                            ip: network.ip,
                            dns_entry: None,
                        }));
                    }

                    // --- Update respects_demand from spec ---
                    self.respects_demand = spec.config.respects_demand;

                    // --- Update suspend_on_idle from spec ---
                    let old_suspend_on_idle = self.suspend_on_idle;
                    self.suspend_on_idle = spec.config.suspend_on_idle;

                    // --- Update run_policy from spec ---
                    let old_run_policy = self.run_policy.clone();
                    self.run_policy = spec.config.run_policy.clone();

                    // Job→Service while completed: clear completed so reconcile launches.
                    if old_run_policy == RunPolicy::Job
                        && self.run_policy == RunPolicy::Service
                        && self.completed
                    {
                        self.completed = false;
                    }

                    // --- Detect pod-affecting spec changes ---
                    let pod_spec_changed =
                        self.current_pod_spec.as_ref() != Some(&spec.pod_spec);
                    self.current_pod_spec = Some(spec.pod_spec.clone());

                    if self.has_spec && pod_spec_changed {
                        // Pod spec changed (Some→Some). Increment version so we
                        // detect stale launches via on_pod_running.
                        self.spec_version += 1;

                        // Reap retained dead pod before clearing failure state.
                        self.reap_retained_pod(ctx);

                        self.consecutive_failures = 0;
                        self.in_backoff = false;
                        self.completed = false;

                        // Discard any suspended artifact — it was produced
                        // from the old spec and cannot be resumed.
                        self.clear_artifact_ref(ctx);

                        // If pod is already Running, restart immediately.
                        // Pending pods are kept — spec mismatch is detected
                        // at on_pod_running.
                        if self.pod_running {
                            self.destroy_current_pod(ctx);
                        }
                    }

                    // --- Handle suspend_on_idle true→false transitions ---
                    if old_suspend_on_idle && !self.suspend_on_idle {
                        // Abandon any in-progress suspend (pod can handle
                        // edge removal at any lifecycle point).
                        if self.awaiting_suspend {
                            self.destroy_current_pod(ctx);
                        }
                        // Discard stale artifact — cold boot next time.
                        self.clear_artifact_ref(ctx);
                    }
                }

                if self.has_spec && !new_has_spec {
                    // Spec removed — clean up and self-destruct.
                    self.destroy_current_pod(ctx);
                    self.current_pod_spec = None;
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
                let has_failed = statuses
                    .iter()
                    .any(|s| matches!(s, PodStatus::Failed { .. }));
                let has_displaced = statuses.iter().any(|s| *s == PodStatus::Displaced);
                let has_finished = statuses
                    .iter()
                    .any(|s| matches!(s, PodStatus::Finished { .. }));

                // Pod reached Suspended terminal state — save artifact and reap.
                let suspended_artifact = statuses.iter().find_map(|s| match s {
                    PodStatus::Suspended { artifact_id } => Some(artifact_id.clone()),
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
                    ctx.set_pod_ownership_edges(vec![]);
                    ctx.set_pod_intent(PodIntent::None);
                    ctx.set_readiness(None);
                    ctx.set_placement(None);
                }

                if let Some(artifact_id) = suspended_artifact {
                    // Pod successfully suspended. Save artifact if spec
                    // hasn't changed since the pod was launched; otherwise
                    // the artifact is stale and must be discarded.
                    if self.spec_version == self.launched_with_spec_version {
                        let port_id = artifact_id;
                        self.artifact_port = Some(port_id);
                        self.artifact_confirmed = false;
                        self.artifact_confirm_gen += 1;
                        ctx.set_workload_artifact_ref_edges(vec![port_id]);
                        ctx.set_artifact_ref(true);
                    }
                    // pod_running already set to false at top of handler
                    // (Suspended is not Running).
                    // pod_worker_id will be cleared by PodWorkerInput signal propagation.
                    self.awaiting_suspend = false;
                    ctx.set_readiness(None);
                    ctx.set_placement(None);
                    // Remove edge → pod will self-destruct (terminal + no owner).
                    ctx.set_pod_ownership_edges(vec![]);
                    ctx.set_pod_intent(PodIntent::None);
                    self.pod_id = None;
                    // Reconcile may create a new pod if demand returned during suspend.
                    self.reconcile(ctx);
                } else if self.pod_running && !was_running {
                    // Pod just became Running — check current signal state
                    // to decide what to do. This replaces PendingIntent.
                    self.on_pod_running(ctx);
                } else if has_failed && self.pod_id.is_some() {
                    let (exit_code, reason) = statuses
                        .iter()
                        .find_map(|s| match s {
                            PodStatus::Failed { exit_code, reason } => {
                                Some((*exit_code, reason.clone()))
                            }
                            _ => None,
                        })
                        .unwrap_or((None, String::new()));
                    self.on_pod_failed(ctx, exit_code, reason);
                } else if has_displaced && self.pod_id.is_some() {
                    self.on_pod_displaced(ctx);
                } else if has_finished && self.pod_id.is_some() {
                    let exit_code = statuses
                        .iter()
                        .find_map(|s| match s {
                            PodStatus::Finished { exit_code } => Some(*exit_code),
                            _ => None,
                        })
                        .unwrap_or(0);
                    self.on_pod_finished(ctx, exit_code);
                } else if !self.pod_running && was_running {
                    // Pod lost running status.
                    ctx.set_readiness(None);
                    ctx.set_placement(None);
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
                    // Always forward placement to endpoints so the worker can
                    // prepare the endpoint table entry before the pod is running.
                    ctx.set_placement(new_worker_id);
                    // If pod is running, also update readiness with the real worker ID.
                    if self.pod_running {
                        self.update_readiness(ctx);
                    }
                }
            }
            WorkloadInput::AdminCommand(cmd) => {
                match cmd {
                    AdminCmd::Scavenge => {
                        // Safe capacity reclamation. Noop if actively demanded.
                        if self.effective_demand() {
                            return;
                        }
                        // Not demanded — reclaim: destroy pod, clear commitment and retry state.
                        // Also discard any suspended artifact.
                        self.committed_to_boot = false;
                        self.consecutive_failures = 0;
                        self.in_backoff = false;
                        self.completed = false;
                        self.clear_artifact_ref(ctx);
                        self.destroy_current_pod(ctx);
                        self.reconcile(ctx);
                    }
                    AdminCmd::Restart => {
                        // Destroy current pod (if any) and let reconcile create
                        // a fresh one. Reset spec version tracking since this is
                        // an intentional restart, not a stale-spec detection.
                        self.consecutive_failures = 0;
                        self.in_backoff = false;
                        self.completed = false;
                        self.destroy_current_pod(ctx);
                        self.launched_with_spec_version = self.spec_version;
                        self.reconcile(ctx);
                    }
                }
                self.update_timer_signal(ctx);
            }
            WorkloadInput::ArtifactInput(valid) => {
                match valid {
                    Some(true) => {
                        self.artifact_confirmed = true;
                        // May unblock pod creation if reconcile was waiting
                        // for artifact confirmation.
                        self.reconcile(ctx);
                    }
                    Some(false) | None => {
                        if self.artifact_port.is_some() {
                            self.artifact_port = None;
                            self.artifact_confirmed = false;
                            ctx.set_workload_artifact_ref_edges(vec![]);
                            ctx.set_artifact_ref(false);
                            self.reconcile(ctx);
                        }
                    }
                }
                self.update_timer_signal(ctx);
                self.update_status_signals(ctx);
            }
            WorkloadInput::WorkloadTimerFired(key) => match key {
                WorkloadTimerKey::RetryBackoff => {
                    if self.in_backoff {
                        // Reap the retained failed pod before reconcile
                        // creates a new one for the retry attempt.
                        self.reap_retained_pod(ctx);
                        self.in_backoff = false;
                        self.reconcile(ctx);
                        self.update_timer_signal(ctx);
                    }
                }
                WorkloadTimerKey::ArtifactConfirm => {
                    if self.artifact_port.is_some() && !self.artifact_confirmed {
                        self.artifact_port = None;
                        ctx.set_workload_artifact_ref_edges(vec![]);
                        ctx.set_artifact_ref(false);
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

        // Resume confirmed successful — drop artifact reference.
        // The physical artifact persists (managed by scheduler/adapter);
        // we just release our claim on it.
        self.clear_artifact_ref(ctx);

        // 1. Spec changed since we launched this pod → restart.
        if self.launched_with_spec_version != self.spec_version {
            self.destroy_current_pod(ctx);
            self.reconcile(ctx);
            return;
        }

        // 2. No demand → let reconcile decide (suspend if enabled, else destroy).
        if !self.effective_demand() {
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
            pod_ip: self.pod_ip,
        }));
    }

    /// Called when a pod reports Finished status (graceful exit, exit code 0).
    /// Not counted as a failure. Cleans up and reconciles.
    pub(crate) fn on_pod_finished(&mut self, ctx: &mut impl WorkloadCtx, exit_code: i32) {
        // pod_running is already false — set by PodStatusInput handler at
        // the top (Finished is not Running).
        self.awaiting_suspend = false;
        self.clear_artifact_ref(ctx);
        ctx.set_readiness(None);
        ctx.set_placement(None);

        // Remove ownership edge — pod is terminal (Finished),
        // so removing the edge triggers self-destruct.
        ctx.set_pod_ownership_edges(vec![]);
        ctx.set_pod_intent(PodIntent::None);
        self.pod_id = None;
        // pod_worker_id will be cleared by PodWorkerInput signal propagation.

        self.last_exit_code = Some(exit_code);

        if self.run_policy == RunPolicy::Job {
            // Job finished successfully — mark completed, don't relaunch.
            self.completed = true;
            self.committed_to_boot = false;
        } else {
            // Service: no failure increment — graceful exit is not a failure.
            // Re-evaluate commitment.
            if !self.effective_demand() {
                self.committed_to_boot = false;
            }
        }

        self.reconcile(ctx);
        self.update_timer_signal(ctx);
    }

    /// Called when a pod reports Failed status. Retains the failed pod for
    /// inspectability during backoff and terminal failure. The pod is reaped
    /// when transitioning out: retry attempt, demand drop, admin command, or
    /// spec change.
    pub(crate) fn on_pod_failed(
        &mut self,
        ctx: &mut impl WorkloadCtx,
        exit_code: Option<i32>,
        reason: String,
    ) {
        // pod_running is already false — set by PodStatusInput handler at
        // the top (Failed is not Running).
        self.awaiting_suspend = false;
        self.clear_artifact_ref(ctx);
        ctx.set_readiness(None);
        ctx.set_placement(None);

        self.last_exit_code = exit_code;
        self.last_failure_reason = Some(reason);

        // Keep the ownership edge — the pod is terminal (Failed) but we
        // retain it for inspectability. It will be reaped on transition out
        // of backoff/failed state.

        self.consecutive_failures += 1;

        // Re-evaluate commitment: no demand after pod death → no reason to retry.
        if !self.effective_demand() {
            self.committed_to_boot = false;
        }
        if self.consecutive_failures >= self.max_retries {
            self.committed_to_boot = false;
        }

        // Enter backoff only if we actually want to retry.
        let want_retry = (self.effective_demand() || self.committed_to_boot)
            && self.consecutive_failures < self.max_retries;
        if want_retry {
            self.in_backoff = true;
            self.backoff_generation += 1;
        } else if !self.effective_demand() {
            // Going dormant — clear failure tracking and reap immediately
            // (no one is looking).
            self.consecutive_failures = 0;
            self.reap_retained_pod(ctx);
        }

        self.reconcile(ctx);
        self.update_timer_signal(ctx);
    }

    /// Called when a pod reports Displaced status (infrastructure loss — worker
    /// disconnect or lease revocation). Not counted as a failure. Cleans up and
    /// immediately reconciles for rescheduling.
    pub(crate) fn on_pod_displaced(&mut self, ctx: &mut impl WorkloadCtx) {
        self.awaiting_suspend = false;
        ctx.set_readiness(None);
        ctx.set_placement(None);

        // Remove ownership edge — pod is terminal (Displaced),
        // so removing the edge triggers self-destruct.
        ctx.set_pod_ownership_edges(vec![]);
        ctx.set_pod_intent(PodIntent::None);
        self.pod_id = None;

        // Do NOT clear artifact_port — the artifact may still be reachable
        // via a shared pool. The scheduler will broadcast ArtifactInvalidated
        // if unreachable.

        // Do NOT increment consecutive_failures — this is infrastructure, not app failure.
        // Do NOT enter in_backoff — allow immediate rescheduling.

        // Re-evaluate commitment.
        if !self.effective_demand() {
            self.committed_to_boot = false;
        }

        self.reconcile(ctx);
        self.update_timer_signal(ctx);
    }

    /// Reap a retained failed pod (if any). Removes the ownership edge so the
    /// terminal pod self-destructs. Only acts on pods held during backoff or
    /// terminal failure — not on actively launching pods.
    fn reap_retained_pod(&mut self, ctx: &mut impl WorkloadCtx) {
        let is_retained_dead = self.pod_id.is_some()
            && !self.pod_running
            && (self.in_backoff || self.consecutive_failures >= self.max_retries);
        if is_retained_dead {
            ctx.set_pod_ownership_edges(vec![]);
            ctx.set_pod_intent(PodIntent::None);
            self.pod_id = None;
        }
    }

    /// Abandon the current pod by removing the ownership edge.
    /// The pod will drive itself to a terminal state and self-destruct.
    /// Any artifact reference is cleared (this is a hard kill).
    pub(crate) fn destroy_current_pod(&mut self, ctx: &mut impl WorkloadCtx) {
        if self.pod_id.is_some() {
            ctx.set_pod_ownership_edges(vec![]);
            ctx.set_pod_intent(PodIntent::None);
            self.pod_id = None;
        }
        // pod_running and pod_worker_id are signal-derived — they will be
        // cleared by PodStatusInput([]) and PodWorkerInput([]) when the
        // abandoned pod removes its reverse edges and self-destructs.
        self.awaiting_suspend = false;
        self.clear_artifact_ref(ctx);
        ctx.set_readiness(None);
        ctx.set_placement(None);
    }

    pub(crate) fn update_timer_signal(&self, ctx: &mut impl WorkloadCtx) {
        let mut timers = vec![];
        if self.in_backoff {
            timers.push(TimerRequest {
                key: WorkloadTimerKey::RetryBackoff,
                generation: self.backoff_generation,
                duration: std::time::Duration::from_millis(500),
            });
        }
        if self.artifact_port.is_some() && !self.artifact_confirmed {
            timers.push(TimerRequest {
                key: WorkloadTimerKey::ArtifactConfirm,
                generation: self.artifact_confirm_gen,
                duration: std::time::Duration::from_millis(100),
            });
        }
        ctx.set_wanted_timers(timers);
    }

    pub(crate) fn update_status_signals(&self, ctx: &mut impl WorkloadCtx) {
        let demand = self.effective_demand() || self.committed_to_boot;
        let is_failed = self.consecutive_failures >= self.max_retries && demand;
        let status = if self.completed {
            WlStatus::Completed {
                exit_code: self.last_exit_code.unwrap_or(0),
            }
        } else if is_failed {
            WlStatus::Failed {
                exit_code: self.last_exit_code,
                reason: self
                    .last_failure_reason
                    .clone()
                    .unwrap_or_default(),
            }
        } else if self.in_backoff {
            WlStatus::RetryBackoff
        } else if self.awaiting_suspend {
            WlStatus::Suspending
        } else if self.artifact_port.is_some() && self.pod_id.is_none() {
            WlStatus::Suspended
        } else if self.pod_running {
            WlStatus::Running
        } else if self.pod_id.is_some() {
            WlStatus::Launching
        } else if !self.has_spec && demand {
            WlStatus::WaitingForSpec
        } else {
            WlStatus::Dormant
        };
        ctx.set_status(status);
        ctx.set_consecutive_failures(self.consecutive_failures);
        ctx.set_spec_stale(
            self.pod_id.is_some() && self.launched_with_spec_version != self.spec_version,
        );
    }

    pub(crate) fn reconcile(&mut self, ctx: &mut impl WorkloadCtx) {
        // If we're waiting for a suspend to complete, don't touch the pod.
        if self.awaiting_suspend {
            return;
        }

        let is_failed = self.consecutive_failures >= self.max_retries;
        let demand = self.effective_demand() || self.committed_to_boot;
        let want_pod = self.has_spec
            && demand
            && !self.in_backoff
            && !is_failed
            && !self.completed;
        self.wants_pod = want_pod;

        if want_pod && self.pod_id.is_none() {
            if self.artifact_port.is_some() && !self.artifact_confirmed {
                // Artifact port referenced but not yet confirmed — wait for
                // confirmation or timeout before creating a pod.
                return;
            }
            // Create new pod — resume from confirmed artifact if available.
            // Keep the artifact reference until the pod reaches Running
            // (on_pod_running). This allows retry from the same artifact
            // if the resume fails.
            let pod = if let Some(port_id) = self.artifact_port {
                PodSm::new_from_artifact(port_id)
            } else {
                PodSm::new()
            };
            let pod_id = ctx.create_pod(pod);
            self.pod_id = Some(pod_id);
            self.launched_with_spec_version = self.spec_version;
            ctx.set_pod_ownership_edges(vec![pod_id]);
            ctx.set_pod_intent(PodIntent::Want);
        } else if want_pod && self.pod_id.is_some() {
            ctx.set_pod_intent(PodIntent::Want);
        } else if !want_pod && self.pod_id.is_some() {
            if self.pod_running && self.suspend_on_idle {
                // Signal pod to suspend — keep edge, pod drives itself to
                // Suspended terminal state.
                ctx.set_pod_intent(PodIntent::Suspend);
                self.awaiting_suspend = true;
            } else if !self.pod_running && (self.in_backoff || is_failed) {
                // Retained failed pod — keep it alive for inspectability.
                // Will be reaped on transition out of backoff/failed.
            } else {
                // Abandon pod (remove edge). Pod will drive itself to
                // terminal and self-destruct.
                ctx.set_pod_ownership_edges(vec![]);
                ctx.set_pod_intent(PodIntent::None);
                self.pod_id = None;
                ctx.set_readiness(None);
                ctx.set_placement(None);
            }
        } else {
            ctx.set_pod_intent(PodIntent::None);
        }
    }
}
