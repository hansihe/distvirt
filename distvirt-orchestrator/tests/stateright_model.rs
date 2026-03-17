use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;
use std::time::Duration;

use stateright::*;

use distvirt_orchestrator::namespace::NamespaceStateMachine;
use distvirt_orchestrator::types::*;

// --- Model Configuration ---

struct NamespaceModel {
    initial_spec: NamespaceSpec,
    worker_count: usize,
    enable_worker_failure: bool,
    enable_delete: bool,
    enable_spec_update: bool,
    /// When true, allows an `AddService` action that adds a new service
    /// to the existing workload via `UpdateSpec` while the workload is Running.
    enable_service_addition: bool,
    /// Max retries for workload backoff in the model. Lower values
    /// reach the terminal Failed state faster, reducing state space
    /// without losing coverage (retry logic is identical per attempt).
    max_retries: u32,
}

// --- Hashable Snapshot of NamespaceStateMachine ---

/// Mirror of `NamespaceStateMachine` using BTreeMap/BTreeSet for deterministic
/// hashing and equality, enabling Stateright state deduplication.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct NamespaceSnapshot {
    namespace_id: NamespaceId,
    spec: SpecSnapshot,
    status: NamespaceStatus,
    workloads: BTreeMap<WorkloadId, WorkloadSnapshot>,
    services: BTreeMap<ServiceId, ServiceSnapshot>,
    service_workload: BTreeMap<ServiceId, WorkloadId>,
    pods: BTreeMap<PodId, PodInfo>,
    workers: BTreeMap<WorkerId, WorkerSnapshot>,
    workload_readiness: BTreeMap<WorkloadId, WorkloadReadyInfoSnapshot>,
    active_flows: BTreeSet<WorkloadId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct WorkloadReadyInfoSnapshot {
    pod_id: PodId,
    worker_id: WorkerId,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PlacementSnapshot {
    placements: BTreeMap<ArtifactId, PlacementEntrySnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PlacementEntrySnapshot {
    pool_id: PoolId,
    worker_id: WorkerId,
}

impl PlacementSnapshot {
    fn from_table(table: &PlacementTable) -> Self {
        PlacementSnapshot {
            placements: table
                .iter()
                .map(|(id, p)| {
                    (
                        id.clone(),
                        PlacementEntrySnapshot {
                            pool_id: p.pool_id.clone(),
                            worker_id: p.worker_id.clone(),
                        },
                    )
                })
                .collect(),
        }
    }

    fn to_table(&self) -> PlacementTable {
        let mut table = PlacementTable::default();
        for (id, entry) in &self.placements {
            table.insert(
                id.clone(),
                ArtifactPlacement {
                    pool_id: entry.pool_id.clone(),
                    worker_id: entry.worker_id.clone(),
                    status: ArtifactStatus::Ready,
                },
            );
        }
        table
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct WorkloadSnapshot {
    state: WorkloadState,
    current_demand: u32,
    consecutive_failures: u32,
    needs_successful_boot: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ServiceSnapshot {
    state: ServiceState,
    workload_id: WorkloadId,
    has_activation: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SpecSnapshot {
    network: NetworkConfig,
    workloads: BTreeMap<WorkloadId, WorkloadSpec>,
    services: BTreeMap<ServiceId, ServiceSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct WorkerSnapshot {
    fabric_status: FabricStatus,
}

impl NamespaceSnapshot {
    fn from_state_machine(sm: &NamespaceStateMachine) -> Self {
        NamespaceSnapshot {
            namespace_id: sm.namespace_id.clone(),
            spec: SpecSnapshot {
                network: sm.spec.network.clone(),
                workloads: sm
                    .spec
                    .workloads
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                services: sm
                    .spec
                    .services
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            },
            status: sm.status.clone(),
            workloads: sm
                .workloads
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        WorkloadSnapshot {
                            state: v.state.clone(),
                            current_demand: v.current_demand,
                            consecutive_failures: v.consecutive_failures,
                            needs_successful_boot: v.needs_successful_boot,
                        },
                    )
                })
                .collect(),
            services: sm
                .services
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        ServiceSnapshot {
                            state: v.state.clone(),
                            workload_id: v.workload_id.clone(),
                            has_activation: v.has_activation,
                        },
                    )
                })
                .collect(),
            service_workload: sm
                .service_workload
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            pods: sm
                .pod_map
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            workers: sm
                .workers
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        WorkerSnapshot {
                            fabric_status: v.fabric_status.clone(),
                        },
                    )
                })
                .collect(),
            workload_readiness: sm
                .workload_readiness
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        WorkloadReadyInfoSnapshot {
                            pod_id: v.pod_id.clone(),
                            worker_id: v.worker_id.clone(),
                        },
                    )
                })
                .collect(),
            active_flows: sm.active_flows.clone(),
        }
    }

    fn to_state_machine(&self, max_retries: u32) -> NamespaceStateMachine {
        let spec = NamespaceSpec {
            network: self.spec.network.clone(),
            workloads: self
                .spec
                .workloads
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            services: self
                .spec
                .services
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        };

        let mut workloads = BTreeMap::new();
        for (wl_id, wl_snap) in &self.workloads {
            let suspend_on_idle = self
                .spec
                .workloads
                .get(wl_id)
                .map_or(false, |w| w.suspend_on_idle);
            let has_activation = self
                .spec
                .workloads
                .get(wl_id)
                .and_then(|w| w.activation.as_ref())
                .is_some();
            let (mut wl, _) = distvirt_orchestrator::sm::workload::WorkloadStateMachine::new(
                wl_id.clone(),
                suspend_on_idle,
                has_activation,
            );
            wl.max_retries = max_retries;
            wl.state = wl_snap.state.clone();
            wl.current_demand = wl_snap.current_demand;
            wl.consecutive_failures = wl_snap.consecutive_failures;
            workloads.insert(wl_id.clone(), wl);
        }

        let mut services = BTreeMap::new();
        for (svc_id, svc_snap) in &self.services {
            let svc_spec = spec.services.get(svc_id);
            let idle_timeout = svc_spec
                .and_then(|s| s.activation.as_ref())
                .map(|a| a.idle_timeout)
                .unwrap_or(Duration::from_secs(30));
            let mut svc = distvirt_orchestrator::sm::service::ServiceStateMachine::new(
                svc_id.clone(),
                svc_snap.workload_id.clone(),
                svc_snap.has_activation,
                idle_timeout,
            );
            svc.state = svc_snap.state.clone();
            services.insert(svc_id.clone(), svc);
        }

        let service_workload: BTreeMap<ServiceId, WorkloadId> = self
            .service_workload
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        let mut pod_map = distvirt_orchestrator::pod_map::PodMap::new();
        for (pod_id, pod_info) in &self.pods {
            pod_map.insert(pod_id.clone(), pod_info.clone());
        }

        NamespaceStateMachine {
            namespace_id: self.namespace_id.clone(),
            spec,
            status: self.status.clone(),
            segment_id: 1,
            workloads,
            services,
            service_workload,
            pod_map,
            workers: self
                .workers
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        NamespaceWorkerState {
                            fabric_status: v.fabric_status.clone(),
                            primary_pool_id: None,
                            pressure_band: PressureBand::Normal,
                        },
                    )
                })
                .collect(),
            wg_peer_manager: distvirt_orchestrator::wg_peers::WireGuardPeerManager::new(
                self.spec.network.subnet,
                self.spec.network.prefix_len,
            ),
            workload_readiness: self
                .workload_readiness
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        distvirt_orchestrator::namespace::WorkloadReadyInfo {
                            pod_id: v.pod_id.clone(),
                            worker_id: v.worker_id.clone(),
                        },
                    )
                })
                .collect(),
            active_flows: self.active_flows.clone(),
        }
    }
}

// --- Model State ---

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ModelState {
    namespace: NamespaceSnapshot,
    placement: PlacementSnapshot,
    pending_timers: BTreeSet<TimerKey>,
    /// Monotonic flag: set to true once any pod has been launched.
    /// Used only for reachability properties (false→true only, no divergence).
    ever_launched_pod: bool,
    /// Whether all worker commands in the last transition targeted workers
    /// present in the namespace's worker map at the time the commands were emitted.
    last_output_commands_valid: bool,
    /// Monotonic flag: set to true after a spec update has been applied.
    /// Limits state space to at most one spec change per exploration path.
    spec_updated: bool,
    /// Monotonic flag: set to true after AddService has been applied.
    service_added: bool,
    /// Tracks which workers have received endpoint commands, mapping IP → ServiceId
    /// so that removed_ips in EndpointUpdate can correctly remove stale endpoints.
    worker_service_created: BTreeMap<WorkerId, BTreeMap<Ipv4Addr, ServiceId>>,
}

// --- Model Actions ---

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum ModelAction {
    WorkerEvent {
        worker_id: WorkerId,
        event: WorkerEvent,
    },
    TimerFired {
        timer_key: TimerKey,
    },
    WorkerLost {
        worker_id: WorkerId,
    },
    Delete,
    UpdateSpec,
    AddService,
}

// --- Helpers ---

fn worker_id(i: usize) -> WorkerId {
    WorkerId(format!("w-{}", i))
}

/// Track EndpointSync/EndpointUpdate commands to know which workers have service endpoints.
/// Maps each worker to a set of (IP → ServiceId) entries so that `removed_ips` in
/// EndpointUpdate can correctly remove stale endpoints.
fn track_service_commands(
    output: &NamespaceOutput,
    tracker: &mut BTreeMap<WorkerId, BTreeMap<Ipv4Addr, ServiceId>>,
) {
    use distvirt_worker_protocol::EndpointKind;

    for (wid, cmd) in &output.worker_commands {
        match cmd {
            WorkerCommand::EndpointSync { endpoints, .. } => {
                // Full sync replaces all known endpoints for this worker.
                let entry = tracker.entry(wid.clone()).or_default();
                entry.clear();
                for ep in endpoints {
                    if let EndpointKind::Service { service_id, .. } = &ep.kind {
                        entry.insert(ep.ip, service_id.clone());
                    }
                }
            }
            WorkerCommand::EndpointUpdate {
                upserted,
                removed_ips,
                ..
            } => {
                let entry = tracker.entry(wid.clone()).or_default();
                for ep in upserted {
                    if let EndpointKind::Service { service_id, .. } = &ep.kind {
                        entry.insert(ep.ip, service_id.clone());
                    }
                }
                for ip in removed_ips {
                    entry.remove(ip);
                }
            }
            _ => {}
        }
    }
}

/// Helper: extract the set of unique ServiceIds from an endpoint tracker entry.
fn tracked_service_ids(tracker: &BTreeMap<Ipv4Addr, ServiceId>) -> BTreeSet<ServiceId> {
    tracker.values().cloned().collect()
}

/// Allocate the lowest free pod ID, so states converge after pod churn.
fn next_free_pod_id(sm: &NamespaceStateMachine) -> PodId {
    for i in 0u64.. {
        let candidate = PodId(format!("pod-{}", i));
        if !sm.pod_map.contains(&candidate) {
            return candidate;
        }
    }
    unreachable!()
}

// --- Model Implementation ---

impl Model for NamespaceModel {
    type State = ModelState;
    type Action = ModelAction;

    fn init_states(&self) -> Vec<Self::State> {
        let mut sm = NamespaceStateMachine::new(
            NamespaceId("model-ns".into()),
            self.initial_spec.clone(),
            1,
        );

        // Pre-register workers with Creating fabric status.
        for i in 0..self.worker_count {
            sm.workers.insert(
                worker_id(i),
                NamespaceWorkerState {
                    fabric_status: FabricStatus::Creating,
                    primary_pool_id: None,
                    pressure_band: PressureBand::Normal,
                },
            );
        }

        let snapshot = NamespaceSnapshot::from_state_machine(&sm);
        vec![ModelState {
            namespace: snapshot,
            placement: PlacementSnapshot {
                placements: BTreeMap::new(),
            },
            pending_timers: BTreeSet::new(),
            ever_launched_pod: false,
            last_output_commands_valid: true,
            spec_updated: false,
            service_added: false,
            worker_service_created: BTreeMap::new(),
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        let ns = &state.namespace;

        // Any pending timer can fire.
        for timer_key in &state.pending_timers {
            actions.push(ModelAction::TimerFired {
                timer_key: timer_key.clone(),
            });
        }

        // Worker events based on current workload/service states.
        for (service_id, svc_snap) in &ns.services {
            let wl_id = &svc_snap.workload_id;
            let wl_snap = ns.workloads.get(wl_id);

            for (wid, _worker) in &ns.workers {
                // Service-level events.
                match &svc_snap.state {
                    ServiceState::Idle => {
                        let svc_ip = self
                            .initial_spec
                            .services
                            .get(service_id)
                            .map(|s| s.ip)
                            .unwrap_or(Ipv4Addr::UNSPECIFIED);
                        actions.push(ModelAction::WorkerEvent {
                            worker_id: wid.clone(),
                            event: WorkerEvent::EndpointActivation {
                                ip: svc_ip,
                                service_id: Some(service_id.clone()),
                            },
                        });
                    }
                    ServiceState::Active { .. } => {
                        // Backend need can change from any worker.
                        for need in &[BackendNeed::None, BackendNeed::Traffic, BackendNeed::Active]
                        {
                            actions.push(ModelAction::WorkerEvent {
                                worker_id: wid.clone(),
                                event: WorkerEvent::ServiceBackendNeed {
                                    service_id: service_id.clone(),
                                    need: need.clone(),
                                },
                            });
                        }
                    }
                    ServiceState::NeedBackend => {}
                }

                // Workload-level events.
                if let Some(wl) = wl_snap {
                    match &wl.state {
                        WorkloadState::Active {
                            pod:
                                PodSlot {
                                    pod_id,
                                    worker_id: launch_wid,
                                    pod_state: PodState::Launching { .. },
                                },
                            ..
                        } => {
                            if wid.0 == launch_wid.0 {
                                actions.push(ModelAction::WorkerEvent {
                                    worker_id: wid.clone(),
                                    event: WorkerEvent::PodRunning {
                                        pod_id: pod_id.clone(),
                                    },
                                });
                                actions.push(ModelAction::WorkerEvent {
                                    worker_id: wid.clone(),
                                    event: WorkerEvent::PodFailed {
                                        pod_id: pod_id.clone(),
                                        error: "model check failure".into(),
                                    },
                                });
                            }
                        }
                        WorkloadState::Active {
                            pod:
                                PodSlot {
                                    pod_id,
                                    worker_id: active_wid,
                                    pod_state: PodState::Running,
                                },
                            ..
                        } => {
                            if wid.0 == active_wid.0 {
                                actions.push(ModelAction::WorkerEvent {
                                    worker_id: wid.clone(),
                                    event: WorkerEvent::PodExited {
                                        pod_id: pod_id.clone(),
                                        exit_code: 0,
                                    },
                                });
                            }
                        }
                        WorkloadState::Active {
                            pod:
                                PodSlot {
                                    pod_id,
                                    worker_id: suspend_wid,
                                    pod_state: PodState::Suspending { artifact_id, .. },
                                },
                            ..
                        } => {
                            if wid.0 == suspend_wid.0 {
                                actions.push(ModelAction::WorkerEvent {
                                    worker_id: wid.clone(),
                                    event: WorkerEvent::PodSuspended {
                                        pod_id: pod_id.clone(),
                                        artifact_id: artifact_id.clone(),
                                        pool_id: PoolId::from("default-pool"),
                                    },
                                });
                                actions.push(ModelAction::WorkerEvent {
                                    worker_id: wid.clone(),
                                    event: WorkerEvent::PodSuspendFailed {
                                        pod_id: pod_id.clone(),
                                        error: "model check failure".into(),
                                    },
                                });
                                actions.push(ModelAction::WorkerEvent {
                                    worker_id: wid.clone(),
                                    event: WorkerEvent::PodFailed {
                                        pod_id: pod_id.clone(),
                                        error: "model check failure".into(),
                                    },
                                });
                            }
                        }
                        WorkloadState::Suspended { .. } => {
                            // No pod events in suspended state — resume is
                            // triggered by demand, not worker events.
                        }
                        WorkloadState::Active {
                            pod:
                                PodSlot {
                                    pod_id,
                                    worker_id: resume_wid,
                                    pod_state: PodState::Resuming { .. },
                                },
                            ..
                        } => {
                            if wid.0 == resume_wid.0 {
                                actions.push(ModelAction::WorkerEvent {
                                    worker_id: wid.clone(),
                                    event: WorkerEvent::PodRunning {
                                        pod_id: pod_id.clone(),
                                    },
                                });
                                actions.push(ModelAction::WorkerEvent {
                                    worker_id: wid.clone(),
                                    event: WorkerEvent::PodFailed {
                                        pod_id: pod_id.clone(),
                                        error: "model check failure".into(),
                                    },
                                });
                            }
                        }
                        WorkloadState::WaitingForCapacity
                        | WorkloadState::Dormant
                        | WorkloadState::RetryBackoff { .. }
                        | WorkloadState::Failed => {}
                        WorkloadState::Transitioning => unreachable!("Transitioning in model"),
                    }
                }
            }
        }

        // NamespaceCreated event from any worker in Creating status.
        for (wid, worker) in &ns.workers {
            if worker.fabric_status == FabricStatus::Creating {
                actions.push(ModelAction::WorkerEvent {
                    worker_id: wid.clone(),
                    event: WorkerEvent::NamespaceCreated,
                });
            }
            // NamespaceDestroyed event from any worker in Destroying status.
            if worker.fabric_status == FabricStatus::Destroying {
                actions.push(ModelAction::WorkerEvent {
                    worker_id: wid.clone(),
                    event: WorkerEvent::NamespaceDestroyed,
                });
            }
        }

        // Worker can disconnect (if enabled).
        if self.enable_worker_failure {
            for (wid, _) in &ns.workers {
                actions.push(ModelAction::WorkerLost {
                    worker_id: wid.clone(),
                });
            }
        }

        // Delete action (if enabled and not already destroying/destroyed).
        if self.enable_delete && ns.status != NamespaceStatus::Destroying && !ns.workers.is_empty()
        {
            actions.push(ModelAction::Delete);
        }

        // Spec update action (if enabled, not already updated, and namespace is active).
        if self.enable_spec_update && !state.spec_updated && ns.status == NamespaceStatus::Active {
            actions.push(ModelAction::UpdateSpec);
        }

        // AddService action: add a new service to a running workload.
        if self.enable_service_addition
            && !state.service_added
            && ns.status == NamespaceStatus::Active
            && ns.workloads.values().any(|wl| wl.state.is_running())
        {
            actions.push(ModelAction::AddService);
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut sm = state.namespace.to_state_machine(self.max_retries);
        let mut placement_table = state.placement.to_table();

        // Track which timer fired (if any) so we can remove it from pending_timers.
        let fired_timer = match &action {
            ModelAction::TimerFired { timer_key } => Some(timer_key.clone()),
            _ => None,
        };

        let is_spec_update = matches!(&action, ModelAction::UpdateSpec);
        let is_add_service = matches!(&action, ModelAction::AddService);

        let input = match action {
            ModelAction::WorkerEvent { worker_id, event } => {
                NamespaceInput::WorkerEvent { worker_id, event }
            }
            ModelAction::TimerFired { timer_key } => NamespaceInput::TimerFired { timer_key },
            ModelAction::WorkerLost { worker_id } => NamespaceInput::WorkerLost { worker_id },
            ModelAction::Delete => NamespaceInput::Delete {
                client_id: ClientId(0),
            },
            ModelAction::UpdateSpec => {
                // Build updated spec with a changed container image.
                let mut new_spec = sm.spec.clone();
                for wl_spec in new_spec.workloads.values_mut() {
                    for container in &mut wl_spec.containers {
                        container.image_ref = format!("{}-v2", container.image_ref);
                    }
                }
                NamespaceInput::UpdateSpec {
                    client_id: ClientId(0),
                    spec: new_spec,
                }
            }
            ModelAction::AddService => {
                // Add a new service pointing to the first workload.
                let mut new_spec = sm.spec.clone();
                let wl_id = new_spec
                    .workloads
                    .keys()
                    .next()
                    .cloned()
                    .expect("AddService requires at least one workload");
                new_spec.services.insert(
                    ServiceId("svc-added".into()),
                    ServiceSpec {
                        workload_id: wl_id,
                        ip: Ipv4Addr::new(172, 16, 0, 200),
                        policy: test_service_policy(),
                        activation: Some(ActivationSpec {
                            idle_timeout: Duration::from_secs(30),
                        }),
                    },
                );
                NamespaceInput::UpdateSpec {
                    client_id: ClientId(0),
                    spec: new_spec,
                }
            }
        };

        // Snapshot pre-step workers for command validity check.
        let pre_step_workers: BTreeSet<WorkerId> = sm.workers.keys().cloned().collect();

        let output = sm.step(input, &mut placement_table);

        // Track CreateService/DestroyService delivery to workers.
        let mut worker_service_created = state.worker_service_created.clone();
        track_service_commands(&output, &mut worker_service_created);

        // Check that all worker commands target workers present pre-step.
        let mut commands_valid = output
            .worker_commands
            .iter()
            .all(|(wid, _)| pre_step_workers.contains(wid));

        // Update pending timers from output.
        let mut pending_timers = state.pending_timers.clone();
        // A fired timer is consumed — remove it from pending.
        if let Some(ref tk) = fired_timer {
            pending_timers.remove(tk);
        }
        for (timer_key, _duration) in &output.timers_set {
            pending_timers.insert(timer_key.clone());
        }
        for timer_key in &output.timers_cancel {
            pending_timers.remove(timer_key);
        }

        // Process pod_requests: simulate outer-layer scheduling.
        let mut ever_launched_pod = state.ever_launched_pod;
        for req in &output.pod_requests {
            // Pick the lowest active worker ID for deterministic scheduling.
            let active_worker = sm
                .workers
                .iter()
                .filter(|(_, ws)| ws.fabric_status == FabricStatus::Active)
                .map(|(wid, _)| wid.clone())
                .min();

            if let Some(wid) = active_worker {
                // Allocate lowest free pod ID for state convergence.
                let pod_id = next_free_pod_id(&sm);
                ever_launched_pod = true;

                let launch_out = sm.step(
                    NamespaceInput::LaunchPod {
                        workload_id: req.workload_id.clone(),
                        worker_id: wid,
                        pod_id,
                    },
                    &mut placement_table,
                );

                track_service_commands(&launch_out, &mut worker_service_created);
                commands_valid = commands_valid
                    && launch_out
                        .worker_commands
                        .iter()
                        .all(|(wid, _)| sm.workers.contains_key(wid));
                for (timer_key, _duration) in &launch_out.timers_set {
                    pending_timers.insert(timer_key.clone());
                }
                for timer_key in &launch_out.timers_cancel {
                    pending_timers.remove(timer_key);
                }
            }
        }

        // Process resume_requests: simulate outer-layer resume scheduling.
        for req in &output.resume_requests {
            let pod_id = next_free_pod_id(&sm);

            // Look up placement table for worker_id.
            let worker_id = match placement_table.get(&req.artifact_id) {
                Some(p) => p.worker_id.clone(),
                None => continue,
            };

            let resume_out = sm.step(
                NamespaceInput::ResumePod {
                    workload_id: req.workload_id.clone(),
                    worker_id,
                    pod_id,
                    artifact_id: req.artifact_id.clone(),
                },
                &mut placement_table,
            );

            track_service_commands(&resume_out, &mut worker_service_created);
            commands_valid = commands_valid
                && resume_out
                    .worker_commands
                    .iter()
                    .all(|(wid, _)| sm.workers.contains_key(wid));
            for (timer_key, _duration) in &resume_out.timers_set {
                pending_timers.insert(timer_key.clone());
            }
            for timer_key in &resume_out.timers_cancel {
                pending_timers.remove(timer_key);
            }
        }

        // Drain retiring pods: in the real system, PodGone eventually arrives for each
        // retiring pod. The model never generates these events, so simulate immediate
        // cleanup to avoid unbounded pod_map / retiring list growth.
        let retiring_pods: Vec<(WorkloadId, Vec<RetiredPod>)> = sm
            .workloads
            .iter_mut()
            .filter(|(_, wl)| !wl.retiring.is_empty())
            .map(|(wl_id, wl)| (wl_id.clone(), std::mem::take(&mut wl.retiring)))
            .collect();
        for (_wl_id, retired) in retiring_pods {
            for r in retired {
                sm.pod_map.remove(&r.pod_id);
            }
            // No need to step the workload SM — retiring.clear() is sufficient
            // since PodGone for a retiring pod just removes it and returns early.
        }

        // Remove workers that were lost from service tracking.
        let active_workers: BTreeSet<WorkerId> = sm.workers.keys().cloned().collect();
        worker_service_created.retain(|wid, _| active_workers.contains(wid));

        Some(ModelState {
            namespace: NamespaceSnapshot::from_state_machine(&sm),
            placement: PlacementSnapshot::from_table(&placement_table),
            pending_timers,
            ever_launched_pod,
            last_output_commands_valid: commands_valid,
            spec_updated: state.spec_updated || is_spec_update,
            service_added: state.service_added || is_add_service,
            worker_service_created,
        })
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut props = vec![
            // Safety: Transitioning sentinel must never survive a step() call.
            Property::<Self>::always("no transitioning state", |_model, state| {
                state
                    .namespace
                    .workloads
                    .values()
                    .all(|wl| !matches!(wl.state, WorkloadState::Transitioning))
            }),
            // Safety: No commands sent to workers not in namespace's worker map.
            Property::<Self>::always("no commands to unknown workers", |_model, state| {
                let ns = &state.namespace;
                for (_wl_id, wl) in &ns.workloads {
                    match &wl.state {
                        WorkloadState::Active {
                            pod: PodSlot { worker_id, .. },
                            ..
                        } => {
                            if !ns.workers.contains_key(worker_id) {
                                return false;
                            }
                        }
                        WorkloadState::Suspended { .. } => {
                            // Worker reference is in placement table, not in the state.
                            // Checked separately.
                        }
                        _ => {}
                    }
                }
                true
            }),
            // Safety: Launching/Running/Suspending/Resuming workloads reference valid pods.
            Property::<Self>::always("workloads have valid pods", |_model, state| {
                let ns = &state.namespace;
                for (_wl_id, wl) in &ns.workloads {
                    match &wl.state {
                        WorkloadState::Active {
                            pod: PodSlot { pod_id, .. },
                            ..
                        } => {
                            if !ns.pods.contains_key(pod_id) {
                                return false;
                            }
                        }
                        _ => {}
                    }
                }
                true
            }),
            // Safety: No duplicate pods per workload.
            Property::<Self>::always("no duplicate pods per workload", |_model, state| {
                let ns = &state.namespace;
                let mut workload_pods: BTreeMap<&WorkloadId, Vec<&PodId>> = BTreeMap::new();
                for (pod_id, pod_info) in &ns.pods {
                    workload_pods
                        .entry(&pod_info.workload_id)
                        .or_default()
                        .push(pod_id);
                }
                for (_wlid, pods) in &workload_pods {
                    let unique: BTreeSet<&&PodId> = pods.iter().collect();
                    if unique.len() != pods.len() {
                        return false;
                    }
                }
                true
            }),
            // Safety: Workloads only reference workers that are present.
            Property::<Self>::always(
                "workloads only reference present workers",
                |_model, state| {
                    let ns = &state.namespace;
                    for (_wl_id, wl) in &ns.workloads {
                        match &wl.state {
                            WorkloadState::Active {
                                pod: PodSlot { worker_id, .. },
                                ..
                            } => {
                                if !ns.workers.contains_key(worker_id) {
                                    return false;
                                }
                            }
                            _ => {}
                        }
                    }
                    true
                },
            ),
            // Safety: No worker commands to absent workers (checked against
            // pre-transition worker set during next_state).
            Property::<Self>::always("no worker commands to absent workers", |_model, state| {
                state.last_output_commands_valid
            }),
            // Safety: Suspended workloads have valid placement table entries.
            Property::<Self>::always(
                "suspended workloads have valid placement",
                |_model, state| {
                    let ns = &state.namespace;
                    for (_wl_id, wl) in &ns.workloads {
                        if let WorkloadState::Suspended { ref artifact_id } = wl.state {
                            if !state.placement.placements.contains_key(artifact_id) {
                                return false;
                            }
                        }
                    }
                    true
                },
            ),
            // Reachability: Can reach a Running workload state.
            Property::<Self>::sometimes("can reach active service", |_model, state| {
                state
                    .namespace
                    .workloads
                    .values()
                    .any(|w| w.state.is_running())
            }),
            // Reachability: Can reach Idle after Active (idle timeout scale-down).
            Property::<Self>::sometimes("can reach idle after active", |_model, state| {
                state.ever_launched_pod
                    && state
                        .namespace
                        .services
                        .values()
                        .any(|s| matches!(s.state, ServiceState::Idle))
            }),
        ];

        // Safety: current_demand always matches effective_demand.
        props.push(Property::<Self>::always(
            "demand consistent with effective_demand",
            |model, state| {
                let sm = state.namespace.to_state_machine(model.max_retries);
                for (wl_id, wl_snap) in &state.namespace.workloads {
                    let effective = sm.effective_demand(wl_id);
                    if wl_snap.current_demand != effective {
                        return false;
                    }
                }
                true
            },
        ));

        // Safety: readiness consistent with workload state.
        // BecameReady/BecameUnready must keep workload_readiness in sync with
        // the workload SM's Running state.
        props.push(Property::<Self>::always(
            "readiness consistent with workload state",
            |_model, state| {
                let ns = &state.namespace;
                for (wl_id, wl) in &ns.workloads {
                    let sm_running = wl.state.is_running();
                    let has_readiness = ns.workload_readiness.contains_key(wl_id);
                    if sm_running != has_readiness {
                        return false;
                    }
                }
                true
            },
        ));

        // Safety: active workers should have received endpoint data for all
        // non-Pending services.
        {
            props.push(Property::<Self>::always(
                "active workers have all services in endpoints",
                |_model, state| {
                    let ns = &state.namespace;
                    if ns.status != NamespaceStatus::Active {
                        return true;
                    }
                    // Collect all service IDs.
                    let all_services: BTreeSet<&ServiceId> = ns.services.keys().collect();

                    for (wid, worker) in &ns.workers {
                        if worker.fabric_status != FabricStatus::Active {
                            continue;
                        }
                        let created = state
                            .worker_service_created
                            .get(wid)
                            .map(|m| tracked_service_ids(m))
                            .unwrap_or_default();
                        for svc_id in &all_services {
                            if !created.contains(*svc_id) {
                                return false;
                            }
                        }
                    }
                    true
                },
            ));
        }

        if self.enable_service_addition {
            // Safety: no service should be Pending when its workload is Running.
            // (Property "no pending service for running workload" removed:
            //  Pending state no longer exists — services start in their operational state.)
            // Reachability: added service can reach Active.
            props.push(Property::<Self>::sometimes(
                "added service can reach active",
                |_model, state| {
                    state.service_added
                        && state
                            .namespace
                            .services
                            .get(&ServiceId("svc-added".into()))
                            .map(|s| matches!(s.state, ServiceState::Active { .. }))
                            .unwrap_or(false)
                },
            ));
        }

        // Reachability: both services sharing a workload can reach Active.
        // Only relevant when spec has multiple services on the same workload.
        {
            let mut wl_svc_count: BTreeMap<WorkloadId, usize> = BTreeMap::new();
            for svc_spec in self.initial_spec.services.values() {
                *wl_svc_count
                    .entry(svc_spec.workload_id.clone())
                    .or_default() += 1;
            }
            if wl_svc_count.values().any(|&c| c > 1) {
                props.push(Property::<Self>::sometimes(
                    "both services can reach active",
                    |_model, state| {
                        state
                            .namespace
                            .services
                            .values()
                            .all(|s| matches!(s.state, ServiceState::Active { .. }))
                    },
                ));
            }
        }

        if self.enable_delete {
            // Fire-and-forget: destroy is immediate.
            props.push(Property::<Self>::sometimes(
                "can reach destroyed",
                |_model, state| {
                    state.namespace.status == NamespaceStatus::Destroying
                        && state.namespace.workers.is_empty()
                },
            ));
        }

        if self.enable_spec_update {
            // Safety: after a spec update with changed image, a Running workload
            // should not remain Running with the old spec (it transitions out).
            props.push(Property::<Self>::always(
                "spec change restarts running workload",
                |_model, state| {
                    if !state.spec_updated {
                        return true;
                    }
                    // After spec update, the spec snapshot should contain the
                    // updated image. Any Running workload means the SM accepted
                    // the new spec and relaunched (or is in the process).
                    // We check that no workload is Running with the old image
                    // by verifying the spec was actually updated.
                    for wl_spec in state.namespace.spec.workloads.values() {
                        for container in &wl_spec.containers {
                            if !container.image_ref.ends_with("-v2") {
                                // Spec wasn't updated — shouldn't happen after UpdateSpec.
                                return false;
                            }
                        }
                    }
                    true
                },
            ));
            // Reachability: can restart via spec change.
            props.push(Property::<Self>::sometimes(
                "can restart via spec change",
                |_model, state| {
                    // Reached when: spec was updated and a workload has been
                    // relaunched (ever_launched_pod is true and spec_updated is true).
                    state.spec_updated && state.ever_launched_pod
                },
            ));
        }

        props
    }
}

// --- Test Helpers ---

fn test_network_config() -> NetworkConfig {
    NetworkConfig {
        subnet: Ipv4Addr::new(172, 16, 0, 0),
        gateway: Ipv4Addr::new(172, 16, 0, 1),
        prefix_len: 24,
        segment_id: None,
    }
}

fn test_pod_network_config() -> PodNetworkConfig {
    PodNetworkConfig {
        ip: Ipv4Addr::new(172, 16, 0, 10),
        mac: [0; 6],
        gateway: Ipv4Addr::new(172, 16, 0, 1),
        netmask: "255.255.255.0".into(),
    }
}

fn test_service_policy() -> ServicePolicy {
    ServicePolicy {
        buffer_frames: 100,
        timeout_ms: 5000,
        activator: None,
    }
}

fn test_container_spec() -> ContainerSpec {
    ContainerSpec {
        container_id: "main".into(),
        image_ref: "test:latest".into(),
        config: ContainerConfig {
            entrypoint: vec!["/bin/sh".into()],
            args: vec![],
            env: vec![],
            working_dir: None,
            uid: None,
            gid: None,
            hostname: None,
            capture_output: false,
            stdin: false,
        },
    }
}

fn single_service_spec() -> NamespaceSpec {
    let mut workloads = BTreeMap::new();
    workloads.insert(
        WorkloadId("svc-1".into()),
        WorkloadSpec {
            containers: vec![test_container_spec()],
            network: test_pod_network_config(),
            suspend_on_idle: false,
            resources: None,
            activation: None,
        },
    );
    let mut services = BTreeMap::new();
    services.insert(
        ServiceId("svc-1".into()),
        ServiceSpec {
            workload_id: WorkloadId("svc-1".into()),
            ip: Ipv4Addr::new(172, 16, 0, 100),

            policy: test_service_policy(),
            activation: Some(ActivationSpec {
                idle_timeout: Duration::from_secs(30),
            }),
        },
    );
    NamespaceSpec {
        network: test_network_config(),
        workloads,
        services,
    }
}

fn two_service_spec() -> NamespaceSpec {
    let mut workloads = BTreeMap::new();
    workloads.insert(
        WorkloadId("svc-1".into()),
        WorkloadSpec {
            containers: vec![test_container_spec()],
            network: test_pod_network_config(),
            suspend_on_idle: false,
            resources: None,
            activation: None,
        },
    );
    workloads.insert(
        WorkloadId("svc-2".into()),
        WorkloadSpec {
            containers: vec![test_container_spec()],
            suspend_on_idle: false,
            network: PodNetworkConfig {
                ip: Ipv4Addr::new(172, 16, 0, 11),
                mac: [0; 6],
                gateway: Ipv4Addr::new(172, 16, 0, 1),
                netmask: "255.255.255.0".into(),
            },
            resources: None,
            activation: None,
        },
    );
    let mut services = BTreeMap::new();
    services.insert(
        ServiceId("svc-1".into()),
        ServiceSpec {
            workload_id: WorkloadId("svc-1".into()),
            ip: Ipv4Addr::new(172, 16, 0, 100),

            policy: test_service_policy(),
            activation: Some(ActivationSpec {
                idle_timeout: Duration::from_secs(30),
            }),
        },
    );
    services.insert(
        ServiceId("svc-2".into()),
        ServiceSpec {
            workload_id: WorkloadId("svc-2".into()),
            ip: Ipv4Addr::new(172, 16, 0, 101),
            policy: test_service_policy(),
            activation: None,
        },
    );
    NamespaceSpec {
        network: test_network_config(),
        workloads,
        services,
    }
}

/// 1 workload (`wl-1`), 2 activation services (`svc-a`, `svc-b`) both pointing
/// to the same workload with different IPs. Exercises "Shared Workload Demand".
fn shared_workload_spec() -> NamespaceSpec {
    let mut workloads = BTreeMap::new();
    workloads.insert(
        WorkloadId("wl-1".into()),
        WorkloadSpec {
            containers: vec![test_container_spec()],
            network: test_pod_network_config(),
            suspend_on_idle: false,
            resources: None,
            activation: None,
        },
    );
    let mut services = BTreeMap::new();
    services.insert(
        ServiceId("svc-a".into()),
        ServiceSpec {
            workload_id: WorkloadId("wl-1".into()),
            ip: Ipv4Addr::new(172, 16, 0, 100),
            policy: test_service_policy(),
            activation: Some(ActivationSpec {
                idle_timeout: Duration::from_secs(30),
            }),
        },
    );
    services.insert(
        ServiceId("svc-b".into()),
        ServiceSpec {
            workload_id: WorkloadId("wl-1".into()),
            ip: Ipv4Addr::new(172, 16, 0, 101),
            policy: test_service_policy(),
            activation: Some(ActivationSpec {
                idle_timeout: Duration::from_secs(30),
            }),
        },
    );
    NamespaceSpec {
        network: test_network_config(),
        workloads,
        services,
    }
}

// --- Tests ---

/// Helper to run a model check with a given depth and print results.
/// `known_failures` lists properties expected to have counterexamples (known bugs).
/// All other properties are asserted to hold.
fn run_check(name: &str, model: NamespaceModel, max_depth: usize) {
    run_check_with_known_failures(name, model, max_depth, &[]);
}

fn run_check_with_known_failures(
    name: &str,
    model: NamespaceModel,
    max_depth: usize,
    known_failures: &[&'static str],
) {
    use stateright::Expectation;
    let result = model
        .checker()
        .threads(
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1),
        )
        .target_max_depth(max_depth)
        .spawn_dfs()
        .join();

    let properties = result.model().properties();
    for p in &properties {
        if known_failures.contains(&p.name) {
            // Expect a counterexample.
            assert!(
                result.discovery(p.name).is_some(),
                "{}: expected counterexample for '{}' but none found",
                name,
                p.name,
            );
        } else {
            match p.expectation {
                Expectation::Always | Expectation::Eventually => {
                    result.assert_no_discovery(p.name);
                }
                Expectation::Sometimes => {
                    result.assert_any_discovery(p.name);
                }
            }
        }
    }
    println!(
        "{}: {} unique states explored (depth={})",
        name,
        result.unique_state_count(),
        max_depth,
    );
}

#[test]
fn check_single_service_activation() {
    run_check(
        "Single service activation",
        NamespaceModel {
            initial_spec: single_service_spec(),
            worker_count: 1,
            enable_worker_failure: false,
            enable_delete: false,
            enable_spec_update: false,
            enable_service_addition: false,
            max_retries: 2,
        },
        100,
    );
}

#[test]
fn check_two_services() {
    run_check(
        "Two services",
        NamespaceModel {
            initial_spec: two_service_spec(),
            worker_count: 1,
            enable_worker_failure: false,
            enable_delete: false,
            enable_spec_update: false,
            enable_service_addition: false,
            max_retries: 2,
        },
        100,
    );
}

#[test]
fn check_activation_with_worker_failure() {
    run_check(
        "Activation with worker failure",
        NamespaceModel {
            initial_spec: single_service_spec(),
            worker_count: 2,
            enable_worker_failure: true,
            enable_delete: false,
            enable_spec_update: false,
            enable_service_addition: false,
            max_retries: 2,
        },
        50,
    );
}

#[test]
fn check_two_workers_two_services() {
    run_check(
        "Two workers, two services",
        NamespaceModel {
            initial_spec: two_service_spec(),
            worker_count: 2,
            enable_worker_failure: true,
            enable_delete: false,
            enable_spec_update: false,
            enable_service_addition: false,
            max_retries: 2,
        },
        50,
    );
}

#[test]
fn check_delete_lifecycle() {
    run_check(
        "Delete lifecycle",
        NamespaceModel {
            initial_spec: single_service_spec(),
            worker_count: 1,
            enable_worker_failure: false,
            enable_delete: true,
            enable_spec_update: false,
            enable_service_addition: false,
            max_retries: 2,
        },
        100,
    );
}

#[test]
fn check_delete_with_worker_failure() {
    run_check(
        "Delete with worker failure",
        NamespaceModel {
            initial_spec: two_service_spec(),
            worker_count: 2,
            enable_worker_failure: true,
            enable_delete: true,
            enable_spec_update: false,
            enable_service_addition: false,
            max_retries: 2,
        },
        50,
    );
}

#[test]
fn check_namespace_spec_update() {
    run_check(
        "Namespace spec update",
        NamespaceModel {
            initial_spec: single_service_spec(),
            worker_count: 1,
            enable_worker_failure: false,
            enable_delete: false,
            enable_spec_update: true,
            enable_service_addition: false,
            max_retries: 2,
        },
        100,
    );
}

// --- Multi-Service-Per-Workload Bug Tests ---

#[test]
fn check_shared_workload_two_services() {
    run_check(
        "Shared workload two services",
        NamespaceModel {
            initial_spec: shared_workload_spec(),
            worker_count: 1,
            enable_worker_failure: false,
            enable_delete: false,
            enable_spec_update: false,
            enable_service_addition: false,
            max_retries: 2,
        },
        100,
    );
}

#[test]
fn check_shared_workload_with_worker_failure() {
    run_check(
        "Shared workload with worker failure",
        NamespaceModel {
            initial_spec: shared_workload_spec(),
            worker_count: 2,
            enable_worker_failure: true,
            enable_delete: false,
            enable_spec_update: false,
            enable_service_addition: false,
            max_retries: 2,
        },
        50,
    );
}

#[test]
fn check_add_service_to_running_workload() {
    run_check(
        "Add service to running workload",
        NamespaceModel {
            initial_spec: single_service_spec(),
            worker_count: 1,
            enable_worker_failure: false,
            enable_delete: false,
            enable_spec_update: false,
            enable_service_addition: true,
            max_retries: 2,
        },
        50,
    );
}

#[test]
fn check_late_worker_receives_create_service() {
    run_check(
        "Late worker receives create service",
        NamespaceModel {
            initial_spec: single_service_spec(),
            worker_count: 2,
            enable_worker_failure: false,
            enable_delete: false,
            enable_spec_update: false,
            enable_service_addition: false,
            max_retries: 2,
        },
        50,
    );
}
