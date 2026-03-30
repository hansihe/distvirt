use std::collections::{BTreeMap, HashMap};
use std::net::Ipv4Addr;
use std::time::Duration;

use tonic::Status;

use distvirt_client_protocol::proto;

use crate::types::*;

// --- Proto -> Internal conversions ---

pub(super) fn convert_proto_spec(spec: proto::NamespaceSpec) -> Result<NamespaceSpecInput, Status> {
    let network = spec
        .network
        .ok_or_else(|| Status::invalid_argument("missing network config"))?;
    let network = parse_network_config(&network.subnet)?;

    let mut workloads = BTreeMap::new();
    for (id, wl) in spec.workloads {
        workloads.insert(WorkloadName(id), convert_proto_workload_input(wl)?);
    }

    let mut services = BTreeMap::new();
    for (id, svc) in spec.services {
        services.insert(id, convert_proto_service_input(svc)?);
    }

    Ok(NamespaceSpecInput {
        network,
        workloads,
        services,
    })
}

fn parse_network_config(subnet_str: &str) -> Result<NetworkConfig, Status> {
    // Parse "10.0.0.0/24" into subnet, gateway, prefix_len.
    let parts: Vec<&str> = subnet_str.split('/').collect();
    if parts.len() != 2 {
        return Err(Status::invalid_argument(format!(
            "invalid subnet format: '{}', expected CIDR notation",
            subnet_str
        )));
    }
    let subnet: Ipv4Addr = parts[0]
        .parse()
        .map_err(|_| Status::invalid_argument(format!("invalid subnet IP: '{}'", parts[0])))?;
    let prefix_len: u8 = parts[1]
        .parse()
        .map_err(|_| Status::invalid_argument(format!("invalid prefix length: '{}'", parts[1])))?;

    // Gateway is subnet + 1 (e.g., 10.0.0.0/24 -> gateway 10.0.0.1).
    let subnet_u32 = u32::from(subnet);
    let gateway = Ipv4Addr::from(subnet_u32 + 1);

    Ok(NetworkConfig {
        subnet,
        gateway,
        prefix_len,
        segment_id: None,
    })
}

fn convert_proto_workload_input(wl: proto::WorkloadSpec) -> Result<WorkloadSpecInput, Status> {
    let run_policy = convert_proto_run_policy(wl.run_policy());

    // IP is optional: empty string = auto-assign, non-empty = explicit override.
    let explicit_ip = if let Some(network) = &wl.network {
        if network.ip.is_empty() {
            None
        } else {
            Some(network.ip.parse::<Ipv4Addr>().map_err(|_| {
                Status::invalid_argument(format!("invalid workload IP: '{}'", network.ip))
            })?)
        }
    } else {
        None
    };

    let containers = wl
        .containers
        .into_iter()
        .map(convert_proto_container_spec)
        .collect::<Result<Vec<_>, _>>()?;

    let resources = wl.resources.map(|r| ResourceRequirements {
        requests: r.requests.map(|v| ResourceValues {
            memory_mib: v.memory_mb,
            vcpus: v.vcpus,
        }),
        limits: r.limits.map(|v| ResourceValues {
            memory_mib: v.memory_mb,
            vcpus: v.vcpus,
        }),
    });

    let activation = wl.activation.map(|act| ActivationSpec {
        idle_timeout: Duration::from_millis(act.idle_timeout_ms),
    });

    let volumes = wl
        .volumes
        .into_iter()
        .map(|v| {
            let volume_type = match v.volume_type {
                Some(proto::volume_spec::VolumeType::EmptyDir(ed)) => {
                    VolumeType::EmptyDir { size_mb: ed.size_mb }
                }
                Some(proto::volume_spec::VolumeType::ConfigData(cd)) => {
                    VolumeType::ConfigData {
                        files: cd
                            .files
                            .into_iter()
                            .map(|f| ConfigDataFile {
                                path: f.path,
                                content: f.content,
                                mode: f.mode,
                            })
                            .collect(),
                    }
                }
                None => VolumeType::EmptyDir { size_mb: 0 },
            };
            VolumeSpec {
                name: v.name,
                volume_type,
            }
        })
        .collect();

    Ok(WorkloadSpecInput {
        explicit_ip,
        containers,
        suspend_on_idle: wl.suspend_on_idle,
        resources,
        activation,
        run_policy,
        respects_demand: wl.respects_demand,
        volumes,
        labels: wl.labels.into_iter().collect(),
    })
}

fn convert_proto_run_policy(policy: proto::RunPolicy) -> RunPolicy {
    match policy {
        proto::RunPolicy::Service => RunPolicy::Service,
        proto::RunPolicy::Job => RunPolicy::Job,
    }
}

fn convert_proto_container_spec(c: proto::ContainerSpec) -> Result<ContainerSpec, Status> {
    let config = c.config.unwrap_or_default();

    let volume_mounts = config
        .volume_mounts
        .into_iter()
        .map(|m| VolumeMountSpec {
            name: m.name,
            mount_path: m.mount_path,
        })
        .collect();

    Ok(ContainerSpec {
        container_id: c.name,
        image_ref: c.image,
        config: ContainerConfig {
            command: if config.has_command { Some(config.command) } else { None },
            args: if config.has_args { Some(config.args) } else { None },
            env: config
                .env
                .into_iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect(),
            working_dir: if config.working_dir.is_empty() {
                None
            } else {
                Some(config.working_dir)
            },
            user: if config.user.is_empty() {
                None
            } else {
                Some(config.user)
            },
            hostname: if config.hostname.is_empty() {
                None
            } else {
                Some(config.hostname)
            },
            capture_output: true,
            stdin: false,
            volume_mounts,
        },
    })
}

fn convert_proto_service_input(svc: proto::ServiceSpec) -> Result<ServiceSpecInput, Status> {
    // IP is optional: empty string = auto-assign, non-empty = explicit override.
    let explicit_ip = if let Some(network) = &svc.network {
        if network.ip.is_empty() {
            None
        } else {
            Some(network.ip.parse::<Ipv4Addr>().map_err(|_| {
                Status::invalid_argument(format!("invalid service IP: '{}'", network.ip))
            })?)
        }
    } else {
        None
    };

    let ports: Vec<crate::types::PortConfig> = svc
        .ports
        .into_iter()
        .map(|p| {
            let activator = match p.activator {
                Some(proto::port_spec::Activator::Tcp(tcp)) => {
                    let max_flows = if tcp.max_flows == 0 { 1024 } else { tcp.max_flows };
                    Some(crate::types::ActivatorKind::Tcp { max_flows })
                }
                Some(proto::port_spec::Activator::Http2(_)) => {
                    Some(crate::types::ActivatorKind::Http2)
                }
                None => None,
            };
            crate::types::PortConfig {
                port: p.port as u16,
                target_port: if p.target_port == 0 { p.port as u16 } else { p.target_port as u16 },
                activator,
            }
        })
        .collect();

    let has_activation = ports.iter().any(|p| p.activator.is_some());

    if has_activation {
        let all_have = ports.iter().all(|p| p.activator.is_some());
        if !all_have {
            return Err(Status::invalid_argument(
                "mixed activated/passthrough ports on the same service are not allowed; \
                 all ports must have activators or none",
            ));
        }
    }

    let idle_timeout = if svc.idle_timeout_ms > 0 {
        Duration::from_millis(svc.idle_timeout_ms)
    } else if has_activation {
        Duration::from_secs(30)
    } else {
        Duration::ZERO
    };

    let buffer_frames = if svc.buffer_frames == 0 { 64 } else { svc.buffer_frames };
    let buffer_timeout_ms = if svc.buffer_timeout_ms == 0 { 5000 } else { svc.buffer_timeout_ms };

    Ok(ServiceSpecInput {
        workload_id: WorkloadName(svc.workload_id),
        explicit_ip,
        ports,
        has_activation,
        idle_timeout,
        buffer_frames,
        buffer_timeout_ms,
        labels: svc.labels.into_iter().collect(),
    })
}

// --- Proto -> Internal conversions (patch) ---

pub(super) fn convert_proto_patch(
    req: proto::PatchNamespaceRequest,
) -> Result<crate::types::NamespacePatchInput, Status> {
    let mut workloads = BTreeMap::new();
    for (name, wl) in req.workloads {
        workloads.insert(WorkloadName(name), convert_proto_workload_input(wl)?);
    }

    let mut services = BTreeMap::new();
    for (name, svc) in req.services {
        services.insert(name, convert_proto_service_input(svc)?);
    }

    Ok(crate::types::NamespacePatchInput {
        workloads,
        services,
        remove_workloads: req.remove_workloads.into_iter().map(WorkloadName).collect(),
        remove_services: req.remove_services,
    })
}

// --- Internal -> Proto conversions (workers/pods) ---

pub(super) fn convert_worker_query_info(
    info: crate::core::orchestrator::worker_state::WorkerQueryInfo,
) -> proto::WorkerInfo {
    proto::WorkerInfo {
        worker_id: info.worker_id.0.to_string(),
        max_pods: info.max_pods,
        available_memory_mb: info.available_memory_mb,
        active_pods: info.active_pods,
    }
}

pub(super) fn convert_pod_status_report(pod: PodStatusReport) -> proto::PodInfo {
    proto::PodInfo {
        pod_id: pod.pod_id.0.to_string(),
        workload_id: pod.workload_id.0.clone(),
        worker_id: pod.worker_id.0.to_string(),
        ip: pod.ip,
        mac: String::new(),
        state: convert_pod_state(&pod.state),
    }
}

fn convert_pod_state(state: &PodStatus) -> i32 {
    match state {
        PodStatus::Launching => proto::PodState::Launching as i32,
        PodStatus::Running => proto::PodState::Running as i32,
        PodStatus::Suspending => proto::PodState::Suspending as i32,
        PodStatus::Suspended => proto::PodState::Suspended as i32,
        PodStatus::Resuming => proto::PodState::Resuming as i32,
        PodStatus::Finished { .. } => proto::PodState::Finished as i32,
        PodStatus::Failed { .. } => proto::PodState::Failed as i32,
        PodStatus::Displaced => proto::PodState::Displaced as i32,
    }
}

// --- Internal -> Proto conversions (namespaces) ---

pub(super) fn convert_namespace_state(status: &NamespaceStatus) -> i32 {
    match status {
        NamespaceStatus::Creating => proto::NamespaceState::Creating as i32,
        NamespaceStatus::Active => proto::NamespaceState::Active as i32,
        NamespaceStatus::Destroying => proto::NamespaceState::Destroying as i32,
    }
}

pub(super) fn convert_status_report(report: NamespaceStatusReport) -> proto::NamespaceStatusReport {
    let mut workloads: HashMap<String, proto::WorkloadStatusReport> = HashMap::new();
    let mut services: HashMap<String, proto::ServiceStatusReport> = HashMap::new();

    for (wl_id, wl) in &report.workloads {
        // Look up pod/worker info from the pods collection if this workload has a pod.
        let (pod_id_ref, worker_id_ref) = wl
            .pod_id
            .as_ref()
            .and_then(|pid| {
                report
                    .pods
                    .get(pid)
                    .map(|p| (Some(&p.pod_id), Some(&p.worker_id)))
            })
            .unwrap_or((wl.pod_id.as_ref(), None));

        workloads.insert(
            wl_id.0.clone(),
            proto::WorkloadStatusReport {
                state: Some(convert_workload_state(
                    &wl.state,
                    &pod_id_ref.cloned(),
                    &worker_id_ref.cloned(),
                )),
                spliced: false,
                ip: wl.ip.clone(),
                labels: wl.labels.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                restart_count: wl.restart_count,
            },
        );
    }

    for (svc_id, svc) in &report.services {
        // Look up pod/worker from the linked workload's pod.
        let (pod_id, worker_id) = report
            .workloads
            .get(&svc.workload_id)
            .and_then(|wl| wl.pod_id.as_ref())
            .and_then(|pid| {
                report
                    .pods
                    .get(pid)
                    .map(|p| (Some(p.pod_id.clone()), Some(p.worker_id.clone())))
            })
            .unwrap_or((None, None));

        services.insert(
            svc_id.clone(),
            proto::ServiceStatusReport {
                workload_id: svc.workload_id.0.clone(),
                state: Some(convert_service_state(
                    &svc.service_state,
                    &pod_id,
                    &worker_id,
                    &svc.backend_need,
                )),
                activation_enabled: svc.activation_enabled,
                spliced: false,
                ip: svc.ip.clone(),
                mac: String::new(),
                labels: svc.labels.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            },
        );
    }

    let mut pods: HashMap<String, proto::PodStatusReportEntry> = HashMap::new();
    for (pod_id, pod) in &report.pods {
        pods.insert(
            pod_id.0.to_string(),
            proto::PodStatusReportEntry {
                workload_id: pod.workload_id.0.clone(),
                worker_id: pod.worker_id.0.to_string(),
                ip: pod.ip.clone(),
                state: convert_pod_state(&pod.state),
            },
        );
    }

    proto::NamespaceStatusReport {
        namespace_id: report.namespace_id.name.clone(),
        state: convert_namespace_state(&report.status),
        workloads,
        services,
        pods,
    }
}

fn convert_workload_state(
    state: &WorkloadStatus,
    pod_id: &Option<PodId>,
    worker_id: &Option<WorkerId>,
) -> proto::WorkloadState {
    let pod_id_str = || pod_id.as_ref().map(|p| p.0.to_string()).unwrap_or_default();
    let worker_id_str = || worker_id.as_ref().map(|w| w.0.to_string()).unwrap_or_default();

    let state = match state {
        WorkloadStatus::Dormant => proto::workload_state::State::Dormant(proto::WorkloadDormant {}),
        WorkloadStatus::WaitingForSpec => {
            proto::workload_state::State::WaitingForSpec(proto::WorkloadWaitingForSpec {})
        }
        WorkloadStatus::Launching => proto::workload_state::State::Launching(proto::WorkloadLaunching {
            pod_id: pod_id_str(),
            worker_id: worker_id_str(),
        }),
        WorkloadStatus::Running => proto::workload_state::State::Running(proto::WorkloadRunning {
            pod_id: pod_id_str(),
            worker_id: worker_id_str(),
        }),
        WorkloadStatus::Suspending => proto::workload_state::State::Suspending(proto::WorkloadSuspending {
            pod_id: pod_id_str(),
            worker_id: worker_id_str(),
        }),
        WorkloadStatus::Suspended => {
            proto::workload_state::State::Suspended(proto::WorkloadSuspended {})
        }
        WorkloadStatus::RetryBackoff => {
            proto::workload_state::State::RetryBackoff(proto::WorkloadRetryBackoff {})
        }
        WorkloadStatus::Failed {
            exit_code,
            reason,
        } => proto::workload_state::State::Failed(proto::WorkloadFailed {
            exit_code: *exit_code,
            reason: reason.clone(),
        }),
        WorkloadStatus::Completed { exit_code } => {
            proto::workload_state::State::Completed(proto::WorkloadCompleted {
                exit_code: *exit_code,
            })
        }
    };
    proto::WorkloadState { state: Some(state) }
}

fn convert_service_state(
    state: &ServiceStatus,
    pod_id: &Option<PodId>,
    worker_id: &Option<WorkerId>,
    backend_need: &Option<BackendNeed>,
) -> proto::ServiceState {
    let state = match state {
        ServiceStatus::Pending => proto::service_state::State::Pending(proto::ServicePending {}),
        ServiceStatus::Idle => proto::service_state::State::Idle(proto::ServiceIdle {}),
        ServiceStatus::NeedBackend => proto::service_state::State::NeedBackend(proto::ServiceNeedBackend {}),
        ServiceStatus::Active => proto::service_state::State::Active(proto::ServiceActive {
            pod_id: pod_id.as_ref().map(|p| p.0.to_string()).unwrap_or_default(),
            worker_id: worker_id.as_ref().map(|w| w.0.to_string()).unwrap_or_default(),
            backend_need: convert_backend_need(backend_need),
        }),
    };
    proto::ServiceState { state: Some(state) }
}

fn convert_backend_need(need: &Option<BackendNeed>) -> i32 {
    match need {
        Some(BackendNeed::None) => proto::BackendNeed::None as i32,
        Some(BackendNeed::Traffic) => proto::BackendNeed::Traffic as i32,
        Some(BackendNeed::Active) => proto::BackendNeed::Active as i32,
        _ => proto::BackendNeed::Unspecified as i32,
    }
}

// --- LogChunk -> Proto conversion ---

pub(super) fn convert_log_chunk(chunk: crate::log_bus::LogChunk) -> proto::LogChunk {
    let timestamp_unix_ms = {
        // Convert Instant to system time approximation.
        let elapsed = chunk.timestamp.elapsed();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        now_ms - elapsed.as_millis() as i64
    };
    proto::LogChunk {
        workload_name: chunk.workload_name.unwrap_or_default(),
        pod_id: chunk.pod_id.0.to_string(),
        container_id: chunk.container_id,
        data: chunk.data,
        timestamp_unix_ms,
        seq: chunk.seq,
    }
}

// --- Observability Event -> Proto Event conversion ---

use crate::adapter::observability::{
    EndpointEventKind, EndpointObservabilityEvent, ObservabilityEvent, PodEventKind,
    PodObservabilityEvent, WorkloadEventKind, WorkloadObservabilityEvent,
};
use crate::id_registry::IdRegistry;
use crate::sm::{self, endpoint::EndpointStatus};

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Convert an `ObservabilityEvent` to zero or more proto `NamespaceEvent`s.
/// Uses the IdRegistry to resolve router IDs to protocol-level string names.
pub(super) fn convert_observability_event(
    event: &ObservabilityEvent,
    registry: &IdRegistry,
) -> Vec<proto::NamespaceEvent> {
    match event {
        ObservabilityEvent::Pod(pod_event) => convert_pod_obs_event(pod_event, registry),
        ObservabilityEvent::Workload(wl_event) => convert_workload_obs_event(wl_event, registry),
        ObservabilityEvent::Endpoint(ep_event) => convert_endpoint_obs_event(ep_event, registry),
    }
}

fn convert_pod_obs_event(
    event: &PodObservabilityEvent,
    registry: &IdRegistry,
) -> Vec<proto::NamespaceEvent> {
    let workload_name = registry
        .pod_workload_name(&event.pod_id)
        .unwrap_or_default();
    let pod_id_str = event.pod_id.0.to_string();

    let inner = match &event.event {
        PodEventKind::Created => Some(proto::pod_event::Event::Created(proto::PodCreated {})),
        PodEventKind::Reaped { .. } => {
            Some(proto::pod_event::Event::Reaped(proto::PodReaped {}))
        }
        PodEventKind::WorkerChanged { new, .. } => {
            if let Some(worker_id) = new {
                Some(proto::pod_event::Event::Scheduled(proto::PodScheduled {
                    worker_id: worker_id.0.to_string(),
                }))
            } else {
                return vec![];
            }
        }
        PodEventKind::StatusChanged { old, new } => convert_pod_status_transition(old, new),
        PodEventKind::MemoryConstrained { reason } => {
            let proto_reason = match reason {
                distvirt_worker_protocol::MemoryConstraintReason::BalloonExhausted => {
                    proto::ConstraintReason::BalloonExhausted
                }
                distvirt_worker_protocol::MemoryConstraintReason::DeflationStalled => {
                    proto::ConstraintReason::DeflationStalled
                }
            };
            Some(proto::pod_event::Event::MemoryConstrained(
                proto::PodMemoryConstrained {
                    reason: proto_reason.into(),
                },
            ))
        }
        PodEventKind::MemoryConstraintCleared => Some(
            proto::pod_event::Event::MemoryConstraintCleared(
                proto::PodMemoryConstraintCleared {},
            ),
        ),
        PodEventKind::OomKill { count } => Some(proto::pod_event::Event::OomKill(
            proto::PodOomKill { count: *count },
        )),
    };

    match inner {
        Some(pod_event) => vec![proto::NamespaceEvent {
            timestamp_unix_ms: now_ms(),
            event: Some(proto::namespace_event::Event::Pod(proto::PodEvent {
                pod_id: pod_id_str,
                workload_id: workload_name,
                event: Some(pod_event),
            })),
        }],
        None => vec![],
    }
}

fn convert_pod_status_transition(
    old: &sm::PodStatus,
    new: &sm::PodStatus,
) -> Option<proto::pod_event::Event> {
    match (old, new) {
        (_, sm::PodStatus::Running) => {
            Some(proto::pod_event::Event::Running(proto::PodRunning {
                worker_id: String::new(),
            }))
        }
        (_, sm::PodStatus::Suspending) => {
            Some(proto::pod_event::Event::Suspending(proto::PodSuspending {
                worker_id: String::new(),
            }))
        }
        (_, sm::PodStatus::Suspended { .. }) => {
            Some(proto::pod_event::Event::Suspended(proto::PodSuspended {
                worker_id: String::new(),
                snapshot_id: String::new(),
            }))
        }
        (_, sm::PodStatus::Finished { exit_code }) => {
            Some(proto::pod_event::Event::Stopped(proto::PodStopped {
                exit_code: *exit_code,
            }))
        }
        (_, sm::PodStatus::Failed { reason, .. }) => {
            Some(proto::pod_event::Event::Failed(proto::PodFailed {
                reason: reason.clone(),
            }))
        }
        (_, sm::PodStatus::Displaced) => {
            Some(proto::pod_event::Event::Displaced(proto::PodDisplaced {}))
        }
        _ => None,
    }
}

fn convert_workload_obs_event(
    event: &WorkloadObservabilityEvent,
    registry: &IdRegistry,
) -> Vec<proto::NamespaceEvent> {
    let workload_name = registry
        .workload_name(&event.workload_id)
        .unwrap_or_default();

    let inner = match &event.event {
        WorkloadEventKind::StatusChanged { old, new } => {
            convert_workload_status_transition(old, new)
        }
    };

    match inner {
        Some(wl_event) => vec![proto::NamespaceEvent {
            timestamp_unix_ms: now_ms(),
            event: Some(proto::namespace_event::Event::Workload(
                proto::WorkloadEvent {
                    workload_id: workload_name,
                    event: Some(wl_event),
                },
            )),
        }],
        None => vec![],
    }
}

fn convert_workload_status_transition(
    old: &sm::WlStatus,
    new: &sm::WlStatus,
) -> Option<proto::workload_event::Event> {
    Some(proto::workload_event::Event::StateChanged(
        proto::WorkloadStateChanged {
            old_state: Some(convert_wl_status_to_proto(old)),
            new_state: Some(convert_wl_status_to_proto(new)),
        },
    ))
}

fn convert_wl_status_to_proto(status: &sm::WlStatus) -> proto::WorkloadState {
    let state = match status {
        sm::WlStatus::Dormant => {
            proto::workload_state::State::Dormant(proto::WorkloadDormant {})
        }
        sm::WlStatus::WaitingForSpec => {
            proto::workload_state::State::WaitingForSpec(proto::WorkloadWaitingForSpec {})
        }
        sm::WlStatus::Launching => {
            proto::workload_state::State::Launching(proto::WorkloadLaunching {
                pod_id: String::new(),
                worker_id: String::new(),
            })
        }
        sm::WlStatus::Running => {
            proto::workload_state::State::Running(proto::WorkloadRunning {
                pod_id: String::new(),
                worker_id: String::new(),
            })
        }
        sm::WlStatus::Suspending => {
            proto::workload_state::State::Suspending(proto::WorkloadSuspending {
                pod_id: String::new(),
                worker_id: String::new(),
            })
        }
        sm::WlStatus::Suspended => {
            proto::workload_state::State::Suspended(proto::WorkloadSuspended {})
        }
        sm::WlStatus::RetryBackoff => {
            proto::workload_state::State::RetryBackoff(proto::WorkloadRetryBackoff {})
        }
        sm::WlStatus::Failed { exit_code, reason } => {
            proto::workload_state::State::Failed(proto::WorkloadFailed {
                exit_code: *exit_code,
                reason: reason.clone(),
            })
        }
        sm::WlStatus::Completed { exit_code } => {
            proto::workload_state::State::Completed(proto::WorkloadCompleted {
                exit_code: *exit_code,
            })
        }
    };
    proto::WorkloadState { state: Some(state) }
}

fn convert_endpoint_obs_event(
    event: &EndpointObservabilityEvent,
    registry: &IdRegistry,
) -> Vec<proto::NamespaceEvent> {
    use crate::id_registry::EndpointOwner;

    let endpoint_id_str = event.endpoint_id.0.to_string();

    // Resolve owner context.
    let (service_id, workload_id) = match registry.endpoint_owner(&event.endpoint_id) {
        Some(EndpointOwner::Service(svc_id)) => {
            (registry.service_name(&svc_id), None)
        }
        Some(EndpointOwner::Workload(wl_id)) => {
            (None, registry.workload_name(&wl_id))
        }
        None => (None, None),
    };

    let inner = match &event.event {
        EndpointEventKind::StatusChanged { old, new } => {
            convert_endpoint_status_transition(old, new)
        }
        EndpointEventKind::IdleTimerChanged { active } => {
            if *active {
                Some(proto::endpoint_event::Event::IdleTimerStarted(
                    proto::EndpointIdleTimerStarted { timeout_ms: 0 },
                ))
            } else {
                Some(proto::endpoint_event::Event::IdleTimerCancelled(
                    proto::EndpointIdleTimerCancelled {
                        reason: proto::IdleTimerCancelReason::NewTraffic.into(),
                    },
                ))
            }
        }
    };

    match inner {
        Some(ep_event) => vec![proto::NamespaceEvent {
            timestamp_unix_ms: now_ms(),
            event: Some(proto::namespace_event::Event::Endpoint(
                proto::EndpointEvent {
                    endpoint_id: endpoint_id_str,
                    service_id,
                    workload_id,
                    event: Some(ep_event),
                },
            )),
        }],
        None => vec![],
    }
}

fn convert_endpoint_status_transition(
    old: &EndpointStatus,
    new: &EndpointStatus,
) -> Option<proto::endpoint_event::Event> {
    match (old, new) {
        (EndpointStatus::Idle, EndpointStatus::NeedBackend)
        | (EndpointStatus::Idle, EndpointStatus::Active) => {
            Some(proto::endpoint_event::Event::Activated(
                proto::EndpointActivated {
                    trigger: proto::EndpointActivationTrigger::Traffic.into(),
                },
            ))
        }
        (EndpointStatus::NeedBackend, EndpointStatus::Active) => {
            Some(proto::endpoint_event::Event::BackendReady(
                proto::EndpointBackendReady {},
            ))
        }
        (_, EndpointStatus::Idle) if *old != EndpointStatus::Idle => {
            Some(proto::endpoint_event::Event::Deactivated(
                proto::EndpointDeactivated {
                    reason: proto::EndpointDeactivationReason::IdleTimeout.into(),
                },
            ))
        }
        _ => None,
    }
}

