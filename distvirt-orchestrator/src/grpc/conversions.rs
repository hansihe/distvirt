use std::collections::{BTreeMap, HashMap};
use std::net::Ipv4Addr;
use std::time::Duration;

use tonic::Status;

use distvirt_client_protocol::proto;

use crate::types::*;

// --- Proto -> Internal conversions ---

pub(super) fn convert_proto_spec(spec: proto::NamespaceSpec) -> Result<NamespaceSpec, Status> {
    let network = spec
        .network
        .ok_or_else(|| Status::invalid_argument("missing network config"))?;
    let network = parse_network_config(&network.subnet)?;

    let mut workloads = BTreeMap::new();
    for (id, wl) in spec.workloads {
        workloads.insert(WorkloadName(id), convert_proto_workload_spec(wl)?);
    }

    let mut services = BTreeMap::new();
    for (id, svc) in spec.services {
        services.insert(id, convert_proto_service_spec(svc)?);
    }

    Ok(NamespaceSpec {
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

fn convert_proto_workload_spec(wl: proto::WorkloadSpec) -> Result<WorkloadSpec, Status> {
    let run_policy = convert_proto_run_policy(wl.run_policy());

    let network = wl
        .network
        .ok_or_else(|| Status::invalid_argument("workload missing network config"))?;
    let ip: Ipv4Addr = network
        .ip
        .parse()
        .map_err(|_| Status::invalid_argument(format!("invalid workload IP: '{}'", network.ip)))?;
    // Gateway and netmask are populated from the namespace's NetworkConfig
    // during pod launch/resume in Namespace::fill_network_from_namespace.
    // Generate a locally-administered unicast MAC from the IP so the guest
    // network stack gets a valid hardware address.
    let ip_octets = ip.octets();
    let pod_network = PodNetworkConfig {
        ip,
        mac: [
            0x02,
            0x00,
            ip_octets[0],
            ip_octets[1],
            ip_octets[2],
            ip_octets[3],
        ],
        gateway: Ipv4Addr::new(0, 0, 0, 0),
        netmask: String::new(),
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

    // Workload-level activation: extract passthrough idle_timeout
    let activation = wl.activation.and_then(|act| {
        act.activator.and_then(|cfg| {
            cfg.activator.and_then(|a| match a {
                proto::activator_config::Activator::Passthrough(p) => {
                    Some(ActivationSpec {
                        idle_timeout: Duration::from_millis(p.idle_timeout_ms),
                    })
                }
                _ => {
                    log::warn!("only passthrough activator is valid on workloads; ignored");
                    None
                }
            })
        })
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

    Ok(WorkloadSpec {
        containers,
        network: pod_network,
        suspend_on_idle: wl.suspend_on_idle,
        resources,
        activation,
        run_policy,
        respects_demand: wl.respects_demand,
        volumes,
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

    let (uid, gid) = if config.user.is_empty() {
        (None, None)
    } else {
        parse_user_field(&config.user)?
    };

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
            entrypoint: config.entrypoint,
            args: config.args,
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
            uid,
            gid,
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

fn parse_user_field(user: &str) -> Result<(Option<u32>, Option<u32>), Status> {
    if let Some((uid_str, gid_str)) = user.split_once(':') {
        let uid: u32 = uid_str
            .parse()
            .map_err(|_| Status::invalid_argument(format!("non-numeric uid: '{}'", uid_str)))?;
        let gid: u32 = gid_str
            .parse()
            .map_err(|_| Status::invalid_argument(format!("non-numeric gid: '{}'", gid_str)))?;
        Ok((Some(uid), Some(gid)))
    } else {
        let uid: u32 = user
            .parse()
            .map_err(|_| Status::invalid_argument(format!("non-numeric user: '{}'", user)))?;
        Ok((Some(uid), None))
    }
}

fn convert_proto_service_spec(svc: proto::ServiceSpec) -> Result<ServiceSpec, Status> {
    let network = svc
        .network
        .ok_or_else(|| Status::invalid_argument("service missing network config"))?;
    let ip: Ipv4Addr = network
        .ip
        .parse()
        .map_err(|_| Status::invalid_argument(format!("invalid service IP: '{}'", network.ip)))?;
    // Build the ServicePolicy from activation config.
    let policy = if let Some(ref act) = svc.activation {
        let activator = act.activator.as_ref().and_then(|a| {
            a.activator.as_ref().map(|inner| match inner {
                proto::activator_config::Activator::Tcp(tcp) => {
                    let ports = if tcp.ports.is_empty() {
                        None
                    } else {
                        Some(tcp.ports.iter().map(|p| *p as u16).collect())
                    };
                    ActivatorConfig::Tcp {
                        ports,
                        tcp_only: false,
                        max_flows: 1024,
                    }
                }
                proto::activator_config::Activator::Http2(_) => ActivatorConfig::Http2 {},
                proto::activator_config::Activator::Passthrough(_) => {
                    // Passthrough on a service: no protocol-specific activator config.
                    // The service will activate on any traffic.
                    ActivatorConfig::Tcp {
                        ports: None,
                        tcp_only: false,
                        max_flows: 1024,
                    }
                }
            })
        });
        ServicePolicy {
            buffer_frames: 64,
            timeout_ms: 5000,
            activator,
        }
    } else {
        ServicePolicy {
            buffer_frames: 0,
            timeout_ms: 0,
            activator: None,
        }
    };

    let activation = svc.activation.and_then(|a| {
        // Extract idle_timeout from inside the activator variant
        let idle_timeout = a.activator.as_ref().and_then(|cfg| {
            cfg.activator.as_ref().and_then(|act| match act {
                proto::activator_config::Activator::Passthrough(p) => {
                    Some(Duration::from_millis(p.idle_timeout_ms))
                }
                proto::activator_config::Activator::Tcp(tcp) if tcp.idle_timeout_ms > 0 => {
                    Some(Duration::from_millis(tcp.idle_timeout_ms))
                }
                _ => None,
            })
        });
        // Default to 30s if no idle_timeout was specified but activation is present
        Some(ActivationSpec {
            idle_timeout: idle_timeout.unwrap_or(Duration::from_secs(30)),
        })
    });

    Ok(ServiceSpec {
        workload_id: WorkloadName(svc.workload_id),
        ip,
        policy,
        activation,
    })
}

// --- Proto -> Internal conversions (patch) ---

pub(super) fn convert_proto_patch(
    req: proto::PatchNamespaceRequest,
) -> Result<crate::types::NamespacePatch, Status> {
    let mut workloads = BTreeMap::new();
    for (name, wl) in req.workloads {
        workloads.insert(WorkloadName(name), convert_proto_workload_spec(wl)?);
    }

    let mut services = BTreeMap::new();
    for (name, svc) in req.services {
        services.insert(name, convert_proto_service_spec(svc)?);
    }

    Ok(crate::types::NamespacePatch {
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
        namespace_id: report.namespace_id.0,
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
        workload_id: chunk.pod_id.0.to_string(),
        container_id: chunk.container_id,
        data: chunk.data,
        timestamp_unix_ms,
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

