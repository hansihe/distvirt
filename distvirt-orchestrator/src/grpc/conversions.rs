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
        workloads.insert(WorkloadId(id), convert_proto_workload_spec(wl)?);
    }

    let mut services = BTreeMap::new();
    for (id, svc) in spec.services {
        services.insert(ServiceId(id), convert_proto_service_spec(svc)?);
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
    let network = wl
        .network
        .ok_or_else(|| Status::invalid_argument("workload missing network config"))?;
    let ip: Ipv4Addr = network
        .ip
        .parse()
        .map_err(|_| Status::invalid_argument(format!("invalid workload IP: '{}'", network.ip)))?;
    // Gateway and netmask are populated from the namespace's NetworkConfig
    // during pod launch in NamespaceStateMachine::handle_launch_pod.
    // Generate a locally-administered unicast MAC from the IP so the guest
    // network stack gets a valid hardware address.
    let ip_octets = ip.octets();
    let pod_network = PodNetworkConfig {
        ip,
        mac: [0x02, 0x00, ip_octets[0], ip_octets[1], ip_octets[2], ip_octets[3]],
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
            memory_mb: v.memory_mb,
            vcpus: v.vcpus,
        }),
        limits: r.limits.map(|v| ResourceValues {
            memory_mb: v.memory_mb,
            vcpus: v.vcpus,
        }),
    });

    Ok(WorkloadSpec {
        containers,
        network: pod_network,
        suspend_on_idle: wl.suspend_on_idle,
        resources,
    })
}

fn convert_proto_container_spec(c: proto::ContainerSpec) -> Result<ContainerSpec, Status> {
    let config = c.config.unwrap_or_default();

    let (uid, gid) = if config.user.is_empty() {
        (None, None)
    } else {
        parse_user_field(&config.user)?
    };

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
                proto::activator_config::Activator::Http2(_) => {
                    ActivatorConfig::Http2 {}
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

    let activation = svc.activation.map(|a| ActivationSpec {
        idle_timeout: Duration::from_millis(a.idle_timeout_ms),
    });

    Ok(ServiceSpec {
        workload_id: WorkloadId(svc.workload_id),
        ip,
        policy,
        activation,
    })
}

// --- Internal -> Proto conversions ---

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

    for (svc_id, svc) in report.services {
        let wl_key = svc.workload_id.0.clone();

        // Build workload entry from service data (first service for each workload wins).
        workloads.entry(wl_key).or_insert_with(|| {
            proto::WorkloadStatusReport {
                state: Some(convert_workload_state_from_strings(
                    &svc.workload_state,
                    &svc.pod_id,
                    &svc.worker_id,
                )),
                spliced: false,
            }
        });

        services.insert(
            svc_id.0,
            proto::ServiceStatusReport {
                workload_id: svc.workload_id.0,
                state: Some(convert_service_state_from_strings(
                    &svc.service_state,
                    &svc.pod_id,
                    &svc.worker_id,
                    &svc.backend_need,
                )),
                activation_enabled: svc.activation_enabled,
                spliced: false,
                ip: svc.ip,
                mac: String::new(),
            },
        );
    }

    proto::NamespaceStatusReport {
        namespace_id: report.namespace_id.0,
        state: convert_namespace_state(&report.status),
        workloads,
        services,
    }
}

fn convert_workload_state_from_strings(
    state: &str,
    pod_id: &Option<PodId>,
    worker_id: &Option<WorkerId>,
) -> proto::WorkloadState {
    let state = match state {
        "launching" => proto::workload_state::State::Launching(proto::WorkloadLaunching {
            pod_id: pod_id.as_ref().map(|p| p.0.clone()).unwrap_or_default(),
            worker_id: worker_id.as_ref().map(|w| w.0.clone()).unwrap_or_default(),
        }),
        "running" => proto::workload_state::State::Running(proto::WorkloadRunning {
            pod_id: pod_id.as_ref().map(|p| p.0.clone()).unwrap_or_default(),
            worker_id: worker_id.as_ref().map(|w| w.0.clone()).unwrap_or_default(),
        }),
        "waiting_for_capacity" => {
            proto::workload_state::State::WaitingForCapacity(proto::WorkloadWaitingForCapacity {})
        }
        _ => proto::workload_state::State::Dormant(proto::WorkloadDormant {}),
    };
    proto::WorkloadState { state: Some(state) }
}

fn convert_service_state_from_strings(
    state: &str,
    pod_id: &Option<PodId>,
    worker_id: &Option<WorkerId>,
    backend_need: &Option<BackendNeed>,
) -> proto::ServiceState {
    let state = match state {
        "idle" => proto::service_state::State::Idle(proto::ServiceIdle {}),
        "need_backend" => {
            proto::service_state::State::NeedBackend(proto::ServiceNeedBackend {})
        }
        "active" => proto::service_state::State::Active(proto::ServiceActive {
            pod_id: pod_id.as_ref().map(|p| p.0.clone()).unwrap_or_default(),
            worker_id: worker_id.as_ref().map(|w| w.0.clone()).unwrap_or_default(),
            backend_need: convert_backend_need(backend_need),
        }),
        _ => proto::service_state::State::Pending(proto::ServicePending {}),
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

// --- SM Event -> Proto Event conversion ---

pub(super) fn convert_sm_event_to_proto(event: SmNamespaceEvent) -> proto::NamespaceEvent {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    match event {
        SmNamespaceEvent::Workload {
            workload_id,
            event: wl_event,
        } => {
            let inner = match wl_event {
                SmWorkloadEvent::DemandChanged {
                    demanding_services,
                } => proto::workload_event::Event::DemandChanged(
                    proto::WorkloadDemandChanged {
                        demanding_services,
                    },
                ),
                SmWorkloadEvent::PodLaunching { pod_id, worker_id } => {
                    proto::workload_event::Event::PodLaunching(proto::WorkloadPodLaunching {
                        pod_id: pod_id.0,
                        worker_id: worker_id.0,
                    })
                }
                SmWorkloadEvent::PodRunning { pod_id, worker_id } => {
                    proto::workload_event::Event::PodRunning(proto::WorkloadPodRunning {
                        pod_id: pod_id.0,
                        worker_id: worker_id.0,
                    })
                }
                SmWorkloadEvent::PodStopped { exit_code } => {
                    proto::workload_event::Event::PodStopped(proto::WorkloadPodStopped {
                        exit_code,
                    })
                }
                SmWorkloadEvent::PodFailed { reason } => {
                    proto::workload_event::Event::PodFailed(proto::WorkloadPodFailed { reason })
                }
                SmWorkloadEvent::PodSuspending { pod_id, worker_id } => {
                    proto::workload_event::Event::PodSuspending(proto::WorkloadPodSuspending {
                        pod_id: pod_id.0,
                        worker_id: worker_id.0,
                    })
                }
                SmWorkloadEvent::PodSuspended { worker_id, artifact_id } => {
                    proto::workload_event::Event::PodSuspended(proto::WorkloadPodSuspended {
                        worker_id: worker_id.0,
                        snapshot_id: artifact_id.0,
                    })
                }
                SmWorkloadEvent::PodSuspendFailed { reason } => {
                    proto::workload_event::Event::PodSuspendFailed(proto::WorkloadPodSuspendFailed { reason })
                }
                SmWorkloadEvent::PodResuming { pod_id, worker_id } => {
                    proto::workload_event::Event::PodResuming(proto::WorkloadPodResuming {
                        pod_id: pod_id.0,
                        worker_id: worker_id.0,
                    })
                }
            };
            proto::NamespaceEvent {
                timestamp_unix_ms: now_ms,
                event: Some(proto::namespace_event::Event::WorkloadEvent(
                    proto::WorkloadEvent {
                        workload_id: workload_id.0,
                        event: Some(inner),
                    },
                )),
            }
        }
        SmNamespaceEvent::Service {
            service_id,
            workload_id,
            event: svc_event,
        } => {
            let inner = match svc_event {
                SmServiceEvent::Activated { trigger } => {
                    let proto_trigger = match trigger {
                        ServiceActivationTrigger::Traffic => proto::ServiceActivationTrigger::Traffic,
                    };
                    proto::service_event::Event::Activated(proto::ServiceActivated {
                        trigger: proto_trigger.into(),
                    })
                }
                SmServiceEvent::BackendReady => {
                    proto::service_event::Event::BackendReady(proto::ServiceBackendReady {})
                }
                SmServiceEvent::IdleTimerStarted { timeout } => {
                    proto::service_event::Event::IdleTimerStarted(
                        proto::ServiceIdleTimerStarted {
                            timeout_ms: timeout.as_millis() as u64,
                        },
                    )
                }
                SmServiceEvent::IdleTimerCancelled { reason } => {
                    let proto_reason = match reason {
                        IdleTimerCancelReason::NewTraffic => proto::IdleTimerCancelReason::NewTraffic,
                    };
                    proto::service_event::Event::IdleTimerCancelled(
                        proto::ServiceIdleTimerCancelled { reason: proto_reason.into() },
                    )
                }
                SmServiceEvent::IdleTimeoutFired => {
                    proto::service_event::Event::IdleTimeoutFired(
                        proto::ServiceIdleTimeoutFired {},
                    )
                }
                SmServiceEvent::Deactivated { reason } => {
                    let proto_reason = match reason {
                        ServiceDeactivationReason::IdleTimeout => proto::ServiceDeactivationReason::IdleTimeout,
                        ServiceDeactivationReason::ForceDeactivate => proto::ServiceDeactivationReason::ForceDeactivate,
                    };
                    proto::service_event::Event::Deactivated(proto::ServiceDeactivated {
                        reason: proto_reason.into(),
                    })
                }
            };
            proto::NamespaceEvent {
                timestamp_unix_ms: now_ms,
                event: Some(proto::namespace_event::Event::ServiceEvent(
                    proto::ServiceEvent {
                        service_id: service_id.0,
                        workload_id: workload_id.0,
                        event: Some(inner),
                    },
                )),
            }
        }
    }
}
