use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use distvirt_client_protocol::proto;
use distvirt_client_protocol::proto::distvirt_client_server::DistvirtClient;

use crate::shell::ShellHandle;
use crate::types::*;

pub struct DistvirtClientService {
    handle: ShellHandle,
}

impl DistvirtClientService {
    pub fn new(handle: ShellHandle) -> Self {
        DistvirtClientService { handle }
    }

    /// Helper: connect a temporary client, send a command, get the response.
    async fn unary_command(&self, command: ClientCommand) -> Result<ClientEvent, Status> {
        let client_id = self.handle.connect_client();
        let result = self.handle.send_command(client_id.clone(), command).await;
        self.handle.disconnect_client(client_id);
        result.map_err(|e| Status::internal(e.to_string()))
    }
}

// --- Proto -> Internal conversions ---

fn convert_proto_spec(spec: proto::NamespaceSpec) -> Result<NamespaceSpec, Status> {
    let network = spec
        .network
        .ok_or_else(|| Status::invalid_argument("missing network config"))?;
    let network = parse_network_config(&network.subnet)?;

    let mut workloads = HashMap::new();
    for (id, wl) in spec.workloads {
        workloads.insert(WorkloadId(id), convert_proto_workload_spec(wl)?);
    }

    let mut services = HashMap::new();
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
    let mac = parse_mac(&network.mac)?;

    // Gateway and netmask are populated from the namespace's NetworkConfig
    // during pod launch in NamespaceStateMachine::handle_launch_pod.
    let pod_network = PodNetworkConfig {
        ip,
        mac,
        gateway: Ipv4Addr::new(0, 0, 0, 0),
        netmask: String::new(),
    };

    let containers = wl
        .containers
        .into_iter()
        .map(convert_proto_container_spec)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(WorkloadSpec {
        containers,
        network: pod_network,
    })
}

fn convert_proto_container_spec(c: proto::ContainerSpec) -> Result<ContainerSpec, Status> {
    let config = c.config.unwrap_or_default();
    Ok(ContainerSpec {
        container_id: c.name,
        image_ref: c.image,
        config: ContainerConfig {
            entrypoint: config.entrypoint.join(" "),
            args: config.args,
            env: config
                .env
                .into_iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect(),
            working_dir: None,
            uid: None,
            gid: None,
            hostname: None,
            capture_output: true,
            stdin: false,
        },
    })
}

fn convert_proto_service_spec(svc: proto::ServiceSpec) -> Result<ServiceSpec, Status> {
    let network = svc
        .network
        .ok_or_else(|| Status::invalid_argument("service missing network config"))?;
    let ip: Ipv4Addr = network
        .ip
        .parse()
        .map_err(|_| Status::invalid_argument(format!("invalid service IP: '{}'", network.ip)))?;
    let mac = parse_mac(&network.mac)?;

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
        mac,
        policy,
        activation,
    })
}

fn parse_mac(mac_str: &str) -> Result<[u8; 6], Status> {
    let parts: Vec<&str> = mac_str.split(':').collect();
    if parts.len() != 6 {
        return Err(Status::invalid_argument(format!(
            "invalid MAC address: '{}'",
            mac_str
        )));
    }
    let mut mac = [0u8; 6];
    for (i, part) in parts.iter().enumerate() {
        mac[i] = u8::from_str_radix(part, 16).map_err(|_| {
            Status::invalid_argument(format!("invalid MAC address byte: '{}'", part))
        })?;
    }
    Ok(mac)
}

// --- Internal -> Proto conversions ---

fn convert_namespace_state(status: &NamespaceStatus) -> i32 {
    match status {
        NamespaceStatus::Creating => proto::NamespaceState::Creating as i32,
        NamespaceStatus::Active => proto::NamespaceState::Active as i32,
        NamespaceStatus::Cloning { .. } => proto::NamespaceState::Active as i32,
        NamespaceStatus::Destroying => proto::NamespaceState::Destroying as i32,
    }
}

fn convert_status_report(report: NamespaceStatusReport) -> proto::NamespaceStatusReport {
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
                spliced: svc.spliced,
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
                spliced: svc.spliced,
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

fn event_to_error_status(message: String) -> Status {
    if message.contains("not found") {
        Status::not_found(message)
    } else if message.contains("already exists") {
        Status::already_exists(message)
    } else if message.contains("not spliced") || message.contains("already spliced") {
        Status::failed_precondition(message)
    } else {
        Status::internal(message)
    }
}

// --- gRPC Service Implementation ---

#[tonic::async_trait]
impl DistvirtClient for DistvirtClientService {
    async fn create_namespace(
        &self,
        request: Request<proto::CreateNamespaceRequest>,
    ) -> Result<Response<proto::CreateNamespaceResponse>, Status> {
        let req = request.into_inner();
        let spec = convert_proto_spec(
            req.spec
                .ok_or_else(|| Status::invalid_argument("missing spec"))?,
        )?;
        let event = self
            .unary_command(ClientCommand::CreateNamespace {
                namespace_id: NamespaceId(req.namespace_id),
                spec,
            })
            .await?;
        match event {
            ClientEvent::Ok => Ok(Response::new(proto::CreateNamespaceResponse {})),
            ClientEvent::Error { message } => Err(event_to_error_status(message)),
            _ => Err(Status::internal("unexpected response from orchestrator")),
        }
    }

    async fn update_namespace(
        &self,
        request: Request<proto::UpdateNamespaceRequest>,
    ) -> Result<Response<proto::UpdateNamespaceResponse>, Status> {
        let req = request.into_inner();
        let spec = convert_proto_spec(
            req.spec
                .ok_or_else(|| Status::invalid_argument("missing spec"))?,
        )?;
        let event = self
            .unary_command(ClientCommand::UpdateNamespace {
                namespace_id: NamespaceId(req.namespace_id),
                spec,
            })
            .await?;
        match event {
            ClientEvent::Ok => Ok(Response::new(proto::UpdateNamespaceResponse {})),
            ClientEvent::Error { message } => Err(event_to_error_status(message)),
            _ => Err(Status::internal("unexpected response from orchestrator")),
        }
    }

    async fn delete_namespace(
        &self,
        request: Request<proto::DeleteNamespaceRequest>,
    ) -> Result<Response<proto::DeleteNamespaceResponse>, Status> {
        let req = request.into_inner();
        let event = self
            .unary_command(ClientCommand::DeleteNamespace {
                namespace_id: NamespaceId(req.namespace_id),
            })
            .await?;
        match event {
            ClientEvent::Ok => Ok(Response::new(proto::DeleteNamespaceResponse {})),
            ClientEvent::Error { message } => Err(event_to_error_status(message)),
            _ => Err(Status::internal("unexpected response from orchestrator")),
        }
    }

    async fn get_namespace_status(
        &self,
        request: Request<proto::GetNamespaceStatusRequest>,
    ) -> Result<Response<proto::GetNamespaceStatusResponse>, Status> {
        let req = request.into_inner();
        let event = self
            .unary_command(ClientCommand::GetNamespaceStatus {
                namespace_id: NamespaceId(req.namespace_id),
            })
            .await?;
        match event {
            ClientEvent::NamespaceStatus { status, .. } => {
                Ok(Response::new(proto::GetNamespaceStatusResponse {
                    status: Some(convert_status_report(status)),
                }))
            }
            ClientEvent::Error { message } => Err(event_to_error_status(message)),
            _ => Err(Status::internal("unexpected response from orchestrator")),
        }
    }

    async fn list_namespaces(
        &self,
        _request: Request<proto::ListNamespacesRequest>,
    ) -> Result<Response<proto::ListNamespacesResponse>, Status> {
        let event = self.unary_command(ClientCommand::ListNamespaces).await?;
        match event {
            ClientEvent::NamespaceList { namespaces } => {
                let proto_namespaces = namespaces
                    .into_iter()
                    .map(convert_status_report)
                    .collect();
                Ok(Response::new(proto::ListNamespacesResponse {
                    namespaces: proto_namespaces,
                }))
            }
            ClientEvent::Error { message } => Err(event_to_error_status(message)),
            _ => Err(Status::internal("unexpected response from orchestrator")),
        }
    }

    async fn splice(
        &self,
        request: Request<proto::SpliceRequest>,
    ) -> Result<Response<proto::SpliceResponse>, Status> {
        let req = request.into_inner();
        let event = self
            .unary_command(ClientCommand::Splice {
                namespace_id: NamespaceId(req.namespace_id),
                workload_id: WorkloadId(req.workload_id),
                worker_id: WorkerId(req.local_worker_id),
            })
            .await?;
        match event {
            ClientEvent::Ok => Ok(Response::new(proto::SpliceResponse {})),
            ClientEvent::Error { message } => Err(event_to_error_status(message)),
            _ => Err(Status::internal("unexpected response from orchestrator")),
        }
    }

    async fn unsplice(
        &self,
        request: Request<proto::UnspliceRequest>,
    ) -> Result<Response<proto::UnspliceResponse>, Status> {
        let req = request.into_inner();
        let event = self
            .unary_command(ClientCommand::Unsplice {
                namespace_id: NamespaceId(req.namespace_id),
                workload_id: WorkloadId(req.workload_id),
            })
            .await?;
        match event {
            ClientEvent::Ok => Ok(Response::new(proto::UnspliceResponse {})),
            ClientEvent::Error { message } => Err(event_to_error_status(message)),
            _ => Err(Status::internal("unexpected response from orchestrator")),
        }
    }

    async fn clone_namespace(
        &self,
        request: Request<proto::CloneNamespaceRequest>,
    ) -> Result<Response<proto::CloneNamespaceResponse>, Status> {
        let req = request.into_inner();
        let event = self
            .unary_command(ClientCommand::CloneNamespace {
                source_namespace_id: NamespaceId(req.source_namespace_id),
                target_namespace_id: NamespaceId(req.target_namespace_id),
            })
            .await?;
        match event {
            ClientEvent::Ok => Ok(Response::new(proto::CloneNamespaceResponse {})),
            ClientEvent::Error { message } => Err(event_to_error_status(message)),
            _ => Err(Status::internal("unexpected response from orchestrator")),
        }
    }

    async fn list_workers(
        &self,
        _request: Request<proto::ListWorkersRequest>,
    ) -> Result<Response<proto::ListWorkersResponse>, Status> {
        let event = self.unary_command(ClientCommand::ListWorkers).await?;
        match event {
            ClientEvent::WorkerList { workers } => {
                let proto_workers = workers
                    .into_iter()
                    .map(|w| proto::WorkerInfo {
                        worker_id: w.worker_id.0,
                        max_pods: w.max_pods,
                        available_memory_mb: w.available_memory_mb,
                        active_pods: w.active_pods,
                    })
                    .collect();
                Ok(Response::new(proto::ListWorkersResponse {
                    workers: proto_workers,
                }))
            }
            ClientEvent::Error { message } => Err(event_to_error_status(message)),
            _ => Err(Status::internal("unexpected response from orchestrator")),
        }
    }

    async fn get_worker(
        &self,
        request: Request<proto::GetWorkerRequest>,
    ) -> Result<Response<proto::GetWorkerResponse>, Status> {
        let req = request.into_inner();
        let event = self
            .unary_command(ClientCommand::GetWorker {
                worker_id: WorkerId(req.worker_id),
            })
            .await?;
        match event {
            ClientEvent::WorkerStatus { worker } => {
                Ok(Response::new(proto::GetWorkerResponse {
                    worker: Some(proto::WorkerInfo {
                        worker_id: worker.worker_id.0,
                        max_pods: worker.max_pods,
                        available_memory_mb: worker.available_memory_mb,
                        active_pods: worker.active_pods,
                    }),
                }))
            }
            ClientEvent::Error { message } => Err(event_to_error_status(message)),
            _ => Err(Status::internal("unexpected response from orchestrator")),
        }
    }

    async fn list_pods(
        &self,
        request: Request<proto::ListPodsRequest>,
    ) -> Result<Response<proto::ListPodsResponse>, Status> {
        let req = request.into_inner();
        let event = self
            .unary_command(ClientCommand::ListPods {
                namespace_id: NamespaceId(req.namespace_id),
            })
            .await?;
        match event {
            ClientEvent::PodList { pods } => {
                let proto_pods = pods
                    .into_iter()
                    .map(|p| proto::PodInfo {
                        pod_id: p.pod_id.0,
                        workload_id: p.workload_id.0,
                        worker_id: p.worker_id.0,
                        ip: p.ip,
                        mac: p.mac,
                        state: match p.state {
                            PodStatus::Launching => proto::PodState::Launching as i32,
                            PodStatus::Running => proto::PodState::Running as i32,
                        },
                    })
                    .collect();
                Ok(Response::new(proto::ListPodsResponse {
                    pods: proto_pods,
                }))
            }
            ClientEvent::Error { message } => Err(event_to_error_status(message)),
            _ => Err(Status::internal("unexpected response from orchestrator")),
        }
    }

    type WatchNamespaceStatusStream =
        ReceiverStream<Result<proto::NamespaceStatusEvent, Status>>;

    async fn watch_namespace_status(
        &self,
        _request: Request<proto::WatchNamespaceStatusRequest>,
    ) -> Result<Response<Self::WatchNamespaceStatusStream>, Status> {
        Err(Status::unimplemented(
            "WatchNamespaceStatus not yet implemented",
        ))
    }

    type StreamLogsStream = ReceiverStream<Result<proto::LogChunk, Status>>;

    async fn stream_logs(
        &self,
        request: Request<proto::StreamLogsRequest>,
    ) -> Result<Response<Self::StreamLogsStream>, Status> {
        let req = request.into_inner();
        let namespace_id = NamespaceId(req.namespace_id);
        let workload_id = req.workload_id.map(WorkloadId);

        let mut log_rx = self.handle.subscribe_logs(namespace_id, workload_id);

        let (tx, rx) = mpsc::channel(256);
        tokio::spawn(async move {
            while let Some(chunk) = log_rx.recv().await {
                let proto_chunk = proto::LogChunk {
                    workload_id: chunk.workload_id.0,
                    data: chunk.data,
                    timestamp_unix_ms: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as i64,
                };
                if tx.send(Ok(proto_chunk)).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn connect_network(
        &self,
        request: Request<proto::ConnectNetworkRequest>,
    ) -> Result<Response<proto::ConnectNetworkResponse>, Status> {
        let req = request.into_inner();
        if req.client_public_key.len() != 32 {
            return Err(Status::invalid_argument("client_public_key must be 32 bytes"));
        }
        let mut pubkey = [0u8; 32];
        pubkey.copy_from_slice(&req.client_public_key);
        let event = self
            .unary_command(ClientCommand::Connect {
                namespace_id: NamespaceId(req.namespace_id),
                client_public_key: pubkey,
            })
            .await?;
        match event {
            ClientEvent::ConnectResult {
                server_public_key,
                endpoint,
                client_ip,
                subnet,
            } => Ok(Response::new(proto::ConnectNetworkResponse {
                server_public_key: server_public_key.to_vec(),
                endpoint,
                client_ip,
                subnet,
            })),
            ClientEvent::Error { message } => Err(event_to_error_status(message)),
            _ => Err(Status::internal("unexpected response from orchestrator")),
        }
    }

    async fn disconnect_network(
        &self,
        request: Request<proto::DisconnectNetworkRequest>,
    ) -> Result<Response<proto::DisconnectNetworkResponse>, Status> {
        let req = request.into_inner();
        if req.client_public_key.len() != 32 {
            return Err(Status::invalid_argument("client_public_key must be 32 bytes"));
        }
        let mut pubkey = [0u8; 32];
        pubkey.copy_from_slice(&req.client_public_key);
        let event = self
            .unary_command(ClientCommand::Disconnect {
                namespace_id: NamespaceId(req.namespace_id),
                client_public_key: pubkey,
            })
            .await?;
        match event {
            ClientEvent::Ok => Ok(Response::new(proto::DisconnectNetworkResponse {})),
            ClientEvent::Error { message } => Err(event_to_error_status(message)),
            _ => Err(Status::internal("unexpected response from orchestrator")),
        }
    }

    type StreamEventsStream = ReceiverStream<Result<proto::NamespaceEvent, Status>>;

    async fn stream_events(
        &self,
        _request: Request<proto::StreamEventsRequest>,
    ) -> Result<Response<Self::StreamEventsStream>, Status> {
        Err(Status::unimplemented("StreamEvents not yet implemented"))
    }
}
