mod conversions;

use std::collections::HashSet;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use distvirt_client_protocol::proto;
use distvirt_client_protocol::proto::distvirt_client_server::DistvirtClient;

use crate::shell::ShellHandle;
use crate::types::*;

use conversions::{convert_proto_spec, convert_sm_event_to_proto, convert_status_report};

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

    async fn deactivate_workload(
        &self,
        request: Request<proto::DeactivateWorkloadRequest>,
    ) -> Result<Response<proto::DeactivateWorkloadResponse>, Status> {
        let req = request.into_inner();
        let event = self
            .unary_command(ClientCommand::DeactivateWorkload {
                namespace_id: NamespaceId(req.namespace_id),
                workload_id: WorkloadId(req.workload_id),
            })
            .await?;
        match event {
            ClientEvent::DeactivateWorkloadResult { deactivated, reason } => {
                Ok(Response::new(proto::DeactivateWorkloadResponse {
                    deactivated,
                    reason,
                }))
            }
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
                        mac: String::new(),
                        state: match p.state {
                            PodStatus::Launching => proto::PodState::Launching as i32,
                            PodStatus::Running => proto::PodState::Running as i32,
                            PodStatus::Suspending => proto::PodState::Suspending as i32,
                            PodStatus::Suspended => proto::PodState::Suspended as i32,
                            PodStatus::Resuming => proto::PodState::Resuming as i32,
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
        request: Request<proto::StreamEventsRequest>,
    ) -> Result<Response<Self::StreamEventsStream>, Status> {
        let req = request.into_inner();
        let namespace_id = NamespaceId(req.namespace_id);
        let workload_ids: HashSet<WorkloadId> = req.workload_ids.into_iter().map(WorkloadId).collect();
        let service_ids: HashSet<ServiceId> = req.service_ids.into_iter().map(ServiceId).collect();

        let mut event_rx =
            self.handle
                .subscribe_events(namespace_id, workload_ids, service_ids);

        let (tx, rx) = mpsc::channel(256);
        tokio::spawn(async move {
            while let Some(event_data) = event_rx.recv().await {
                let proto_event = convert_sm_event_to_proto(event_data.event);
                if tx.send(Ok(proto_event)).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
