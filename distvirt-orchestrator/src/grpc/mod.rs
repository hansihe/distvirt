mod conversions;

use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use distvirt_client_protocol::proto;
use distvirt_client_protocol::proto::distvirt_client_server::DistvirtClient;

use crate::shell::r#async::ShellHandle;
use crate::types::NamespaceId;

use conversions::convert_proto_spec;

/// Validate the `Authorization: Bearer <token>` header against the configured secret.
pub fn check_client_auth(req: Request<()>, secret: &str) -> Result<Request<()>, Status> {
    let token = req
        .metadata()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    match token {
        Some(t) if crate::util::constant_time_eq(t.as_bytes(), secret.as_bytes()) => Ok(req),
        Some(_) => Err(Status::unauthenticated("invalid token")),
        None => Err(Status::unauthenticated("missing authorization header")),
    }
}

pub struct DistvirtClientService {
    handle: ShellHandle,
}

impl DistvirtClientService {
    pub fn new(handle: ShellHandle) -> Self {
        DistvirtClientService { handle }
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
        let network = distvirt_worker_protocol::NetworkConfig {
            segment_id: None,
            subnet: spec.network.subnet,
            gateway: spec.network.gateway,
            prefix_len: spec.network.prefix_len,
        };
        self.handle
            .create_namespace(NamespaceId(req.namespace_id), network)
            .await;
        Ok(Response::new(proto::CreateNamespaceResponse {}))
    }

    async fn update_namespace(
        &self,
        _request: Request<proto::UpdateNamespaceRequest>,
    ) -> Result<Response<proto::UpdateNamespaceResponse>, Status> {
        Err(Status::unimplemented(
            "UpdateNamespace not yet implemented in new core",
        ))
    }

    async fn delete_namespace(
        &self,
        request: Request<proto::DeleteNamespaceRequest>,
    ) -> Result<Response<proto::DeleteNamespaceResponse>, Status> {
        let req = request.into_inner();
        self.handle
            .destroy_namespace(NamespaceId(req.namespace_id))
            .await;
        Ok(Response::new(proto::DeleteNamespaceResponse {}))
    }

    async fn get_namespace_status(
        &self,
        _request: Request<proto::GetNamespaceStatusRequest>,
    ) -> Result<Response<proto::GetNamespaceStatusResponse>, Status> {
        Err(Status::unimplemented(
            "GetNamespaceStatus not yet implemented in new core",
        ))
    }

    async fn list_namespaces(
        &self,
        _request: Request<proto::ListNamespacesRequest>,
    ) -> Result<Response<proto::ListNamespacesResponse>, Status> {
        Err(Status::unimplemented(
            "ListNamespaces not yet implemented in new core",
        ))
    }

    async fn splice(
        &self,
        _request: Request<proto::SpliceRequest>,
    ) -> Result<Response<proto::SpliceResponse>, Status> {
        Err(Status::unimplemented(
            "Splice not yet implemented in new core",
        ))
    }

    async fn unsplice(
        &self,
        _request: Request<proto::UnspliceRequest>,
    ) -> Result<Response<proto::UnspliceResponse>, Status> {
        Err(Status::unimplemented(
            "Unsplice not yet implemented in new core",
        ))
    }

    async fn deactivate_workload(
        &self,
        _request: Request<proto::DeactivateWorkloadRequest>,
    ) -> Result<Response<proto::DeactivateWorkloadResponse>, Status> {
        Err(Status::unimplemented(
            "DeactivateWorkload not yet implemented in new core",
        ))
    }

    async fn clone_namespace(
        &self,
        _request: Request<proto::CloneNamespaceRequest>,
    ) -> Result<Response<proto::CloneNamespaceResponse>, Status> {
        Err(Status::unimplemented(
            "CloneNamespace not yet implemented in new core",
        ))
    }

    async fn list_workers(
        &self,
        _request: Request<proto::ListWorkersRequest>,
    ) -> Result<Response<proto::ListWorkersResponse>, Status> {
        Err(Status::unimplemented(
            "ListWorkers not yet implemented in new core",
        ))
    }

    async fn get_worker(
        &self,
        _request: Request<proto::GetWorkerRequest>,
    ) -> Result<Response<proto::GetWorkerResponse>, Status> {
        Err(Status::unimplemented(
            "GetWorker not yet implemented in new core",
        ))
    }

    async fn list_pods(
        &self,
        _request: Request<proto::ListPodsRequest>,
    ) -> Result<Response<proto::ListPodsResponse>, Status> {
        Err(Status::unimplemented(
            "ListPods not yet implemented in new core",
        ))
    }

    type WatchNamespaceStatusStream = ReceiverStream<Result<proto::NamespaceStatusEvent, Status>>;

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
        _request: Request<proto::StreamLogsRequest>,
    ) -> Result<Response<Self::StreamLogsStream>, Status> {
        Err(Status::unimplemented(
            "StreamLogs not yet implemented in new core",
        ))
    }

    async fn connect_network(
        &self,
        _request: Request<proto::ConnectNetworkRequest>,
    ) -> Result<Response<proto::ConnectNetworkResponse>, Status> {
        Err(Status::unimplemented(
            "ConnectNetwork not yet implemented in new core",
        ))
    }

    async fn disconnect_network(
        &self,
        _request: Request<proto::DisconnectNetworkRequest>,
    ) -> Result<Response<proto::DisconnectNetworkResponse>, Status> {
        Err(Status::unimplemented(
            "DisconnectNetwork not yet implemented in new core",
        ))
    }

    type StreamEventsStream = ReceiverStream<Result<proto::NamespaceEvent, Status>>;

    async fn stream_events(
        &self,
        _request: Request<proto::StreamEventsRequest>,
    ) -> Result<Response<Self::StreamEventsStream>, Status> {
        Err(Status::unimplemented(
            "StreamEvents not yet implemented in new core",
        ))
    }
}
