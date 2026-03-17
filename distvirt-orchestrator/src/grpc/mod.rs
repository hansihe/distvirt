mod conversions;

use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use distvirt_client_protocol::proto;
use distvirt_client_protocol::proto::distvirt_client_server::DistvirtClient;

use crate::core::ClientError;
use crate::shell::r#async::ShellHandle;
use crate::types::NamespaceId;

use conversions::{convert_pod_status_report, convert_proto_spec, convert_status_report, convert_worker_query_info};

fn client_error_to_status(err: ClientError) -> Status {
    match err {
        ClientError::WorkerNotFound => Status::not_found(err.to_string()),
        ClientError::NamespaceNotFound => Status::not_found(err.to_string()),
        ClientError::NamespaceAlreadyExists => Status::already_exists(err.to_string()),
        ClientError::NoTunnelWorker => Status::failed_precondition(err.to_string()),
        ClientError::IpExhausted => Status::resource_exhausted(err.to_string()),
        ClientError::ShellGone => Status::unavailable(err.to_string()),
    }
}

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
            .await
            .map_err(client_error_to_status)?;
        Ok(Response::new(proto::CreateNamespaceResponse {}))
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
        self.handle
            .update_namespace(NamespaceId(req.namespace_id), spec)
            .await
            .map_err(client_error_to_status)?;
        Ok(Response::new(proto::UpdateNamespaceResponse {}))
    }

    async fn delete_namespace(
        &self,
        request: Request<proto::DeleteNamespaceRequest>,
    ) -> Result<Response<proto::DeleteNamespaceResponse>, Status> {
        let req = request.into_inner();
        self.handle
            .destroy_namespace(NamespaceId(req.namespace_id))
            .await
            .map_err(client_error_to_status)?;
        Ok(Response::new(proto::DeleteNamespaceResponse {}))
    }

    async fn get_namespace_status(
        &self,
        request: Request<proto::GetNamespaceStatusRequest>,
    ) -> Result<Response<proto::GetNamespaceStatusResponse>, Status> {
        let req = request.into_inner();
        let report = self
            .handle
            .get_namespace_status(NamespaceId(req.namespace_id))
            .await
            .map_err(client_error_to_status)?;
        Ok(Response::new(proto::GetNamespaceStatusResponse {
            status: Some(convert_status_report(report)),
        }))
    }

    async fn list_namespaces(
        &self,
        _request: Request<proto::ListNamespacesRequest>,
    ) -> Result<Response<proto::ListNamespacesResponse>, Status> {
        let reports = self
            .handle
            .list_namespaces()
            .await
            .map_err(client_error_to_status)?;
        Ok(Response::new(proto::ListNamespacesResponse {
            namespaces: reports.into_iter().map(convert_status_report).collect(),
        }))
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
        let workers = self
            .handle
            .list_workers()
            .await
            .map_err(client_error_to_status)?;
        Ok(Response::new(proto::ListWorkersResponse {
            workers: workers.into_iter().map(convert_worker_query_info).collect(),
        }))
    }

    async fn get_worker(
        &self,
        request: Request<proto::GetWorkerRequest>,
    ) -> Result<Response<proto::GetWorkerResponse>, Status> {
        let req = request.into_inner();
        let worker_id_num: u64 = req
            .worker_id
            .parse()
            .map_err(|_| Status::invalid_argument("invalid worker_id"))?;
        let worker_id = crate::core::GlobalWorkerId::from(worker_id_num);
        let worker = self
            .handle
            .get_worker(worker_id)
            .await
            .map_err(client_error_to_status)?;
        Ok(Response::new(proto::GetWorkerResponse {
            worker: Some(convert_worker_query_info(worker)),
        }))
    }

    async fn list_pods(
        &self,
        request: Request<proto::ListPodsRequest>,
    ) -> Result<Response<proto::ListPodsResponse>, Status> {
        let req = request.into_inner();
        let pods = self
            .handle
            .list_pods(NamespaceId(req.namespace_id))
            .await
            .map_err(client_error_to_status)?;
        Ok(Response::new(proto::ListPodsResponse {
            pods: pods.into_iter().map(convert_pod_status_report).collect(),
        }))
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
        request: Request<proto::ConnectNetworkRequest>,
    ) -> Result<Response<proto::ConnectNetworkResponse>, Status> {
        let req = request.into_inner();

        let client_public_key: [u8; 32] = req
            .client_public_key
            .as_slice()
            .try_into()
            .map_err(|_| Status::invalid_argument("client_public_key must be 32 bytes"))?;

        let result = self
            .handle
            .connect_network(NamespaceId(req.namespace_id), client_public_key)
            .await
            .map_err(client_error_to_status)?;

        Ok(Response::new(proto::ConnectNetworkResponse {
            server_public_key: result.server_public_key.to_vec(),
            endpoint: result.endpoint,
            client_ip: result.client_ip.to_string(),
            subnet: result.subnet,
        }))
    }

    async fn disconnect_network(
        &self,
        request: Request<proto::DisconnectNetworkRequest>,
    ) -> Result<Response<proto::DisconnectNetworkResponse>, Status> {
        let req = request.into_inner();

        let client_public_key: [u8; 32] = req
            .client_public_key
            .as_slice()
            .try_into()
            .map_err(|_| Status::invalid_argument("client_public_key must be 32 bytes"))?;

        self.handle
            .disconnect_network(NamespaceId(req.namespace_id), client_public_key)
            .await
            .map_err(client_error_to_status)?;

        Ok(Response::new(proto::DisconnectNetworkResponse {}))
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
