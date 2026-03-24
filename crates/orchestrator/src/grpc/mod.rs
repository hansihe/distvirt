mod conversions;

use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use distvirt_client_protocol::proto;
use distvirt_client_protocol::proto::distvirt_client_server::DistvirtClient;

use distvirt_worker_protocol::PodId;

use crate::core::ClientError;
use crate::event_bus::EventBusHandle;
use crate::id_registry::IdRegistryMap;
use crate::log_bus::LogBusHandle;
use crate::shell::r#async::ShellHandle;
use crate::types::NamespaceId;

use conversions::{convert_pod_status_report, convert_proto_patch, convert_proto_spec, convert_status_report, convert_worker_query_info};
use crate::types::{IpAllocKind, IpAllocResult};

fn client_error_to_status(err: ClientError) -> Status {
    match err {
        ClientError::WorkerNotFound => Status::not_found(err.to_string()),
        ClientError::NamespaceNotFound => Status::not_found(err.to_string()),
        ClientError::NamespaceAlreadyExists => Status::already_exists(err.to_string()),
        ClientError::NoTunnelWorker => Status::failed_precondition(err.to_string()),
        ClientError::IpExhausted => Status::resource_exhausted(err.to_string()),
        ClientError::IpAllocation(_) => Status::invalid_argument(err.to_string()),
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
    log_bus: LogBusHandle,
    event_bus: EventBusHandle,
    id_registry_map: IdRegistryMap,
}

impl DistvirtClientService {
    pub fn new(
        handle: ShellHandle,
        log_bus: LogBusHandle,
        event_bus: EventBusHandle,
        id_registry_map: IdRegistryMap,
    ) -> Self {
        DistvirtClientService {
            handle,
            log_bus,
            event_bus,
            id_registry_map,
        }
    }
}

fn alloc_to_proto_ips(alloc: &IpAllocResult) -> (
    std::collections::HashMap<String, proto::IpAllocation>,
    std::collections::HashMap<String, proto::IpAllocation>,
) {
    let workload_ips = alloc.workload_ips.iter().map(|(k, v)| {
        (k.0.clone(), proto::IpAllocation {
            ip: v.ip.to_string(),
            is_manual: v.kind == IpAllocKind::Manual,
        })
    }).collect();
    let service_ips = alloc.service_ips.iter().map(|(k, v)| {
        (k.clone(), proto::IpAllocation {
            ip: v.ip.to_string(),
            is_manual: v.kind == IpAllocKind::Manual,
        })
    }).collect();
    (workload_ips, service_ips)
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
        let namespace_id = NamespaceId(req.namespace_id);
        self.handle
            .create_namespace(namespace_id.clone(), network)
            .await
            .map_err(client_error_to_status)?;
        let alloc = self.handle
            .update_namespace(namespace_id, spec)
            .await
            .map_err(client_error_to_status)?;
        let (workload_ips, service_ips) = alloc_to_proto_ips(&alloc);
        Ok(Response::new(proto::CreateNamespaceResponse {
            workload_ips,
            service_ips,
        }))
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
        let alloc = self.handle
            .update_namespace(NamespaceId(req.namespace_id), spec)
            .await
            .map_err(client_error_to_status)?;
        let (workload_ips, service_ips) = alloc_to_proto_ips(&alloc);
        Ok(Response::new(proto::UpdateNamespaceResponse {
            workload_ips,
            service_ips,
        }))
    }

    async fn patch_namespace(
        &self,
        request: Request<proto::PatchNamespaceRequest>,
    ) -> Result<Response<proto::PatchNamespaceResponse>, Status> {
        let req = request.into_inner();
        let ns_id = NamespaceId(req.namespace_id.clone());
        let patch = convert_proto_patch(req)?;
        let alloc = self.handle
            .patch_namespace(ns_id, patch)
            .await
            .map_err(client_error_to_status)?;
        let (workload_ips, service_ips) = alloc_to_proto_ips(&alloc);
        Ok(Response::new(proto::PatchNamespaceResponse {
            workload_ips,
            service_ips,
        }))
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

    type StreamLogsStream = ReceiverStream<Result<proto::StreamLogsResponse, Status>>;

    async fn stream_logs(
        &self,
        request: Request<proto::StreamLogsRequest>,
    ) -> Result<Response<Self::StreamLogsStream>, Status> {
        let req = request.into_inner();
        let namespace_id = NamespaceId(req.namespace_id);

        let container_filter = if req.container_ids.is_empty() {
            None
        } else {
            Some(req.container_ids)
        };

        let pod_filter: Option<Vec<PodId>> = if req.pod_ids.is_empty() {
            None
        } else {
            Some(req.pod_ids.iter().map(|id| PodId(id.parse::<u64>().unwrap_or(0))).collect())
        };

        let (historical, mut live_rx) = if let Some(ref workload_name) = req.workload_name {
            self.log_bus.subscribe_by_workload(
                &namespace_id,
                workload_name,
                container_filter.as_deref(),
            )
        } else {
            self.log_bus
                .subscribe(&namespace_id, pod_filter.as_deref(), container_filter.as_deref())
        };

        let (tx, rx) = tokio::sync::mpsc::channel(4096);

        tokio::spawn(async move {
            // Send historical chunks first.
            for chunk in historical {
                let proto_chunk = conversions::convert_log_chunk(chunk);
                let resp = proto::StreamLogsResponse {
                    message: Some(proto::stream_logs_response::Message::LogChunk(proto_chunk)),
                };
                if tx.send(Ok(resp)).await.is_err() {
                    return;
                }
            }
            // Send historical-complete marker.
            let marker = proto::StreamLogsResponse {
                message: Some(proto::stream_logs_response::Message::HistoricalComplete(
                    proto::HistoricalComplete {},
                )),
            };
            if tx.send(Ok(marker)).await.is_err() {
                return;
            }
            // Then stream live chunks.
            while let Some(chunk) = live_rx.recv().await {
                let proto_chunk = conversions::convert_log_chunk(chunk);
                let resp = proto::StreamLogsResponse {
                    message: Some(proto::stream_logs_response::Message::LogChunk(proto_chunk)),
                };
                if tx.send(Ok(resp)).await.is_err() {
                    return;
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
        request: Request<proto::StreamEventsRequest>,
    ) -> Result<Response<Self::StreamEventsStream>, Status> {
        let req = request.into_inner();
        let namespace_id = NamespaceId(req.namespace_id);

        let registry = self
            .id_registry_map
            .get(&namespace_id)
            .ok_or_else(|| Status::not_found("namespace not found"))?;

        let (historical, mut live_rx) = self.event_bus.subscribe(&namespace_id);

        let (tx, rx) = tokio::sync::mpsc::channel(256);

        tokio::spawn(async move {
            // Send historical events first.
            for event in &historical {
                let proto_events =
                    conversions::convert_observability_event(event, &registry);
                for proto_event in proto_events {
                    if tx.send(Ok(proto_event)).await.is_err() {
                        return;
                    }
                }
            }
            // Then stream live events.
            while let Some(event) = live_rx.recv().await {
                let proto_events =
                    conversions::convert_observability_event(&event, &registry);
                for proto_event in proto_events {
                    if tx.send(Ok(proto_event)).await.is_err() {
                        return;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type AttachWorkloadStream = ReceiverStream<Result<proto::AttachWorkloadOutput, Status>>;

    async fn attach_workload(
        &self,
        _request: Request<tonic::Streaming<proto::AttachWorkloadInput>>,
    ) -> Result<Response<Self::AttachWorkloadStream>, Status> {
        Err(Status::unimplemented(
            "AttachWorkload not yet implemented",
        ))
    }
}
