use distvirt_client_protocol::*;

use crate::client::{self, Client};
use crate::format;

pub async fn logs(
    mut client: Client,
    namespace_id: &str,
    workload: Option<&str>,
    _follow: bool,
) -> anyhow::Result<()> {
    let mut stream = client
        .stream_logs(StreamLogsRequest {
            namespace_id: namespace_id.to_string(),
            workload_id: workload.map(|w| w.to_string()),
            container_ids: vec![],
        })
        .await
        .map_err(client::handle_grpc_error)?
        .into_inner();

    while let Some(chunk) = stream.message().await.map_err(client::handle_grpc_error)? {
        format::print_log_chunk(&chunk);
    }

    Ok(())
}

pub async fn events(
    mut client: Client,
    namespace_id: &str,
    workloads: &[String],
    services: &[String],
    _follow: bool,
) -> anyhow::Result<()> {
    let mut stream = client
        .stream_events(StreamEventsRequest {
            namespace_id: namespace_id.to_string(),
            workload_ids: workloads.to_vec(),
            service_ids: services.to_vec(),
        })
        .await
        .map_err(client::handle_grpc_error)?
        .into_inner();

    while let Some(event) = stream.message().await.map_err(client::handle_grpc_error)? {
        format::print_event_line(&event);
    }

    Ok(())
}
