use std::time::Duration;

use distvirt_client::connection::{handle_grpc_error, Client};
use distvirt_client_protocol::stream_logs_response::Message;

use crate::format;

pub async fn logs(
    mut client: Client,
    namespace_id: &str,
    workload: Option<&str>,
    follow: bool,
) -> anyhow::Result<()> {
    let mut stream =
        distvirt_client::operations::stream_logs(&mut client, namespace_id, workload).await?;

    while let Some(resp) = stream.message().await.map_err(handle_grpc_error)? {
        match resp.message {
            Some(Message::LogChunk(chunk)) => format::print_log_chunk(&chunk),
            Some(Message::HistoricalComplete(_)) => {
                if !follow {
                    break;
                }
            }
            None => {}
        }
    }

    Ok(())
}

pub async fn events(
    mut client: Client,
    namespace_id: &str,
    workloads: &[String],
    services: &[String],
    follow: bool,
) -> anyhow::Result<()> {
    let mut stream =
        distvirt_client::operations::stream_events(&mut client, namespace_id, workloads, services)
            .await?;

    if follow {
        while let Some(event) = stream.message().await.map_err(handle_grpc_error)? {
            format::print_event_line(&event);
        }
    } else {
        loop {
            match tokio::time::timeout(Duration::from_secs(2), stream.message()).await {
                Ok(Ok(Some(event))) => format::print_event_line(&event),
                Ok(Ok(None)) | Err(_) => break,
                Ok(Err(e)) => return Err(handle_grpc_error(e).into()),
            }
        }
    }

    Ok(())
}
