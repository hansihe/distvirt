use distvirt_client_protocol::*;

use crate::client::{self, Client};

pub async fn splice(
    mut client: Client,
    namespace_id: &str,
    workload_id: &str,
    worker_id: &str,
) -> anyhow::Result<()> {
    client
        .splice(SpliceRequest {
            namespace_id: namespace_id.to_string(),
            workload_id: workload_id.to_string(),
            local_worker_id: worker_id.to_string(),
        })
        .await
        .map_err(client::handle_grpc_error)?;

    eprintln!(
        "spliced {}/{} to worker {}",
        namespace_id, workload_id, worker_id
    );
    eprintln!("press Ctrl+C to unsplice");

    // Hold open until Ctrl+C
    tokio::signal::ctrl_c().await?;

    eprintln!("\nunsplicing...");
    client
        .unsplice(UnspliceRequest {
            namespace_id: namespace_id.to_string(),
            workload_id: workload_id.to_string(),
        })
        .await
        .map_err(client::handle_grpc_error)?;

    eprintln!("unspliced");
    Ok(())
}
