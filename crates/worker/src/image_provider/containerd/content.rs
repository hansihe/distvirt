use std::collections::HashMap;

use anyhow::Context;
use containerd_client::services::v1::{
    Info as ContentInfo, ReadContentRequest, UpdateRequest as ContentUpdateRequest,
    content_client::ContentClient,
};
use tonic::transport::Channel;

use super::ns_request;

pub async fn read_content(
    content: &mut ContentClient<Channel>,
    namespace: &str,
    digest: &str,
) -> anyhow::Result<Vec<u8>> {
    let req = ReadContentRequest {
        digest: digest.to_string(),
        ..Default::default()
    };
    let resp = content
        .read(ns_request(req, namespace))
        .await
        .with_context(|| format!("reading content {}", digest))?;

    let mut data = Vec::new();
    let mut stream = resp.into_inner();
    while let Some(chunk) = stream.message().await.context("reading content stream")? {
        data.extend_from_slice(&chunk.data);
    }
    Ok(data)
}

/// Set the GC reference label on the image config blob linking it to
/// the committed snapshot chain for the given snapshotter.
///
/// Sets label: `containerd.io/gc.ref.snapshot.<snapshotter>` = `<final_chain_id>`
///
/// This keeps committed layer snapshots alive permanently (as opposed to
/// leases which are temporary operation protection).
pub async fn set_snapshot_gc_label(
    channel: &Channel,
    namespace: &str,
    config_digest: &str,
    final_chain_id: &str,
    snapshotter: &str,
) -> anyhow::Result<()> {
    let mut content_client = ContentClient::new(channel.clone());

    let label_key = format!("containerd.io/gc.ref.snapshot.{}", snapshotter);
    let mut labels = HashMap::new();
    labels.insert(label_key.clone(), final_chain_id.to_string());

    let req = ContentUpdateRequest {
        info: Some(ContentInfo {
            digest: config_digest.to_string(),
            labels,
            ..Default::default()
        }),
        update_mask: Some(prost_types::FieldMask {
            paths: vec![format!("labels.{}", label_key)],
        }),
    };

    content_client
        .update(ns_request(req, namespace))
        .await
        .with_context(|| {
            format!(
                "setting GC ref label on config {} for {}/{}",
                config_digest, snapshotter, final_chain_id
            )
        })?;

    log::info!(
        "set GC ref label: config {} -> {}/{}",
        config_digest,
        snapshotter,
        final_chain_id
    );

    Ok(())
}
