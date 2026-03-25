use anyhow::Context;
use containerd_client::services::v1::{ReadContentRequest, content_client::ContentClient};
use containerd_client::with_namespace;
use tonic::Request;
use tonic::transport::Channel;

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
        .read(with_namespace!(req, namespace))
        .await
        .with_context(|| format!("reading content {}", digest))?;

    let mut data = Vec::new();
    let mut stream = resp.into_inner();
    while let Some(chunk) = stream.message().await.context("reading content stream")? {
        data.extend_from_slice(&chunk.data);
    }
    Ok(data)
}
