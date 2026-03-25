pub mod content;
pub mod image;
pub mod lease;
pub mod resource;
pub mod snapshot;
pub mod unpack;

use anyhow::Context;
use containerd_client as client;
use tonic::transport::Channel;

// Re-export public items for use by provider implementations.
pub use image::{
    ensure_image, extract_files_from_layers, read_image_config, resolve_platform_manifest,
};
pub use unpack::ensure_unpacked;

pub(crate) fn generate_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Connect to a containerd instance.
pub async fn connect(socket: &str) -> anyhow::Result<Channel> {
    client::connect(socket)
        .await
        .with_context(|| format!("connecting to containerd at {}", socket))
}
