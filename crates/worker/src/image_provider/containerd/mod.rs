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
pub use image::{ensure_image, extract_files_from_layers};
pub use unpack::ensure_unpacked;

pub(crate) fn generate_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Build a tonic Request with the `containerd-namespace` metadata header.
///
/// For read-only operations (Stat, Content.Read) that don't need lease
/// protection. For operations that create resources, use
/// `ContainerdLease::request()` instead to get automatic lease protection.
pub fn ns_request<T>(msg: T, namespace: &str) -> tonic::Request<T> {
    let mut req = tonic::Request::new(msg);
    req.metadata_mut().insert(
        "containerd-namespace",
        namespace.parse().expect("valid namespace"),
    );
    req
}

/// Connect to a containerd instance.
pub async fn connect(socket: &str) -> anyhow::Result<Channel> {
    client::connect(socket)
        .await
        .with_context(|| format!("connecting to containerd at {}", socket))
}
