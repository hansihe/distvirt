use std::path::PathBuf;

use anyhow::{Context, bail};
use containerd_client::services::v1::PluginsRequest;
use containerd_client::services::v1::content_client::ContentClient;
use containerd_client::services::v1::introspection_client::IntrospectionClient;
use containerd_client::services::v1::snapshots::{
    RemoveSnapshotRequest, StatSnapshotRequest, ViewSnapshotRequest,
    snapshots_client::SnapshotsClient,
};
use oci_spec::image::ImageConfiguration;
use sha2::{Digest, Sha256};
use tonic::transport::Channel;

use super::content::read_content;
use super::generate_id;
use super::image::resolve_platform_manifest;
use super::lease::ContainerdLease;
use super::ns_request;

/// Verify that a snapshotter plugin is loaded and initialized in containerd.
pub async fn check_snapshotter(channel: &Channel, snapshotter: &str) -> anyhow::Result<()> {
    let mut introspection = IntrospectionClient::new(channel.clone());
    let resp = introspection
        .plugins(PluginsRequest {
            filters: vec![format!(
                "type==io.containerd.snapshotter.v1,id=={}",
                snapshotter
            )],
        })
        .await
        .context("querying containerd plugins")?;

    let plugins = resp.into_inner().plugins;
    let plugin = plugins.into_iter().next().with_context(|| {
        format!(
            "{} snapshotter is not registered in containerd \
             (is the plugin installed?)",
            snapshotter
        )
    })?;

    if let Some(init_err) = plugin.init_err {
        bail!(
            "{} snapshotter failed to initialize: {}",
            snapshotter,
            init_err.message
        );
    }

    Ok(())
}

/// Check if a snapshot exists in the given snapshotter.
///
/// Returns `Ok(true)` if the snapshot exists, `Ok(false)` if not found.
pub async fn stat_snapshot(
    channel: &Channel,
    namespace: &str,
    snapshotter: &str,
    key: &str,
) -> anyhow::Result<bool> {
    let mut snapshots = SnapshotsClient::new(channel.clone());
    let req = StatSnapshotRequest {
        snapshotter: snapshotter.to_string(),
        key: key.to_string(),
    };
    match snapshots.stat(ns_request(req, namespace)).await {
        Ok(_) => Ok(true),
        Err(e) if e.code() == tonic::Code::NotFound => Ok(false),
        Err(e) => Err(e).with_context(|| {
            format!("stat snapshot {}/{}", snapshotter, key)
        }),
    }
}

/// Remove a snapshot (view or active) from the given snapshotter.
pub async fn remove_snapshot(
    channel: &Channel,
    namespace: &str,
    snapshotter: &str,
    key: &str,
) -> anyhow::Result<()> {
    let mut snapshots = SnapshotsClient::new(channel.clone());
    let req = RemoveSnapshotRequest {
        snapshotter: snapshotter.to_string(),
        key: key.to_string(),
    };
    snapshots
        .remove(ns_request(req, namespace))
        .await
        .with_context(|| format!("removing snapshot {}/{}", snapshotter, key))?;
    Ok(())
}

/// Create a blockfile snapshot view for an image.
///
/// Computes the final chain ID from the image manifest, creates a snapshot
/// view (auto-leased via the provided lease), and returns the block file path
/// along with the view's snapshot key (for cleanup via `remove_snapshot`).
pub async fn create_blockfile_view(
    channel: &Channel,
    lease: &ContainerdLease,
    image_ref: &str,
) -> anyhow::Result<(PathBuf, String)> {
    let manifest =
        resolve_platform_manifest(channel, lease.namespace(), image_ref).await?;
    let config_digest = manifest.config().digest().to_string();

    let mut content_client = ContentClient::new(channel.clone());
    let config_bytes =
        read_content(&mut content_client, lease.namespace(), &config_digest).await?;
    let img_config: ImageConfiguration =
        serde_json::from_slice(&config_bytes).context("parsing image config")?;
    let diff_ids = img_config.rootfs().diff_ids();
    let chain_ids = compute_chain_ids(diff_ids);
    let final_chain_id = chain_ids.last().context("image has no layers")?;

    let mut snapshots = SnapshotsClient::new(channel.clone());
    let view_key = format!("distvirt-view-{}", generate_id());
    let req = ViewSnapshotRequest {
        snapshotter: "blockfile".to_string(),
        key: view_key.clone(),
        parent: final_chain_id.clone(),
        labels: Default::default(),
    };
    // Auto-leased: the view snapshot is added to the lease in the same
    // DB transaction as creation.
    let resp = snapshots
        .view(lease.request(req))
        .await
        .context("creating blockfile snapshot view")?;
    let mounts = resp.into_inner().mounts;

    if mounts.is_empty() {
        bail!("no mounts returned for blockfile snapshot view");
    }

    let blockfile_path = PathBuf::from(&mounts[0].source);
    log::info!("blockfile snapshot at {:?}", blockfile_path);

    Ok((blockfile_path, view_key))
}

/// Compute OCI chain IDs from diff IDs.
///
/// chain[0] = diff_ids[0]
/// chain[n] = sha256(chain[n-1] + " " + diff_ids[n])
pub fn compute_chain_ids(diff_ids: &[String]) -> Vec<String> {
    let mut chain_ids: Vec<String> = Vec::with_capacity(diff_ids.len());
    for (i, diff_id) in diff_ids.iter().enumerate() {
        let chain_id = if i == 0 {
            diff_id.clone()
        } else {
            let mut hasher = Sha256::new();
            hasher.update(chain_ids[i - 1].as_bytes());
            hasher.update(b" ");
            hasher.update(diff_id.as_bytes());
            format!("sha256:{:x}", hasher.finalize())
        };
        chain_ids.push(chain_id);
    }
    chain_ids
}
