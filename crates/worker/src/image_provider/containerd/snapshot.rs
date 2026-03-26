use std::path::PathBuf;

use anyhow::{Context, bail};
use containerd_client::services::v1::PluginsRequest;
use containerd_client::services::v1::introspection_client::IntrospectionClient;
use containerd_client::services::v1::snapshots::{
    RemoveSnapshotRequest, StatSnapshotRequest, ViewSnapshotRequest,
    snapshots_client::SnapshotsClient,
};
use sha2::{Digest, Sha256};
use tonic::transport::Channel;

use super::generate_id;
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

/// Create a snapshot view and return the block file path and view key.
///
/// Note: `Snapshots.View` does NOT auto-add to the lease despite the lease
/// header being set. The view is short-lived (copied and removed immediately
/// by the caller), so the GC risk window is negligible.
pub async fn create_blockfile_view(
    channel: &Channel,
    lease: &ContainerdLease,
    snapshotter: &str,
    final_chain_id: &str,
) -> anyhow::Result<(PathBuf, String)> {
    let mut snapshots = SnapshotsClient::new(channel.clone());
    let view_key = format!("distvirt-view-{}", generate_id());
    let req = ViewSnapshotRequest {
        snapshotter: snapshotter.to_string(),
        key: view_key.clone(),
        parent: final_chain_id.to_string(),
        labels: Default::default(),
    };
    let resp = snapshots
        .view(lease.request(req))
        .await
        .context("creating snapshot view")?;
    let mounts = resp.into_inner().mounts;

    if mounts.is_empty() {
        bail!("no mounts returned for snapshot view");
    }

    let blockfile_path = PathBuf::from(&mounts[0].source);
    log::info!("snapshot view at {:?}", blockfile_path);

    Ok((blockfile_path, view_key))
}

/// Create a snapshot view for the overlayfs snapshotter and return the mount
/// descriptors and view key.
///
/// The returned mounts describe how to assemble the merged rootfs:
/// - Multi-layer: `type="overlay"` with `lowerdir=...` in options
/// - Single-layer: `type="bind"` with `source` pointing to the layer directory
///
/// The caller is responsible for mounting these and cleaning up (unmount +
/// `remove_snapshot` of the view key) when done.
pub async fn create_overlayfs_view(
    channel: &Channel,
    lease: &ContainerdLease,
    snapshotter: &str,
    final_chain_id: &str,
) -> anyhow::Result<(Vec<containerd_client::types::Mount>, String)> {
    let mut snapshots = SnapshotsClient::new(channel.clone());
    let view_key = format!("distvirt-view-{}", generate_id());
    let req = ViewSnapshotRequest {
        snapshotter: snapshotter.to_string(),
        key: view_key.clone(),
        parent: final_chain_id.to_string(),
        labels: Default::default(),
    };
    let resp = snapshots
        .view(lease.request(req))
        .await
        .context("creating overlayfs snapshot view")?;
    let mounts = resp.into_inner().mounts;

    if mounts.is_empty() {
        bail!("no mounts returned for overlayfs snapshot view");
    }

    log::info!(
        "overlayfs snapshot view created (key={}, mounts={})",
        view_key,
        mounts.len()
    );

    Ok((mounts, view_key))
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
