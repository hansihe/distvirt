use std::collections::HashMap;
use std::sync::Mutex;

use anyhow::{Context, bail};
use containerd_client::services::v1::snapshots::{
    CommitSnapshotRequest, PrepareSnapshotRequest,
    snapshots_client::SnapshotsClient,
};
use containerd_client::services::v1::{
    ApplyRequest, content_client::ContentClient,
    diff_client::DiffClient,
};
use containerd_client::with_namespace;
use oci_spec::image::ImageConfiguration;
use tokio_util::sync::CancellationToken;
use tonic::Request;
use tonic::transport::Channel;

use super::content::read_content;
use super::generate_id;
use super::image::resolve_platform_manifest;
use super::lease::ContainerdLease;
use super::resource::SnapshotRef;
use super::snapshot::{check_snapshotter, compute_chain_ids};

/// Coordinates concurrent layer unpack operations so that only one task
/// unpacks a given layer (identified by chain ID) at a time.  Other tasks
/// wanting the same layer wait for the in-progress unpack to finish, then
/// re-check whether the snapshot now exists.
pub struct UnpackCoordinator {
    in_progress: Mutex<HashMap<String, CancellationToken>>,
}

impl Default for UnpackCoordinator {
    fn default() -> Self {
        Self {
            in_progress: Mutex::new(HashMap::new()),
        }
    }
}

/// Ensure the image is unpacked with the given snapshotter.
///
/// Uses the lease's `try_add_resource` to atomically check whether each
/// layer snapshot exists and protect it from GC. Layers that don't exist
/// are unpacked, with the `UnpackCoordinator` preventing concurrent
/// unpack of the same layer by multiple tasks.
pub async fn ensure_unpacked(
    channel: &Channel,
    namespace: &str,
    image_ref: &str,
    snapshotter: &str,
    coordinator: &UnpackCoordinator,
    lease: &ContainerdLease,
) -> anyhow::Result<()> {
    // Fail early if the snapshotter plugin isn't available.
    check_snapshotter(channel, snapshotter).await?;

    log::info!(
        "unpacking image {} with {} snapshotter",
        image_ref, snapshotter
    );

    // Resolve manifest and read config to get diff_ids and layer descriptors.
    let manifest = resolve_platform_manifest(channel, namespace, image_ref).await?;
    let layers = manifest.layers();
    let config_digest = manifest.config().digest().to_string();

    let mut content = ContentClient::new(channel.clone());
    let config_bytes = read_content(&mut content, namespace, &config_digest).await?;
    let img_config: ImageConfiguration =
        serde_json::from_slice(&config_bytes).context("parsing image config")?;
    let diff_ids = img_config.rootfs().diff_ids();

    if diff_ids.len() != layers.len() {
        bail!(
            "image config has {} diff_ids but manifest has {} layers",
            diff_ids.len(),
            layers.len()
        );
    }

    // Compute chain IDs for each layer.
    let chain_ids = compute_chain_ids(diff_ids);

    let mut snapshots = SnapshotsClient::new(channel.clone());
    let mut diff = DiffClient::new(channel.clone());

    for (i, (layer_desc, chain_id)) in layers.iter().zip(chain_ids.iter()).enumerate() {
        let snapshot_ref = SnapshotRef {
            snapshotter: snapshotter.to_string(),
            key: chain_id.clone(),
        };

        // Coordinate with other tasks to ensure only one unpacks this layer.
        // Loop handles the case where a concurrent unpack fails and we retry.
        let drop_guard = loop {
            // Try adding the committed snapshot to our lease. If it succeeds,
            // the snapshot exists and is now protected from GC.
            if lease.try_add_resource(&snapshot_ref).await? {
                log::debug!(
                    "layer {}/{}: snapshot {} exists, added to lease",
                    i + 1, layers.len(), chain_id
                );
                break None;
            }

            enum Action {
                Wait(CancellationToken),
                Unpack(tokio_util::sync::DropGuard),
            }

            let action = {
                let mut map = coordinator.in_progress.lock().unwrap();
                if let Some(token) = map.get(chain_id).cloned() {
                    Action::Wait(token)
                } else {
                    let token = CancellationToken::new();
                    let drop_guard = token.clone().drop_guard();
                    map.insert(chain_id.clone(), token);
                    Action::Unpack(drop_guard)
                }
            };

            match action {
                Action::Wait(token) => {
                    log::debug!(
                        "layer {}/{}: waiting for concurrent unpack of {}",
                        i + 1, layers.len(), chain_id
                    );
                    token.cancelled().await;
                    // Loop back to re-check via try_add_resource.
                    continue;
                }
                Action::Unpack(guard) => break Some(guard),
            }
        };

        // If drop_guard is None, the snapshot already exists — skip this layer.
        let drop_guard = match drop_guard {
            Some(g) => g,
            None => continue,
        };

        let parent = if i == 0 {
            String::new()
        } else {
            chain_ids[i - 1].clone()
        };

        let active_key = format!("extract-{}-{}", generate_id(), i);
        log::debug!(
            "layer {}/{}: preparing snapshot (parent={})",
            i + 1,
            layers.len(),
            if parent.is_empty() { "<none>" } else { &parent }
        );

        // Prepare a writable snapshot.
        let prepare_req = PrepareSnapshotRequest {
            snapshotter: snapshotter.to_string(),
            key: active_key.clone(),
            parent,
            labels: Default::default(),
        };
        let prep_resp = snapshots
            .prepare(with_namespace!(prepare_req, namespace))
            .await
            .with_context(|| format!("preparing snapshot for layer {}", i + 1))?;
        let mounts = prep_resp.into_inner().mounts;

        // Protect the active snapshot from GC during apply.
        let active_ref = SnapshotRef {
            snapshotter: snapshotter.to_string(),
            key: active_key.clone(),
        };
        lease.add_resource(&active_ref).await?;

        // Apply the layer diff.
        let apply_req = ApplyRequest {
            diff: Some(containerd_client::types::Descriptor {
                media_type: layer_desc.media_type().to_string(),
                digest: layer_desc.digest().to_string(),
                size: layer_desc.size() as i64,
                annotations: Default::default(),
            }),
            mounts,
            payloads: Default::default(),
            sync_fs: false,
        };
        diff.apply(with_namespace!(apply_req, namespace))
            .await
            .with_context(|| format!("applying layer {} diff", i + 1))?;

        // Commit the snapshot with chain_id as the key.
        let commit_req = CommitSnapshotRequest {
            snapshotter: snapshotter.to_string(),
            name: chain_id.clone(),
            key: active_key.clone(),
            labels: Default::default(),
        };
        snapshots
            .commit(with_namespace!(commit_req, namespace))
            .await
            .with_context(|| format!("committing snapshot for layer {}", i + 1))?;

        // Add the committed snapshot to the lease.
        lease.add_resource(&snapshot_ref).await?;

        // Unpack succeeded — remove from in-progress map and drop the guard
        // to wake any waiters.
        coordinator.in_progress.lock().unwrap().remove(chain_id);
        drop(drop_guard);

        log::debug!("layer {}/{}: committed as {}", i + 1, layers.len(), chain_id);
    }

    log::info!(
        "image {} unpacked with {} snapshotter",
        image_ref, snapshotter
    );

    Ok(())
}
