use std::collections::HashMap;
use std::sync::Mutex;

use anyhow::{Context, bail};
use containerd_client::services::v1::snapshots::{
    CommitSnapshotRequest, PrepareSnapshotRequest,
    snapshots_client::SnapshotsClient,
};
use containerd_client::services::v1::{
    ApplyRequest, Info as ContentInfo, UpdateRequest as ContentUpdateRequest,
    content_client::ContentClient,
    diff_client::DiffClient,
};
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

use super::generate_id;
use super::image::ResolvedImage;
use super::lease::ContainerdLease;
use super::ns_request;
use super::snapshot::{check_snapshotter, remove_snapshot, stat_snapshot};

const MAX_PREPARE_RETRIES: usize = 3;

/// Coordinates concurrent layer unpack operations so that only one task
/// unpacks a given layer (identified by chain ID) at a time. Other tasks
/// wanting the same layer wait for the in-progress unpack to finish, then
/// re-attempt (the optimistic prepare pattern handles the rest).
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

impl UnpackCoordinator {
    /// Wait until no other task is unpacking this chain_id, then claim it.
    ///
    /// Returns a guard that, when dropped, signals waiting tasks.
    /// Call `release()` before dropping to clean up the map entry.
    async fn claim(&self, chain_id: &str) -> tokio_util::sync::DropGuard {
        loop {
            let waiter = {
                let mut map = self.in_progress.lock().unwrap();
                match map.get(chain_id) {
                    Some(token) if !token.is_cancelled() => {
                        // Another task is working on it — wait.
                        token.clone()
                    }
                    _ => {
                        // No entry, or stale (cancelled) entry — claim it.
                        map.remove(chain_id); // no-op if not present
                        let token = CancellationToken::new();
                        let guard = token.clone().drop_guard();
                        map.insert(chain_id.to_string(), token);
                        return guard;
                    }
                }
            };
            // Wait outside the lock for the current holder to finish.
            waiter.cancelled().await;
        }
    }

    /// Remove the map entry for a chain_id.
    fn release(&self, chain_id: &str) {
        self.in_progress.lock().unwrap().remove(chain_id);
    }
}

/// Ensure the image is unpacked with the given snapshotter.
///
/// Uses the optimistic prepare-first pattern from containerd's Go client:
/// no Stat before Prepare — instead, reacts to AlreadyExists/NotFound errors.
/// This avoids TOCTOU races between snapshot existence checks and creation.
///
/// The `UnpackCoordinator` prevents concurrent in-process unpack of the
/// same layer. Cross-process races are handled by the protocol itself
/// (AlreadyExists on Commit is treated as success).
pub async fn ensure_unpacked(
    channel: &Channel,
    lease: &ContainerdLease,
    resolved: &ResolvedImage,
    snapshotter: &str,
    coordinator: &UnpackCoordinator,
) -> anyhow::Result<()> {
    check_snapshotter(channel, snapshotter).await?;

    let layers = resolved.layers();
    let chain_ids = resolved.chain_ids();
    let total = layers.len();

    log::info!(
        "unpacking image with {} snapshotter ({} layers)",
        snapshotter, total,
    );

    let mut snapshots = SnapshotsClient::new(channel.clone());
    let mut diff = DiffClient::new(channel.clone());
    let mut content = ContentClient::new(channel.clone());

    for (i, (layer, chain_id)) in layers.iter().zip(chain_ids.iter()).enumerate() {
        let parent = if i == 0 { "" } else { &chain_ids[i - 1] };

        // Coordinate: wait if another in-process task is unpacking this layer.
        let guard = coordinator.claim(chain_id).await;

        let result = unpack_layer(
            &mut snapshots,
            &mut diff,
            &mut content,
            lease,
            channel,
            snapshotter,
            chain_id,
            parent,
            layer,
            i + 1,
            total,
        )
        .await;

        // Release map entry then drop guard (wake waiters) regardless of result.
        coordinator.release(chain_id);
        drop(guard);

        result?;
    }

    log::info!("image unpacked with {} snapshotter", snapshotter);
    Ok(())
}

/// Unpack a single image layer using the optimistic prepare-first pattern.
///
/// Does NOT Stat before Prepare — avoids TOCTOU races. Instead:
/// - AlreadyExists on Prepare → Stat chainID: exists=skip, missing=retry
/// - AlreadyExists on Commit  → concurrent unpack won, treat as success
/// - Any failure after Prepare → remove active snapshot to prevent leaks
///
/// Retries Prepare up to 3 times with fresh random keys.
async fn unpack_layer(
    snapshots: &mut SnapshotsClient<Channel>,
    diff: &mut DiffClient<Channel>,
    content: &mut ContentClient<Channel>,
    lease: &ContainerdLease,
    channel: &Channel,
    snapshotter: &str,
    chain_id: &str,
    parent: &str,
    layer: &oci_spec::image::Descriptor,
    layer_num: usize,
    total_layers: usize,
) -> anyhow::Result<()> {
    let namespace = lease.namespace();

    for attempt in 0..MAX_PREPARE_RETRIES {
        let active_key = format!("extract-{}-{}", generate_id(), attempt);

        log::debug!(
            "layer {}/{}: preparing snapshot (parent={}, attempt={})",
            layer_num,
            total_layers,
            if parent.is_empty() { "<none>" } else { parent },
            attempt + 1,
        );

        // Optimistic: attempt Prepare directly, react to errors.
        let prepare_req = PrepareSnapshotRequest {
            snapshotter: snapshotter.to_string(),
            key: active_key.clone(),
            parent: parent.to_string(),
            labels: Default::default(),
        };
        let mounts = match snapshots.prepare(lease.request(prepare_req)).await {
            Ok(resp) => resp.into_inner().mounts,
            Err(e) if e.code() == tonic::Code::AlreadyExists => {
                // Disambiguate: committed snapshot exists, or key collision?
                if stat_snapshot(channel, namespace, snapshotter, chain_id).await? {
                    log::debug!(
                        "layer {}/{}: snapshot {} already exists, skipping",
                        layer_num, total_layers, chain_id,
                    );
                    return Ok(());
                }
                // Key collision with another active snapshot — retry.
                log::debug!(
                    "layer {}/{}: prepare key collision, retrying",
                    layer_num, total_layers,
                );
                continue;
            }
            Err(e) if e.code() == tonic::Code::NotFound => {
                bail!(
                    "layer {}/{}: parent snapshot {} not found",
                    layer_num, total_layers, parent,
                );
            }
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("preparing snapshot for layer {}/{}", layer_num, total_layers)
                });
            }
        };

        // Active snapshot created — must clean up on any failure path.
        match apply_and_commit(
            snapshots, diff, content, lease, channel, snapshotter, chain_id, &active_key, layer,
            mounts, layer_num, total_layers,
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(e) => {
                let _ = remove_snapshot(channel, namespace, snapshotter, &active_key).await;
                return Err(e);
            }
        }
    }

    bail!(
        "layer {}/{}: failed to prepare snapshot after {} retries",
        layer_num, total_layers, MAX_PREPARE_RETRIES,
    )
}

/// Apply a layer diff and commit the snapshot.
///
/// Handles AlreadyExists on commit (concurrent out-of-process unpack won).
/// On other errors, the caller is responsible for cleaning up the active
/// snapshot.
async fn apply_and_commit(
    snapshots: &mut SnapshotsClient<Channel>,
    diff: &mut DiffClient<Channel>,
    content: &mut ContentClient<Channel>,
    lease: &ContainerdLease,
    channel: &Channel,
    snapshotter: &str,
    chain_id: &str,
    active_key: &str,
    layer: &oci_spec::image::Descriptor,
    mounts: Vec<containerd_client::types::Mount>,
    layer_num: usize,
    total_layers: usize,
) -> anyhow::Result<()> {
    let namespace = lease.namespace();

    // Apply the layer diff.
    let apply_req = ApplyRequest {
        diff: Some(containerd_client::types::Descriptor {
            media_type: layer.media_type().to_string(),
            digest: layer.digest().to_string(),
            size: layer.size() as i64,
            annotations: Default::default(),
        }),
        mounts,
        payloads: Default::default(),
        sync_fs: false,
    };
    let apply_resp = diff
        .apply(lease.request(apply_req))
        .await
        .with_context(|| format!("applying layer {}/{} diff", layer_num, total_layers))?;

    // Set the uncompressed digest label on the compressed layer blob so
    // containerd can map compressed→uncompressed for future operations.
    if let Some(applied) = apply_resp.into_inner().applied {
        let compressed_digest = layer.digest().to_string();
        let uncompressed_digest = &applied.digest;
        if !uncompressed_digest.is_empty() && *uncompressed_digest != compressed_digest {
            let label_key = "containerd.io/image.uncompressed".to_string();
            let mut labels = HashMap::new();
            labels.insert(label_key.clone(), uncompressed_digest.clone());
            let req = ContentUpdateRequest {
                info: Some(ContentInfo {
                    digest: compressed_digest,
                    labels,
                    ..Default::default()
                }),
                update_mask: Some(prost_types::FieldMask {
                    paths: vec![format!("labels.{}", label_key)],
                }),
            };
            content
                .update(ns_request(req, namespace))
                .await
                .with_context(|| {
                    format!(
                        "setting uncompressed label for layer {}/{}",
                        layer_num, total_layers
                    )
                })?;
        }
    }

    // Commit the snapshot with chain_id as the permanent name.
    let commit_req = CommitSnapshotRequest {
        snapshotter: snapshotter.to_string(),
        name: chain_id.to_string(),
        key: active_key.to_string(),
        labels: Default::default(),
    };
    match snapshots.commit(lease.request(commit_req)).await {
        Ok(_) => {
            log::debug!(
                "layer {}/{}: committed as {}",
                layer_num, total_layers, chain_id,
            );
            Ok(())
        }
        Err(e) if e.code() == tonic::Code::AlreadyExists => {
            log::debug!(
                "layer {}/{}: snapshot {} already committed by concurrent unpack",
                layer_num, total_layers, chain_id,
            );
            // Clean up our active snapshot since we lost the race.
            let _ = remove_snapshot(channel, namespace, snapshotter, active_key).await;
            Ok(())
        }
        Err(e) => Err(e).with_context(|| {
            format!(
                "committing snapshot for layer {}/{}",
                layer_num, total_layers
            )
        }),
    }
}
