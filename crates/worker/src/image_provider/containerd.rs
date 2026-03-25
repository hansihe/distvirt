use std::collections::HashMap;
use std::env::consts;
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use containerd_client as client;
use containerd_client::services::v1::snapshots::{
    CommitSnapshotRequest, PrepareSnapshotRequest, RemoveSnapshotRequest, StatSnapshotRequest,
    ViewSnapshotRequest, snapshots_client::SnapshotsClient,
};
use containerd_client::services::v1::{
    ApplyRequest, GetImageRequest, InfoRequest, PluginsRequest, ReadContentRequest, StreamInit,
    TransferOptions, TransferRequest, UpdateRequest, content_client::ContentClient,
    diff_client::DiffClient, images_client::ImagesClient,
    introspection_client::IntrospectionClient, streaming_client::StreamingClient,
    transfer_client::TransferClient,
};
use containerd_client::to_any;
use containerd_client::types::Platform;
use containerd_client::types::transfer::{
    AuthResponse, AuthType, ImageStore, OciRegistry, RegistryResolver,
};
use containerd_client::with_namespace;
use oci_spec::image::{ImageConfiguration, ImageManifest};
use prost::Name;
use sha2::{Digest, Sha256};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tonic::transport::Channel;

use super::docker_config;

pub use crate::oci::ImageConfig;

/// RAII guard that removes a blockfile snapshot view on drop.
pub struct BlockfileCleanup {
    pub channel: Channel,
    pub namespace: String,
    pub view_key: String,
    pub handle: tokio::runtime::Handle,
}

impl Drop for BlockfileCleanup {
    fn drop(&mut self) {
        let channel = self.channel.clone();
        let namespace = self.namespace.clone();
        let view_key = self.view_key.clone();
        self.handle.spawn(async move {
            let mut snapshots = SnapshotsClient::new(channel);
            let req = RemoveSnapshotRequest {
                snapshotter: "blockfile".to_string(),
                key: view_key.clone(),
            };
            if let Err(e) = snapshots.remove(with_namespace!(req, &namespace)).await {
                log::warn!("BlockfileCleanup drop: snapshot remove {:?}: {}", view_key, e);
            }
        });
    }
}

fn oci_arch() -> &'static str {
    match consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        _ => consts::ARCH,
    }
}

/// Connect to a containerd instance.
pub async fn connect(socket: &str) -> anyhow::Result<Channel> {
    client::connect(socket)
        .await
        .with_context(|| format!("connecting to containerd at {}", socket))
}

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

/// Resolve the platform-specific manifest for an image.
///
/// Handles both single manifests and multi-arch image indexes.
pub async fn resolve_platform_manifest(
    channel: &Channel,
    namespace: &str,
    image_ref: &str,
) -> anyhow::Result<ImageManifest> {
    let mut images = ImagesClient::new(channel.clone());
    let mut content = ContentClient::new(channel.clone());

    let req = GetImageRequest {
        name: image_ref.to_string(),
    };
    let resp = images
        .get(with_namespace!(req, namespace))
        .await
        .with_context(|| format!("getting image {}", image_ref))?;
    let image = resp.into_inner().image.context("image record missing")?;
    let target = image.target.context("image target descriptor missing")?;

    let manifest_bytes = read_content(&mut content, namespace, &target.digest).await?;

    // Try to parse as image index first (multi-arch), then as single manifest.
    if let Ok(index) = serde_json::from_slice::<oci_spec::image::ImageIndex>(&manifest_bytes) {
        let arch = oci_arch();
        let platform_manifest = index
            .manifests()
            .iter()
            .find(|m| {
                m.platform().as_ref().is_some_and(|p| {
                    p.os() == &oci_spec::image::Os::Linux && p.architecture().to_string() == arch
                })
            })
            .with_context(|| format!("no manifest found for linux/{arch}"))?;
        let inner_bytes =
            read_content(&mut content, namespace, &platform_manifest.digest().to_string()).await?;
        serde_json::from_slice(&inner_bytes).context("parsing image manifest")
    } else {
        serde_json::from_slice(&manifest_bytes).context("parsing image manifest")
    }
}

/// Ensure the image content is present in containerd's content store.
///
/// If the image already exists locally, this is a no-op. Otherwise it pulls
/// from the registry. This only downloads content — it does NOT unpack with
/// any snapshotter. Use `ensure_unpacked` afterwards.
pub async fn ensure_image(
    channel: &Channel,
    namespace: &str,
    image_ref: &str,
    docker_config: Option<&Path>,
) -> anyhow::Result<()> {
    // Check if the image already exists locally.
    let mut images = ImagesClient::new(channel.clone());
    let req = GetImageRequest {
        name: image_ref.to_string(),
    };
    if images.get(with_namespace!(req, namespace)).await.is_ok() {
        log::info!("image {} already present locally", image_ref);
        return Ok(());
    }

    // Pull from registry.
    let arch = oci_arch();
    log::info!("pulling image {} for linux/{}", image_ref, arch);

    let credential =
        docker_config.and_then(|path| docker_config::lookup_credentials(path, image_ref));

    let resolver = if let Some(cred) = &credential {
        let stream_id = format!("distvirt-auth-{}", generate_id());
        log::debug!(
            "setting up auth stream {} for image {} (username={})",
            stream_id, image_ref, cred.username
        );
        setup_auth_stream(channel, namespace, &stream_id, cred.clone()).await?;
        Some(RegistryResolver {
            auth_stream: stream_id,
            ..Default::default()
        })
    } else {
        log::debug!("no credentials found for image {}", image_ref);
        None
    };

    let platform = Platform {
        os: "linux".to_string(),
        architecture: arch.to_string(),
        variant: String::new(),
        os_version: String::new(),
    };

    let source = OciRegistry {
        reference: image_ref.to_string(),
        resolver,
    };

    let destination = ImageStore {
        name: image_ref.to_string(),
        platforms: vec![platform],
        // No `unpacks` — we only want to download content, not unpack.
        ..Default::default()
    };

    let request = TransferRequest {
        source: Some(to_any(&source)),
        destination: Some(to_any(&destination)),
        options: Some(TransferOptions::default()),
    };

    let mut transfer = TransferClient::new(channel.clone());
    transfer
        .transfer(with_namespace!(request, namespace))
        .await
        .with_context(|| format!("pulling image {}", image_ref))?;
    log::info!("image {} pulled successfully", image_ref);

    Ok(())
}

/// Ensure the image is unpacked with the given snapshotter.
///
/// If already unpacked, this is a no-op. Otherwise performs a manual unpack
/// by iterating layers and using the Snapshots + Diff gRPC APIs, then sets
/// the GC ref label on the config content.
pub async fn ensure_unpacked(
    channel: &Channel,
    namespace: &str,
    image_ref: &str,
    snapshotter: &str,
) -> anyhow::Result<()> {
    // Fail early if the snapshotter plugin isn't available.
    check_snapshotter(channel, snapshotter).await?;

    // Check if already unpacked.
    if has_snapshot_unpack(channel, namespace, image_ref, snapshotter).await? {
        log::info!(
            "image {} already unpacked with {} snapshotter",
            image_ref, snapshotter
        );
        return Ok(());
    }

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
        // Check if this snapshot already exists (e.g. shared base layer or partial unpack).
        let stat_req = StatSnapshotRequest {
            snapshotter: snapshotter.to_string(),
            key: chain_id.clone(),
        };
        if snapshots
            .stat(with_namespace!(stat_req, namespace))
            .await
            .is_ok()
        {
            log::debug!("layer {}/{}: snapshot {} already exists", i + 1, layers.len(), chain_id);
            continue;
        }

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
        // If the snapshot was committed concurrently (or by a previous partial
        // run), treat "already exists" as success and clean up the active key.
        let commit_req = CommitSnapshotRequest {
            snapshotter: snapshotter.to_string(),
            name: chain_id.clone(),
            key: active_key.clone(),
            labels: Default::default(),
        };
        match snapshots
            .commit(with_namespace!(commit_req, namespace))
            .await
        {
            Ok(_) => {}
            Err(e) if e.code() == tonic::Code::AlreadyExists => {
                log::debug!(
                    "layer {}/{}: snapshot {} already committed, cleaning up active key",
                    i + 1, layers.len(), chain_id
                );
                // Remove the active snapshot we prepared since it's no longer needed.
                let rm_req = RemoveSnapshotRequest {
                    snapshotter: snapshotter.to_string(),
                    key: active_key,
                };
                let _ = snapshots.remove(with_namespace!(rm_req, namespace)).await;
            }
            Err(e) => {
                return Err(e)
                    .with_context(|| format!("committing snapshot for layer {}", i + 1));
            }
        }

        log::debug!("layer {}/{}: committed as {}", i + 1, layers.len(), chain_id);
    }

    // Set the GC ref label on the config content so `get_blockfile_path` and
    // `has_snapshot_unpack` can find the final snapshot.
    let final_chain_id = chain_ids
        .last()
        .context("image has no layers")?;
    let label_key = format!("containerd.io/gc.ref.snapshot.{}", snapshotter);

    // Read existing labels and merge.
    let info_req = InfoRequest {
        digest: config_digest.clone(),
    };
    let info_resp = content
        .info(with_namespace!(info_req, namespace))
        .await
        .context("reading config content info")?;
    let mut info = info_resp.into_inner().info.context("content info missing")?;
    info.labels.insert(label_key, final_chain_id.clone());

    let update_req = UpdateRequest {
        info: Some(info),
        update_mask: Some(prost_types::FieldMask {
            paths: vec!["labels".to_string()],
        }),
    };
    content
        .update(with_namespace!(update_req, namespace))
        .await
        .context("updating config content labels")?;

    log::info!(
        "image {} unpacked with {} snapshotter (snapshot: {})",
        image_ref, snapshotter, final_chain_id
    );

    Ok(())
}

/// Compute OCI chain IDs from diff IDs.
///
/// chain[0] = diff_ids[0]
/// chain[n] = sha256(chain[n-1] + " " + diff_ids[n])
fn compute_chain_ids(diff_ids: &[String]) -> Vec<String> {
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

/// Check whether an image has been unpacked with a given snapshotter by looking
/// for the snapshot label on the config content.
async fn has_snapshot_unpack(
    channel: &Channel,
    namespace: &str,
    image_ref: &str,
    snapshotter: &str,
) -> anyhow::Result<bool> {
    let manifest = resolve_platform_manifest(channel, namespace, image_ref).await?;
    let config_digest = manifest.config().digest().to_string();

    let mut content = ContentClient::new(channel.clone());
    let req = InfoRequest {
        digest: config_digest,
    };
    let resp = content
        .info(with_namespace!(req, namespace))
        .await
        .context("getting content info for config")?;
    let info = resp.into_inner().info.context("content info missing")?;

    let label = format!("containerd.io/gc.ref.snapshot.{}", snapshotter);
    Ok(info.labels.contains_key(&label))
}

/// Read OCI image configuration (entrypoint, cmd, env, user, etc).
pub async fn read_image_config(
    channel: &Channel,
    namespace: &str,
    image_ref: &str,
) -> anyhow::Result<ImageConfig> {
    let manifest = resolve_platform_manifest(channel, namespace, image_ref).await?;
    let config_digest = manifest.config().digest().to_string();

    let mut content = ContentClient::new(channel.clone());
    let config_bytes = read_content(&mut content, namespace, &config_digest).await?;
    let img_config: ImageConfiguration =
        serde_json::from_slice(&config_bytes).context("parsing image config")?;

    let oci_config = img_config.config().as_ref();

    Ok(ImageConfig {
        entrypoint: oci_config
            .and_then(|c| c.entrypoint().clone())
            .unwrap_or_default(),
        cmd: oci_config.and_then(|c| c.cmd().clone()).unwrap_or_default(),
        env: oci_config.and_then(|c| c.env().clone()).unwrap_or_default(),
        working_dir: oci_config
            .and_then(|c| c.working_dir().clone())
            .filter(|s| !s.is_empty()),
        user: oci_config
            .and_then(|c| c.user().clone())
            .filter(|s| !s.is_empty()),
        passwd_entries: Vec::new(),
        group_entries: Vec::new(),
    })
}

/// Get the blockfile path for an image from the blockfile snapshotter.
///
/// Creates a snapshot view and returns the block file path along with a
/// cleanup guard that removes the view on drop.
pub async fn get_blockfile_path(
    channel: &Channel,
    namespace: &str,
    image_ref: &str,
) -> anyhow::Result<(PathBuf, BlockfileCleanup)> {
    let manifest = resolve_platform_manifest(channel, namespace, image_ref).await?;
    let config_digest = manifest.config().digest().to_string();

    let mut content = ContentClient::new(channel.clone());
    let req = InfoRequest {
        digest: config_digest.clone(),
    };
    let resp = content
        .info(with_namespace!(req, namespace))
        .await
        .context("getting content info for config")?;
    let info = resp.into_inner().info.context("content info missing")?;

    let snapshot_key = info
        .labels
        .get("containerd.io/gc.ref.snapshot.blockfile")
        .context("blockfile snapshot key label not found on config content")?
        .clone();

    let mut snapshots = SnapshotsClient::new(channel.clone());
    let view_key = format!("distvirt-view-{}", generate_id());
    let req = ViewSnapshotRequest {
        snapshotter: "blockfile".to_string(),
        key: view_key.clone(),
        parent: snapshot_key,
        labels: Default::default(),
    };
    let resp = snapshots
        .view(with_namespace!(req, namespace))
        .await
        .context("creating blockfile snapshot view")?;
    let mounts = resp.into_inner().mounts;

    if mounts.is_empty() {
        bail!("no mounts returned for blockfile snapshot view");
    }

    let blockfile_path = PathBuf::from(&mounts[0].source);
    log::info!("blockfile snapshot at {:?}", blockfile_path);

    let cleanup = BlockfileCleanup {
        channel: channel.clone(),
        namespace: namespace.to_string(),
        view_key,
        handle: tokio::runtime::Handle::current(),
    };

    Ok((blockfile_path, cleanup))
}

/// Extract specific files from OCI image layer tarballs.
///
/// Walks layers top-to-bottom (highest priority first). For each target path,
/// returns the file contents if found, or omits it if deleted by a whiteout.
pub async fn extract_files_from_layers(
    channel: &Channel,
    namespace: &str,
    image_ref: &str,
    target_paths: &[&str],
) -> anyhow::Result<HashMap<String, Vec<u8>>> {
    let manifest = resolve_platform_manifest(channel, namespace, image_ref).await?;
    let layers = manifest.layers();

    let mut content = ContentClient::new(channel.clone());

    // Track which target paths are still unresolved.
    let mut remaining: std::collections::HashSet<String> =
        target_paths.iter().map(|s| s.to_string()).collect();
    let mut results: HashMap<String, Vec<u8>> = HashMap::new();

    // Walk layers top-to-bottom (last layer has highest priority).
    for layer_desc in layers.iter().rev() {
        if remaining.is_empty() {
            break;
        }

        let media_type = layer_desc.media_type().to_string();
        let blob = read_content(&mut content, namespace, &layer_desc.digest().to_string()).await?;

        // Decompress based on media type.
        let tar_data: Box<dyn Read> = if media_type.contains("+gzip") || media_type.contains(".gzip")
        {
            Box::new(flate2::read::GzDecoder::new(blob.as_slice()))
        } else if media_type.ends_with(".tar") || media_type.ends_with("/tar") {
            // Uncompressed tar.
            Box::new(blob.as_slice())
        } else {
            bail!(
                "unsupported layer media type: {} (layer {})",
                media_type,
                layer_desc.digest()
            );
        };

        let mut archive = tar::Archive::new(tar_data);
        let entries = archive.entries().context("reading tar entries")?;

        for entry_result in entries {
            let mut entry = match entry_result {
                Ok(e) => e,
                Err(e) => {
                    log::warn!("skipping unreadable tar entry: {}", e);
                    continue;
                }
            };

            let path = match entry.path() {
                Ok(p) => p.to_path_buf(),
                Err(_) => continue,
            };

            // Normalize: strip leading "./" if present.
            let path_str = path
                .to_str()
                .unwrap_or_default()
                .trim_start_matches("./")
                .to_string();

            // Check for opaque whiteout — means the entire directory was replaced.
            if path_str.ends_with("/.wh..wh..opq") {
                let dir = path_str.trim_end_matches("/.wh..wh..opq");
                // Any remaining targets under this directory are deleted.
                remaining.retain(|target| !target.starts_with(dir));
                continue;
            }

            // Check for file whiteout.
            if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                if let Some(deleted_name) = file_name.strip_prefix(".wh.") {
                    let parent = path.parent().unwrap_or(Path::new(""));
                    let deleted_path = parent.join(deleted_name);
                    let deleted_str = deleted_path
                        .to_str()
                        .unwrap_or_default()
                        .trim_start_matches("./");
                    remaining.remove(deleted_str);
                    continue;
                }
            }

            // Check if this is a target file.
            if remaining.contains(&path_str) {
                let mut data = Vec::new();
                entry.read_to_end(&mut data).with_context(|| {
                    format!("reading {} from layer {}", path_str, layer_desc.digest())
                })?;
                remaining.remove(&path_str);
                results.insert(path_str, data);
            }
        }
    }

    Ok(results)
}

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

/// Open a bidirectional streaming connection with containerd and spawn a task
/// that responds to auth callbacks with the provided credentials.
async fn setup_auth_stream(
    channel: &Channel,
    namespace: &str,
    stream_id: &str,
    cred: docker_config::RegistryCredential,
) -> anyhow::Result<()> {
    let mut streaming = StreamingClient::new(channel.clone());

    let (tx, rx) = tokio::sync::mpsc::channel::<prost_types::Any>(4);

    // Send the StreamInit as the first message to register the stream ID.
    let init = StreamInit {
        id: stream_id.to_string(),
    };
    tx.send(to_any(&init)).await.context("sending StreamInit")?;

    let stream = ReceiverStream::new(rx);
    let mut req = Request::new(stream);
    let md = req.metadata_mut();
    md.insert(
        "containerd-namespace",
        namespace
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid containerd namespace header value {:?}: {}", namespace, e))?,
    );
    let resp = streaming.stream(req).await.context("opening auth stream")?;

    let mut inbound = resp.into_inner();

    // Spawn a task to handle auth callbacks.
    let stream_id_owned = stream_id.to_string();
    tokio::spawn(async move {
        use containerd_client::types::transfer::AuthRequest;
        use prost::Message as _;

        loop {
            match inbound.message().await {
                Ok(Some(any)) => {
                    // Check if this is an AuthRequest.
                    if any.type_url == AuthRequest::full_name()
                        || any.type_url == format!("/{}", AuthRequest::full_name())
                        || any.type_url.ends_with("AuthRequest")
                    {
                        // Log the auth request details for debugging.
                        match AuthRequest::decode(any.value.as_slice()) {
                            Ok(req) => {
                                log::debug!(
                                    "auth stream {}: auth request host={}, ref={}, www_auth={:?}",
                                    stream_id_owned, req.host, req.reference, req.wwwauthenticate
                                );
                            }
                            Err(e) => {
                                log::warn!(
                                    "auth stream {}: failed to decode AuthRequest: {}",
                                    stream_id_owned, e
                                );
                            }
                        }

                        log::debug!(
                            "auth stream {}: responding with credentials (username={})",
                            stream_id_owned, cred.username
                        );
                        let response = AuthResponse {
                            auth_type: AuthType::Credentials as i32,
                            username: cred.username.clone(),
                            secret: cred.password.clone(),
                            expire_at: None,
                        };

                        let resp_any = to_any(&response);

                        if tx.send(resp_any).await.is_err() {
                            log::warn!("auth stream {}: sender closed", stream_id_owned);
                            break;
                        }
                    } else {
                        log::debug!(
                            "auth stream {}: ignoring message type {}",
                            stream_id_owned, any.type_url
                        );
                    }
                }
                Ok(None) => {
                    log::debug!("auth stream {}: closed by containerd", stream_id_owned);
                    break;
                }
                Err(e) => {
                    log::warn!("auth stream {}: error: {}", stream_id_owned, e);
                    break;
                }
            }
        }
    });

    Ok(())
}

fn generate_id() -> String {
    uuid::Uuid::new_v4().to_string()
}
