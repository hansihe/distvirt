use std::collections::HashMap;
use std::env::consts;
use std::io::Read;
use std::path::Path;

use anyhow::{Context, bail};
use containerd_client::services::v1::{
    GetImageRequest, StreamInit, TransferOptions, TransferRequest,
    content_client::ContentClient, images_client::ImagesClient,
    streaming_client::StreamingClient, transfer_client::TransferClient,
};
use containerd_client::to_any;
use containerd_client::types::Platform;
use containerd_client::types::transfer::{
    AuthResponse, AuthType, ImageStore, OciRegistry, RegistryResolver,
};
use oci_spec::image::{ImageConfiguration, ImageManifest};
use prost::Name;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tonic::transport::Channel;

use super::content::read_content;
use super::generate_id;
use super::lease::ContainerdLease;
use super::ns_request;
use super::super::docker_config;

pub use crate::oci::ImageConfig;

fn oci_arch() -> &'static str {
    match consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        _ => consts::ARCH,
    }
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
        .get(ns_request(req, namespace))
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
    lease: &ContainerdLease,
    image_ref: &str,
    docker_config: Option<&Path>,
) -> anyhow::Result<()> {
    let namespace = lease.namespace();

    // Check if the image already exists locally.
    let mut images = ImagesClient::new(channel.clone());
    let req = GetImageRequest {
        name: image_ref.to_string(),
    };
    if images.get(ns_request(req, namespace)).await.is_ok() {
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
        .transfer(lease.request(request))
        .await
        .with_context(|| format!("pulling image {}", image_ref))?;
    log::info!("image {} pulled successfully", image_ref);

    Ok(())
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
