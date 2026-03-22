use std::env::consts;
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use containerd_client as client;
use containerd_client::services::v1::snapshots::{
    RemoveSnapshotRequest, ViewSnapshotRequest, snapshots_client::SnapshotsClient,
};
use containerd_client::services::v1::{
    GetImageRequest, InfoRequest, ReadContentRequest, StreamInit, TransferOptions, TransferRequest,
    content_client::ContentClient, images_client::ImagesClient, streaming_client::StreamingClient,
    transfer_client::TransferClient,
};
use containerd_client::to_any;
use containerd_client::types::Platform;
use containerd_client::types::transfer::{
    AuthResponse, AuthType, ImageStore, OciRegistry, RegistryResolver, UnpackConfiguration,
};
use containerd_client::with_namespace;
use oci_spec::image::ImageConfiguration;
use prost::Name;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tonic::transport::Channel;

use super::docker_config;

pub use crate::oci::ImageConfig;

/// A prepared OCI image with rootfs mounted and config parsed.
/// Cleans up the snapshot view and mount on drop.
pub struct PreparedImage {
    pub config: ImageConfig,
    pub rootfs_path: PathBuf,
    snapshot_key: String,
    channel: Channel,
    namespace: String,
    handle: tokio::runtime::Handle,
}

impl Drop for PreparedImage {
    fn drop(&mut self) {
        // Unmount the rootfs. Avoid unwrap — panicking in Drop can abort.
        if let Err(e) = crate::linux::mount::umount_detach(&self.rootfs_path) {
            log::warn!("PreparedImage drop: umount {:?}: {}", self.rootfs_path, e);
        }
        if let Err(e) = std::fs::remove_dir(&self.rootfs_path) {
            log::warn!("PreparedImage drop: remove_dir {:?}: {}", self.rootfs_path, e);
        }

        // Remove the snapshot view (fire-and-forget, best-effort cleanup).
        let channel = self.channel.clone();
        let namespace = self.namespace.clone();
        let snapshot_key = self.snapshot_key.clone();
        self.handle.spawn(async move {
            let mut snapshots = SnapshotsClient::new(channel);
            let req = RemoveSnapshotRequest {
                snapshotter: "overlayfs".to_string(),
                key: snapshot_key.clone(),
            };
            if let Err(e) = snapshots.remove(with_namespace!(req, &namespace)).await {
                log::warn!("PreparedImage drop: snapshot remove {:?}: {}", snapshot_key, e);
            }
        });
    }
}

/// Pull an image via containerd, parse its config, and mount a read-only rootfs view.
pub async fn prepare_image(
    socket: &str,
    namespace: &str,
    image_ref: &str,
    docker_config: Option<&Path>,
) -> anyhow::Result<PreparedImage> {
    let channel = client::connect(socket)
        .await
        .with_context(|| format!("connecting to containerd at {}", socket))?;

    // Step 1: Pull image via TransferClient.
    pull_image(&channel, namespace, image_ref, docker_config).await?;

    // Step 2: Read image config.
    let config = read_image_config(&channel, namespace, image_ref).await?;

    // Step 3: Mount rootfs via snapshot view.
    let (rootfs_path, snapshot_key) = mount_rootfs(&channel, namespace, image_ref).await?;

    Ok(PreparedImage {
        config,
        rootfs_path,
        snapshot_key,
        channel,
        namespace: namespace.to_string(),
        handle: tokio::runtime::Handle::current(),
    })
}

async fn pull_image(
    channel: &Channel,
    namespace: &str,
    image_ref: &str,
    docker_config: Option<&Path>,
) -> anyhow::Result<()> {
    // Check if the image already exists locally (e.g. imported via `ctr image import`).
    let mut images = ImagesClient::new(channel.clone());
    let req = GetImageRequest {
        name: image_ref.to_string(),
    };
    match images.get(with_namespace!(req, namespace)).await {
        Ok(_) => {
            log::info!("image {} already exists locally, skipping pull", image_ref);
            return Ok(());
        }
        Err(e) => {
            log::debug!("image {} not found locally ({}), will pull", image_ref, e);
        }
    }

    let arch = match consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        _ => consts::ARCH,
    };

    // Look up registry credentials from docker config, if provided.
    let credential =
        docker_config.and_then(|path| docker_config::lookup_credentials(path, image_ref));

    // Set up auth stream if we have credentials.
    let resolver = if let Some(cred) = credential {
        let stream_id = format!("distvirt-auth-{}", uuid_simple());
        setup_auth_stream(channel, namespace, &stream_id, cred).await?;
        Some(RegistryResolver {
            auth_stream: stream_id,
            ..Default::default()
        })
    } else {
        None
    };

    let mut transfer = TransferClient::new(channel.clone());

    let source = OciRegistry {
        reference: image_ref.to_string(),
        resolver,
    };

    let platform = Platform {
        os: "linux".to_string(),
        architecture: arch.to_string(),
        variant: String::new(),
        os_version: String::new(),
    };

    let destination = ImageStore {
        name: image_ref.to_string(),
        platforms: vec![platform.clone()],
        unpacks: vec![UnpackConfiguration {
            platform: Some(platform),
            ..Default::default()
        }],
        ..Default::default()
    };

    let request = TransferRequest {
        source: Some(to_any(&source)),
        destination: Some(to_any(&destination)),
        options: Some(TransferOptions::default()),
    };

    log::info!("pulling image {} for linux/{}", image_ref, arch);
    transfer
        .transfer(with_namespace!(request, namespace))
        .await
        .with_context(|| format!("pulling image {}", image_ref))?;
    log::info!("image {} pulled successfully", image_ref);

    Ok(())
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
    tokio::spawn(async move {
        use containerd_client::types::transfer::AuthRequest;

        loop {
            match inbound.message().await {
                Ok(Some(any)) => {
                    // Check if this is an AuthRequest.
                    if any.type_url == AuthRequest::full_name()
                        || any.type_url == format!("/{}", AuthRequest::full_name())
                        || any.type_url.ends_with("AuthRequest")
                    {
                        log::debug!("received auth callback, responding with credentials");
                        let response = AuthResponse {
                            auth_type: AuthType::Credentials as i32,
                            username: cred.username.clone(),
                            secret: cred.password.clone(),
                            expire_at: None,
                        };
                        if tx.send(to_any(&response)).await.is_err() {
                            log::warn!("auth stream sender closed");
                            break;
                        }
                    } else {
                        log::debug!("auth stream: ignoring message type {}", any.type_url);
                    }
                }
                Ok(None) => {
                    log::debug!("auth stream closed by containerd");
                    break;
                }
                Err(e) => {
                    log::warn!("auth stream error: {}", e);
                    break;
                }
            }
        }
    });

    Ok(())
}

async fn read_image_config(
    channel: &Channel,
    namespace: &str,
    image_ref: &str,
) -> anyhow::Result<ImageConfig> {
    let mut images = ImagesClient::new(channel.clone());
    let mut content = ContentClient::new(channel.clone());

    // Get the image record to find the target descriptor.
    let req = GetImageRequest {
        name: image_ref.to_string(),
    };
    let resp = images
        .get(with_namespace!(req, namespace))
        .await
        .with_context(|| format!("getting image {}", image_ref))?;
    let image = resp.into_inner().image.context("image record missing")?;
    let target = image.target.context("image target descriptor missing")?;

    // Read the manifest/index from content store.
    let manifest_bytes = read_content(&mut content, namespace, &target.digest).await?;

    // Try to parse as image index first (multi-arch), then as manifest.
    let config_digest = if let Ok(index) =
        serde_json::from_slice::<oci_spec::image::ImageIndex>(&manifest_bytes)
    {
        // Multi-arch: find the platform-specific manifest.
        let arch = match consts::ARCH {
            "x86_64" => "amd64",
            "aarch64" => "arm64",
            _ => consts::ARCH,
        };
        let platform_manifest = index
            .manifests()
            .iter()
            .find(|m| {
                m.platform().as_ref().is_some_and(|p| {
                    p.os() == &oci_spec::image::Os::Linux && p.architecture().to_string() == arch
                })
            })
            .context("no manifest found for linux/{arch}")?;
        let manifest_digest = platform_manifest.digest().to_string();
        let inner_manifest_bytes = read_content(&mut content, namespace, &manifest_digest).await?;
        let manifest: oci_spec::image::ImageManifest =
            serde_json::from_slice(&inner_manifest_bytes).context("parsing image manifest")?;
        manifest.config().digest().to_string()
    } else {
        let manifest: oci_spec::image::ImageManifest =
            serde_json::from_slice(&manifest_bytes).context("parsing image manifest")?;
        manifest.config().digest().to_string()
    };

    // Read the image config.
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
    })
}

async fn read_content(
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

async fn mount_rootfs(
    channel: &Channel,
    namespace: &str,
    image_ref: &str,
) -> anyhow::Result<(PathBuf, String)> {
    let mut images = ImagesClient::new(channel.clone());
    let mut content = ContentClient::new(channel.clone());
    let mut snapshots = SnapshotsClient::new(channel.clone());

    // Get image -> target -> manifest -> config digest -> find snapshot key from labels.
    let req = GetImageRequest {
        name: image_ref.to_string(),
    };
    let resp = images
        .get(with_namespace!(req, namespace))
        .await
        .context("getting image for rootfs")?;
    let image = resp.into_inner().image.context("image missing")?;
    let target = image.target.context("target missing")?;

    // Read manifest (handle index).
    let manifest_bytes = read_content(&mut content, namespace, &target.digest).await?;
    let config_digest = if let Ok(index) =
        serde_json::from_slice::<oci_spec::image::ImageIndex>(&manifest_bytes)
    {
        let arch = match consts::ARCH {
            "x86_64" => "amd64",
            "aarch64" => "arm64",
            _ => consts::ARCH,
        };
        let pm = index
            .manifests()
            .iter()
            .find(|m| {
                m.platform().as_ref().is_some_and(|p| {
                    p.os() == &oci_spec::image::Os::Linux && p.architecture().to_string() == arch
                })
            })
            .context("no platform manifest")?;
        let inner = read_content(&mut content, namespace, &pm.digest().to_string()).await?;
        let manifest: oci_spec::image::ImageManifest =
            serde_json::from_slice(&inner).context("parsing manifest")?;
        manifest.config().digest().to_string()
    } else {
        let manifest: oci_spec::image::ImageManifest =
            serde_json::from_slice(&manifest_bytes).context("parsing manifest")?;
        manifest.config().digest().to_string()
    };

    // Find the snapshot key from content info labels.
    let mut content_client = ContentClient::new(channel.clone());
    let req = InfoRequest {
        digest: config_digest.clone(),
    };
    let resp = content_client
        .info(with_namespace!(req, namespace))
        .await
        .context("getting content info for config")?;
    let info = resp.into_inner().info.context("content info missing")?;
    let snapshot_key = info
        .labels
        .get("containerd.io/gc.ref.snapshot.overlayfs")
        .context("snapshot key label not found on config content")?
        .clone();

    // Create a view snapshot from the top layer.
    let view_key = format!("distvirt-view-{}", uuid_simple());
    let req = ViewSnapshotRequest {
        snapshotter: "overlayfs".to_string(),
        key: view_key.clone(),
        parent: snapshot_key,
        labels: Default::default(),
    };
    let resp = snapshots
        .view(with_namespace!(req, namespace))
        .await
        .context("creating snapshot view")?;
    let mounts = resp.into_inner().mounts;

    // Mount the overlayfs at a temp directory.
    let mount_dir: PathBuf = tempfile::tempdir()
        .context("creating temp mount dir")?
        .keep();

    if mounts.is_empty() {
        bail!("no mounts returned for snapshot view");
    }

    let mount = &mounts[0];
    let options = mount.options.join(",");
    log::debug!(
        "snapshot mount: source={:?}, type={:?}, target={:?}, options={:?}",
        mount.source,
        mount.r#type,
        mount_dir,
        options
    );

    // Containerd returns "bind" type for single-layer images, which needs
    // MS_BIND flag rather than a filesystem type string.
    let is_bind = mount.r#type == "bind";
    let mut flags: libc::c_ulong = libc::MS_RDONLY;
    let fstype = if is_bind {
        flags |= libc::MS_BIND;
        ""
    } else {
        mount.r#type.as_str()
    };

    // Parse option flags like "rbind" and "ro" into mount flags.
    let mut data_options = Vec::new();
    for opt in &mount.options {
        match opt.as_str() {
            "rbind" => flags |= libc::MS_BIND | libc::MS_REC,
            "ro" => flags |= libc::MS_RDONLY,
            other => data_options.push(other),
        }
    }
    let data_str = data_options.join(",");

    crate::linux::mount::mount(&mount.source, &mount_dir, fstype, flags, &data_str)?;

    log::info!("mounted rootfs view at {:?}", mount_dir);
    Ok((mount_dir, view_key))
}

/// Generate a simple pseudo-unique ID (no uuid crate dependency).
fn uuid_simple() -> String {
    use std::time::SystemTime;
    let d = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    format!("{:x}{:x}", d.as_secs(), d.subsec_nanos())
}
