use std::env::consts;
use std::path::PathBuf;

use anyhow::{bail, Context};
use containerd_client as client;
use containerd_client::services::v1::{
    content_client::ContentClient, images_client::ImagesClient,
    transfer_client::TransferClient, GetImageRequest, InfoRequest, ReadContentRequest,
    TransferOptions, TransferRequest,
};
use containerd_client::services::v1::snapshots::{
    snapshots_client::SnapshotsClient, RemoveSnapshotRequest, ViewSnapshotRequest,
};
use containerd_client::to_any;
use containerd_client::types::transfer::{ImageStore, OciRegistry, UnpackConfiguration};
use containerd_client::types::Platform;
use containerd_client::with_namespace;
use oci_spec::image::ImageConfiguration;
use tonic::transport::Channel;
use tonic::Request;

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
        // Unmount the rootfs.
        let path_c = std::ffi::CString::new(self.rootfs_path.to_str().unwrap_or("")).unwrap();
        unsafe {
            libc::umount2(path_c.as_ptr(), libc::MNT_DETACH);
        }
        let _ = std::fs::remove_dir(&self.rootfs_path);

        // Remove the snapshot view (fire-and-forget, best-effort cleanup).
        let channel = self.channel.clone();
        let namespace = self.namespace.clone();
        let snapshot_key = self.snapshot_key.clone();
        self.handle.spawn(async move {
            let mut snapshots = SnapshotsClient::new(channel);
            let req = RemoveSnapshotRequest {
                snapshotter: "overlayfs".to_string(),
                key: snapshot_key,
            };
            let _ = snapshots.remove(with_namespace!(req, &namespace)).await;
        });
    }
}

/// Pull an image via containerd, parse its config, and mount a read-only rootfs view.
pub async fn prepare_image(
    socket: &str,
    namespace: &str,
    image_ref: &str,
) -> anyhow::Result<PreparedImage> {
    let channel = client::connect(socket)
        .await
        .with_context(|| format!("connecting to containerd at {}", socket))?;

    // Step 1: Pull image via TransferClient.
    pull_image(&channel, namespace, image_ref).await?;

    // Step 2: Read image config.
    let config = read_image_config(&channel, namespace, image_ref).await?;

    // Step 3: Mount rootfs via snapshot view.
    let (rootfs_path, snapshot_key) =
        mount_rootfs(&channel, namespace, image_ref).await?;

    Ok(PreparedImage {
        config,
        rootfs_path,
        snapshot_key,
        channel,
        namespace: namespace.to_string(),
        handle: tokio::runtime::Handle::current(),
    })
}

async fn pull_image(channel: &Channel, namespace: &str, image_ref: &str) -> anyhow::Result<()> {
    let arch = match consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        _ => consts::ARCH,
    };

    let mut transfer = TransferClient::new(channel.clone());

    let source = OciRegistry {
        reference: image_ref.to_string(),
        resolver: Default::default(),
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
    let image = resp
        .into_inner()
        .image
        .context("image record missing")?;
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
                    p.os() == &oci_spec::image::Os::Linux
                        && p.architecture().to_string() == arch
                })
            })
            .context("no manifest found for linux/{arch}")?;
        let manifest_digest = platform_manifest
            .digest()
            .to_string();
        let inner_manifest_bytes =
            read_content(&mut content, namespace, &manifest_digest).await?;
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
        cmd: oci_config
            .and_then(|c| c.cmd().clone())
            .unwrap_or_default(),
        env: oci_config
            .and_then(|c| c.env().clone())
            .unwrap_or_default(),
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
    while let Some(chunk) = stream
        .message()
        .await
        .context("reading content stream")?
    {
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
                    p.os() == &oci_spec::image::Os::Linux
                        && p.architecture().to_string() == arch
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
    let source_c =
        std::ffi::CString::new(mount.source.as_str()).context("mount source")?;
    let target_c =
        std::ffi::CString::new(mount_dir.to_str().unwrap()).context("mount target")?;
    let options = mount.options.join(",");
    log::debug!(
        "snapshot mount: source={:?}, type={:?}, target={:?}, options={:?}",
        mount.source, mount.r#type, mount_dir, options
    );

    // Containerd returns "bind" type for single-layer images, which needs
    // MS_BIND flag rather than a filesystem type string.
    let is_bind = mount.r#type == "bind";
    let mut flags = libc::MS_RDONLY;
    let fstype_c;
    if is_bind {
        flags |= libc::MS_BIND;
        fstype_c = std::ffi::CString::new("").context("mount type")?;
    } else {
        fstype_c = std::ffi::CString::new(mount.r#type.as_str()).context("mount type")?;
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
    let options_c = std::ffi::CString::new(data_str.as_str()).context("mount options")?;

    let ret = unsafe {
        libc::mount(
            source_c.as_ptr(),
            target_c.as_ptr(),
            fstype_c.as_ptr(),
            flags,
            options_c.as_ptr() as *const libc::c_void,
        )
    };
    if ret != 0 {
        let err = std::io::Error::last_os_error();
        bail!(
            "mount at {:?}: {} (source={:?}, type={:?}, options={:?})",
            mount_dir,
            err,
            mount.source,
            mount.r#type,
            options
        );
    }

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
