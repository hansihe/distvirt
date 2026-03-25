use std::path::PathBuf;

use anyhow::Context;
use tonic::transport::Channel;

use super::containerd;
use super::containerd::lease::LeaseManager;
use super::containerd::resource::ContentRef;
use super::containerd::unpack::UnpackCoordinator;
use super::{ImageProvider, PreparedArtifact};

/// Provides a container filesystem by pulling an OCI image via containerd
/// with the blockfile snapshotter, which produces ext4 block files directly.
pub struct ContainerdBlockfileProvider {
    channel: Channel,
    namespace: String,
    docker_config: Option<PathBuf>,
    unpack_coordinator: UnpackCoordinator,
    lease_manager: LeaseManager,
}

impl ContainerdBlockfileProvider {
    pub async fn new(
        socket: String,
        namespace: String,
        docker_config: Option<PathBuf>,
    ) -> anyhow::Result<Self> {
        let channel = containerd::connect(&socket).await?;
        let lease_manager = LeaseManager::new(channel.clone(), namespace.clone());
        Ok(Self {
            channel,
            namespace,
            docker_config,
            unpack_coordinator: UnpackCoordinator::default(),
            lease_manager,
        })
    }
}

impl ImageProvider for ContainerdBlockfileProvider {
    async fn prepare(&self, image_ref: &str) -> anyhow::Result<PreparedArtifact> {
        // Create a lease for this VM's resources.
        let lease = self.lease_manager.create_lease().await?;

        // Pull image if not present locally.
        containerd::ensure_image(
            &self.channel,
            &self.namespace,
            image_ref,
            self.docker_config.as_deref(),
        )
        .await
        .context("ensuring image is pulled")?;

        // Add image content to the lease for GC protection.
        let manifest =
            containerd::resolve_platform_manifest(&self.channel, &self.namespace, image_ref)
                .await?;
        lease
            .add_resource(&ContentRef(manifest.config().digest().to_string()))
            .await?;
        for layer in manifest.layers() {
            lease
                .add_resource(&ContentRef(layer.digest().to_string()))
                .await?;
        }

        // Unpack layers with lease-based lifecycle.
        containerd::ensure_unpacked(
            &self.channel,
            &self.namespace,
            image_ref,
            "blockfile",
            &self.unpack_coordinator,
            &lease,
        )
        .await
        .context("ensuring image is unpacked with blockfile snapshotter")?;

        let mut config = containerd::read_image_config(&self.channel, &self.namespace, image_ref)
            .await
            .context("reading image config")?;

        // Extract /etc/passwd and /etc/group from layer tarballs for UID/GID resolution.
        let files = containerd::extract_files_from_layers(
            &self.channel,
            &self.namespace,
            image_ref,
            &["etc/passwd", "etc/group"],
        )
        .await
        .context("extracting passwd/group from layers")?;

        if let Some(data) = files.get("etc/passwd") {
            if let Ok(content) = std::str::from_utf8(data) {
                config.passwd_entries = crate::oci::parse_passwd(content);
            }
        }
        if let Some(data) = files.get("etc/group") {
            if let Ok(content) = std::str::from_utf8(data) {
                config.group_entries = crate::oci::parse_group(content);
            }
        }

        // Create a snapshot view and add it to the lease.
        let (blockfile_path, view_ref) =
            containerd::snapshot::create_blockfile_view(&self.channel, &self.namespace, image_ref)
                .await
                .context("creating blockfile view")?;
        lease.add_resource(&view_ref).await?;

        log::info!("prepared container image at {}", blockfile_path.display());

        // The lease is the cleanup handle — when dropped, resources become GC-eligible.
        Ok(PreparedArtifact::new(blockfile_path, Some(config), lease))
    }
}
