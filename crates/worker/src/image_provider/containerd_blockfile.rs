use std::path::PathBuf;

use anyhow::Context;
use tonic::transport::Channel;

use super::containerd;
use super::containerd::lease::LeaseManager;
use super::containerd::resource::ImageRef;
use super::containerd::unpack::UnpackCoordinator;
use super::{ImageProvider, PreparedArtifact};

/// Provides a container filesystem by pulling an OCI image via containerd
/// with the blockfile snapshotter, which produces ext4 block files directly.
pub struct ContainerdBlockfileProvider {
    channel: Channel,
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
            docker_config,
            unpack_coordinator: UnpackCoordinator::default(),
            lease_manager,
        })
    }
}

impl ImageProvider for ContainerdBlockfileProvider {
    async fn prepare(&self, image_ref: &str) -> anyhow::Result<PreparedArtifact> {
        // Single operation lease — short-lived, protects resources during
        // pull + unpack + copy. Expires after 1 hour as a crash safety net.
        let lease = self.lease_manager.create_lease().await?;

        // Add the image to our lease for transitive GC protection of all
        // content blobs via the image record's GC ref labels.
        lease.add_resource(&ImageRef(image_ref.to_string())).await?;

        // Pull image if not present locally (auto-leased via header).
        containerd::ensure_image(
            &self.channel,
            &lease,
            image_ref,
            self.docker_config.as_deref(),
        )
        .await
        .context("ensuring image is pulled")?;

        // Unpack layers (Stat for existence, auto-leased Prepare/Commit/Apply).
        containerd::ensure_unpacked(
            &self.channel,
            &lease,
            image_ref,
            "blockfile",
            &self.unpack_coordinator,
        )
        .await
        .context("ensuring image is unpacked with blockfile snapshotter")?;

        // Set permanent GC protection for committed snapshots.
        // This label on the config blob keeps the snapshot chain alive
        // across pod lifecycles (leases are only temporary).
        containerd::content::set_snapshot_gc_label(
            &self.channel,
            lease.namespace(),
            image_ref,
            "blockfile",
        )
        .await
        .context("setting snapshot GC ref label")?;

        let mut config =
            containerd::read_image_config(&self.channel, lease.namespace(), image_ref)
                .await
                .context("reading image config")?;

        // Extract /etc/passwd and /etc/group from layer tarballs for UID/GID resolution.
        let files = containerd::extract_files_from_layers(
            &self.channel,
            lease.namespace(),
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

        // Create a temporary view, copy the blockfile out of containerd
        // storage, then immediately remove the view. The VM reads its own
        // copy — fully decoupled from containerd's snapshot lifecycle.
        let (view_path, view_key) =
            containerd::snapshot::create_blockfile_view(&self.channel, &lease, image_ref)
                .await
                .context("creating blockfile view")?;

        let temp_file =
            tempfile::NamedTempFile::new().context("creating temp file for blockfile copy")?;
        std::fs::copy(&view_path, temp_file.path()).with_context(|| {
            format!(
                "copying blockfile from {} to {}",
                view_path.display(),
                temp_file.path().display()
            )
        })?;
        let image_path = temp_file.path().to_path_buf();

        log::info!(
            "copied blockfile to {} (view={})",
            image_path.display(),
            view_key
        );

        // Remove the view immediately — we have our own copy now.
        containerd::snapshot::remove_snapshot(
            &self.channel,
            lease.namespace(),
            "blockfile",
            &view_key,
        )
        .await
        .context("removing blockfile view after copy")?;

        // Lease is dropped here — committed snapshots are protected by GC
        // ref labels, not the lease. The NamedTempFile is stored as the
        // cleanup handle: deleted when PreparedArtifact drops. On Linux,
        // Firecracker's open fd keeps the data alive even after unlink.
        Ok(PreparedArtifact::new(image_path, Some(config), temp_file))
    }
}
