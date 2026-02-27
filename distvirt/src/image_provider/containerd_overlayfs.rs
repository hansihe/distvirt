use anyhow::Context;
use tempfile::NamedTempFile;

use crate::containerd;
use crate::image;
use super::{ImageProvider, PreparedArtifact};

/// Provides a container filesystem by pulling an OCI image via containerd,
/// mounting the overlayfs snapshot, and building an ext4 image from it.
pub struct ContainerdOverlayfsProvider {
    pub socket: String,
    pub namespace: String,
}

impl ImageProvider for ContainerdOverlayfsProvider {
    async fn prepare(&self, image_ref: &str) -> anyhow::Result<PreparedArtifact> {
        let prepared = containerd::prepare_image(&self.socket, &self.namespace, image_ref)
            .await
            .context("preparing containerd image")?;

        let rootfs_path = prepared.rootfs_path.clone();
        let tmp = NamedTempFile::new().context("create temp file for ext4 image")?;
        let image_path = tmp.path().to_path_buf();
        tokio::task::spawn_blocking(move || {
            image::build_ext4_image(&rootfs_path, &image_path)
        })
        .await
        .context("spawn_blocking ext4 build")?
        .context("build ext4 image from containerd rootfs")?;

        let image_path = tmp.path().to_path_buf();
        log::info!("built container image at {}", image_path.display());

        let config = prepared.config.clone();

        // Keep both the PreparedImage (snapshot/mount) and temp file alive
        // until the artifact is dropped.
        Ok(PreparedArtifact::new(
            image_path,
            Some(config),
            (prepared, tmp),
        ))
    }
}
