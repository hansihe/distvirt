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
    fn prepare(&self, image_ref: &str) -> anyhow::Result<PreparedArtifact> {
        let prepared = containerd::prepare_image(&self.socket, &self.namespace, image_ref)
            .context("preparing containerd image")?;

        let tmp = NamedTempFile::new().context("create temp file for ext4 image")?;
        let image_path = tmp.path().to_path_buf();
        image::build_ext4_image(&prepared.rootfs_path, &image_path)
            .context("build ext4 image from containerd rootfs")?;

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
