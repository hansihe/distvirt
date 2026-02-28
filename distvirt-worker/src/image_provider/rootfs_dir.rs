use std::path::PathBuf;

use anyhow::Context;
use tempfile::NamedTempFile;

use crate::image_provider::image;
use super::{ImageProvider, PreparedArtifact};

/// Provides a container filesystem by building an ext4 image from a rootfs directory.
pub struct RootfsDirProvider;

impl ImageProvider for RootfsDirProvider {
    async fn prepare(&self, image_ref: &str) -> anyhow::Result<PreparedArtifact> {
        let rootfs = PathBuf::from(image_ref);

        let tmp = NamedTempFile::new().context("create temp file for ext4 image")?;
        let image_path = tmp.path().to_path_buf();
        let rootfs_clone = rootfs.clone();
        let image_path_clone = image_path.clone();
        tokio::task::spawn_blocking(move || {
            image::build_ext4_image(&rootfs_clone, &image_path_clone)
        })
        .await
        .context("spawn_blocking ext4 build")?
        .context("build ext4 image from rootfs directory")?;

        log::info!("built container image at {}", image_path.display());

        Ok(PreparedArtifact::new(image_path, None, tmp))
    }
}
