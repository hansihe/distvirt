use std::path::Path;

use anyhow::Context;
use tempfile::NamedTempFile;

use crate::image;
use super::{ImageProvider, PreparedArtifact};

/// Provides a container filesystem by building an ext4 image from a rootfs directory.
pub struct RootfsDirProvider;

impl ImageProvider for RootfsDirProvider {
    fn prepare(&self, image_ref: &str) -> anyhow::Result<PreparedArtifact> {
        let rootfs = Path::new(image_ref);

        let tmp = NamedTempFile::new().context("create temp file for ext4 image")?;
        let image_path = tmp.path().to_path_buf();
        image::build_ext4_image(rootfs, &image_path)
            .context("build ext4 image from rootfs directory")?;

        log::info!("built container image at {}", image_path.display());

        Ok(PreparedArtifact::new(image_path, None, tmp))
    }
}
