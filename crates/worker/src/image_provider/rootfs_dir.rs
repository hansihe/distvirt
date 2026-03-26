use std::path::PathBuf;

use super::{ImageProvider, PreparedArtifact};

/// Provides a container filesystem by pointing directly at a rootfs directory.
///
/// The directory is served via virtiofs — no ext4 image building needed.
pub struct RootfsDirProvider;

impl ImageProvider for RootfsDirProvider {
    async fn prepare(&self, image_ref: &str) -> anyhow::Result<PreparedArtifact> {
        let rootfs_dir = PathBuf::from(image_ref);
        log::info!("rootfs directory: {}", rootfs_dir.display());
        Ok(PreparedArtifact::Directory {
            path: rootfs_dir,
            oci_config: None,
            _cleanup: None,
        })
    }
}
