pub mod containerd_overlayfs;
pub mod rootfs_dir;

use std::any::Any;
use std::path::PathBuf;

use crate::containerd::ImageConfig;

/// A prepared container filesystem artifact ready for Firecracker.
pub struct PreparedArtifact {
    /// File or block device path for Firecracker's container disk.
    pub image_path: PathBuf,
    /// OCI image config, if available (None for bare rootfs providers).
    pub oci_config: Option<ImageConfig>,
    /// RAII cleanup handle — dropped when the artifact is dropped.
    _cleanup: Box<dyn Any + Send>,
}

impl PreparedArtifact {
    pub fn new(
        image_path: PathBuf,
        oci_config: Option<ImageConfig>,
        cleanup: impl Any + Send + 'static,
    ) -> Self {
        Self {
            image_path,
            oci_config,
            _cleanup: Box::new(cleanup),
        }
    }
}

/// Trait for preparing a container filesystem for Firecracker.
///
/// Implementations interpret `image_ref` according to their backend
/// (e.g. a directory path, an OCI image reference, etc).
pub trait ImageProvider {
    fn prepare(&self, image_ref: &str) -> anyhow::Result<PreparedArtifact>;
}
