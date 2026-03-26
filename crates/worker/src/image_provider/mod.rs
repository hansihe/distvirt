pub(crate) mod containerd;
pub mod containerd_blockfile;
pub mod containerd_overlayfs;
pub(crate) mod docker_config;
pub(crate) mod image;
pub mod rootfs_dir;
pub mod stub;

use std::any::Any;
use std::future::Future;
use std::path::PathBuf;

use crate::oci::ImageConfig;

/// A prepared container filesystem artifact.
///
/// The `rootfs_dir` points to a directory containing the merged rootfs.
/// For virtiofs-based launch, virtiofsd serves this directory directly.
pub struct PreparedArtifact {
    /// Directory containing the merged container rootfs (read-only).
    pub rootfs_dir: PathBuf,
    /// OCI image config, if available (None for bare rootfs providers).
    pub oci_config: Option<ImageConfig>,
    /// RAII cleanup handle — dropped when the artifact is dropped.
    _cleanup: Box<dyn Any + Send>,
}

impl PreparedArtifact {
    pub fn new(
        rootfs_dir: PathBuf,
        oci_config: Option<ImageConfig>,
        cleanup: impl Any + Send + 'static,
    ) -> Self {
        Self {
            rootfs_dir,
            oci_config,
            _cleanup: Box::new(cleanup),
        }
    }
}

/// Trait for preparing a container filesystem.
///
/// Implementations interpret `image_ref` according to their backend
/// (e.g. a directory path, an OCI image reference, etc).
pub trait ImageProvider: Send + Sync {
    fn prepare(
        &self,
        image_ref: &str,
    ) -> impl Future<Output = anyhow::Result<PreparedArtifact>> + Send;
}
