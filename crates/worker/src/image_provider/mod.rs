pub(crate) mod containerd;
pub mod containerd_blockfile;
pub mod containerd_overlayfs;
pub(crate) mod docker_config;
pub(crate) mod image;
pub mod rootfs_dir;
pub mod stub;

use std::future::Future;
use std::path::PathBuf;

use crate::oci::ImageConfig;

pub use containerd::image::ResolvedImage;
pub use containerd::lease::ContainerdLease;
pub use containerd::unpack::UnpackCoordinator;

/// A prepared container image artifact.
///
/// For `Containerd`: image is pulled, manifest resolved, OCI config extracted.
/// The VMM handles unpacking, view creation, and mounting.
///
/// For `Directory`: a local directory is used directly as the rootfs.
pub enum PreparedArtifact {
    /// Image pulled in containerd. VMM handles unpack + view + mount.
    Containerd {
        image_ref: String,
        oci_config: Option<ImageConfig>,
        resolved: ResolvedImage,
        lease: ContainerdLease,
    },
    /// Local directory served via virtiofs (testing, development).
    Directory {
        path: PathBuf,
        oci_config: Option<ImageConfig>,
        /// Optional RAII cleanup handle.
        _cleanup: Option<Box<dyn std::any::Any + Send>>,
    },
    /// Pre-built ext4 block image. VMM copies into tmpdir and attaches as
    /// a block device (legacy path, no virtiofs).
    BlockDevice {
        image_path: PathBuf,
        oci_config: Option<ImageConfig>,
        /// Optional RAII cleanup handle (e.g. NamedTempFile).
        _cleanup: Option<Box<dyn std::any::Any + Send>>,
    },
}

impl PreparedArtifact {
    pub fn oci_config(&self) -> Option<&ImageConfig> {
        match self {
            PreparedArtifact::Containerd { oci_config, .. }
            | PreparedArtifact::Directory { oci_config, .. }
            | PreparedArtifact::BlockDevice { oci_config, .. } => oci_config.as_ref(),
        }
    }

    /// Get the image reference string (for snapshot metadata).
    pub fn image_ref_str(&self) -> &str {
        match self {
            PreparedArtifact::Containerd { image_ref, .. } => image_ref,
            PreparedArtifact::Directory { path, .. } => path.to_str().unwrap_or(""),
            PreparedArtifact::BlockDevice { image_path, .. } => {
                image_path.to_str().unwrap_or("")
            }
        }
    }
}

/// Trait for preparing a container image.
///
/// Implementations interpret `image_ref` according to their backend
/// (e.g. a directory path, an OCI image reference, etc).
pub trait ImageProvider: Send + Sync {
    fn prepare(
        &self,
        image_ref: &str,
    ) -> impl Future<Output = anyhow::Result<PreparedArtifact>> + Send;
}
