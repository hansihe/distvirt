use std::path::PathBuf;

use super::{ImageProvider, PreparedArtifact};

/// Stub image provider that always returns `/dev/null`. For use with `TestVmm`
/// which ignores the container rootfs directory entirely.
pub struct StubImageProvider;

impl ImageProvider for StubImageProvider {
    async fn prepare(&self, _image_ref: &str) -> anyhow::Result<PreparedArtifact> {
        Ok(PreparedArtifact::new(PathBuf::from("/dev/null"), None, ()))
    }
}
