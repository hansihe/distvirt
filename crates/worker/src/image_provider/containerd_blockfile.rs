use std::path::PathBuf;

use anyhow::Context;

use super::containerd;
use super::{ImageProvider, PreparedArtifact};

/// Provides a container filesystem by pulling an OCI image via containerd
/// with the blockfile snapshotter, which produces ext4 block files directly.
pub struct ContainerdBlockfileProvider {
    pub socket: String,
    pub namespace: String,
    pub docker_config: Option<PathBuf>,
}

impl ImageProvider for ContainerdBlockfileProvider {
    async fn prepare(&self, image_ref: &str) -> anyhow::Result<PreparedArtifact> {
        let channel = containerd::connect(&self.socket).await?;

        containerd::ensure_image(
            &channel,
            &self.namespace,
            image_ref,
            self.docker_config.as_deref(),
        )
        .await
        .context("ensuring image is pulled")?;

        containerd::ensure_unpacked(&channel, &self.namespace, image_ref, "blockfile")
            .await
            .context("ensuring image is unpacked with blockfile snapshotter")?;

        let mut config = containerd::read_image_config(&channel, &self.namespace, image_ref)
            .await
            .context("reading image config")?;

        // Extract /etc/passwd and /etc/group from layer tarballs for UID/GID resolution.
        let files = containerd::extract_files_from_layers(
            &channel,
            &self.namespace,
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

        let (blockfile_path, cleanup) =
            containerd::get_blockfile_path(&channel, &self.namespace, image_ref)
                .await
                .context("getting blockfile path")?;

        log::info!("prepared container image at {}", blockfile_path.display());

        Ok(PreparedArtifact::new(blockfile_path, Some(config), cleanup))
    }
}
