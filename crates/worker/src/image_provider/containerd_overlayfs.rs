use std::path::PathBuf;

use anyhow::Context;
use tempfile::NamedTempFile;

use super::containerd;
use super::image;
use super::{ImageProvider, PreparedArtifact};

/// Provides a container filesystem by pulling an OCI image via containerd,
/// mounting the overlayfs snapshot, and building an ext4 image from it.
pub struct ContainerdOverlayfsProvider {
    pub socket: String,
    pub namespace: String,
    pub docker_config: Option<PathBuf>,
}

impl ImageProvider for ContainerdOverlayfsProvider {
    async fn prepare(&self, image_ref: &str) -> anyhow::Result<PreparedArtifact> {
        let prepared = containerd::prepare_image(
            &self.socket,
            &self.namespace,
            image_ref,
            self.docker_config.as_deref(),
        )
        .await
        .context("preparing containerd image")?;

        // Read /etc/passwd and /etc/group from the mounted rootfs before
        // building the ext4 image. These are used later to resolve named users
        // (e.g. "postgres") to numeric uid/gid.
        let mut config = prepared.config.clone();
        let passwd_path = prepared.rootfs_path.join("etc/passwd");
        let group_path = prepared.rootfs_path.join("etc/group");
        if let Ok(content) = std::fs::read_to_string(&passwd_path) {
            config.passwd_entries = crate::oci::parse_passwd(&content);
        }
        if let Ok(content) = std::fs::read_to_string(&group_path) {
            config.group_entries = crate::oci::parse_group(&content);
        }

        let rootfs_path = prepared.rootfs_path.clone();
        let tmp = NamedTempFile::new().context("create temp file for ext4 image")?;
        let image_path = tmp.path().to_path_buf();
        tokio::task::spawn_blocking(move || image::build_ext4_image(&rootfs_path, &image_path))
            .await
            .context("spawn_blocking ext4 build")?
            .context("build ext4 image from containerd rootfs")?;

        let image_path = tmp.path().to_path_buf();
        log::info!("built container image at {}", image_path.display());

        // Keep both the PreparedImage (snapshot/mount) and temp file alive
        // until the artifact is dropped.
        Ok(PreparedArtifact::new(
            image_path,
            Some(config),
            (prepared, tmp),
        ))
    }
}
