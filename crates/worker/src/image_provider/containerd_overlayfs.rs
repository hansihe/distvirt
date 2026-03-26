use std::path::PathBuf;

use anyhow::Context;
use tonic::transport::Channel;

use super::containerd;
use super::containerd::image::ResolvedImage;
use super::containerd::lease::LeaseManager;
use super::{ImageProvider, PreparedArtifact};
use crate::linux::mount;

pub(crate) const OVERLAYFS_SNAPSHOTTER: &str = "overlayfs";

/// Provides a prepared container image by pulling an OCI image via containerd.
///
/// Returns a `PreparedArtifact::Containerd` with the resolved image metadata
/// and a lease. The VMM handles unpacking, view creation, and mounting.
pub struct ContainerdOverlayfsProvider {
    channel: Channel,
    docker_config: Option<PathBuf>,
    lease_manager: LeaseManager,
}

impl ContainerdOverlayfsProvider {
    pub async fn new(
        socket: impl Into<String>,
        namespace: impl Into<String>,
        docker_config: Option<PathBuf>,
    ) -> anyhow::Result<Self> {
        let socket = socket.into();
        let namespace = namespace.into();
        let channel = containerd::connect(&socket).await?;
        let lease_manager = LeaseManager::new(channel.clone(), namespace);

        // Clean up leases orphaned by a previous crash before we start
        // creating new ones. This makes any resources held only by those
        // leases eligible for containerd GC.
        // Skipped during parallel e2e tests to avoid deleting leases from
        // concurrently running workers.
        if std::env::var("DISTVIRT_SKIP_LEASE_CLEANUP").is_err() {
            lease_manager.cleanup_stale_leases().await?;
        }

        Ok(Self {
            channel,
            docker_config,
            lease_manager,
        })
    }

    /// Get the containerd channel for sharing with other components.
    pub fn channel(&self) -> &Channel {
        &self.channel
    }

    /// Get the containerd namespace.
    pub fn namespace(&self) -> &str {
        self.lease_manager.namespace()
    }
}

impl ImageProvider for ContainerdOverlayfsProvider {
    async fn prepare(&self, image_ref: &str) -> anyhow::Result<PreparedArtifact> {
        // Persistent lease protects resources from GC for the artifact's
        // lifetime. No gc.expire label — cleanup relies on RAII drop (normal
        // operation) or cleanup_stale_leases() at worker startup (crash).
        let lease = self.lease_manager.create_persistent_lease().await?;

        // Pull image if not present locally.
        containerd::ensure_image(
            &self.channel,
            &lease,
            image_ref,
            self.docker_config.as_deref(),
        )
        .await
        .context("ensuring image is pulled")?;

        // Resolve image metadata.
        let resolved = ResolvedImage::resolve(&self.channel, lease.namespace(), image_ref)
            .await
            .context("resolving image metadata")?;

        // Extract OCI config + passwd/group from layer tarballs.
        let mut config = resolved.image_config();
        let files = containerd::extract_files_from_layers(
            &self.channel,
            lease.namespace(),
            &resolved,
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

        Ok(PreparedArtifact::Containerd {
            image_ref: image_ref.to_string(),
            oci_config: Some(config),
            resolved,
            lease,
        })
    }
}

/// Mount containerd snapshot mount descriptors onto a target directory.
///
/// Handles both overlay mounts (multi-layer) and bind mounts (single-layer).
pub(crate) fn mount_containerd_mounts(
    mounts: &[containerd_client::types::Mount],
    target: &std::path::Path,
) -> anyhow::Result<()> {
    for m in mounts {
        let options = m.options.join(",");
        let flags = parse_mount_flags(&m.options);
        let data_opts = data_options(&m.options);

        mount::mount(&m.source, target, &m.r#type, flags, &data_opts)
            .with_context(|| {
                format!(
                    "mount type={} source={} options={} on {}",
                    m.r#type,
                    m.source,
                    options,
                    target.display()
                )
            })?;
    }
    Ok(())
}

/// Parse standard mount flag strings into libc flag bits.
pub(crate) fn parse_mount_flags(options: &[String]) -> libc::c_ulong {
    let mut flags: libc::c_ulong = 0;
    for opt in options {
        match opt.as_str() {
            "ro" => flags |= libc::MS_RDONLY as libc::c_ulong,
            "rbind" => flags |= (libc::MS_BIND | libc::MS_REC) as libc::c_ulong,
            "bind" => flags |= libc::MS_BIND as libc::c_ulong,
            _ => {}
        }
    }
    flags
}

/// Collect non-flag options into a comma-separated data string for mount(2).
pub(crate) fn data_options(options: &[String]) -> String {
    options
        .iter()
        .filter(|o| !matches!(o.as_str(), "ro" | "rbind" | "bind" | "rw"))
        .cloned()
        .collect::<Vec<_>>()
        .join(",")
}

/// RAII cleanup for an overlayfs snapshot mount.
///
/// On drop: unmounts the overlay, removes the containerd view, and drops the
/// lease (which triggers async deletion via LeaseManager).
pub(crate) struct OverlayfsCleanup {
    /// Temp directory used as the mountpoint. Must be unmounted before the
    /// TempDir is dropped (otherwise TempDir::drop fails to remove the dir).
    pub(crate) mountpoint: tempfile::TempDir,
    pub(crate) view_key: String,
    pub(crate) channel: Channel,
    pub(crate) namespace: String,
}

impl Drop for OverlayfsCleanup {
    fn drop(&mut self) {
        // Unmount synchronously — must happen before TempDir is dropped.
        if let Err(e) = mount::umount_detach(self.mountpoint.path()) {
            log::warn!(
                "failed to unmount overlayfs view at {}: {}",
                self.mountpoint.path().display(),
                e
            );
        }

        // Remove the containerd view asynchronously. We can't .await in Drop,
        // so spawn a task.
        let channel = self.channel.clone();
        let namespace = self.namespace.clone();
        let view_key = self.view_key.clone();
        tokio::spawn(async move {
            if let Err(e) = containerd::snapshot::remove_snapshot(
                &channel,
                &namespace,
                OVERLAYFS_SNAPSHOTTER,
                &view_key,
            )
            .await
            {
                log::warn!("failed to remove overlayfs view {}: {}", view_key, e);
            } else {
                log::debug!("removed overlayfs view {}", view_key);
            }
        });
    }
}
