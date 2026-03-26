use std::path::PathBuf;

use anyhow::Context;
use tonic::transport::Channel;

use super::containerd;
use super::containerd::image::ResolvedImage;
use super::containerd::lease::LeaseManager;
use super::containerd::unpack::UnpackCoordinator;
use super::{ImageProvider, PreparedArtifact};
use crate::linux::mount;

const OVERLAYFS_SNAPSHOTTER: &str = "overlayfs";

/// Provides a container filesystem by pulling an OCI image via containerd
/// with the overlayfs snapshotter, creating a read-only View, and mounting
/// the merged overlay on a temp directory.
///
/// The returned `PreparedArtifact` holds the mounted directory and an RAII
/// cleanup handle that unmounts, removes the containerd view, and drops the
/// lease when the artifact is dropped.
pub struct ContainerdOverlayfsProvider {
    channel: Channel,
    docker_config: Option<PathBuf>,
    unpack_coordinator: UnpackCoordinator,
    lease_manager: LeaseManager,
}

impl ContainerdOverlayfsProvider {
    pub async fn new(
        socket: String,
        namespace: String,
        docker_config: Option<PathBuf>,
    ) -> anyhow::Result<Self> {
        let channel = containerd::connect(&socket).await?;
        let lease_manager = LeaseManager::new(channel.clone(), namespace.clone());

        // Clean up leases orphaned by a previous crash before we start
        // creating new ones. This makes any resources held only by those
        // leases eligible for containerd GC.
        lease_manager.cleanup_stale_leases().await?;

        Ok(Self {
            channel,
            docker_config,
            unpack_coordinator: UnpackCoordinator::default(),
            lease_manager,
        })
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
        let final_chain_id = resolved
            .final_chain_id()
            .context("image has no layers")?
            .to_string();

        // Unpack layers with the overlayfs snapshotter.
        containerd::ensure_unpacked(
            &self.channel,
            &lease,
            &resolved,
            OVERLAYFS_SNAPSHOTTER,
            &self.unpack_coordinator,
        )
        .await
        .context("unpacking image with overlayfs snapshotter")?;

        // Set permanent GC protection for committed snapshots.
        containerd::content::set_snapshot_gc_label(
            &self.channel,
            lease.namespace(),
            resolved.config_digest(),
            &final_chain_id,
            OVERLAYFS_SNAPSHOTTER,
        )
        .await
        .context("setting snapshot GC ref label")?;

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

        // Create a read-only view of the final snapshot.
        let (mounts, view_key) = containerd::snapshot::create_overlayfs_view(
            &self.channel,
            &lease,
            OVERLAYFS_SNAPSHOTTER,
            &final_chain_id,
        )
        .await
        .context("creating overlayfs view")?;

        // Mount the view on a temp directory.
        let mountpoint = tempfile::tempdir().context("creating temp mountpoint")?;
        mount_containerd_mounts(&mounts, mountpoint.path())
            .context("mounting overlayfs snapshot view")?;

        let rootfs_dir = mountpoint.path().to_path_buf();
        log::info!(
            "overlayfs view mounted at {} (view={})",
            rootfs_dir.display(),
            view_key,
        );

        // Build cleanup handle that unmounts, removes the view, and drops the lease.
        let cleanup = OverlayfsCleanup {
            mountpoint,
            view_key,
            channel: self.channel.clone(),
            namespace: lease.namespace().to_string(),
            // Lease is moved into the cleanup handle — it stays alive as long
            // as the PreparedArtifact, and gets dropped (deleted) on cleanup.
            _lease: lease,
        };

        Ok(PreparedArtifact::new(rootfs_dir, Some(config), cleanup))
    }
}

/// Mount containerd snapshot mount descriptors onto a target directory.
///
/// Handles both overlay mounts (multi-layer) and bind mounts (single-layer).
fn mount_containerd_mounts(
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
fn parse_mount_flags(options: &[String]) -> libc::c_ulong {
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
fn data_options(options: &[String]) -> String {
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
struct OverlayfsCleanup {
    /// Temp directory used as the mountpoint. Must be unmounted before the
    /// TempDir is dropped (otherwise TempDir::drop fails to remove the dir).
    mountpoint: tempfile::TempDir,
    view_key: String,
    channel: Channel,
    namespace: String,
    /// Lease kept alive for the artifact's lifetime. Dropped after view removal.
    _lease: super::containerd::lease::ContainerdLease,
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
        // so spawn a task. The lease is still alive (dropped after this struct),
        // protecting the snapshot from GC until the removal completes.
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
