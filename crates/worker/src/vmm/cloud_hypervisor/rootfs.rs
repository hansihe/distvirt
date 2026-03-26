use std::path::Path;

use anyhow::Context;

use super::ContainerdConfig;
use crate::image_provider::PreparedArtifact;
use crate::image_provider::containerd_overlayfs::{
    OverlayfsCleanup, OVERLAYFS_SNAPSHOTTER, mount_containerd_mounts,
};
use crate::vmm::copy_file_writable;
use crate::vmm::virtiofs::{VirtiofsdProcess, spawn_virtiofsd};

/// Result of materializing a container rootfs for the VM.
pub(super) struct MaterializedRootfs {
    /// virtiofsd processes to keep alive (ownership transferred to instance).
    pub virtiofsd_processes: Vec<VirtiofsdProcess>,
    /// virtiofs tags that were created (for vm_config fs array). Empty for block mode.
    pub virtiofs_tags: Vec<String>,
    /// Overlayfs cleanup handle (containerd path only).
    pub overlayfs_cleanup: Option<OverlayfsCleanup>,
    /// Containerd lease to keep alive.
    pub lease: Option<crate::image_provider::ContainerdLease>,
    /// True if container is a block device (no virtiofs).
    pub use_block_container: bool,
}

impl MaterializedRootfs {
    /// Empty result for restore without a container image.
    pub fn empty() -> Self {
        MaterializedRootfs {
            virtiofsd_processes: Vec::new(),
            virtiofs_tags: Vec::new(),
            overlayfs_cleanup: None,
            lease: None,
            use_block_container: false,
        }
    }
}

/// Materialize a container rootfs from a `PreparedArtifact`.
///
/// - **Containerd**: unpack layers, create overlayfs view, mount, spawn virtiofsd.
/// - **Directory**: spawn virtiofsd serving the directory directly.
/// - **BlockDevice**: copy block image to `working_dir`.
pub(super) async fn materialize(
    artifact: PreparedArtifact,
    containerd: Option<&ContainerdConfig>,
    virtiofsd_bin: &Path,
    working_dir: &Path,
) -> anyhow::Result<MaterializedRootfs> {
    let mut virtiofsd_processes = Vec::new();
    let mut virtiofs_tags = Vec::new();
    let mut overlayfs_cleanup: Option<OverlayfsCleanup> = None;
    let mut lease: Option<crate::image_provider::ContainerdLease> = None;
    let mut use_block_container = false;

    match artifact {
        PreparedArtifact::Containerd {
            resolved,
            lease: container_lease,
            ..
        } => {
            let ctrd = containerd
                .context("containerd connection required for Containerd artifact")?;

            // Unpack layers + set GC labels.
            crate::image_provider::containerd::ensure_unpacked_with_gc_labels(
                &ctrd.channel,
                &container_lease,
                &resolved,
                OVERLAYFS_SNAPSHOTTER,
                &ctrd.unpack_coordinator,
            )
            .await
            .context("ensure image unpacked with overlayfs snapshotter")?;

            let final_chain_id = resolved
                .final_chain_id()
                .context("image has no layers")?
                .to_string();

            // Create overlayfs view.
            let (mounts, view_key) =
                crate::image_provider::containerd::snapshot::create_overlayfs_view(
                    &ctrd.channel,
                    &container_lease,
                    OVERLAYFS_SNAPSHOTTER,
                    &final_chain_id,
                )
                .await
                .context("creating overlayfs view")?;

            // Mount the view onto a separate TempDir (OverlayfsCleanup
            // needs a TempDir to unmount in Drop).
            let rootfs_tmpdir =
                tempfile::tempdir().context("create rootfs mountpoint tempdir")?;
            mount_containerd_mounts(&mounts, rootfs_tmpdir.path())
                .context("mounting overlayfs snapshot view")?;

            log::info!(
                "overlayfs view mounted at {} (view={})",
                rootfs_tmpdir.path().display(),
                view_key,
            );

            // Spawn virtiofsd for container rootfs (always read-only).
            let proc = spawn_virtiofsd(
                virtiofsd_bin,
                working_dir,
                "container-rootfs",
                rootfs_tmpdir.path(),
                true,
            )
            .await?;
            virtiofsd_processes.push(proc);
            virtiofs_tags.push("container-rootfs".to_string());

            overlayfs_cleanup = Some(OverlayfsCleanup {
                mountpoint: rootfs_tmpdir,
                view_key,
                channel: ctrd.channel.clone(),
                namespace: ctrd.namespace.clone(),
            });
            lease = Some(container_lease);
        }
        PreparedArtifact::Directory { path, .. } => {
            // Serve directory directly via virtiofsd (always read-only).
            let proc = spawn_virtiofsd(
                virtiofsd_bin,
                working_dir,
                "container-rootfs",
                &path,
                true,
            )
            .await?;
            virtiofsd_processes.push(proc);
            virtiofs_tags.push("container-rootfs".to_string());
        }
        PreparedArtifact::BlockDevice { image_path, .. } => {
            // Legacy path: copy block image into working dir as container device.
            log::info!("cloud-hypervisor: copying container block image to tmpdir");
            copy_file_writable(&image_path, &working_dir.join("container.ext4")).await?;
            use_block_container = true;
        }
    }

    Ok(MaterializedRootfs {
        virtiofsd_processes,
        virtiofs_tags,
        overlayfs_cleanup,
        lease,
        use_block_container,
    })
}
