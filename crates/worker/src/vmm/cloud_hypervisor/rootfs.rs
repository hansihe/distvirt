use std::path::Path;

use anyhow::Context;

use super::ContainerdConfig;
use crate::image_provider::containerd_overlayfs::{
    OverlayfsCleanup, OVERLAYFS_SNAPSHOTTER, mount_containerd_mounts,
};
use crate::image_provider::{ContainerdLease, ResolvedImage};
use crate::vmm::copy_file_writable;
use crate::vmm::virtiofs::{VirtiofsdProcess, spawn_virtiofsd};

/// Result of materializing a containerd image mount.
pub(super) struct MaterializedContainerd {
    pub virtiofsd_process: VirtiofsdProcess,
    pub overlayfs_cleanup: OverlayfsCleanup,
    pub lease: ContainerdLease,
}

/// Materialize a containerd image as a virtiofs mount.
///
/// Unpacks layers, creates an overlayfs view, mounts it, and spawns virtiofsd.
pub(super) async fn materialize_containerd(
    resolved: &ResolvedImage,
    lease: ContainerdLease,
    containerd: &ContainerdConfig,
    virtiofsd_bin: &Path,
    working_dir: &Path,
    tag: &str,
) -> anyhow::Result<MaterializedContainerd> {
    // Unpack layers + set GC labels.
    crate::image_provider::containerd::ensure_unpacked_with_gc_labels(
        &containerd.channel,
        &lease,
        resolved,
        OVERLAYFS_SNAPSHOTTER,
        &containerd.unpack_coordinator,
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
            &containerd.channel,
            &lease,
            OVERLAYFS_SNAPSHOTTER,
            &final_chain_id,
        )
        .await
        .context("creating overlayfs view")?;

    // Mount the view onto a separate TempDir.
    let rootfs_tmpdir = tempfile::tempdir().context("create rootfs mountpoint tempdir")?;
    mount_containerd_mounts(&mounts, rootfs_tmpdir.path())
        .context("mounting overlayfs snapshot view")?;

    log::info!(
        "overlayfs view mounted at {} (view={})",
        rootfs_tmpdir.path().display(),
        view_key,
    );

    // Spawn virtiofsd (always read-only for containerd images).
    let virtiofsd_process = spawn_virtiofsd(
        virtiofsd_bin,
        working_dir,
        tag,
        rootfs_tmpdir.path(),
        true,
    )
    .await?;

    let overlayfs_cleanup = OverlayfsCleanup {
        mountpoint: rootfs_tmpdir,
        view_key,
        channel: containerd.channel.clone(),
        namespace: containerd.namespace.clone(),
    };

    Ok(MaterializedContainerd {
        virtiofsd_process,
        overlayfs_cleanup,
        lease,
    })
}

/// Spawn virtiofsd for a host directory.
pub(super) async fn materialize_directory(
    dir_path: &Path,
    virtiofsd_bin: &Path,
    working_dir: &Path,
    tag: &str,
    read_only: bool,
) -> anyhow::Result<VirtiofsdProcess> {
    spawn_virtiofsd(virtiofsd_bin, working_dir, tag, dir_path, read_only).await
}

/// Copy a block image into the working directory.
pub(super) async fn materialize_block(
    image_path: &Path,
    working_dir: &Path,
    filename: &str,
) -> anyhow::Result<()> {
    log::info!(
        "cloud-hypervisor: copying block image {} to tmpdir",
        image_path.display()
    );
    copy_file_writable(image_path, &working_dir.join(filename)).await
}

