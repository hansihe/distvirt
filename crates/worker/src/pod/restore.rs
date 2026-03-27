use anyhow::Context;
use distvirt_worker_protocol::{PodId, PodNetworkConfig, WorkerEvent};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::fabric::{Fabric, FabricPort};
use crate::image_provider::ImageProvider;
use crate::managed_vm::ManagedVm;
use crate::task_handle::TaskHandle;
use crate::vmm::{
    MountRestoreKind, NetConfig, RestoreContext, RestoreMount, SnapshotArtifacts, VmMountSource,
    Vmm,
};

use super::{setup_instance, PodResources};

/// Prepare mounts on the destination host and restore the VM from a snapshot.
///
/// 1. Read `mount_restore_info` from snapshot metadata. If empty (old snapshot),
///    fall back to constructing from deprecated `container_image_ref` + `config_volumes`.
/// 2. For each `MountRestoreInfo`, re-establish the mount source.
/// 3. Build `RestoreContext` and call `vmm.restore()`.
/// 4. Wire up fabric + vsock connection.
pub(crate) async fn pod_restore<V: Vmm + 'static, P: ImageProvider + 'static>(
    vmm: &V,
    image_provider: &P,
    fabric: &Fabric<FabricPort>,
    _event_tx: &mpsc::Sender<WorkerEvent>,
    _namespace_id: &distvirt_worker_protocol::NamespaceId,
    pod_id: &PodId,
    network: PodNetworkConfig,
    snapshot: SnapshotArtifacts,
    cancel: &CancellationToken,
) -> anyhow::Result<(
    ManagedVm<V::Instance>,
    Option<TaskHandle<()>>,
    PodResources,
)> {
    // Determine mount restore info — prefer the new field, fall back to deprecated fields.
    let mount_restore_info = if !snapshot.metadata.mount_restore_info.is_empty() {
        snapshot.metadata.mount_restore_info.clone()
    } else {
        // Legacy fallback: construct from deprecated fields.
        let mut info = Vec::new();
        if let Some(ref image_ref) = snapshot.metadata.container_image_ref {
            info.push(crate::vmm::MountRestoreInfo {
                tag: "container".to_string(),
                kind: MountRestoreKind::ImageRef {
                    image_ref: image_ref.clone(),
                },
            });
        }
        for cv in &snapshot.metadata.config_volumes {
            info.push(crate::vmm::MountRestoreInfo {
                tag: cv.tag.clone(),
                kind: MountRestoreKind::ConfigData {
                    files: cv.files.clone(),
                },
            });
        }
        info
    };

    // Re-establish each mount source on the destination host.
    let mut restore_mounts = Vec::new();
    let mut config_data_dirs = Vec::new();

    for mri in &mount_restore_info {
        match &mri.kind {
            MountRestoreKind::ImageRef { image_ref } => {
                log::info!(
                    "pod '{}': preparing image '{}' for restore",
                    pod_id,
                    image_ref
                );
                let artifact = tokio::select! {
                    result = image_provider.prepare(image_ref) => {
                        result.context("preparing image for restore")?
                    }
                    _ = cancel.cancelled() => {
                        anyhow::bail!("cancelled during image prepare for restore");
                    }
                };
                restore_mounts.push(RestoreMount {
                    tag: mri.tag.clone(),
                    source: artifact.into_mount_source(),
                });
            }
            MountRestoreKind::ConfigData { files } => {
                let dir = crate::volume::create_config_data_dir(files)
                    .await
                    .with_context(|| {
                        format!("recreate config volume '{}' from snapshot", mri.tag)
                    })?;
                let dir_path = dir.path().to_path_buf();
                restore_mounts.push(RestoreMount {
                    tag: mri.tag.clone(),
                    source: VmMountSource::Directory { path: dir_path },
                });
                config_data_dirs.push(dir);
            }
            MountRestoreKind::Persisted => {
                // Data is in the snapshot directory; no host-side action needed.
            }
        }
    }

    let net_config = NetConfig::from(&network);
    let ctx = RestoreContext {
        net: Some(net_config),
        mounts: restore_mounts,
    };

    let instance = tokio::select! {
        result = vmm.restore(&snapshot, ctx) => {
            result.context("restore VM from snapshot")?
        }
        _ = cancel.cancelled() => {
            anyhow::bail!("cancelled during VM restore");
        }
    };
    log::info!("worker: pod '{}' VM restored from snapshot", pod_id);

    let (vm, port_task) = setup_instance(instance, fabric, pod_id, &network, cancel).await?;

    let resources = PodResources {
        _prepared_volumes: Vec::new(),
        _vol_tmpdir: None,
        _config_data_dirs: config_data_dirs,
    };

    Ok((vm, port_task, resources))
}
