mod api_client;
mod instance;
mod rootfs;
mod snapshot_patch;
mod vm_config;

pub use instance::CloudHypervisorInstance;

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;

use api_client::ApiClient;
use instance::InstanceArgs;
use vm_config::AdditionalDrive;

use super::{
    BaseVmConfig, GuestDevice, MountRequest, MountRestoreInfo, PlannedMount, ProvidedAccess,
    ResolvedEntry, ResolvedMounts, RestoreContext, SnapshotArtifacts, SnapshotMetadata,
    SnapshotVirtiofsMount, SnapshotVolumeDrive, VmBuilder, VmMountSource, Vmm,
    spawn_exit_monitor, spawn_serial_task, wait_for_file,
};
use crate::fabric::{FabricPort, Port};
use crate::image_provider::{ContainerdLease, ResolvedImage};
use crate::image_provider::containerd_overlayfs::OverlayfsCleanup;
use crate::linux::net::PersistentTap;
use crate::vmm::virtiofs::VirtiofsdProcess;

/// Containerd connection config for the VMM.
///
/// Uses `Arc<UnpackCoordinator>` so the config can be cloned into builders
/// without moving the coordinator out of `CloudHypervisor`.
pub struct ContainerdConfig {
    pub channel: tonic::transport::Channel,
    pub namespace: String,
    pub unpack_coordinator: Arc<crate::image_provider::containerd::unpack::UnpackCoordinator>,
}

/// Cloud Hypervisor VMM implementation.
pub struct CloudHypervisor {
    pub cloud_hypervisor_bin: PathBuf,
    pub virtiofsd_bin: PathBuf,
    pub containerd: Option<ContainerdConfig>,
}

impl CloudHypervisor {
    pub fn new(
        cloud_hypervisor_bin: impl Into<PathBuf>,
        virtiofsd_bin: impl Into<PathBuf>,
        containerd: Option<ContainerdConfig>,
    ) -> Self {
        CloudHypervisor {
            cloud_hypervisor_bin: cloud_hypervisor_bin.into(),
            virtiofsd_bin: virtiofsd_bin.into(),
            containerd,
        }
    }
}

/// Result of spawning the Cloud Hypervisor process (before any API calls).
struct SpawnedCloudHypervisor {
    child: tokio::process::Child,
    serial_stdout: Option<tokio::process::ChildStdout>,
    api_socket: PathBuf,
    vsock_uds_path: PathBuf,
}

async fn spawn_cloud_hypervisor(
    bin: &Path,
    working_dir: &Path,
    serial_console: bool,
) -> anyhow::Result<SpawnedCloudHypervisor> {
    let api_socket = working_dir.join("ch-api.sock");
    let vsock_uds_path = working_dir.join("vsock.sock");

    let mut cmd = tokio::process::Command::new(bin);
    cmd.current_dir(working_dir);
    cmd.arg("--api-socket").arg(&api_socket);
    if serial_console {
        cmd.stdout(Stdio::piped());
    } else {
        cmd.stdout(Stdio::null());
    }
    cmd.stderr(Stdio::null());
    let mut child = cmd.spawn().context("spawn cloud-hypervisor")?;

    let serial_stdout = if serial_console {
        child.stdout.take()
    } else {
        None
    };

    wait_for_file(&api_socket, Duration::from_secs(5))
        .await
        .context("waiting for cloud-hypervisor API socket")?;

    Ok(SpawnedCloudHypervisor {
        child,
        serial_stdout,
        api_socket,
        vsock_uds_path,
    })
}

/// Convert a TAP device into a fabric port.
fn tap_to_fabric_port(tap: PersistentTap, guest_mac: [u8; 6]) -> anyhow::Result<FabricPort> {
    let socket = tap
        .into_packet_socket()
        .context("open packet socket on TAP")?;
    Ok(FabricPort::Tap(Port::new(socket, guest_mac)))
}

// ---------------------------------------------------------------------------
// Deferred mount: recorded in add_mount(), executed in launch()
// ---------------------------------------------------------------------------

/// A mount request recorded by the builder, to be processed during launch().
enum DeferredMount {
    ContainerdImage {
        tag: String,
        resolved: ResolvedImage,
        lease: ContainerdLease,
    },
    Directory {
        tag: String,
        path: PathBuf,
    },
    BlockImage {
        tag: String,
        path: PathBuf,
        read_only: bool,
    },
}

/// A scratch device request recorded by add_scratch_device().
struct DeferredScratch {
    tag: String,
    size_mib: u32,
}

// ---------------------------------------------------------------------------
// CloudHypervisorBuilder
// ---------------------------------------------------------------------------

/// Builder for configuring and launching a Cloud Hypervisor VM.
///
/// Created by `CloudHypervisor::builder()`. Accumulates mount requests
/// synchronously; all async work (virtiofsd spawn, containerd materialize)
/// happens in `launch()`.
pub struct CloudHypervisorBuilder {
    // VM configuration
    base: BaseVmConfig,
    cloud_hypervisor_bin: PathBuf,
    virtiofsd_bin: PathBuf,
    containerd: Option<ContainerdConfig>,
    tmpdir: tempfile::TempDir,

    // Deferred work
    mounts: Vec<DeferredMount>,
    scratches: Vec<DeferredScratch>,

    // Snapshot context
    mount_restore_info: Vec<MountRestoreInfo>,
}

impl CloudHypervisorBuilder {
    /// Plan the access type for a mount source without doing async work.
    fn plan_access(source: &VmMountSource) -> ProvidedAccess {
        match source {
            VmMountSource::ContainerdImage { .. } => ProvidedAccess::VirtioFs { read_only: true },
            VmMountSource::Directory { .. } => ProvidedAccess::VirtioFs { read_only: true },
            VmMountSource::BlockImage { read_only, .. } => {
                ProvidedAccess::BlockDevice { read_only: *read_only }
            }
        }
    }
}

impl VmBuilder for CloudHypervisorBuilder {
    type Instance = CloudHypervisorInstance;

    fn add_mount(&mut self, request: MountRequest) -> anyhow::Result<PlannedMount> {
        let provided = Self::plan_access(&request.source);

        let deferred = match request.source {
            VmMountSource::ContainerdImage { resolved, lease } => {
                // Validate that containerd config is available.
                if self.containerd.is_none() {
                    anyhow::bail!(
                        "containerd connection required for ContainerdImage mount '{}'",
                        request.tag
                    );
                }
                DeferredMount::ContainerdImage {
                    tag: request.tag.clone(),
                    resolved,
                    lease,
                }
            }
            VmMountSource::Directory { path } => DeferredMount::Directory {
                tag: request.tag.clone(),
                path,
            },
            VmMountSource::BlockImage { path, read_only } => DeferredMount::BlockImage {
                tag: request.tag.clone(),
                path,
                read_only,
            },
        };

        self.mounts.push(deferred);

        Ok(PlannedMount {
            tag: request.tag,
            provided,
        })
    }

    fn add_scratch_device(&mut self, tag: &str, size_mib: u32) -> anyhow::Result<()> {
        self.scratches.push(DeferredScratch {
            tag: tag.to_string(),
            size_mib,
        });
        Ok(())
    }

    fn set_snapshot_context(&mut self, mount_restore_info: Vec<MountRestoreInfo>) {
        self.mount_restore_info = mount_restore_info;
    }

    async fn launch(self) -> anyhow::Result<(CloudHypervisorInstance, ResolvedMounts)> {
        let CloudHypervisorBuilder {
            base,
            cloud_hypervisor_bin,
            virtiofsd_bin,
            containerd,
            tmpdir,
            mounts,
            scratches,
            mount_restore_info,
        } = self;

        // Accumulators for VM config and instance ownership.
        let mut virtiofsd_processes: Vec<VirtiofsdProcess> = Vec::new();
        let mut virtiofs_tags: Vec<String> = Vec::new();
        let mut additional_drives: Vec<AdditionalDrive> = Vec::new();
        let mut overlayfs_cleanup: Option<OverlayfsCleanup> = None;
        let mut lease: Option<ContainerdLease> = None;
        let mut resolved_entries: Vec<ResolvedEntry> = Vec::new();

        // Block device index: /dev/vda = rootfs (always), /dev/vdb onwards.
        // We start at 'b' because vda is always the rootfs disk.
        let mut block_idx: u8 = 0;

        // --- Process scratch devices first (they get sequential block device letters) ---
        for scratch in &scratches {
            let filename = format!("scratch-{}.ext4", scratch.tag);
            let overlay_path = tmpdir.path().join(&filename);
            crate::volume::create_overlay_image(&overlay_path, scratch.size_mib as u64)
                .await
                .with_context(|| {
                    format!("create scratch device '{}' ({}MiB)", scratch.tag, scratch.size_mib)
                })?;
            additional_drives.push(AdditionalDrive {
                filename: filename.clone(),
                read_only: false,
            });
            let device_letter = (b'b' + block_idx) as char;
            resolved_entries.push(ResolvedEntry {
                tag: scratch.tag.clone(),
                guest: GuestDevice::Device {
                    path: format!("/dev/vd{}", device_letter),
                },
            });
            block_idx += 1;
        }

        // --- Process deferred mounts ---
        for deferred in mounts {
            match deferred {
                DeferredMount::ContainerdImage {
                    tag,
                    resolved,
                    lease: mount_lease,
                } => {
                    let ctrd = containerd.as_ref().context(
                        "containerd connection required for ContainerdImage mount",
                    )?;
                    let result = rootfs::materialize_containerd(
                        &resolved,
                        mount_lease,
                        ctrd,
                        &virtiofsd_bin,
                        tmpdir.path(),
                        &tag,
                    )
                    .await?;
                    virtiofsd_processes.push(result.virtiofsd_process);
                    virtiofs_tags.push(tag.clone());
                    overlayfs_cleanup = Some(result.overlayfs_cleanup);
                    lease = Some(result.lease);
                    resolved_entries.push(ResolvedEntry {
                        tag,
                        guest: GuestDevice::VirtioFs {
                            virtiofs_tag: virtiofs_tags.last().unwrap().clone(),
                        },
                    });
                }
                DeferredMount::Directory { tag, path } => {
                    let proc = rootfs::materialize_directory(
                        &path,
                        &virtiofsd_bin,
                        tmpdir.path(),
                        &tag,
                        true,
                    )
                    .await?;
                    virtiofsd_processes.push(proc);
                    virtiofs_tags.push(tag.clone());
                    resolved_entries.push(ResolvedEntry {
                        tag,
                        guest: GuestDevice::VirtioFs {
                            virtiofs_tag: virtiofs_tags.last().unwrap().clone(),
                        },
                    });
                }
                DeferredMount::BlockImage {
                    tag,
                    path,
                    read_only,
                } => {
                    let filename = path
                        .file_name()
                        .context("block image has no filename")?
                        .to_str()
                        .context("block image filename is not valid UTF-8")?
                        .to_string();
                    rootfs::materialize_block(&path, tmpdir.path(), &filename).await?;
                    additional_drives.push(AdditionalDrive {
                        filename: filename.clone(),
                        read_only,
                    });
                    let device_letter = (b'b' + block_idx) as char;
                    resolved_entries.push(ResolvedEntry {
                        tag,
                        guest: GuestDevice::Device {
                            path: format!("/dev/vd{}", device_letter),
                        },
                    });
                    block_idx += 1;
                }
            }
        }

        // --- Determine if we need shared memory (virtiofs requires it) ---
        let has_virtiofs = !virtiofs_tags.is_empty();

        // --- Spawn Cloud Hypervisor ---
        let spawned = spawn_cloud_hypervisor(
            &cloud_hypervisor_bin,
            tmpdir.path(),
            base.serial_console,
        )
        .await?;
        log::info!("cloud-hypervisor: process spawned, configuring VM");

        // --- Build VM config JSON + TAP ---
        let built = vm_config::build(
            &base.kernel_path,
            base.vcpu_count,
            base.mem_size_mib,
            base.balloon.as_ref(),
            base.serial_console,
            has_virtiofs, // virtiofs requires shared memory
            &additional_drives,
            &virtiofs_tags,
            base.net.as_ref(),
        )?;

        // Log disk configuration before creating VM.
        if let Some(disks) = built.config_json.get("disks") {
            log::info!("cloud-hypervisor: disk config: {}", disks);
        }

        // --- Create and boot VM ---
        let api = ApiClient::new(spawned.api_socket.clone());
        api.request("PUT", "/api/v1/vm.create", Some(&built.config_json))
            .await
            .context("vm.create")?;
        api.request("PUT", "/api/v1/vm.boot", None)
            .await
            .context("vm.boot")?;
        log::info!("cloud-hypervisor: instance started");

        let fabric_port = match (built.tap, base.net.as_ref()) {
            (Some(tap), Some(net)) => Some(tap_to_fabric_port(tap, net.guest_mac)?),
            _ => None,
        };

        let (exit_rx, exit_monitor) = spawn_exit_monitor(&spawned.child);
        let serial_task = spawned.serial_stdout.map(spawn_serial_task);

        // --- Build snapshot metadata ---
        let volume_drives: Vec<SnapshotVolumeDrive> = additional_drives
            .iter()
            .map(|d| SnapshotVolumeDrive {
                filename: d.filename.clone(),
                read_only: d.read_only,
            })
            .collect();

        let virtiofs_snapshot: Vec<SnapshotVirtiofsMount> = virtiofs_tags
            .iter()
            .map(|tag| SnapshotVirtiofsMount {
                tag: tag.clone(),
                // Source dir is not meaningful for snapshot (reconstructed on restore).
                source_dir: PathBuf::new(),
            })
            .collect();

        let snapshot_metadata = SnapshotMetadata {
            kernel_path: base.kernel_path,
            rootfs_source_path: base.rootfs_image_path,
            balloon_configured: base.balloon.is_some(),
            serial_console: base.serial_console,
            volume_drives,
            virtiofs_mounts: virtiofs_snapshot,
            mount_restore_info,
            // Deprecated fields — no longer populated by builder path.
            container_image_ref: None,
            config_volumes: vec![],
        };

        let resolved_mounts = ResolvedMounts {
            entries: resolved_entries,
        };

        let instance = CloudHypervisorInstance::new(InstanceArgs {
            child: spawned.child,
            virtiofsd_processes,
            overlayfs_cleanup,
            lease,
            config_vol_tmpdirs: Vec::new(),
            serial_task,
            exit_monitor,
            tmpdir,
            vsock_uds_path: spawned.vsock_uds_path,
            api,
            fabric_port,
            exit_rx,
            snapshot_metadata,
        });

        Ok((instance, resolved_mounts))
    }
}

// ---------------------------------------------------------------------------
// Vmm implementation
// ---------------------------------------------------------------------------

impl Vmm for CloudHypervisor {
    type Builder = CloudHypervisorBuilder;
    type Instance = CloudHypervisorInstance;

    fn builder(&self, base: BaseVmConfig) -> anyhow::Result<CloudHypervisorBuilder> {
        let tmpdir = tempfile::tempdir().context("create tmpdir")?;

        // Copy rootfs image into tmpdir (writable copy).
        // This is synchronous because we use std::fs — the rootfs copy is
        // fast for the small guest-init image and avoids making builder() async.
        log::info!("cloud-hypervisor: copying rootfs image to tmpdir");
        std::fs::copy(&base.rootfs_image_path, tmpdir.path().join("rootfs.ext4"))
            .with_context(|| {
                format!(
                    "copy rootfs {} to {}",
                    base.rootfs_image_path.display(),
                    tmpdir.path().display()
                )
            })?;
        // Make writable.
        let mut perms = std::fs::metadata(tmpdir.path().join("rootfs.ext4"))?.permissions();
        perms.set_readonly(false);
        std::fs::set_permissions(tmpdir.path().join("rootfs.ext4"), perms)?;

        // Clone the containerd config for the builder. Channel and Arc are
        // cheap to clone; the builder validates at add_mount() time if
        // containerd is actually needed.
        let containerd = self.containerd.as_ref().map(|c| ContainerdConfig {
            channel: c.channel.clone(),
            namespace: c.namespace.clone(),
            unpack_coordinator: Arc::clone(&c.unpack_coordinator),
        });

        Ok(CloudHypervisorBuilder {
            base,
            cloud_hypervisor_bin: self.cloud_hypervisor_bin.clone(),
            virtiofsd_bin: self.virtiofsd_bin.clone(),
            containerd,
            tmpdir,
            mounts: Vec::new(),
            scratches: Vec::new(),
            mount_restore_info: Vec::new(),
        })
    }

    async fn restore(
        &self,
        snapshot: &SnapshotArtifacts,
        ctx: RestoreContext,
    ) -> anyhow::Result<CloudHypervisorInstance> {
        let metadata = &snapshot.metadata;
        let tmpdir = tempfile::tempdir().context("create tmpdir for restore")?;

        // --- Copy snapshot artifacts ---
        snapshot_patch::copy_snapshot_to_tmpdir(snapshot, tmpdir.path()).await?;

        // --- Process restore mounts ---
        let mut virtiofsd_processes: Vec<VirtiofsdProcess> = Vec::new();
        let mut overlayfs_cleanup: Option<OverlayfsCleanup> = None;
        let mut lease: Option<ContainerdLease> = None;
        let config_vol_tmpdirs: Vec<tempfile::TempDir> = Vec::new();

        for mount in ctx.mounts {
            match mount.source {
                VmMountSource::ContainerdImage {
                    resolved,
                    lease: mount_lease,
                } => {
                    let ctrd = self
                        .containerd
                        .as_ref()
                        .context("containerd connection required for ContainerdImage restore")?;
                    let result = rootfs::materialize_containerd(
                        &resolved,
                        mount_lease,
                        ctrd,
                        &self.virtiofsd_bin,
                        tmpdir.path(),
                        &mount.tag,
                    )
                    .await?;
                    virtiofsd_processes.push(result.virtiofsd_process);
                    overlayfs_cleanup = Some(result.overlayfs_cleanup);
                    lease = Some(result.lease);
                }
                VmMountSource::Directory { path } => {
                    let proc = rootfs::materialize_directory(
                        &path,
                        &self.virtiofsd_bin,
                        tmpdir.path(),
                        &mount.tag,
                        true,
                    )
                    .await?;
                    virtiofsd_processes.push(proc);
                }
                VmMountSource::BlockImage { .. } => {
                    // Block images are persisted in the snapshot; no host-side
                    // action needed on restore (already copied by
                    // copy_snapshot_to_tmpdir).
                }
            }
        }

        // --- Create TAP if networking is configured ---
        let tap = if ctx.net.is_some() {
            let tap = PersistentTap::create().context("create TAP device for restore")?;
            tap.bring_up()
                .context("bring TAP interface up for restore")?;
            log::info!("restore: created TAP {}", tap.name());
            Some(tap)
        } else {
            None
        };

        // --- Patch snapshot config (single pass) ---
        snapshot_patch::patch_snapshot_config(
            tmpdir.path(),
            tap.as_ref().map(|t| t.name()),
        )
        .await?;

        // --- Spawn CH and restore ---
        let spawned = spawn_cloud_hypervisor(
            &self.cloud_hypervisor_bin,
            tmpdir.path(),
            metadata.serial_console,
        )
        .await?;

        let api = ApiClient::new(spawned.api_socket.clone());
        let source_url = format!("file://{}", tmpdir.path().display());
        api.request(
            "PUT",
            "/api/v1/vm.restore",
            Some(&serde_json::json!({"source_url": source_url})),
        )
        .await
        .context("vm.restore")?;
        api.request("PUT", "/api/v1/vm.resume", None)
            .await
            .context("vm.resume")?;

        let fabric_port = match (tap, &ctx.net) {
            (Some(tap), Some(net_cfg)) => Some(tap_to_fabric_port(tap, net_cfg.guest_mac)?),
            _ => None,
        };

        let (exit_rx, exit_monitor) = spawn_exit_monitor(&spawned.child);
        let serial_task = spawned.serial_stdout.map(spawn_serial_task);

        log::info!(
            "VM restored from snapshot at {}",
            snapshot.snapshot_dir.display()
        );

        let instance = CloudHypervisorInstance::new(InstanceArgs {
            child: spawned.child,
            virtiofsd_processes,
            overlayfs_cleanup,
            lease,
            config_vol_tmpdirs,
            serial_task,
            exit_monitor,
            tmpdir,
            vsock_uds_path: spawned.vsock_uds_path,
            api,
            fabric_port,
            exit_rx,
            snapshot_metadata: metadata.clone(),
        });

        Ok(instance)
    }
}
