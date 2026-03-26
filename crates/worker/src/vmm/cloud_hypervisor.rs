use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use anyhow::{Context, bail};
use tokio::net::UnixStream;
use tokio::sync::watch;

use super::{
    LaunchResult, RestoreContext, SnapshotArtifacts, SnapshotConfigVolume, SnapshotMetadata,
    SnapshotVirtiofsMount, SnapshotVolumeDrive, VmConfig, VmInstance, VmVolumeSource, Vmm,
    VolumeMountInstruction, api_request, copy_file_writable, spawn_exit_monitor,
    spawn_serial_task, try_vsock_connect, wait_for_file,
};
use super::virtiofs::{VirtiofsdProcess, spawn_virtiofsd};
use crate::fabric::{FabricPort, Port};
use crate::image_provider::PreparedArtifact;
use crate::image_provider::containerd_overlayfs::{
    OverlayfsCleanup, OVERLAYFS_SNAPSHOTTER, mount_containerd_mounts,
};
use crate::linux::net::PersistentTap;
use crate::task_handle::TaskHandle;

/// Containerd connection config for the VMM.
pub struct ContainerdConfig {
    pub channel: tonic::transport::Channel,
    pub namespace: String,
    pub unpack_coordinator: crate::image_provider::containerd::unpack::UnpackCoordinator,
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

/// CH-internal: an additional block device to attach.
struct AdditionalDrive {
    filename: String,
    read_only: bool,
}

/// CH-internal: a virtiofs mount configuration.
struct VirtiofsMount {
    tag: String,
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

impl Vmm for CloudHypervisor {
    type Instance = CloudHypervisorInstance;

    async fn launch(
        &self,
        config: VmConfig,
    ) -> anyhow::Result<(CloudHypervisorInstance, LaunchResult)> {
        let tmpdir = tempfile::tempdir().context("create tmpdir")?;

        // Copy rootfs image into tmpdir (writable copy).
        log::info!("cloud-hypervisor: copying rootfs image to tmpdir");
        copy_file_writable(
            &config.rootfs_image_path,
            &tmpdir.path().join("rootfs.ext4"),
        )
        .await?;

        // --- Materialize container rootfs ---
        let mut virtiofsd_processes = Vec::new();
        let mut virtiofs_mounts = Vec::new();
        let mut overlayfs_cleanup: Option<OverlayfsCleanup> = None;
        let mut lease: Option<crate::image_provider::ContainerdLease> = None;
        let mut use_block_container = false;

        match config.container_image {
            PreparedArtifact::Containerd {
                resolved,
                lease: container_lease,
                ..
            } => {
                let ctrd = self
                    .containerd
                    .as_ref()
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

                // Spawn virtiofsd for container rootfs.
                let proc = spawn_virtiofsd(
                    &self.virtiofsd_bin,
                    tmpdir.path(),
                    "container-rootfs",
                    rootfs_tmpdir.path(),
                )
                .await?;
                virtiofsd_processes.push(proc);
                virtiofs_mounts.push(VirtiofsMount {
                    tag: "container-rootfs".to_string(),
                });

                overlayfs_cleanup = Some(OverlayfsCleanup {
                    mountpoint: rootfs_tmpdir,
                    view_key,
                    channel: ctrd.channel.clone(),
                    namespace: ctrd.namespace.clone(),
                });
                lease = Some(container_lease);
            }
            PreparedArtifact::Directory { path, .. } => {
                // Serve directory directly via virtiofsd.
                let proc = spawn_virtiofsd(
                    &self.virtiofsd_bin,
                    tmpdir.path(),
                    "container-rootfs",
                    &path,
                )
                .await?;
                virtiofsd_processes.push(proc);
                virtiofs_mounts.push(VirtiofsMount {
                    tag: "container-rootfs".to_string(),
                });
            }
            PreparedArtifact::BlockDevice { image_path, .. } => {
                // Legacy path: copy block image into tmpdir as container device.
                log::info!("cloud-hypervisor: copying container block image to tmpdir");
                copy_file_writable(&image_path, &tmpdir.path().join("container.ext4")).await?;
                use_block_container = true;
            }
        }

        if !use_block_container {
            // Overlay ext4 for virtiofs upper/work dirs.
            log::info!("cloud-hypervisor: creating overlay image");
            let overlay_path = tmpdir.path().join("overlay.ext4");
            crate::volume::create_overlay_image(&overlay_path, 256)
                .await
                .context("create overlay image")?;
        }

        // --- Process volumes ---
        let mut additional_drives = Vec::new();
        let mut volume_mount_instructions = Vec::new();
        let mut block_idx: u8 = 0;

        for vol in &config.volumes {
            match &vol.source {
                VmVolumeSource::BlockImage { image_path } => {
                    let filename = image_path
                        .file_name()
                        .context("block image has no filename")?
                        .to_str()
                        .context("block image filename is not valid UTF-8")?
                        .to_string();
                    log::info!(
                        "cloud-hypervisor: copying volume '{}' to tmpdir",
                        vol.name
                    );
                    copy_file_writable(image_path, &tmpdir.path().join(&filename)).await?;
                    additional_drives.push(AdditionalDrive {
                        filename: filename.clone(),
                        read_only: vol.read_only,
                    });
                    let device = format!("/dev/vd{}", (b'c' + block_idx) as char);
                    volume_mount_instructions.push(VolumeMountInstruction {
                        name: vol.name.clone(),
                        source: distvirt_guest_protocol::VolumeSource::Device {
                            device,
                        },
                        read_only: vol.read_only,
                    });
                    block_idx += 1;
                }
                VmVolumeSource::Directory { dir_path } => {
                    let tag = format!("vol-{}", vol.name);
                    let proc = spawn_virtiofsd(
                        &self.virtiofsd_bin,
                        tmpdir.path(),
                        &tag,
                        dir_path,
                    )
                    .await?;
                    virtiofsd_processes.push(proc);
                    virtiofs_mounts.push(VirtiofsMount { tag: tag.clone() });
                    volume_mount_instructions.push(VolumeMountInstruction {
                        name: vol.name.clone(),
                        source: distvirt_guest_protocol::VolumeSource::VirtioFs { tag },
                        read_only: vol.read_only,
                    });
                }
            }
        }

        // --- Spawn Cloud Hypervisor ---
        let spawned = spawn_cloud_hypervisor(
            &self.cloud_hypervisor_bin,
            tmpdir.path(),
            config.serial_console,
        )
        .await?;
        log::info!("cloud-hypervisor: process spawned, configuring VM");
        let SpawnedCloudHypervisor {
            child,
            serial_stdout,
            api_socket,
            vsock_uds_path,
        } = spawned;

        // --- Build VM config JSON ---
        let kernel_path_str = config
            .kernel_path
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("kernel_path is not valid UTF-8"))?;

        // Disks: vda = rootfs, vdb = overlay or container block image, vdc+ = volumes.
        let mut disks = vec![
            serde_json::json!({"path": "./rootfs.ext4", "readonly": false}),
        ];
        if use_block_container {
            disks.push(serde_json::json!({"path": "./container.ext4", "readonly": false}));
        } else {
            disks.push(serde_json::json!({"path": "./overlay.ext4", "readonly": false}));
        }
        for drive in &additional_drives {
            disks.push(
                serde_json::json!({"path": format!("./{}", drive.filename), "readonly": drive.read_only}),
            );
        }

        let boot_args = {
            let mut args = "console=ttyS0 reboot=k panic=-1 root=/dev/vda init=/sbin/init distvirt.shutdown=poweroff".to_string();
            if let Some(ref balloon) = config.balloon {
                args.push_str(&format!(" distvirt.balloon_mib={}", balloon.amount_mib));
            }
            args
        };

        let mut vm_config_json = serde_json::json!({
            "payload": {
                "kernel": kernel_path_str,
                "cmdline": boot_args,
            },
            "disks": disks,
            "vsock": {
                "cid": 3,
                "socket": "./vsock.sock",
            },
            "cpus": {
                "boot_vcpus": config.vcpu_count,
                "max_vcpus": config.vcpu_count,
            },
            "memory": {
                "size": (config.mem_size_mib as u64) * 1024 * 1024,
                "shared": !use_block_container,
            },
            "serial": {
                "mode": if config.serial_console { "Tty" } else { "Off" },
            },
            "console": {
                "mode": "Off",
            },
        });

        if let Some(ref balloon) = config.balloon {
            vm_config_json["balloon"] = serde_json::json!({
                "size": (balloon.amount_mib as u64) * 1024 * 1024,
                "deflate_on_oom": balloon.deflate_on_oom,
            });
        }

        if !virtiofs_mounts.is_empty() {
            let fs_array: Vec<serde_json::Value> = virtiofs_mounts
                .iter()
                .map(|vfs| {
                    serde_json::json!({
                        "tag": vfs.tag,
                        "socket": format!("./virtiofs-{}.sock", vfs.tag),
                        "num_queues": 1,
                        "queue_size": 1024,
                    })
                })
                .collect();
            vm_config_json["fs"] = serde_json::json!(fs_array);
        }

        let tap = if let Some(ref net) = config.net {
            let tap = PersistentTap::create().context("create TAP device")?;
            tap.bring_up().context("bring TAP interface up")?;
            let mac_str = format!(
                "{:02X}:{:02X}:{:02X}:{:02X}:{:02X}:{:02X}",
                net.guest_mac[0], net.guest_mac[1], net.guest_mac[2],
                net.guest_mac[3], net.guest_mac[4], net.guest_mac[5]
            );
            vm_config_json["net"] = serde_json::json!([{
                "tap": tap.name(),
                "mac": mac_str,
                "offload_tso": false,
                "offload_ufo": false,
                "offload_csum": false,
            }]);
            log::info!("configured network: tap={}, guest_ip={}", tap.name(), net.guest_ip);
            Some(tap)
        } else {
            None
        };

        // --- Create and boot VM ---
        api_request("PUT", &api_socket, "/api/v1/vm.create", Some(&vm_config_json))
            .await
            .context("vm.create")?;
        api_request("PUT", &api_socket, "/api/v1/vm.boot", None)
            .await
            .context("vm.boot")?;
        log::info!("cloud-hypervisor: instance started");

        let fabric_port = if let Some(tap) = tap {
            let guest_mac = config.net.as_ref().unwrap().guest_mac;
            let socket = tap
                .into_packet_socket()
                .context("open packet socket on TAP")?;
            Some(FabricPort::Tap(Port::new(socket, guest_mac)))
        } else {
            None
        };

        let (exit_rx, _exit_monitor) = spawn_exit_monitor(&child);
        let _serial_task = serial_stdout.map(spawn_serial_task);

        // Build snapshot metadata from instance state.
        let volume_drives: Vec<SnapshotVolumeDrive> = additional_drives
            .iter()
            .map(|d| SnapshotVolumeDrive {
                filename: d.filename.clone(),
                read_only: d.read_only,
            })
            .collect();

        let virtiofs_snapshot: Vec<SnapshotVirtiofsMount> = virtiofs_mounts
            .iter()
            .map(|vfs| SnapshotVirtiofsMount {
                tag: vfs.tag.clone(),
                // Source dir is not meaningful for snapshot (reconstructed on restore).
                source_dir: PathBuf::new(),
            })
            .collect();

        let container_rootfs = if use_block_container {
            distvirt_guest_protocol::ContainerRootfs::Device {
                device: "/dev/vdb".to_string(),
            }
        } else {
            distvirt_guest_protocol::ContainerRootfs::VirtioFsOverlay {
                tag: "container-rootfs".to_string(),
                overlay_device: "/dev/vdb".to_string(),
            }
        };

        let launch_result = LaunchResult {
            container_rootfs,
            volume_mounts: volume_mount_instructions,
        };

        let instance = CloudHypervisorInstance {
            child,
            _virtiofsd_processes: virtiofsd_processes,
            _overlayfs_cleanup: overlayfs_cleanup,
            _lease: lease,
            _config_vol_tmpdirs: Vec::new(),
            vsock_uds_path,
            api_socket,
            fabric_port,
            _serial_task,
            exit_rx,
            _exit_monitor,
            _tmpdir: tmpdir,
            kernel_path: config.kernel_path,
            rootfs_source_path: config.rootfs_image_path,
            balloon_configured: config.balloon.is_some(),
            serial_console: config.serial_console,
            volume_drives,
            virtiofs_mounts: virtiofs_snapshot,
            container_image_ref: config.snapshot_context.container_image_ref,
            config_volumes: config.snapshot_context.config_volumes,
        };

        Ok((instance, launch_result))
    }

    async fn restore(
        &self,
        snapshot: &SnapshotArtifacts,
        ctx: RestoreContext,
    ) -> anyhow::Result<CloudHypervisorInstance> {
        let metadata = &snapshot.metadata;
        let snapshot_dir = &snapshot.snapshot_dir;

        let tmpdir = tempfile::tempdir().context("create tmpdir for restore")?;

        // Copy rootfs and overlay from snapshot.
        copy_file_writable(
            &metadata.rootfs_source_path,
            &tmpdir.path().join("rootfs.ext4"),
        )
        .await?;
        copy_file_writable(
            &snapshot_dir.join("overlay.ext4"),
            &tmpdir.path().join("overlay.ext4"),
        )
        .await?;

        // Copy volume images from snapshot.
        for vd in &metadata.volume_drives {
            copy_file_writable(
                &snapshot_dir.join(&vd.filename),
                &tmpdir.path().join(&vd.filename),
            )
            .await
            .with_context(|| format!("copy volume image '{}' from snapshot", vd.filename))?;
        }

        // Copy CH snapshot files.
        for filename in &["config.json", "state.json", "memory-ranges"] {
            tokio::fs::copy(
                snapshot_dir.join(filename),
                tmpdir.path().join(filename),
            )
            .await
            .with_context(|| format!("copy {} from snapshot", filename))?;
        }

        // --- Materialize container rootfs ---
        let mut virtiofsd_processes = Vec::new();
        let mut overlayfs_cleanup: Option<OverlayfsCleanup> = None;
        let mut lease: Option<crate::image_provider::ContainerdLease> = None;

        if let Some(container_image) = ctx.container_image {
            match container_image {
                PreparedArtifact::Containerd {
                    resolved,
                    lease: container_lease,
                    ..
                } => {
                    let ctrd = self
                        .containerd
                        .as_ref()
                        .context("containerd connection required for restore")?;

                    crate::image_provider::containerd::ensure_unpacked_with_gc_labels(
                        &ctrd.channel,
                        &container_lease,
                        &resolved,
                        OVERLAYFS_SNAPSHOTTER,
                        &ctrd.unpack_coordinator,
                    )
                    .await
                    .context("ensure image unpacked for restore")?;

                    let final_chain_id = resolved
                        .final_chain_id()
                        .context("image has no layers")?
                        .to_string();

                    let (mounts, view_key) =
                        crate::image_provider::containerd::snapshot::create_overlayfs_view(
                            &ctrd.channel,
                            &container_lease,
                            OVERLAYFS_SNAPSHOTTER,
                            &final_chain_id,
                        )
                        .await
                        .context("creating overlayfs view for restore")?;

                    let rootfs_tmpdir =
                        tempfile::tempdir().context("create rootfs mountpoint for restore")?;
                    mount_containerd_mounts(&mounts, rootfs_tmpdir.path())
                        .context("mounting overlayfs view for restore")?;

                    let proc = spawn_virtiofsd(
                        &self.virtiofsd_bin,
                        tmpdir.path(),
                        "container-rootfs",
                        rootfs_tmpdir.path(),
                    )
                    .await?;
                    virtiofsd_processes.push(proc);

                    overlayfs_cleanup = Some(OverlayfsCleanup {
                        mountpoint: rootfs_tmpdir,
                        view_key,
                        channel: ctrd.channel.clone(),
                        namespace: ctrd.namespace.clone(),
                    });
                    lease = Some(container_lease);
                }
                PreparedArtifact::Directory { path, .. } => {
                    let proc = spawn_virtiofsd(
                        &self.virtiofsd_bin,
                        tmpdir.path(),
                        "container-rootfs",
                        &path,
                    )
                    .await?;
                    virtiofsd_processes.push(proc);
                }
                PreparedArtifact::BlockDevice { .. } => {
                    // Block device container image: already in snapshot as
                    // part of the CH state. Nothing to reconstruct.
                }
            }
        }

        // --- Recreate ConfigData volumes ---
        let config_vol_handles =
            crate::volume::prepare_config_volumes_from_snapshot(&ctx.config_volumes)
                .await
                .context("recreate config volumes for restore")?;

        let mut config_vol_tmpdirs = Vec::new();
        for (tag, dir_path, dir) in config_vol_handles {
            let proc = spawn_virtiofsd(
                &self.virtiofsd_bin,
                tmpdir.path(),
                &tag,
                &dir_path,
            )
            .await?;
            virtiofsd_processes.push(proc);
            config_vol_tmpdirs.push(dir);
        }

        // Patch vsock socket path in config.json.
        patch_snapshot_config_vsock(&tmpdir.path().join("config.json"), tmpdir.path())
            .await
            .context("patch vsock socket path in snapshot config.json")?;

        // Patch virtiofs socket paths in config.json.
        if !metadata.virtiofs_mounts.is_empty() {
            patch_snapshot_config_fs(&tmpdir.path().join("config.json"), tmpdir.path())
                .await
                .context("patch virtiofs socket paths in snapshot config.json")?;
        }

        // Create fresh TAP if networking is configured.
        let tap = if ctx.net.is_some() {
            let tap = PersistentTap::create().context("create TAP device for restore")?;
            tap.bring_up()
                .context("bring TAP interface up for restore")?;
            log::info!("restore: created TAP {}", tap.name());
            patch_snapshot_config_tap(&tmpdir.path().join("config.json"), tap.name())
                .await
                .context("patch TAP name in snapshot config.json")?;
            Some(tap)
        } else {
            None
        };

        // Spawn CH and restore.
        let spawned = spawn_cloud_hypervisor(
            &self.cloud_hypervisor_bin,
            tmpdir.path(),
            metadata.serial_console,
        )
        .await?;
        let SpawnedCloudHypervisor {
            child,
            serial_stdout,
            api_socket,
            vsock_uds_path,
        } = spawned;

        let source_url = format!("file://{}", tmpdir.path().display());
        api_request(
            "PUT",
            &api_socket,
            "/api/v1/vm.restore",
            Some(&serde_json::json!({"source_url": source_url})),
        )
        .await
        .context("vm.restore")?;
        api_request("PUT", &api_socket, "/api/v1/vm.resume", None)
            .await
            .context("vm.resume")?;

        let fabric_port = if let (Some(tap), Some(net_cfg)) = (tap, &ctx.net) {
            let socket = tap
                .into_packet_socket()
                .context("open packet socket on TAP (restore)")?;
            Some(FabricPort::Tap(Port::new(socket, net_cfg.guest_mac)))
        } else {
            None
        };

        let (exit_rx, _exit_monitor) = spawn_exit_monitor(&child);
        let _serial_task = serial_stdout.map(spawn_serial_task);

        log::info!("VM restored from snapshot at {}", snapshot_dir.display());

        Ok(CloudHypervisorInstance {
            child,
            _virtiofsd_processes: virtiofsd_processes,
            _overlayfs_cleanup: overlayfs_cleanup,
            _lease: lease,
            _config_vol_tmpdirs: config_vol_tmpdirs,
            vsock_uds_path,
            api_socket,
            fabric_port,
            _serial_task,
            exit_rx,
            _exit_monitor,
            _tmpdir: tmpdir,
            kernel_path: metadata.kernel_path.clone(),
            rootfs_source_path: metadata.rootfs_source_path.clone(),
            balloon_configured: metadata.balloon_configured,
            serial_console: metadata.serial_console,
            volume_drives: metadata.volume_drives.clone(),
            virtiofs_mounts: metadata.virtiofs_mounts.clone(),
            container_image_ref: metadata.container_image_ref.clone(),
            config_volumes: metadata.config_volumes.clone(),
        })
    }
}

pub struct CloudHypervisorInstance {
    child: tokio::process::Child,
    /// virtiofsd processes sharing host directories with the guest.
    /// Dropped after child (field order) — virtiofsd exit is harmless once CH is dead.
    _virtiofsd_processes: Vec<VirtiofsdProcess>,
    /// Overlayfs view cleanup — unmounts + removes containerd view on drop.
    _overlayfs_cleanup: Option<OverlayfsCleanup>,
    /// Containerd lease — keeps blobs alive. Dropped after overlayfs cleanup.
    _lease: Option<crate::image_provider::ContainerdLease>,
    /// Config volume temp directories — kept alive for virtiofsd.
    _config_vol_tmpdirs: Vec<tempfile::TempDir>,
    vsock_uds_path: PathBuf,
    api_socket: PathBuf,
    fabric_port: Option<FabricPort>,
    _serial_task: Option<TaskHandle<()>>,
    exit_rx: watch::Receiver<Option<ExitStatus>>,
    _exit_monitor: TaskHandle<()>,
    _tmpdir: tempfile::TempDir,
    kernel_path: PathBuf,
    rootfs_source_path: PathBuf,
    balloon_configured: bool,
    serial_console: bool,
    volume_drives: Vec<SnapshotVolumeDrive>,
    virtiofs_mounts: Vec<SnapshotVirtiofsMount>,
    container_image_ref: Option<String>,
    config_volumes: Vec<SnapshotConfigVolume>,
}

impl Drop for CloudHypervisorInstance {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

impl VmInstance for CloudHypervisorInstance {
    async fn connect_vsock(&self, port: u32) -> anyhow::Result<UnixStream> {
        let sock_path = self.vsock_uds_path.clone();
        log::info!(
            "connecting to guest vsock port {} via {}",
            port,
            sock_path.display()
        );
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            match try_vsock_connect(&sock_path, port).await {
                Ok(stream) => {
                    log::info!("vsock connected");
                    return Ok(stream);
                }
                Err(_) => {
                    if tokio::time::Instant::now() >= deadline {
                        bail!(
                            "timeout connecting to guest vsock port {} via {}",
                            port,
                            sock_path.display()
                        );
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }

    fn take_fabric_port(&mut self) -> Option<FabricPort> {
        self.fabric_port.take()
    }

    async fn wait(&mut self) -> anyhow::Result<ExitStatus> {
        self.exit_rx
            .wait_for(|s| s.is_some())
            .await
            .map_err(|_| anyhow::anyhow!("exit monitor task dropped"))?;
        let status = self
            .child
            .wait()
            .await
            .context("wait for cloud-hypervisor")?;
        Ok(status)
    }

    async fn kill(&mut self) -> anyhow::Result<()> {
        self.child
            .kill()
            .await
            .context("kill cloud-hypervisor")?;
        Ok(())
    }

    fn take_exit_signal(&mut self) -> Option<watch::Receiver<Option<ExitStatus>>> {
        Some(self.exit_rx.clone())
    }

    async fn set_balloon(&mut self, amount_mib: u32) -> anyhow::Result<()> {
        if !self.balloon_configured {
            bail!("balloon device not configured for this VM");
        }
        api_request(
            "PUT",
            &self.api_socket,
            "/api/v1/vm.resize",
            Some(&serde_json::json!({"desired_balloon": (amount_mib as u64) * 1024 * 1024})),
        )
        .await
        .context("set balloon size")?;
        Ok(())
    }

    async fn snapshot(&mut self, snapshot_dir: &Path) -> anyhow::Result<SnapshotArtifacts> {
        tokio::fs::create_dir_all(snapshot_dir)
            .await
            .with_context(|| format!("create snapshot dir {}", snapshot_dir.display()))?;

        api_request("PUT", &self.api_socket, "/api/v1/vm.pause", None)
            .await
            .context("pause VM")?;

        let destination_url = format!("file://{}", snapshot_dir.display());
        api_request(
            "PUT",
            &self.api_socket,
            "/api/v1/vm.snapshot",
            Some(&serde_json::json!({"destination_url": destination_url})),
        )
        .await
        .context("create snapshot")?;

        let tmpdir_path = self._tmpdir.path();

        tokio::fs::copy(
            tmpdir_path.join("overlay.ext4"),
            snapshot_dir.join("overlay.ext4"),
        )
        .await
        .context("copy overlay.ext4 to snapshot dir")?;

        for vd in &self.volume_drives {
            tokio::fs::copy(
                tmpdir_path.join(&vd.filename),
                snapshot_dir.join(&vd.filename),
            )
            .await
            .with_context(|| format!("copy volume '{}' to snapshot dir", vd.filename))?;
        }

        let metadata = SnapshotMetadata {
            kernel_path: self.kernel_path.clone(),
            rootfs_source_path: self.rootfs_source_path.clone(),
            balloon_configured: self.balloon_configured,
            serial_console: self.serial_console,
            volume_drives: self.volume_drives.clone(),
            virtiofs_mounts: self.virtiofs_mounts.clone(),
            container_image_ref: self.container_image_ref.clone(),
            config_volumes: self.config_volumes.clone(),
        };
        let metadata_json =
            serde_json::to_vec_pretty(&metadata).context("serialize snapshot metadata")?;
        tokio::fs::write(snapshot_dir.join("metadata.json"), &metadata_json)
            .await
            .context("write metadata.json")?;

        log::info!("snapshot created at {}", snapshot_dir.display());

        Ok(SnapshotArtifacts {
            snapshot_dir: snapshot_dir.to_owned(),
            metadata,
        })
    }
}

async fn patch_snapshot_config_vsock(config_path: &Path, tmpdir: &Path) -> anyhow::Result<()> {
    let data = tokio::fs::read_to_string(config_path)
        .await
        .context("read config.json for vsock patching")?;
    let mut config: serde_json::Value =
        serde_json::from_str(&data).context("parse config.json for vsock patching")?;
    if let Some(vsock) = config.get_mut("vsock") {
        if let Some(obj) = vsock.as_object_mut() {
            let new_socket = tmpdir.join("vsock.sock");
            obj.insert(
                "socket".to_string(),
                serde_json::json!(new_socket.to_str().unwrap()),
            );
        }
    }
    let patched =
        serde_json::to_string_pretty(&config).context("serialize patched config (vsock)")?;
    tokio::fs::write(config_path, patched)
        .await
        .context("write patched config.json (vsock)")?;
    Ok(())
}

async fn patch_snapshot_config_fs(config_path: &Path, tmpdir: &Path) -> anyhow::Result<()> {
    let data = tokio::fs::read_to_string(config_path)
        .await
        .context("read config.json for fs patching")?;
    let mut config: serde_json::Value =
        serde_json::from_str(&data).context("parse config.json for fs patching")?;
    if let Some(fs_array) = config.get_mut("fs").and_then(|f| f.as_array_mut()) {
        for fs_entry in fs_array {
            if let Some(obj) = fs_entry.as_object_mut() {
                if let Some(tag) = obj.get("tag").and_then(|t| t.as_str()).map(String::from) {
                    let new_socket = tmpdir.join(format!("virtiofs-{}.sock", tag));
                    obj.insert(
                        "socket".to_string(),
                        serde_json::json!(new_socket.to_str().unwrap()),
                    );
                }
            }
        }
    }
    let patched = serde_json::to_string_pretty(&config).context("serialize patched config (fs)")?;
    tokio::fs::write(config_path, patched)
        .await
        .context("write patched config.json (fs)")?;
    Ok(())
}

async fn patch_snapshot_config_tap(config_path: &Path, new_tap_name: &str) -> anyhow::Result<()> {
    let data = tokio::fs::read_to_string(config_path)
        .await
        .context("read config.json")?;
    let mut config: serde_json::Value =
        serde_json::from_str(&data).context("parse config.json")?;
    if let Some(nets) = config.get_mut("net").and_then(|n| n.as_array_mut()) {
        for net in nets {
            if let Some(obj) = net.as_object_mut() {
                obj.insert("tap".to_string(), serde_json::json!(new_tap_name));
            }
        }
    }
    let patched = serde_json::to_string_pretty(&config).context("serialize patched config")?;
    tokio::fs::write(config_path, patched)
        .await
        .context("write patched config.json")?;
    Ok(())
}
