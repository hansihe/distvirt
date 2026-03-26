mod api_client;
mod instance;
mod rootfs;
mod snapshot_patch;
mod vm_config;

pub use instance::CloudHypervisorInstance;

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::Context;

use api_client::ApiClient;
use instance::InstanceArgs;
use vm_config::AdditionalDrive;

use super::{
    LaunchResult, RestoreContext, SnapshotArtifacts, SnapshotMetadata, SnapshotVirtiofsMount,
    SnapshotVolumeDrive, VmConfig, VmVolumeSource, Vmm, VolumeMountInstruction,
    copy_file_writable, spawn_exit_monitor, spawn_serial_task, wait_for_file,
};
use super::virtiofs::spawn_virtiofsd;
use crate::fabric::{FabricPort, Port};
use crate::linux::net::PersistentTap;

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
        let rootfs = rootfs::materialize(
            config.container_image,
            self.containerd.as_ref(),
            &self.virtiofsd_bin,
            tmpdir.path(),
        )
        .await?;

        if !rootfs.use_block_container {
            // Overlay ext4 for virtiofs upper/work dirs.
            log::info!("cloud-hypervisor: creating overlay image");
            let overlay_path = tmpdir.path().join("overlay.ext4");
            crate::volume::create_overlay_image(&overlay_path, 256)
                .await
                .context("create overlay image")?;
        }

        // --- Process volumes ---
        let mut virtiofsd_processes = rootfs.virtiofsd_processes;
        let mut virtiofs_tags = rootfs.virtiofs_tags;
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
                        source: distvirt_guest_protocol::VolumeSource::Device { device },
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
                        vol.read_only,
                    )
                    .await?;
                    virtiofsd_processes.push(proc);
                    virtiofs_tags.push(tag.clone());
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

        // --- Build VM config JSON + TAP ---
        let built = vm_config::build(
            &config.kernel_path,
            config.vcpu_count,
            config.mem_size_mib,
            config.balloon.as_ref(),
            config.serial_console,
            rootfs.use_block_container,
            &additional_drives,
            &virtiofs_tags,
            config.net.as_ref(),
        )?;

        // --- Create and boot VM ---
        let api = ApiClient::new(spawned.api_socket.clone());
        api.request("PUT", "/api/v1/vm.create", Some(&built.config_json))
            .await
            .context("vm.create")?;
        api.request("PUT", "/api/v1/vm.boot", None)
            .await
            .context("vm.boot")?;
        log::info!("cloud-hypervisor: instance started");

        let fabric_port = match (built.tap, config.net.as_ref()) {
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
            kernel_path: config.kernel_path,
            rootfs_source_path: config.rootfs_image_path,
            balloon_configured: config.balloon.is_some(),
            serial_console: config.serial_console,
            volume_drives,
            virtiofs_mounts: virtiofs_snapshot,
            container_image_ref: config.snapshot_context.container_image_ref,
            config_volumes: config.snapshot_context.config_volumes,
        };

        // --- Build launch result ---
        let container_rootfs = if rootfs.use_block_container {
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

        let instance = CloudHypervisorInstance::new(InstanceArgs {
            child: spawned.child,
            virtiofsd_processes,
            overlayfs_cleanup: rootfs.overlayfs_cleanup,
            lease: rootfs.lease,
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

        Ok((instance, launch_result))
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

        // --- Materialize container rootfs ---
        let rootfs = if let Some(container_image) = ctx.container_image {
            rootfs::materialize(
                container_image,
                self.containerd.as_ref(),
                &self.virtiofsd_bin,
                tmpdir.path(),
            )
            .await?
        } else {
            rootfs::MaterializedRootfs::empty()
        };

        let mut virtiofsd_processes = rootfs.virtiofsd_processes;

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
                true, // config volumes are always read-only
            )
            .await?;
            virtiofsd_processes.push(proc);
            config_vol_tmpdirs.push(dir);
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
            overlayfs_cleanup: rootfs.overlayfs_cleanup,
            lease: rootfs.lease,
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
