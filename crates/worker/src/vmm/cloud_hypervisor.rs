use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use anyhow::{Context, bail};
use tokio::net::UnixStream;
use tokio::sync::watch;

use super::{
    NetConfig, SnapshotArtifacts, SnapshotMetadata, VmConfig, VmInstance, Vmm, api_request,
    copy_file_writable, spawn_exit_monitor, spawn_serial_task, try_vsock_connect, wait_for_file,
};
use crate::fabric::{FabricPort, Port};
use crate::linux::net::PersistentTap;
use crate::task_handle::TaskHandle;

/// Cloud Hypervisor VMM implementation.
pub struct CloudHypervisor {
    pub cloud_hypervisor_bin: PathBuf,
}

impl CloudHypervisor {
    pub fn new(cloud_hypervisor_bin: impl Into<PathBuf>) -> Self {
        CloudHypervisor {
            cloud_hypervisor_bin: cloud_hypervisor_bin.into(),
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

/// Spawn the Cloud Hypervisor process with cwd = `working_dir`, wait for the
/// API socket, and return the child + captured stdout (if serial console is on).
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

    async fn launch(&self, config: &VmConfig) -> anyhow::Result<CloudHypervisorInstance> {
        let tmpdir = tempfile::tempdir().context("create tmpdir")?;

        // Copy rootfs and container images into tmpdir (writable copies).
        log::info!("cloud-hypervisor: copying rootfs image to tmpdir");
        copy_file_writable(
            &config.rootfs_image_path,
            &tmpdir.path().join("rootfs.ext4"),
        )
        .await?;
        log::info!("cloud-hypervisor: copying container image to tmpdir");
        copy_file_writable(
            &config.container_image_path,
            &tmpdir.path().join("container.ext4"),
        )
        .await?;
        log::info!("cloud-hypervisor: images copied, spawning cloud-hypervisor");

        // Build boot args.
        // Cloud Hypervisor uses PCI, so no `pci=off`.
        let mut boot_args = format!("console=ttyS0 reboot=k panic=-1 root=/dev/vda init=/sbin/init distvirt.shutdown=poweroff");
        if let Some(ref balloon) = config.balloon {
            boot_args.push_str(&format!(" distvirt.balloon_mib={}", balloon.amount_mib));
        }

        // Write config drive if there are pre-vsock commands to bake in.
        if !config.initial_commands.is_empty() {
            let config_img_path = tmpdir.path().join("config.img");
            let json_payload = serde_json::to_vec(&config.initial_commands)
                .context("serialize initial_commands")?;
            let mut img_data = Vec::with_capacity(4 + json_payload.len());
            img_data.extend_from_slice(&(json_payload.len() as u32).to_le_bytes());
            img_data.extend_from_slice(&json_payload);
            tokio::fs::write(&config_img_path, &img_data)
                .await
                .context("write config.img")?;
            boot_args.push_str(" distvirt.config_device=/dev/vdc");
        }

        // Copy and prepare additional drives (volumes).
        for drive in &config.additional_drives {
            let filename = drive
                .image_path
                .file_name()
                .context("additional drive has no filename")?
                .to_str()
                .context("additional drive filename is not valid UTF-8")?;
            log::info!(
                "cloud-hypervisor: copying volume drive '{}' to tmpdir",
                drive.drive_id
            );
            copy_file_writable(&drive.image_path, &tmpdir.path().join(filename)).await?;
        }

        // Spawn Cloud Hypervisor and wait for API socket.
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

        // Build the full VmConfig JSON for vm.create.
        let kernel_path_str = config
            .kernel_path
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("kernel_path is not valid UTF-8"))?;

        // Disks: rootfs (vda), container (vdb), optional config (vdc), then volumes.
        let mut disks = vec![
            serde_json::json!({"path": "./rootfs.ext4", "readonly": false}),
            serde_json::json!({"path": "./container.ext4", "readonly": false}),
        ];
        if !config.initial_commands.is_empty() {
            disks.push(serde_json::json!({"path": "./config.img", "readonly": true}));
        }
        for drive in &config.additional_drives {
            let filename = drive.image_path.file_name().unwrap().to_str().unwrap();
            disks.push(
                serde_json::json!({"path": format!("./{}", filename), "readonly": drive.read_only}),
            );
        }

        let mut vm_config = serde_json::json!({
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
            },
            "serial": {
                "mode": if config.serial_console { "Tty" } else { "Off" },
            },
            "console": {
                "mode": "Off",
            },
        });

        // Configure balloon device if requested.
        if let Some(ref balloon) = config.balloon {
            vm_config["balloon"] = serde_json::json!({
                "size": (balloon.amount_mib as u64) * 1024 * 1024,
                "deflate_on_oom": balloon.deflate_on_oom,
            });
        }

        // Configure network interface if requested.
        let tap = if let Some(ref net) = config.net {
            let tap = PersistentTap::create().context("create TAP device")?;
            tap.bring_up().context("bring TAP interface up")?;

            let mac_str = format!(
                "{:02X}:{:02X}:{:02X}:{:02X}:{:02X}:{:02X}",
                net.guest_mac[0],
                net.guest_mac[1],
                net.guest_mac[2],
                net.guest_mac[3],
                net.guest_mac[4],
                net.guest_mac[5]
            );

            // Disable offloads — we do raw L2 injection via AF_PACKET on the
            // TAP device. With offloads enabled the guest expects the hypervisor
            // to handle segmentation, but our packet path doesn't do that.
            vm_config["net"] = serde_json::json!([{
                "tap": tap.name(),
                "mac": mac_str,
                "offload_tso": false,
                "offload_ufo": false,
                "offload_csum": false,
            }]);

            log::info!(
                "configured network: tap={}, guest_ip={}",
                tap.name(),
                net.guest_ip
            );
            Some(tap)
        } else {
            None
        };

        // Create and boot the VM.
        api_request("PUT", &api_socket, "/api/v1/vm.create", Some(&vm_config))
            .await
            .context("vm.create")?;
        api_request("PUT", &api_socket, "/api/v1/vm.boot", None)
            .await
            .context("vm.boot")?;
        log::info!("cloud-hypervisor: instance started");

        // Open AF_PACKET socket on the TAP and wrap as FabricPort.
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

        let volume_drives: Vec<super::SnapshotVolumeDrive> = config
            .additional_drives
            .iter()
            .map(|d| super::SnapshotVolumeDrive {
                filename: d
                    .image_path
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string(),
                read_only: d.read_only,
            })
            .collect();

        Ok(CloudHypervisorInstance {
            child,
            vsock_uds_path,
            api_socket,
            fabric_port,
            _serial_task,
            exit_rx,
            _exit_monitor,
            _tmpdir: tmpdir,
            kernel_path: config.kernel_path.clone(),
            rootfs_source_path: config.rootfs_image_path.clone(),
            balloon_configured: config.balloon.is_some(),
            serial_console: config.serial_console,
            volume_drives,
        })
    }

    async fn restore(
        &self,
        snapshot: &SnapshotArtifacts,
        net: Option<&NetConfig>,
    ) -> anyhow::Result<CloudHypervisorInstance> {
        let metadata = &snapshot.metadata;
        let snapshot_dir = &snapshot.snapshot_dir;

        let tmpdir = tempfile::tempdir().context("create tmpdir for restore")?;

        // Copy rootfs from original source and container from snapshot.
        copy_file_writable(
            &metadata.rootfs_source_path,
            &tmpdir.path().join("rootfs.ext4"),
        )
        .await?;
        copy_file_writable(
            &snapshot_dir.join("container.ext4"),
            &tmpdir.path().join("container.ext4"),
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

        // Copy CH snapshot files (config.json, state.json, memory-ranges).
        for filename in &["config.json", "state.json", "memory-ranges"] {
            tokio::fs::copy(
                snapshot_dir.join(filename),
                tmpdir.path().join(filename),
            )
            .await
            .with_context(|| format!("copy {} from snapshot", filename))?;
        }

        // Create fresh TAP if networking is configured.
        let tap = if net.is_some() {
            let tap = PersistentTap::create().context("create TAP device for restore")?;
            tap.bring_up()
                .context("bring TAP interface up for restore")?;
            log::info!("restore: created TAP {}", tap.name());

            // Patch CH's config.json to use the new TAP device name.
            patch_snapshot_config_tap(&tmpdir.path().join("config.json"), tap.name())
                .await
                .context("patch TAP name in snapshot config.json")?;

            Some(tap)
        } else {
            None
        };

        // Spawn Cloud Hypervisor and wait for API socket.
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

        // Restore VM from snapshot.
        let source_url = format!("file://{}", tmpdir.path().display());
        api_request(
            "PUT",
            &api_socket,
            "/api/v1/vm.restore",
            Some(&serde_json::json!({"source_url": source_url})),
        )
        .await
        .context("vm.restore")?;

        // CH leaves the VM paused after restore — resume it.
        api_request("PUT", &api_socket, "/api/v1/vm.resume", None)
            .await
            .context("vm.resume")?;

        // Open AF_PACKET socket on the new TAP and wrap as FabricPort.
        let fabric_port = if let (Some(tap), Some(net_cfg)) = (tap, net) {
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
        })
    }
}

pub struct CloudHypervisorInstance {
    child: tokio::process::Child,
    vsock_uds_path: PathBuf,
    api_socket: PathBuf,
    fabric_port: Option<FabricPort>,
    _serial_task: Option<TaskHandle<()>>,
    exit_rx: watch::Receiver<Option<ExitStatus>>,
    _exit_monitor: TaskHandle<()>,
    /// Tmpdir holding disk images and sockets. Dropped after child is killed
    /// (field order matters — child is declared first).
    _tmpdir: tempfile::TempDir,
    kernel_path: PathBuf,
    rootfs_source_path: PathBuf,
    balloon_configured: bool,
    serial_console: bool,
    volume_drives: Vec<super::SnapshotVolumeDrive>,
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

        // Retry loop — the guest needs time to boot and start listening.
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
        // Cloud Hypervisor uses vm.resize with desired_balloon in bytes.
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

        // 1. Pause vCPUs.
        api_request("PUT", &self.api_socket, "/api/v1/vm.pause", None)
            .await
            .context("pause VM")?;

        // 2. Create snapshot — CH writes config.json, state.json, memory-ranges
        //    into the destination directory.
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

        // 3. Copy container.ext4 from tmpdir into snapshot dir.
        tokio::fs::copy(
            tmpdir_path.join("container.ext4"),
            snapshot_dir.join("container.ext4"),
        )
        .await
        .context("copy container.ext4 to snapshot dir")?;

        // 3b. Copy volume images from tmpdir into snapshot dir.
        for vd in &self.volume_drives {
            tokio::fs::copy(
                tmpdir_path.join(&vd.filename),
                snapshot_dir.join(&vd.filename),
            )
            .await
            .with_context(|| format!("copy volume '{}' to snapshot dir", vd.filename))?;
        }

        // 4. Write metadata.json.
        let metadata = SnapshotMetadata {
            kernel_path: self.kernel_path.clone(),
            rootfs_source_path: self.rootfs_source_path.clone(),
            balloon_configured: self.balloon_configured,
            serial_console: self.serial_console,
            volume_drives: self.volume_drives.clone(),
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

/// Patch the TAP device name in a Cloud Hypervisor snapshot's config.json.
///
/// CH saves its full VM config as `config.json` in the snapshot directory and
/// re-reads it on restore. By rewriting the `tap` field in the `net` array we
/// can point the restored VM at a freshly created TAP device without needing
/// FD passing (`net_fds` / `SCM_RIGHTS`).
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
