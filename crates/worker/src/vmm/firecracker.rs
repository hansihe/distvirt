use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use anyhow::{Context, bail};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::watch;

use super::{
    NetConfig, SnapshotArtifacts, SnapshotMetadata, VmConfig, VmInstance, Vmm, copy_file_writable,
    spawn_exit_monitor, spawn_serial_task, wait_for_file,
};
use crate::fabric::{FabricPort, Port};
use crate::linux::net::PersistentTap;
use crate::task_handle::TaskHandle;

/// Firecracker VMM implementation.
pub struct Firecracker {
    pub firecracker_bin: PathBuf,
}

impl Firecracker {
    pub fn new(firecracker_bin: impl Into<PathBuf>) -> Self {
        Firecracker {
            firecracker_bin: firecracker_bin.into(),
        }
    }
}

/// Result of spawning the Firecracker process (before any API calls).
struct SpawnedFirecracker {
    child: tokio::process::Child,
    serial_stdout: Option<tokio::process::ChildStdout>,
    api_socket: PathBuf,
    vsock_uds_path: PathBuf,
}

/// Spawn the Firecracker process with cwd = `working_dir`, wait for the API
/// socket, and return the child + captured stdout (if serial console is on).
async fn spawn_firecracker(
    bin: &Path,
    working_dir: &Path,
    serial_console: bool,
) -> anyhow::Result<SpawnedFirecracker> {
    let api_socket = working_dir.join("firecracker.sock");
    let vsock_uds_path = working_dir.join("vsock.sock");

    let mut cmd = tokio::process::Command::new(bin);
    cmd.current_dir(working_dir);
    cmd.arg("--api-sock").arg("./firecracker.sock");
    if serial_console {
        cmd.stdout(Stdio::piped());
    } else {
        cmd.stdout(Stdio::null());
    }
    cmd.stderr(Stdio::null());
    let mut child = cmd.spawn().context("spawn firecracker")?;

    let serial_stdout = if serial_console {
        child.stdout.take()
    } else {
        None
    };

    wait_for_file(&api_socket, Duration::from_secs(5))
        .await
        .context("waiting for firecracker API socket")?;

    Ok(SpawnedFirecracker {
        child,
        serial_stdout,
        api_socket,
        vsock_uds_path,
    })
}


impl Vmm for Firecracker {
    type Instance = FirecrackerInstance;

    async fn launch(&self, config: &VmConfig) -> anyhow::Result<FirecrackerInstance> {
        let tmpdir = tempfile::tempdir().context("create tmpdir")?;

        // Copy rootfs and container images into tmpdir (writable copies).
        log::info!("firecracker: copying rootfs image to tmpdir");
        copy_file_writable(
            &config.rootfs_image_path,
            &tmpdir.path().join("rootfs.ext4"),
        )
        .await?;
        log::info!("firecracker: copying container image to tmpdir");
        copy_file_writable(
            &config.container_image_path,
            &tmpdir.path().join("container.ext4"),
        )
        .await?;
        log::info!("firecracker: images copied, spawning firecracker");

        // Spawn Firecracker and wait for API socket.
        let spawned =
            spawn_firecracker(&self.firecracker_bin, tmpdir.path(), config.serial_console).await?;
        log::info!("firecracker: process spawned, configuring VM");
        let SpawnedFirecracker {
            child,
            serial_stdout,
            api_socket,
            vsock_uds_path,
        } = spawned;

        // Configure the VM via the API.
        //
        // Boot args for the microVM kernel:
        //   console=ttyS0  — serial console for boot logs (captured via stdout)
        //   reboot=k       — use keyboard controller reset (prevents triple-fault reboot loop)
        //   panic=-1       — reboot immediately on kernel panic (no delay)
        //   pci=off        — disable PCI bus scanning (Firecracker has no PCI, saves boot time)
        //   init=/sbin/init — our custom init binary (not systemd)
        let mut boot_args = "console=ttyS0 reboot=k panic=-1 pci=off init=/sbin/init".to_string();
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

        api_request("PUT",
            &api_socket,
            "/boot-source",
            &serde_json::json!({
                "kernel_image_path": config.kernel_path.to_str()
                    .ok_or_else(|| anyhow::anyhow!("kernel_path is not valid UTF-8: {:?}", config.kernel_path))?,
                "boot_args": boot_args
            }),
        )
        .await
        .context("configure boot-source")?;

        api_request(
            "PUT",
            &api_socket,
            "/drives/rootfs",
            &serde_json::json!({
                "drive_id": "rootfs",
                "path_on_host": "./rootfs.ext4",
                "is_root_device": true,
                "is_read_only": false
            }),
        )
        .await
        .context("configure rootfs drive")?;

        api_request(
            "PUT",
            &api_socket,
            "/drives/container",
            &serde_json::json!({
                "drive_id": "container",
                "path_on_host": "./container.ext4",
                "is_root_device": false,
                "is_read_only": false
            }),
        )
        .await
        .context("configure container drive")?;

        // Register config drive if present.
        if !config.initial_commands.is_empty() {
            api_request(
                "PUT",
                &api_socket,
                "/drives/config",
                &serde_json::json!({
                    "drive_id": "config",
                    "path_on_host": "./config.img",
                    "is_root_device": false,
                    "is_read_only": true
                }),
            )
            .await
            .context("configure config drive")?;
        }

        // Copy and register additional drives (volumes).
        for drive in &config.additional_drives {
            let filename = drive
                .image_path
                .file_name()
                .context("additional drive has no filename")?
                .to_str()
                .context("additional drive filename is not valid UTF-8")?;
            log::info!("firecracker: copying volume drive '{}' to tmpdir", drive.drive_id);
            copy_file_writable(&drive.image_path, &tmpdir.path().join(filename)).await?;
            api_request(
                "PUT",
                &api_socket,
                &format!("/drives/{}", drive.drive_id),
                &serde_json::json!({
                    "drive_id": &drive.drive_id,
                    "path_on_host": format!("./{}", filename),
                    "is_root_device": false,
                    "is_read_only": drive.read_only,
                }),
            )
            .await
            .with_context(|| format!("configure additional drive '{}'", drive.drive_id))?;
        }

        api_request(
            "PUT",
            &api_socket,
            "/vsock",
            &serde_json::json!({
                // CID 0 and 1 are reserved (hypervisor and host). CID 2 is
                // conventionally the host in some setups. We use 3 as the
                // guest CID — the exact value doesn't matter since we connect
                // via the UDS path, not by CID.
                "guest_cid": 3,
                "uds_path": "./vsock.sock"
            }),
        )
        .await
        .context("configure vsock")?;

        api_request(
            "PUT",
            &api_socket,
            "/machine-config",
            &serde_json::json!({
                "vcpu_count": config.vcpu_count,
                "mem_size_mib": config.mem_size_mib
            }),
        )
        .await
        .context("configure machine")?;

        // Configure balloon device if requested.
        if let Some(ref balloon) = config.balloon {
            api_request(
                "PUT",
                &api_socket,
                "/balloon",
                &serde_json::json!({
                    "amount_mib": balloon.amount_mib,
                    "deflate_on_oom": balloon.deflate_on_oom,
                    "stats_polling_interval_s": balloon.stats_polling_interval_s
                }),
            )
            .await
            .context("configure balloon")?;
        }

        // Configure network interface if requested.
        let tap = if let Some(ref net) = config.net {
            let tap = PersistentTap::create().context("create TAP device")?;
            tap.bring_up().context("bring TAP interface up")?;

            api_request(
                "PUT",
                &api_socket,
                "/network-interfaces/eth0",
                &serde_json::json!({
                    "iface_id": "eth0",
                    "host_dev_name": tap.name(),
                    "guest_mac": format!("{:02X}:{:02X}:{:02X}:{:02X}:{:02X}:{:02X}",
                        net.guest_mac[0], net.guest_mac[1], net.guest_mac[2],
                        net.guest_mac[3], net.guest_mac[4], net.guest_mac[5])
                }),
            )
            .await
            .context("configure network interface")?;

            log::info!(
                "configured network: tap={}, guest_ip={}",
                tap.name(),
                net.guest_ip
            );
            Some(tap)
        } else {
            None
        };

        log::info!("firecracker: starting instance");
        api_request(
            "PUT",
            &api_socket,
            "/actions",
            &serde_json::json!({
                "action_type": "InstanceStart"
            }),
        )
        .await
        .context("start instance")?;
        log::info!("firecracker: instance started");

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

        Ok(FirecrackerInstance {
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
    ) -> anyhow::Result<FirecrackerInstance> {
        let metadata = &snapshot.metadata;
        let snapshot_dir = &snapshot.snapshot_dir;

        // 1. Create new tmpdir.
        let tmpdir = tempfile::tempdir().context("create tmpdir for restore")?;

        // 2. Copy rootfs from original source path and container.ext4 from snapshot.
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

        // 2b. Copy volume images from snapshot.
        for vd in &metadata.volume_drives {
            copy_file_writable(
                &snapshot_dir.join(&vd.filename),
                &tmpdir.path().join(&vd.filename),
            )
            .await
            .with_context(|| format!("copy volume image '{}' from snapshot", vd.filename))?;
        }

        // 3. Copy snapshot.bin and mem.bin from snapshot dir into tmpdir.
        tokio::fs::copy(
            snapshot_dir.join("snapshot.bin"),
            tmpdir.path().join("snapshot.bin"),
        )
        .await
        .context("copy snapshot.bin from snapshot")?;
        tokio::fs::copy(snapshot_dir.join("mem.bin"), tmpdir.path().join("mem.bin"))
            .await
            .context("copy mem.bin from snapshot")?;

        // 4. Create fresh TAP if networking is configured.
        let tap = if net.is_some() {
            let tap = PersistentTap::create().context("create TAP device for restore")?;
            tap.bring_up()
                .context("bring TAP interface up for restore")?;
            log::info!("restore: created TAP {}", tap.name());
            Some(tap)
        } else {
            None
        };

        // 5. Spawn Firecracker and wait for API socket.
        let spawned = spawn_firecracker(
            &self.firecracker_bin,
            tmpdir.path(),
            metadata.serial_console,
        )
        .await?;
        let SpawnedFirecracker {
            child,
            serial_stdout,
            api_socket,
            vsock_uds_path,
        } = spawned;

        // 6. Load snapshot with network overrides.
        let mut load_body = serde_json::json!({
            "snapshot_path": "./snapshot.bin",
            "mem_backend": {
                "backend_path": "./mem.bin",
                "backend_type": "File",
            },
            "resume_vm": true,
        });

        // Add network overrides if we have a TAP.
        if let Some(ref tap) = tap {
            load_body["network_overrides"] = serde_json::json!([
                {
                    "iface_id": "eth0",
                    "host_dev_name": tap.name(),
                }
            ]);
        }

        api_request("PUT", &api_socket, "/snapshot/load", &load_body)
            .await
            .context("load snapshot")?;

        // 7. Open AF_PACKET socket on the new TAP and wrap as FabricPort.
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

        Ok(FirecrackerInstance {
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

pub struct FirecrackerInstance {
    child: tokio::process::Child,
    vsock_uds_path: PathBuf,
    api_socket: PathBuf,
    fabric_port: Option<FabricPort>,
    _serial_task: Option<TaskHandle<()>>,
    /// Watch channel that fires when the VM process exits (via pidfd).
    exit_rx: watch::Receiver<Option<ExitStatus>>,
    /// Background task monitoring the process via pidfd.
    _exit_monitor: TaskHandle<()>,
    _tmpdir: tempfile::TempDir,
    /// Stored for snapshot metadata — needed to reconstruct the VM on restore.
    kernel_path: PathBuf,
    /// Stored for snapshot metadata — the original rootfs image path (re-copied on restore).
    rootfs_source_path: PathBuf,
    /// Whether a balloon device was configured at launch (needed for set_balloon/snapshot).
    balloon_configured: bool,
    /// Whether serial console output is enabled.
    serial_console: bool,
    /// Volume drives attached to the VM (for snapshot/restore).
    volume_drives: Vec<super::SnapshotVolumeDrive>,
}

impl Drop for FirecrackerInstance {
    fn drop(&mut self) {
        // Safety net: if the instance is dropped without explicit cleanup
        // (e.g., task abort, non-graceful stop), send SIGKILL to the process.
        //
        // We use `start_kill()` (not `kill().await`) because Drop is synchronous.
        // start_kill() sends SIGKILL without awaiting — the OS will reap the zombie.
        //
        // IMPORTANT: Rust drops struct fields in declaration order. `child` is
        // declared before `_tmpdir`, so the process is killed before the tmpdir
        // is removed. Reordering the struct fields would cause Firecracker to
        // lose its working directory while still running.
        let _ = self.child.start_kill();
    }
}

impl VmInstance for FirecrackerInstance {
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
        // Reap the child properly (should return immediately since process is dead).
        let status = self.child.wait().await.context("wait for firecracker")?;
        Ok(status)
    }

    async fn kill(&mut self) -> anyhow::Result<()> {
        self.child.kill().await.context("kill firecracker")?;
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
            "PATCH",
            &self.api_socket,
            "/balloon",
            &serde_json::json!({"amount_mib": amount_mib}),
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
        api_request(
            "PATCH",
            &self.api_socket,
            "/vm",
            &serde_json::json!({"state": "Paused"}),
        )
        .await
        .context("pause VM")?;

        // 2. Create snapshot — Firecracker writes to its cwd (tmpdir).
        api_request(
            "PUT",
            &self.api_socket,
            "/snapshot/create",
            &serde_json::json!({
                "snapshot_path": "./snapshot.bin",
                "mem_file_path": "./mem.bin",
            }),
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

        // 4. Copy snapshot.bin and mem.bin from tmpdir into snapshot dir.
        tokio::fs::copy(
            tmpdir_path.join("snapshot.bin"),
            snapshot_dir.join("snapshot.bin"),
        )
        .await
        .context("copy snapshot.bin to snapshot dir")?;
        tokio::fs::copy(tmpdir_path.join("mem.bin"), snapshot_dir.join("mem.bin"))
            .await
            .context("copy mem.bin to snapshot dir")?;

        // 5. Write metadata.json.
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

/// Connect to a guest vsock listener via Firecracker's UDS using async I/O.
async fn try_vsock_connect(sock_path: &Path, port: u32) -> anyhow::Result<UnixStream> {
    use tokio::io::{AsyncBufReadExt, BufReader};

    let stream = UnixStream::connect(sock_path).await?;

    let connect_cmd = format!("CONNECT {}\n", port);
    let (reader, mut writer) = stream.into_split();
    writer.write_all(connect_cmd.as_bytes()).await?;
    writer.flush().await?;

    let mut reader = BufReader::new(reader);
    let mut response = String::new();
    // Use tokio::time::timeout to avoid hanging forever.
    tokio::time::timeout(Duration::from_secs(5), reader.read_line(&mut response))
        .await
        .context("timeout reading vsock CONNECT response")?
        .context("read vsock CONNECT response")?;

    if !response.starts_with("OK ") {
        bail!("vsock CONNECT failed: {}", response.trim());
    }

    // Reunite the split halves.
    Ok(reader.into_inner().reunite(writer)?)
}

/// Send a PUT request to the Firecracker API over a Unix socket.
///
/// Uses raw HTTP — each request uses a fresh connection (Firecracker is
/// one-request-per-connection).
async fn api_request(
    method: &str,
    socket_path: &Path,
    path: &str,
    body: &serde_json::Value,
) -> anyhow::Result<()> {
    let start = std::time::Instant::now();
    log::debug!("firecracker API: {} {}", method, path);

    let body_bytes = serde_json::to_vec(body)?;

    let request = format!(
        "{} {} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        method,
        path,
        body_bytes.len()
    );

    let mut stream = UnixStream::connect(socket_path)
        .await
        .with_context(|| format!("connect to API socket {}", socket_path.display()))?;

    stream.write_all(request.as_bytes()).await?;
    stream.write_all(&body_bytes).await?;
    stream.flush().await?;

    // Read the response with a timeout.
    let mut response = Vec::new();
    let read_result = tokio::time::timeout(Duration::from_secs(5), async {
        let mut buf = [0u8; 4096];
        loop {
            match stream.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    response.extend_from_slice(&buf[..n]);
                    // Check if we have a complete response.
                    if let Ok(s) = std::str::from_utf8(&response) {
                        if s.contains("\r\n\r\n") {
                            if let Some(cl) = parse_content_length(s) {
                                if let Some(body_start) = s.find("\r\n\r\n") {
                                    let body_received = response.len() - body_start - 4;
                                    if body_received >= cl {
                                        break;
                                    }
                                }
                            } else {
                                break;
                            }
                        }
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    })
    .await;

    match read_result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return Err(e).context("read API response"),
        Err(_) => {
            log::warn!(
                "Firecracker API: read timeout on PUT {}, checking partial response",
                path
            );
        }
    }

    let response_str = String::from_utf8_lossy(&response);

    if let Some(status_line) = response_str.lines().next() {
        if !status_line.contains("204") && !status_line.contains("200") {
            bail!("Firecracker API error on {} {}:\n{}", method, path, response_str);
        }
    }

    let elapsed = start.elapsed();
    if elapsed.as_millis() > 500 {
        log::warn!("firecracker API: {} {} took {:.1}s", method, path, elapsed.as_secs_f64());
    } else {
        log::debug!("firecracker API: {} {} completed in {:?}", method, path, elapsed);
    }

    Ok(())
}

fn parse_content_length(headers: &str) -> Option<usize> {
    for line in headers.lines() {
        if let Some(val) = line.strip_prefix("Content-Length: ") {
            return val.trim().parse().ok();
        }
        if let Some(val) = line.strip_prefix("content-length: ") {
            return val.trim().parse().ok();
        }
    }
    None
}

