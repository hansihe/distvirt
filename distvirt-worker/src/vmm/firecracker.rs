use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{bail, Context};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

use super::{NetConfig, SnapshotArtifacts, SnapshotMetadata, VmConfig, VmInstance, Vmm};
use crate::tap::TapDevice;
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

impl Vmm for Firecracker {
    type Instance = FirecrackerInstance;

    async fn launch(&self, config: &VmConfig) -> anyhow::Result<FirecrackerInstance> {
        let tmpdir = tempfile::tempdir().context("create tmpdir")?;

        // Copy rootfs image to tmpdir — Firecracker needs a writable rootfs,
        // but the source may be in a read-only location (e.g. Nix store).
        // Each VM gets its own copy to allow independent filesystem mutations.
        let rootfs_path = tmpdir.path().join("rootfs.ext4");
        tokio::fs::copy(&config.rootfs_image_path, &rootfs_path)
            .await
            .with_context(|| {
                format!("copy rootfs from {}", config.rootfs_image_path.display())
            })?;
        // Ensure the copy is writable (source may be read-only, e.g. from nix store).
        let mut perms = tokio::fs::metadata(&rootfs_path).await?.permissions();
        perms.set_readonly(false);
        tokio::fs::set_permissions(&rootfs_path, perms).await?;

        // Copy container image to tmpdir so all per-VM state is self-contained.
        let container_path = tmpdir.path().join("container.ext4");
        tokio::fs::copy(&config.container_image_path, &container_path)
            .await
            .with_context(|| {
                format!(
                    "copy container from {}",
                    config.container_image_path.display()
                )
            })?;
        let mut perms = tokio::fs::metadata(&container_path).await?.permissions();
        perms.set_readonly(false);
        tokio::fs::set_permissions(&container_path, perms).await?;

        let api_socket = tmpdir.path().join("firecracker.sock");
        let vsock_uds_path = tmpdir.path().join("vsock.sock");

        // Start firecracker process. Container output is captured separately via vsock I/O sessions.
        // When serial_console is enabled, pipe stdout so we can forward kernel boot logs.
        // Set working directory to tmpdir so Firecracker uses relative paths.
        // This makes snapshots portable — restore just needs the same file layout.
        let mut cmd = tokio::process::Command::new(&self.firecracker_bin);
        cmd.current_dir(tmpdir.path());
        cmd.arg("--api-sock").arg("./firecracker.sock");
        if config.serial_console {
            cmd.stdout(Stdio::piped());
        } else {
            cmd.stdout(Stdio::null());
        }
        cmd.stderr(Stdio::null());
        let mut child = cmd.spawn().context("spawn firecracker")?;

        // Take stdout for serial console.
        let serial_stdout = if config.serial_console {
            child.stdout.take()
        } else {
            None
        };

        // Wait for API socket to appear.
        wait_for_file(&api_socket, Duration::from_secs(5))
            .await
            .context("waiting for firecracker API socket")?;

        // Configure the VM via the API.
        api_request("PUT",
            &api_socket,
            "/boot-source",
            &serde_json::json!({
                "kernel_image_path": config.kernel_path.to_str()
                    .ok_or_else(|| anyhow::anyhow!("kernel_path is not valid UTF-8: {:?}", config.kernel_path))?,
                // Boot args for the microVM kernel:
                //   console=ttyS0  — serial console for boot logs (captured via stdout)
                //   reboot=k       — use keyboard controller reset (prevents triple-fault reboot loop)
                //   panic=1        — reboot 1s after kernel panic (fast failure detection)
                //   pci=off        — disable PCI bus scanning (Firecracker has no PCI, saves boot time)
                //   init=/sbin/init — our custom init binary (not systemd)
                "boot_args": "console=ttyS0 reboot=k panic=1 pci=off init=/sbin/init"
            }),
        )
        .await
        .context("configure boot-source")?;

        api_request("PUT",
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

        api_request("PUT",
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

        api_request("PUT",
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

        api_request("PUT",
            &api_socket,
            "/machine-config",
            &serde_json::json!({
                "vcpu_count": config.vcpu_count,
                "mem_size_mib": config.mem_size_mib
            }),
        )
        .await
        .context("configure machine")?;

        // Configure network interface if requested.
        let tap_name = if let Some(ref net) = config.net {
            let tap_name =
                crate::tap::create_persistent_tap().context("create TAP device")?;

            crate::tap::bring_interface_up(&tap_name)
                .context("bring TAP interface up")?;

            api_request("PUT",
                &api_socket,
                "/network-interfaces/eth0",
                &serde_json::json!({
                    "iface_id": "eth0",
                    "host_dev_name": tap_name,
                    "guest_mac": format!("{:02X}:{:02X}:{:02X}:{:02X}:{:02X}:{:02X}",
                        net.guest_mac[0], net.guest_mac[1], net.guest_mac[2],
                        net.guest_mac[3], net.guest_mac[4], net.guest_mac[5])
                }),
            )
            .await
            .context("configure network interface")?;

            log::info!(
                "configured network: tap={}, guest_ip={}",
                tap_name,
                net.guest_ip
            );
            Some(tap_name)
        } else {
            None
        };

        api_request("PUT",
            &api_socket,
            "/actions",
            &serde_json::json!({
                "action_type": "InstanceStart"
            }),
        )
        .await
        .context("start instance")?;

        // Open AF_PACKET socket on the TAP for host-side L2 frame I/O.
        let tap = if let Some(ref name) = tap_name {
            Some(crate::tap::open_packet_socket(name).context("open packet socket on TAP")?)
        } else {
            None
        };

        let mut instance = FirecrackerInstance {
            child,
            vsock_uds_path,
            api_socket,
            tap,
            serial_stdout,
            _serial_task: None,
            _tmpdir: tmpdir,
            kernel_path: config.kernel_path.clone(),
            rootfs_source_path: config.rootfs_image_path.clone(),
        };

        // Spawn serial console reader.
        if let Some(stdout) = instance.serial_stdout.take() {
            instance._serial_task = Some(TaskHandle::spawn(async move {
                use tokio::io::{AsyncBufReadExt, BufReader};
                let mut lines = BufReader::new(stdout).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    log::debug!("[serial] {}", line);
                }
            }));
        }

        Ok(instance)
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

        // 2. Copy rootfs from original source path into tmpdir.
        let rootfs_dest = tmpdir.path().join("rootfs.ext4");
        tokio::fs::copy(&metadata.rootfs_source_path, &rootfs_dest)
            .await
            .with_context(|| {
                format!("copy rootfs from {}", metadata.rootfs_source_path.display())
            })?;
        let mut perms = tokio::fs::metadata(&rootfs_dest).await?.permissions();
        perms.set_readonly(false);
        tokio::fs::set_permissions(&rootfs_dest, perms).await?;

        // 3. Copy container.ext4 from snapshot dir into tmpdir.
        tokio::fs::copy(
            snapshot_dir.join("container.ext4"),
            tmpdir.path().join("container.ext4"),
        )
        .await
        .context("copy container.ext4 from snapshot")?;
        let mut perms = tokio::fs::metadata(tmpdir.path().join("container.ext4"))
            .await?
            .permissions();
        perms.set_readonly(false);
        tokio::fs::set_permissions(tmpdir.path().join("container.ext4"), perms).await?;

        // 4. Copy snapshot.bin and mem.bin from snapshot dir into tmpdir.
        tokio::fs::copy(
            snapshot_dir.join("snapshot.bin"),
            tmpdir.path().join("snapshot.bin"),
        )
        .await
        .context("copy snapshot.bin from snapshot")?;
        tokio::fs::copy(
            snapshot_dir.join("mem.bin"),
            tmpdir.path().join("mem.bin"),
        )
        .await
        .context("copy mem.bin from snapshot")?;

        // 5. Create fresh TAP if networking is configured.
        let tap_name = if net.is_some() {
            let name = crate::tap::create_persistent_tap()
                .context("create TAP device for restore")?;
            crate::tap::bring_interface_up(&name)
                .context("bring TAP interface up for restore")?;
            log::info!("restore: created TAP {}", name);
            Some(name)
        } else {
            None
        };

        // 6. Spawn Firecracker with cwd = tmpdir.
        let api_socket = tmpdir.path().join("firecracker.sock");
        let vsock_uds_path = tmpdir.path().join("vsock.sock");

        let mut cmd = tokio::process::Command::new(&self.firecracker_bin);
        cmd.current_dir(tmpdir.path());
        cmd.arg("--api-sock").arg("./firecracker.sock");
        cmd.stdout(Stdio::null());
        cmd.stderr(Stdio::null());
        let child = cmd.spawn().context("spawn firecracker for restore")?;

        // 7. Wait for API socket.
        wait_for_file(&api_socket, Duration::from_secs(5))
            .await
            .context("waiting for firecracker API socket (restore)")?;

        // 8. Load snapshot with network overrides.
        let mut load_body = serde_json::json!({
            "snapshot_path": "./snapshot.bin",
            "mem_backend": {
                "backend_path": "./mem.bin",
                "backend_type": "File",
            },
            "resume_vm": true,
        });

        // Add network overrides if we have a TAP.
        if let Some(ref tap) = tap_name {
            load_body["network_overrides"] = serde_json::json!([
                {
                    "iface_id": "eth0",
                    "host_dev_name": tap,
                }
            ]);
        }

        api_request("PUT",&api_socket, "/snapshot/load", &load_body)
            .await
            .context("load snapshot")?;

        // 9. Open AF_PACKET socket on the new TAP.
        let tap = if let Some(ref name) = tap_name {
            Some(
                crate::tap::open_packet_socket(name)
                    .context("open packet socket on TAP (restore)")?,
            )
        } else {
            None
        };

        log::info!("VM restored from snapshot at {}", snapshot_dir.display());

        Ok(FirecrackerInstance {
            child,
            vsock_uds_path,
            api_socket,
            tap,
            serial_stdout: None,
            _serial_task: None,
            _tmpdir: tmpdir,
            kernel_path: metadata.kernel_path.clone(),
            rootfs_source_path: metadata.rootfs_source_path.clone(),
        })
    }
}

pub struct FirecrackerInstance {
    child: tokio::process::Child,
    vsock_uds_path: PathBuf,
    api_socket: PathBuf,
    tap: Option<TapDevice>,
    serial_stdout: Option<tokio::process::ChildStdout>,
    _serial_task: Option<TaskHandle<()>>,
    _tmpdir: tempfile::TempDir,
    /// Stored for snapshot metadata — needed to reconstruct the VM on restore.
    kernel_path: PathBuf,
    /// Stored for snapshot metadata — the original rootfs image path (re-copied on restore).
    rootfs_source_path: PathBuf,
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

    fn tap(&self) -> Option<&TapDevice> {
        self.tap.as_ref()
    }

    fn take_tap(&mut self) -> Option<TapDevice> {
        self.tap.take()
    }

    async fn wait(&mut self) -> anyhow::Result<()> {
        self.child.wait().await.context("wait for firecracker")?;
        Ok(())
    }

    async fn kill(&mut self) -> anyhow::Result<()> {
        self.child.kill().await.context("kill firecracker")?;
        Ok(())
    }

    async fn snapshot(&mut self, snapshot_dir: &Path) -> anyhow::Result<SnapshotArtifacts> {
        tokio::fs::create_dir_all(snapshot_dir)
            .await
            .with_context(|| format!("create snapshot dir {}", snapshot_dir.display()))?;

        // 1. Pause vCPUs.
        api_request("PATCH",
            &self.api_socket,
            "/vm",
            &serde_json::json!({"state": "Paused"}),
        )
        .await
        .context("pause VM")?;

        // 2. Create snapshot — Firecracker writes to its cwd (tmpdir).
        api_request("PUT",
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

        // 4. Copy snapshot.bin and mem.bin from tmpdir into snapshot dir.
        tokio::fs::copy(
            tmpdir_path.join("snapshot.bin"),
            snapshot_dir.join("snapshot.bin"),
        )
        .await
        .context("copy snapshot.bin to snapshot dir")?;
        tokio::fs::copy(
            tmpdir_path.join("mem.bin"),
            snapshot_dir.join("mem.bin"),
        )
        .await
        .context("copy mem.bin to snapshot dir")?;

        // 5. Write metadata.json.
        let metadata = SnapshotMetadata {
            kernel_path: self.kernel_path.clone(),
            rootfs_source_path: self.rootfs_source_path.clone(),
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
            log::warn!("Firecracker API: read timeout on PUT {}, checking partial response", path);
        }
    }

    let response_str = String::from_utf8_lossy(&response);

    if let Some(status_line) = response_str.lines().next() {
        if !status_line.contains("204") && !status_line.contains("200") {
            bail!(
                "Firecracker API error on PUT {}:\n{}",
                path,
                response_str
            );
        }
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

async fn wait_for_file(path: &Path, timeout: Duration) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if tokio::fs::try_exists(path).await.unwrap_or(false) {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("timeout waiting for {}", path.display());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
