use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{bail, Context};
use tokio::net::UnixStream;

use super::{VmConfig, VmInstance, Vmm};
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
        // The Firecracker API is sync HTTP over UDS — wrap in spawn_blocking.
        let firecracker_bin = self.firecracker_bin.clone();
        let config_kernel = config.kernel_path.clone();
        let config_rootfs = config.rootfs_image_path.clone();
        let config_container = config.container_image_path.clone();
        let config_vcpu = config.vcpu_count;
        let config_mem = config.mem_size_mib;
        let config_net = config.net.as_ref().map(|n| (n.guest_ip.clone(), n.netmask.clone(), n.gateway.clone(), n.guest_mac));
        let config_serial = config.serial_console;

        let mut instance = tokio::task::spawn_blocking(move || {
            launch_sync(
                &firecracker_bin,
                &config_kernel,
                &config_rootfs,
                &config_container,
                config_vcpu,
                config_mem,
                config_net.as_ref(),
                config_serial,
            )
        })
        .await
        .context("spawn_blocking launch")??;

        // Spawn serial console reader in async context so we can use TaskHandle.
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
}

fn launch_sync(
    firecracker_bin: &Path,
    kernel_path: &Path,
    rootfs_image_path: &Path,
    container_image_path: &Path,
    vcpu_count: u32,
    mem_size_mib: u32,
    net: Option<&(String, String, String, [u8; 6])>,
    serial_console: bool,
) -> anyhow::Result<FirecrackerInstance> {
    let tmpdir = tempfile::tempdir().context("create tmpdir")?;

    // Copy rootfs image to tmpdir (Firecracker needs writable).
    let rootfs_path = tmpdir.path().join("rootfs.ext4");
    std::fs::copy(rootfs_image_path, &rootfs_path).with_context(|| {
        format!("copy rootfs from {}", rootfs_image_path.display())
    })?;
    // Ensure the copy is writable (source may be read-only, e.g. from nix store).
    let mut perms = std::fs::metadata(&rootfs_path)?.permissions();
    perms.set_readonly(false);
    std::fs::set_permissions(&rootfs_path, perms)?;

    let api_socket = tmpdir.path().join("firecracker.sock");
    let vsock_uds_path = tmpdir.path().join("vsock.sock");

    // Start firecracker process. Container output is captured separately via vsock I/O sessions.
    // When serial_console is enabled, pipe stdout so we can forward kernel boot logs.
    let mut cmd = tokio::process::Command::new(firecracker_bin);
    cmd.arg("--api-sock").arg(&api_socket);
    if serial_console {
        cmd.stdout(Stdio::piped());
    } else {
        cmd.stdout(Stdio::null());
    }
    cmd.stderr(Stdio::null());
    let mut child = cmd.spawn().context("spawn firecracker")?;

    // Take stdout for serial console — will be spawned as a TaskHandle
    // in the async launch() method after spawn_blocking returns.
    let serial_stdout = if serial_console {
        child.stdout.take()
    } else {
        None
    };

    // Wait for API socket to appear.
    wait_for_file(&api_socket, Duration::from_secs(5))
        .context("waiting for firecracker API socket")?;

    // Configure the VM via the API.
    let api = |path: &str, body: &serde_json::Value| -> anyhow::Result<()> {
        api_put(&api_socket, path, body)
    };

    api(
        "/boot-source",
        &serde_json::json!({
            "kernel_image_path": kernel_path.to_str().unwrap(),
            "boot_args": "console=ttyS0 reboot=k panic=1 pci=off init=/sbin/init"
        }),
    )
    .context("configure boot-source")?;

    api(
        "/drives/rootfs",
        &serde_json::json!({
            "drive_id": "rootfs",
            "path_on_host": rootfs_path.to_str().unwrap(),
            "is_root_device": true,
            "is_read_only": false
        }),
    )
    .context("configure rootfs drive")?;

    api(
        "/drives/container",
        &serde_json::json!({
            "drive_id": "container",
            "path_on_host": container_image_path.to_str().unwrap(),
            "is_root_device": false,
            "is_read_only": false
        }),
    )
    .context("configure container drive")?;

    api(
        "/vsock",
        &serde_json::json!({
            "guest_cid": 3,
            "uds_path": vsock_uds_path.to_str().unwrap()
        }),
    )
    .context("configure vsock")?;

    api(
        "/machine-config",
        &serde_json::json!({
            "vcpu_count": vcpu_count,
            "mem_size_mib": mem_size_mib
        }),
    )
    .context("configure machine")?;

    // Configure network interface if requested.
    let tap_name = if let Some((guest_ip, _netmask, _gateway, guest_mac)) = net {
        let tap_name = crate::tap::create_persistent_tap()
            .context("create TAP device")?;

        crate::tap::bring_interface_up(&tap_name)
            .context("bring TAP interface up")?;

        api(
            "/network-interfaces/eth0",
            &serde_json::json!({
                "iface_id": "eth0",
                "host_dev_name": tap_name,
                "guest_mac": format!("{:02X}:{:02X}:{:02X}:{:02X}:{:02X}:{:02X}",
                    guest_mac[0], guest_mac[1], guest_mac[2],
                    guest_mac[3], guest_mac[4], guest_mac[5])
            }),
        )
        .context("configure network interface")?;

        log::info!(
            "configured network: tap={}, guest_ip={}",
            tap_name,
            guest_ip
        );
        Some(tap_name)
    } else {
        None
    };

    api(
        "/actions",
        &serde_json::json!({
            "action_type": "InstanceStart"
        }),
    )
    .context("start instance")?;

    // Open AF_PACKET socket on the TAP for host-side L2 frame I/O.
    let tap = if let Some(ref name) = tap_name {
        Some(crate::tap::open_packet_socket(name).context("open packet socket on TAP")?)
    } else {
        None
    };

    Ok(FirecrackerInstance {
        child,
        vsock_uds_path,
        tap,
        serial_stdout,
        _serial_task: None,
        _tmpdir: tmpdir,
    })
}

pub struct FirecrackerInstance {
    child: tokio::process::Child,
    vsock_uds_path: PathBuf,
    tap: Option<TapDevice>,
    serial_stdout: Option<tokio::process::ChildStdout>,
    _serial_task: Option<TaskHandle<()>>,
    _tmpdir: tempfile::TempDir,
}

impl Drop for FirecrackerInstance {
    fn drop(&mut self) {
        // Safety net: if the instance is dropped without explicit cleanup
        // (e.g., task abort, non-graceful stop), send SIGKILL to the process.
        // start_kill() is non-async and safe to call in synchronous Drop.
        // Field drop order ensures `child` is killed before `_tmpdir` is removed.
        let _ = self.child.start_kill();
    }
}

impl VmInstance for FirecrackerInstance {
    async fn connect_vsock(&self, port: u32) -> anyhow::Result<UnixStream> {
        let sock_path = self.vsock_uds_path.clone();

        log::info!("connecting to guest vsock port {} via {}", port, sock_path.display());

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
}

/// Connect to a guest vsock listener via Firecracker's UDS using async I/O.
async fn try_vsock_connect(sock_path: &Path, port: u32) -> anyhow::Result<UnixStream> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

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
fn api_put(socket_path: &Path, path: &str, body: &serde_json::Value) -> anyhow::Result<()> {
    let body_bytes = serde_json::to_vec(body)?;

    let request = format!(
        "PUT {} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        path,
        body_bytes.len()
    );

    let mut stream = std::os::unix::net::UnixStream::connect(socket_path)
        .with_context(|| format!("connect to API socket {}", socket_path.display()))?;

    stream.write_all(request.as_bytes())?;
    stream.write_all(&body_bytes)?;
    stream.flush()?;

    // Read the response.
    let mut response = Vec::new();
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    loop {
        let mut buf = [0u8; 4096];
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => response.extend_from_slice(&buf[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) if e.kind() == std::io::ErrorKind::TimedOut => break,
            Err(e) => return Err(e).context("read API response"),
        }
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

fn wait_for_file(path: &Path, timeout: Duration) -> anyhow::Result<()> {
    let deadline = std::time::Instant::now() + timeout;
    while !path.exists() {
        if std::time::Instant::now() >= deadline {
            bail!("timeout waiting for {}", path.display());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Ok(())
}
