use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use anyhow::{bail, Context};

use super::{VmConfig, VmInstance, Vmm};

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

    fn launch(&self, config: &VmConfig) -> anyhow::Result<FirecrackerInstance> {
        let tmpdir = tempfile::tempdir().context("create tmpdir")?;

        // Copy rootfs image to tmpdir (Firecracker needs writable).
        let rootfs_path = tmpdir.path().join("rootfs.ext4");
        std::fs::copy(&config.rootfs_image_path, &rootfs_path).with_context(|| {
            format!(
                "copy rootfs from {}",
                config.rootfs_image_path.display()
            )
        })?;
        // Ensure the copy is writable (source may be read-only, e.g. from nix store).
        let mut perms = std::fs::metadata(&rootfs_path)?.permissions();
        perms.set_readonly(false);
        std::fs::set_permissions(&rootfs_path, perms)?;

        let api_socket = tmpdir.path().join("firecracker.sock");
        let vsock_uds_path = tmpdir.path().join("vsock.sock");

        // Start firecracker process.
        let child = Command::new(&self.firecracker_bin)
            .arg("--api-sock")
            .arg(&api_socket)
            .spawn()
            .context("spawn firecracker")?;

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
                "kernel_image_path": config.kernel_path.to_str().unwrap(),
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
                "path_on_host": config.container_image_path.to_str().unwrap(),
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
                "vcpu_count": config.vcpu_count,
                "mem_size_mib": config.mem_size_mib
            }),
        )
        .context("configure machine")?;

        api(
            "/actions",
            &serde_json::json!({
                "action_type": "InstanceStart"
            }),
        )
        .context("start instance")?;

        Ok(FirecrackerInstance {
            child,
            vsock_uds_path,
            _tmpdir: tmpdir,
        })
    }
}

pub struct FirecrackerInstance {
    child: Child,
    vsock_uds_path: PathBuf,
    _tmpdir: tempfile::TempDir,
}

impl VmInstance for FirecrackerInstance {
    fn connect_vsock(&self, port: u32) -> anyhow::Result<UnixStream> {
        // For host-initiated connections to a guest listener, Firecracker
        // requires connecting to the vsock UDS and sending a CONNECT handshake.
        let sock_path = &self.vsock_uds_path;

        log::info!("connecting to guest vsock port {} via {}", port, sock_path.display());

        // Retry loop — the guest needs time to boot and start listening.
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match try_vsock_connect(sock_path, port) {
                Ok(stream) => {
                    log::info!("vsock connected");
                    return Ok(stream);
                }
                Err(_) => {
                    if Instant::now() >= deadline {
                        bail!(
                            "timeout connecting to guest vsock port {} via {}",
                            port,
                            sock_path.display()
                        );
                    }
                    std::thread::sleep(Duration::from_millis(200));
                }
            }
        }
    }

    fn wait(&mut self) -> anyhow::Result<()> {
        self.child.wait().context("wait for firecracker")?;
        Ok(())
    }

    fn kill(&mut self) -> anyhow::Result<()> {
        self.child.kill().context("kill firecracker")?;
        Ok(())
    }
}

/// Connect to a guest vsock listener via Firecracker's UDS.
///
/// The host connects to the vsock UDS and sends `CONNECT <port>\n`.
/// Firecracker responds with `OK <port>\n` on success.
fn try_vsock_connect(sock_path: &Path, port: u32) -> anyhow::Result<UnixStream> {
    use std::io::BufRead;

    let mut stream = UnixStream::connect(sock_path)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;

    let connect_cmd = format!("CONNECT {}\n", port);
    stream.write_all(connect_cmd.as_bytes())?;
    stream.flush()?;

    let mut reader = std::io::BufReader::new(&stream);
    let mut response = String::new();
    reader.read_line(&mut response)?;

    if !response.starts_with("OK ") {
        bail!("vsock CONNECT failed: {}", response.trim());
    }

    // Clear the read timeout for normal operation.
    stream.set_read_timeout(None)?;
    Ok(stream)
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

    let mut stream = UnixStream::connect(socket_path)
        .with_context(|| format!("connect to API socket {}", socket_path.display()))?;

    stream.write_all(request.as_bytes())?;
    stream.write_all(&body_bytes)?;
    stream.flush()?;

    // Read the response.
    let mut response = Vec::new();
    // Set a read timeout so we don't hang forever.
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
        // Check if we've received the full response (look for end of HTTP body).
        if let Ok(s) = std::str::from_utf8(&response) {
            if s.contains("\r\n\r\n") {
                // For our purposes, once we have headers + any body, we're done.
                // Check if we have Content-Length and have read enough.
                if let Some(cl) = parse_content_length(s) {
                    if let Some(body_start) = s.find("\r\n\r\n") {
                        let body_received = response.len() - body_start - 4;
                        if body_received >= cl {
                            break;
                        }
                    }
                } else {
                    // No content-length, the headers-only response is complete.
                    break;
                }
            }
        }
    }

    let response_str = String::from_utf8_lossy(&response);

    // Check for HTTP error status.
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
    let deadline = Instant::now() + timeout;
    while !path.exists() {
        if Instant::now() >= deadline {
            bail!("timeout waiting for {}", path.display());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Ok(())
}
