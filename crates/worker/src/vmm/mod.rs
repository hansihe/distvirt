pub mod cloud_hypervisor;
// pub mod firecracker;
pub mod guest_sim;
pub mod qemu;
pub mod test_vmm;
pub(crate) mod virtiofs;

use std::future::Future;
use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::time::Duration;

use anyhow::Context;
use distvirt_worker_protocol::PodNetworkConfig;
use serde::{Deserialize, Serialize};
use tokio::net::UnixStream;
use tokio::sync::watch;

use crate::fabric::FabricPort;
use crate::image_provider::PreparedArtifact;
use crate::task_handle::TaskHandle;

// ---------------------------------------------------------------------------
// VM configuration types (high-level, VMM-agnostic)
// ---------------------------------------------------------------------------

/// Network configuration for a VM.
#[derive(Clone)]
pub struct NetConfig {
    pub guest_ip: String,
    pub netmask: String,
    pub gateway: String,
    pub guest_mac: [u8; 6],
}

impl From<&PodNetworkConfig> for NetConfig {
    fn from(pnc: &PodNetworkConfig) -> Self {
        NetConfig {
            guest_ip: pnc.ip.to_string(),
            netmask: pnc.netmask.clone(),
            gateway: pnc.gateway.to_string(),
            guest_mac: pnc.mac,
        }
    }
}

/// Configuration for the virtio-balloon device.
#[derive(Clone, Debug)]
pub struct BalloonConfig {
    pub amount_mib: u32,
    pub deflate_on_oom: bool,
    pub stats_polling_interval_s: u32,
}

/// High-level VM configuration.
///
/// The VMM decides how to expose the container image and volumes to the guest
/// (device assignment, virtiofs, overlay, etc).
pub struct VmConfig {
    pub kernel_path: PathBuf,
    pub rootfs_image_path: PathBuf,
    pub vcpu_count: u32,
    pub mem_size_mib: u32,
    pub net: Option<NetConfig>,
    pub serial_console: bool,
    pub balloon: Option<BalloonConfig>,
    /// Container image — the VMM decides how to expose this to the guest.
    pub container_image: PreparedArtifact,
    /// Volumes to attach — the VMM decides the attachment mechanism.
    pub volumes: Vec<VmVolume>,
    /// Context persisted by the VMM in snapshot metadata.
    pub snapshot_context: SnapshotContext,
}

/// A volume to attach to the VM.
pub struct VmVolume {
    pub name: String,
    pub source: VmVolumeSource,
    pub read_only: bool,
}

/// How the volume data is available on the host.
pub enum VmVolumeSource {
    /// Block device image file (e.g. EmptyDir ext4).
    BlockImage { image_path: PathBuf },
    /// Directory to share (e.g. ConfigData).
    Directory { dir_path: PathBuf },
}

/// Context the VMM persists in snapshot metadata for cross-host restore.
pub struct SnapshotContext {
    pub container_image_ref: Option<String>,
    pub config_volumes: Vec<SnapshotConfigVolume>,
}

// ---------------------------------------------------------------------------
// Launch result types (VMM -> supervisor)
// ---------------------------------------------------------------------------

/// Instructions from the VMM to the supervisor for guest setup.
///
/// The supervisor relays these to the guest via the control protocol
/// without interpreting device names or tags.
pub struct LaunchResult {
    pub container_rootfs: distvirt_guest_protocol::ContainerRootfs,
    pub volume_mounts: Vec<VolumeMountInstruction>,
}

/// How the supervisor should tell the guest to mount a volume.
pub struct VolumeMountInstruction {
    pub name: String,
    pub source: distvirt_guest_protocol::VolumeSource,
    pub read_only: bool,
}

// ---------------------------------------------------------------------------
// Restore context
// ---------------------------------------------------------------------------

/// Context for restoring a VM from a snapshot.
pub struct RestoreContext {
    pub net: Option<NetConfig>,
    /// Re-prepared container image on the destination host.
    pub container_image: Option<PreparedArtifact>,
    /// ConfigData volume specs for recreation on the destination.
    pub config_volumes: Vec<SnapshotConfigVolume>,
}

// ---------------------------------------------------------------------------
// Snapshot metadata types (serialized to disk)
// ---------------------------------------------------------------------------

/// Volume drive info persisted in snapshot metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotVolumeDrive {
    pub filename: String,
    pub read_only: bool,
}

/// virtiofs mount info persisted in snapshot metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotVirtiofsMount {
    pub tag: String,
    pub source_dir: PathBuf,
}

/// ConfigData volume info persisted in snapshot metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotConfigVolume {
    pub name: String,
    pub tag: String,
    pub files: Vec<distvirt_worker_protocol::ConfigDataFile>,
}

/// Metadata persisted as `metadata.json` in a snapshot directory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub kernel_path: PathBuf,
    pub rootfs_source_path: PathBuf,
    #[serde(default)]
    pub balloon_configured: bool,
    #[serde(default)]
    pub serial_console: bool,
    #[serde(default)]
    pub volume_drives: Vec<SnapshotVolumeDrive>,
    #[serde(default)]
    pub virtiofs_mounts: Vec<SnapshotVirtiofsMount>,
    #[serde(default)]
    pub container_image_ref: Option<String>,
    #[serde(default)]
    pub config_volumes: Vec<SnapshotConfigVolume>,
}

/// Artifacts produced by a VM snapshot.
pub struct SnapshotArtifacts {
    pub snapshot_dir: PathBuf,
    pub metadata: SnapshotMetadata,
}

// ---------------------------------------------------------------------------
// VMM traits
// ---------------------------------------------------------------------------

/// A VMM implementation that can launch and restore VMs.
pub trait Vmm: Send + Sync {
    type Instance: VmInstance;

    /// Launch a VM with the given configuration.
    ///
    /// Takes `VmConfig` by value because it owns the `PreparedArtifact`
    /// (including the containerd lease for the `Containerd` variant).
    /// Returns the VM instance and instructions for guest setup.
    fn launch(
        &self,
        config: VmConfig,
    ) -> impl Future<Output = anyhow::Result<(Self::Instance, LaunchResult)>> + Send;

    /// Restore a VM from a snapshot.
    fn restore(
        &self,
        snapshot: &SnapshotArtifacts,
        ctx: RestoreContext,
    ) -> impl Future<Output = anyhow::Result<Self::Instance>> + Send {
        let _ = (snapshot, ctx);
        async { anyhow::bail!("snapshot restore not supported by this VMM") }
    }
}

/// A running VM instance.
pub trait VmInstance: Send + 'static {
    fn connect_vsock(&self, port: u32) -> impl Future<Output = anyhow::Result<UnixStream>> + Send;
    fn take_fabric_port(&mut self) -> Option<FabricPort>;
    fn wait(&mut self) -> impl Future<Output = anyhow::Result<ExitStatus>> + Send;
    fn kill(&mut self) -> impl Future<Output = anyhow::Result<()>> + Send;

    fn take_exit_signal(&mut self) -> Option<watch::Receiver<Option<ExitStatus>>> {
        None
    }

    fn snapshot(
        &mut self,
        snapshot_dir: &Path,
    ) -> impl Future<Output = anyhow::Result<SnapshotArtifacts>> + Send {
        let _ = snapshot_dir;
        async { anyhow::bail!("snapshot not supported by this VM instance") }
    }

    fn set_balloon(&mut self, amount_mib: u32) -> impl Future<Output = anyhow::Result<()>> + Send {
        let _ = amount_mib;
        async { anyhow::bail!("balloon not supported by this VM instance") }
    }
}

// ---------------------------------------------------------------------------
// Shared utilities used by VMM backends
// ---------------------------------------------------------------------------

pub(crate) async fn copy_file_writable(src: &Path, dest: &Path) -> anyhow::Result<()> {
    tokio::fs::copy(src, dest)
        .await
        .with_context(|| format!("copy {} to {}", src.display(), dest.display()))?;
    let mut perms = tokio::fs::metadata(dest).await?.permissions();
    perms.set_readonly(false);
    tokio::fs::set_permissions(dest, perms).await?;
    Ok(())
}

pub(crate) async fn wait_for_file(path: &Path, timeout: Duration) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if tokio::fs::try_exists(path).await.unwrap_or(false) {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("timeout waiting for {}", path.display());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub(crate) fn spawn_exit_monitor(
    child: &tokio::process::Child,
) -> (watch::Receiver<Option<ExitStatus>>, TaskHandle<()>) {
    let pid = child.id().expect("child has pid");
    let (exit_tx, exit_rx) = watch::channel(None);
    let handle = TaskHandle::spawn(async move {
        let status = crate::linux::process::wait_for_exit_pidfd(pid).await;
        let _ = exit_tx.send(Some(status));
    });
    (exit_rx, handle)
}

pub(crate) fn spawn_serial_task(stdout: tokio::process::ChildStdout) -> TaskHandle<()> {
    TaskHandle::spawn(async move {
        use tokio::io::{AsyncBufReadExt, BufReader};
        let mut lines = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            log::debug!("[serial] {}", line);
        }
    })
}

pub(crate) async fn api_request(
    method: &str,
    socket_path: &Path,
    path: &str,
    body: Option<&serde_json::Value>,
) -> anyhow::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let start = std::time::Instant::now();
    log::info!("vmm API: {} {}", method, path);

    let mut stream = UnixStream::connect(socket_path)
        .await
        .with_context(|| format!("connect to API socket {}", socket_path.display()))?;

    if let Some(body) = body {
        let body_bytes = serde_json::to_vec(body)?;
        let request = format!(
            "{} {} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            method,
            path,
            body_bytes.len()
        );
        stream.write_all(request.as_bytes()).await?;
        stream.write_all(&body_bytes).await?;
    } else {
        let request = format!(
            "{} {} HTTP/1.1\r\nHost: localhost\r\n\r\n",
            method,
            path,
        );
        stream.write_all(request.as_bytes()).await?;
    }
    stream.flush().await?;

    let mut response = Vec::new();
    let read_result = tokio::time::timeout(Duration::from_secs(5), async {
        let mut buf = [0u8; 4096];
        loop {
            match stream.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    response.extend_from_slice(&buf[..n]);
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
                "vmm API: read timeout on {} {}, checking partial response",
                method,
                path
            );
        }
    }

    let response_str = String::from_utf8_lossy(&response);

    if let Some(status_line) = response_str.lines().next() {
        if !status_line.contains("200")
            && !status_line.contains("201")
            && !status_line.contains("204")
        {
            anyhow::bail!("vmm API error on {} {}:\n{}", method, path, response_str);
        }
    }

    let elapsed = start.elapsed();
    if elapsed.as_millis() > 500 {
        log::warn!(
            "vmm API: {} {} took {:.1}s",
            method,
            path,
            elapsed.as_secs_f64()
        );
    } else {
        log::info!("vmm API: {} {} completed in {:?}", method, path, elapsed);
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

pub(crate) async fn try_vsock_connect(sock_path: &Path, port: u32) -> anyhow::Result<UnixStream> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let stream = UnixStream::connect(sock_path).await?;

    let connect_cmd = format!("CONNECT {}\n", port);
    let (reader, mut writer) = stream.into_split();
    writer.write_all(connect_cmd.as_bytes()).await?;
    writer.flush().await?;

    let mut reader = BufReader::new(reader);
    let mut response = String::new();
    tokio::time::timeout(Duration::from_secs(5), reader.read_line(&mut response))
        .await
        .context("timeout reading vsock CONNECT response")?
        .context("read vsock CONNECT response")?;

    if !response.starts_with("OK ") {
        anyhow::bail!("vsock CONNECT failed: {}", response.trim());
    }

    Ok(reader.into_inner().reunite(writer)?)
}
