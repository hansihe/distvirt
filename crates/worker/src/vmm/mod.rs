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
use crate::image_provider::{ContainerdLease, ResolvedImage};
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

/// Context the pod layer persists in snapshot metadata for cross-host restore.
///
/// The VMM stores this in snapshot metadata but does not interpret it.
pub struct SnapshotContext {
    pub mount_restore_info: Vec<MountRestoreInfo>,
}

// ---------------------------------------------------------------------------
// Restore context
// ---------------------------------------------------------------------------

/// Context for restoring a VM from a snapshot.
pub struct RestoreContext {
    pub net: Option<NetConfig>,
    /// Mount sources to re-establish on the destination host.
    pub mounts: Vec<RestoreMount>,
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
    /// Pod-layer mount restore info (new field).
    #[serde(default)]
    pub mount_restore_info: Vec<MountRestoreInfo>,

    // --- Deprecated fields kept for backward compat with old snapshots ---
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
// Builder-based VMM types (mount-centric interface)
// ---------------------------------------------------------------------------

/// Base VM configuration (VMM-agnostic, no mount/container specifics).
pub struct BaseVmConfig {
    pub kernel_path: PathBuf,
    pub rootfs_image_path: PathBuf,
    pub vcpu_count: u32,
    pub mem_size_mib: u32,
    pub net: Option<NetConfig>,
    pub serial_console: bool,
    pub balloon: Option<BalloonConfig>,
}

/// A request to mount a source into the guest VM.
pub struct MountRequest {
    /// Opaque tag identifying this mount. Used as join key between request and result.
    pub tag: String,
    /// The host-side source to expose to the guest.
    pub source: VmMountSource,
}

/// How the mount source is available on the host.
pub enum VmMountSource {
    /// A containerd image — VMM decides strategy (virtiofs+overlayfs, block, etc.)
    ContainerdImage {
        resolved: ResolvedImage,
        lease: ContainerdLease,
    },
    /// A host directory to share with the guest.
    Directory { path: PathBuf },
    /// A block device image file.
    BlockImage { path: PathBuf, read_only: bool },
}

/// VMM's response to a mount request: what it will actually provide.
pub struct PlannedMount {
    pub tag: String,
    pub provided: ProvidedAccess,
}

/// How the VMM will expose a requested mount to the guest.
#[derive(Clone, Debug)]
pub enum ProvidedAccess {
    /// The mount will be available as a virtiofs share.
    VirtioFs { read_only: bool },
    /// The mount will be available as a block device.
    BlockDevice { read_only: bool },
}

/// Final device assignments after VM launch.
pub struct ResolvedMounts {
    pub entries: Vec<ResolvedEntry>,
}

impl ResolvedMounts {
    /// Look up a resolved mount by tag.
    pub fn get(&self, tag: &str) -> Option<&ResolvedEntry> {
        self.entries.iter().find(|e| e.tag == tag)
    }
}

/// A single resolved mount: tag mapped to guest-visible device.
pub struct ResolvedEntry {
    pub tag: String,
    pub guest: GuestDevice,
}

/// How a mount is accessible from inside the guest.
pub enum GuestDevice {
    /// Accessible as a virtiofs filesystem with this tag.
    VirtioFs { virtiofs_tag: String },
    /// Accessible as a block device at this path.
    Device { path: String },
}

/// A mount source to re-establish during VM restore.
pub struct RestoreMount {
    /// Tag matching the original mount (from snapshot metadata).
    pub tag: String,
    /// The host-side source on the destination host.
    pub source: VmMountSource,
}

/// Information needed by the pod layer to rebuild a mount on restore.
/// Stored in snapshot metadata, interpreted by the pod layer (not the VMM).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MountRestoreInfo {
    pub tag: String,
    pub kind: MountRestoreKind,
}

/// How to restore a specific mount on the destination host.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MountRestoreKind {
    /// Re-prepare the container image via the image provider.
    ImageRef { image_ref: String },
    /// Recreate a config directory from file specs.
    ConfigData {
        files: Vec<distvirt_worker_protocol::ConfigDataFile>,
    },
    /// Data is persisted in the snapshot directory; no host-side action needed.
    Persisted,
}

// ---------------------------------------------------------------------------
// VmArtifacts — one-time artifacts produced alongside a VM instance
// ---------------------------------------------------------------------------

/// One-time artifacts produced when a VM is launched or restored.
///
/// These are setup-time values that should be consumed exactly once,
/// rather than living on the `VmInstance` trait behind `Option::take` patterns.
pub struct VmArtifacts<I> {
    pub instance: I,
    pub vsock_stream: UnixStream,
    pub fabric_port: Option<FabricPort>,
    pub exit_signal: watch::Receiver<Option<ExitStatus>>,
}

// ---------------------------------------------------------------------------
// VmBuilder trait
// ---------------------------------------------------------------------------

/// A builder for configuring and launching a VM.
///
/// The builder negotiates mount strategy: each `add_mount` call returns
/// what the VMM will provide, allowing the caller to adapt (e.g., request
/// a scratch device for overlay if the VMM gives a read-only virtiofs share).
pub trait VmBuilder: Send {
    type Instance: VmInstance;

    /// Register a mount source. Returns what the VMM will provide.
    fn add_mount(&mut self, request: MountRequest) -> anyhow::Result<PlannedMount>;

    /// Request a scratch block device (e.g., for overlay upper/work dirs).
    fn add_scratch_device(&mut self, tag: &str, size_mib: u32) -> anyhow::Result<()>;

    /// Set the snapshot context (pod-layer metadata to persist in snapshots).
    fn set_snapshot_context(&mut self, mount_restore_info: Vec<MountRestoreInfo>);

    /// Finalize configuration and launch the VM.
    fn launch(self) -> impl Future<Output = anyhow::Result<(VmArtifacts<Self::Instance>, ResolvedMounts)>> + Send;
}

// ---------------------------------------------------------------------------
// VMM traits
// ---------------------------------------------------------------------------

/// A VMM implementation that can launch and restore VMs.
pub trait Vmm: Send + Sync {
    type Builder: VmBuilder<Instance = Self::Instance>;
    type Instance: VmInstance;

    /// Create a builder for configuring and launching a new VM.
    ///
    /// The builder negotiates mount strategy: each `add_mount` call returns
    /// what the VMM will provide, allowing the caller to adapt.
    fn builder(&self, base: BaseVmConfig) -> anyhow::Result<Self::Builder>;

    /// Restore a VM from a snapshot.
    ///
    /// The caller provides mount sources to re-establish on the destination
    /// host. No negotiation — decisions are already baked into the snapshot.
    fn restore(
        &self,
        snapshot: &SnapshotArtifacts,
        ctx: RestoreContext,
    ) -> impl Future<Output = anyhow::Result<VmArtifacts<Self::Instance>>> + Send {
        let _ = (snapshot, ctx);
        async { anyhow::bail!("snapshot restore not supported by this VMM") }
    }
}

/// A running VM instance.
pub trait VmInstance: Send + 'static {
    fn wait(&mut self) -> impl Future<Output = anyhow::Result<ExitStatus>> + Send;
    fn kill(&mut self) -> impl Future<Output = anyhow::Result<()>> + Send;

    /// Clone-snapshot: pause, snapshot, resume. VM keeps running afterward.
    fn snapshot(
        &mut self,
        snapshot_dir: &Path,
    ) -> impl Future<Output = anyhow::Result<SnapshotArtifacts>> + Send {
        let _ = snapshot_dir;
        async { anyhow::bail!("snapshot not supported by this VM instance") }
    }

    /// Suspend: pause, snapshot, teardown. Consumes the instance.
    ///
    /// The instance is dropped after the snapshot is written. The `Drop` impl
    /// on the underlying VMM process handle kills the child.
    fn suspend(
        self,
        snapshot_dir: &Path,
    ) -> impl Future<Output = anyhow::Result<SnapshotArtifacts>> + Send
    where
        Self: Sized,
    {
        let _ = snapshot_dir;
        async { anyhow::bail!("suspend not supported by this VM instance") }
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

pub(crate) fn spawn_stderr_task(stderr: tokio::process::ChildStderr) -> TaskHandle<()> {
    TaskHandle::spawn(async move {
        use tokio::io::{AsyncBufReadExt, BufReader};
        let mut lines = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            log::warn!("[cloud-hypervisor stderr] {}", line);
        }
    })
}

pub(crate) async fn api_request(
    method: &str,
    socket_path: &Path,
    path: &str,
    body: Option<&serde_json::Value>,
) -> anyhow::Result<()> {
    api_request_with_timeout(method, socket_path, path, body, Duration::from_secs(30)).await
}

pub(crate) async fn api_request_with_timeout(
    method: &str,
    socket_path: &Path,
    path: &str,
    body: Option<&serde_json::Value>,
    timeout: Duration,
) -> anyhow::Result<()> {
    use http_body_util::{BodyExt, Empty, Full};
    use hyper::body::Bytes;
    use hyper::Request;

    let start = std::time::Instant::now();
    log::info!("vmm API: {} {}", method, path);

    let stream = UnixStream::connect(socket_path)
        .await
        .with_context(|| format!("connect to API socket {}", socket_path.display()))?;
    let io = hyper_util::rt::TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io)
        .await
        .context("HTTP handshake with VMM API")?;
    tokio::spawn(conn);

    let request = if let Some(body) = body {
        let body_bytes = serde_json::to_vec(body)?;
        Request::builder()
            .method(method)
            .uri(path)
            .header("Host", "localhost")
            .header("Content-Type", "application/json")
            .body(Full::new(Bytes::from(body_bytes)).map_err(|e| match e {}).boxed())
            .context("build request")?
    } else {
        Request::builder()
            .method(method)
            .uri(path)
            .header("Host", "localhost")
            .body(Empty::<Bytes>::new().map_err(|e| match e {}).boxed())
            .context("build request")?
    };

    let response = tokio::time::timeout(timeout, sender.send_request(request))
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "vmm API timeout: {} {} did not respond within {:.1}s",
                method,
                path,
                timeout.as_secs_f64(),
            )
        })?
        .with_context(|| format!("vmm API request {} {}", method, path))?;

    let status = response.status();
    if !status.is_success() {
        let body_bytes = response
            .into_body()
            .collect()
            .await
            .map(|c| c.to_bytes())
            .unwrap_or_default();
        let body_str = String::from_utf8_lossy(&body_bytes);
        anyhow::bail!(
            "vmm API error on {} {}: {} {}",
            method,
            path,
            status,
            body_str.trim(),
        );
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
