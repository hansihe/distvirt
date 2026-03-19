pub mod firecracker;
pub mod guest_sim;
pub mod qemu;
pub mod test_vmm;

use std::future::Future;
use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::time::Duration;

use anyhow::Context;
use distvirt_guest_protocol::HostMessage;
use distvirt_worker_protocol::PodNetworkConfig;
use serde::{Deserialize, Serialize};
use tokio::net::UnixStream;
use tokio::sync::watch;

use crate::fabric::FabricPort;
use crate::task_handle::TaskHandle;

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
    /// Size of the balloon in MiB (memory reclaimed from the guest).
    pub amount_mib: u32,
    /// Allow the guest to deflate the balloon on OOM.
    pub deflate_on_oom: bool,
    /// Interval in seconds for balloon statistics polling (0 = disabled).
    pub stats_polling_interval_s: u32,
}

/// An additional block device to attach to the VM (for volumes).
#[derive(Clone, Debug)]
pub struct AdditionalDrive {
    pub drive_id: String,
    pub image_path: PathBuf,
    pub read_only: bool,
}

/// Configuration for launching a VM.
pub struct VmConfig {
    pub kernel_path: PathBuf,
    pub rootfs_image_path: PathBuf,
    pub container_image_path: PathBuf,
    pub vcpu_count: u32,
    pub mem_size_mib: u32,
    pub net: Option<NetConfig>,
    /// If true, forward the VM serial console (kernel boot logs) to the host log at debug level.
    pub serial_console: bool,
    /// Optional balloon device for memory overcommit.
    pub balloon: Option<BalloonConfig>,
    /// Commands to bake into a config drive for pre-vsock execution.
    /// When non-empty, a config drive image is created and attached to the VM.
    pub initial_commands: Vec<HostMessage>,
    /// Additional block devices to attach (volume images).
    pub additional_drives: Vec<AdditionalDrive>,
}

/// Metadata persisted as `metadata.json` in a snapshot directory.
///
/// Contains the source paths needed to reconstruct the VM environment on restore.
/// Volume drive info persisted in snapshot metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotVolumeDrive {
    pub filename: String,
    pub read_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    /// Absolute path to the kernel image used for boot (needed by Firecracker restore).
    pub kernel_path: PathBuf,
    /// Absolute path to the original rootfs image (re-copied into tmpdir on restore).
    pub rootfs_source_path: PathBuf,
    /// Whether a balloon device was configured (needed for restore to enable `set_balloon`).
    #[serde(default)]
    pub balloon_configured: bool,
    /// Whether serial console output was enabled (needed for restore to pipe stdout).
    #[serde(default)]
    pub serial_console: bool,
    /// Volume drives attached to the VM (needed for snapshot/restore).
    #[serde(default)]
    pub volume_drives: Vec<SnapshotVolumeDrive>,
}

/// Artifacts produced by a VM snapshot.
///
/// The snapshot directory has this layout:
/// ```text
/// <snapshot_dir>/
///   metadata.json   # SnapshotMetadata
///   snapshot.bin    # Firecracker device state
///   mem.bin         # VM memory dump
///   container.ext4  # Container drive with runtime writes
/// ```
pub struct SnapshotArtifacts {
    /// Path to the snapshot directory.
    pub snapshot_dir: PathBuf,
    /// Deserialized metadata from `metadata.json`.
    pub metadata: SnapshotMetadata,
}

/// A VMM implementation that can launch VMs.
pub trait Vmm: Send + Sync {
    type Instance: VmInstance;
    fn launch(
        &self,
        config: &VmConfig,
    ) -> impl Future<Output = anyhow::Result<Self::Instance>> + Send;

    /// Restore a VM from a snapshot. The `net` config provides the network
    /// parameters for the restored instance (fresh TAP, potentially new IP).
    fn restore(
        &self,
        snapshot: &SnapshotArtifacts,
        net: Option<&NetConfig>,
    ) -> impl Future<Output = anyhow::Result<Self::Instance>> + Send {
        let _ = (snapshot, net);
        async { anyhow::bail!("snapshot restore not supported by this VMM") }
    }
}

/// A running VM instance.
pub trait VmInstance: Send + 'static {
    /// Connect to the guest's vsock on the given port.
    fn connect_vsock(&self, port: u32) -> impl Future<Output = anyhow::Result<UnixStream>> + Send;
    /// Take the fabric port for host-side network I/O, if networking is configured.
    fn take_fabric_port(&mut self) -> Option<FabricPort>;
    /// Wait for the VM process to exit.
    fn wait(&mut self) -> impl Future<Output = anyhow::Result<ExitStatus>> + Send;
    /// Kill the VM process.
    fn kill(&mut self) -> impl Future<Output = anyhow::Result<()>> + Send;

    /// Take a signal that fires when the VM process exits.
    ///
    /// Used by callers that need to `select!` on process death alongside
    /// other futures. The receiver resolves to `Some(ExitStatus)` when
    /// the process exits.
    fn take_exit_signal(&mut self) -> Option<watch::Receiver<Option<ExitStatus>>> {
        None
    }

    /// Snapshot the VM to the given directory. Pauses vCPUs, writes snapshot
    /// files, and copies the container disk. The caller should kill the VM
    /// after this returns.
    fn snapshot(
        &mut self,
        snapshot_dir: &Path,
    ) -> impl Future<Output = anyhow::Result<SnapshotArtifacts>> + Send {
        let _ = snapshot_dir;
        async { anyhow::bail!("snapshot not supported by this VM instance") }
    }

    /// Update the balloon device size. `amount_mib` is the amount of memory
    /// to reclaim from the guest.
    fn set_balloon(&mut self, amount_mib: u32) -> impl Future<Output = anyhow::Result<()>> + Send {
        let _ = amount_mib;
        async { anyhow::bail!("balloon not supported by this VM instance") }
    }
}

// ---------------------------------------------------------------------------
// Shared utilities used by VMM backends
// ---------------------------------------------------------------------------

/// Copy a file and ensure the destination is writable.
///
/// Some VMMs need writable disk images, but the source may live in a
/// read-only location (e.g. Nix store). Each VM gets its own copy.
pub(crate) async fn copy_file_writable(src: &Path, dest: &Path) -> anyhow::Result<()> {
    tokio::fs::copy(src, dest)
        .await
        .with_context(|| format!("copy {} to {}", src.display(), dest.display()))?;
    let mut perms = tokio::fs::metadata(dest).await?.permissions();
    perms.set_readonly(false);
    tokio::fs::set_permissions(dest, perms).await?;
    Ok(())
}

/// Poll for a file to appear on disk, with a timeout.
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

/// Spawn a pidfd-based exit monitor for a child process.
/// Returns a watch receiver that fires with the exit status, plus the
/// background task handle.
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

/// Spawn a task that line-logs child stdout at debug level.
pub(crate) fn spawn_serial_task(stdout: tokio::process::ChildStdout) -> TaskHandle<()> {
    TaskHandle::spawn(async move {
        use tokio::io::{AsyncBufReadExt, BufReader};
        let mut lines = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            log::debug!("[serial] {}", line);
        }
    })
}
