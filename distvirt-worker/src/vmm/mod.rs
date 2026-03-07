pub mod firecracker;

use std::future::Future;
use std::path::{Path, PathBuf};

use distvirt_guest_protocol::HostMessage;
use serde::{Deserialize, Serialize};
use tokio::net::UnixStream;

use crate::tap::TapDevice;

/// Network configuration for a VM.
#[derive(Clone)]
pub struct NetConfig {
    pub guest_ip: String,
    pub netmask: String,
    pub gateway: String,
    pub guest_mac: [u8; 6],
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
}

/// Metadata persisted as `metadata.json` in a snapshot directory.
///
/// Contains the source paths needed to reconstruct the VM environment on restore.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    /// Absolute path to the kernel image used for boot (needed by Firecracker restore).
    pub kernel_path: PathBuf,
    /// Absolute path to the original rootfs image (re-copied into tmpdir on restore).
    pub rootfs_source_path: PathBuf,
    /// Whether a balloon device was configured (needed for restore to enable `set_balloon`).
    #[serde(default)]
    pub balloon_configured: bool,
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
    fn launch(&self, config: &VmConfig) -> impl Future<Output = anyhow::Result<Self::Instance>> + Send;

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
    /// Get the TAP device for host-side L2 frame I/O, if networking is configured.
    fn tap(&self) -> Option<&TapDevice>;
    /// Take ownership of the TAP device, if networking is configured.
    fn take_tap(&mut self) -> Option<TapDevice>;
    /// Wait for the VM process to exit.
    fn wait(&mut self) -> impl Future<Output = anyhow::Result<()>> + Send;
    /// Kill the VM process.
    fn kill(&mut self) -> impl Future<Output = anyhow::Result<()>> + Send;

    /// Snapshot the VM to the given directory. Pauses vCPUs, writes snapshot
    /// files, and copies the container disk. The caller should kill the VM
    /// after this returns.
    fn snapshot(&mut self, snapshot_dir: &Path) -> impl Future<Output = anyhow::Result<SnapshotArtifacts>> + Send {
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
