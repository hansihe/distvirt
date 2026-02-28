pub mod firecracker;

use std::future::Future;
use std::path::PathBuf;

use tokio::net::UnixStream;

use crate::tap::TapDevice;

/// Network configuration for a VM.
#[derive(Clone)]
pub struct NetConfig {
    pub guest_ip: String,
    pub netmask: String,
    pub gateway: String,
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
}

/// A VMM implementation that can launch VMs.
#[allow(async_fn_in_trait)]
pub trait Vmm: Send + Sync {
    type Instance: VmInstance;
    async fn launch(&self, config: &VmConfig) -> anyhow::Result<Self::Instance>;
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
}
