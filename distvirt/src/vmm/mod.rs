pub mod firecracker;

use std::os::unix::net::UnixStream;
use std::path::PathBuf;

/// Configuration for launching a VM.
pub struct VmConfig {
    pub kernel_path: PathBuf,
    pub rootfs_image_path: PathBuf,
    pub container_image_path: PathBuf,
    pub vcpu_count: u32,
    pub mem_size_mib: u32,
}

/// A VMM implementation that can launch VMs.
pub trait Vmm {
    type Instance: VmInstance;
    fn launch(&self, config: &VmConfig) -> anyhow::Result<Self::Instance>;
}

/// A running VM instance.
pub trait VmInstance {
    /// Connect to the guest's vsock on the given port.
    fn connect_vsock(&self, port: u32) -> anyhow::Result<UnixStream>;
    /// Wait for the VM process to exit.
    fn wait(&mut self) -> anyhow::Result<()>;
    /// Kill the VM process.
    fn kill(&mut self) -> anyhow::Result<()>;
}
