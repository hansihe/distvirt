use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;

use crate::memory::init::read_cmdline_param;

/// How the guest should shut down.
#[derive(Debug, Clone)]
pub enum ShutdownMode {
    /// ACPI power-off (for VMMs like Cloud Hypervisor).
    PowerOff,
    /// Reboot/triple-fault (needed for Firecracker which doesn't support ACPI).
    Reboot,
}

/// Transport configuration for the host↔guest connection.
#[derive(Debug, Clone)]
pub enum TransportConfig {
    Vsock { port: u32 },
    VirtioSerial { device: Option<PathBuf> },
}

/// Unified configuration for guest-init, replacing scattered /proc/cmdline reads.
#[derive(Debug, Clone)]
pub struct GuestConfig {
    /// Initial balloon size in MiB (None = memory management disabled).
    pub balloon_mib: Option<u32>,
    /// Transport to use for host communication.
    pub transport: TransportConfig,
    /// Config drive block device path (None = no pre-vsock config).
    pub config_device: Option<PathBuf>,
    /// How to shut down the VM.
    pub shutdown_mode: ShutdownMode,
    /// Timeout for SIGTERM during graceful container shutdown.
    pub shutdown_timeout: Duration,
    /// Timeout for SIGKILL after SIGTERM timeout expires.
    pub shutdown_kill_timeout: Duration,
}

impl GuestConfig {
    /// Parse configuration from /proc/cmdline (production path).
    pub fn from_cmdline() -> anyhow::Result<GuestConfig> {
        let transport = match read_cmdline_param("distvirt.transport").as_deref() {
            Some("virtio-serial") => TransportConfig::VirtioSerial {
                device: read_cmdline_param("distvirt.transport_device").map(PathBuf::from),
            },
            _ => TransportConfig::Vsock {
                port: distvirt_guest_protocol::VSOCK_CONTROL_PORT,
            },
        };

        let balloon_mib = match read_cmdline_param("distvirt.balloon_mib") {
            Some(v) => {
                let mib = v
                    .parse::<u32>()
                    .with_context(|| format!("parse distvirt.balloon_mib={:?}", v))?;
                Some(mib)
            }
            None => None,
        };

        let config_device = read_cmdline_param("distvirt.config_device").map(PathBuf::from);

        let shutdown_mode = match read_cmdline_param("distvirt.shutdown").as_deref() {
            Some("poweroff") => ShutdownMode::PowerOff,
            _ => ShutdownMode::Reboot,
        };

        Ok(GuestConfig {
            balloon_mib,
            transport,
            config_device,
            shutdown_mode,
            shutdown_timeout: Duration::from_secs(2),
            shutdown_kill_timeout: Duration::from_millis(200),
        })
    }
}
