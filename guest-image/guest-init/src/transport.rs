use std::os::unix::io::AsRawFd;
use std::path::PathBuf;

use anyhow::{Context, bail};

use crate::vsock::VsockListener;

/// Abstraction over the host↔guest transport.
///
/// With Firecracker, guest-init listens on a vsock port and the host connects.
/// With QEMU (virtio-serial), guest-init opens a virtio-serial port device
/// and the host connects to the corresponding QEMU chardev unix socket.
///
/// In both cases, `accept()` returns a file descriptor suitable for yamux.
pub enum TransportListener {
    Vsock(VsockListener),
    VirtioSerial { path: PathBuf },
}

impl TransportListener {
    /// Wait for a host connection and return the connected stream as a File.
    ///
    /// For vsock, this blocks until a new connection arrives (supports
    /// reconnect after suspend/resume). For virtio-serial, this opens the
    /// device file (the host connects to the QEMU chardev socket).
    pub async fn accept(&self) -> anyhow::Result<std::fs::File> {
        match self {
            TransportListener::Vsock(listener) => listener.accept().await,
            TransportListener::VirtioSerial { path } => {
                let file = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(path)
                    .with_context(|| format!("open virtio-serial port {}", path.display()))?;

                // Set non-blocking for async I/O.
                let fd = file.as_raw_fd();
                let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
                if flags < 0 {
                    bail!("fcntl(F_GETFL): {}", std::io::Error::last_os_error());
                }
                if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
                    bail!(
                        "fcntl(F_SETFL, O_NONBLOCK): {}",
                        std::io::Error::last_os_error()
                    );
                }

                log::info!("opened virtio-serial port {}", path.display());
                Ok(file)
            }
        }
    }
}

/// Find a virtio-serial port device by its name.
///
/// QEMU assigns names to virtio-serial ports via the `name=` property on
/// `virtserialport` devices. The kernel exposes these at
/// `/sys/class/virtio-ports/vportNpM/name`. Without udev, the corresponding
/// `/dev/virtio-ports/<name>` symlinks don't exist, so we scan sysfs to find
/// the right `/dev/vportNpM` device.
pub fn find_virtio_serial_port(port_name: &str) -> anyhow::Result<PathBuf> {
    let sysfs_dir = std::path::Path::new("/sys/class/virtio-ports");
    if !sysfs_dir.exists() {
        bail!(
            "no virtio-serial ports found (is CONFIG_VIRTIO_CONSOLE enabled?)"
        );
    }

    for entry in std::fs::read_dir(sysfs_dir).context("read /sys/class/virtio-ports")? {
        let entry = entry?;
        let name_path = entry.path().join("name");
        if let Ok(name) = std::fs::read_to_string(&name_path) {
            if name.trim() == port_name {
                let dev_name = entry.file_name();
                let dev_path = PathBuf::from("/dev").join(&dev_name);
                log::info!(
                    "found virtio-serial port '{}' at {}",
                    port_name,
                    dev_path.display()
                );
                return Ok(dev_path);
            }
        }
    }

    bail!(
        "virtio-serial port '{}' not found in sysfs",
        port_name
    )
}
