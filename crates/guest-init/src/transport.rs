use std::os::unix::io::AsRawFd;
use std::path::PathBuf;

use anyhow::{Context, bail};
use async_io::Async;

use crate::vsock::VsockListener;

/// Combined trait for a bidirectional async stream.
pub trait AsyncStream: futures::io::AsyncRead + futures::io::AsyncWrite + Unpin + Send {}

/// Blanket impl: anything that is AsyncRead + AsyncWrite + Unpin + Send is an AsyncStream.
impl<T: futures::io::AsyncRead + futures::io::AsyncWrite + Unpin + Send> AsyncStream for T {}

/// A boxed async stream suitable for yamux.
///
/// All transport variants produce this type. Production variants wrap the raw
/// fd in `Async` inside `accept()` and box the result. The test variant
/// receives pre-wrapped streams from a channel.
pub type BoxedStream = Box<dyn AsyncStream>;

/// Abstraction over the host<->guest transport.
///
/// With Cloud Hypervisor, guest-init listens on a vsock port and the host
/// connects. With QEMU (virtio-serial), guest-init opens a virtio-serial port
/// device and the host connects to the corresponding QEMU chardev unix socket.
/// In tests, pre-connected streams are provided via a channel.
///
/// In all cases, `accept()` returns an `AsyncStream` ready for yamux.
pub enum TransportListener {
    Vsock(VsockListener),
    VirtioSerial { path: PathBuf },
    /// Channel of pre-connected streams for testing.
    /// Supports multiple `accept()` calls for reconnection testing.
    Test(async_channel::Receiver<BoxedStream>),
}

impl TransportListener {
    /// Wait for a host connection and return an async stream ready for yamux.
    ///
    /// Production variants open the fd, wrap it in `Async`, and box the result.
    /// The test variant receives pre-wrapped streams from the channel.
    pub async fn accept(&self) -> anyhow::Result<BoxedStream> {
        match self {
            TransportListener::Vsock(listener) => {
                let file = listener.accept().await?;
                let async_file = Async::new(file).context("wrap vsock fd in Async")?;
                Ok(Box::new(async_file) as BoxedStream)
            }
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
                let async_file = Async::new(file).context("wrap virtio-serial fd in Async")?;
                Ok(Box::new(async_file))
            }
            TransportListener::Test(rx) => {
                rx.recv()
                    .await
                    .map_err(|_| anyhow::anyhow!("test transport channel closed"))
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
