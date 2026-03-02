use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

use tokio::io::unix::AsyncFd;
use tokio::sync::mpsc;

use crate::tap::TapDevice;

/// Unique identifier for a port within the fabric.
pub type PortId = usize;

/// Trait abstracting async L2 frame send/receive.
///
/// Implemented by `Port` for real TAP devices and by test doubles in tests.
pub trait FramePort: Send + Sync + 'static {
    fn recv_frame(&self, buf: &mut [u8]) -> impl std::future::Future<Output = io::Result<usize>> + Send;
    fn send_frame(&self, buf: &[u8]) -> impl std::future::Future<Output = io::Result<usize>> + Send;
}

/// An async L2 port wrapping a TapDevice's AF_PACKET socket.
///
/// Uses tokio's `AsyncFd` for readiness notification, then performs
/// non-blocking `recv`/`send` via libc.
pub struct Port {
    async_fd: AsyncFd<OwnedFd>,
    /// The underlying TapDevice (kept alive for Drop cleanup).
    _tap: TapDevice,
}

impl Port {
    /// Create a new async port from a TapDevice.
    ///
    /// Sets O_NONBLOCK on the socket fd before wrapping in AsyncFd.
    pub fn new(tap: TapDevice) -> io::Result<Self> {
        // Set non-blocking mode on the socket fd.
        let raw_fd = tap.socket.as_raw_fd();
        let flags = unsafe { libc::fcntl(raw_fd, libc::F_GETFL) };
        if flags < 0 {
            return Err(io::Error::last_os_error());
        }
        let ret = unsafe { libc::fcntl(raw_fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
        if ret < 0 {
            return Err(io::Error::last_os_error());
        }

        // We need to separate the OwnedFd from the TapDevice for AsyncFd,
        // but TapDevice must stay alive (it cleans up the TAP on Drop).
        // Use dup() to create a second fd for AsyncFd.
        let dup_fd = unsafe { libc::dup(raw_fd) };
        if dup_fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // Set non-blocking on the dup'd fd too.
        let flags2 = unsafe { libc::fcntl(dup_fd, libc::F_GETFL) };
        if flags2 >= 0 {
            unsafe { libc::fcntl(dup_fd, libc::F_SETFL, flags2 | libc::O_NONBLOCK) };
        }
        let owned_dup = unsafe { OwnedFd::from_raw_fd(dup_fd) };

        let async_fd = AsyncFd::new(owned_dup)?;

        Ok(Port {
            async_fd,
            _tap: tap,
        })
    }

    /// Asynchronously receive an L2 frame into `buf`.
    /// Returns the number of bytes read.
    pub async fn recv_frame(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            let mut guard = self.async_fd.readable().await?;

            match guard.try_io(|inner| {
                let fd = inner.as_raw_fd();
                let n = unsafe {
                    libc::recv(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len(), 0)
                };
                if n < 0 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(n as usize)
                }
            }) {
                Ok(result) => return result,
                Err(_would_block) => continue,
            }
        }
    }

    /// Asynchronously send an L2 frame.
    /// Returns the number of bytes written.
    pub async fn send_frame(&self, buf: &[u8]) -> io::Result<usize> {
        loop {
            let mut guard = self.async_fd.writable().await?;

            match guard.try_io(|inner| {
                let fd = inner.as_raw_fd();
                let n = unsafe {
                    libc::send(fd, buf.as_ptr() as *const libc::c_void, buf.len(), 0)
                };
                if n < 0 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(n as usize)
                }
            }) {
                Ok(result) => return result,
                Err(_would_block) => continue,
            }
        }
    }
}

impl FramePort for Port {
    async fn recv_frame(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.recv_frame(buf).await
    }

    async fn send_frame(&self, buf: &[u8]) -> io::Result<usize> {
        self.send_frame(buf).await
    }
}

/// A channel-backed L2 port for adapter virtual interfaces.
///
/// The fabric side holds a `ChannelPort`; the adapter side holds the
/// opposite ends of the mpsc channels.
pub struct ChannelPort {
    rx: tokio::sync::Mutex<mpsc::Receiver<Vec<u8>>>,
    tx: mpsc::Sender<Vec<u8>>,
}

impl ChannelPort {
    /// Create a new channel port pair.
    ///
    /// Returns `(port, adapter_tx, adapter_rx)` where:
    /// - `port` is the fabric-side `ChannelPort` (implements `FramePort`)
    /// - `adapter_tx` sends frames *into* the fabric (adapter → fabric)
    /// - `adapter_rx` receives frames *from* the fabric (fabric → adapter)
    pub fn new(buffer_size: usize) -> (Self, mpsc::Sender<Vec<u8>>, mpsc::Receiver<Vec<u8>>) {
        // adapter→fabric direction
        let (adapter_tx, fabric_rx) = mpsc::channel(buffer_size);
        // fabric→adapter direction
        let (fabric_tx, adapter_rx) = mpsc::channel(buffer_size);

        let port = ChannelPort {
            rx: tokio::sync::Mutex::new(fabric_rx),
            tx: fabric_tx,
        };

        (port, adapter_tx, adapter_rx)
    }
}

impl FramePort for ChannelPort {
    async fn recv_frame(&self, buf: &mut [u8]) -> io::Result<usize> {
        let mut rx = self.rx.lock().await;
        match rx.recv().await {
            Some(frame) => {
                let len = frame.len().min(buf.len());
                buf[..len].copy_from_slice(&frame[..len]);
                Ok(len)
            }
            None => Err(io::Error::new(io::ErrorKind::BrokenPipe, "channel closed")),
        }
    }

    async fn send_frame(&self, buf: &[u8]) -> io::Result<usize> {
        let data = buf.to_vec();
        let len = data.len();
        self.tx
            .send(data)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "channel closed"))?;
        Ok(len)
    }
}

/// Enum dispatch for fabric ports: either a real TAP or a virtual channel.
pub enum FabricPort {
    Tap(Port),
    Virtual(ChannelPort),
}

impl FramePort for FabricPort {
    async fn recv_frame(&self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            FabricPort::Tap(p) => p.recv_frame(buf).await,
            FabricPort::Virtual(p) => p.recv_frame(buf).await,
        }
    }

    async fn send_frame(&self, buf: &[u8]) -> io::Result<usize> {
        match self {
            FabricPort::Tap(p) => p.send_frame(buf).await,
            FabricPort::Virtual(p) => p.send_frame(buf).await,
        }
    }
}
