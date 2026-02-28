use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

use tokio::io::unix::AsyncFd;

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
