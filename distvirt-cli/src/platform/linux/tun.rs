use std::ffi::CStr;
use std::fs::OpenOptions;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

use anyhow::{bail, Context};
use tokio::io::unix::AsyncFd;

const TUNSETIFF: libc::c_ulong = 0x400454ca;
const IFF_TUN: libc::c_short = 0x0001;
const IFF_NO_PI: libc::c_short = 0x1000;

#[repr(C)]
struct Ifreq {
    ifr_name: [u8; libc::IFNAMSIZ],
    ifr_flags: libc::c_short,
    _pad: [u8; 22],
}

/// A non-persistent TUN device for L3 packet I/O.
///
/// The device is destroyed when the fd is closed (i.e. when this struct is dropped).
/// Async reads/writes use `tokio::io::unix::AsyncFd`.
pub struct TunDevice {
    async_fd: AsyncFd<OwnedFd>,
    pub name: String,
}

impl TunDevice {
    /// Create a new TUN device with a kernel-assigned name.
    pub fn create() -> anyhow::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/net/tun")
            .context("open /dev/net/tun")?;

        let mut ifr = Ifreq {
            ifr_name: [0u8; libc::IFNAMSIZ],
            ifr_flags: IFF_TUN | IFF_NO_PI,
            _pad: [0u8; 22],
        };

        let ret =
            unsafe { libc::ioctl(file.as_raw_fd(), TUNSETIFF as _, &mut ifr as *mut Ifreq) };
        if ret < 0 {
            return Err(io::Error::last_os_error()).context("TUNSETIFF ioctl");
        }

        let name = CStr::from_bytes_until_nul(&ifr.ifr_name)
            .context("parse tun device name")?
            .to_str()
            .context("tun device name not utf8")?
            .to_string();

        // Set fd non-blocking for async I/O.
        let raw_fd = file.as_raw_fd();
        let flags = unsafe { libc::fcntl(raw_fd, libc::F_GETFL) };
        if flags < 0 {
            bail!("fcntl F_GETFL: {}", io::Error::last_os_error());
        }
        let ret = unsafe { libc::fcntl(raw_fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
        if ret < 0 {
            bail!("fcntl F_SETFL O_NONBLOCK: {}", io::Error::last_os_error());
        }

        // Consume the File into an OwnedFd without closing.
        let owned_fd = unsafe { OwnedFd::from_raw_fd(raw_fd) };
        std::mem::forget(file);

        let async_fd = AsyncFd::new(owned_fd).context("AsyncFd::new")?;

        log::info!("created TUN device: {}", name);
        Ok(TunDevice { async_fd, name })
    }

    /// Read a single IP packet from the TUN device.
    pub async fn read_packet(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            let mut guard = self.async_fd.readable().await?;
            match guard.try_io(|fd| {
                let n = unsafe {
                    libc::read(
                        fd.as_raw_fd(),
                        buf.as_mut_ptr() as *mut libc::c_void,
                        buf.len(),
                    )
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

    /// Write a single IP packet to the TUN device.
    pub async fn write_packet(&self, buf: &[u8]) -> io::Result<usize> {
        loop {
            let mut guard = self.async_fd.writable().await?;
            match guard.try_io(|fd| {
                let n = unsafe {
                    libc::write(
                        fd.as_raw_fd(),
                        buf.as_ptr() as *const libc::c_void,
                        buf.len(),
                    )
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
