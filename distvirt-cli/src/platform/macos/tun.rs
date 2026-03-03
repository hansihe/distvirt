use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

use anyhow::{bail, Context};
use tokio::io::unix::AsyncFd;

/// A non-persistent TUN device for L3 packet I/O (macOS utun).
///
/// The device is destroyed when the fd is closed (i.e. when this struct is dropped).
/// Async reads/writes use `tokio::io::unix::AsyncFd`.
///
/// macOS utun devices prepend a 4-byte protocol header (AF_INET/AF_INET6)
/// on every read/write. This struct transparently strips/adds it so callers
/// see raw IP packets, matching the Linux TUN behaviour.
pub struct TunDevice {
    async_fd: AsyncFd<OwnedFd>,
    pub name: String,
}

// ioctl request code for CTLIOCGINFO on macOS.
const CTLIOCGINFO: libc::c_ulong = 0xc0644e03;

// PF_SYSTEM / SYSPROTO_CONTROL constants.
const PF_SYSTEM: libc::c_int = libc::AF_SYSTEM;
const SYSPROTO_CONTROL: libc::c_int = 2;
const AF_SYS_CONTROL: libc::c_int = 2;
const UTUN_CONTROL_NAME: &[u8] = b"com.apple.net.utun_control\0";
const UTUN_OPT_IFNAME: libc::c_int = 2;

/// AF_INET in network byte order as a 4-byte header.
const AF_INET_HDR: [u8; 4] = [0, 0, 0, 2];

#[repr(C)]
struct CtlInfo {
    ctl_id: u32,
    ctl_name: [u8; 96],
}

#[repr(C)]
struct SockaddrCtl {
    sc_len: u8,
    sc_family: u8,
    ss_sysaddr: u16,
    sc_id: u32,
    sc_unit: u32,
    sc_reserved: [u32; 5],
}

impl TunDevice {
    /// Create a new utun device with a kernel-assigned name.
    pub fn create() -> anyhow::Result<Self> {
        // 1. Open PF_SYSTEM / SYSPROTO_CONTROL socket.
        let fd = unsafe { libc::socket(PF_SYSTEM, libc::SOCK_DGRAM, SYSPROTO_CONTROL) };
        if fd < 0 {
            bail!("socket(PF_SYSTEM): {}", io::Error::last_os_error());
        }
        let owned_fd = unsafe { OwnedFd::from_raw_fd(fd) };

        // 2. CTLIOCGINFO to get the control ID for utun.
        let mut info = CtlInfo {
            ctl_id: 0,
            ctl_name: [0u8; 96],
        };
        info.ctl_name[..UTUN_CONTROL_NAME.len()].copy_from_slice(UTUN_CONTROL_NAME);

        let ret = unsafe { libc::ioctl(owned_fd.as_raw_fd(), CTLIOCGINFO as _, &mut info) };
        if ret < 0 {
            bail!("CTLIOCGINFO ioctl: {}", io::Error::last_os_error());
        }

        // 3. Connect with sc_unit = 0 for auto-assignment.
        let addr = SockaddrCtl {
            sc_len: std::mem::size_of::<SockaddrCtl>() as u8,
            sc_family: libc::AF_SYSTEM as u8,
            ss_sysaddr: AF_SYS_CONTROL as u16,
            sc_id: info.ctl_id,
            sc_unit: 0,
            sc_reserved: [0; 5],
        };

        let ret = unsafe {
            libc::connect(
                owned_fd.as_raw_fd(),
                &addr as *const SockaddrCtl as *const libc::sockaddr,
                std::mem::size_of::<SockaddrCtl>() as libc::socklen_t,
            )
        };
        if ret < 0 {
            bail!("connect(utun): {}", io::Error::last_os_error());
        }

        // 4. Get the assigned interface name.
        let mut name_buf = [0u8; libc::IFNAMSIZ];
        let mut name_len: libc::socklen_t = name_buf.len() as libc::socklen_t;
        let ret = unsafe {
            libc::getsockopt(
                owned_fd.as_raw_fd(),
                SYSPROTO_CONTROL,
                UTUN_OPT_IFNAME,
                name_buf.as_mut_ptr() as *mut libc::c_void,
                &mut name_len,
            )
        };
        if ret < 0 {
            bail!("getsockopt(UTUN_OPT_IFNAME): {}", io::Error::last_os_error());
        }

        let name = std::ffi::CStr::from_bytes_until_nul(&name_buf)
            .context("parse utun device name")?
            .to_str()
            .context("utun device name not utf8")?
            .to_string();

        // 5. Set non-blocking for async I/O.
        let flags = unsafe { libc::fcntl(owned_fd.as_raw_fd(), libc::F_GETFL) };
        if flags < 0 {
            bail!("fcntl F_GETFL: {}", io::Error::last_os_error());
        }
        let ret = unsafe {
            libc::fcntl(owned_fd.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK)
        };
        if ret < 0 {
            bail!("fcntl F_SETFL O_NONBLOCK: {}", io::Error::last_os_error());
        }

        let async_fd = AsyncFd::new(owned_fd).context("AsyncFd::new")?;

        log::info!("created utun device: {}", name);
        Ok(TunDevice { async_fd, name })
    }

    /// Read a single IP packet from the utun device.
    ///
    /// Strips the 4-byte protocol header that macOS prepends.
    pub async fn read_packet(&self, buf: &mut [u8]) -> io::Result<usize> {
        // We need space for the 4-byte header + the caller's buffer.
        let mut raw_buf = vec![0u8; buf.len() + 4];
        loop {
            let mut guard = self.async_fd.readable().await?;
            match guard.try_io(|fd| {
                let n = unsafe {
                    libc::read(
                        fd.as_raw_fd(),
                        raw_buf.as_mut_ptr() as *mut libc::c_void,
                        raw_buf.len(),
                    )
                };
                if n < 0 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(n as usize)
                }
            }) {
                Ok(result) => {
                    let n = result?;
                    if n <= 4 {
                        // No payload after the header — skip.
                        continue;
                    }
                    let payload_len = n - 4;
                    buf[..payload_len].copy_from_slice(&raw_buf[4..n]);
                    return Ok(payload_len);
                }
                Err(_would_block) => continue,
            }
        }
    }

    /// Write a single IP packet to the utun device.
    ///
    /// Prepends the 4-byte AF_INET header that macOS expects.
    pub async fn write_packet(&self, buf: &[u8]) -> io::Result<usize> {
        let mut raw_buf = Vec::with_capacity(4 + buf.len());
        raw_buf.extend_from_slice(&AF_INET_HDR);
        raw_buf.extend_from_slice(buf);

        loop {
            let mut guard = self.async_fd.writable().await?;
            match guard.try_io(|fd| {
                let n = unsafe {
                    libc::write(
                        fd.as_raw_fd(),
                        raw_buf.as_ptr() as *const libc::c_void,
                        raw_buf.len(),
                    )
                };
                if n < 0 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(n as usize)
                }
            }) {
                Ok(result) => {
                    let n = result?;
                    // Return the payload bytes written (excluding the 4-byte header).
                    return Ok(if n > 4 { n - 4 } else { 0 });
                }
                Err(_would_block) => continue,
            }
        }
    }
}
