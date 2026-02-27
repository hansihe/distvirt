use std::ffi::CStr;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

use tokio::io::unix::AsyncFd;

/// TUN device ioctl constants.
const TUNSETIFF: libc::c_ulong = 0x400454ca;
const TUNSETOFFLOAD: libc::c_ulong = 0x400454d0;
const IFF_TUN: libc::c_short = 0x0001;
const IFF_NO_PI: libc::c_short = 0x1000;
const IFF_VNET_HDR: libc::c_short = 0x4000;
const TUN_F_CSUM: libc::c_uint = 0x01;

/// Ioctl constants for setting interface address and netmask.
const SIOCSIFADDR: libc::c_ulong = 0x8916;
const SIOCSIFNETMASK: libc::c_ulong = 0x891c;

/// Ifreq for TUN ioctls.
#[repr(C)]
struct Ifreq {
    ifr_name: [u8; libc::IFNAMSIZ],
    ifr_flags: libc::c_short,
    _pad: [u8; 22],
}

/// Create a TUN device with vnet header support for checksum offloading.
/// Returns the fd and interface name.
pub fn create_tun() -> anyhow::Result<(OwnedFd, String)> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/net/tun")
        .map_err(|e| anyhow::anyhow!("open /dev/net/tun: {}", e))?;

    let mut ifr = Ifreq {
        ifr_name: [0u8; libc::IFNAMSIZ],
        ifr_flags: IFF_TUN | IFF_NO_PI | IFF_VNET_HDR,
        _pad: [0u8; 22],
    };

    let ret = unsafe { libc::ioctl(file.as_raw_fd(), TUNSETIFF as _, &mut ifr as *mut Ifreq) };
    if ret < 0 {
        return Err(anyhow::anyhow!(
            "TUNSETIFF ioctl: {}",
            io::Error::last_os_error()
        ));
    }

    // Enable checksum offloading so the kernel completes partial checksums.
    let ret = unsafe { libc::ioctl(file.as_raw_fd(), TUNSETOFFLOAD as _, TUN_F_CSUM as libc::c_ulong) };
    if ret < 0 {
        return Err(anyhow::anyhow!(
            "TUNSETOFFLOAD ioctl: {}",
            io::Error::last_os_error()
        ));
    }

    let name = CStr::from_bytes_until_nul(&ifr.ifr_name)
        .map_err(|e| anyhow::anyhow!("parse TUN device name: {}", e))?
        .to_str()
        .map_err(|e| anyhow::anyhow!("TUN device name not utf8: {}", e))?
        .to_string();

    // Convert File to OwnedFd (keep the fd open).
    let fd = unsafe { OwnedFd::from_raw_fd(file.as_raw_fd()) };
    std::mem::forget(file); // prevent double-close

    Ok((fd, name))
}

/// Configure IP address and netmask on a network interface via ioctls.
pub fn configure_tun_ip(name: &str, ip: [u8; 4], netmask: [u8; 4]) -> anyhow::Result<()> {
    let sock = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0) };
    if sock < 0 {
        return Err(anyhow::anyhow!(
            "socket(AF_INET): {}",
            io::Error::last_os_error()
        ));
    }
    let sock = unsafe { OwnedFd::from_raw_fd(sock) };

    // Set IP address.
    set_ifaddr(sock.as_raw_fd(), name, SIOCSIFADDR, ip)?;

    // Set netmask.
    set_ifaddr(sock.as_raw_fd(), name, SIOCSIFNETMASK, netmask)?;

    Ok(())
}

/// Helper for SIOCSIFADDR / SIOCSIFNETMASK ioctls.
fn set_ifaddr(
    sock_fd: i32,
    name: &str,
    ioctl_cmd: libc::c_ulong,
    addr: [u8; 4],
) -> anyhow::Result<()> {
    let mut ifr: libc::ifreq = unsafe { std::mem::zeroed() };
    let name_bytes = name.as_bytes();
    let copy_len = name_bytes.len().min(libc::IFNAMSIZ - 1);
    unsafe {
        std::ptr::copy_nonoverlapping(
            name_bytes.as_ptr(),
            ifr.ifr_name.as_mut_ptr() as *mut u8,
            copy_len,
        );
    }

    // Build sockaddr_in.
    let mut sin: libc::sockaddr_in = unsafe { std::mem::zeroed() };
    sin.sin_family = libc::AF_INET as libc::sa_family_t;
    sin.sin_addr.s_addr = u32::from_ne_bytes(addr);

    // Copy sockaddr_in into ifr_ifru (which is a union containing sockaddr).
    unsafe {
        std::ptr::copy_nonoverlapping(
            &sin as *const libc::sockaddr_in as *const u8,
            &mut ifr.ifr_ifru as *mut _ as *mut u8,
            std::mem::size_of::<libc::sockaddr_in>(),
        );
    }

    let ret = unsafe { libc::ioctl(sock_fd, ioctl_cmd as _, &ifr) };
    if ret < 0 {
        return Err(anyhow::anyhow!(
            "ioctl 0x{:x} on {}: {}",
            ioctl_cmd,
            name,
            io::Error::last_os_error()
        ));
    }

    Ok(())
}

/// Async read from TUN fd.
pub async fn tun_read(tun: &AsyncFd<OwnedFd>, buf: &mut [u8]) -> io::Result<usize> {
    loop {
        let mut guard = tun.readable().await?;

        match guard.try_io(|inner| {
            let fd = inner.as_raw_fd();
            let n = unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
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

/// Async write to TUN fd.
pub async fn tun_write(tun: &AsyncFd<OwnedFd>, buf: &[u8]) -> io::Result<usize> {
    loop {
        let mut guard = tun.writable().await?;

        match guard.try_io(|inner| {
            let fd = inner.as_raw_fd();
            let n =
                unsafe { libc::write(fd, buf.as_ptr() as *const libc::c_void, buf.len()) };
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
