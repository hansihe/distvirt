//! Network device helpers: TAP, TUN, packet sockets, interface configuration.

use std::ffi::CStr;
use std::fs::OpenOptions;
use std::io;
use std::mem::ManuallyDrop;
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd};

use anyhow::{Context, bail};
use tokio::io::unix::AsyncFd;

use super::fd;

// ---------------------------------------------------------------------------
// Ioctl constants from <linux/if_tun.h> and <linux/sockios.h>
// ---------------------------------------------------------------------------

const TUNSETIFF: libc::c_ulong = 0x400454ca;
const TUNSETPERSIST: libc::c_ulong = 0x400454cb;
const TUNSETOFFLOAD: libc::c_ulong = 0x400454d0;

const IFF_TUN: libc::c_short = 0x0001;
const IFF_TAP: libc::c_short = 0x0002;
const IFF_NO_PI: libc::c_short = 0x1000;
const IFF_VNET_HDR: libc::c_short = 0x4000;

const TUN_F_CSUM: libc::c_uint = 0x01;

const SIOCGIFFLAGS: libc::c_ulong = 0x8913;
const SIOCSIFFLAGS: libc::c_ulong = 0x8914;
const SIOCSIFADDR: libc::c_ulong = 0x8916;
const SIOCSIFNETMASK: libc::c_ulong = 0x891c;

const PACKET_VNET_HDR: libc::c_int = 15;
const PACKET_IGNORE_OUTGOING: libc::c_int = 23;

// ---------------------------------------------------------------------------
// Ifreq for TUNSETIFF (simplified layout, only needs name + flags)
// ---------------------------------------------------------------------------

#[repr(C)]
struct TunIfreq {
    ifr_name: [u8; libc::IFNAMSIZ],
    ifr_flags: libc::c_short,
    _pad: [u8; 22],
}

/// Parse an interface name from an ifreq `ifr_name` field.
fn parse_ifr_name(ifr_name: &[u8; libc::IFNAMSIZ]) -> anyhow::Result<String> {
    CStr::from_bytes_until_nul(ifr_name)
        .context("parse interface name")?
        .to_str()
        .context("interface name not utf8")
        .map(|s| s.to_string())
}

/// Copy an interface name into an ifreq `ifr_name` field.
fn write_ifr_name(ifr_name: &mut [libc::c_char; libc::IFNAMSIZ], name: &str) {
    let name_bytes = name.as_bytes();
    let copy_len = name_bytes.len().min(libc::IFNAMSIZ - 1);
    unsafe {
        std::ptr::copy_nonoverlapping(
            name_bytes.as_ptr(),
            ifr_name.as_mut_ptr() as *mut u8,
            copy_len,
        );
    }
}

// ---------------------------------------------------------------------------
// TunDevice — async TUN device with owned fd
// ---------------------------------------------------------------------------

/// An async TUN device for IP-level traffic with vnet header support.
///
/// Owns the TUN file descriptor and provides async read/write methods.
pub struct TunDevice {
    fd: AsyncFd<OwnedFd>,
    name: String,
}

impl TunDevice {
    /// Create a new TUN device with vnet header support.
    ///
    /// The device is set to non-blocking mode and wrapped in `AsyncFd`
    /// for use with tokio.
    pub fn create() -> anyhow::Result<Self> {
        let (tun_fd, name) = create_tun()?;
        fd::set_nonblocking(&tun_fd)?;
        let fd = AsyncFd::new(tun_fd).context("AsyncFd::new for TUN")?;
        Ok(TunDevice { fd, name })
    }

    /// Get the TUN device interface name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Configure IP address and netmask on this TUN device.
    pub fn configure_ip(&self, ip: [u8; 4], netmask: [u8; 4]) -> anyhow::Result<()> {
        configure_interface_ip(&self.name, ip, netmask)
    }

    /// Bring this TUN device's interface UP.
    pub fn bring_up(&self) -> anyhow::Result<()> {
        bring_interface_up(&self.name)
    }

    /// Async read from the TUN device.
    pub async fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            let mut guard = self.fd.readable().await?;

            match guard.try_io(|inner| {
                let raw = inner.as_raw_fd();
                let n =
                    unsafe { libc::read(raw, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
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

    /// Async write to the TUN device.
    pub async fn write(&self, buf: &[u8]) -> io::Result<usize> {
        loop {
            let mut guard = self.fd.writable().await?;

            match guard.try_io(|inner| {
                let raw = inner.as_raw_fd();
                let n = unsafe { libc::write(raw, buf.as_ptr() as *const libc::c_void, buf.len()) };
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

// ---------------------------------------------------------------------------
// PersistentTap — persistent TAP device (for Firecracker)
// ---------------------------------------------------------------------------

/// A persistent TAP device that Firecracker can open by name.
///
/// The TAP is destroyed when dropped (unless ownership is transferred
/// via [`into_packet_socket`](PersistentTap::into_packet_socket)).
pub struct PersistentTap {
    name: String,
}

impl PersistentTap {
    /// Create a new persistent TAP device.
    ///
    /// Requires `CAP_NET_ADMIN` or root.
    pub fn create() -> anyhow::Result<Self> {
        let name = create_persistent_tap()?;
        Ok(PersistentTap { name })
    }

    /// Get the TAP interface name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Bring this TAP device's interface UP.
    pub fn bring_up(&self) -> anyhow::Result<()> {
        bring_interface_up(&self.name)
    }

    /// Convert this TAP device into an async `PacketSocket`.
    ///
    /// Opens an AF_PACKET socket bound to the TAP interface. The TAP
    /// cleanup responsibility is transferred to the returned `PacketSocket`.
    pub fn into_packet_socket(self) -> anyhow::Result<PacketSocket> {
        let ifindex = get_ifindex(&self.name).context("get TAP ifindex")?;
        let socket_fd = open_packet_socket(ifindex)?;
        fd::set_nonblocking(&socket_fd)?;
        let async_fd = AsyncFd::new(socket_fd).context("AsyncFd::new for packet socket")?;

        // Transfer cleanup responsibility to PacketSocket — don't run our Drop.
        let this = ManuallyDrop::new(self);
        let tap_name = unsafe { std::ptr::read(&this.name) };

        log::info!(
            "opened AF_PACKET socket on {} (ifindex={})",
            tap_name,
            ifindex
        );
        Ok(PacketSocket {
            fd: async_fd,
            tap_name,
        })
    }
}

impl Drop for PersistentTap {
    fn drop(&mut self) {
        if let Err(e) = destroy_persistent_tap(&self.name) {
            log::warn!("failed to destroy TAP device {}: {:#}", self.name, e);
        }
    }
}

// ---------------------------------------------------------------------------
// PacketSocket — async AF_PACKET socket bound to a TAP
// ---------------------------------------------------------------------------

/// An async AF_PACKET socket bound to a persistent TAP device.
///
/// Provides async recv/send for L2 frames. Destroys the underlying
/// TAP device on drop.
pub struct PacketSocket {
    fd: AsyncFd<OwnedFd>,
    tap_name: String,
}

impl PacketSocket {
    /// Get the TAP interface name this socket is bound to.
    pub fn tap_name(&self) -> &str {
        &self.tap_name
    }

    /// Async receive from the AF_PACKET socket.
    pub async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            let mut guard = self.fd.readable().await?;

            match guard.try_io(|inner| {
                let raw = inner.as_raw_fd();
                let n =
                    unsafe { libc::recv(raw, buf.as_mut_ptr() as *mut libc::c_void, buf.len(), 0) };
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

    /// Async send to the AF_PACKET socket.
    pub async fn send(&self, buf: &[u8]) -> io::Result<usize> {
        loop {
            let mut guard = self.fd.writable().await?;

            match guard.try_io(|inner| {
                let raw = inner.as_raw_fd();
                let n =
                    unsafe { libc::send(raw, buf.as_ptr() as *const libc::c_void, buf.len(), 0) };
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

impl Drop for PacketSocket {
    fn drop(&mut self) {
        if let Err(e) = destroy_persistent_tap(&self.tap_name) {
            log::warn!("failed to destroy TAP device {}: {:#}", self.tap_name, e);
        }
    }
}

// ---------------------------------------------------------------------------
// TAP devices (internal helpers)
// ---------------------------------------------------------------------------

/// Create a persistent TAP device and return its name.
fn create_persistent_tap() -> anyhow::Result<String> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/net/tun")
        .context("open /dev/net/tun")?;

    let mut ifr = TunIfreq {
        ifr_name: [0u8; libc::IFNAMSIZ],
        ifr_flags: IFF_TAP | IFF_NO_PI,
        _pad: [0u8; 22],
    };

    let ret = unsafe { libc::ioctl(file.as_raw_fd(), TUNSETIFF as _, &mut ifr as *mut TunIfreq) };
    if ret < 0 {
        bail!("TUNSETIFF ioctl: {}", io::Error::last_os_error());
    }

    let ret = unsafe { libc::ioctl(file.as_raw_fd(), TUNSETPERSIST as _, 1 as libc::c_int) };
    if ret < 0 {
        bail!("TUNSETPERSIST ioctl: {}", io::Error::last_os_error());
    }

    let name = parse_ifr_name(&ifr.ifr_name)?;
    log::info!("created persistent TAP device: {}", name);
    Ok(name)
}

/// Destroy a persistent TAP device by clearing the persist flag.
fn destroy_persistent_tap(name: &str) -> anyhow::Result<()> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/net/tun")
        .context("open /dev/net/tun")?;

    let mut ifr = TunIfreq {
        ifr_name: [0u8; libc::IFNAMSIZ],
        ifr_flags: IFF_TAP | IFF_NO_PI,
        _pad: [0u8; 22],
    };

    let name_bytes = name.as_bytes();
    let copy_len = name_bytes.len().min(libc::IFNAMSIZ - 1);
    ifr.ifr_name[..copy_len].copy_from_slice(&name_bytes[..copy_len]);

    let ret = unsafe { libc::ioctl(file.as_raw_fd(), TUNSETIFF as _, &mut ifr as *mut TunIfreq) };
    if ret < 0 {
        bail!(
            "TUNSETIFF ioctl (destroy {}): {}",
            name,
            io::Error::last_os_error()
        );
    }

    let ret = unsafe { libc::ioctl(file.as_raw_fd(), TUNSETPERSIST as _, 0 as libc::c_int) };
    if ret < 0 {
        bail!("TUNSETPERSIST(0) ioctl: {}", io::Error::last_os_error());
    }

    log::info!("destroyed TAP device: {}", name);
    Ok(())
}

// ---------------------------------------------------------------------------
// TUN devices (internal helper)
// ---------------------------------------------------------------------------

/// Create a TUN device with vnet header support for checksum offloading.
/// Returns the fd and interface name.
fn create_tun() -> anyhow::Result<(OwnedFd, String)> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/net/tun")
        .context("open /dev/net/tun")?;

    let mut ifr = TunIfreq {
        ifr_name: [0u8; libc::IFNAMSIZ],
        ifr_flags: IFF_TUN | IFF_NO_PI | IFF_VNET_HDR,
        _pad: [0u8; 22],
    };

    let ret = unsafe { libc::ioctl(file.as_raw_fd(), TUNSETIFF as _, &mut ifr as *mut TunIfreq) };
    if ret < 0 {
        bail!("TUNSETIFF ioctl: {}", io::Error::last_os_error());
    }

    // Enable TUN_F_CSUM so the kernel handles checksum completion for
    // outgoing packets with VIRTIO_NET_HDR_F_NEEDS_CSUM set.
    let ret = unsafe {
        libc::ioctl(
            file.as_raw_fd(),
            TUNSETOFFLOAD as _,
            TUN_F_CSUM as libc::c_ulong,
        )
    };
    if ret < 0 {
        bail!("TUNSETOFFLOAD ioctl: {}", io::Error::last_os_error());
    }

    let name = parse_ifr_name(&ifr.ifr_name)?;
    let fd = unsafe { OwnedFd::from_raw_fd(file.into_raw_fd()) };

    Ok((fd, name))
}

// ---------------------------------------------------------------------------
// Interface configuration (internal helpers)
// ---------------------------------------------------------------------------

/// Bring a network interface UP using SIOCSIFFLAGS.
fn bring_interface_up(name: &str) -> anyhow::Result<()> {
    let sock = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0) };
    if sock < 0 {
        bail!("socket(AF_INET): {}", io::Error::last_os_error());
    }
    let sock = unsafe { OwnedFd::from_raw_fd(sock) };

    let mut ifr: libc::ifreq = unsafe { std::mem::zeroed() };
    write_ifr_name(&mut ifr.ifr_name, name);

    let ret = unsafe { libc::ioctl(sock.as_raw_fd(), SIOCGIFFLAGS as _, &mut ifr) };
    if ret < 0 {
        bail!("SIOCGIFFLAGS({}): {}", name, io::Error::last_os_error());
    }

    unsafe {
        ifr.ifr_ifru.ifru_flags |= (libc::IFF_UP | libc::IFF_RUNNING) as libc::c_short;
    }

    let ret = unsafe { libc::ioctl(sock.as_raw_fd(), SIOCSIFFLAGS as _, &ifr) };
    if ret < 0 {
        bail!("SIOCSIFFLAGS({}): {}", name, io::Error::last_os_error());
    }

    log::info!("brought interface {} up", name);
    Ok(())
}

/// Get the interface index for a named network interface.
fn get_ifindex(name: &str) -> anyhow::Result<i32> {
    let name_c = std::ffi::CString::new(name)?;
    let idx = unsafe { libc::if_nametoindex(name_c.as_ptr()) };
    if idx == 0 {
        bail!("if_nametoindex({}): {}", name, io::Error::last_os_error());
    }
    Ok(idx as i32)
}

/// Configure IP address and netmask on a network interface.
fn configure_interface_ip(name: &str, ip: [u8; 4], netmask: [u8; 4]) -> anyhow::Result<()> {
    let sock = open_inet_dgram_socket()?;
    set_ifaddr(sock.as_raw_fd(), name, SIOCSIFADDR, ip)?;
    set_ifaddr(sock.as_raw_fd(), name, SIOCSIFNETMASK, netmask)?;
    Ok(())
}

fn open_inet_dgram_socket() -> anyhow::Result<OwnedFd> {
    let sock = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0) };
    if sock < 0 {
        bail!("socket(AF_INET): {}", io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(sock) })
}

fn set_ifaddr(
    sock_fd: i32,
    name: &str,
    ioctl_cmd: libc::c_ulong,
    addr: [u8; 4],
) -> anyhow::Result<()> {
    let mut ifr: libc::ifreq = unsafe { std::mem::zeroed() };
    write_ifr_name(&mut ifr.ifr_name, name);

    let mut sin: libc::sockaddr_in = unsafe { std::mem::zeroed() };
    sin.sin_family = libc::AF_INET as libc::sa_family_t;
    sin.sin_addr.s_addr = u32::from_ne_bytes(addr);

    unsafe {
        std::ptr::copy_nonoverlapping(
            &sin as *const libc::sockaddr_in as *const u8,
            &mut ifr.ifr_ifru as *mut _ as *mut u8,
            std::mem::size_of::<libc::sockaddr_in>(),
        );
    }

    let ret = unsafe { libc::ioctl(sock_fd, ioctl_cmd as _, &ifr) };
    if ret < 0 {
        bail!(
            "ioctl 0x{:x} on {}: {}",
            ioctl_cmd,
            name,
            io::Error::last_os_error()
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// AF_PACKET sockets (internal helper)
// ---------------------------------------------------------------------------

/// Open an AF_PACKET socket bound to an interface for L2 frame I/O.
fn open_packet_socket(ifindex: i32) -> anyhow::Result<OwnedFd> {
    let fd = unsafe {
        libc::socket(
            libc::AF_PACKET,
            libc::SOCK_RAW | libc::SOCK_CLOEXEC,
            (libc::ETH_P_ALL as u16).to_be() as libc::c_int,
        )
    };
    if fd < 0 {
        bail!("socket(AF_PACKET): {}", io::Error::last_os_error());
    }

    let socket = unsafe { OwnedFd::from_raw_fd(fd) };

    // Bind to the specific interface.
    let mut sll: libc::sockaddr_ll = unsafe { std::mem::zeroed() };
    sll.sll_family = libc::AF_PACKET as u16;
    sll.sll_protocol = (libc::ETH_P_ALL as u16).to_be();
    sll.sll_ifindex = ifindex;

    let ret = unsafe {
        libc::bind(
            socket.as_raw_fd(),
            &sll as *const libc::sockaddr_ll as *const libc::sockaddr,
            std::mem::size_of::<libc::sockaddr_ll>() as libc::socklen_t,
        )
    };
    if ret < 0 {
        bail!(
            "bind AF_PACKET to ifindex {}: {}",
            ifindex,
            io::Error::last_os_error()
        );
    }

    // Enable PACKET_VNET_HDR for virtio-net header on recv/send.
    setsockopt_int(socket.as_raw_fd(), libc::SOL_PACKET, PACKET_VNET_HDR, 1)
        .with_context(|| format!("setsockopt PACKET_VNET_HDR on ifindex {}", ifindex))?;

    // Ignore outgoing frames on recv path (non-fatal on older kernels).
    if let Err(e) = setsockopt_int(
        socket.as_raw_fd(),
        libc::SOL_PACKET,
        PACKET_IGNORE_OUTGOING,
        1,
    ) {
        log::warn!(
            "setsockopt PACKET_IGNORE_OUTGOING on ifindex {}: {} (kernel may be too old)",
            ifindex,
            e
        );
    }

    log::info!(
        "opened AF_PACKET socket on ifindex {} with PACKET_VNET_HDR",
        ifindex
    );
    Ok(socket)
}

fn setsockopt_int(fd: i32, level: i32, optname: i32, val: i32) -> io::Result<()> {
    let ret = unsafe {
        libc::setsockopt(
            fd,
            level,
            optname,
            &val as *const libc::c_int as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
