use std::ffi::CStr;
use std::fs::OpenOptions;
use std::os::fd::{FromRawFd, OwnedFd};
use std::os::unix::io::AsRawFd;

use anyhow::{bail, Context};

/// Flags and constants for TUN/TAP devices.
const TUNSETIFF: libc::c_ulong = 0x400454ca;
const TUNSETPERSIST: libc::c_ulong = 0x400454cb;
const IFF_TAP: libc::c_short = 0x0002;
const IFF_NO_PI: libc::c_short = 0x1000;

#[repr(C)]
struct Ifreq {
    ifr_name: [u8; libc::IFNAMSIZ],
    ifr_flags: libc::c_short,
    _pad: [u8; 22],
}

/// A TAP device with an AF_PACKET socket for host-side L2 frame I/O.
///
/// The TAP device is created as persistent so Firecracker can open it by name.
/// Host code reads/writes raw Ethernet frames via the AF_PACKET socket, which
/// is bound to the TAP interface by index.
///
/// Dropped automatically: the persist flag is cleared and the device is
/// destroyed when this struct is dropped.
pub struct TapDevice {
    /// AF_PACKET socket for reading/writing L2 frames.
    pub socket: OwnedFd,
    /// The TAP interface name (e.g. "tap0").
    pub name: String,
    /// The interface index.
    pub ifindex: i32,
}

impl TapDevice {
    /// Get the raw fd of the AF_PACKET socket.
    pub fn as_raw_fd(&self) -> i32 {
        self.socket.as_raw_fd()
    }
}

impl Drop for TapDevice {
    fn drop(&mut self) {
        if let Err(e) = destroy_persistent_tap(&self.name) {
            log::warn!("failed to destroy TAP device {}: {:#}", self.name, e);
        }
    }
}

/// Create a persistent TAP device and return its name.
///
/// The device survives after the creating fd is closed, allowing Firecracker
/// to open it by name. Must be cleaned up with `destroy_persistent_tap()` or
/// by dropping the `TapDevice`.
///
/// Requires `CAP_NET_ADMIN` or root.
pub fn create_persistent_tap() -> anyhow::Result<String> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/net/tun")
        .context("open /dev/net/tun")?;

    let mut ifr = Ifreq {
        ifr_name: [0u8; libc::IFNAMSIZ],
        ifr_flags: IFF_TAP | IFF_NO_PI,
        _pad: [0u8; 22],
    };

    let ret = unsafe { libc::ioctl(file.as_raw_fd(), TUNSETIFF as _, &mut ifr as *mut Ifreq) };
    if ret < 0 {
        bail!("TUNSETIFF ioctl: {}", std::io::Error::last_os_error());
    }

    let ret = unsafe { libc::ioctl(file.as_raw_fd(), TUNSETPERSIST as _, 1 as libc::c_int) };
    if ret < 0 {
        bail!("TUNSETPERSIST ioctl: {}", std::io::Error::last_os_error());
    }

    let name = CStr::from_bytes_until_nul(&ifr.ifr_name)
        .context("parse tap device name")?
        .to_str()
        .context("tap device name not utf8")?
        .to_string();

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

    let mut ifr = Ifreq {
        ifr_name: [0u8; libc::IFNAMSIZ],
        ifr_flags: IFF_TAP | IFF_NO_PI,
        _pad: [0u8; 22],
    };

    let name_bytes = name.as_bytes();
    let copy_len = name_bytes.len().min(libc::IFNAMSIZ - 1);
    ifr.ifr_name[..copy_len].copy_from_slice(&name_bytes[..copy_len]);

    let ret = unsafe { libc::ioctl(file.as_raw_fd(), TUNSETIFF as _, &mut ifr as *mut Ifreq) };
    if ret < 0 {
        bail!(
            "TUNSETIFF ioctl (destroy {}): {}",
            name,
            std::io::Error::last_os_error()
        );
    }

    let ret = unsafe { libc::ioctl(file.as_raw_fd(), TUNSETPERSIST as _, 0 as libc::c_int) };
    if ret < 0 {
        bail!("TUNSETPERSIST(0) ioctl: {}", std::io::Error::last_os_error());
    }

    log::info!("destroyed TAP device: {}", name);
    Ok(())
}

/// Bring a network interface UP using a SIOCSIFFLAGS ioctl.
pub fn bring_interface_up(name: &str) -> anyhow::Result<()> {
    let sock = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0) };
    if sock < 0 {
        bail!("socket(AF_INET): {}", std::io::Error::last_os_error());
    }
    let sock = unsafe { OwnedFd::from_raw_fd(sock) };

    // First get current flags with SIOCGIFFLAGS.
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

    const SIOCGIFFLAGS: libc::c_ulong = 0x8913;
    const SIOCSIFFLAGS: libc::c_ulong = 0x8914;

    let ret = unsafe { libc::ioctl(sock.as_raw_fd(), SIOCGIFFLAGS as _, &mut ifr) };
    if ret < 0 {
        bail!("SIOCGIFFLAGS({}): {}", name, std::io::Error::last_os_error());
    }

    // Set IFF_UP | IFF_RUNNING.
    unsafe {
        ifr.ifr_ifru.ifru_flags |= (libc::IFF_UP | libc::IFF_RUNNING) as libc::c_short;
    }

    let ret = unsafe { libc::ioctl(sock.as_raw_fd(), SIOCSIFFLAGS as _, &ifr) };
    if ret < 0 {
        bail!("SIOCSIFFLAGS({}): {}", name, std::io::Error::last_os_error());
    }

    log::info!("brought interface {} up", name);
    Ok(())
}

/// Get the interface index for a named network interface.
fn get_ifindex(name: &str) -> anyhow::Result<i32> {
    let name_c = std::ffi::CString::new(name)?;
    let idx = unsafe { libc::if_nametoindex(name_c.as_ptr()) };
    if idx == 0 {
        bail!(
            "if_nametoindex({}): {}",
            name,
            std::io::Error::last_os_error()
        );
    }
    Ok(idx as i32)
}

/// Open an AF_PACKET socket bound to a TAP interface for L2 frame I/O.
///
/// The socket sees all Ethernet frames traversing the TAP interface:
/// - Frames written by Firecracker (guest TX) appear on the socket's RX path
/// - Frames sent via the socket are delivered to Firecracker's TAP fd (guest RX)
///
/// Requires `CAP_NET_RAW` or root.
pub fn open_packet_socket(tap_name: &str) -> anyhow::Result<TapDevice> {
    let ifindex = get_ifindex(tap_name).context("get TAP ifindex")?;

    // Create AF_PACKET, SOCK_RAW socket for all Ethernet protocols.
    let fd = unsafe {
        libc::socket(
            libc::AF_PACKET,
            libc::SOCK_RAW | libc::SOCK_CLOEXEC,
            (libc::ETH_P_ALL as u16).to_be() as libc::c_int,
        )
    };
    if fd < 0 {
        bail!("socket(AF_PACKET): {}", std::io::Error::last_os_error());
    }

    let socket = unsafe { OwnedFd::from_raw_fd(fd) };

    // Bind to the specific TAP interface.
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
            "bind AF_PACKET to {}: {}",
            tap_name,
            std::io::Error::last_os_error()
        );
    }

    // Enable PACKET_VNET_HDR so each recv/send includes a 10-byte virtio-net
    // header, preserving checksum offload metadata (NEEDS_CSUM, csum_start, etc.).
    const PACKET_VNET_HDR: libc::c_int = 15;
    let val: libc::c_int = 1;
    let ret = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_PACKET,
            PACKET_VNET_HDR,
            &val as *const libc::c_int as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if ret < 0 {
        bail!(
            "setsockopt PACKET_VNET_HDR on {}: {}",
            tap_name,
            std::io::Error::last_os_error()
        );
    }

    // Ignore outgoing frames on the recv path. Without this, frames sent via
    // this socket (e.g. DNAT'd packets forwarded to a backend) are echoed back
    // as PACKET_OUTGOING and re-enter the fabric's dispatch loop. This corrupts
    // the MAC table: the echoed frame's source MAC (belonging to a different
    // port) gets learned on this port, causing return traffic to be dropped by
    // loopback avoidance.
    const PACKET_IGNORE_OUTGOING: libc::c_int = 23;
    let val: libc::c_int = 1;
    let ret = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_PACKET,
            PACKET_IGNORE_OUTGOING,
            &val as *const libc::c_int as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if ret < 0 {
        log::warn!(
            "setsockopt PACKET_IGNORE_OUTGOING on {}: {} (kernel may be too old, falling back to no filtering)",
            tap_name,
            std::io::Error::last_os_error()
        );
    }

    log::info!(
        "opened AF_PACKET socket on {} (ifindex={}) with PACKET_VNET_HDR",
        tap_name,
        ifindex
    );
    Ok(TapDevice {
        socket,
        name: tap_name.to_string(),
        ifindex,
    })
}
