use std::ffi::CStr;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd};

use tokio::io::unix::AsyncFd;

use crate::packet::{FABRIC_HDR_SZ, FLAG_NEEDS_CSUM, IP_PROTO_TCP, IP_PROTO_UDP};

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

    // Enable TUN_F_CSUM so the kernel handles checksum completion for
    // outgoing packets with VIRTIO_NET_HDR_F_NEEDS_CSUM set. Without this,
    // packets written to the TUN with partial checksums would be sent out
    // with invalid checksums.
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

    let fd = unsafe { OwnedFd::from_raw_fd(file.into_raw_fd()) };

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
    // s_addr is in network byte order, but our `addr` array is already in
    // network order ([10, 0, 0, 1] = 10.0.0.1). `from_ne_bytes` reinterprets
    // the bytes as-is on the native platform, which is correct because the
    // array is already in the wire format the kernel expects.
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
async fn tun_read(tun: &AsyncFd<OwnedFd>, buf: &mut [u8]) -> io::Result<usize> {
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
async fn tun_write(tun: &AsyncFd<OwnedFd>, buf: &[u8]) -> io::Result<usize> {
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

/// TUN-based internet egress/ingress component.
///
/// Manages a TUN device for routing pod traffic to the host network.
pub(crate) struct TunEgress {
    tun: AsyncFd<OwnedFd>,
    tun_name: String,
}

impl TunEgress {
    /// Create a new TUN egress: create TUN device, configure IP, set non-blocking.
    pub fn new(gateway_ip: [u8; 4], netmask: [u8; 4]) -> anyhow::Result<Self> {
        let (tun_fd, tun_name) = create_tun()?;
        configure_tun_ip(&tun_name, gateway_ip, netmask)?;
        crate::tap::bring_interface_up(&tun_name)?;

        // Check ip_forward.
        match std::fs::read_to_string("/proc/sys/net/ipv4/ip_forward") {
            Ok(val) if val.trim() != "1" => {
                log::warn!(
                    "gateway: /proc/sys/net/ipv4/ip_forward is '{}', NAT will not work. \
                     Run: sysctl -w net.ipv4.ip_forward=1",
                    val.trim()
                );
            }
            Err(e) => log::warn!("gateway: could not read ip_forward: {}", e),
            _ => {}
        }

        // Set non-blocking on the TUN fd.
        let raw = tun_fd.as_raw_fd();
        let flags = unsafe { libc::fcntl(raw, libc::F_GETFL) };
        if flags < 0 {
            return Err(io::Error::last_os_error().into());
        }
        if unsafe { libc::fcntl(raw, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
            return Err(io::Error::last_os_error().into());
        }
        let async_fd = AsyncFd::new(tun_fd)?;

        log::info!("gateway: created TUN device {}", tun_name);

        Ok(TunEgress {
            tun: async_fd,
            tun_name,
        })
    }

    /// Size of the kernel virtio-net header used by TUN devices.
    const VNET_HDR_SZ: usize = 10;

    /// Write an egress frame to the TUN device.
    ///
    /// Converts `[fabric_hdr(3)][IP]` to `[vnet_hdr(10)][IP]` for the kernel.
    /// If NEEDS_CSUM is set, derives `csum_start` and `csum_offset` from the IP header.
    pub async fn write_egress(&self, packet: &[u8]) {
        if packet.len() < FABRIC_HDR_SZ + 20 {
            return;
        }
        let fabric_flags = packet[0];
        let ip_packet = &packet[FABRIC_HDR_SZ..];

        // Build a 10-byte virtio-net header (struct virtio_net_hdr):
        //   [0]      flags         — VIRTIO_NET_HDR_F_NEEDS_CSUM = 1
        //   [1]      gso_type      — 0 (no segmentation offload)
        //   [2..4]   hdr_len       — 0 (unused without GSO)
        //   [4..6]   gso_size      — 0 (unused without GSO)
        //   [6..8]   csum_start    — byte offset of transport header from packet start
        //   [8..10]  csum_offset   — byte offset of checksum field within transport header
        // All multi-byte fields are little-endian per virtio spec §5.1.6.
        let mut vnet_hdr = [0u8; Self::VNET_HDR_SZ];
        if fabric_flags & FLAG_NEEDS_CSUM != 0 {
            vnet_hdr[0] = 1; // VIRTIO_NET_HDR_F_NEEDS_CSUM
            // Derive csum_start and csum_offset from IP header.
            let ihl = (ip_packet[0] & 0x0f) as usize * 4;
            let protocol = ip_packet[9];
            let csum_offset: u16 = match protocol {
                IP_PROTO_TCP => 16,
                IP_PROTO_UDP => 6,
                _ => 0,
            };
            let csum_start = ihl as u16;
            vnet_hdr[6..8].copy_from_slice(&csum_start.to_le_bytes());
            vnet_hdr[8..10].copy_from_slice(&csum_offset.to_le_bytes());
        }

        let mut tun_frame = Vec::with_capacity(Self::VNET_HDR_SZ + ip_packet.len());
        tun_frame.extend_from_slice(&vnet_hdr);
        tun_frame.extend_from_slice(ip_packet);

        if let Err(e) = tun_write(&self.tun, &tun_frame).await {
            log::warn!("gateway: TUN write error: {}", e);
        }
    }

    /// Async read from the TUN device. Wraps the low-level `tun_read`.
    pub async fn read_ingress(&self, buf: &mut [u8]) -> io::Result<usize> {
        tun_read(&self.tun, buf).await
    }

    /// Build a fabric frame from a TUN ingress packet.
    ///
    /// TUN reads `[vnet_hdr(10)][IP]` from the kernel.
    /// Converts to `[fabric_hdr(3)][IP]` for the fabric.
    /// Extracts NEEDS_CSUM flag from vnet byte 0; discards csum_start/csum_offset
    /// (they'll be derived from the IP header when needed).
    pub fn build_ingress_frame(&self, tun_buf: &[u8], n: usize) -> Option<Vec<u8>> {
        if n < Self::VNET_HDR_SZ + 20 {
            return None;
        }
        let vnet_flags = tun_buf[0];
        let needs_csum = vnet_flags & 1; // VIRTIO_NET_HDR_F_NEEDS_CSUM
        let ip_packet = &tun_buf[Self::VNET_HDR_SZ..n];

        Some(crate::packet::with_fabric_header(needs_csum, 0, ip_packet))
    }

    /// Get the TUN device interface name.
    pub fn name(&self) -> &str {
        &self.tun_name
    }
}
