use std::collections::HashMap;
use std::ffi::CStr;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::time::{Duration, Instant};

use tokio::io::unix::AsyncFd;

use crate::packet::{ETH_HDR_LEN, ETHERTYPE_IPV4, FabricFrame, VNET_HDR_SZ};
use crate::fabric::switch::GATEWAY_MAC;
use super::adjust_vnet_csum_start;

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
/// Manages a TUN device for routing pod traffic to the host network,
/// and an IP-to-MAC table for building return frames.
pub(crate) struct TunEgress {
    tun: AsyncFd<OwnedFd>,
    tun_name: String,
    ip_mac_table: HashMap<[u8; 4], ([u8; 6], Instant)>,
    ip_mac_timeout: Duration,
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
            ip_mac_table: HashMap::new(),
            ip_mac_timeout: Duration::from_secs(300),
        })
    }

    /// Write an egress frame to the TUN device.
    ///
    /// Learns the source MAC from the frame, strips the Ethernet header,
    /// adjusts the vnet checksum offset, and writes to TUN.
    pub async fn write_egress(&mut self, ff: &FabricFrame<'_>) {
        let src_mac = ff.src_mac();
        let eth_frame = ff.eth_payload();
        let ip_packet = &eth_frame[ETH_HDR_LEN..];
        let src_ip: [u8; 4] = ip_packet[12..16].try_into().unwrap();
        self.ip_mac_table.insert(src_ip, (src_mac, Instant::now()));

        // Copy vnet header and adjust csum_start for TUN (IP-level,
        // no ethernet header), then write [vnet_hdr][ip_packet] to TUN.
        let mut vnet_hdr = ff.vnet_hdr();
        adjust_vnet_csum_start(&mut vnet_hdr, -(ETH_HDR_LEN as i16));
        let mut tun_buf_out = Vec::with_capacity(VNET_HDR_SZ + ip_packet.len());
        tun_buf_out.extend_from_slice(&vnet_hdr);
        tun_buf_out.extend_from_slice(ip_packet);
        if let Err(e) = tun_write(&self.tun, &tun_buf_out).await {
            log::warn!("gateway: TUN write error: {}", e);
        }
    }

    /// Async read from the TUN device. Wraps the low-level `tun_read`.
    pub async fn read_ingress(&self, buf: &mut [u8]) -> io::Result<usize> {
        tun_read(&self.tun, buf).await
    }

    /// Build a fabric frame from a TUN ingress packet.
    ///
    /// Looks up the destination MAC from the ip_mac_table, adjusts the vnet
    /// checksum offset, and constructs `[vnet_hdr][eth_hdr][ip_packet]`.
    /// Returns `None` if no MAC mapping exists for the destination IP.
    pub fn build_ingress_frame(&self, tun_buf: &[u8], n: usize) -> Option<Vec<u8>> {
        if n < VNET_HDR_SZ + 20 {
            return None;
        }
        let tun_vnet_hdr = &tun_buf[..VNET_HDR_SZ];
        let ip_packet = &tun_buf[VNET_HDR_SZ..n];

        let dst_ip: [u8; 4] = ip_packet[16..20].try_into().unwrap();
        let dst_mac = match self.ip_mac_table.get(&dst_ip) {
            Some((mac, _)) => *mac,
            None => {
                log::debug!(
                    "gateway: ingress no MAC for {}.{}.{}.{}, dropping",
                    dst_ip[0], dst_ip[1], dst_ip[2], dst_ip[3],
                );
                return None;
            }
        };

        // Adjust vnet header csum_start for fabric (adds ethernet header).
        let mut vnet_hdr: [u8; VNET_HDR_SZ] = tun_vnet_hdr.try_into().unwrap();
        adjust_vnet_csum_start(&mut vnet_hdr, ETH_HDR_LEN as i16);

        // Build fabric frame: [vnet_hdr][eth_hdr][ip_packet]
        let mut frame = Vec::with_capacity(VNET_HDR_SZ + ETH_HDR_LEN + ip_packet.len());
        frame.extend_from_slice(&vnet_hdr);
        frame.extend_from_slice(&dst_mac);
        frame.extend_from_slice(&GATEWAY_MAC);
        frame.extend_from_slice(&ETHERTYPE_IPV4.to_be_bytes());
        frame.extend_from_slice(ip_packet);

        Some(frame)
    }

    /// Remove stale ip_mac entries that have exceeded the timeout.
    pub fn sweep_stale(&mut self) {
        let now = Instant::now();
        let before = self.ip_mac_table.len();
        self.ip_mac_table.retain(|_ip, (_, inserted)| {
            now.duration_since(*inserted) <= self.ip_mac_timeout
        });
        let expired = before - self.ip_mac_table.len();
        if expired > 0 {
            log::info!(
                "gateway: swept {} stale ip_mac entries ({} remaining)",
                expired,
                self.ip_mac_table.len()
            );
        }
    }

    /// Get the TUN device interface name.
    pub fn name(&self) -> &str {
        &self.tun_name
    }
}
