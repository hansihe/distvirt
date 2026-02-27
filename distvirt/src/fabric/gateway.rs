use std::collections::{HashMap, VecDeque};
use std::ffi::CStr;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

use smoltcp::iface::{Config, Interface, SocketHandle, SocketSet};
use smoltcp::phy::{Checksum, Device, DeviceCapabilities, Medium, RxToken, TxToken};
use smoltcp::socket::udp;
use smoltcp::time::Instant as SmolInstant;
use smoltcp::wire::{EthernetAddress, HardwareAddress, IpAddress, IpCidr, IpEndpoint};

use tokio::io::unix::AsyncFd;
use tokio::net::UdpSocket as TokioUdpSocket;
use tokio::sync::mpsc;

use super::switch::{ETH_HEADER_LEN, GATEWAY_IP, GATEWAY_MAC, VNET_HDR_SZ};

/// TUN device ioctl constants.
const TUNSETIFF: libc::c_ulong = 0x400454ca;
const TUNSETOFFLOAD: libc::c_ulong = 0x400454d0;
const IFF_TUN: libc::c_short = 0x0001;
const IFF_NO_PI: libc::c_short = 0x1000;
const IFF_VNET_HDR: libc::c_short = 0x4000;
const TUN_F_CSUM: libc::c_uint = 0x01;

const VIRTIO_NET_HDR_F_NEEDS_CSUM: u8 = 1;

/// Ioctl constants for setting interface address and netmask.
const SIOCSIFADDR: libc::c_ulong = 0x8916;
const SIOCSIFNETMASK: libc::c_ulong = 0x891c;

/// EtherType constants.
const ETHERTYPE_IPV4: u16 = 0x0800;
const ETHERTYPE_ARP: u16 = 0x0806;

/// Channel buffer size for gateway communication.
const CHANNEL_BUF: usize = 256;

// --- smoltcp phy::Device backed by VecDeque ---

struct ChannelRxToken(Vec<u8>);

impl RxToken for ChannelRxToken {
    fn consume<R, F>(self, f: F) -> R
    where
        F: FnOnce(&[u8]) -> R,
    {
        f(&self.0)
    }
}

struct ChannelTxToken<'a>(&'a mut VecDeque<Vec<u8>>);

impl<'a> TxToken for ChannelTxToken<'a> {
    fn consume<R, F>(self, len: usize, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        let mut buf = vec![0u8; len];
        let result = f(&mut buf);
        self.0.push_back(buf);
        result
    }
}

struct ChannelDevice {
    rx_queue: VecDeque<Vec<u8>>,
    tx_queue: VecDeque<Vec<u8>>,
}

impl ChannelDevice {
    fn new() -> Self {
        ChannelDevice {
            rx_queue: VecDeque::new(),
            tx_queue: VecDeque::new(),
        }
    }
}

impl Device for ChannelDevice {
    type RxToken<'a> = ChannelRxToken where Self: 'a;
    type TxToken<'a> = ChannelTxToken<'a> where Self: 'a;

    fn receive(&mut self, _timestamp: SmolInstant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        let pkt = self.rx_queue.pop_front()?;
        Some((ChannelRxToken(pkt), ChannelTxToken(&mut self.tx_queue)))
    }

    fn transmit(&mut self, _timestamp: SmolInstant) -> Option<Self::TxToken<'_>> {
        Some(ChannelTxToken(&mut self.tx_queue))
    }

    fn capabilities(&self) -> DeviceCapabilities {
        let mut caps = DeviceCapabilities::default();
        caps.medium = Medium::Ethernet;
        caps.max_transmission_unit = 1514;
        // Frames arrive from AF_PACKET on a TAP with virtio-net checksum
        // offloading, so checksums may be partial/invalid. Use Checksum::Tx
        // so smoltcp computes valid checksums on transmit but skips
        // verification on receive.
        caps.checksum.ipv4 = Checksum::Tx;
        caps.checksum.udp = Checksum::Tx;
        caps.checksum.tcp = Checksum::Tx;
        caps.checksum.icmpv4 = Checksum::Tx;
        caps
    }
}

// --- Ifreq for TUN ioctls ---

#[repr(C)]
struct Ifreq {
    ifr_name: [u8; libc::IFNAMSIZ],
    ifr_flags: libc::c_short,
    _pad: [u8; 22],
}

// --- FabricGateway: smoltcp IP stack + TUN egress + DNS forwarding ---

/// Combined gateway that handles ARP (via smoltcp), DNS forwarding (via smoltcp
/// UDP socket), and internet egress (via TUN device with host NAT).
pub struct FabricGateway {
    // smoltcp userspace IP stack
    iface: Interface,
    device: ChannelDevice,
    sockets: SocketSet<'static>,
    dns_handle: SocketHandle,

    // TUN for internet egress
    tun: AsyncFd<OwnedFd>,
    tun_name: String,
    ip_mac_table: HashMap<[u8; 4], [u8; 6]>,

    // Channels to/from fabric switch
    egress_rx: mpsc::Receiver<Vec<u8>>,
    ingress_tx: mpsc::Sender<Vec<u8>>,

    // DNS upstream forwarding
    upstream_socket: TokioUdpSocket,
    upstream_servers: Vec<SocketAddr>,
    pending_dns: HashMap<u16, IpEndpoint>,

    // Timing
    boot_time: std::time::Instant,
}

impl FabricGateway {
    /// Create a new fabric gateway with smoltcp interface, TUN device, and DNS forwarder.
    ///
    /// Returns the gateway and channel endpoints for the fabric:
    /// - `egress_tx`: send frames destined for the gateway here
    /// - `ingress_rx`: receive frames from the gateway to inject into the fabric
    pub fn new() -> anyhow::Result<(Self, mpsc::Sender<Vec<u8>>, mpsc::Receiver<Vec<u8>>)> {
        // Create TUN device for internet egress.
        let (tun_fd, tun_name) = create_tun()?;
        configure_tun_ip(&tun_name, GATEWAY_IP, [255, 255, 255, 0])?;
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

        // Create smoltcp interface with gateway MAC and IP.
        let boot_time = std::time::Instant::now();
        let mut device = ChannelDevice::new();
        let config = Config::new(HardwareAddress::Ethernet(EthernetAddress(GATEWAY_MAC)));
        let mut iface = Interface::new(config, &mut device, SmolInstant::from_millis(0));
        iface.update_ip_addrs(|addrs| {
            addrs
                .push(IpCidr::new(
                    IpAddress::v4(GATEWAY_IP[0], GATEWAY_IP[1], GATEWAY_IP[2], GATEWAY_IP[3]),
                    24,
                ))
                .ok();
        });

        // Create UDP socket for DNS on port 53.
        let dns_socket = udp::Socket::new(
            udp::PacketBuffer::new(vec![udp::PacketMetadata::EMPTY; 16], vec![0u8; 8192]),
            udp::PacketBuffer::new(vec![udp::PacketMetadata::EMPTY; 16], vec![0u8; 8192]),
        );
        let mut sockets = SocketSet::new(vec![]);
        let dns_handle = sockets.add(dns_socket);
        {
            let sock = sockets.get_mut::<udp::Socket>(dns_handle);
            sock.bind(53)
                .map_err(|e| anyhow::anyhow!("bind DNS socket: {:?}", e))?;
        }

        // Parse upstream DNS servers from host /etc/resolv.conf.
        let upstream_servers = parse_resolv_conf();
        log::info!("gateway: upstream DNS servers: {:?}", upstream_servers);

        // Create upstream UDP socket on host network.
        let upstream_socket = {
            let std_sock = std::net::UdpSocket::bind("0.0.0.0:0")?;
            std_sock.set_nonblocking(true)?;
            TokioUdpSocket::from_std(std_sock)?
        };

        // Fabric channels.
        let (egress_tx, egress_rx) = mpsc::channel(CHANNEL_BUF);
        let (ingress_tx, ingress_rx) = mpsc::channel(CHANNEL_BUF);

        log::info!(
            "gateway: created TUN device {} with smoltcp interface at 172.16.0.1/24",
            tun_name
        );

        Ok((
            FabricGateway {
                iface,
                device,
                sockets,
                dns_handle,
                tun: async_fd,
                tun_name,
                ip_mac_table: HashMap::new(),
                egress_rx,
                ingress_tx,
                upstream_socket,
                upstream_servers,
                pending_dns: HashMap::new(),
                boot_time,
            },
            egress_tx,
            ingress_rx,
        ))
    }

    fn smoltcp_now(&self) -> SmolInstant {
        SmolInstant::from_millis(self.boot_time.elapsed().as_millis() as i64)
    }

    /// Poll smoltcp and drain any generated frames to the fabric.
    /// smoltcp generates raw ethernet frames with valid checksums, so we prepend
    /// a zeroed vnet header (no offload flags needed).
    fn poll_and_drain(&mut self) {
        let ts = self.smoltcp_now();
        self.iface.poll(ts, &mut self.device, &mut self.sockets);
        while let Some(eth_frame) = self.device.tx_queue.pop_front() {
            let mut frame = Vec::with_capacity(VNET_HDR_SZ + eth_frame.len());
            frame.extend_from_slice(&[0u8; VNET_HDR_SZ]);
            frame.extend_from_slice(&eth_frame);
            if let Err(e) = self.ingress_tx.try_send(frame) {
                log::warn!("gateway: ingress channel send error: {}", e);
            }
        }
    }

    /// Process any DNS queries received by the smoltcp UDP socket.
    /// Forwards them to the upstream DNS server.
    async fn process_dns_queries(&mut self) {
        loop {
            let (query, endpoint) = {
                let sock = self.sockets.get_mut::<udp::Socket>(self.dns_handle);
                match sock.recv() {
                    Ok((data, meta)) => (data.to_vec(), meta.endpoint),
                    Err(_) => break,
                }
            };

            if query.len() < 2 {
                continue;
            }

            let query_id = u16::from_be_bytes([query[0], query[1]]);
            log::info!("gateway: DNS query id={} from {}", query_id, endpoint);

            self.pending_dns.insert(query_id, endpoint);

            if let Some(upstream) = self.upstream_servers.first() {
                if let Err(e) = self.upstream_socket.send_to(&query, upstream).await {
                    log::warn!("gateway: DNS upstream send error: {}", e);
                }
            }
        }
    }

    /// Handle a DNS response from upstream: write it to the smoltcp DNS socket
    /// targeting the original client, then poll to generate the frame.
    fn handle_dns_response(&mut self, response: &[u8]) {
        if response.len() < 2 {
            return;
        }

        let query_id = u16::from_be_bytes([response[0], response[1]]);
        if let Some(endpoint) = self.pending_dns.remove(&query_id) {
            log::info!("gateway: DNS response id={} -> {}", query_id, endpoint);
            let sock = self.sockets.get_mut::<udp::Socket>(self.dns_handle);
            if let Err(e) = sock.send_slice(response, endpoint) {
                log::warn!("gateway: DNS response send to smoltcp: {:?}", e);
            }
            self.poll_and_drain();
        } else {
            log::info!(
                "gateway: DNS response id={} has no pending query",
                query_id
            );
        }
    }

    /// Run the gateway main loop.
    pub async fn run(mut self) {
        let mut tun_buf = vec![0u8; 65536];
        let mut dns_buf = vec![0u8; 4096];

        loop {
            let ts = self.smoltcp_now();
            let delay = self.iface.poll_delay(ts, &self.sockets);
            let poll_deadline = match delay {
                Some(d) => {
                    tokio::time::Instant::now()
                        + std::time::Duration::from_millis(d.total_millis() as u64)
                }
                None => tokio::time::Instant::now() + std::time::Duration::from_secs(86400),
            };

            tokio::select! {
                // Frame from fabric (gateway-destined unicast or broadcast copy)
                frame = self.egress_rx.recv() => {
                    let frame = match frame {
                        Some(f) => f,
                        None => {
                            log::info!("gateway: egress channel closed, shutting down");
                            break;
                        }
                    };

                    if frame.len() < VNET_HDR_SZ + ETH_HEADER_LEN {
                        continue;
                    }

                    let eth_frame = &frame[VNET_HDR_SZ..];
                    let ethertype = u16::from_be_bytes([eth_frame[12], eth_frame[13]]);

                    // Determine if this frame should go to smoltcp or TUN.
                    let to_smoltcp = if ethertype == ETHERTYPE_ARP {
                        // All ARP frames go to smoltcp (it handles ARP for the gateway IP).
                        true
                    } else if ethertype == ETHERTYPE_IPV4
                        && eth_frame.len() >= ETH_HEADER_LEN + 20
                    {
                        // IPv4 frames destined for the gateway IP go to smoltcp (DNS etc).
                        let dst_ip: [u8; 4] = eth_frame[ETH_HEADER_LEN + 16..ETH_HEADER_LEN + 20]
                            .try_into()
                            .unwrap();
                        dst_ip == GATEWAY_IP
                    } else {
                        false
                    };

                    if to_smoltcp {
                        // smoltcp expects raw ethernet frames without vnet header.
                        self.device.rx_queue.push_back(eth_frame.to_vec());
                        self.poll_and_drain();
                        self.process_dns_queries().await;
                    } else if ethertype == ETHERTYPE_IPV4
                        && eth_frame.len() >= ETH_HEADER_LEN + 20
                    {
                        // Internet egress: learn src MAC, strip Ethernet, write to TUN.
                        let src_mac: [u8; 6] = eth_frame[6..12].try_into().unwrap();
                        let ip_packet = &eth_frame[ETH_HEADER_LEN..];
                        let src_ip: [u8; 4] = ip_packet[12..16].try_into().unwrap();
                        self.ip_mac_table.insert(src_ip, src_mac);

                        // Copy vnet header and adjust csum_start for TUN (IP-level,
                        // no ethernet header), then write [vnet_hdr][ip_packet] to TUN.
                        let mut vnet_hdr: [u8; VNET_HDR_SZ] = frame[..VNET_HDR_SZ].try_into().unwrap();
                        adjust_vnet_csum_start(&mut vnet_hdr, -(ETH_HEADER_LEN as i16));
                        let mut tun_buf_out = Vec::with_capacity(VNET_HDR_SZ + ip_packet.len());
                        tun_buf_out.extend_from_slice(&vnet_hdr);
                        tun_buf_out.extend_from_slice(ip_packet);
                        if let Err(e) = tun_write(&self.tun, &tun_buf_out).await {
                            log::warn!("gateway: TUN write error: {}", e);
                        }
                    }
                }

                // TUN ingress: internet -> fabric
                result = tun_read(&self.tun, &mut tun_buf) => {
                    let n = match result {
                        Ok(0) => {
                            log::info!("gateway: TUN EOF, shutting down");
                            break;
                        }
                        Ok(n) => n,
                        Err(e) => {
                            log::warn!("gateway: TUN read error: {}", e);
                            continue;
                        }
                    };

                    // TUN provides [vnet_hdr][ip_packet] via IFF_VNET_HDR.
                    if n < VNET_HDR_SZ + 20 {
                        continue;
                    }
                    let tun_vnet_hdr = &tun_buf[..VNET_HDR_SZ];
                    let ip_packet = &tun_buf[VNET_HDR_SZ..n];

                    let dst_ip: [u8; 4] = ip_packet[16..20].try_into().unwrap();
                    let dst_mac = match self.ip_mac_table.get(&dst_ip) {
                        Some(mac) => *mac,
                        None => {
                            log::debug!(
                                "gateway: ingress no MAC for {}.{}.{}.{}, dropping",
                                dst_ip[0], dst_ip[1], dst_ip[2], dst_ip[3],
                            );
                            continue;
                        }
                    };

                    // Adjust vnet header csum_start for fabric (adds ethernet header).
                    let mut vnet_hdr: [u8; VNET_HDR_SZ] = tun_vnet_hdr.try_into().unwrap();
                    adjust_vnet_csum_start(&mut vnet_hdr, ETH_HEADER_LEN as i16);

                    // Build fabric frame: [vnet_hdr][eth_hdr][ip_packet]
                    let mut frame = Vec::with_capacity(VNET_HDR_SZ + ETH_HEADER_LEN + ip_packet.len());
                    frame.extend_from_slice(&vnet_hdr);
                    frame.extend_from_slice(&dst_mac);
                    frame.extend_from_slice(&GATEWAY_MAC);
                    frame.extend_from_slice(&ETHERTYPE_IPV4.to_be_bytes());
                    frame.extend_from_slice(ip_packet);

                    if let Err(e) = self.ingress_tx.send(frame).await {
                        log::warn!("gateway: ingress channel send error: {}", e);
                        break;
                    }
                }

                // DNS upstream response
                result = self.upstream_socket.recv_from(&mut dns_buf) => {
                    match result {
                        Ok((n, _addr)) => {
                            self.handle_dns_response(&dns_buf[..n]);
                        }
                        Err(e) => {
                            log::warn!("gateway: DNS upstream recv error: {}", e);
                        }
                    }
                }

                // smoltcp timer (ARP cache, retransmissions)
                _ = tokio::time::sleep_until(poll_deadline) => {
                    self.poll_and_drain();
                }
            }
        }

        log::info!(
            "gateway: shut down (TUN device {} will be destroyed)",
            self.tun_name
        );
    }
}

// --- TUN helper functions ---

/// Create a TUN device with vnet header support for checksum offloading.
/// Returns the fd and interface name.
fn create_tun() -> anyhow::Result<(OwnedFd, String)> {
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
fn configure_tun_ip(name: &str, ip: [u8; 4], netmask: [u8; 4]) -> anyhow::Result<()> {
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

/// Adjust the `csum_start` field of a virtio-net header by `delta` bytes.
/// Only modifies the header if VIRTIO_NET_HDR_F_NEEDS_CSUM is set.
/// `csum_start` is at bytes 6-7 (little-endian u16).
fn adjust_vnet_csum_start(vnet_hdr: &mut [u8; 10], delta: i16) {
    if vnet_hdr[0] & VIRTIO_NET_HDR_F_NEEDS_CSUM == 0 {
        return;
    }
    let csum_start = u16::from_le_bytes([vnet_hdr[6], vnet_hdr[7]]);
    let adjusted = (csum_start as i16).wrapping_add(delta) as u16;
    vnet_hdr[6..8].copy_from_slice(&adjusted.to_le_bytes());
}

/// Parse /etc/resolv.conf for upstream nameservers. Falls back to 8.8.8.8.
fn parse_resolv_conf() -> Vec<SocketAddr> {
    let content = match std::fs::read_to_string("/etc/resolv.conf") {
        Ok(c) => c,
        Err(_) => {
            return vec![SocketAddr::new(
                IpAddr::V4(std::net::Ipv4Addr::new(8, 8, 8, 8)),
                53,
            )];
        }
    };

    let mut servers = Vec::new();
    for line in content.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("nameserver") {
            let addr_str = rest.trim();
            if let Ok(ip) = addr_str.parse::<IpAddr>() {
                servers.push(SocketAddr::new(ip, 53));
            }
        }
    }

    if servers.is_empty() {
        servers.push(SocketAddr::new(
            IpAddr::V4(std::net::Ipv4Addr::new(8, 8, 8, 8)),
            53,
        ));
    }

    servers
}
