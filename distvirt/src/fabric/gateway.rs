use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::os::fd::{AsRawFd, OwnedFd};

use smoltcp::iface::{Config, Interface, SocketHandle, SocketSet};
use smoltcp::phy::{Checksum, Device, DeviceCapabilities, Medium, RxToken, TxToken};
use smoltcp::socket::udp;
use smoltcp::time::Instant as SmolInstant;
use smoltcp::wire::{EthernetAddress, HardwareAddress, IpAddress, IpCidr, IpEndpoint};

use tokio::io::unix::AsyncFd;
use tokio::net::UdpSocket as TokioUdpSocket;
use tokio::sync::mpsc;

use super::dns::{self, DnsRegistry};
use super::switch::{ETH_HEADER_LEN, GATEWAY_IP, GATEWAY_MAC, VNET_HDR_SZ};
use super::tun::{configure_tun_ip, create_tun, tun_read, tun_write};

const VIRTIO_NET_HDR_F_NEEDS_CSUM: u8 = 1;

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

    // Local DNS registry (service name -> IP)
    registry: DnsRegistry,

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
    pub fn new(registry: DnsRegistry) -> anyhow::Result<(Self, mpsc::Sender<Vec<u8>>, mpsc::Receiver<Vec<u8>>)> {
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
                registry,
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

            // Try local registry first.
            if let Some(response) = dns::try_resolve(&self.registry, &query) {
                log::info!("gateway: DNS query id={} resolved locally", query_id);
                let sock = self.sockets.get_mut::<udp::Socket>(self.dns_handle);
                if let Err(e) = sock.send_slice(&response, endpoint) {
                    log::warn!("gateway: DNS local response send: {:?}", e);
                }
                self.poll_and_drain();
                continue;
            }

            // Fall back to upstream.
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
        Err(_) => return parse_resolv_conf_content(""),
    };
    parse_resolv_conf_content(&content)
}

/// Parse the content of a resolv.conf file for nameserver entries.
/// Falls back to 8.8.8.8:53 if no valid nameservers are found.
fn parse_resolv_conf_content(content: &str) -> Vec<SocketAddr> {
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

#[cfg(test)]
mod tests {
    use super::*;

    // --- adjust_vnet_csum_start tests ---

    #[test]
    fn adjust_vnet_csum_start_with_needs_csum_negative_delta() {
        // Simulate fabric→TUN path: subtract ETH_HEADER_LEN (14) from csum_start
        let mut hdr = [0u8; 10];
        hdr[0] = VIRTIO_NET_HDR_F_NEEDS_CSUM; // flags = NEEDS_CSUM
        let csum_start: u16 = 24; // e.g., offset to TCP header in fabric frame
        hdr[6..8].copy_from_slice(&csum_start.to_le_bytes());

        adjust_vnet_csum_start(&mut hdr, -(ETH_HEADER_LEN as i16));
        let result = u16::from_le_bytes([hdr[6], hdr[7]]);
        assert_eq!(result, 10); // 24 - 14 = 10
    }

    #[test]
    fn adjust_vnet_csum_start_with_needs_csum_positive_delta() {
        // Simulate TUN→fabric path: add ETH_HEADER_LEN (14) to csum_start
        let mut hdr = [0u8; 10];
        hdr[0] = VIRTIO_NET_HDR_F_NEEDS_CSUM;
        let csum_start: u16 = 10;
        hdr[6..8].copy_from_slice(&csum_start.to_le_bytes());

        adjust_vnet_csum_start(&mut hdr, ETH_HEADER_LEN as i16);
        let result = u16::from_le_bytes([hdr[6], hdr[7]]);
        assert_eq!(result, 24); // 10 + 14 = 24
    }

    #[test]
    fn adjust_vnet_csum_start_without_needs_csum_unchanged() {
        let mut hdr = [0u8; 10];
        hdr[0] = 0; // no NEEDS_CSUM flag
        let csum_start: u16 = 42;
        hdr[6..8].copy_from_slice(&csum_start.to_le_bytes());

        adjust_vnet_csum_start(&mut hdr, 100);
        let result = u16::from_le_bytes([hdr[6], hdr[7]]);
        assert_eq!(result, 42); // unchanged
    }

    // --- parse_resolv_conf_content tests ---

    #[test]
    fn parse_resolv_conf_single_nameserver() {
        let content = "nameserver 1.1.1.1\n";
        let servers = parse_resolv_conf_content(content);
        assert_eq!(servers.len(), 1);
        assert_eq!(
            servers[0],
            SocketAddr::new(IpAddr::V4(std::net::Ipv4Addr::new(1, 1, 1, 1)), 53)
        );
    }

    #[test]
    fn parse_resolv_conf_multiple_nameservers() {
        let content = "nameserver 1.1.1.1\nnameserver 8.8.4.4\n";
        let servers = parse_resolv_conf_content(content);
        assert_eq!(servers.len(), 2);
        assert_eq!(
            servers[0],
            SocketAddr::new(IpAddr::V4(std::net::Ipv4Addr::new(1, 1, 1, 1)), 53)
        );
        assert_eq!(
            servers[1],
            SocketAddr::new(IpAddr::V4(std::net::Ipv4Addr::new(8, 8, 4, 4)), 53)
        );
    }

    #[test]
    fn parse_resolv_conf_empty_falls_back() {
        let servers = parse_resolv_conf_content("");
        assert_eq!(servers.len(), 1);
        assert_eq!(
            servers[0],
            SocketAddr::new(IpAddr::V4(std::net::Ipv4Addr::new(8, 8, 8, 8)), 53)
        );
    }

    #[test]
    fn parse_resolv_conf_comments_and_other_lines_ignored() {
        let content = "# comment\nsearch example.com\nnameserver 9.9.9.9\noptions ndots:5\n";
        let servers = parse_resolv_conf_content(content);
        assert_eq!(servers.len(), 1);
        assert_eq!(
            servers[0],
            SocketAddr::new(IpAddr::V4(std::net::Ipv4Addr::new(9, 9, 9, 9)), 53)
        );
    }

    #[test]
    fn parse_resolv_conf_ipv6_nameserver() {
        let content = "nameserver 2001:4860:4860::8888\n";
        let servers = parse_resolv_conf_content(content);
        assert_eq!(servers.len(), 1);
        assert_eq!(
            servers[0],
            SocketAddr::new("2001:4860:4860::8888".parse::<IpAddr>().unwrap(), 53)
        );
    }
}
