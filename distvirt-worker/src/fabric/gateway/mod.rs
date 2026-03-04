use std::collections::VecDeque;
use std::time::Instant;

use smoltcp::iface::{Config, Interface, SocketHandle, SocketSet};
use smoltcp::phy::{Checksum, Device, DeviceCapabilities, Medium, RxToken, TxToken};
use smoltcp::socket::udp;
use smoltcp::time::Instant as SmolInstant;
use smoltcp::wire::{HardwareAddress, IpAddress, IpCidr};

use tokio::sync::mpsc;

pub(crate) mod dns;
pub(crate) mod tun;

pub use dns::DnsRegistry;

use crate::packet::{FabricPacket, with_fabric_header};
use dns::DnsForwarder;
use tun::TunEgress;

/// Convert a prefix length (0–32) to a 4-byte netmask.
fn prefix_len_to_netmask(prefix_len: u8) -> [u8; 4] {
    if prefix_len == 0 {
        [0, 0, 0, 0]
    } else {
        let mask = !0u32 << (32 - prefix_len);
        mask.to_be_bytes()
    }
}

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
        caps.medium = Medium::Ip;
        caps.max_transmission_unit = 1500;
        caps.checksum.ipv4 = Checksum::Tx;
        caps.checksum.udp = Checksum::Tx;
        caps.checksum.tcp = Checksum::Tx;
        caps.checksum.icmpv4 = Checksum::Tx;
        caps
    }
}

// --- FabricGateway: smoltcp IP stack + TUN egress + DNS forwarding ---

/// Combined gateway that handles DNS forwarding (via `DnsForwarder`) and
/// internet egress (via `TunEgress`).
///
/// Receives L3 fabric packets `[vnet][IP]`. For packets to the gateway IP
/// (DNS), feeds raw IP to smoltcp. For internet egress, passes directly to
/// TUN (same format).
///
/// smoltcp runs on `Medium::Ip`, operating directly on L3 packets.
pub struct FabricGateway {
    // smoltcp userspace IP stack — coordination hub
    iface: Interface,
    device: ChannelDevice,
    sockets: SocketSet<'static>,
    dns_handle: SocketHandle,
    boot_time: Instant,

    // Extracted sub-components
    dns: DnsForwarder,
    tun: TunEgress,

    // Channels to/from fabric switch
    egress_rx: mpsc::Receiver<Vec<u8>>,
    ingress_tx: mpsc::Sender<Vec<u8>>,

    // Pod subnet gateway IP (for routing DNS queries from pods)
    pod_gateway_ip: [u8; 4],
    /// Subnet mask for filtering TUN ingress (only inject packets destined for the pod subnet).
    pod_subnet_mask: u32,
    pod_subnet_bits: u32,
}

impl FabricGateway {
    /// Create a new fabric gateway with smoltcp interface, TUN device, and DNS forwarder.
    ///
    /// Returns the gateway and channel endpoints for the fabric:
    /// - `egress_tx`: send packets destined for the gateway here
    /// - `ingress_rx`: receive packets from the gateway to inject into the fabric
    pub fn new(registry: DnsRegistry, pod_gateway_ip: [u8; 4], pod_prefix_len: u8) -> anyhow::Result<(Self, mpsc::Sender<Vec<u8>>, mpsc::Receiver<Vec<u8>>)> {
        // Create TUN egress sub-component.
        let tun = TunEgress::new(pod_gateway_ip, prefix_len_to_netmask(pod_prefix_len))?;

        // Create DNS forwarder sub-component.
        let dns = DnsForwarder::new(registry)?;

        // Create smoltcp interface with gateway IP.
        let boot_time = Instant::now();
        let mut device = ChannelDevice::new();
        let config = Config::new(HardwareAddress::Ip);
        let mut iface = Interface::new(config, &mut device, SmolInstant::from_millis(0));
        iface.update_ip_addrs(|addrs| {
            addrs
                .push(IpCidr::new(
                    IpAddress::v4(pod_gateway_ip[0], pod_gateway_ip[1], pod_gateway_ip[2], pod_gateway_ip[3]),
                    pod_prefix_len,
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

        // Fabric channels.
        let (egress_tx, egress_rx) = mpsc::channel(CHANNEL_BUF);
        let (ingress_tx, ingress_rx) = mpsc::channel(CHANNEL_BUF);

        log::info!(
            "gateway: created TUN device {} with smoltcp interface at {}.{}.{}.{}/{}",
            tun.name(),
            pod_gateway_ip[0], pod_gateway_ip[1], pod_gateway_ip[2], pod_gateway_ip[3],
            pod_prefix_len,
        );

        let pod_subnet_mask = if pod_prefix_len >= 32 { u32::MAX } else { !0u32 << (32 - pod_prefix_len) };
        let pod_subnet_bits = u32::from_be_bytes(pod_gateway_ip) & pod_subnet_mask;

        Ok((
            FabricGateway {
                iface,
                device,
                sockets,
                dns_handle,
                boot_time,
                dns,
                tun,
                egress_rx,
                ingress_tx,
                pod_gateway_ip,
                pod_subnet_mask,
                pod_subnet_bits,
            },
            egress_tx,
            ingress_rx,
        ))
    }

    fn smoltcp_now(&self) -> SmolInstant {
        SmolInstant::from_millis(self.boot_time.elapsed().as_millis() as i64)
    }

    /// Poll smoltcp and drain any generated IP packets to the fabric.
    fn poll_and_drain(&mut self) {
        let ts = self.smoltcp_now();
        self.iface.poll(ts, &mut self.device, &mut self.sockets);

        while let Some(ip_packet) = self.device.tx_queue.pop_front() {
            let fabric_packet = with_fabric_header(0, 0, &ip_packet);
            if let Err(e) = self.ingress_tx.try_send(fabric_packet) {
                log::warn!("gateway: ingress channel send error: {}", e);
            }
        }
    }

    /// Run the gateway main loop.
    pub async fn run(mut self) {
        let mut tun_buf = vec![0u8; 65536];

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
                // Packet from fabric (gateway-destined)
                packet = self.egress_rx.recv() => {
                    let packet = match packet {
                        Some(f) => f,
                        None => {
                            log::info!("gateway: egress channel closed, shutting down");
                            break;
                        }
                    };

                    let fp = match FabricPacket::new(&packet) {
                        Some(f) => f,
                        None => continue,
                    };

                    // Determine if this packet should go to smoltcp (gateway IP) or TUN.
                    let ip_pkt = fp.ip_packet();
                    let to_smoltcp = if ip_pkt.len() >= 20 {
                        let dst_ip: [u8; 4] = ip_pkt[16..20].try_into().unwrap();
                        dst_ip == self.pod_gateway_ip
                    } else {
                        false
                    };

                    if to_smoltcp {
                        // Feed raw IP packet directly to smoltcp.
                        self.device.rx_queue.push_back(ip_pkt.to_vec());
                        self.poll_and_drain();
                        let dns_sock = self.sockets.get_mut::<udp::Socket>(self.dns_handle);
                        if self.dns.process_queries(dns_sock) {
                            self.poll_and_drain();
                        }
                    } else {
                        // Internet egress via TUN — fabric format matches TUN format.
                        self.tun.write_egress(&packet).await;
                    }
                }

                // TUN ingress: internet -> fabric
                result = self.tun.read_ingress(&mut tun_buf) => {
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

                    if let Some(frame) = self.tun.build_ingress_frame(&tun_buf, n) {
                        // Only inject packets destined for the pod subnet into the fabric.
                        // The kernel sends its own traffic on the TUN (IGMP, mDNS, DHCP, etc.)
                        // which is irrelevant to pods.
                        let ip_pkt = &frame[crate::packet::FABRIC_HDR_SZ..];
                        if ip_pkt.len() >= 20 {
                            let dst = u32::from_be_bytes([ip_pkt[16], ip_pkt[17], ip_pkt[18], ip_pkt[19]]);
                            if dst & self.pod_subnet_mask != self.pod_subnet_bits {
                                log::trace!(
                                    "gateway: TUN ingress dropped (dst {} not in pod subnet)",
                                    std::net::Ipv4Addr::from(dst)
                                );
                                continue;
                            }
                        }
                        if let Err(e) = self.ingress_tx.send(frame).await {
                            log::warn!("gateway: ingress channel send error: {}", e);
                            break;
                        }
                    }
                }

                // DNS upstream response (from hickory-resolver)
                result = self.dns.result_rx().recv() => {
                    if let Some(result) = result {
                        let dns_sock = self.sockets.get_mut::<udp::Socket>(self.dns_handle);
                        if self.dns.write_result(result, dns_sock) {
                            self.poll_and_drain();
                        }
                    }
                }

                // smoltcp timer (retransmissions)
                _ = tokio::time::sleep_until(poll_deadline) => {
                    self.poll_and_drain();
                }

            }
        }

        log::info!(
            "gateway: shut down (TUN device {} will be destroyed)",
            self.tun.name()
        );
    }
}

