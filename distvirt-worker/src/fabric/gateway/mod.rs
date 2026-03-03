use std::collections::VecDeque;
use std::time::{Duration, Instant};

use smoltcp::iface::{Config, Interface, SocketHandle, SocketSet};
use smoltcp::phy::{Checksum, Device, DeviceCapabilities, Medium, RxToken, TxToken};
use smoltcp::socket::udp;
use smoltcp::time::Instant as SmolInstant;
use smoltcp::wire::{EthernetAddress, HardwareAddress, IpAddress, IpCidr};

use tokio::sync::mpsc;

pub(crate) mod dns;
pub(crate) mod tun;

pub use dns::DnsRegistry;

use crate::packet::{ETH_HDR_LEN, ETHERTYPE_ARP, ETHERTYPE_IPV4, FabricFrame, with_vnet_header};
use super::switch::GATEWAY_MAC;
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

const VIRTIO_NET_HDR_F_NEEDS_CSUM: u8 = 1;

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

/// Combined gateway that handles ARP (via smoltcp), DNS forwarding (via
/// `DnsForwarder`), and internet egress (via `TunEgress`).
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
}

impl FabricGateway {
    /// Create a new fabric gateway with smoltcp interface, TUN device, and DNS forwarder.
    ///
    /// Returns the gateway and channel endpoints for the fabric:
    /// - `egress_tx`: send frames destined for the gateway here
    /// - `ingress_rx`: receive frames from the gateway to inject into the fabric
    pub fn new(registry: DnsRegistry, pod_gateway_ip: [u8; 4], pod_prefix_len: u8) -> anyhow::Result<(Self, mpsc::Sender<Vec<u8>>, mpsc::Receiver<Vec<u8>>)> {
        // Create TUN egress sub-component.
        let tun = TunEgress::new(pod_gateway_ip, prefix_len_to_netmask(pod_prefix_len))?;

        // Create DNS forwarder sub-component.
        let dns = DnsForwarder::new(registry)?;

        // Create smoltcp interface with gateway MAC and IP.
        let boot_time = Instant::now();
        let mut device = ChannelDevice::new();
        let config = Config::new(HardwareAddress::Ethernet(EthernetAddress(GATEWAY_MAC)));
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
            let frame = with_vnet_header(&eth_frame);
            if let Err(e) = self.ingress_tx.try_send(frame) {
                log::warn!("gateway: ingress channel send error: {}", e);
            }
        }
    }

    /// Run the gateway main loop.
    pub async fn run(mut self) {
        let mut tun_buf = vec![0u8; 65536];
        let mut sweep_interval = tokio::time::interval(Duration::from_secs(5));

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

                    let ff = match FabricFrame::new(&frame) {
                        Some(f) => f,
                        None => continue,
                    };
                    let eth_frame = ff.eth_payload();
                    let ethertype = ff.ethertype();

                    // Determine if this frame should go to smoltcp or TUN.
                    let to_smoltcp = if ethertype == ETHERTYPE_ARP {
                        // All ARP frames go to smoltcp (it handles ARP for the gateway IP).
                        true
                    } else if ethertype == ETHERTYPE_IPV4
                        && eth_frame.len() >= ETH_HDR_LEN + 20
                    {
                        // IPv4 frames destined for the gateway IP go to smoltcp (DNS etc).
                        let dst_ip: [u8; 4] = eth_frame[ETH_HDR_LEN + 16..ETH_HDR_LEN + 20]
                            .try_into()
                            .unwrap();
                        dst_ip == self.pod_gateway_ip
                    } else {
                        false
                    };

                    if to_smoltcp {
                        // smoltcp expects raw ethernet frames without vnet header.
                        self.device.rx_queue.push_back(eth_frame.to_vec());
                        self.poll_and_drain();
                        let dns_sock = self.sockets.get_mut::<udp::Socket>(self.dns_handle);
                        if self.dns.process_queries(dns_sock) {
                            self.poll_and_drain();
                        }
                    } else if ethertype == ETHERTYPE_IPV4
                        && eth_frame.len() >= ETH_HDR_LEN + 20
                    {
                        // Internet egress via TUN sub-component.
                        self.tun.write_egress(&ff).await;
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

                // smoltcp timer (ARP cache, retransmissions)
                _ = tokio::time::sleep_until(poll_deadline) => {
                    self.poll_and_drain();
                }

                // Periodic sweep of stale entries
                _ = sweep_interval.tick() => {
                    self.tun.sweep_stale();
                }
            }
        }

        log::info!(
            "gateway: shut down (TUN device {} will be destroyed)",
            self.tun.name()
        );
    }
}

/// Adjust the `csum_start` field of a virtio-net header by `delta` bytes.
/// Only modifies the header if VIRTIO_NET_HDR_F_NEEDS_CSUM is set.
/// `csum_start` is at bytes 6-7 (little-endian u16).
pub(super) fn adjust_vnet_csum_start(vnet_hdr: &mut [u8; 10], delta: i16) {
    if vnet_hdr[0] & VIRTIO_NET_HDR_F_NEEDS_CSUM == 0 {
        return;
    }
    let csum_start = u16::from_le_bytes([vnet_hdr[6], vnet_hdr[7]]);
    let adjusted = (csum_start as i16).wrapping_add(delta) as u16;
    vnet_hdr[6..8].copy_from_slice(&adjusted.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // --- adjust_vnet_csum_start tests ---

    #[test]
    fn adjust_vnet_csum_start_with_needs_csum_negative_delta() {
        // Simulate fabric→TUN path: subtract ETH_HDR_LEN (14) from csum_start
        let mut hdr = [0u8; 10];
        hdr[0] = VIRTIO_NET_HDR_F_NEEDS_CSUM; // flags = NEEDS_CSUM
        let csum_start: u16 = 24; // e.g., offset to TCP header in fabric frame
        hdr[6..8].copy_from_slice(&csum_start.to_le_bytes());

        adjust_vnet_csum_start(&mut hdr, -(ETH_HDR_LEN as i16));
        let result = u16::from_le_bytes([hdr[6], hdr[7]]);
        assert_eq!(result, 10); // 24 - 14 = 10
    }

    #[test]
    fn adjust_vnet_csum_start_with_needs_csum_positive_delta() {
        // Simulate TUN→fabric path: add ETH_HDR_LEN (14) to csum_start
        let mut hdr = [0u8; 10];
        hdr[0] = VIRTIO_NET_HDR_F_NEEDS_CSUM;
        let csum_start: u16 = 10;
        hdr[6..8].copy_from_slice(&csum_start.to_le_bytes());

        adjust_vnet_csum_start(&mut hdr, ETH_HDR_LEN as i16);
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

    #[test]
    fn ip_mac_table_timeout_removes_stale_entries() {
        let mut table: HashMap<[u8; 4], ([u8; 6], Instant)> = HashMap::new();
        let old = Instant::now() - Duration::from_secs(600);
        let recent = Instant::now();

        table.insert([10, 0, 0, 1], ([0x02; 6], old));
        table.insert([10, 0, 0, 2], ([0x03; 6], recent));

        let timeout = Duration::from_secs(300);
        let now = Instant::now();
        table.retain(|_ip, (_, inserted)| {
            now.duration_since(*inserted) <= timeout
        });

        assert!(table.get(&[10, 0, 0, 1]).is_none(), "old entry should be removed");
        assert!(table.get(&[10, 0, 0, 2]).is_some(), "recent entry should remain");
    }
}
