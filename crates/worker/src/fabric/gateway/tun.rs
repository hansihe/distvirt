use std::io;
use std::net::Ipv4Addr;
use std::process::Command;

use anyhow::Context;
use tokio::sync::mpsc;

use crate::linux::net::TunDevice;
use crate::packet::{FABRIC_HDR_SZ, FLAG_NEEDS_CSUM, IP_PROTO_TCP, IP_PROTO_UDP};

/// Abstraction over the internet egress/ingress path.
///
/// Operates at the fabric-frame level: `[fabric_hdr(3)][IP]`.
pub trait EgressPort: Send + 'static {
    /// Write a fabric-format packet to the egress device.
    fn write_egress(&self, fabric_packet: &[u8]) -> impl Future<Output = ()> + Send;
    /// Read the next ingress frame in fabric format. Returns `None` on EOF/close.
    fn read_ingress_frame(&self) -> impl Future<Output = Option<Vec<u8>>> + Send;
    /// Human-readable name for logging.
    fn name(&self) -> &str;
}

use std::future::Future;

/// TUN-based internet egress/ingress component.
///
/// Manages a TUN device for routing pod traffic to the host network.
/// Sets up iptables MASQUERADE for the pod subnet on creation and
/// removes the rule on drop.
pub struct TunEgress {
    tun: TunDevice,
    /// The MASQUERADE rule parameters, kept for cleanup on drop.
    /// Held purely for its `Drop` impl — removing the iptables rule when the
    /// TUN device is destroyed.
    _masquerade_rule: Option<MasqueradeRule>,
}

/// Parameters for an iptables MASQUERADE rule so we can remove it on drop.
struct MasqueradeRule {
    subnet: String,
    out_iface: String,
}

impl TunEgress {
    /// Create a new TUN egress: create TUN device, configure IP, set non-blocking,
    /// and set up iptables MASQUERADE for the pod subnet.
    pub fn new(gateway_ip: [u8; 4], netmask: [u8; 4]) -> anyhow::Result<Self> {
        let tun = TunDevice::create()?;
        tun.configure_ip(gateway_ip, netmask)?;
        tun.bring_up()?;

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

        // Set up iptables MASQUERADE so egress traffic from the pod subnet
        // gets SNATted to the host's IP on the outgoing interface.
        let masquerade_rule = match setup_masquerade(gateway_ip, netmask) {
            Ok(rule) => Some(rule),
            Err(e) => {
                log::warn!("gateway: failed to set up MASQUERADE: {:#}", e);
                None
            }
        };

        log::info!("gateway: created TUN device {}", tun.name());

        Ok(TunEgress { tun, _masquerade_rule: masquerade_rule })
    }

    /// Size of the kernel virtio-net header used by TUN devices.
    const VNET_HDR_SZ: usize = 10;

    /// Write an egress frame to the TUN device.
    ///
    /// Converts `[fabric_hdr(3)][IP]` to `[vnet_hdr(10)][IP]` for the kernel.
    /// If NEEDS_CSUM is set, derives `csum_start` and `csum_offset` from the IP header.
    async fn write_egress_inner(&self, packet: &[u8]) {
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

        if let Err(e) = self.tun.write(&tun_frame).await {
            log::warn!("gateway: TUN write error: {}", e);
        }
    }

    /// Async read from the TUN device. Wraps the low-level `tun_read`.
    pub async fn read_ingress(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.tun.read(buf).await
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
}

impl EgressPort for TunEgress {
    async fn write_egress(&self, fabric_packet: &[u8]) {
        self.write_egress_inner(fabric_packet).await;
    }

    async fn read_ingress_frame(&self) -> Option<Vec<u8>> {
        let mut buf = vec![0u8; 65536];
        match self.read_ingress(&mut buf).await {
            Ok(0) => None,
            Ok(n) => self.build_ingress_frame(&buf, n),
            Err(e) => {
                log::warn!("gateway: TUN read error: {}", e);
                None
            }
        }
    }

    fn name(&self) -> &str {
        self.tun.name()
    }
}

/// Channel-based egress for testing. No TUN device, no root required.
pub struct ChannelEgress {
    egress_tx: mpsc::Sender<Vec<u8>>,
    ingress_rx: tokio::sync::Mutex<mpsc::Receiver<Vec<u8>>>,
}

impl ChannelEgress {
    /// Create a new channel egress.
    ///
    /// Returns `(egress, internet_rx, internet_tx)` where:
    /// - `internet_rx`: test side receives packets the gateway sends to "the internet"
    /// - `internet_tx`: test side injects packets from "the internet" into the gateway
    pub fn new(buf: usize) -> (Self, mpsc::Receiver<Vec<u8>>, mpsc::Sender<Vec<u8>>) {
        let (egress_tx, internet_rx) = mpsc::channel(buf);
        let (internet_tx, ingress_rx) = mpsc::channel(buf);
        (
            ChannelEgress {
                egress_tx,
                ingress_rx: tokio::sync::Mutex::new(ingress_rx),
            },
            internet_rx,
            internet_tx,
        )
    }
}

impl EgressPort for ChannelEgress {
    async fn write_egress(&self, fabric_packet: &[u8]) {
        let _ = self.egress_tx.try_send(fabric_packet.to_vec());
    }

    async fn read_ingress_frame(&self) -> Option<Vec<u8>> {
        self.ingress_rx.lock().await.recv().await
    }

    fn name(&self) -> &str {
        "channel-egress"
    }
}

// ---------------------------------------------------------------------------
// iptables MASQUERADE helpers
// ---------------------------------------------------------------------------

/// Compute the network address from a gateway IP and netmask, returning a
/// CIDR string like "172.16.0.0/16".
fn subnet_cidr(gateway_ip: [u8; 4], netmask: [u8; 4]) -> String {
    let ip = u32::from_be_bytes(gateway_ip);
    let mask = u32::from_be_bytes(netmask);
    let network = ip & mask;
    let prefix_len = mask.count_ones();
    let net_addr = Ipv4Addr::from(network);
    format!("{}/{}", net_addr, prefix_len)
}

/// Determine the default-route output interface by reading `/proc/net/route`.
fn default_route_interface() -> anyhow::Result<String> {
    let contents = std::fs::read_to_string("/proc/net/route")
        .context("read /proc/net/route")?;
    for line in contents.lines().skip(1) {
        let mut fields = line.split_whitespace();
        let iface = fields.next().unwrap_or("");
        let dest = fields.next().unwrap_or("");
        // Default route has destination 00000000.
        if dest == "00000000" {
            return Ok(iface.to_string());
        }
    }
    anyhow::bail!("no default route found in /proc/net/route")
}

/// Add an iptables MASQUERADE rule for the pod subnet.
fn setup_masquerade(gateway_ip: [u8; 4], netmask: [u8; 4]) -> anyhow::Result<MasqueradeRule> {
    let subnet = subnet_cidr(gateway_ip, netmask);
    let out_iface = default_route_interface()?;

    // Check if the rule already exists to avoid duplicates.
    let check = Command::new("iptables")
        .args(["-t", "nat", "-C", "POSTROUTING",
               "-s", &subnet, "-o", &out_iface, "-j", "MASQUERADE"])
        .output()
        .context("run iptables -C")?;

    if check.status.success() {
        log::info!(
            "gateway: MASQUERADE rule already exists for {} via {}",
            subnet, out_iface
        );
        return Ok(MasqueradeRule { subnet, out_iface });
    }

    let output = Command::new("iptables")
        .args(["-t", "nat", "-A", "POSTROUTING",
               "-s", &subnet, "-o", &out_iface, "-j", "MASQUERADE"])
        .output()
        .context("run iptables -A")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("iptables MASQUERADE add failed: {}", stderr.trim());
    }

    log::info!(
        "gateway: added MASQUERADE rule: -s {} -o {} -j MASQUERADE",
        subnet, out_iface
    );
    Ok(MasqueradeRule { subnet, out_iface })
}

impl Drop for MasqueradeRule {
    fn drop(&mut self) {
        let result = Command::new("iptables")
            .args(["-t", "nat", "-D", "POSTROUTING",
                   "-s", &self.subnet, "-o", &self.out_iface, "-j", "MASQUERADE"])
            .output();

        match result {
            Ok(output) if output.status.success() => {
                log::info!(
                    "gateway: removed MASQUERADE rule for {} via {}",
                    self.subnet, self.out_iface
                );
            }
            Ok(output) => {
                let stderr = String::from_utf8_lossy(&output.stderr);
                log::warn!(
                    "gateway: failed to remove MASQUERADE rule: {}",
                    stderr.trim()
                );
            }
            Err(e) => {
                log::warn!("gateway: failed to run iptables -D: {}", e);
            }
        }
    }
}
