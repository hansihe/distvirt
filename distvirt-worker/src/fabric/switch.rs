use std::collections::HashMap;
use std::time::{Duration, Instant};

use super::port::PortId;

/// Synthetic gateway MAC address (locally administered).
pub const GATEWAY_MAC: [u8; 6] = [0x02, 0x00, 0x00, 0x00, 0x00, 0x01];

/// Gateway IP address.
pub const GATEWAY_IP: [u8; 4] = [172, 16, 0, 1];

/// Gateway IP address as a string (for use in guest configuration).
pub const GATEWAY_IP_STR: &str = "172.16.0.1";

/// Ethernet header size.
pub const ETH_HEADER_LEN: usize = 14;

/// Virtio-net header size (10 bytes), prepended to all frames in the fabric.
pub const VNET_HDR_SZ: usize = 10;

/// MAC address learning table mapping MAC addresses to port IDs.
pub struct MacTable {
    table: HashMap<[u8; 6], (PortId, Instant)>,
}

impl MacTable {
    pub fn new() -> Self {
        MacTable {
            table: HashMap::new(),
        }
    }

    /// Learn (insert or update) a source MAC to port mapping.
    pub fn learn(&mut self, mac: [u8; 6], port_id: PortId) {
        if !is_broadcast(&mac) && !is_multicast(&mac) {
            self.table.insert(mac, (port_id, Instant::now()));
        }
    }

    /// Look up which port a destination MAC is associated with.
    pub fn lookup(&self, mac: &[u8; 6]) -> Option<PortId> {
        self.table.get(mac).map(|(port_id, _)| *port_id)
    }

    /// Remove entries older than `max_age`.
    pub fn gc(&mut self, max_age: Duration) {
        let now = Instant::now();
        let before = self.table.len();
        self.table.retain(|_mac, (_, seen)| {
            now.duration_since(*seen) <= max_age
        });
        let expired = before - self.table.len();
        if expired > 0 {
            log::info!("mac_table: gc removed {} stale entries ({} remaining)", expired, self.table.len());
        }
    }
}

/// Check if a MAC address is the broadcast address (ff:ff:ff:ff:ff:ff).
pub fn is_broadcast(mac: &[u8; 6]) -> bool {
    *mac == [0xff; 6]
}

/// Check if a MAC is multicast (bit 0 of first octet set, excluding broadcast).
fn is_multicast(mac: &[u8; 6]) -> bool {
    mac[0] & 0x01 != 0 && !is_broadcast(mac)
}

/// Parse the destination MAC, source MAC, and ethertype from an Ethernet frame.
/// Returns None if the frame is too short.
#[allow(dead_code)]
pub fn parse_ethernet_header(frame: &[u8]) -> Option<([u8; 6], [u8; 6], u16)> {
    if frame.len() < ETH_HEADER_LEN {
        return None;
    }
    let dst: [u8; 6] = frame[0..6].try_into().unwrap();
    let src: [u8; 6] = frame[6..12].try_into().unwrap();
    let ethertype = u16::from_be_bytes([frame[12], frame[13]]);
    Some((dst, src, ethertype))
}

/// Extract the destination IPv4 address from an Ethernet frame (without vnet header).
/// Returns `Some(ip)` if the frame is IPv4 (ethertype 0x0800) and long enough
/// to contain the IP destination address (20 bytes of IP header minimum).
pub fn extract_ipv4_dst(frame: &[u8]) -> Option<std::net::Ipv4Addr> {
    // frame layout: [dst_mac(6)][src_mac(6)][ethertype(2)][ip_header...]
    // IP dest address is at IP header offset 16..20 (absolute frame offset 30..34).
    if frame.len() < ETH_HEADER_LEN + 20 {
        return None;
    }
    let ethertype = u16::from_be_bytes([frame[12], frame[13]]);
    if ethertype != 0x0800 {
        return None;
    }
    let dst_ip = std::net::Ipv4Addr::new(frame[30], frame[31], frame[32], frame[33]);
    Some(dst_ip)
}

/// Zero-copy wrapper over a raw fabric frame: `[vnet_hdr][eth_hdr][payload]`.
///
/// Created via `FabricFrame::new()` which validates minimum size
/// (`VNET_HDR_SZ + ETH_HEADER_LEN` = 24 bytes). All accessor methods
/// are safe to call on a validated frame.
pub struct FabricFrame<'a> {
    raw: &'a [u8],
}

impl<'a> FabricFrame<'a> {
    /// Parse a raw fabric frame. Returns `None` if too short for vnet + ethernet headers.
    pub fn new(raw: &'a [u8]) -> Option<Self> {
        if raw.len() < VNET_HDR_SZ + ETH_HEADER_LEN {
            return None;
        }
        Some(FabricFrame { raw })
    }

    /// Total frame length including vnet header.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.raw.len()
    }

    /// The vnet header as a fixed-size array.
    pub fn vnet_hdr(&self) -> [u8; VNET_HDR_SZ] {
        self.raw[..VNET_HDR_SZ].try_into().unwrap()
    }

    /// The ethernet frame (everything after the vnet header).
    pub fn eth_payload(&self) -> &'a [u8] {
        &self.raw[VNET_HDR_SZ..]
    }

    /// Destination MAC address.
    pub fn dst_mac(&self) -> [u8; 6] {
        self.raw[VNET_HDR_SZ..VNET_HDR_SZ + 6].try_into().unwrap()
    }

    /// Source MAC address.
    pub fn src_mac(&self) -> [u8; 6] {
        self.raw[VNET_HDR_SZ + 6..VNET_HDR_SZ + 12].try_into().unwrap()
    }

    /// EtherType field.
    pub fn ethertype(&self) -> u16 {
        u16::from_be_bytes([self.raw[VNET_HDR_SZ + 12], self.raw[VNET_HDR_SZ + 13]])
    }

    /// Extract destination IPv4 address if this is an IPv4 frame.
    pub fn ipv4_dst(&self) -> Option<std::net::Ipv4Addr> {
        extract_ipv4_dst(self.eth_payload())
    }

    /// Extract source IPv4 address if this is an IPv4 frame.
    pub fn ipv4_src(&self) -> Option<std::net::Ipv4Addr> {
        extract_ipv4_src(self.eth_payload())
    }
}

/// Build an owned fabric frame by prepending a zeroed vnet header to an ethernet frame.
pub fn with_vnet_header(eth_frame: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(VNET_HDR_SZ + eth_frame.len());
    frame.extend_from_slice(&[0u8; VNET_HDR_SZ]);
    frame.extend_from_slice(eth_frame);
    frame
}

/// Rewrite the destination MAC address in a fabric frame (after the vnet header).
///
/// Panics if the frame is shorter than `VNET_HDR_SZ + 6` bytes.
pub fn rewrite_dst_mac(frame: &mut [u8], mac: &[u8; 6]) {
    frame[VNET_HDR_SZ..VNET_HDR_SZ + 6].copy_from_slice(mac);
}

/// Rewrite the source MAC address in a fabric frame (after the vnet header).
///
/// Panics if the frame is shorter than `VNET_HDR_SZ + 12` bytes.
pub fn rewrite_src_mac(frame: &mut [u8], mac: &[u8; 6]) {
    frame[VNET_HDR_SZ + 6..VNET_HDR_SZ + 12].copy_from_slice(mac);
}

/// Extract the source IPv4 address from an Ethernet frame (without vnet header).
pub fn extract_ipv4_src(frame: &[u8]) -> Option<std::net::Ipv4Addr> {
    if frame.len() < ETH_HEADER_LEN + 20 {
        return None;
    }
    let ethertype = u16::from_be_bytes([frame[12], frame[13]]);
    if ethertype != 0x0800 {
        return None;
    }
    Some(std::net::Ipv4Addr::new(frame[26], frame[27], frame[28], frame[29]))
}

/// Extract the IP protocol number from an Ethernet frame (without vnet header).
pub fn extract_ip_protocol(frame: &[u8]) -> Option<u8> {
    if frame.len() < ETH_HEADER_LEN + 20 {
        return None;
    }
    let ethertype = u16::from_be_bytes([frame[12], frame[13]]);
    if ethertype != 0x0800 {
        return None;
    }
    Some(frame[23]) // IP protocol at offset 9 from IP header start (14+9=23)
}

/// Extract transport-layer source and destination ports from an Ethernet frame
/// (without vnet header). Works for TCP (protocol 6) and UDP (protocol 17).
/// Returns `(src_port, dst_port)` or `None` for non-TCP/UDP or too-short frames.
pub fn extract_transport_ports(frame: &[u8]) -> Option<(u16, u16)> {
    if frame.len() < ETH_HEADER_LEN + 20 {
        return None;
    }
    let ethertype = u16::from_be_bytes([frame[12], frame[13]]);
    if ethertype != 0x0800 {
        return None;
    }
    let protocol = frame[23];
    if protocol != 6 && protocol != 17 {
        return None;
    }
    let ihl = (frame[14] & 0x0f) as usize * 4;
    let transport_start = ETH_HEADER_LEN + ihl;
    if frame.len() < transport_start + 4 {
        return None;
    }
    let src_port = u16::from_be_bytes([frame[transport_start], frame[transport_start + 1]]);
    let dst_port = u16::from_be_bytes([frame[transport_start + 2], frame[transport_start + 3]]);
    Some((src_port, dst_port))
}

/// Incremental checksum update helper: given an old checksum and a pair of
/// old/new 16-bit words, compute the updated checksum per RFC 1624.
fn incremental_csum_update(old_csum: u16, old_word: u16, new_word: u16) -> u16 {
    // ~(~HC + ~m + m') using ones-complement arithmetic
    let mut sum: u32 = (!old_csum as u32) + (!old_word as u32) + (new_word as u32);
    while sum >> 16 != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !sum as u16
}

/// Rewrite the destination IPv4 address in a fabric frame (with vnet header)
/// and update the IP header checksum incrementally.
///
/// Also adjusts the transport partial checksum if `VIRTIO_NET_HDR_F_NEEDS_CSUM`
/// is set in the vnet header.
pub fn rewrite_ipv4_dst(frame: &mut [u8], old_ip: std::net::Ipv4Addr, new_ip: std::net::Ipv4Addr) {
    let eth_start = VNET_HDR_SZ;
    let ip_start = eth_start + ETH_HEADER_LEN;
    // Minimum: vnet + eth + 20 bytes IP header
    if frame.len() < ip_start + 20 {
        return;
    }

    let old_octets = old_ip.octets();
    let new_octets = new_ip.octets();

    // Dst IP is at IP header offset 16..20
    let dst_off = ip_start + 16;
    frame[dst_off..dst_off + 4].copy_from_slice(&new_octets);

    // Incremental IP header checksum update
    update_ip_header_csum(frame, ip_start, &old_octets, &new_octets);

    // Adjust transport partial checksum if needed
    update_transport_csum_for_ip_change(frame, &old_octets, &new_octets);
}

/// Rewrite the source IPv4 address in a fabric frame (with vnet header)
/// and update the IP header checksum incrementally.
///
/// Also adjusts the transport partial checksum if `VIRTIO_NET_HDR_F_NEEDS_CSUM`
/// is set in the vnet header.
pub fn rewrite_ipv4_src(frame: &mut [u8], old_ip: std::net::Ipv4Addr, new_ip: std::net::Ipv4Addr) {
    let eth_start = VNET_HDR_SZ;
    let ip_start = eth_start + ETH_HEADER_LEN;
    if frame.len() < ip_start + 20 {
        return;
    }

    let old_octets = old_ip.octets();
    let new_octets = new_ip.octets();

    // Src IP is at IP header offset 12..16
    let src_off = ip_start + 12;
    frame[src_off..src_off + 4].copy_from_slice(&new_octets);

    update_ip_header_csum(frame, ip_start, &old_octets, &new_octets);
    update_transport_csum_for_ip_change(frame, &old_octets, &new_octets);
}

/// Incrementally update the IP header checksum after changing a 4-byte IP address.
fn update_ip_header_csum(frame: &mut [u8], ip_start: usize, old_octets: &[u8; 4], new_octets: &[u8; 4]) {
    let csum_off = ip_start + 10;
    let old_csum = u16::from_be_bytes([frame[csum_off], frame[csum_off + 1]]);

    let old_hi = u16::from_be_bytes([old_octets[0], old_octets[1]]);
    let old_lo = u16::from_be_bytes([old_octets[2], old_octets[3]]);
    let new_hi = u16::from_be_bytes([new_octets[0], new_octets[1]]);
    let new_lo = u16::from_be_bytes([new_octets[2], new_octets[3]]);

    let csum = incremental_csum_update(old_csum, old_hi, new_hi);
    let csum = incremental_csum_update(csum, old_lo, new_lo);

    frame[csum_off..csum_off + 2].copy_from_slice(&csum.to_be_bytes());
}

/// Virtio-net header flags: VIRTIO_NET_HDR_F_NEEDS_CSUM
const VIRTIO_NET_HDR_F_NEEDS_CSUM: u8 = 1;

/// Adjust the transport-layer partial checksum when an IP address changes,
/// if the vnet header indicates `NEEDS_CSUM`.
///
/// The vnet header layout (10 bytes):
///   [0] flags, [1] gso_type, [2..4] hdr_len, [4..6] gso_size,
///   [6..8] csum_start, [8..10] csum_offset
fn update_transport_csum_for_ip_change(frame: &mut [u8], old_octets: &[u8; 4], new_octets: &[u8; 4]) {
    if frame.len() < VNET_HDR_SZ {
        return;
    }
    let flags = frame[0];
    if flags & VIRTIO_NET_HDR_F_NEEDS_CSUM == 0 {
        return;
    }

    let csum_start = u16::from_le_bytes([frame[6], frame[7]]) as usize;
    let csum_offset = u16::from_le_bytes([frame[8], frame[9]]) as usize;

    // csum_start/csum_offset are relative to the ethernet frame (after vnet header)
    let abs_csum_pos = VNET_HDR_SZ + csum_start + csum_offset;
    if frame.len() < abs_csum_pos + 2 {
        return;
    }

    let old_csum = u16::from_be_bytes([frame[abs_csum_pos], frame[abs_csum_pos + 1]]);

    let old_hi = u16::from_be_bytes([old_octets[0], old_octets[1]]);
    let old_lo = u16::from_be_bytes([old_octets[2], old_octets[3]]);
    let new_hi = u16::from_be_bytes([new_octets[0], new_octets[1]]);
    let new_lo = u16::from_be_bytes([new_octets[2], new_octets[3]]);

    let csum = incremental_csum_update(old_csum, old_hi, new_hi);
    let csum = incremental_csum_update(csum, old_lo, new_lo);

    frame[abs_csum_pos..abs_csum_pos + 2].copy_from_slice(&csum.to_be_bytes());
}

/// Format a MAC address for logging.
pub fn format_mac(bytes: &[u8; 6]) -> String {
    format!(
        "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- MacTable tests ---

    #[test]
    fn mac_table_learn_and_lookup() {
        let mut table = MacTable::new();
        let mac = [0x02, 0x00, 0x00, 0x00, 0x00, 0x01];
        table.learn(mac, 3);
        assert_eq!(table.lookup(&mac), Some(3));
    }

    #[test]
    fn mac_table_lookup_unknown_returns_none() {
        let table = MacTable::new();
        let mac = [0x02, 0x00, 0x00, 0x00, 0x00, 0x01];
        assert_eq!(table.lookup(&mac), None);
    }

    #[test]
    fn mac_table_learn_migration_updates_port() {
        let mut table = MacTable::new();
        let mac = [0x02, 0x00, 0x00, 0x00, 0x00, 0x01];
        table.learn(mac, 1);
        table.learn(mac, 5);
        assert_eq!(table.lookup(&mac), Some(5));
    }

    #[test]
    fn mac_table_learn_ignores_broadcast() {
        let mut table = MacTable::new();
        table.learn([0xff; 6], 1);
        assert_eq!(table.lookup(&[0xff; 6]), None);
    }

    #[test]
    fn mac_table_learn_ignores_multicast() {
        let mut table = MacTable::new();
        // Bit 0 of first octet set = multicast
        let mac = [0x01, 0x00, 0x5e, 0x00, 0x00, 0x01];
        table.learn(mac, 2);
        assert_eq!(table.lookup(&mac), None);
    }

    #[test]
    fn mac_table_gc_removes_stale_entries() {
        let mut table = MacTable::new();
        let mac_a = [0x02, 0x00, 0x00, 0x00, 0x00, 0x0a];
        let mac_b = [0x02, 0x00, 0x00, 0x00, 0x00, 0x0b];
        table.learn(mac_a, 1);
        table.learn(mac_b, 2);

        // Both should be present with a generous max_age.
        table.gc(Duration::from_secs(300));
        assert_eq!(table.lookup(&mac_a), Some(1));
        assert_eq!(table.lookup(&mac_b), Some(2));

        // With zero max_age, all entries should be removed.
        table.gc(Duration::from_secs(0));
        assert_eq!(table.lookup(&mac_a), None);
        assert_eq!(table.lookup(&mac_b), None);
    }

    #[test]
    fn mac_table_multiple_macs_resolve_correctly() {
        let mut table = MacTable::new();
        let mac_a = [0x02, 0x00, 0x00, 0x00, 0x00, 0x0a];
        let mac_b = [0x02, 0x00, 0x00, 0x00, 0x00, 0x0b];
        let mac_c = [0x02, 0x00, 0x00, 0x00, 0x00, 0x0c];
        table.learn(mac_a, 1);
        table.learn(mac_b, 2);
        table.learn(mac_c, 3);
        assert_eq!(table.lookup(&mac_a), Some(1));
        assert_eq!(table.lookup(&mac_b), Some(2));
        assert_eq!(table.lookup(&mac_c), Some(3));
    }

    // --- parse_ethernet_header tests ---

    #[test]
    fn parse_ethernet_header_valid_frame() {
        let mut frame = [0u8; 20];
        // dst MAC
        frame[0..6].copy_from_slice(&[0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff]);
        // src MAC
        frame[6..12].copy_from_slice(&[0x11, 0x22, 0x33, 0x44, 0x55, 0x66]);
        // ethertype 0x0800 (IPv4)
        frame[12..14].copy_from_slice(&[0x08, 0x00]);

        let (dst, src, ethertype) = parse_ethernet_header(&frame).unwrap();
        assert_eq!(dst, [0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff]);
        assert_eq!(src, [0x11, 0x22, 0x33, 0x44, 0x55, 0x66]);
        assert_eq!(ethertype, 0x0800);
    }

    #[test]
    fn parse_ethernet_header_too_short() {
        assert!(parse_ethernet_header(&[0u8; 13]).is_none());
    }

    #[test]
    fn parse_ethernet_header_exactly_14_bytes() {
        let frame = [0u8; 14];
        assert!(parse_ethernet_header(&frame).is_some());
    }

    #[test]
    fn parse_ethernet_header_empty() {
        assert!(parse_ethernet_header(&[]).is_none());
    }

    // --- format_mac tests ---

    // --- extract_ipv4_dst tests ---

    #[test]
    fn extract_ipv4_dst_valid_ipv4_frame() {
        // Build a minimal IPv4 frame: 14 byte eth header + 20 byte IP header.
        let mut frame = [0u8; 34];
        // ethertype = 0x0800 (IPv4)
        frame[12] = 0x08;
        frame[13] = 0x00;
        // IP dest at offset 30..34 = 192.168.1.42
        frame[30] = 192;
        frame[31] = 168;
        frame[32] = 1;
        frame[33] = 42;
        assert_eq!(
            extract_ipv4_dst(&frame),
            Some(std::net::Ipv4Addr::new(192, 168, 1, 42))
        );
    }

    #[test]
    fn extract_ipv4_dst_non_ipv4_ethertype() {
        let mut frame = [0u8; 34];
        // ethertype = 0x0806 (ARP)
        frame[12] = 0x08;
        frame[13] = 0x06;
        assert_eq!(extract_ipv4_dst(&frame), None);
    }

    #[test]
    fn extract_ipv4_dst_frame_too_short() {
        let frame = [0u8; 33]; // needs at least 34
        assert_eq!(extract_ipv4_dst(&frame), None);
    }

    // --- format_mac tests ---

    #[test]
    fn format_mac_known() {
        assert_eq!(format_mac(&[0x02, 0x00, 0x00, 0x00, 0x00, 0x01]), "02:00:00:00:00:01");
    }

    #[test]
    fn format_mac_broadcast() {
        assert_eq!(format_mac(&[0xff; 6]), "ff:ff:ff:ff:ff:ff");
    }

    // --- FabricFrame tests ---

    #[test]
    fn fabric_frame_rejects_too_short() {
        // Needs VNET_HDR_SZ (10) + ETH_HEADER_LEN (14) = 24 bytes minimum.
        assert!(FabricFrame::new(&[0u8; 23]).is_none());
        assert!(FabricFrame::new(&[]).is_none());
    }

    #[test]
    fn fabric_frame_accepts_minimum_size() {
        let buf = [0u8; 24]; // exactly VNET_HDR_SZ + ETH_HEADER_LEN
        assert!(FabricFrame::new(&buf).is_some());
    }

    #[test]
    fn fabric_frame_accessors() {
        let mut buf = [0u8; 30];
        // dst MAC at offset 10..16
        buf[10..16].copy_from_slice(&[0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff]);
        // src MAC at offset 16..22
        buf[16..22].copy_from_slice(&[0x11, 0x22, 0x33, 0x44, 0x55, 0x66]);
        // ethertype at offset 22..24
        buf[22..24].copy_from_slice(&[0x08, 0x00]);

        let ff = FabricFrame::new(&buf).unwrap();
        assert_eq!(ff.dst_mac(), [0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff]);
        assert_eq!(ff.src_mac(), [0x11, 0x22, 0x33, 0x44, 0x55, 0x66]);
        assert_eq!(ff.ethertype(), 0x0800);
        assert_eq!(ff.len(), 30);
        assert_eq!(ff.eth_payload().len(), 20); // 30 - 10
        assert_eq!(ff.vnet_hdr(), [0u8; VNET_HDR_SZ]);
    }

    #[test]
    fn fabric_frame_ipv4_dst() {
        // Build: [vnet(10)][eth(14)][ip_hdr(20)] = 44 bytes
        let mut buf = [0u8; 44];
        buf[22..24].copy_from_slice(&[0x08, 0x00]); // ethertype IPv4
        // IP dst at eth offset 30..34 = raw offset 40..44
        buf[40] = 192; buf[41] = 168; buf[42] = 1; buf[43] = 42;

        let ff = FabricFrame::new(&buf).unwrap();
        assert_eq!(ff.ipv4_dst(), Some(std::net::Ipv4Addr::new(192, 168, 1, 42)));
    }

    #[test]
    fn fabric_frame_ipv4_dst_non_ipv4() {
        let mut buf = [0u8; 44];
        buf[22..24].copy_from_slice(&[0x08, 0x06]); // ARP
        let ff = FabricFrame::new(&buf).unwrap();
        assert_eq!(ff.ipv4_dst(), None);
    }

    // --- with_vnet_header tests ---

    #[test]
    fn with_vnet_header_prepends_zeroed_header() {
        let eth = [0xaa, 0xbb, 0xcc];
        let frame = with_vnet_header(&eth);
        assert_eq!(frame.len(), VNET_HDR_SZ + 3);
        assert_eq!(&frame[..VNET_HDR_SZ], &[0u8; VNET_HDR_SZ]);
        assert_eq!(&frame[VNET_HDR_SZ..], &[0xaa, 0xbb, 0xcc]);
    }

    // --- rewrite_dst_mac tests ---

    #[test]
    fn rewrite_dst_mac_overwrites_correctly() {
        let mut frame = [0u8; 24];
        frame[10..16].copy_from_slice(&[0x01; 6]); // original dst mac
        let new_mac = [0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff];
        rewrite_dst_mac(&mut frame, &new_mac);
        assert_eq!(&frame[VNET_HDR_SZ..VNET_HDR_SZ + 6], &new_mac);
    }
}
