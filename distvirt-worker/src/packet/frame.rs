//! Fabric frame wrappers and free functions for frame field extraction and mutation.

use std::net::Ipv4Addr;

use super::checksum::{incremental_csum_update, incremental_partial_update};
use super::{ETH_HDR_LEN, ETHERTYPE_IPV4, IP_PROTO_TCP, IP_PROTO_UDP, VNET_HDR_SZ};

// ---------------------------------------------------------------------------
// FabricFrame (immutable)
// ---------------------------------------------------------------------------

/// Zero-copy wrapper over a raw fabric frame: `[vnet_hdr][eth_hdr][payload]`.
///
/// Created via `FabricFrame::new()` which validates minimum size
/// (`VNET_HDR_SZ + ETH_HDR_LEN` = 24 bytes). All accessor methods
/// are safe to call on a validated frame.
pub struct FabricFrame<'a> {
    raw: &'a [u8],
}

impl<'a> FabricFrame<'a> {
    /// Parse a raw fabric frame. Returns `None` if too short for vnet + ethernet headers.
    pub fn new(raw: &'a [u8]) -> Option<Self> {
        if raw.len() < VNET_HDR_SZ + ETH_HDR_LEN {
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
        self.raw[VNET_HDR_SZ + 6..VNET_HDR_SZ + 12]
            .try_into()
            .unwrap()
    }

    /// EtherType field.
    pub fn ethertype(&self) -> u16 {
        u16::from_be_bytes([self.raw[VNET_HDR_SZ + 12], self.raw[VNET_HDR_SZ + 13]])
    }

    /// Extract destination IPv4 address if this is an IPv4 frame.
    pub fn ipv4_dst(&self) -> Option<Ipv4Addr> {
        extract_ipv4_dst(self.eth_payload())
    }

    /// Extract source IPv4 address if this is an IPv4 frame.
    pub fn ipv4_src(&self) -> Option<Ipv4Addr> {
        extract_ipv4_src(self.eth_payload())
    }
}

// ---------------------------------------------------------------------------
// FabricFrameMut (mutable)
// ---------------------------------------------------------------------------

/// Mutable zero-copy wrapper over a raw fabric frame.
///
/// Provides all read accessors from `FabricFrame` plus mutation helpers
/// for MAC/IP rewriting with incremental checksum updates.
pub struct FabricFrameMut<'a> {
    raw: &'a mut [u8],
}

#[allow(dead_code)]
impl<'a> FabricFrameMut<'a> {
    /// Parse a mutable raw fabric frame. Returns `None` if too short.
    pub fn new(raw: &'a mut [u8]) -> Option<Self> {
        if raw.len() < VNET_HDR_SZ + ETH_HDR_LEN {
            return None;
        }
        Some(FabricFrameMut { raw })
    }

    /// Total frame length including vnet header.
    pub fn len(&self) -> usize {
        self.raw.len()
    }

    /// The ethernet frame (everything after the vnet header).
    pub fn eth_payload(&self) -> &[u8] {
        &self.raw[VNET_HDR_SZ..]
    }

    /// Destination MAC address.
    pub fn dst_mac(&self) -> [u8; 6] {
        self.raw[VNET_HDR_SZ..VNET_HDR_SZ + 6].try_into().unwrap()
    }

    /// Source MAC address.
    pub fn src_mac(&self) -> [u8; 6] {
        self.raw[VNET_HDR_SZ + 6..VNET_HDR_SZ + 12]
            .try_into()
            .unwrap()
    }

    /// EtherType field.
    pub fn ethertype(&self) -> u16 {
        u16::from_be_bytes([self.raw[VNET_HDR_SZ + 12], self.raw[VNET_HDR_SZ + 13]])
    }

    /// Extract destination IPv4 address if this is an IPv4 frame.
    pub fn ipv4_dst(&self) -> Option<Ipv4Addr> {
        extract_ipv4_dst(&self.raw[VNET_HDR_SZ..])
    }

    /// Extract source IPv4 address if this is an IPv4 frame.
    pub fn ipv4_src(&self) -> Option<Ipv4Addr> {
        extract_ipv4_src(&self.raw[VNET_HDR_SZ..])
    }

    /// Rewrite the destination MAC address.
    pub fn rewrite_dst_mac(&mut self, mac: &[u8; 6]) {
        self.raw[VNET_HDR_SZ..VNET_HDR_SZ + 6].copy_from_slice(mac);
    }

    /// Rewrite the source MAC address.
    pub fn rewrite_src_mac(&mut self, mac: &[u8; 6]) {
        self.raw[VNET_HDR_SZ + 6..VNET_HDR_SZ + 12].copy_from_slice(mac);
    }

    /// Rewrite the destination IPv4 address and update IP + transport checksums.
    pub fn rewrite_ipv4_dst(&mut self, old_ip: Ipv4Addr, new_ip: Ipv4Addr) {
        rewrite_ipv4_dst(self.raw, old_ip, new_ip);
    }

    /// Rewrite the source IPv4 address and update IP + transport checksums.
    pub fn rewrite_ipv4_src(&mut self, old_ip: Ipv4Addr, new_ip: Ipv4Addr) {
        rewrite_ipv4_src(self.raw, old_ip, new_ip);
    }

    /// Complete virtio NEEDS_CSUM offload.
    pub fn complete_checksum(&mut self) {
        super::checksum::complete_checksum(self.raw);
    }
}

// ---------------------------------------------------------------------------
// Free functions — frame field extraction (operate on ethernet frames without vnet header)
// ---------------------------------------------------------------------------

/// Check if a MAC address is the broadcast address (ff:ff:ff:ff:ff:ff).
pub fn is_broadcast(mac: &[u8; 6]) -> bool {
    *mac == [0xff; 6]
}

/// Check if a MAC is multicast (bit 0 of first octet set, excluding broadcast).
pub fn is_multicast(mac: &[u8; 6]) -> bool {
    mac[0] & 0x01 != 0 && !is_broadcast(mac)
}

/// Extract the destination IPv4 address from an Ethernet frame (without vnet header).
pub fn extract_ipv4_dst(frame: &[u8]) -> Option<Ipv4Addr> {
    if frame.len() < ETH_HDR_LEN + 20 {
        return None;
    }
    let ethertype = u16::from_be_bytes([frame[12], frame[13]]);
    if ethertype != ETHERTYPE_IPV4 {
        return None;
    }
    Some(Ipv4Addr::new(frame[30], frame[31], frame[32], frame[33]))
}

/// Extract the source IPv4 address from an Ethernet frame (without vnet header).
pub fn extract_ipv4_src(frame: &[u8]) -> Option<Ipv4Addr> {
    if frame.len() < ETH_HDR_LEN + 20 {
        return None;
    }
    let ethertype = u16::from_be_bytes([frame[12], frame[13]]);
    if ethertype != ETHERTYPE_IPV4 {
        return None;
    }
    Some(Ipv4Addr::new(frame[26], frame[27], frame[28], frame[29]))
}

/// Extract the IP protocol number from an Ethernet frame (without vnet header).
pub fn extract_ip_protocol(frame: &[u8]) -> Option<u8> {
    if frame.len() < ETH_HDR_LEN + 20 {
        return None;
    }
    let ethertype = u16::from_be_bytes([frame[12], frame[13]]);
    if ethertype != ETHERTYPE_IPV4 {
        return None;
    }
    Some(frame[23]) // IP protocol at offset 9 from IP header start (14+9=23)
}

/// Extract transport-layer source and destination ports from an Ethernet frame
/// (without vnet header). Works for TCP (protocol 6) and UDP (protocol 17).
pub fn extract_transport_ports(frame: &[u8]) -> Option<(u16, u16)> {
    if frame.len() < ETH_HDR_LEN + 20 {
        return None;
    }
    let ethertype = u16::from_be_bytes([frame[12], frame[13]]);
    if ethertype != ETHERTYPE_IPV4 {
        return None;
    }
    let protocol = frame[23];
    if protocol != IP_PROTO_TCP && protocol != IP_PROTO_UDP {
        return None;
    }
    let ihl = (frame[14] & 0x0f) as usize * 4;
    let transport_start = ETH_HDR_LEN + ihl;
    if frame.len() < transport_start + 4 {
        return None;
    }
    let src_port = u16::from_be_bytes([frame[transport_start], frame[transport_start + 1]]);
    let dst_port =
        u16::from_be_bytes([frame[transport_start + 2], frame[transport_start + 3]]);
    Some((src_port, dst_port))
}

/// Extract TCP flags from an Ethernet frame (without vnet header).
/// Returns the flags byte (offset 13 in TCP header): FIN=0x01, SYN=0x02, RST=0x04, PSH=0x08, ACK=0x10, URG=0x20.
pub fn extract_tcp_flags(frame: &[u8]) -> Option<u8> {
    if frame.len() < ETH_HDR_LEN + 20 {
        return None;
    }
    let ethertype = u16::from_be_bytes([frame[12], frame[13]]);
    if ethertype != ETHERTYPE_IPV4 {
        return None;
    }
    let protocol = frame[23];
    if protocol != IP_PROTO_TCP {
        return None;
    }
    let ihl = (frame[14] & 0x0f) as usize * 4;
    let tcp_start = ETH_HDR_LEN + ihl;
    if frame.len() < tcp_start + 14 {
        return None;
    }
    Some(frame[tcp_start + 13])
}

/// Format TCP flags byte as human-readable string (e.g. "[SYN ACK]").
pub fn format_tcp_flags(flags: u8) -> String {
    let mut parts = Vec::new();
    if flags & 0x02 != 0 { parts.push("SYN"); }
    if flags & 0x10 != 0 { parts.push("ACK"); }
    if flags & 0x08 != 0 { parts.push("PSH"); }
    if flags & 0x01 != 0 { parts.push("FIN"); }
    if flags & 0x04 != 0 { parts.push("RST"); }
    if flags & 0x20 != 0 { parts.push("URG"); }
    format!("[{}]", parts.join(" "))
}

/// Format a MAC address for logging.
pub fn format_mac(bytes: &[u8; 6]) -> String {
    format!(
        "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5]
    )
}

// ---------------------------------------------------------------------------
// Free functions — fabric frame helpers (operate on full vnet+eth frames)
// ---------------------------------------------------------------------------

/// Wrap a raw IP packet into a fabric frame: `[vnet_hdr(10)][eth_hdr(14)][ip_packet]`.
pub fn ip_to_fabric_frame(ip_packet: &[u8], src_mac: &[u8; 6], dst_mac: &[u8; 6]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(VNET_HDR_SZ + ETH_HDR_LEN + ip_packet.len());
    frame.extend_from_slice(&[0u8; VNET_HDR_SZ]);
    frame.extend_from_slice(dst_mac);
    frame.extend_from_slice(src_mac);
    frame.extend_from_slice(&ETHERTYPE_IPV4.to_be_bytes());
    frame.extend_from_slice(ip_packet);
    frame
}

/// Extract the raw IP packet from a fabric frame, stripping vnet_hdr + ethernet header.
///
/// Returns `None` if the frame is too short or not IPv4 (ethertype 0x0800).
pub fn fabric_frame_to_ip(frame: &[u8]) -> Option<&[u8]> {
    if frame.len() < VNET_HDR_SZ + ETH_HDR_LEN {
        return None;
    }
    let ethertype = u16::from_be_bytes([frame[VNET_HDR_SZ + 12], frame[VNET_HDR_SZ + 13]]);
    if ethertype != ETHERTYPE_IPV4 {
        return None;
    }
    Some(&frame[VNET_HDR_SZ + ETH_HDR_LEN..])
}

/// Extract the ethertype from a fabric frame.
pub fn fabric_frame_ethertype(frame: &[u8]) -> Option<u16> {
    if frame.len() < VNET_HDR_SZ + ETH_HDR_LEN {
        return None;
    }
    Some(u16::from_be_bytes([
        frame[VNET_HDR_SZ + 12],
        frame[VNET_HDR_SZ + 13],
    ]))
}

/// Extract the destination IPv4 address from a raw IP packet.
pub fn ip_packet_dst(packet: &[u8]) -> Option<Ipv4Addr> {
    if packet.len() < 20 {
        return None;
    }
    Some(Ipv4Addr::new(
        packet[16],
        packet[17],
        packet[18],
        packet[19],
    ))
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

// ---------------------------------------------------------------------------
// IP address rewriting with incremental checksum updates
// ---------------------------------------------------------------------------

/// Virtio-net header flags: VIRTIO_NET_HDR_F_NEEDS_CSUM
const VIRTIO_NET_HDR_F_NEEDS_CSUM: u8 = 1;

/// Rewrite the destination IPv4 address in a fabric frame (with vnet header)
/// and update the IP header checksum incrementally.
///
/// Also adjusts the transport partial checksum if `VIRTIO_NET_HDR_F_NEEDS_CSUM`
/// is set in the vnet header.
pub fn rewrite_ipv4_dst(frame: &mut [u8], old_ip: Ipv4Addr, new_ip: Ipv4Addr) {
    let eth_start = VNET_HDR_SZ;
    let ip_start = eth_start + ETH_HDR_LEN;
    if frame.len() < ip_start + 20 {
        return;
    }

    let old_octets = old_ip.octets();
    let new_octets = new_ip.octets();

    // Dst IP is at IP header offset 16..20
    let dst_off = ip_start + 16;
    frame[dst_off..dst_off + 4].copy_from_slice(&new_octets);

    update_ip_header_csum(frame, ip_start, &old_octets, &new_octets);
    update_transport_csum_for_ip_change(frame, &old_octets, &new_octets);
}

/// Rewrite the source IPv4 address in a fabric frame (with vnet header)
/// and update the IP header checksum incrementally.
///
/// Also adjusts the transport partial checksum if `VIRTIO_NET_HDR_F_NEEDS_CSUM`
/// is set in the vnet header.
pub fn rewrite_ipv4_src(frame: &mut [u8], old_ip: Ipv4Addr, new_ip: Ipv4Addr) {
    let eth_start = VNET_HDR_SZ;
    let ip_start = eth_start + ETH_HDR_LEN;
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
fn update_ip_header_csum(
    frame: &mut [u8],
    ip_start: usize,
    old_octets: &[u8; 4],
    new_octets: &[u8; 4],
) {
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

/// Adjust the transport-layer checksum when an IP address changes.
///
/// Handles two cases:
/// 1. **NEEDS_CSUM set**: the checksum field contains a raw pseudo-header
///    partial *sum* (not complemented). Updated with direct one's-complement
///    addition (`partial - old + new`).
/// 2. **NEEDS_CSUM not set**: the checksum field contains a completed
///    (complemented) checksum. Updated with the RFC 1624 incremental formula.
fn update_transport_csum_for_ip_change(
    frame: &mut [u8],
    old_octets: &[u8; 4],
    new_octets: &[u8; 4],
) {
    if frame.len() < VNET_HDR_SZ {
        return;
    }
    let flags = frame[0];

    if flags & VIRTIO_NET_HDR_F_NEEDS_CSUM != 0 {
        // Partial checksum path: use csum_start/csum_offset from vnet header.
        let csum_start = u16::from_le_bytes([frame[6], frame[7]]) as usize;
        let csum_offset = u16::from_le_bytes([frame[8], frame[9]]) as usize;

        let abs_csum_pos = VNET_HDR_SZ + csum_start + csum_offset;
        if frame.len() < abs_csum_pos + 2 {
            return;
        }

        let old_partial = u16::from_be_bytes([frame[abs_csum_pos], frame[abs_csum_pos + 1]]);

        let old_hi = u16::from_be_bytes([old_octets[0], old_octets[1]]);
        let old_lo = u16::from_be_bytes([old_octets[2], old_octets[3]]);
        let new_hi = u16::from_be_bytes([new_octets[0], new_octets[1]]);
        let new_lo = u16::from_be_bytes([new_octets[2], new_octets[3]]);

        let partial = incremental_partial_update(old_partial, old_hi, new_hi);
        let partial = incremental_partial_update(partial, old_lo, new_lo);

        frame[abs_csum_pos..abs_csum_pos + 2].copy_from_slice(&partial.to_be_bytes());
    } else {
        // Completed checksum path: find transport checksum by IP protocol.
        let eth_start = VNET_HDR_SZ;
        let ip_start = eth_start + ETH_HDR_LEN;
        if frame.len() < ip_start + 20 {
            return;
        }
        let ethertype = u16::from_be_bytes([frame[eth_start + 12], frame[eth_start + 13]]);
        if ethertype != ETHERTYPE_IPV4 {
            return;
        }
        let ihl = (frame[ip_start] & 0x0f) as usize * 4;
        let protocol = frame[ip_start + 9];
        let transport_start = ip_start + ihl;

        let csum_offset = match protocol {
            IP_PROTO_TCP => 16,
            IP_PROTO_UDP => 6,
            _ => return,
        };
        let abs_csum_pos = transport_start + csum_offset;
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
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packet::{ETH_HDR_LEN, VNET_HDR_SZ};

    #[test]
    fn round_trip_ip_to_fabric_frame() {
        let ip_packet = &[
            0x45, 0x00, 0x00, 0x1c, 0x00, 0x00, 0x00, 0x00, 0x40, 0x11, 0x00, 0x00, 0xac, 0x10,
            0x00, 0x02, 0xac, 0x10, 0x00, 0x03,
        ];
        let src_mac = [0x02, 0xaa, 0xbb, 0xcc, 0xdd, 0xee];
        let dst_mac = [0x02, 0x11, 0x22, 0x33, 0x44, 0x55];

        let frame = ip_to_fabric_frame(ip_packet, &src_mac, &dst_mac);
        assert_eq!(frame.len(), VNET_HDR_SZ + ETH_HDR_LEN + ip_packet.len());
        assert_eq!(&frame[..VNET_HDR_SZ], &[0u8; VNET_HDR_SZ]);
        assert_eq!(&frame[VNET_HDR_SZ..VNET_HDR_SZ + 6], &dst_mac);
        assert_eq!(&frame[VNET_HDR_SZ + 6..VNET_HDR_SZ + 12], &src_mac);
        assert_eq!(&frame[VNET_HDR_SZ + 12..VNET_HDR_SZ + 14], &[0x08, 0x00]);

        let extracted = fabric_frame_to_ip(&frame).unwrap();
        assert_eq!(extracted, ip_packet);
    }

    #[test]
    fn fabric_frame_to_ip_rejects_non_ipv4() {
        let mut frame = vec![0u8; VNET_HDR_SZ + ETH_HDR_LEN + 4];
        frame[VNET_HDR_SZ + 12] = 0x08;
        frame[VNET_HDR_SZ + 13] = 0x06;
        assert!(fabric_frame_to_ip(&frame).is_none());
    }

    #[test]
    fn fabric_frame_to_ip_rejects_short() {
        let frame = vec![0u8; VNET_HDR_SZ + ETH_HDR_LEN - 1];
        assert!(fabric_frame_to_ip(&frame).is_none());
    }

    // --- extract_ipv4_dst tests ---

    #[test]
    fn extract_ipv4_dst_valid_ipv4_frame() {
        let mut frame = [0u8; 34];
        frame[12] = 0x08;
        frame[13] = 0x00;
        frame[30] = 192;
        frame[31] = 168;
        frame[32] = 1;
        frame[33] = 42;
        assert_eq!(
            extract_ipv4_dst(&frame),
            Some(Ipv4Addr::new(192, 168, 1, 42))
        );
    }

    #[test]
    fn extract_ipv4_dst_non_ipv4_ethertype() {
        let mut frame = [0u8; 34];
        frame[12] = 0x08;
        frame[13] = 0x06;
        assert_eq!(extract_ipv4_dst(&frame), None);
    }

    #[test]
    fn extract_ipv4_dst_frame_too_short() {
        let frame = [0u8; 33];
        assert_eq!(extract_ipv4_dst(&frame), None);
    }

    // --- format_mac tests ---

    #[test]
    fn format_mac_known() {
        assert_eq!(
            format_mac(&[0x02, 0x00, 0x00, 0x00, 0x00, 0x01]),
            "02:00:00:00:00:01"
        );
    }

    #[test]
    fn format_mac_broadcast() {
        assert_eq!(format_mac(&[0xff; 6]), "ff:ff:ff:ff:ff:ff");
    }

    // --- FabricFrame tests ---

    #[test]
    fn fabric_frame_rejects_too_short() {
        assert!(FabricFrame::new(&[0u8; 23]).is_none());
        assert!(FabricFrame::new(&[]).is_none());
    }

    #[test]
    fn fabric_frame_accepts_minimum_size() {
        let buf = [0u8; 24];
        assert!(FabricFrame::new(&buf).is_some());
    }

    #[test]
    fn fabric_frame_accessors() {
        let mut buf = [0u8; 30];
        buf[10..16].copy_from_slice(&[0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff]);
        buf[16..22].copy_from_slice(&[0x11, 0x22, 0x33, 0x44, 0x55, 0x66]);
        buf[22..24].copy_from_slice(&[0x08, 0x00]);

        let ff = FabricFrame::new(&buf).unwrap();
        assert_eq!(ff.dst_mac(), [0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff]);
        assert_eq!(ff.src_mac(), [0x11, 0x22, 0x33, 0x44, 0x55, 0x66]);
        assert_eq!(ff.ethertype(), 0x0800);
        assert_eq!(ff.len(), 30);
        assert_eq!(ff.eth_payload().len(), 20);
        assert_eq!(ff.vnet_hdr(), [0u8; VNET_HDR_SZ]);
    }

    #[test]
    fn fabric_frame_ipv4_dst() {
        let mut buf = [0u8; 44];
        buf[22..24].copy_from_slice(&[0x08, 0x00]);
        buf[40] = 192;
        buf[41] = 168;
        buf[42] = 1;
        buf[43] = 42;

        let ff = FabricFrame::new(&buf).unwrap();
        assert_eq!(ff.ipv4_dst(), Some(Ipv4Addr::new(192, 168, 1, 42)));
    }

    #[test]
    fn fabric_frame_ipv4_dst_non_ipv4() {
        let mut buf = [0u8; 44];
        buf[22..24].copy_from_slice(&[0x08, 0x06]);
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
        frame[10..16].copy_from_slice(&[0x01; 6]);
        let new_mac = [0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff];
        rewrite_dst_mac(&mut frame, &new_mac);
        assert_eq!(&frame[VNET_HDR_SZ..VNET_HDR_SZ + 6], &new_mac);
    }

    // --- rewrite checksum oracle tests using etherparse ---

    fn verify_ip_header_checksum(ip_hdr: &[u8]) -> bool {
        let ihl = (ip_hdr[0] & 0x0f) as usize * 4;
        let hdr = &ip_hdr[..ihl];
        let mut sum: u32 = 0;
        for i in (0..hdr.len()).step_by(2) {
            sum += u16::from_be_bytes([hdr[i], hdr[i + 1]]) as u32;
        }
        while sum >> 16 != 0 {
            sum = (sum & 0xffff) + (sum >> 16);
        }
        sum as u16 == 0xffff
    }

    fn build_tcp_fabric_frame(
        src_ip: [u8; 4],
        dst_ip: [u8; 4],
        src_port: u16,
        dst_port: u16,
        payload: &[u8],
    ) -> Vec<u8> {
        use etherparse::PacketBuilder;
        let src_mac = [0x02, 0xaa, 0xbb, 0xcc, 0xdd, 0x01];
        let dst_mac = [0x02, 0xaa, 0xbb, 0xcc, 0xdd, 0x02];
        let builder = PacketBuilder::ethernet2(src_mac, dst_mac)
            .ipv4(src_ip, dst_ip, 64)
            .tcp(src_port, dst_port, 1000, 65535);
        let mut eth_frame = Vec::new();
        builder.write(&mut eth_frame, payload).unwrap();
        with_vnet_header(&eth_frame)
    }

    fn tcp_pseudo_header_csum(src_ip: [u8; 4], dst_ip: [u8; 4], tcp_len: u16) -> u16 {
        let mut sum: u32 = 0;
        sum += u16::from_be_bytes([src_ip[0], src_ip[1]]) as u32;
        sum += u16::from_be_bytes([src_ip[2], src_ip[3]]) as u32;
        sum += u16::from_be_bytes([dst_ip[0], dst_ip[1]]) as u32;
        sum += u16::from_be_bytes([dst_ip[2], dst_ip[3]]) as u32;
        sum += 6u32;
        sum += tcp_len as u32;
        while sum >> 16 != 0 {
            sum = (sum & 0xffff) + (sum >> 16);
        }
        sum as u16
    }

    fn build_tcp_fabric_frame_needs_csum(
        src_ip: [u8; 4],
        dst_ip: [u8; 4],
        src_port: u16,
        dst_port: u16,
        payload: &[u8],
    ) -> Vec<u8> {
        use etherparse::PacketBuilder;
        let src_mac = [0x02, 0xaa, 0xbb, 0xcc, 0xdd, 0x01];
        let dst_mac = [0x02, 0xaa, 0xbb, 0xcc, 0xdd, 0x02];
        let builder = PacketBuilder::ethernet2(src_mac, dst_mac)
            .ipv4(src_ip, dst_ip, 64)
            .tcp(src_port, dst_port, 1000, 65535);
        let mut eth_frame = Vec::new();
        builder.write(&mut eth_frame, payload).unwrap();

        let ihl = (eth_frame[14] & 0x0f) as usize * 4;
        let tcp_start = ETH_HDR_LEN + ihl;

        let mut frame = Vec::with_capacity(VNET_HDR_SZ + eth_frame.len());
        frame.push(1u8); // flags = NEEDS_CSUM
        frame.push(0);
        frame.extend_from_slice(&[0, 0]);
        frame.extend_from_slice(&[0, 0]);
        frame.extend_from_slice(&(tcp_start as u16).to_le_bytes());
        frame.extend_from_slice(&16u16.to_le_bytes());
        frame.extend_from_slice(&eth_frame);

        let ip_total_len = u16::from_be_bytes([eth_frame[16], eth_frame[17]]);
        let tcp_len = ip_total_len - (ihl as u16);
        let pseudo = tcp_pseudo_header_csum(src_ip, dst_ip, tcp_len);
        let tcp_csum_abs = VNET_HDR_SZ + tcp_start + 16;
        frame[tcp_csum_abs] = (pseudo >> 8) as u8;
        frame[tcp_csum_abs + 1] = (pseudo & 0xff) as u8;

        frame
    }

    fn verify_tcp_checksum(frame: &[u8]) -> bool {
        let ip_start = VNET_HDR_SZ + ETH_HDR_LEN;
        let ihl = (frame[ip_start] & 0x0f) as usize * 4;
        let tcp_start = ip_start + ihl;
        let src_ip = &frame[ip_start + 12..ip_start + 16];
        let dst_ip = &frame[ip_start + 16..ip_start + 20];
        let tcp_data = &frame[tcp_start..];
        let tcp_len = tcp_data.len() as u16;

        let mut sum: u32 = 0;
        sum += u16::from_be_bytes([src_ip[0], src_ip[1]]) as u32;
        sum += u16::from_be_bytes([src_ip[2], src_ip[3]]) as u32;
        sum += u16::from_be_bytes([dst_ip[0], dst_ip[1]]) as u32;
        sum += u16::from_be_bytes([dst_ip[2], dst_ip[3]]) as u32;
        sum += 6u32;
        sum += tcp_len as u32;

        let mut i = 0;
        while i + 1 < tcp_data.len() {
            sum += u16::from_be_bytes([tcp_data[i], tcp_data[i + 1]]) as u32;
            i += 2;
        }
        if i < tcp_data.len() {
            sum += (tcp_data[i] as u32) << 8;
        }
        while sum >> 16 != 0 {
            sum = (sum & 0xffff) + (sum >> 16);
        }
        sum as u16 == 0xffff
    }

    #[test]
    fn test_rewrite_ipv4_dst_ip_checksum_valid() {
        let src_ip = [10, 0, 0, 1];
        let old_dst = [10, 0, 0, 2];
        let new_dst = [10, 0, 0, 99];
        let mut frame = build_tcp_fabric_frame(src_ip, old_dst, 12345, 80, &[]);

        rewrite_ipv4_dst(
            &mut frame,
            Ipv4Addr::from(old_dst),
            Ipv4Addr::from(new_dst),
        );

        let ip_start = VNET_HDR_SZ + ETH_HDR_LEN;
        assert!(verify_ip_header_checksum(&frame[ip_start..]));
    }

    #[test]
    fn test_rewrite_ipv4_src_ip_checksum_valid() {
        let old_src = [10, 0, 0, 1];
        let new_src = [10, 0, 0, 50];
        let dst_ip = [10, 0, 0, 2];
        let mut frame = build_tcp_fabric_frame(old_src, dst_ip, 12345, 80, &[]);

        rewrite_ipv4_src(
            &mut frame,
            Ipv4Addr::from(old_src),
            Ipv4Addr::from(new_src),
        );

        let ip_start = VNET_HDR_SZ + ETH_HDR_LEN;
        assert!(verify_ip_header_checksum(&frame[ip_start..]));
    }

    #[test]
    fn test_rewrite_ipv4_dst_transport_csum_valid() {
        use crate::packet::complete_checksum;

        let src_ip = [172, 16, 0, 5];
        let old_dst = [172, 16, 0, 10];
        let new_dst = [172, 16, 0, 99];
        let mut frame =
            build_tcp_fabric_frame_needs_csum(src_ip, old_dst, 9000, 443, b"hello");

        rewrite_ipv4_dst(
            &mut frame,
            Ipv4Addr::from(old_dst),
            Ipv4Addr::from(new_dst),
        );
        complete_checksum(&mut frame);

        let ip_start = VNET_HDR_SZ + ETH_HDR_LEN;
        assert!(verify_ip_header_checksum(&frame[ip_start..]));
        assert!(verify_tcp_checksum(&frame));
    }

    #[test]
    fn test_rewrite_both_src_dst_checksums_valid() {
        use crate::packet::complete_checksum;

        let old_src = [172, 16, 0, 5];
        let old_dst = [172, 16, 0, 10];
        let new_src = [10, 0, 0, 1];
        let new_dst = [10, 0, 0, 2];
        let mut frame =
            build_tcp_fabric_frame_needs_csum(old_src, old_dst, 5555, 8080, b"test data");

        rewrite_ipv4_src(
            &mut frame,
            Ipv4Addr::from(old_src),
            Ipv4Addr::from(new_src),
        );
        rewrite_ipv4_dst(
            &mut frame,
            Ipv4Addr::from(old_dst),
            Ipv4Addr::from(new_dst),
        );
        complete_checksum(&mut frame);

        let ip_start = VNET_HDR_SZ + ETH_HDR_LEN;
        assert!(verify_ip_header_checksum(&frame[ip_start..]));
        assert!(verify_tcp_checksum(&frame));
    }

    #[test]
    fn test_rewrite_ipv4_dst_no_needs_csum_tcp_checksum_valid() {
        let src_ip = [10, 0, 0, 3];
        let old_dst = [10, 0, 0, 99];
        let new_dst = [10, 0, 0, 2];
        let mut frame =
            build_tcp_fabric_frame(src_ip, old_dst, 45678, 80, b"hello-buffered");

        assert!(verify_tcp_checksum(&frame));

        rewrite_ipv4_dst(
            &mut frame,
            Ipv4Addr::from(old_dst),
            Ipv4Addr::from(new_dst),
        );

        let ip_start = VNET_HDR_SZ + ETH_HDR_LEN;
        assert!(verify_ip_header_checksum(&frame[ip_start..]));
        assert!(verify_tcp_checksum(&frame));
    }

    #[test]
    fn test_rewrite_ipv4_src_no_needs_csum_tcp_checksum_valid() {
        let old_src = [10, 0, 0, 2];
        let new_src = [10, 0, 0, 99];
        let dst_ip = [10, 0, 0, 3];
        let mut frame = build_tcp_fabric_frame(old_src, dst_ip, 80, 45678, b"response");

        assert!(verify_tcp_checksum(&frame));

        rewrite_ipv4_src(
            &mut frame,
            Ipv4Addr::from(old_src),
            Ipv4Addr::from(new_src),
        );

        let ip_start = VNET_HDR_SZ + ETH_HDR_LEN;
        assert!(verify_ip_header_checksum(&frame[ip_start..]));
        assert!(verify_tcp_checksum(&frame));
    }

    #[test]
    fn test_dnat_snat_round_trip_no_needs_csum() {
        let client_ip = [10, 0, 0, 3];
        let service_ip = [10, 0, 0, 99];
        let backend_ip = [10, 0, 0, 2];

        let mut syn_frame = build_tcp_fabric_frame(client_ip, service_ip, 45678, 80, &[]);
        assert!(verify_tcp_checksum(&syn_frame));

        rewrite_ipv4_dst(
            &mut syn_frame,
            Ipv4Addr::from(service_ip),
            Ipv4Addr::from(backend_ip),
        );

        let ip_start = VNET_HDR_SZ + ETH_HDR_LEN;
        assert!(verify_ip_header_checksum(&syn_frame[ip_start..]));
        assert!(verify_tcp_checksum(&syn_frame));

        let mut synack_frame = build_tcp_fabric_frame(backend_ip, client_ip, 80, 45678, &[]);
        assert!(verify_tcp_checksum(&synack_frame));

        rewrite_ipv4_src(
            &mut synack_frame,
            Ipv4Addr::from(backend_ip),
            Ipv4Addr::from(service_ip),
        );

        assert!(verify_ip_header_checksum(&synack_frame[ip_start..]));
        assert!(verify_tcp_checksum(&synack_frame));
    }

    #[test]
    fn test_dnat_snat_round_trip_needs_csum() {
        use crate::packet::complete_checksum;

        let client_ip = [10, 0, 0, 3];
        let service_ip = [10, 0, 0, 99];
        let backend_ip = [10, 0, 0, 2];

        let mut syn_frame =
            build_tcp_fabric_frame_needs_csum(client_ip, service_ip, 45678, 80, &[]);

        rewrite_ipv4_dst(
            &mut syn_frame,
            Ipv4Addr::from(service_ip),
            Ipv4Addr::from(backend_ip),
        );

        let ip_start = VNET_HDR_SZ + ETH_HDR_LEN;
        assert!(verify_ip_header_checksum(&syn_frame[ip_start..]));

        complete_checksum(&mut syn_frame);
        assert!(verify_tcp_checksum(&syn_frame));

        let mut synack_frame =
            build_tcp_fabric_frame_needs_csum(backend_ip, client_ip, 80, 45678, &[]);

        rewrite_ipv4_src(
            &mut synack_frame,
            Ipv4Addr::from(backend_ip),
            Ipv4Addr::from(service_ip),
        );

        assert!(verify_ip_header_checksum(&synack_frame[ip_start..]));

        complete_checksum(&mut synack_frame);
        assert!(verify_tcp_checksum(&synack_frame));
    }

    #[test]
    fn test_dnat_partial_checksum_correct() {
        let src_ip = [10, 0, 0, 3];
        let old_dst = [10, 0, 0, 99];
        let new_dst = [10, 0, 0, 2];
        let mut frame =
            build_tcp_fabric_frame_needs_csum(src_ip, old_dst, 45678, 80, b"payload");

        rewrite_ipv4_dst(
            &mut frame,
            Ipv4Addr::from(old_dst),
            Ipv4Addr::from(new_dst),
        );

        let ip_start = VNET_HDR_SZ + ETH_HDR_LEN;
        let ihl = (frame[ip_start] & 0x0f) as usize * 4;
        let tcp_start = ip_start + ihl;
        let actual_partial = u16::from_be_bytes([frame[tcp_start + 16], frame[tcp_start + 17]]);

        let ip_total_len = u16::from_be_bytes([frame[ip_start + 2], frame[ip_start + 3]]);
        let tcp_len = ip_total_len - (ihl as u16);
        let expected_partial = tcp_pseudo_header_csum(src_ip, new_dst, tcp_len);

        assert_eq!(actual_partial, expected_partial);
    }
}
