//! Fabric packet wrappers and free functions for IP packet field extraction and mutation.
//!
//! The internal fabric format is `[fabric_hdr(3)][IP packet]` — no Ethernet header.
//! Ethernet framing is only added at TAP device boundaries where guest VMs need it.

use std::net::Ipv4Addr;

use zerocopy::Ref;

use super::checksum::{incremental_csum_update, incremental_partial_update};
use super::{FABRIC_HDR_SZ, FLAG_NEEDS_CSUM, FabricHeader, IP_PROTO_TCP, IP_PROTO_UDP};

// ---------------------------------------------------------------------------
// FabricPacket (immutable)
// ---------------------------------------------------------------------------

/// Zero-copy wrapper over a raw fabric packet: `[fabric_hdr(3)][IP packet]`.
///
/// Created via `FabricPacket::new()` which validates minimum size
/// (`FABRIC_HDR_SZ + 20` = 23 bytes for IPv4 header). All accessor methods
/// are safe to call on a validated packet.
pub struct FabricPacket<'a> {
    raw: &'a [u8],
}

impl<'a> FabricPacket<'a> {
    /// Parse a raw fabric packet. Returns `None` if too short for fabric hdr + minimal IP header.
    pub fn new(raw: &'a [u8]) -> Option<Self> {
        if raw.len() < FABRIC_HDR_SZ + 20 {
            return None;
        }
        Some(FabricPacket { raw })
    }

    /// Total packet length including fabric header.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.raw.len()
    }

    /// The raw bytes.
    pub fn as_bytes(&self) -> &'a [u8] {
        self.raw
    }

    /// The fabric header as a typed reference.
    pub fn fabric_header(&self) -> &FabricHeader {
        let (hdr, _) = Ref::<_, FabricHeader>::from_prefix(self.raw).unwrap();
        Ref::into_ref(hdr)
    }

    /// The IP packet (everything after the fabric header).
    pub fn ip_packet(&self) -> &'a [u8] {
        &self.raw[FABRIC_HDR_SZ..]
    }

    /// Extract destination IPv4 address.
    pub fn ipv4_dst(&self) -> Ipv4Addr {
        let ip = &self.raw[FABRIC_HDR_SZ..];
        Ipv4Addr::new(ip[16], ip[17], ip[18], ip[19])
    }

    /// Extract source IPv4 address.
    pub fn ipv4_src(&self) -> Ipv4Addr {
        let ip = &self.raw[FABRIC_HDR_SZ..];
        Ipv4Addr::new(ip[12], ip[13], ip[14], ip[15])
    }

    /// Extract IP protocol number.
    pub fn ip_protocol(&self) -> u8 {
        self.raw[FABRIC_HDR_SZ + 9]
    }

    /// Extract transport-layer source and destination ports (TCP/UDP only).
    pub fn transport_ports(&self) -> Option<(u16, u16)> {
        let protocol = self.ip_protocol();
        if protocol != IP_PROTO_TCP && protocol != IP_PROTO_UDP {
            return None;
        }
        // IPv4 IHL (Internet Header Length) is the low 4 bits of the first
        // byte, encoding the header length in 4-byte words. Multiply by 4 to
        // get the byte offset where the transport header starts.
        let ihl = (self.raw[FABRIC_HDR_SZ] & 0x0f) as usize * 4;
        let transport_start = FABRIC_HDR_SZ + ihl;
        if self.raw.len() < transport_start + 4 {
            return None;
        }
        let src_port =
            u16::from_be_bytes([self.raw[transport_start], self.raw[transport_start + 1]]);
        let dst_port =
            u16::from_be_bytes([self.raw[transport_start + 2], self.raw[transport_start + 3]]);
        Some((src_port, dst_port))
    }

    /// Extract TCP flags byte. Returns None if not TCP.
    pub fn tcp_flags(&self) -> Option<u8> {
        if self.ip_protocol() != IP_PROTO_TCP {
            return None;
        }
        let ihl = (self.raw[FABRIC_HDR_SZ] & 0x0f) as usize * 4;
        let tcp_start = FABRIC_HDR_SZ + ihl;
        if self.raw.len() < tcp_start + 14 {
            return None;
        }
        Some(self.raw[tcp_start + 13])
    }
}

// ---------------------------------------------------------------------------
// FabricPacketMut (mutable)
// ---------------------------------------------------------------------------

/// Mutable zero-copy wrapper over a raw fabric packet.
///
/// Provides all read accessors from `FabricPacket` plus mutation helpers
/// for IP rewriting with incremental checksum updates.
pub struct FabricPacketMut<'a> {
    raw: &'a mut [u8],
}

#[allow(dead_code)]
impl<'a> FabricPacketMut<'a> {
    /// Parse a mutable raw fabric packet. Returns `None` if too short.
    pub fn new(raw: &'a mut [u8]) -> Option<Self> {
        if raw.len() < FABRIC_HDR_SZ + 20 {
            return None;
        }
        Some(FabricPacketMut { raw })
    }

    /// Total packet length including fabric header.
    pub fn len(&self) -> usize {
        self.raw.len()
    }

    /// The IP packet (everything after the fabric header).
    pub fn ip_packet(&self) -> &[u8] {
        &self.raw[FABRIC_HDR_SZ..]
    }

    /// Extract destination IPv4 address.
    pub fn ipv4_dst(&self) -> Ipv4Addr {
        let ip = &self.raw[FABRIC_HDR_SZ..];
        Ipv4Addr::new(ip[16], ip[17], ip[18], ip[19])
    }

    /// Extract source IPv4 address.
    pub fn ipv4_src(&self) -> Ipv4Addr {
        let ip = &self.raw[FABRIC_HDR_SZ..];
        Ipv4Addr::new(ip[12], ip[13], ip[14], ip[15])
    }

    /// Extract IP protocol number.
    pub fn ip_protocol(&self) -> u8 {
        self.raw[FABRIC_HDR_SZ + 9]
    }

    /// Extract transport-layer source and destination ports (TCP/UDP only).
    pub fn transport_ports(&self) -> Option<(u16, u16)> {
        let protocol = self.ip_protocol();
        if protocol != IP_PROTO_TCP && protocol != IP_PROTO_UDP {
            return None;
        }
        let ihl = (self.raw[FABRIC_HDR_SZ] & 0x0f) as usize * 4;
        let transport_start = FABRIC_HDR_SZ + ihl;
        if self.raw.len() < transport_start + 4 {
            return None;
        }
        let src_port =
            u16::from_be_bytes([self.raw[transport_start], self.raw[transport_start + 1]]);
        let dst_port =
            u16::from_be_bytes([self.raw[transport_start + 2], self.raw[transport_start + 3]]);
        Some((src_port, dst_port))
    }

    /// Rewrite the destination IPv4 address and update IP + transport checksums.
    pub fn rewrite_ipv4_dst(&mut self, old_ip: Ipv4Addr, new_ip: Ipv4Addr) {
        rewrite_ipv4_dst(self.raw, old_ip, new_ip);
    }

    /// Rewrite the source IPv4 address and update IP + transport checksums.
    pub fn rewrite_ipv4_src(&mut self, old_ip: Ipv4Addr, new_ip: Ipv4Addr) {
        rewrite_ipv4_src(self.raw, old_ip, new_ip);
    }

    /// Complete deferred checksum offload.
    pub fn complete_checksum(&mut self) {
        super::checksum::complete_checksum(self.raw);
    }
}

// ---------------------------------------------------------------------------
// Free functions — IP packet field extraction (operate on raw IP packets, no headers)
// ---------------------------------------------------------------------------

/// Extract the destination IPv4 address from a raw IP packet (no fabric/eth headers).
pub fn ip_packet_dst(packet: &[u8]) -> Option<Ipv4Addr> {
    if packet.len() < 20 {
        return None;
    }
    Some(Ipv4Addr::new(
        packet[16], packet[17], packet[18], packet[19],
    ))
}

/// Extract the source IPv4 address from a raw IP packet (no fabric/eth headers).
pub fn ip_packet_src(packet: &[u8]) -> Option<Ipv4Addr> {
    if packet.len() < 20 {
        return None;
    }
    Some(Ipv4Addr::new(
        packet[12], packet[13], packet[14], packet[15],
    ))
}

/// Extract the IP protocol number from a raw IP packet (no fabric/eth headers).
pub fn ip_packet_protocol(packet: &[u8]) -> Option<u8> {
    if packet.len() < 20 {
        return None;
    }
    Some(packet[9])
}

/// Extract transport-layer source and destination ports from a raw IP packet.
/// Works for TCP (protocol 6) and UDP (protocol 17).
pub fn ip_packet_transport_ports(packet: &[u8]) -> Option<(u16, u16)> {
    if packet.len() < 20 {
        return None;
    }
    let protocol = packet[9];
    if protocol != IP_PROTO_TCP && protocol != IP_PROTO_UDP {
        return None;
    }
    let ihl = (packet[0] & 0x0f) as usize * 4;
    let transport_start = ihl;
    if packet.len() < transport_start + 4 {
        return None;
    }
    let src_port = u16::from_be_bytes([packet[transport_start], packet[transport_start + 1]]);
    let dst_port = u16::from_be_bytes([packet[transport_start + 2], packet[transport_start + 3]]);
    Some((src_port, dst_port))
}

/// Format TCP flags byte as human-readable string (e.g. "[SYN ACK]").
pub fn format_tcp_flags(flags: u8) -> String {
    let mut parts = Vec::new();
    if flags & 0x02 != 0 {
        parts.push("SYN");
    }
    if flags & 0x10 != 0 {
        parts.push("ACK");
    }
    if flags & 0x08 != 0 {
        parts.push("PSH");
    }
    if flags & 0x01 != 0 {
        parts.push("FIN");
    }
    if flags & 0x04 != 0 {
        parts.push("RST");
    }
    if flags & 0x20 != 0 {
        parts.push("URG");
    }
    format!("[{}]", parts.join(" "))
}

// ---------------------------------------------------------------------------
// Free functions — fabric packet helpers (operate on full fabric_hdr+IP packets)
// ---------------------------------------------------------------------------

/// Build an owned fabric packet by prepending a FabricHeader to an IP packet.
pub fn with_fabric_header(flags: u8, segment_id: u16, ip_packet: &[u8]) -> Vec<u8> {
    use zerocopy::IntoBytes;
    let hdr = FabricHeader {
        flags,
        segment_id: zerocopy::network_endian::U16::new(segment_id),
    };
    let mut packet = Vec::with_capacity(FABRIC_HDR_SZ + ip_packet.len());
    packet.extend_from_slice(hdr.as_bytes());
    packet.extend_from_slice(ip_packet);
    packet
}

// ---------------------------------------------------------------------------
// IP address rewriting with incremental checksum updates
// ---------------------------------------------------------------------------

/// Rewrite the destination IPv4 address in a fabric packet (with fabric header)
/// and update the IP header checksum incrementally.
///
/// Also adjusts the transport partial checksum if `FLAG_NEEDS_CSUM`
/// is set in the fabric header.
pub fn rewrite_ipv4_dst(frame: &mut [u8], old_ip: Ipv4Addr, new_ip: Ipv4Addr) {
    let ip_start = FABRIC_HDR_SZ;
    if frame.len() < ip_start + 20 {
        return;
    }

    let old_octets = old_ip.octets();
    let new_octets = new_ip.octets();

    // Dst IP is at IP header offset 16..20
    let dst_off = ip_start + 16;
    frame[dst_off..dst_off + 4].copy_from_slice(&new_octets);

    update_ip_header_csum(frame, ip_start, &old_octets, &new_octets);
    update_transport_csum_for_ip_change(frame, ip_start, &old_octets, &new_octets);
}

/// Rewrite the source IPv4 address in a fabric packet (with fabric header)
/// and update the IP header checksum incrementally.
///
/// Also adjusts the transport partial checksum if `FLAG_NEEDS_CSUM`
/// is set in the fabric header.
pub fn rewrite_ipv4_src(frame: &mut [u8], old_ip: Ipv4Addr, new_ip: Ipv4Addr) {
    let ip_start = FABRIC_HDR_SZ;
    if frame.len() < ip_start + 20 {
        return;
    }

    let old_octets = old_ip.octets();
    let new_octets = new_ip.octets();

    // Src IP is at IP header offset 12..16
    let src_off = ip_start + 12;
    frame[src_off..src_off + 4].copy_from_slice(&new_octets);

    update_ip_header_csum(frame, ip_start, &old_octets, &new_octets);
    update_transport_csum_for_ip_change(frame, ip_start, &old_octets, &new_octets);
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
/// Handles two distinct cases that MUST NOT be confused — using the wrong
/// update algorithm silently produces invalid checksums:
///
/// 1. **NEEDS_CSUM set** (guest used checksum offload): the checksum field
///    contains a raw pseudo-header partial *sum* (NOT one's-complement
///    negated). This is the value the guest kernel computes from the
///    pseudo-header (src IP, dst IP, proto, length) and leaves in the
///    checksum field for the host to complete. Because it's a raw partial
///    sum, we update it with direct one's-complement arithmetic:
///    `partial - old_word + new_word` (via `incremental_partial_update`).
///
/// 2. **NEEDS_CSUM not set** (completed checksum): the checksum field
///    contains a final, complemented checksum. Updated with the standard
///    RFC 1624 incremental formula: `~(~HC + ~m + m')` (via
///    `incremental_csum_update`).
fn update_transport_csum_for_ip_change(
    frame: &mut [u8],
    ip_start: usize,
    old_octets: &[u8; 4],
    new_octets: &[u8; 4],
) {
    if frame.len() < FABRIC_HDR_SZ + 20 {
        return;
    }

    let flags = frame[0];
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

    let old_hi = u16::from_be_bytes([old_octets[0], old_octets[1]]);
    let old_lo = u16::from_be_bytes([old_octets[2], old_octets[3]]);
    let new_hi = u16::from_be_bytes([new_octets[0], new_octets[1]]);
    let new_lo = u16::from_be_bytes([new_octets[2], new_octets[3]]);

    if flags & FLAG_NEEDS_CSUM != 0 {
        // Partial checksum path: derive position from IP header.
        let old_partial = u16::from_be_bytes([frame[abs_csum_pos], frame[abs_csum_pos + 1]]);

        let partial = incremental_partial_update(old_partial, old_hi, new_hi);
        let partial = incremental_partial_update(partial, old_lo, new_lo);

        frame[abs_csum_pos..abs_csum_pos + 2].copy_from_slice(&partial.to_be_bytes());
    } else {
        // Completed checksum path.
        let old_csum = u16::from_be_bytes([frame[abs_csum_pos], frame[abs_csum_pos + 1]]);

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
    use crate::packet::FABRIC_HDR_SZ;

    // --- FabricPacket tests ---

    #[test]
    fn fabric_packet_rejects_too_short() {
        assert!(FabricPacket::new(&[0u8; 22]).is_none()); // FABRIC_HDR_SZ + 20 - 1
        assert!(FabricPacket::new(&[]).is_none());
    }

    #[test]
    fn fabric_packet_accepts_minimum_size() {
        let mut buf = [0u8; 23]; // FABRIC_HDR_SZ + 20
        buf[FABRIC_HDR_SZ] = 0x45; // version=4, IHL=5
        assert!(FabricPacket::new(&buf).is_some());
    }

    #[test]
    fn fabric_packet_accessors() {
        let mut buf = [0u8; 43]; // FABRIC_HDR_SZ + 40
        // IP header at offset FABRIC_HDR_SZ
        buf[FABRIC_HDR_SZ] = 0x45; // version=4, IHL=5
        buf[FABRIC_HDR_SZ + 9] = 6; // TCP
        // src IP at offset 12
        buf[FABRIC_HDR_SZ + 12] = 10;
        buf[FABRIC_HDR_SZ + 13] = 0;
        buf[FABRIC_HDR_SZ + 14] = 0;
        buf[FABRIC_HDR_SZ + 15] = 1;
        // dst IP at offset 16
        buf[FABRIC_HDR_SZ + 16] = 192;
        buf[FABRIC_HDR_SZ + 17] = 168;
        buf[FABRIC_HDR_SZ + 18] = 1;
        buf[FABRIC_HDR_SZ + 19] = 42;

        let fp = FabricPacket::new(&buf).unwrap();
        assert_eq!(fp.ipv4_src(), Ipv4Addr::new(10, 0, 0, 1));
        assert_eq!(fp.ipv4_dst(), Ipv4Addr::new(192, 168, 1, 42));
        assert_eq!(fp.ip_protocol(), 6);
        assert_eq!(fp.len(), 43);
        assert_eq!(fp.ip_packet().len(), 40);
        let hdr = fp.fabric_header();
        assert_eq!(hdr.flags, 0);
        assert_eq!(hdr.segment_id.get(), 0);
    }

    // --- with_fabric_header tests ---

    #[test]
    fn with_fabric_header_prepends_header() {
        let ip = [0x45, 0x00, 0x00];
        let packet = with_fabric_header(0, 0, &ip);
        assert_eq!(packet.len(), FABRIC_HDR_SZ + 3);
        assert_eq!(&packet[..FABRIC_HDR_SZ], &[0u8; FABRIC_HDR_SZ]);
        assert_eq!(&packet[FABRIC_HDR_SZ..], &[0x45, 0x00, 0x00]);
    }

    #[test]
    fn with_fabric_header_flags_and_segment_id() {
        let ip = [0x45];
        let packet = with_fabric_header(FLAG_NEEDS_CSUM, 0x1234, &ip);
        assert_eq!(packet[0], FLAG_NEEDS_CSUM);
        // segment_id is big-endian
        assert_eq!(packet[1], 0x12);
        assert_eq!(packet[2], 0x34);
        assert_eq!(packet[FABRIC_HDR_SZ], 0x45);
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

    /// Build a TCP fabric packet [fabric_hdr][IP+TCP] using etherparse.
    fn build_tcp_fabric_packet(
        src_ip: [u8; 4],
        dst_ip: [u8; 4],
        src_port: u16,
        dst_port: u16,
        payload: &[u8],
    ) -> Vec<u8> {
        use etherparse::PacketBuilder;
        let builder = PacketBuilder::ipv4(src_ip, dst_ip, 64).tcp(src_port, dst_port, 1000, 65535);
        let mut ip_frame = Vec::new();
        builder.write(&mut ip_frame, payload).unwrap();
        with_fabric_header(0, 0, &ip_frame)
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

    fn build_tcp_fabric_packet_needs_csum(
        src_ip: [u8; 4],
        dst_ip: [u8; 4],
        src_port: u16,
        dst_port: u16,
        payload: &[u8],
    ) -> Vec<u8> {
        use etherparse::PacketBuilder;
        let builder = PacketBuilder::ipv4(src_ip, dst_ip, 64).tcp(src_port, dst_port, 1000, 65535);
        let mut ip_frame = Vec::new();
        builder.write(&mut ip_frame, payload).unwrap();

        let ihl = (ip_frame[0] & 0x0f) as usize * 4;
        let tcp_start = ihl;

        // Build fabric header with NEEDS_CSUM flag
        let frame = with_fabric_header(FLAG_NEEDS_CSUM, 0, &ip_frame);
        let mut frame = frame;

        let ip_total_len = u16::from_be_bytes([ip_frame[2], ip_frame[3]]);
        let tcp_len = ip_total_len - (ihl as u16);
        let pseudo = tcp_pseudo_header_csum(src_ip, dst_ip, tcp_len);
        let tcp_csum_abs = FABRIC_HDR_SZ + tcp_start + 16;
        frame[tcp_csum_abs] = (pseudo >> 8) as u8;
        frame[tcp_csum_abs + 1] = (pseudo & 0xff) as u8;

        frame
    }

    fn verify_tcp_checksum(frame: &[u8]) -> bool {
        let ip_start = FABRIC_HDR_SZ;
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
        let mut frame = build_tcp_fabric_packet(src_ip, old_dst, 12345, 80, &[]);

        rewrite_ipv4_dst(&mut frame, Ipv4Addr::from(old_dst), Ipv4Addr::from(new_dst));

        let ip_start = FABRIC_HDR_SZ;
        assert!(verify_ip_header_checksum(&frame[ip_start..]));
    }

    #[test]
    fn test_rewrite_ipv4_src_ip_checksum_valid() {
        let old_src = [10, 0, 0, 1];
        let new_src = [10, 0, 0, 50];
        let dst_ip = [10, 0, 0, 2];
        let mut frame = build_tcp_fabric_packet(old_src, dst_ip, 12345, 80, &[]);

        rewrite_ipv4_src(&mut frame, Ipv4Addr::from(old_src), Ipv4Addr::from(new_src));

        let ip_start = FABRIC_HDR_SZ;
        assert!(verify_ip_header_checksum(&frame[ip_start..]));
    }

    #[test]
    fn test_rewrite_ipv4_dst_transport_csum_valid() {
        use crate::packet::complete_checksum;

        let src_ip = [172, 16, 0, 5];
        let old_dst = [172, 16, 0, 10];
        let new_dst = [172, 16, 0, 99];
        let mut frame = build_tcp_fabric_packet_needs_csum(src_ip, old_dst, 9000, 443, b"hello");

        rewrite_ipv4_dst(&mut frame, Ipv4Addr::from(old_dst), Ipv4Addr::from(new_dst));
        complete_checksum(&mut frame);

        let ip_start = FABRIC_HDR_SZ;
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
            build_tcp_fabric_packet_needs_csum(old_src, old_dst, 5555, 8080, b"test data");

        rewrite_ipv4_src(&mut frame, Ipv4Addr::from(old_src), Ipv4Addr::from(new_src));
        rewrite_ipv4_dst(&mut frame, Ipv4Addr::from(old_dst), Ipv4Addr::from(new_dst));
        complete_checksum(&mut frame);

        let ip_start = FABRIC_HDR_SZ;
        assert!(verify_ip_header_checksum(&frame[ip_start..]));
        assert!(verify_tcp_checksum(&frame));
    }

    #[test]
    fn test_rewrite_ipv4_dst_no_needs_csum_tcp_checksum_valid() {
        let src_ip = [10, 0, 0, 3];
        let old_dst = [10, 0, 0, 99];
        let new_dst = [10, 0, 0, 2];
        let mut frame = build_tcp_fabric_packet(src_ip, old_dst, 45678, 80, b"hello-buffered");

        assert!(verify_tcp_checksum(&frame));

        rewrite_ipv4_dst(&mut frame, Ipv4Addr::from(old_dst), Ipv4Addr::from(new_dst));

        let ip_start = FABRIC_HDR_SZ;
        assert!(verify_ip_header_checksum(&frame[ip_start..]));
        assert!(verify_tcp_checksum(&frame));
    }

    #[test]
    fn test_rewrite_ipv4_src_no_needs_csum_tcp_checksum_valid() {
        let old_src = [10, 0, 0, 2];
        let new_src = [10, 0, 0, 99];
        let dst_ip = [10, 0, 0, 3];
        let mut frame = build_tcp_fabric_packet(old_src, dst_ip, 80, 45678, b"response");

        assert!(verify_tcp_checksum(&frame));

        rewrite_ipv4_src(&mut frame, Ipv4Addr::from(old_src), Ipv4Addr::from(new_src));

        let ip_start = FABRIC_HDR_SZ;
        assert!(verify_ip_header_checksum(&frame[ip_start..]));
        assert!(verify_tcp_checksum(&frame));
    }

    #[test]
    fn test_dnat_snat_round_trip_no_needs_csum() {
        let client_ip = [10, 0, 0, 3];
        let service_ip = [10, 0, 0, 99];
        let backend_ip = [10, 0, 0, 2];

        let mut syn_frame = build_tcp_fabric_packet(client_ip, service_ip, 45678, 80, &[]);
        assert!(verify_tcp_checksum(&syn_frame));

        rewrite_ipv4_dst(
            &mut syn_frame,
            Ipv4Addr::from(service_ip),
            Ipv4Addr::from(backend_ip),
        );

        let ip_start = FABRIC_HDR_SZ;
        assert!(verify_ip_header_checksum(&syn_frame[ip_start..]));
        assert!(verify_tcp_checksum(&syn_frame));

        let mut synack_frame = build_tcp_fabric_packet(backend_ip, client_ip, 80, 45678, &[]);
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
            build_tcp_fabric_packet_needs_csum(client_ip, service_ip, 45678, 80, &[]);

        rewrite_ipv4_dst(
            &mut syn_frame,
            Ipv4Addr::from(service_ip),
            Ipv4Addr::from(backend_ip),
        );

        let ip_start = FABRIC_HDR_SZ;
        assert!(verify_ip_header_checksum(&syn_frame[ip_start..]));

        complete_checksum(&mut syn_frame);
        assert!(verify_tcp_checksum(&syn_frame));

        let mut synack_frame =
            build_tcp_fabric_packet_needs_csum(backend_ip, client_ip, 80, 45678, &[]);

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
        let mut frame = build_tcp_fabric_packet_needs_csum(src_ip, old_dst, 45678, 80, b"payload");

        rewrite_ipv4_dst(&mut frame, Ipv4Addr::from(old_dst), Ipv4Addr::from(new_dst));

        let ip_start = FABRIC_HDR_SZ;
        let ihl = (frame[ip_start] & 0x0f) as usize * 4;
        let tcp_start = ip_start + ihl;
        let actual_partial = u16::from_be_bytes([frame[tcp_start + 16], frame[tcp_start + 17]]);

        let ip_total_len = u16::from_be_bytes([frame[ip_start + 2], frame[ip_start + 3]]);
        let tcp_len = ip_total_len - (ihl as u16);
        let expected_partial = tcp_pseudo_header_csum(src_ip, new_dst, tcp_len);

        assert_eq!(actual_partial, expected_partial);
    }
}
