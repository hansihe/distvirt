//! Internet checksum computation: full, incremental (RFC 1624), and fabric offload completion.

use super::{FABRIC_HDR_SZ, FLAG_NEEDS_CSUM, IP_PROTO_TCP, IP_PROTO_UDP};

/// Compute the internet checksum (one's-complement sum) over `data`.
///
/// Returns the complemented checksum suitable for use in IP/TCP/UDP headers.
pub fn internet_checksum(data: &[u8]) -> u16 {
    let mut sum: u32 = 0;
    let mut i = 0;
    while i + 1 < data.len() {
        sum += u16::from_be_bytes([data[i], data[i + 1]]) as u32;
        i += 2;
    }
    if i < data.len() {
        sum += (data[i] as u32) << 8;
    }
    while sum >> 16 != 0 {
        sum = (sum & 0xFFFF) + (sum >> 16);
    }
    !(sum as u16)
}

/// Incremental checksum update per RFC 1624.
///
/// Given an old *complemented* checksum and a pair of old/new 16-bit words,
/// compute the updated checksum: `~(~HC + ~m + m')`.
pub fn incremental_csum_update(old_csum: u16, old_word: u16, new_word: u16) -> u16 {
    let mut sum: u32 = (!old_csum as u32) + (!old_word as u32) + (new_word as u32);
    while sum >> 16 != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !sum as u16
}

/// Incremental update for a raw partial sum (not a complemented checksum).
///
/// Computes `old_partial - old_word + new_word` in one's-complement arithmetic.
pub fn incremental_partial_update(old_partial: u16, old_word: u16, new_word: u16) -> u16 {
    let mut sum: u32 = (old_partial as u32) + (!old_word as u16 as u32) + (new_word as u32);
    while sum >> 16 != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    sum as u16
}

/// Complete deferred checksum offload if the fabric header has NEEDS_CSUM set.
///
/// Instead of reading csum_start/csum_offset from the header (as with virtio),
/// we derive them from the IP packet:
/// - `csum_start = IHL * 4` (transport header offset relative to IP start)
/// - `csum_offset` = 16 for TCP (checksum field is at TCP header byte 16),
///   6 for UDP (checksum field is at UDP header byte 6)
///
/// Must be called before stripping the fabric header for transmission outside
/// the fabric (e.g. WireGuard, TUN egress).
pub fn complete_checksum(frame: &mut [u8]) {
    if frame.len() < FABRIC_HDR_SZ + 20 {
        return; // too short: need at least fabric header + minimal IP header
    }
    let flags = frame[0];
    if flags & FLAG_NEEDS_CSUM == 0 {
        return;
    }

    let ip_start = FABRIC_HDR_SZ;
    let ihl = (frame[ip_start] & 0x0f) as usize * 4;
    let protocol = frame[ip_start + 9];

    let csum_offset = match protocol {
        IP_PROTO_TCP => 16,
        IP_PROTO_UDP => 6,
        _ => return, // unknown protocol, can't derive checksum position
    };

    // csum_start is the transport header offset relative to IP packet start
    let csum_start = ihl;

    // Absolute positions in the buffer.
    let abs_start = ip_start + csum_start;
    let abs_csum = abs_start + csum_offset;

    if abs_csum + 2 > frame.len() || abs_start > frame.len() {
        return; // malformed
    }

    log::debug!(
        "complete_checksum: flags=0x{:02x} csum_start={} csum_offset={} abs_start={} abs_csum={}",
        flags,
        csum_start,
        csum_offset,
        abs_start,
        abs_csum,
    );

    // Do NOT zero the checksum field. When the guest kernel sets NEEDS_CSUM
    // (virtio checksum offload), it places a pre-computed pseudo-header
    // partial sum in the TCP/UDP checksum field. internet_checksum() folds
    // this partial sum into the final result — zeroing it would discard the
    // pseudo-header contribution and produce an incorrect checksum.
    // This matches Linux's skb_checksum_help() behaviour.

    // Compute internet checksum over [abs_start..end].
    let data = &frame[abs_start..];
    let checksum = internet_checksum(data);

    frame[abs_csum] = (checksum >> 8) as u8;
    frame[abs_csum + 1] = (checksum & 0xFF) as u8;

    // Clear the NEEDS_CSUM flag.
    frame[0] = flags & !FLAG_NEEDS_CSUM;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packet::frame::with_fabric_header;
    use crate::packet::{FABRIC_HDR_SZ, FLAG_NEEDS_CSUM};

    // --- complete_checksum oracle tests using etherparse ---

    /// Build a TCP IP packet using etherparse (correct checksums, no Ethernet header).
    fn build_tcp_ip_packet(
        src_ip: [u8; 4],
        dst_ip: [u8; 4],
        src_port: u16,
        dst_port: u16,
        payload: &[u8],
    ) -> Vec<u8> {
        use etherparse::PacketBuilder;
        let builder = PacketBuilder::ipv4(src_ip, dst_ip, 64).tcp(src_port, dst_port, 1000, 65535);
        let mut ip_packet = Vec::new();
        builder.write(&mut ip_packet, payload).unwrap();
        ip_packet
    }

    /// Compute TCP pseudo-header checksum (partial sum the guest kernel would place
    /// in the TCP checksum field when using NEEDS_CSUM offload).
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

    /// Build a fabric frame [fabric_hdr][IP] with NEEDS_CSUM set and the TCP checksum
    /// replaced by the pseudo-header partial (simulating guest offload).
    fn simulate_guest_needs_csum(ip_packet: &[u8], src_ip: [u8; 4], dst_ip: [u8; 4]) -> Vec<u8> {
        let ihl = (ip_packet[0] & 0x0f) as usize * 4;
        let tcp_start = ihl;

        let mut frame = with_fabric_header(FLAG_NEEDS_CSUM, 0, ip_packet);

        // Compute TCP segment length
        let ip_total_len = u16::from_be_bytes([ip_packet[2], ip_packet[3]]);
        let tcp_len = ip_total_len - (ihl as u16);

        // Replace TCP checksum field with pseudo-header partial
        let tcp_csum_abs = FABRIC_HDR_SZ + tcp_start + 16;
        let pseudo = tcp_pseudo_header_csum(src_ip, dst_ip, tcp_len);
        frame[tcp_csum_abs] = (pseudo >> 8) as u8;
        frame[tcp_csum_abs + 1] = (pseudo & 0xff) as u8;

        frame
    }

    /// Extract TCP checksum from a fabric frame [fabric_hdr][IP].
    fn extract_tcp_checksum(frame: &[u8]) -> u16 {
        let ihl = (frame[FABRIC_HDR_SZ] & 0x0f) as usize * 4;
        let tcp_start = FABRIC_HDR_SZ + ihl;
        u16::from_be_bytes([frame[tcp_start + 16], frame[tcp_start + 17]])
    }

    /// Extract TCP checksum from a raw IP packet (no fabric header).
    fn extract_tcp_checksum_ip(ip_packet: &[u8]) -> u16 {
        let ihl = (ip_packet[0] & 0x0f) as usize * 4;
        let tcp_start = ihl;
        u16::from_be_bytes([ip_packet[tcp_start + 16], ip_packet[tcp_start + 17]])
    }

    #[test]
    fn test_complete_checksum_tcp_matches_etherparse() {
        let src_ip = [10, 0, 0, 1];
        let dst_ip = [10, 0, 0, 2];
        let ip_packet = build_tcp_ip_packet(src_ip, dst_ip, 12345, 80, &[]);
        let expected_csum = extract_tcp_checksum_ip(&ip_packet);

        let mut frame = simulate_guest_needs_csum(&ip_packet, src_ip, dst_ip);
        complete_checksum(&mut frame);

        let actual_csum = extract_tcp_checksum(&frame);
        assert_eq!(
            actual_csum, expected_csum,
            "complete_checksum TCP csum 0x{:04x} != etherparse 0x{:04x}",
            actual_csum, expected_csum
        );
        // NEEDS_CSUM flag should be cleared
        assert_eq!(frame[0] & FLAG_NEEDS_CSUM, 0);
    }

    #[test]
    fn test_complete_checksum_tcp_with_payload() {
        let src_ip = [172, 16, 0, 5];
        let dst_ip = [172, 16, 0, 10];
        let payload = b"hello world";
        let ip_packet = build_tcp_ip_packet(src_ip, dst_ip, 9000, 443, payload);
        let expected_csum = extract_tcp_checksum_ip(&ip_packet);

        let mut frame = simulate_guest_needs_csum(&ip_packet, src_ip, dst_ip);
        complete_checksum(&mut frame);

        let actual_csum = extract_tcp_checksum(&frame);
        assert_eq!(
            actual_csum, expected_csum,
            "with payload: 0x{:04x} != 0x{:04x}",
            actual_csum, expected_csum
        );
    }

    #[test]
    fn test_complete_checksum_tcp_odd_payload() {
        let src_ip = [192, 168, 1, 1];
        let dst_ip = [192, 168, 1, 2];
        // 13 bytes — odd length exercises the padding branch
        let payload = b"hello world!!";
        let ip_packet = build_tcp_ip_packet(src_ip, dst_ip, 5555, 8080, payload);
        let expected_csum = extract_tcp_checksum_ip(&ip_packet);

        let mut frame = simulate_guest_needs_csum(&ip_packet, src_ip, dst_ip);
        complete_checksum(&mut frame);

        let actual_csum = extract_tcp_checksum(&frame);
        assert_eq!(
            actual_csum, expected_csum,
            "odd payload: 0x{:04x} != 0x{:04x}",
            actual_csum, expected_csum
        );
    }

    #[test]
    fn test_complete_checksum_no_flag_is_noop() {
        let src_ip = [10, 0, 0, 1];
        let dst_ip = [10, 0, 0, 2];
        let ip_packet = build_tcp_ip_packet(src_ip, dst_ip, 12345, 80, &[]);

        // Prepend zeroed fabric header (no NEEDS_CSUM flag)
        let mut frame = with_fabric_header(0, 0, &ip_packet);

        let original = frame.clone();
        complete_checksum(&mut frame);

        // Frame should be completely unchanged
        assert_eq!(frame, original);
    }
}
