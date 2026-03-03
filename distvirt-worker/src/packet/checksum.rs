//! Internet checksum computation: full, incremental (RFC 1624), and virtio offload completion.

use super::{ETH_HDR_LEN, VNET_HDR_SZ};

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

/// Complete virtio checksum offload if the vnet header has NEEDS_CSUM set.
///
/// The guest kernel may defer TCP/UDP checksum computation to the host via the
/// virtio-net header's `NEEDS_CSUM` flag. This function reads the flag and, if
/// set, computes the internet checksum over the specified range and writes it
/// into the frame. Must be called before stripping the vnet header for
/// transmission outside the fabric (e.g. WireGuard).
pub fn complete_checksum(frame: &mut [u8]) {
    if frame.len() < VNET_HDR_SZ + ETH_HDR_LEN {
        return;
    }
    const VIRTIO_NET_HDR_F_NEEDS_CSUM: u8 = 1;
    let flags = frame[0];
    if flags & VIRTIO_NET_HDR_F_NEEDS_CSUM == 0 {
        return;
    }
    // csum_start and csum_offset are relative to the ethernet frame (after vnet header).
    let csum_start = u16::from_le_bytes([frame[6], frame[7]]) as usize;
    let csum_offset = u16::from_le_bytes([frame[8], frame[9]]) as usize;

    // Absolute positions in the buffer.
    let abs_start = VNET_HDR_SZ + csum_start;
    let abs_csum = abs_start + csum_offset;

    if abs_csum + 2 > frame.len() || abs_start > frame.len() {
        return; // malformed
    }

    log::debug!(
        "complete_checksum: flags=0x{:02x} csum_start={} csum_offset={} abs_start={} abs_csum={}",
        flags, csum_start, csum_offset, abs_start, abs_csum,
    );

    // Do NOT zero the checksum field — the guest kernel places the TCP/UDP
    // pseudo-header partial checksum there. The host must include it in the
    // sum (matching Linux skb_checksum_help() behaviour).

    // Compute internet checksum over [abs_start..end].
    let data = &frame[abs_start..];
    let checksum = internet_checksum(data);

    frame[abs_csum] = (checksum >> 8) as u8;
    frame[abs_csum + 1] = (checksum & 0xFF) as u8;

    // Clear the NEEDS_CSUM flag.
    frame[0] = flags & !VIRTIO_NET_HDR_F_NEEDS_CSUM;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packet::{ETH_HDR_LEN, VNET_HDR_SZ};

    // --- complete_checksum oracle tests using etherparse ---

    /// Build a TCP Ethernet frame using etherparse (correct checksums).
    fn build_tcp_eth_frame(
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
        eth_frame
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

    /// Take an etherparse-built Ethernet frame, replace the TCP checksum with
    /// the pseudo-header partial, and prepend a vnet header with NEEDS_CSUM.
    fn simulate_guest_needs_csum(eth_frame: &[u8], src_ip: [u8; 4], dst_ip: [u8; 4]) -> Vec<u8> {
        let mut frame = Vec::with_capacity(VNET_HDR_SZ + eth_frame.len());
        frame.push(1u8); // flags = VIRTIO_NET_HDR_F_NEEDS_CSUM
        frame.push(0);   // gso_type
        frame.extend_from_slice(&[0, 0]); // hdr_len
        frame.extend_from_slice(&[0, 0]); // gso_size

        let ihl = (eth_frame[14] & 0x0f) as usize * 4;
        let tcp_start = ETH_HDR_LEN + ihl;
        frame.extend_from_slice(&(tcp_start as u16).to_le_bytes()); // csum_start
        frame.extend_from_slice(&16u16.to_le_bytes()); // csum_offset (TCP checksum at byte 16)

        frame.extend_from_slice(eth_frame);

        // Compute TCP segment length
        let ip_total_len = u16::from_be_bytes([eth_frame[16], eth_frame[17]]);
        let tcp_len = ip_total_len - (ihl as u16);

        // Replace TCP checksum field with pseudo-header partial
        let tcp_csum_abs = VNET_HDR_SZ + tcp_start + 16;
        let pseudo = tcp_pseudo_header_csum(src_ip, dst_ip, tcp_len);
        frame[tcp_csum_abs] = (pseudo >> 8) as u8;
        frame[tcp_csum_abs + 1] = (pseudo & 0xff) as u8;

        frame
    }

    /// Extract the TCP checksum from a fabric frame (with vnet header).
    fn extract_tcp_checksum(frame: &[u8]) -> u16 {
        let ihl = (frame[VNET_HDR_SZ + ETH_HDR_LEN] & 0x0f) as usize * 4;
        let tcp_start = VNET_HDR_SZ + ETH_HDR_LEN + ihl;
        u16::from_be_bytes([frame[tcp_start + 16], frame[tcp_start + 17]])
    }

    /// Extract the TCP checksum from an Ethernet frame (no vnet header).
    fn extract_tcp_checksum_eth(eth_frame: &[u8]) -> u16 {
        let ihl = (eth_frame[14] & 0x0f) as usize * 4;
        let tcp_start = ETH_HDR_LEN + ihl;
        u16::from_be_bytes([eth_frame[tcp_start + 16], eth_frame[tcp_start + 17]])
    }

    #[test]
    fn test_complete_checksum_tcp_matches_etherparse() {
        let src_ip = [10, 0, 0, 1];
        let dst_ip = [10, 0, 0, 2];
        let eth_frame = build_tcp_eth_frame(src_ip, dst_ip, 12345, 80, &[]);
        let expected_csum = extract_tcp_checksum_eth(&eth_frame);

        let mut frame = simulate_guest_needs_csum(&eth_frame, src_ip, dst_ip);
        complete_checksum(&mut frame);

        let actual_csum = extract_tcp_checksum(&frame);
        assert_eq!(
            actual_csum, expected_csum,
            "complete_checksum TCP csum 0x{:04x} != etherparse 0x{:04x}",
            actual_csum, expected_csum
        );
        // NEEDS_CSUM flag should be cleared
        assert_eq!(frame[0] & 1, 0);
    }

    #[test]
    fn test_complete_checksum_tcp_with_payload() {
        let src_ip = [172, 16, 0, 5];
        let dst_ip = [172, 16, 0, 10];
        let payload = b"hello world";
        let eth_frame = build_tcp_eth_frame(src_ip, dst_ip, 9000, 443, payload);
        let expected_csum = extract_tcp_checksum_eth(&eth_frame);

        let mut frame = simulate_guest_needs_csum(&eth_frame, src_ip, dst_ip);
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
        let eth_frame = build_tcp_eth_frame(src_ip, dst_ip, 5555, 8080, payload);
        let expected_csum = extract_tcp_checksum_eth(&eth_frame);

        let mut frame = simulate_guest_needs_csum(&eth_frame, src_ip, dst_ip);
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
        let eth_frame = build_tcp_eth_frame(src_ip, dst_ip, 12345, 80, &[]);

        // Prepend zeroed vnet header (no NEEDS_CSUM flag)
        let mut frame = Vec::with_capacity(VNET_HDR_SZ + eth_frame.len());
        frame.extend_from_slice(&[0u8; VNET_HDR_SZ]);
        frame.extend_from_slice(&eth_frame);

        let original = frame.clone();
        complete_checksum(&mut frame);

        // Frame should be completely unchanged
        assert_eq!(frame, original);
    }
}
