//! L3↔L2 frame conversion helpers for ingress adapters.
//!
//! WireGuard carries raw IP packets, but the fabric carries Ethernet frames
//! with a virtio-net header prefix. These helpers bridge the gap.

use std::net::Ipv4Addr;

/// Virtio-net header size (10 bytes), prepended to all frames in the fabric.
pub const VNET_HDR_SZ: usize = 10;

/// Ethernet header size (dst MAC + src MAC + ethertype).
pub const ETH_HDR_LEN: usize = 14;

/// Broadcast MAC address.
pub const BROADCAST_MAC: [u8; 6] = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff];

/// Wrap a raw IP packet into a fabric frame: `[vnet_hdr(10)][eth_hdr(14)][ip_packet]`.
pub fn ip_to_fabric_frame(ip_packet: &[u8], src_mac: &[u8; 6], dst_mac: &[u8; 6]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(VNET_HDR_SZ + ETH_HDR_LEN + ip_packet.len());
    // Zeroed virtio-net header.
    frame.extend_from_slice(&[0u8; VNET_HDR_SZ]);
    // Ethernet header.
    frame.extend_from_slice(dst_mac);
    frame.extend_from_slice(src_mac);
    frame.extend_from_slice(&0x0800u16.to_be_bytes()); // IPv4
    // Payload.
    frame.extend_from_slice(ip_packet);
    frame
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
    let checksum = !(sum as u16);

    frame[abs_csum] = (checksum >> 8) as u8;
    frame[abs_csum + 1] = (checksum & 0xFF) as u8;

    // Clear the NEEDS_CSUM flag.
    frame[0] = flags & !VIRTIO_NET_HDR_F_NEEDS_CSUM;
}

/// Extract the raw IP packet from a fabric frame, stripping vnet_hdr + ethernet header.
///
/// Returns `None` if the frame is too short or not IPv4 (ethertype 0x0800).
pub fn fabric_frame_to_ip(frame: &[u8]) -> Option<&[u8]> {
    if frame.len() < VNET_HDR_SZ + ETH_HDR_LEN {
        return None;
    }
    let ethertype = u16::from_be_bytes([
        frame[VNET_HDR_SZ + 12],
        frame[VNET_HDR_SZ + 13],
    ]);
    if ethertype != 0x0800 {
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

/// Extract the destination MAC from a fabric frame.
pub fn fabric_frame_dst_mac(frame: &[u8]) -> Option<[u8; 6]> {
    if frame.len() < VNET_HDR_SZ + 6 {
        return None;
    }
    let mut mac = [0u8; 6];
    mac.copy_from_slice(&frame[VNET_HDR_SZ..VNET_HDR_SZ + 6]);
    Some(mac)
}

/// Parse an ARP request from a fabric frame.
///
/// Returns `Some((target_ip, sender_mac, sender_ip))` if the frame is a valid
/// ARP request for IPv4 over Ethernet. Returns `None` otherwise.
pub fn parse_arp_request(frame: &[u8]) -> Option<(Ipv4Addr, [u8; 6], Ipv4Addr)> {
    if frame.len() < VNET_HDR_SZ + ETH_HDR_LEN + 28 {
        return None;
    }
    let ethertype = u16::from_be_bytes([
        frame[VNET_HDR_SZ + 12],
        frame[VNET_HDR_SZ + 13],
    ]);
    if ethertype != 0x0806 {
        return None;
    }
    let arp = &frame[VNET_HDR_SZ + ETH_HDR_LEN..];
    // Check: hardware type Ethernet (1), protocol type IPv4 (0x0800), op=request (1).
    let hw_type = u16::from_be_bytes([arp[0], arp[1]]);
    let proto_type = u16::from_be_bytes([arp[2], arp[3]]);
    let op = u16::from_be_bytes([arp[6], arp[7]]);
    if hw_type != 1 || proto_type != 0x0800 || op != 1 {
        return None;
    }
    let mut sender_mac = [0u8; 6];
    sender_mac.copy_from_slice(&arp[8..14]);
    let sender_ip = Ipv4Addr::new(arp[14], arp[15], arp[16], arp[17]);
    let target_ip = Ipv4Addr::new(arp[24], arp[25], arp[26], arp[27]);
    Some((target_ip, sender_mac, sender_ip))
}

/// Build an ARP reply fabric frame with vnet_hdr prefix.
pub fn build_arp_reply(
    target_mac: &[u8; 6],
    target_ip: Ipv4Addr,
    sender_mac: &[u8; 6],
    sender_ip: Ipv4Addr,
) -> Vec<u8> {
    let mut frame = Vec::with_capacity(VNET_HDR_SZ + ETH_HDR_LEN + 28);
    // Zeroed virtio-net header.
    frame.extend_from_slice(&[0u8; VNET_HDR_SZ]);
    // Ethernet header: dst=target, src=sender, ethertype=ARP.
    frame.extend_from_slice(target_mac);
    frame.extend_from_slice(sender_mac);
    frame.extend_from_slice(&0x0806u16.to_be_bytes());
    // ARP payload.
    let mut arp = [0u8; 28];
    arp[0..2].copy_from_slice(&[0x00, 0x01]); // hardware type: Ethernet
    arp[2..4].copy_from_slice(&[0x08, 0x00]); // protocol type: IPv4
    arp[4] = 6; // hardware size
    arp[5] = 4; // protocol size
    arp[6..8].copy_from_slice(&[0x00, 0x02]); // operation: reply
    arp[8..14].copy_from_slice(sender_mac); // sender hardware address
    arp[14..18].copy_from_slice(&sender_ip.octets()); // sender protocol address
    arp[18..24].copy_from_slice(target_mac); // target hardware address
    arp[24..28].copy_from_slice(&target_ip.octets()); // target protocol address
    frame.extend_from_slice(&arp);
    frame
}

/// Extract the destination IPv4 address from a raw IP packet.
pub fn ip_packet_dst(packet: &[u8]) -> Option<Ipv4Addr> {
    if packet.len() < 20 {
        return None;
    }
    Some(Ipv4Addr::new(packet[16], packet[17], packet[18], packet[19]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_ip_to_fabric_frame() {
        let ip_packet = &[
            0x45, 0x00, 0x00, 0x1c, // version, IHL, total length
            0x00, 0x00, 0x00, 0x00, // identification, flags, fragment offset
            0x40, 0x11, 0x00, 0x00, // TTL, protocol (UDP), checksum
            0xac, 0x10, 0x00, 0x02, // src IP: 172.16.0.2
            0xac, 0x10, 0x00, 0x03, // dst IP: 172.16.0.3
        ];
        let src_mac = [0x02, 0xaa, 0xbb, 0xcc, 0xdd, 0xee];
        let dst_mac = [0x02, 0x11, 0x22, 0x33, 0x44, 0x55];

        let frame = ip_to_fabric_frame(ip_packet, &src_mac, &dst_mac);
        assert_eq!(frame.len(), VNET_HDR_SZ + ETH_HDR_LEN + ip_packet.len());

        // Verify vnet header is zeroed.
        assert_eq!(&frame[..VNET_HDR_SZ], &[0u8; VNET_HDR_SZ]);

        // Verify ethernet header.
        assert_eq!(&frame[VNET_HDR_SZ..VNET_HDR_SZ + 6], &dst_mac);
        assert_eq!(&frame[VNET_HDR_SZ + 6..VNET_HDR_SZ + 12], &src_mac);
        assert_eq!(&frame[VNET_HDR_SZ + 12..VNET_HDR_SZ + 14], &[0x08, 0x00]);

        // Round-trip.
        let extracted = fabric_frame_to_ip(&frame).unwrap();
        assert_eq!(extracted, ip_packet);
    }

    #[test]
    fn fabric_frame_to_ip_rejects_non_ipv4() {
        let mut frame = vec![0u8; VNET_HDR_SZ + ETH_HDR_LEN + 4];
        // Set ethertype to ARP.
        frame[VNET_HDR_SZ + 12] = 0x08;
        frame[VNET_HDR_SZ + 13] = 0x06;
        assert!(fabric_frame_to_ip(&frame).is_none());
    }

    #[test]
    fn fabric_frame_to_ip_rejects_short() {
        let frame = vec![0u8; VNET_HDR_SZ + ETH_HDR_LEN - 1];
        assert!(fabric_frame_to_ip(&frame).is_none());
    }

    #[test]
    fn arp_round_trip() {
        let target_mac = [0x02, 0xaa, 0xbb, 0xcc, 0xdd, 0xee];
        let target_ip = Ipv4Addr::new(172, 16, 0, 2);
        let sender_mac = [0x02, 0x11, 0x22, 0x33, 0x44, 0x55];
        let sender_ip = Ipv4Addr::new(172, 16, 0, 10);

        let reply = build_arp_reply(&target_mac, target_ip, &sender_mac, sender_ip);
        assert_eq!(reply.len(), VNET_HDR_SZ + ETH_HDR_LEN + 28);

        // The reply frame itself is an ARP reply (op=2), not a request.
        // But parse_arp_request should not match it.
        assert!(parse_arp_request(&reply).is_none());

        // Build a request frame and test parsing.
        let mut request = Vec::with_capacity(VNET_HDR_SZ + ETH_HDR_LEN + 28);
        request.extend_from_slice(&[0u8; VNET_HDR_SZ]);
        request.extend_from_slice(&BROADCAST_MAC);
        request.extend_from_slice(&target_mac);
        request.extend_from_slice(&0x0806u16.to_be_bytes());
        let mut arp = [0u8; 28];
        arp[0..2].copy_from_slice(&[0x00, 0x01]); // hw type
        arp[2..4].copy_from_slice(&[0x08, 0x00]); // proto type
        arp[4] = 6;
        arp[5] = 4;
        arp[6..8].copy_from_slice(&[0x00, 0x01]); // op=request
        arp[8..14].copy_from_slice(&target_mac);
        arp[14..18].copy_from_slice(&target_ip.octets());
        arp[18..24].copy_from_slice(&[0; 6]); // unknown target hw
        arp[24..28].copy_from_slice(&sender_ip.octets());
        request.extend_from_slice(&arp);

        let (parsed_target_ip, parsed_sender_mac, parsed_sender_ip) =
            parse_arp_request(&request).unwrap();
        assert_eq!(parsed_target_ip, sender_ip);
        assert_eq!(parsed_sender_mac, target_mac);
        assert_eq!(parsed_sender_ip, target_ip);
    }

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
        // Source IP
        sum += u16::from_be_bytes([src_ip[0], src_ip[1]]) as u32;
        sum += u16::from_be_bytes([src_ip[2], src_ip[3]]) as u32;
        // Dest IP
        sum += u16::from_be_bytes([dst_ip[0], dst_ip[1]]) as u32;
        sum += u16::from_be_bytes([dst_ip[2], dst_ip[3]]) as u32;
        // Zero + Protocol (TCP = 6)
        sum += 6u32;
        // TCP length
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
        // vnet header: flags=NEEDS_CSUM, gso_type=0, hdr_len=0, gso_size=0,
        // csum_start, csum_offset
        frame.push(1u8); // flags = VIRTIO_NET_HDR_F_NEEDS_CSUM
        frame.push(0);   // gso_type
        frame.extend_from_slice(&[0, 0]); // hdr_len
        frame.extend_from_slice(&[0, 0]); // gso_size

        // csum_start = offset of TCP header within eth frame = 14 (eth) + 20 (ip)
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
