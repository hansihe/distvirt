//! L3 packet parsing and flow tracking using etherparse.

use std::collections::HashMap;
use std::net::IpAddr;

use crate::types::{IpProtocol, PacketFlow, PacketInfo};

/// Five-tuple flow key for packet correlation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct FlowKey {
    src_ip: IpAddr,
    dst_ip: IpAddr,
    protocol: u8,
    src_port: u16,
    dst_port: u16,
}

/// Assigns stable `u64` flow IDs keyed by five-tuple.
pub struct FlowTracker {
    flows: HashMap<FlowKey, PacketFlow>,
    next_id: PacketFlow,
}

impl FlowTracker {
    pub fn new() -> Self {
        FlowTracker {
            flows: HashMap::new(),
            next_id: 1,
        }
    }

    /// Get or assign a flow ID for the given key.
    fn get_or_assign(&mut self, key: FlowKey) -> PacketFlow {
        *self.flows.entry(key).or_insert_with(|| {
            let id = self.next_id;
            self.next_id += 1;
            id
        })
    }

    /// Number of tracked flows.
    pub fn flow_count(&self) -> usize {
        self.flows.len()
    }
}

/// Offset of the virtio-net header prepended to frames.
#[allow(dead_code)]
const VNET_HDR_SZ: usize = 10;

/// Parse an IP packet (with vnet header) into a `PacketInfo`.
///
/// Returns `None` for non-IPv4 packets or unparseable packets.
/// `raw_frame_with_vnet` is the full frame including the vnet header, stored
/// as-is in the `PacketInfo` for activator-owned buffering/replay.
pub fn parse_frame_to_packet_info(
    ip_packet: &[u8],
    raw_frame_with_vnet: &[u8],
    flow_tracker: &mut FlowTracker,
) -> Option<PacketInfo> {
    let packet = etherparse::SlicedPacket::from_ip(ip_packet).ok()?;

    let (src_addr, dst_addr) = match packet.net {
        Some(etherparse::NetSlice::Ipv4(ref ipv4)) => {
            let src = IpAddr::from(ipv4.header().source_addr());
            let dst = IpAddr::from(ipv4.header().destination_addr());
            (src, dst)
        }
        // IPv6 support possible in future
        _ => return None,
    };

    let (protocol, src_port, dst_port, tcp_flags, payload_len, ip_proto_num) =
        match packet.transport {
            Some(etherparse::TransportSlice::Tcp(ref tcp)) => {
                let flags = tcp.slice()[13]; // TCP flags byte
                let payload_len = ip_packet.len()
                    .saturating_sub(packet.net.as_ref().map_or(0, |n| match n {
                        etherparse::NetSlice::Ipv4(v4) => v4.header().total_len() as usize,
                        _ => 0,
                    }));
                (
                    IpProtocol::Tcp,
                    tcp.source_port(),
                    tcp.destination_port(),
                    Some(flags),
                    payload_len,
                    6u8,
                )
            }
            Some(etherparse::TransportSlice::Udp(ref udp)) => (
                IpProtocol::Udp,
                udp.source_port(),
                udp.destination_port(),
                None,
                0usize,
                17u8,
            ),
            _ => (IpProtocol::Other, 0, 0, None, 0usize, 0u8),
        };

    let flow_key = FlowKey {
        src_ip: src_addr,
        dst_ip: dst_addr,
        protocol: ip_proto_num,
        src_port,
        dst_port,
    };

    let flow = flow_tracker.get_or_assign(flow_key);

    Some(PacketInfo {
        flow,
        src_addr,
        dst_addr,
        src_port,
        dst_port,
        protocol,
        tcp_flags,
        payload_len,
        raw_frame: raw_frame_with_vnet.to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal IPv4 + TCP packet.
    fn build_tcp_frame(
        src_ip: [u8; 4],
        dst_ip: [u8; 4],
        src_port: u16,
        dst_port: u16,
        tcp_flags: u8,
    ) -> Vec<u8> {
        use etherparse::PacketBuilder;

        let builder = PacketBuilder::ipv4(src_ip, dst_ip, 64)
            .tcp(src_port, dst_port, 1000, 65535);

        let mut buf = Vec::new();
        builder.write(&mut buf, &[]).unwrap();

        // Set TCP flags manually (offset: ip(20) + tcp flags at byte 13)
        let tcp_start = 20;
        buf[tcp_start + 13] = tcp_flags;

        // Recalculate TCP checksum would be needed for real use,
        // but etherparse parsing doesn't validate checksums by default.

        buf
    }

    /// Build a minimal IPv4 + UDP packet.
    fn build_udp_frame(
        src_ip: [u8; 4],
        dst_ip: [u8; 4],
        src_port: u16,
        dst_port: u16,
    ) -> Vec<u8> {
        use etherparse::PacketBuilder;

        let builder = PacketBuilder::ipv4(src_ip, dst_ip, 64)
            .udp(src_port, dst_port);

        let mut buf = Vec::new();
        builder.write(&mut buf, &[1, 2, 3]).unwrap();
        buf
    }

    #[test]
    fn parse_tcp_syn() {
        let ip_packet = build_tcp_frame(
            [10, 0, 0, 1],
            [10, 0, 0, 2],
            12345,
            80,
            0x02, // SYN
        );
        let vnet_prefix = vec![0u8; VNET_HDR_SZ];
        let raw = [&vnet_prefix[..], &ip_packet[..]].concat();

        let mut tracker = FlowTracker::new();
        let info = parse_frame_to_packet_info(&ip_packet, &raw, &mut tracker).unwrap();

        assert_eq!(info.src_addr, IpAddr::from([10, 0, 0, 1]));
        assert_eq!(info.dst_addr, IpAddr::from([10, 0, 0, 2]));
        assert_eq!(info.src_port, 12345);
        assert_eq!(info.dst_port, 80);
        assert_eq!(info.protocol, IpProtocol::Tcp);
        assert_eq!(info.tcp_flags, Some(0x02));
        assert_eq!(info.flow, 1);
        assert_eq!(info.raw_frame, raw);
    }

    #[test]
    fn same_flow_gets_same_id() {
        let frame1 = build_tcp_frame([10, 0, 0, 1], [10, 0, 0, 2], 12345, 80, 0x02);
        let frame2 = build_tcp_frame([10, 0, 0, 1], [10, 0, 0, 2], 12345, 80, 0x10); // ACK

        let mut tracker = FlowTracker::new();
        let info1 = parse_frame_to_packet_info(&frame1, &frame1, &mut tracker).unwrap();
        let info2 = parse_frame_to_packet_info(&frame2, &frame2, &mut tracker).unwrap();

        assert_eq!(info1.flow, info2.flow);
        assert_eq!(tracker.flow_count(), 1);
    }

    #[test]
    fn different_flows_get_different_ids() {
        let frame1 = build_tcp_frame([10, 0, 0, 1], [10, 0, 0, 2], 12345, 80, 0x02);
        let frame2 = build_tcp_frame([10, 0, 0, 1], [10, 0, 0, 2], 12346, 80, 0x02);

        let mut tracker = FlowTracker::new();
        let info1 = parse_frame_to_packet_info(&frame1, &frame1, &mut tracker).unwrap();
        let info2 = parse_frame_to_packet_info(&frame2, &frame2, &mut tracker).unwrap();

        assert_ne!(info1.flow, info2.flow);
        assert_eq!(tracker.flow_count(), 2);
    }

    #[test]
    fn parse_tcp_rst() {
        let frame = build_tcp_frame([10, 0, 0, 1], [10, 0, 0, 2], 12345, 80, 0x04); // RST

        let mut tracker = FlowTracker::new();
        let info = parse_frame_to_packet_info(&frame, &frame, &mut tracker).unwrap();

        assert_eq!(info.tcp_flags, Some(0x04));
        assert_eq!(info.protocol, IpProtocol::Tcp);
    }

    #[test]
    fn parse_udp() {
        let frame = build_udp_frame([10, 0, 0, 1], [10, 0, 0, 2], 5353, 5353);

        let mut tracker = FlowTracker::new();
        let info = parse_frame_to_packet_info(&frame, &frame, &mut tracker).unwrap();

        assert_eq!(info.protocol, IpProtocol::Udp);
        assert_eq!(info.src_port, 5353);
        assert_eq!(info.dst_port, 5353);
        assert!(info.tcp_flags.is_none());
    }

    #[test]
    fn reverse_direction_different_flow() {
        let frame_fwd = build_tcp_frame([10, 0, 0, 1], [10, 0, 0, 2], 12345, 80, 0x02);
        let frame_rev = build_tcp_frame([10, 0, 0, 2], [10, 0, 0, 1], 80, 12345, 0x12); // SYN-ACK

        let mut tracker = FlowTracker::new();
        let info_fwd = parse_frame_to_packet_info(&frame_fwd, &frame_fwd, &mut tracker).unwrap();
        let info_rev = parse_frame_to_packet_info(&frame_rev, &frame_rev, &mut tracker).unwrap();

        assert_ne!(info_fwd.flow, info_rev.flow);
        assert_eq!(tracker.flow_count(), 2);
    }

    #[test]
    fn different_protocol_different_flow() {
        let tcp_frame = build_tcp_frame([10, 0, 0, 1], [10, 0, 0, 2], 5353, 5353, 0x02);
        let udp_frame = build_udp_frame([10, 0, 0, 1], [10, 0, 0, 2], 5353, 5353);

        let mut tracker = FlowTracker::new();
        let info_tcp = parse_frame_to_packet_info(&tcp_frame, &tcp_frame, &mut tracker).unwrap();
        let info_udp = parse_frame_to_packet_info(&udp_frame, &udp_frame, &mut tracker).unwrap();

        assert_ne!(info_tcp.flow, info_udp.flow);
        assert_eq!(info_tcp.protocol, IpProtocol::Tcp);
        assert_eq!(info_udp.protocol, IpProtocol::Udp);
    }

    #[test]
    fn tcp_with_payload() {
        use etherparse::PacketBuilder;

        let builder = PacketBuilder::ipv4([10, 0, 0, 1], [10, 0, 0, 2], 64)
            .tcp(1234, 80, 1000, 65535);

        let payload = b"GET / HTTP/1.1\r\n";
        let mut buf = Vec::new();
        builder.write(&mut buf, payload).unwrap();

        let mut tracker = FlowTracker::new();
        let info = parse_frame_to_packet_info(&buf, &buf, &mut tracker).unwrap();

        assert_eq!(info.protocol, IpProtocol::Tcp);
        assert_eq!(info.src_port, 1234);
        assert_eq!(info.dst_port, 80);
    }

    #[test]
    fn empty_frame_returns_none() {
        let mut tracker = FlowTracker::new();
        assert!(parse_frame_to_packet_info(&[], &[], &mut tracker).is_none());
    }

    #[test]
    fn non_ip_returns_none() {
        // Garbage bytes that can't parse as a valid IP packet
        let frame = vec![0xff, 0xfe, 0x00, 0x01, 0x02, 0x03];

        let mut tracker = FlowTracker::new();
        assert!(parse_frame_to_packet_info(&frame, &frame, &mut tracker).is_none());
    }
}
