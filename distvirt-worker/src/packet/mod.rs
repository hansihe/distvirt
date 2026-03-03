//! Consolidated packet manipulation: constants, frame wrappers, checksums, ARP.

pub mod arp;
pub mod checksum;
pub mod frame;

// --- Shared constants ---

/// Virtio-net header size (10 bytes), prepended to all frames in the fabric.
pub const VNET_HDR_SZ: usize = 10;

/// Ethernet header size (dst MAC + src MAC + ethertype).
pub const ETH_HDR_LEN: usize = 14;

/// Broadcast MAC address.
pub const BROADCAST_MAC: [u8; 6] = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff];

/// EtherType for IPv4.
pub const ETHERTYPE_IPV4: u16 = 0x0800;

/// EtherType for ARP.
pub const ETHERTYPE_ARP: u16 = 0x0806;

/// IP protocol number for TCP.
pub const IP_PROTO_TCP: u8 = 6;

/// IP protocol number for UDP.
pub const IP_PROTO_UDP: u8 = 17;

// --- Re-exports ---

pub use arp::{build_arp_reply, parse_arp_request};
pub use checksum::complete_checksum;
pub use frame::{
    FabricFrame, extract_ip_protocol, extract_ipv4_dst, extract_ipv4_src,
    extract_transport_ports, fabric_frame_ethertype, fabric_frame_to_ip, format_mac,
    ip_packet_dst, ip_to_fabric_frame, is_broadcast, is_multicast, rewrite_dst_mac,
    rewrite_ipv4_dst, rewrite_ipv4_src, rewrite_src_mac, with_vnet_header,
};
