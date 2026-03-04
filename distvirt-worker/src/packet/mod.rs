//! Consolidated packet manipulation: constants, frame wrappers, checksums.

pub mod checksum;
pub mod frame;

// --- FabricHeader ---

use std::mem::size_of;
use zerocopy::{FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, network_endian::U16};

/// Custom 3-byte fabric header prepended to all IP packets in the fabric.
///
/// Replaces the 10-byte virtio-net header. `csum_start`/`csum_offset` are no
/// longer stored — they are derived from the IP header when checksum completion
/// is needed.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, Clone, Copy)]
#[repr(C)]
pub struct FabricHeader {
    /// Bit 0: NEEDS_CSUM — transport checksum must be completed before
    /// leaving the fabric (e.g. WireGuard egress, TUN egress).
    pub flags: u8,
    /// Network-endian segment ID for inter-worker routing (future use).
    pub segment_id: U16,
}

/// Size of the fabric header in bytes.
pub const FABRIC_HDR_SZ: usize = size_of::<FabricHeader>(); // 3

/// Flag: transport checksum needs to be completed.
pub const FLAG_NEEDS_CSUM: u8 = 0x01;

// --- Shared constants ---

/// IP protocol number for TCP.
pub const IP_PROTO_TCP: u8 = 6;

/// IP protocol number for UDP.
pub const IP_PROTO_UDP: u8 = 17;

// --- Re-exports ---

pub use checksum::complete_checksum;
pub use frame::{
    FabricPacket,
    format_tcp_flags,
    ip_packet_dst, ip_packet_protocol, ip_packet_src,
    ip_packet_transport_ports,
    rewrite_ipv4_dst, rewrite_ipv4_src,
    with_fabric_header,
};
