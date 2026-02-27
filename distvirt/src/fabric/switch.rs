use std::collections::HashMap;

use super::port::PortId;

/// Synthetic gateway MAC address (locally administered).
pub const GATEWAY_MAC: [u8; 6] = [0x02, 0x00, 0x00, 0x00, 0x00, 0x01];

/// Gateway IP address.
pub const GATEWAY_IP: [u8; 4] = [172, 16, 0, 1];

/// Ethernet header size.
const ETH_HEADER_LEN: usize = 14;

/// ARP packet total size (header + payload for IPv4/Ethernet).
const ARP_PACKET_LEN: usize = 28;

/// EtherType for ARP.
const ETHERTYPE_ARP: u16 = 0x0806;

/// ARP operation codes.
const ARP_OP_REQUEST: u16 = 1;
const ARP_OP_REPLY: u16 = 2;

/// MAC address learning table mapping MAC addresses to port IDs.
pub struct MacTable {
    table: HashMap<[u8; 6], PortId>,
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
            self.table.insert(mac, port_id);
        }
    }

    /// Look up which port a destination MAC is associated with.
    pub fn lookup(&self, mac: &[u8; 6]) -> Option<PortId> {
        self.table.get(mac).copied()
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
pub fn parse_ethernet_header(frame: &[u8]) -> Option<([u8; 6], [u8; 6], u16)> {
    if frame.len() < ETH_HEADER_LEN {
        return None;
    }
    let dst: [u8; 6] = frame[0..6].try_into().unwrap();
    let src: [u8; 6] = frame[6..12].try_into().unwrap();
    let ethertype = u16::from_be_bytes([frame[12], frame[13]]);
    Some((dst, src, ethertype))
}

/// Check if a frame is an ARP request for the gateway IP.
pub fn is_arp_request_for_gateway(frame: &[u8]) -> bool {
    if frame.len() < ETH_HEADER_LEN + ARP_PACKET_LEN {
        return false;
    }
    let ethertype = u16::from_be_bytes([frame[12], frame[13]]);
    if ethertype != ETHERTYPE_ARP {
        return false;
    }

    let arp = &frame[ETH_HEADER_LEN..];

    // Check hardware type (Ethernet = 1), protocol type (IPv4 = 0x0800),
    // hw addr len (6), proto addr len (4), operation (request = 1).
    let hw_type = u16::from_be_bytes([arp[0], arp[1]]);
    let proto_type = u16::from_be_bytes([arp[2], arp[3]]);
    let hw_len = arp[4];
    let proto_len = arp[5];
    let operation = u16::from_be_bytes([arp[6], arp[7]]);

    if hw_type != 1 || proto_type != 0x0800 || hw_len != 6 || proto_len != 4 {
        return false;
    }
    if operation != ARP_OP_REQUEST {
        return false;
    }

    // Target protocol address is at offset 24 in the ARP payload.
    let target_ip: [u8; 4] = arp[24..28].try_into().unwrap();
    target_ip == GATEWAY_IP
}

/// Build an ARP reply frame for a gateway ARP request.
///
/// Takes the original request frame and constructs a proper reply with
/// the gateway's synthetic MAC and IP.
pub fn build_arp_reply(request_frame: &[u8]) -> Option<Vec<u8>> {
    if request_frame.len() < ETH_HEADER_LEN + ARP_PACKET_LEN {
        return None;
    }

    let arp = &request_frame[ETH_HEADER_LEN..];

    // Extract sender info from the request.
    let sender_mac: [u8; 6] = arp[8..14].try_into().unwrap();
    let sender_ip: [u8; 4] = arp[14..18].try_into().unwrap();

    let mut reply = vec![0u8; ETH_HEADER_LEN + ARP_PACKET_LEN];

    // Ethernet header: dst = requester, src = gateway.
    reply[0..6].copy_from_slice(&sender_mac);
    reply[6..12].copy_from_slice(&GATEWAY_MAC);
    reply[12..14].copy_from_slice(&ETHERTYPE_ARP.to_be_bytes());

    // ARP payload.
    let arp_reply = &mut reply[ETH_HEADER_LEN..];
    // Hardware type: Ethernet (1).
    arp_reply[0..2].copy_from_slice(&1u16.to_be_bytes());
    // Protocol type: IPv4 (0x0800).
    arp_reply[2..4].copy_from_slice(&0x0800u16.to_be_bytes());
    // Hardware address length: 6.
    arp_reply[4] = 6;
    // Protocol address length: 4.
    arp_reply[5] = 4;
    // Operation: reply (2).
    arp_reply[6..8].copy_from_slice(&ARP_OP_REPLY.to_be_bytes());
    // Sender hardware address: gateway MAC.
    arp_reply[8..14].copy_from_slice(&GATEWAY_MAC);
    // Sender protocol address: gateway IP.
    arp_reply[14..18].copy_from_slice(&GATEWAY_IP);
    // Target hardware address: requester's MAC.
    arp_reply[18..24].copy_from_slice(&sender_mac);
    // Target protocol address: requester's IP.
    arp_reply[24..28].copy_from_slice(&sender_ip);

    Some(reply)
}

/// Format a MAC address for logging.
pub fn format_mac(bytes: &[u8; 6]) -> String {
    format!(
        "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5]
    )
}
