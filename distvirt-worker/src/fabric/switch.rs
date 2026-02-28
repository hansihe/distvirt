use std::collections::HashMap;

use super::port::PortId;

/// Synthetic gateway MAC address (locally administered).
pub const GATEWAY_MAC: [u8; 6] = [0x02, 0x00, 0x00, 0x00, 0x00, 0x01];

/// Gateway IP address.
pub const GATEWAY_IP: [u8; 4] = [172, 16, 0, 1];

/// Ethernet header size.
pub const ETH_HEADER_LEN: usize = 14;

/// Virtio-net header size (10 bytes), prepended to all frames in the fabric.
pub const VNET_HDR_SZ: usize = 10;

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

    #[test]
    fn format_mac_known() {
        assert_eq!(format_mac(&[0x02, 0x00, 0x00, 0x00, 0x00, 0x01]), "02:00:00:00:00:01");
    }

    #[test]
    fn format_mac_broadcast() {
        assert_eq!(format_mac(&[0xff; 6]), "ff:ff:ff:ff:ff:ff");
    }
}
