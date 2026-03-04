use std::collections::HashMap;
use std::net::Ipv4Addr;

use super::port::PortId;

/// L3 routing table mapping pod IPs to port IDs.
pub struct IpPortTable {
    by_ip: HashMap<Ipv4Addr, PortId>,
    by_port: HashMap<PortId, Ipv4Addr>,
}

impl IpPortTable {
    pub fn new() -> Self {
        IpPortTable {
            by_ip: HashMap::new(),
            by_port: HashMap::new(),
        }
    }

    /// Insert a mapping from IP to port ID.
    pub fn insert(&mut self, ip: Ipv4Addr, port_id: PortId) {
        self.by_ip.insert(ip, port_id);
        self.by_port.insert(port_id, ip);
    }

    /// Look up which port an IP is associated with.
    pub fn lookup(&self, ip: &Ipv4Addr) -> Option<PortId> {
        self.by_ip.get(ip).copied()
    }

    /// Check if an IP is registered.
    pub fn contains_ip(&self, ip: &Ipv4Addr) -> bool {
        self.by_ip.contains_key(ip)
    }

    /// Remove entries for a port (called on port cleanup).
    pub fn remove_by_port(&mut self, port_id: PortId) {
        if let Some(ip) = self.by_port.remove(&port_id) {
            self.by_ip.remove(&ip);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ip_port_table_insert_and_lookup() {
        let mut table = IpPortTable::new();
        let ip = Ipv4Addr::new(172, 16, 0, 10);
        table.insert(ip, 3);
        let port_id = table.lookup(&ip).unwrap();
        assert_eq!(port_id, 3);
    }

    #[test]
    fn ip_port_table_lookup_unknown_returns_none() {
        let table = IpPortTable::new();
        assert!(table.lookup(&Ipv4Addr::new(10, 0, 0, 1)).is_none());
    }

    #[test]
    fn ip_port_table_contains_ip() {
        let mut table = IpPortTable::new();
        let ip = Ipv4Addr::new(172, 16, 0, 10);
        assert!(!table.contains_ip(&ip));
        table.insert(ip, 1);
        assert!(table.contains_ip(&ip));
    }

    #[test]
    fn ip_port_table_remove_by_port() {
        let mut table = IpPortTable::new();
        let ip = Ipv4Addr::new(172, 16, 0, 10);
        table.insert(ip, 5);
        assert!(table.contains_ip(&ip));
        table.remove_by_port(5);
        assert!(!table.contains_ip(&ip));
    }

    #[test]
    fn ip_port_table_remove_by_port_unknown_noop() {
        let mut table = IpPortTable::new();
        table.remove_by_port(99); // should not panic
    }
}
