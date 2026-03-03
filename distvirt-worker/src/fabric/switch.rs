use std::collections::HashMap;
use std::time::{Duration, Instant};

use super::port::PortId;

use crate::packet::{is_broadcast, is_multicast};

/// Synthetic gateway MAC address (locally administered).
pub const GATEWAY_MAC: [u8; 6] = [0x02, 0x00, 0x00, 0xff, 0xff, 0x01];

/// MAC address learning table mapping MAC addresses to port IDs.
pub struct MacTable {
    table: HashMap<[u8; 6], (PortId, Instant)>,
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
            self.table.insert(mac, (port_id, Instant::now()));
        }
    }

    /// Look up which port a destination MAC is associated with.
    pub fn lookup(&self, mac: &[u8; 6]) -> Option<PortId> {
        self.table.get(mac).map(|(port_id, _)| *port_id)
    }

    /// Remove entries older than `max_age`.
    pub fn gc(&mut self, max_age: Duration) {
        let now = Instant::now();
        let before = self.table.len();
        self.table.retain(|_mac, (_, seen)| {
            now.duration_since(*seen) <= max_age
        });
        let expired = before - self.table.len();
        if expired > 0 {
            log::info!("mac_table: gc removed {} stale entries ({} remaining)", expired, self.table.len());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

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
        let mac = [0x01, 0x00, 0x5e, 0x00, 0x00, 0x01];
        table.learn(mac, 2);
        assert_eq!(table.lookup(&mac), None);
    }

    #[test]
    fn mac_table_gc_removes_stale_entries() {
        let mut table = MacTable::new();
        let mac_a = [0x02, 0x00, 0x00, 0x00, 0x00, 0x0a];
        let mac_b = [0x02, 0x00, 0x00, 0x00, 0x00, 0x0b];
        table.learn(mac_a, 1);
        table.learn(mac_b, 2);

        table.gc(Duration::from_secs(300));
        assert_eq!(table.lookup(&mac_a), Some(1));
        assert_eq!(table.lookup(&mac_b), Some(2));

        table.gc(Duration::from_secs(0));
        assert_eq!(table.lookup(&mac_a), None);
        assert_eq!(table.lookup(&mac_b), None);
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
}
