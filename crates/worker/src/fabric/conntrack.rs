use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::time::{Duration, Instant};

/// 5-tuple key for connection tracking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConntrackKey {
    pub src_ip: Ipv4Addr,
    pub dst_ip: Ipv4Addr,
    pub protocol: u8,
    pub src_port: u16,
    pub dst_port: u16,
}

/// Connection tracking entry storing the original service VIP.
#[derive(Debug, Clone)]
pub struct ConntrackEntry {
    pub service_ip: Ipv4Addr,
    #[allow(dead_code)]
    pub backend_ip: Ipv4Addr,
    pub last_seen: Instant,
}

/// Connection tracking table for service DNAT.
///
/// On the forward path (DNAT), we insert an entry keyed by the reverse-direction
/// 5-tuple so that return traffic from the backend gets reverse-DNAT'd.
pub struct ConntrackTable {
    reverse: HashMap<ConntrackKey, ConntrackEntry>,
}

impl ConntrackTable {
    pub fn new() -> Self {
        ConntrackTable {
            reverse: HashMap::new(),
        }
    }

    /// Insert a reverse NAT entry for a DNAT'd connection.
    ///
    /// The key should be the reverse-direction 5-tuple:
    /// `(src=backend_ip, dst=client_ip, proto, src_port=service_port, dst_port=client_port)`
    pub fn insert(&mut self, key: ConntrackKey, entry: ConntrackEntry) {
        self.reverse.insert(key, entry);
    }

    /// Look up a NAT entry by the packet's forward 5-tuple.
    /// Updates `last_seen` on hit to keep active connections alive during GC.
    /// Without this, long-lived idle connections (e.g. keep-alive HTTP) would
    /// have their NAT entries reaped and return traffic would stop working.
    pub fn lookup(&mut self, key: &ConntrackKey) -> Option<&ConntrackEntry> {
        if let Some(entry) = self.reverse.get_mut(key) {
            entry.last_seen = Instant::now();
            Some(entry)
        } else {
            None
        }
    }

    /// Remove entries older than `max_age`.
    pub fn gc(&mut self, max_age: Duration) {
        let now = Instant::now();
        let before = self.reverse.len();
        self.reverse
            .retain(|_, entry| now.duration_since(entry.last_seen) <= max_age);
        let expired = before - self.reverse.len();
        if expired > 0 {
            log::info!(
                "conntrack: gc removed {} stale entries ({} remaining)",
                expired,
                self.reverse.len()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_lookup() {
        let mut table = ConntrackTable::new();
        let key = ConntrackKey {
            src_ip: Ipv4Addr::new(172, 16, 0, 130),
            dst_ip: Ipv4Addr::new(10, 0, 0, 1),
            protocol: 6,
            src_port: 80,
            dst_port: 12345,
        };
        let entry = ConntrackEntry {
            service_ip: Ipv4Addr::new(172, 16, 0, 50),
            backend_ip: Ipv4Addr::new(172, 16, 0, 130),
            last_seen: Instant::now(),
        };
        table.insert(key, entry);
        assert!(table.lookup(&key).is_some());
    }

    #[test]
    fn lookup_miss() {
        let mut table = ConntrackTable::new();
        let key = ConntrackKey {
            src_ip: Ipv4Addr::new(10, 0, 0, 1),
            dst_ip: Ipv4Addr::new(10, 0, 0, 2),
            protocol: 6,
            src_port: 1234,
            dst_port: 80,
        };
        assert!(table.lookup(&key).is_none());
    }

    #[test]
    fn gc_removes_stale() {
        let mut table = ConntrackTable::new();
        let key = ConntrackKey {
            src_ip: Ipv4Addr::new(172, 16, 0, 130),
            dst_ip: Ipv4Addr::new(10, 0, 0, 1),
            protocol: 6,
            src_port: 80,
            dst_port: 12345,
        };
        let entry = ConntrackEntry {
            service_ip: Ipv4Addr::new(172, 16, 0, 50),
            backend_ip: Ipv4Addr::new(172, 16, 0, 130),
            last_seen: Instant::now(),
        };
        table.insert(key, entry);

        // With generous max_age, entry should survive.
        table.gc(Duration::from_secs(300));
        assert!(table.lookup(&key).is_some());

        // With zero max_age, entry should be removed.
        table.gc(Duration::from_secs(0));
        assert!(table.lookup(&key).is_none());
    }
}
