use std::collections::{HashMap, VecDeque};
use std::net::Ipv4Addr;
use std::time::Instant;

use distvirt_worker_protocol::{FabricRouteEntry, RouteDestination};

/// What the fabric should do with a frame after consulting the route table.
#[derive(Debug, PartialEq, Eq)]
pub enum RouteAction {
    /// Frame accepted into buffer, do not forward.
    Buffered,
    /// Policy says drop (buffer_frames=0 or buffer full/expired).
    Drop,
    /// Destination is on a remote worker (stub: log + drop for now).
    RemoteWorker { worker_id: String },
    /// No route entry exists; caller should flood as before.
    NoRoute,
}

/// Internal state for a single route entry.
struct RouteState {
    entry: FabricRouteEntry,
    buffer: VecDeque<Vec<u8>>,
    buffer_start: Option<Instant>,
}

/// Route table for the fabric: maps destination IPs to route entries with
/// optional frame buffering for placeholder destinations.
pub struct RouteTable {
    routes: HashMap<Ipv4Addr, RouteState>,
    /// Per-IP debounce tracking for route miss events.
    last_miss: HashMap<Ipv4Addr, Instant>,
    /// Debounce window for miss events.
    miss_debounce: std::time::Duration,
}

impl RouteTable {
    pub fn new() -> Self {
        RouteTable {
            routes: HashMap::new(),
            last_miss: HashMap::new(),
            miss_debounce: std::time::Duration::from_secs(1),
        }
    }

    /// Full replacement of the route table.
    pub fn sync(&mut self, entries: Vec<FabricRouteEntry>) {
        self.routes.clear();
        for entry in entries {
            let ip = entry.ip;
            self.routes.insert(
                ip,
                RouteState {
                    entry,
                    buffer: VecDeque::new(),
                    buffer_start: None,
                },
            );
        }
        // Clear debounce state on full sync.
        self.last_miss.clear();
    }

    /// Incremental delta update.
    pub fn update(&mut self, added: Vec<FabricRouteEntry>, removed_ips: Vec<Ipv4Addr>) {
        for ip in removed_ips {
            self.routes.remove(&ip);
            self.last_miss.remove(&ip);
        }
        for entry in added {
            let ip = entry.ip;
            self.routes.insert(
                ip,
                RouteState {
                    entry,
                    buffer: VecDeque::new(),
                    buffer_start: None,
                },
            );
        }
    }

    /// Look up a destination IP and optionally buffer the frame.
    ///
    /// Returns `(RouteAction, should_fire_miss)` where `should_fire_miss` is
    /// true if a route miss event should be emitted (respecting debounce).
    pub fn lookup_and_buffer(&mut self, dst_ip: Ipv4Addr, frame: &[u8]) -> (RouteAction, bool) {
        let now = Instant::now();

        let state = match self.routes.get_mut(&dst_ip) {
            Some(s) => s,
            None => return (RouteAction::NoRoute, false),
        };

        match &state.entry.destination {
            RouteDestination::RemoteWorker { worker_id } => {
                let action = RouteAction::RemoteWorker {
                    worker_id: worker_id.clone(),
                };
                (action, false)
            }
            RouteDestination::Placeholder { buffer_policy } => {
                let buffer_frames = buffer_policy.buffer_frames;
                let timeout_ms = buffer_policy.timeout_ms;

                // Check debounce inline to avoid double-borrow of self.
                let should_miss = match self.last_miss.get(&dst_ip) {
                    Some(last) if now.duration_since(*last) < self.miss_debounce => false,
                    _ => {
                        self.last_miss.insert(dst_ip, now);
                        true
                    }
                };

                if buffer_frames == 0 {
                    return (RouteAction::Drop, should_miss);
                }

                // Re-borrow state after last_miss manipulation.
                let state = self.routes.get_mut(&dst_ip).unwrap();

                // Check timeout: if buffer has been active too long, drop.
                if let Some(start) = state.buffer_start {
                    let timeout = std::time::Duration::from_millis(timeout_ms as u64);
                    if now.duration_since(start) >= timeout {
                        state.buffer.clear();
                        state.buffer_start = None;
                        return (RouteAction::Drop, should_miss);
                    }
                }

                // Buffer is within limits?
                if state.buffer.len() >= buffer_frames as usize {
                    return (RouteAction::Drop, should_miss);
                }

                // Accept into buffer.
                if state.buffer_start.is_none() {
                    state.buffer_start = Some(now);
                }
                state.buffer.push_back(frame.to_vec());
                (RouteAction::Buffered, should_miss)
            }
        }
    }

    /// Drain buffered frames for an IP (called when pod activates).
    pub fn flush_buffer(&mut self, ip: Ipv4Addr) -> Vec<Vec<u8>> {
        if let Some(state) = self.routes.get_mut(&ip) {
            state.buffer_start = None;
            state.buffer.drain(..).collect()
        } else {
            Vec::new()
        }
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use distvirt_worker_protocol::BufferPolicy;

    fn placeholder_entry(ip: Ipv4Addr, buffer_frames: u32, timeout_ms: u32) -> FabricRouteEntry {
        FabricRouteEntry {
            ip,
            mac: [0x02, 0x00, 0x00, 0x00, 0x00, 0x99],
            destination: RouteDestination::Placeholder {
                buffer_policy: BufferPolicy {
                    hold_tcp_syn: false,
                    buffer_frames,
                    timeout_ms,
                },
            },
        }
    }

    fn remote_entry(ip: Ipv4Addr, worker_id: &str) -> FabricRouteEntry {
        FabricRouteEntry {
            ip,
            mac: [0x02, 0x00, 0x00, 0x00, 0x00, 0x99],
            destination: RouteDestination::RemoteWorker {
                worker_id: worker_id.to_string(),
            },
        }
    }

    const IP_A: Ipv4Addr = Ipv4Addr::new(172, 16, 0, 10);
    const IP_B: Ipv4Addr = Ipv4Addr::new(172, 16, 0, 11);
    const FRAME: &[u8] = &[0xDE, 0xAD, 0xBE, 0xEF];

    #[test]
    fn no_route_returns_noroute() {
        let mut rt = RouteTable::new();
        let (action, miss) = rt.lookup_and_buffer(IP_A, FRAME);
        assert_eq!(action, RouteAction::NoRoute);
        assert!(!miss);
    }

    #[test]
    fn sync_replaces_all_routes() {
        let mut rt = RouteTable::new();
        rt.sync(vec![placeholder_entry(IP_A, 10, 5000)]);
        let (action, _) = rt.lookup_and_buffer(IP_A, FRAME);
        assert_eq!(action, RouteAction::Buffered);

        // Sync with empty clears.
        rt.sync(vec![]);
        let (action, _) = rt.lookup_and_buffer(IP_A, FRAME);
        assert_eq!(action, RouteAction::NoRoute);
    }

    #[test]
    fn update_adds_and_removes() {
        let mut rt = RouteTable::new();
        rt.sync(vec![
            placeholder_entry(IP_A, 10, 5000),
            placeholder_entry(IP_B, 10, 5000),
        ]);

        // Remove IP_A, add nothing.
        rt.update(vec![], vec![IP_A]);
        let (action, _) = rt.lookup_and_buffer(IP_A, FRAME);
        assert_eq!(action, RouteAction::NoRoute);

        // IP_B still there.
        let (action, _) = rt.lookup_and_buffer(IP_B, FRAME);
        assert_eq!(action, RouteAction::Buffered);
    }

    #[test]
    fn placeholder_buffers_frames() {
        let mut rt = RouteTable::new();
        rt.sync(vec![placeholder_entry(IP_A, 3, 5000)]);

        for _ in 0..3 {
            let (action, _) = rt.lookup_and_buffer(IP_A, FRAME);
            assert_eq!(action, RouteAction::Buffered);
        }

        // 4th frame exceeds buffer — dropped.
        let (action, _) = rt.lookup_and_buffer(IP_A, FRAME);
        assert_eq!(action, RouteAction::Drop);
    }

    #[test]
    fn drop_policy_zero_buffer() {
        let mut rt = RouteTable::new();
        rt.sync(vec![placeholder_entry(IP_A, 0, 5000)]);

        let (action, miss) = rt.lookup_and_buffer(IP_A, FRAME);
        assert_eq!(action, RouteAction::Drop);
        assert!(miss); // first hit fires miss
    }

    #[test]
    fn flush_drains_buffer() {
        let mut rt = RouteTable::new();
        rt.sync(vec![placeholder_entry(IP_A, 10, 5000)]);

        for _ in 0..3 {
            rt.lookup_and_buffer(IP_A, FRAME);
        }

        let frames = rt.flush_buffer(IP_A);
        assert_eq!(frames.len(), 3);

        // Flush again — empty.
        let frames = rt.flush_buffer(IP_A);
        assert!(frames.is_empty());
    }

    #[test]
    fn flush_nonexistent_ip_returns_empty() {
        let mut rt = RouteTable::new();
        let frames = rt.flush_buffer(IP_A);
        assert!(frames.is_empty());
    }

    #[test]
    fn remote_worker_returns_remote_action() {
        let mut rt = RouteTable::new();
        rt.sync(vec![remote_entry(IP_A, "worker-1")]);

        let (action, miss) = rt.lookup_and_buffer(IP_A, FRAME);
        assert_eq!(
            action,
            RouteAction::RemoteWorker {
                worker_id: "worker-1".to_string()
            }
        );
        assert!(!miss);
    }

    #[test]
    fn miss_debounce() {
        let mut rt = RouteTable::new();
        rt.miss_debounce = std::time::Duration::from_millis(100);
        rt.sync(vec![placeholder_entry(IP_A, 10, 5000)]);

        // First hit: fires miss.
        let (_, miss1) = rt.lookup_and_buffer(IP_A, FRAME);
        assert!(miss1);

        // Immediate second hit: debounced.
        let (_, miss2) = rt.lookup_and_buffer(IP_A, FRAME);
        assert!(!miss2);
    }

    #[test]
    fn buffer_timeout_expires() {
        let mut rt = RouteTable::new();
        // 0ms timeout = expires immediately on next check.
        rt.sync(vec![placeholder_entry(IP_A, 10, 0)]);

        // First frame starts the buffer.
        let (action1, _) = rt.lookup_and_buffer(IP_A, FRAME);
        assert_eq!(action1, RouteAction::Buffered);

        // Second frame: timeout_ms=0 means any elapsed time >= 0ms triggers expiry.
        // Since Instant::now() will be >= buffer_start, it expires.
        let (action2, _) = rt.lookup_and_buffer(IP_A, FRAME);
        assert_eq!(action2, RouteAction::Drop);
    }
}
