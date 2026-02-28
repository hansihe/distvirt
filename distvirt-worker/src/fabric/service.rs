use std::collections::{HashMap, VecDeque};
use std::net::Ipv4Addr;
use std::time::{Duration, Instant};

use distvirt_worker_protocol::ServicePolicy;

/// What the fabric should do with a frame that matched a service IP.
#[derive(Debug, PartialEq, Eq)]
pub enum ServiceAction {
    /// Forward to the ready backend pod.
    Forward { pod_ip: Ipv4Addr, pod_mac: [u8; 6] },
    /// Frame was accepted into the service buffer.
    Buffered,
    /// Frame was dropped (buffer full or timed out).
    Drop,
}

/// A fabric-level service entity with its own virtual IP/MAC,
/// separate from the backing pod.
struct ServiceEntity {
    service_id: String,
    ip: Ipv4Addr,
    mac: [u8; 6],
    policy: ServicePolicy,
    backend_ip: Option<Ipv4Addr>,
    backend_mac: Option<[u8; 6]>,
    ready: bool,
    buffer: VecDeque<Vec<u8>>,
    buffer_start: Option<Instant>,
}

/// Table of service entities indexed by IP for fast frame-path lookup.
pub struct ServiceTable {
    by_ip: HashMap<Ipv4Addr, ServiceEntity>,
    id_to_ip: HashMap<String, Ipv4Addr>,
    last_activation: HashMap<Ipv4Addr, Instant>,
    activation_debounce: Duration,
}

impl ServiceTable {
    pub fn new() -> Self {
        ServiceTable {
            by_ip: HashMap::new(),
            id_to_ip: HashMap::new(),
            last_activation: HashMap::new(),
            activation_debounce: Duration::from_secs(1),
        }
    }

    /// Register a new service entity.
    pub fn create(&mut self, service_id: String, ip: Ipv4Addr, mac: [u8; 6], policy: ServicePolicy) {
        let entity = ServiceEntity {
            service_id: service_id.clone(),
            ip,
            mac,
            policy,
            backend_ip: None,
            backend_mac: None,
            ready: false,
            buffer: VecDeque::new(),
            buffer_start: None,
        };
        self.by_ip.insert(ip, entity);
        self.id_to_ip.insert(service_id, ip);
    }

    /// Remove a service entity, returning it if it existed.
    pub fn destroy(&mut self, service_id: &str) -> bool {
        if let Some(ip) = self.id_to_ip.remove(service_id) {
            self.by_ip.remove(&ip);
            self.last_activation.remove(&ip);
            true
        } else {
            false
        }
    }

    /// Update the backend for a service. Clears readiness and resets the buffer.
    pub fn update_backend(&mut self, service_id: &str, backend: Option<(Ipv4Addr, [u8; 6])>) {
        let ip = match self.id_to_ip.get(service_id) {
            Some(ip) => *ip,
            None => return,
        };
        if let Some(entity) = self.by_ip.get_mut(&ip) {
            match backend {
                Some((pod_ip, pod_mac)) => {
                    entity.backend_ip = Some(pod_ip);
                    entity.backend_mac = Some(pod_mac);
                }
                None => {
                    entity.backend_ip = None;
                    entity.backend_mac = None;
                }
            }
            entity.ready = false;
            entity.buffer.clear();
            entity.buffer_start = None;
        }
    }

    /// Mark a service as ready. Returns the buffered frames and backend MAC
    /// so the caller can flush them to the backend port.
    pub fn mark_ready(&mut self, service_id: &str) -> Option<(Vec<Vec<u8>>, [u8; 6])> {
        let ip = match self.id_to_ip.get(service_id) {
            Some(ip) => *ip,
            None => return None,
        };
        let entity = self.by_ip.get_mut(&ip)?;
        if entity.backend_mac.is_none() {
            return None;
        }
        entity.ready = true;
        let backend_mac = entity.backend_mac.unwrap();
        let frames: Vec<Vec<u8>> = entity.buffer.drain(..).collect();
        entity.buffer_start = None;
        Some((frames, backend_mac))
    }

    /// Check if a destination IP belongs to a service. If so, buffer or forward
    /// the frame and return the action + whether an activation event should fire.
    ///
    /// Returns `None` if `dst_ip` is not a service IP (caller should fall through
    /// to route table logic).
    pub fn lookup_and_buffer(&mut self, dst_ip: Ipv4Addr, frame: &[u8]) -> Option<(ServiceAction, bool)> {
        let entity = self.by_ip.get_mut(&dst_ip)?;
        let now = Instant::now();

        // If ready with a backend, forward directly.
        if entity.ready {
            if let (Some(pod_ip), Some(pod_mac)) = (entity.backend_ip, entity.backend_mac) {
                return Some((ServiceAction::Forward { pod_ip, pod_mac }, false));
            }
        }

        // Not ready or no backend — check if we should activate (with debounce).
        let should_activate = match self.last_activation.get(&dst_ip) {
            Some(last) if now.duration_since(*last) < self.activation_debounce => false,
            _ => {
                self.last_activation.insert(dst_ip, now);
                true
            }
        };

        // Re-borrow entity after last_activation manipulation.
        let entity = self.by_ip.get_mut(&dst_ip).unwrap();

        let buffer_frames = entity.policy.buffer_frames;
        let timeout_ms = entity.policy.timeout_ms;

        if buffer_frames == 0 {
            return Some((ServiceAction::Drop, should_activate));
        }

        // Check timeout.
        if let Some(start) = entity.buffer_start {
            let timeout = Duration::from_millis(timeout_ms as u64);
            if now.duration_since(start) >= timeout {
                entity.buffer.clear();
                entity.buffer_start = None;
                return Some((ServiceAction::Drop, should_activate));
            }
        }

        // Check buffer capacity.
        if entity.buffer.len() >= buffer_frames as usize {
            return Some((ServiceAction::Drop, should_activate));
        }

        // Accept into buffer.
        if entity.buffer_start.is_none() {
            entity.buffer_start = Some(now);
        }
        entity.buffer.push_back(frame.to_vec());
        Some((ServiceAction::Buffered, should_activate))
    }

    /// Look up the service MAC for a given IP (used for ARP replies).
    pub fn get_mac(&self, ip: &Ipv4Addr) -> Option<[u8; 6]> {
        self.by_ip.get(ip).map(|e| e.mac)
    }

    /// Get the service_id for a given IP (used for activation events).
    pub fn get_service_id(&self, ip: &Ipv4Addr) -> Option<&str> {
        self.by_ip.get(ip).map(|e| e.service_id.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SVC_IP: Ipv4Addr = Ipv4Addr::new(172, 16, 0, 2);
    const SVC_MAC: [u8; 6] = [0x06, 0x00, 0xAC, 0x10, 0x00, 0x02];
    const POD_IP: Ipv4Addr = Ipv4Addr::new(172, 16, 0, 130);
    const POD_MAC: [u8; 6] = [0x06, 0x00, 0xAC, 0x10, 0x00, 0x82];
    const FRAME: &[u8] = &[0xDE, 0xAD, 0xBE, 0xEF];

    fn default_policy() -> ServicePolicy {
        ServicePolicy {
            buffer_frames: 64,
            timeout_ms: 30000,
        }
    }

    #[test]
    fn unknown_ip_returns_none() {
        let mut table = ServiceTable::new();
        assert!(table.lookup_and_buffer(SVC_IP, FRAME).is_none());
    }

    #[test]
    fn buffers_when_not_ready() {
        let mut table = ServiceTable::new();
        table.create("svc1".into(), SVC_IP, SVC_MAC, default_policy());

        let result = table.lookup_and_buffer(SVC_IP, FRAME);
        assert!(matches!(result, Some((ServiceAction::Buffered, true))));
    }

    #[test]
    fn forwards_when_ready() {
        let mut table = ServiceTable::new();
        table.create("svc1".into(), SVC_IP, SVC_MAC, default_policy());
        table.update_backend("svc1", Some((POD_IP, POD_MAC)));
        table.mark_ready("svc1");

        let result = table.lookup_and_buffer(SVC_IP, FRAME);
        assert!(matches!(
            result,
            Some((ServiceAction::Forward { pod_ip, pod_mac }, false))
            if pod_ip == POD_IP && pod_mac == POD_MAC
        ));
    }

    #[test]
    fn mark_ready_returns_buffered_frames() {
        let mut table = ServiceTable::new();
        table.create("svc1".into(), SVC_IP, SVC_MAC, default_policy());
        table.update_backend("svc1", Some((POD_IP, POD_MAC)));

        // Buffer some frames.
        for _ in 0..3 {
            table.lookup_and_buffer(SVC_IP, FRAME);
        }

        let result = table.mark_ready("svc1");
        let (frames, mac) = result.unwrap();
        assert_eq!(frames.len(), 3);
        assert_eq!(mac, POD_MAC);
    }

    #[test]
    fn update_backend_clears_ready_and_buffer() {
        let mut table = ServiceTable::new();
        table.create("svc1".into(), SVC_IP, SVC_MAC, default_policy());
        table.update_backend("svc1", Some((POD_IP, POD_MAC)));
        table.mark_ready("svc1");

        // Service is ready — now update backend clears readiness.
        table.update_backend("svc1", None);

        let result = table.lookup_and_buffer(SVC_IP, FRAME);
        assert!(matches!(result, Some((ServiceAction::Buffered, _))));
    }

    #[test]
    fn destroy_removes_service() {
        let mut table = ServiceTable::new();
        table.create("svc1".into(), SVC_IP, SVC_MAC, default_policy());
        assert!(table.destroy("svc1"));
        assert!(table.lookup_and_buffer(SVC_IP, FRAME).is_none());
    }

    #[test]
    fn activation_debounced() {
        let mut table = ServiceTable::new();
        table.create("svc1".into(), SVC_IP, SVC_MAC, default_policy());

        let (_, activate1) = table.lookup_and_buffer(SVC_IP, FRAME).unwrap();
        assert!(activate1);

        let (_, activate2) = table.lookup_and_buffer(SVC_IP, FRAME).unwrap();
        assert!(!activate2);
    }

    #[test]
    fn buffer_capacity_drops_excess() {
        let mut table = ServiceTable::new();
        table.create(
            "svc1".into(),
            SVC_IP,
            SVC_MAC,
            ServicePolicy {
                buffer_frames: 2,
                timeout_ms: 30000,
            },
        );

        table.lookup_and_buffer(SVC_IP, FRAME);
        table.lookup_and_buffer(SVC_IP, FRAME);
        let result = table.lookup_and_buffer(SVC_IP, FRAME);
        assert!(matches!(result, Some((ServiceAction::Drop, _))));
    }

    #[test]
    fn get_mac_returns_service_mac() {
        let mut table = ServiceTable::new();
        table.create("svc1".into(), SVC_IP, SVC_MAC, default_policy());
        assert_eq!(table.get_mac(&SVC_IP), Some(SVC_MAC));
    }
}
