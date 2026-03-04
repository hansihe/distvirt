use std::collections::{HashMap, VecDeque};
use std::net::Ipv4Addr;
use std::time::{Duration, Instant};

use distvirt_activator::types::Action;
use distvirt_worker_protocol::ServicePolicy;

use crate::packet::FabricPacket;
use super::service_activator::ServiceProcessor;

/// What the fabric should do with a frame that matched a service IP.
#[derive(Debug)]
pub enum ServiceAction {
    /// Forward to the ready backend pod.
    Forward { pod_ip: Ipv4Addr, service_ip: Ipv4Addr },
    /// Frame was accepted into the service buffer.
    Buffered,
    /// Frame was dropped (buffer full or timed out).
    Drop,
    /// Activator processed the frame and returned actions for the fabric to execute.
    ActivatorActions {
        actions: Vec<Action>,
        service_id: String,
    },
    /// L4 stream manager processed the frame and produced outgoing frames + non-L4 actions.
    L4Result {
        actions: Vec<Action>,
        frames: Vec<Vec<u8>>,
        service_id: String,
        poll_delay: Option<Duration>,
    },
}

/// A fabric-level service entity with its own virtual IP,
/// separate from the backing pod.
struct ServiceEntity {
    service_id: String,
    ip: Ipv4Addr,
    policy: ServicePolicy,
    backend_ip: Option<Ipv4Addr>,
    /// Implicit state machine for service lifecycle:
    ///   Pending (no backend) → BackendAssigned (backend set, not ready)
    ///   → Ready (mark_ready called) → Reachable (backend IP in ip_port_table)
    ///   → Draining (backend removed or changed).
    ready: bool,
    buffer: VecDeque<Vec<u8>>,
    buffer_start: Option<Instant>,
    processor: ServiceProcessor,
}

/// Result of marking a service as ready.
#[derive(Debug)]
pub enum MarkReadyResult {
    /// L3 passthrough mode: buffered frames + backend info + activator actions.
    Passthrough {
        frames: Vec<Vec<u8>>,
        backend_ip: Ipv4Addr,
        service_ip: Ipv4Addr,
        actions: Vec<Action>,
    },
    /// L4 stream mode: outgoing frames + non-L4 actions via ServiceAction::L4Result.
    L4(ServiceAction),
}


/// Data returned by `flush_by_backend_ip` for each service whose buffer was drained.
pub struct ServiceFlushData {
    pub service_ip: Ipv4Addr,
    pub backend_ip: Ipv4Addr,
    pub frames: Vec<Vec<u8>>,
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
    pub fn create(
        &mut self,
        service_id: String,
        ip: Ipv4Addr,
        policy: ServicePolicy,
        processor: ServiceProcessor,
    ) {
        let entity = ServiceEntity {
            service_id: service_id.clone(),
            ip,
            policy,
            backend_ip: None,
            ready: false,
            buffer: VecDeque::new(),
            buffer_start: None,
            processor,
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

    /// Update the backend for a service. Clears readiness. Only clears the
    /// buffer when the backend is removed or changes to a different pod;
    /// setting a backend for the first time preserves buffered frames so
    /// `mark_ready` can flush them.
    pub fn update_backend(&mut self, service_id: &str, backend: Option<Ipv4Addr>) {
        let ip = match self.id_to_ip.get(service_id) {
            Some(ip) => *ip,
            None => return,
        };
        if let Some(entity) = self.by_ip.get_mut(&ip) {
            let old_backend_ip = entity.backend_ip;
            let has_backend = backend.is_some();
            entity.backend_ip = backend;
            entity.ready = false;

            // Clear buffer when backend is removed or IP changes to a
            // different pod. When setting a backend for the first time
            // (None → Some), preserve the buffer.
            let should_clear = match (old_backend_ip, entity.backend_ip) {
                (_, None) => true,                          // backend removed
                (Some(old), Some(new)) if old != new => true, // IP changed
                _ => false,                                 // None → Some: keep buffer
            };
            if should_clear {
                entity.buffer.clear();
                entity.buffer_start = None;
            }
            entity.processor.on_backend_update(
                has_backend,
                entity.backend_ip,
            );
        }
    }

    /// Mark a service as ready. Returns buffered frames / activator actions
    /// (L3 passthrough mode) or an L4Result (L4 stream mode).
    pub fn mark_ready(&mut self, service_id: &str) -> Option<MarkReadyResult> {
        let ip = match self.id_to_ip.get(service_id) {
            Some(ip) => *ip,
            None => return None,
        };
        let entity = self.by_ip.get_mut(&ip)?;
        if entity.backend_ip.is_none() {
            log::warn!("service '{}': mark_ready called but no backend set", service_id);
            return None;
        }
        entity.ready = true;

        log::debug!(
            "service '{}': mark_ready: buffer_len={}, has_stream_manager={}",
            entity.service_id, entity.buffer.len(),
            entity.processor.has_stream_manager()
        );

        // L4/L3 activator path: delegate to processor.
        if let Some(svc_action) = entity.processor.on_mark_ready(&entity.service_id) {
            if entity.processor.has_stream_manager() {
                return Some(MarkReadyResult::L4(svc_action));
            }
            // L3 activator: drain buffer and return Passthrough with actions.
            let frames: Vec<Vec<u8>> = entity.buffer.drain(..).collect();
            entity.buffer_start = None;
            let actions = match svc_action {
                ServiceAction::ActivatorActions { actions, .. } => actions,
                _ => Vec::new(),
            };

            log::debug!(
                "service '{}': mark_ready produced {} frames, {} actions",
                entity.service_id, frames.len(), actions.len()
            );

            let backend_ip = entity.backend_ip.unwrap();
            let service_ip = entity.ip;
            return Some(MarkReadyResult::Passthrough { frames, backend_ip, service_ip, actions });
        }

        // Passthrough: drain buffer.
        let frames: Vec<Vec<u8>> = entity.buffer.drain(..).collect();
        entity.buffer_start = None;

        log::debug!(
            "service '{}': mark_ready produced {} frames, 0 actions",
            entity.service_id, frames.len()
        );

        let backend_ip = entity.backend_ip.unwrap();
        let service_ip = entity.ip;
        Some(MarkReadyResult::Passthrough { frames, backend_ip, service_ip, actions: Vec::new() })
    }

    /// Check if a destination IP belongs to a service. If so, buffer or forward
    /// the frame and return the action + whether an activation event should fire.
    ///
    /// Returns `None` if `dst_ip` is not a service IP (caller should fall through
    /// to route table logic).
    ///
    /// `is_reachable` checks whether the backend IP is reachable (i.e. has a port
    /// in the `ip_port_table`).
    pub fn lookup_and_buffer<F>(&mut self, dst_ip: Ipv4Addr, frame: &[u8], is_reachable: F) -> Option<(ServiceAction, bool)>
    where
        F: Fn(&Ipv4Addr) -> bool,
    {
        let entity = self.by_ip.get_mut(&dst_ip)?;
        let now = Instant::now();

        // If ready with a backend and the backend IP is reachable, forward directly.
        // If the IP is not reachable (port not yet added), fall through to the
        // buffering path so frames are preserved until the port appears.
        if entity.ready {
            if let Some(pod_ip) = entity.backend_ip {
                if is_reachable(&pod_ip) {
                    let service_ip = entity.ip;
                    return Some((ServiceAction::Forward { pod_ip, service_ip }, false));
                } else {
                    log::debug!(
                        "service '{}': ready but backend IP {} not reachable, falling through to buffer",
                        entity.service_id, pod_ip
                    );
                }
            }
        }

        // L4/L3 activator path: delegate to processor.
        if !matches!(entity.processor, ServiceProcessor::Passthrough) {
            let fp = FabricPacket::new(frame)?;
            if let Some(result) = entity.processor.process_frame(
                &entity.service_id,
                fp.ip_packet(),
                frame,
            ) {
                return Some((result, false));
            }
            // process_frame returned None on L3 error — fall through to buffering.
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

    /// Look up NAT-relevant info for a service by its ID.
    /// Returns `(service_ip, backend_ip)`.
    pub fn get_nat_info_by_id(&self, service_id: &str) -> Option<(Ipv4Addr, Ipv4Addr)> {
        let ip = self.id_to_ip.get(service_id)?;
        let entity = self.by_ip.get(ip)?;
        Some((entity.ip, entity.backend_ip?))
    }

    /// Get the service_id for a given IP (used for activation events).
    pub fn get_service_id(&self, ip: &Ipv4Addr) -> Option<&str> {
        self.by_ip.get(ip).map(|e| e.service_id.as_str())
    }

    /// Look up the service IP for a given service ID.
    pub fn get_ip_by_id(&self, service_id: &str) -> Option<Ipv4Addr> {
        self.id_to_ip.get(service_id).copied()
    }

    /// Handle a smoltcp timeout for a service IP.
    ///
    /// Calls `handle_timeout()` on the StreamManager, runs the activator loop,
    /// and returns the resulting `ServiceAction` (if the service has an L4 path).
    pub fn handle_timeout_for_ip(&mut self, ip: Ipv4Addr) -> Option<ServiceAction> {
        let entity = self.by_ip.get_mut(&ip)?;
        entity.processor.handle_timeout(&entity.service_id)
    }

    /// Drain buffered frames from all ready services whose backend IP matches `ip`.
    ///
    /// Used when a new port is added: the port's IP becomes reachable, so any
    /// service buffers waiting for that IP can be flushed immediately.
    pub fn flush_by_backend_ip(&mut self, backend_ip: &Ipv4Addr) -> Vec<ServiceFlushData> {
        let mut result = Vec::new();
        for (ip, entity) in self.by_ip.iter_mut() {
            if entity.ready && entity.backend_ip.as_ref() == Some(backend_ip) && !entity.buffer.is_empty() {
                log::info!(
                    "service '{}': flush_by_backend_ip draining {} frames for IP {}",
                    entity.service_id, entity.buffer.len(), backend_ip
                );
                let frames: Vec<Vec<u8>> = entity.buffer.drain(..).collect();
                entity.buffer_start = None;
                result.push(ServiceFlushData {
                    service_ip: *ip,
                    backend_ip: entity.backend_ip.unwrap(),
                    frames,
                });
            }
        }
        if result.is_empty() {
            log::debug!(
                "flush_by_backend_ip: no ready services with buffer for IP {}",
                backend_ip
            );
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packet::with_fabric_header;

    const SVC_IP: Ipv4Addr = Ipv4Addr::new(172, 16, 0, 2);
    const POD_IP: Ipv4Addr = Ipv4Addr::new(172, 16, 0, 130);
    const FRAME: &[u8] = &[0xDE, 0xAD, 0xBE, 0xEF];

    fn default_policy() -> ServicePolicy {
        ServicePolicy {
            buffer_frames: 64,
            timeout_ms: 30000,
            activator: None,
        }
    }

    #[test]
    fn unknown_ip_returns_none() {
        let mut table = ServiceTable::new();
        assert!(table.lookup_and_buffer(SVC_IP, FRAME, |_| true).is_none());
    }

    #[test]
    fn buffers_when_not_ready() {
        let mut table = ServiceTable::new();
        table.create("svc1".into(), SVC_IP, default_policy(), ServiceProcessor::Passthrough);

        let result = table.lookup_and_buffer(SVC_IP, FRAME, |_ip| true);
        assert!(matches!(result, Some((ServiceAction::Buffered, true))));
    }

    #[test]
    fn forwards_when_ready() {
        let mut table = ServiceTable::new();
        table.create("svc1".into(), SVC_IP, default_policy(), ServiceProcessor::Passthrough);
        table.update_backend("svc1", Some(POD_IP));
        table.mark_ready("svc1");

        let result = table.lookup_and_buffer(SVC_IP, FRAME, |_ip| true);
        assert!(matches!(
            result,
            Some((ServiceAction::Forward { pod_ip, .. }, false))
            if pod_ip == POD_IP
        ));
    }

    #[test]
    fn mark_ready_returns_buffered_frames() {
        let mut table = ServiceTable::new();
        table.create("svc1".into(), SVC_IP, default_policy(), ServiceProcessor::Passthrough);
        table.update_backend("svc1", Some(POD_IP));

        // Buffer some frames.
        for _ in 0..3 {
            table.lookup_and_buffer(SVC_IP, FRAME, |_ip| true);
        }

        let result = table.mark_ready("svc1");
        match result.unwrap() {
            MarkReadyResult::Passthrough { frames, service_ip, .. } => {
                assert_eq!(frames.len(), 3);
                assert_eq!(service_ip, SVC_IP);
            }
            _ => panic!("expected Passthrough result"),
        }
    }

    #[test]
    fn update_backend_clears_ready_and_buffer() {
        let mut table = ServiceTable::new();
        table.create("svc1".into(), SVC_IP, default_policy(), ServiceProcessor::Passthrough);
        table.update_backend("svc1", Some(POD_IP));
        table.mark_ready("svc1");

        // Service is ready — now update backend clears readiness.
        table.update_backend("svc1", None);

        let result = table.lookup_and_buffer(SVC_IP, FRAME, |_ip| true);
        assert!(matches!(result, Some((ServiceAction::Buffered, _))));
    }

    #[test]
    fn destroy_removes_service() {
        let mut table = ServiceTable::new();
        table.create("svc1".into(), SVC_IP, default_policy(), ServiceProcessor::Passthrough);
        assert!(table.destroy("svc1"));
        assert!(table.lookup_and_buffer(SVC_IP, FRAME, |_| true).is_none());
    }

    #[test]
    fn activation_debounced() {
        let mut table = ServiceTable::new();
        table.create("svc1".into(), SVC_IP, default_policy(), ServiceProcessor::Passthrough);

        let (_, activate1) = table.lookup_and_buffer(SVC_IP, FRAME, |_| true).unwrap();
        assert!(activate1);

        let (_, activate2) = table.lookup_and_buffer(SVC_IP, FRAME, |_| true).unwrap();
        assert!(!activate2);
    }

    #[test]
    fn buffer_capacity_drops_excess() {
        let mut table = ServiceTable::new();
        table.create(
            "svc1".into(),
            SVC_IP,
            ServicePolicy {
                buffer_frames: 2,
                timeout_ms: 30000,
                activator: None,
            },
            ServiceProcessor::Passthrough,
        );

        table.lookup_and_buffer(SVC_IP, FRAME, |_ip| true);
        table.lookup_and_buffer(SVC_IP, FRAME, |_ip| true);
        let result = table.lookup_and_buffer(SVC_IP, FRAME, |_ip| true);
        assert!(matches!(result, Some((ServiceAction::Drop, _))));
    }

    /// Regression test for Bug 1: `update_backend` clears buffered frames.
    ///
    /// The orchestrator calls `update_backend` followed by `mark_ready`.
    /// Frames buffered *before* the backend is set should survive
    /// `update_backend` and be returned by `mark_ready`.
    #[test]
    fn update_backend_preserves_buffered_frames() {
        let mut table = ServiceTable::new();
        table.create("svc1".into(), SVC_IP, default_policy(), ServiceProcessor::Passthrough);

        // Buffer 3 frames while there is no backend yet.
        for _ in 0..3 {
            let result = table.lookup_and_buffer(SVC_IP, FRAME, |_ip| true);
            assert!(matches!(result, Some((ServiceAction::Buffered, _))));
        }

        // Set the backend — this should NOT clear the buffer.
        table.update_backend("svc1", Some(POD_IP));

        // Mark ready — should return the 3 buffered frames.
        let result = table.mark_ready("svc1");
        match result.unwrap() {
            MarkReadyResult::Passthrough { frames, .. } => {
                assert_eq!(
                    frames.len(),
                    3,
                    "update_backend should not clear frames buffered before backend was set"
                );
            }
            _ => panic!("expected Passthrough result"),
        }
    }

    /// Try to load the TCP activator. Returns None if WASM components aren't built.
    fn try_load_tcp_activator() -> Option<(distvirt_activator::ActivatorRuntime, distvirt_activator::ActivatorInstance)> {
        let component_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../activators/target/components");
        let runtime = distvirt_activator::ActivatorRuntime::new(&component_dir).ok()?;
        let component = runtime.get_component("tcp")?;
        let instance = distvirt_activator::ActivatorInstance::new(runtime.engine(), component).ok()?;
        Some((runtime, instance))
    }

    /// Build a valid TCP SYN frame with fabric header using etherparse.
    /// Produces L3 fabric format: [fabric_hdr(3)][IP+TCP].
    fn make_tcp_frame_for_service(
        src_ip: [u8; 4],
        dst_ip: [u8; 4],
        src_port: u16,
        dst_port: u16,
    ) -> Vec<u8> {
        use etherparse::PacketBuilder;

        let builder = PacketBuilder::ipv4(src_ip, dst_ip, 64)
            .tcp(src_port, dst_port, 1000, 65535);

        let mut ip_packet = Vec::new();
        builder.write(&mut ip_packet, &[]).unwrap();

        // Set SYN flag: ip(20) + tcp flags at byte 13
        let tcp_start = 20;
        ip_packet[tcp_start + 13] = 0x02; // SYN

        with_fabric_header(0, 0, &ip_packet)
    }

    #[test]
    fn l4_mark_ready_processes_backend_available() {
        let Some((_runtime, instance)) = try_load_tcp_activator() else {
            eprintln!("SKIP: TCP activator WASM not built");
            return;
        };

        let sm = distvirt_activator::StreamManager::new(
            distvirt_activator::StreamManagerConfig {
                service_ip: SVC_IP,
                listen_ports: vec![80],
                tcp_buffer_size: 4096,
                listen_pool_size: 2,
            },
        );

        let mut table = ServiceTable::new();
        table.create(
            "svc1".into(),
            SVC_IP,
            ServicePolicy {
                buffer_frames: 64,
                timeout_ms: 30000,
                activator: Some(distvirt_worker_protocol::ActivatorConfig::Tcp {
                    ports: None,
                    tcp_only: false,
                    max_flows: 1024,
                }),
            },
            ServiceProcessor::L4 { activator: Some(instance), stream_manager: sm },
        );

        // Feed a TCP SYN to the L4 path (after vnet header).
        let syn_frame = make_tcp_frame_for_service(
            [10, 0, 0, 1],
            SVC_IP.octets(),
            12345,
            80,
        );
        let result = table.lookup_and_buffer(SVC_IP, &syn_frame, |_ip| true);
        assert!(
            matches!(result, Some((ServiceAction::L4Result { .. }, _))),
            "SYN should trigger L4Result"
        );

        // Set backend and mark ready.
        table.update_backend("svc1", Some(POD_IP));
        let ready_result = table.mark_ready("svc1");
        assert!(ready_result.is_some(), "mark_ready should return Some");

        match ready_result.unwrap() {
            MarkReadyResult::L4(ServiceAction::L4Result { .. }) => {
                // In the L4 path, the stream manager handles TCP buffering
                // (via smoltcp), not the activator's flow map. So
                // BackendAvailable(true) won't produce ReplayPacket actions
                // here — the SM replays traffic through its own TCP state
                // machine. We just verify the L4 result path is taken.
            }
            other => panic!("expected L4 result, got: {:?}", other),
        }
    }

    #[test]
    fn handle_timeout_for_ip_returns_l4_result() {
        let sm = distvirt_activator::StreamManager::new(
            distvirt_activator::StreamManagerConfig {
                service_ip: SVC_IP,
                listen_ports: vec![80],
                tcp_buffer_size: 4096,
                listen_pool_size: 2,
            },
        );

        let mut table = ServiceTable::new();
        table.create(
            "svc1".into(),
            SVC_IP,
            default_policy(),
            ServiceProcessor::L4 {
                activator: None,
                stream_manager: sm,
            },
        );

        // handle_timeout_for_ip on a service with a StreamManager should return Some(L4Result).
        let result = table.handle_timeout_for_ip(SVC_IP);
        assert!(result.is_some(), "handle_timeout_for_ip should return Some for L4 service");
        assert!(
            matches!(result.unwrap(), ServiceAction::L4Result { .. }),
            "should return L4Result"
        );
    }

    #[test]
    fn handle_timeout_for_ip_returns_none_for_l3() {
        let mut table = ServiceTable::new();
        table.create("svc1".into(), SVC_IP, default_policy(), ServiceProcessor::Passthrough);

        // L3 service (no StreamManager) should return None.
        let result = table.handle_timeout_for_ip(SVC_IP);
        assert!(result.is_none(), "handle_timeout_for_ip should return None for L3 service");
    }

    #[test]
    fn activator_mark_ready_returns_replay_actions() {
        let Some((_runtime, instance)) = try_load_tcp_activator() else {
            eprintln!("SKIP: TCP activator WASM not built");
            return;
        };

        let mut table = ServiceTable::new();
        table.create(
            "svc1".into(),
            SVC_IP,
            ServicePolicy {
                buffer_frames: 64,
                timeout_ms: 30000,
                activator: Some(distvirt_worker_protocol::ActivatorConfig::Tcp {
                    ports: None,
                    tcp_only: false,
                    max_flows: 1024,
                }),
            },
            ServiceProcessor::L3 {
                activator: instance,
                flow_tracker: distvirt_activator::FlowTracker::new(),
            },
        );

        // Feed a TCP SYN frame via lookup_and_buffer.
        let syn_frame = make_tcp_frame_for_service(
            [10, 0, 0, 1],
            SVC_IP.octets(),
            12345,
            80,
        );
        let result = table.lookup_and_buffer(SVC_IP, &syn_frame, |_ip| true);
        assert!(
            matches!(result, Some((ServiceAction::ActivatorActions { .. }, _))),
            "SYN should trigger activator actions"
        );

        // Set backend and mark ready.
        table.update_backend("svc1", Some(POD_IP));
        let ready_result = table.mark_ready("svc1");
        assert!(ready_result.is_some(), "mark_ready should return Some");

        match ready_result.unwrap() {
            MarkReadyResult::Passthrough { service_ip, actions, .. } => {
                assert_eq!(service_ip, SVC_IP);
                let replay_count = actions
                    .iter()
                    .filter(|a| matches!(a, Action::ReplayPacket(_)))
                    .count();
                assert!(replay_count > 0, "mark_ready should return ReplayPacket actions for buffered SYN");
            }
            _ => panic!("expected Passthrough result"),
        }
    }
}
