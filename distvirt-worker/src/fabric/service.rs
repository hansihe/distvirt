use std::collections::{HashMap, VecDeque};
use std::net::Ipv4Addr;
use std::time::{Duration, Instant};

use distvirt_activator::{ActivatorInstance, FlowTracker, StreamManager, StreamManagerOutput, is_l4_action, parse_frame_to_packet_info};
use distvirt_activator::types::{Action, Event};
use distvirt_worker_protocol::ServicePolicy;

use super::switch::FabricFrame;

/// What the fabric should do with a frame that matched a service IP.
#[derive(Debug)]
pub enum ServiceAction {
    /// Forward to the ready backend pod.
    Forward { pod_ip: Ipv4Addr, pod_mac: [u8; 6], service_ip: Ipv4Addr, service_mac: [u8; 6] },
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

/// A fabric-level service entity with its own virtual IP/MAC,
/// separate from the backing pod.
struct ServiceEntity {
    service_id: String,
    ip: Ipv4Addr,
    mac: [u8; 6],
    policy: ServicePolicy,
    backend_ip: Option<Ipv4Addr>,
    backend_mac: Option<[u8; 6]>,
    /// Implicit state machine for service lifecycle:
    ///   Pending (no backend) → BackendAssigned (backend set, not ready)
    ///   → Ready (mark_ready called) → Reachable (backend MAC in mac_table)
    ///   → Draining (backend removed or changed).
    /// Formalizing this as an explicit enum is a future option; stateright
    /// could be used to model-check transitions.
    ready: bool,
    buffer: VecDeque<Vec<u8>>,
    buffer_start: Option<Instant>,
    activator: Option<ActivatorInstance>,
    flow_tracker: Option<FlowTracker>,
    stream_manager: Option<StreamManager>,
}

/// Result of marking a service as ready.
#[derive(Debug)]
pub enum MarkReadyResult {
    /// L3 passthrough mode: buffered frames + backend info + activator actions.
    Passthrough {
        frames: Vec<Vec<u8>>,
        backend_mac: [u8; 6],
        backend_ip: Ipv4Addr,
        service_ip: Ipv4Addr,
        service_mac: [u8; 6],
        actions: Vec<Action>,
    },
    /// L4 stream mode: outgoing frames + non-L4 actions via ServiceAction::L4Result.
    L4(ServiceAction),
}

/// Run L4 event cycle on a service entity: feed eth_frame to StreamManager,
/// run activator event loop, collect outgoing frames and non-L4 actions.
fn run_l4_cycle(entity: &mut ServiceEntity, eth_frame: &[u8]) -> ServiceAction {
    let sm = entity.stream_manager.as_mut().unwrap();
    let sm_output = sm.receive_frame(eth_frame);
    process_l4_output(entity, sm_output)
}

/// Process StreamManagerOutput through the activator event loop (bounded to 4 rounds).
fn process_l4_output(entity: &mut ServiceEntity, mut sm_output: StreamManagerOutput) -> ServiceAction {
    let sm = entity.stream_manager.as_mut().unwrap();
    let mut all_non_l4_actions = Vec::new();

    if let Some(ref mut activator) = entity.activator {
        for _ in 0..4 {
            for event in sm_output.events.drain(..) {
                activator.push_event(event);
            }
            if !activator.has_pending_events() {
                break;
            }
            let actions = match activator.process_events() {
                Ok(a) => a,
                Err(e) => {
                    log::error!("activator error for service '{}': {:#}", entity.service_id, e);
                    break;
                }
            };
            let mut new_events = Vec::new();
            for action in &actions {
                if is_l4_action(action) {
                    let out = sm.execute_action(action);
                    new_events.extend(out.events);
                    sm_output.frames.extend(out.frames);
                }
            }
            all_non_l4_actions.extend(actions.into_iter().filter(|a| !is_l4_action(a)));
            sm_output.events = new_events;
        }

        // Warn if the event loop didn't fully converge within 4 rounds.
        if !sm_output.events.is_empty() || activator.has_pending_events() {
            log::warn!(
                "service '{}': L4 event loop hit 4-round cap with {} pending SM events and {} pending activator events",
                entity.service_id,
                sm_output.events.len(),
                if activator.has_pending_events() { "some" } else { "no" },
            );
        }
    }

    let poll_delay = sm.poll_delay();
    ServiceAction::L4Result {
        actions: all_non_l4_actions,
        frames: sm_output.frames,
        service_id: entity.service_id.clone(),
        poll_delay,
    }
}

/// Data returned by `flush_by_backend_mac` for each service whose buffer was drained.
pub struct ServiceFlushData {
    pub service_ip: Ipv4Addr,
    pub service_mac: [u8; 6],
    pub backend_ip: Ipv4Addr,
    pub backend_mac: [u8; 6],
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
        mac: [u8; 6],
        policy: ServicePolicy,
        activator: Option<ActivatorInstance>,
        stream_manager: Option<StreamManager>,
    ) {
        let flow_tracker = if activator.is_some() && stream_manager.is_none() {
            Some(FlowTracker::new())
        } else {
            None
        };
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
            activator,
            flow_tracker,
            stream_manager,
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
    pub fn update_backend(&mut self, service_id: &str, backend: Option<(Ipv4Addr, [u8; 6])>) {
        let ip = match self.id_to_ip.get(service_id) {
            Some(ip) => *ip,
            None => return,
        };
        if let Some(entity) = self.by_ip.get_mut(&ip) {
            let old_backend_mac = entity.backend_mac;
            let has_backend = backend.is_some();
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

            // Clear buffer when backend is removed or MAC changes to a
            // different pod. When setting a backend for the first time
            // (None → Some), preserve the buffer.
            let should_clear = match (old_backend_mac, entity.backend_mac) {
                (_, None) => true,                          // backend removed
                (Some(old), Some(new)) if old != new => true, // MAC changed
                _ => false,                                 // None → Some: keep buffer
            };
            if should_clear {
                entity.buffer.clear();
                entity.buffer_start = None;
            }
            if let Some(ref mut sm) = entity.stream_manager {
                sm.update_backend(
                    entity.backend_ip,
                    entity.backend_mac,
                );
            }
            if let Some(ref mut activator) = entity.activator {
                activator.push_event(Event::BackendAvailable(has_backend));
            }
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
        if entity.backend_mac.is_none() {
            log::warn!("service '{}': mark_ready called but no backend_mac set", service_id);
            return None;
        }
        entity.ready = true;
        let backend_mac = entity.backend_mac.unwrap();

        log::debug!(
            "service '{}': mark_ready: buffer_len={}, has_activator={}, has_stream_manager={}",
            entity.service_id, entity.buffer.len(),
            entity.activator.is_some(), entity.stream_manager.is_some()
        );

        // L4 path: push BackendAvailable event through the stream manager cycle.
        if entity.stream_manager.is_some() {
            if let Some(ref mut activator) = entity.activator {
                activator.push_event(Event::BackendAvailable(true));
            }
            let sm = entity.stream_manager.as_mut().unwrap();
            let sm_output = sm.handle_timeout();
            let svc_action = process_l4_output(entity, sm_output);
            return Some(MarkReadyResult::L4(svc_action));
        }

        let frames: Vec<Vec<u8>> = entity.buffer.drain(..).collect();
        entity.buffer_start = None;

        let actions = if let Some(ref mut activator) = entity.activator {
            activator.push_event(Event::BackendAvailable(true));
            match activator.process_events() {
                Ok(actions) => actions,
                Err(e) => {
                    log::error!("activator error for service '{}': {:#}", entity.service_id, e);
                    Vec::new()
                }
            }
        } else {
            Vec::new()
        };

        log::debug!(
            "service '{}': mark_ready produced {} frames, {} actions",
            entity.service_id, frames.len(), actions.len()
        );

        let backend_ip = entity.backend_ip.unwrap();
        let service_ip = entity.ip;
        let service_mac = entity.mac;
        Some(MarkReadyResult::Passthrough { frames, backend_mac, backend_ip, service_ip, service_mac, actions })
    }

    /// Check if a destination IP belongs to a service. If so, buffer or forward
    /// the frame and return the action + whether an activation event should fire.
    ///
    /// Returns `None` if `dst_ip` is not a service IP (caller should fall through
    /// to route table logic).
    pub fn lookup_and_buffer<F>(&mut self, dst_ip: Ipv4Addr, frame: &[u8], is_reachable: F) -> Option<(ServiceAction, bool)>
    where
        F: Fn(&[u8; 6]) -> bool,
    {
        let entity = self.by_ip.get_mut(&dst_ip)?;
        let now = Instant::now();

        // If ready with a backend and the backend MAC is reachable, forward directly.
        // If the MAC is not reachable (port not yet added), fall through to the
        // buffering path so frames are preserved until the port appears.
        if entity.ready {
            if let (Some(pod_ip), Some(pod_mac)) = (entity.backend_ip, entity.backend_mac) {
                if is_reachable(&pod_mac) {
                    let service_ip = entity.ip;
                    let service_mac = entity.mac;
                    return Some((ServiceAction::Forward { pod_ip, pod_mac, service_ip, service_mac }, false));
                } else {
                    log::debug!(
                        "service '{}': ready but backend MAC {} not reachable, falling through to buffer",
                        entity.service_id, super::switch::format_mac(&pod_mac)
                    );
                }
            }
        }

        // L4 stream manager path: feed raw frame, run activator event loop.
        if entity.stream_manager.is_some() {
            let ff = FabricFrame::new(frame)?;
            let result = run_l4_cycle(entity, ff.eth_payload());
            return Some((result, false));
        }

        // L3 activator path: parse frame, push event, process.
        if let Some(ref mut activator) = entity.activator {
            let ff = FabricFrame::new(frame)?;
            let flow_tracker = entity.flow_tracker.as_mut().unwrap();
            if let Some(packet_info) = parse_frame_to_packet_info(ff.eth_payload(), frame, flow_tracker) {
                activator.push_event(Event::Packet(packet_info));
            }
            match activator.process_events() {
                Ok(actions) => {
                    let service_id = entity.service_id.clone();
                    return Some((ServiceAction::ActivatorActions { actions, service_id }, false));
                }
                Err(e) => {
                    log::error!("activator error for service '{}': {:#}", entity.service_id, e);
                    // Fall through to passthrough buffering on error
                }
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

    /// Look up NAT-relevant info for a service by its ID.
    /// Returns `(service_ip, service_mac, backend_ip, backend_mac)`.
    pub fn get_nat_info_by_id(&self, service_id: &str) -> Option<(Ipv4Addr, [u8; 6], Ipv4Addr, [u8; 6])> {
        let ip = self.id_to_ip.get(service_id)?;
        let entity = self.by_ip.get(ip)?;
        Some((entity.ip, entity.mac, entity.backend_ip?, entity.backend_mac?))
    }

    /// Look up the service MAC for a given IP (used for ARP replies).
    pub fn get_mac(&self, ip: &Ipv4Addr) -> Option<[u8; 6]> {
        self.by_ip.get(ip).map(|e| e.mac)
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
        let sm = entity.stream_manager.as_mut()?;
        let sm_output = sm.handle_timeout();
        Some(process_l4_output(entity, sm_output))
    }

    /// Drain buffered frames from all ready services whose backend MAC matches `mac`.
    ///
    /// Used when a new port is added: the port's MAC becomes reachable, so any
    /// service buffers waiting for that MAC can be flushed immediately.
    pub fn flush_by_backend_mac(&mut self, mac: &[u8; 6]) -> Vec<ServiceFlushData> {
        let mut result = Vec::new();
        for (ip, entity) in self.by_ip.iter_mut() {
            if entity.ready && entity.backend_mac.as_ref() == Some(mac) && !entity.buffer.is_empty() {
                log::info!(
                    "service '{}': flush_by_backend_mac draining {} frames for MAC {}",
                    entity.service_id, entity.buffer.len(), super::switch::format_mac(mac)
                );
                let frames: Vec<Vec<u8>> = entity.buffer.drain(..).collect();
                entity.buffer_start = None;
                result.push(ServiceFlushData {
                    service_ip: *ip,
                    service_mac: entity.mac,
                    backend_ip: entity.backend_ip.unwrap(),
                    backend_mac: *mac,
                    frames,
                });
            }
        }
        if result.is_empty() {
            log::debug!(
                "flush_by_backend_mac: no ready services with buffer for MAC {}",
                super::switch::format_mac(mac)
            );
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::switch::with_vnet_header;

    const SVC_IP: Ipv4Addr = Ipv4Addr::new(172, 16, 0, 2);
    const SVC_MAC: [u8; 6] = [0x06, 0x00, 0xAC, 0x10, 0x00, 0x02];
    const POD_IP: Ipv4Addr = Ipv4Addr::new(172, 16, 0, 130);
    const POD_MAC: [u8; 6] = [0x06, 0x00, 0xAC, 0x10, 0x00, 0x82];
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
        table.create("svc1".into(), SVC_IP, SVC_MAC, default_policy(), None, None);

        let result = table.lookup_and_buffer(SVC_IP, FRAME, |_| true);
        assert!(matches!(result, Some((ServiceAction::Buffered, true))));
    }

    #[test]
    fn forwards_when_ready() {
        let mut table = ServiceTable::new();
        table.create("svc1".into(), SVC_IP, SVC_MAC, default_policy(), None, None);
        table.update_backend("svc1", Some((POD_IP, POD_MAC)));
        table.mark_ready("svc1");

        let result = table.lookup_and_buffer(SVC_IP, FRAME, |_| true);
        assert!(matches!(
            result,
            Some((ServiceAction::Forward { pod_ip, pod_mac, .. }, false))
            if pod_ip == POD_IP && pod_mac == POD_MAC
        ));
    }

    #[test]
    fn mark_ready_returns_buffered_frames() {
        let mut table = ServiceTable::new();
        table.create("svc1".into(), SVC_IP, SVC_MAC, default_policy(), None, None);
        table.update_backend("svc1", Some((POD_IP, POD_MAC)));

        // Buffer some frames.
        for _ in 0..3 {
            table.lookup_and_buffer(SVC_IP, FRAME, |_| true);
        }

        let result = table.mark_ready("svc1");
        match result.unwrap() {
            MarkReadyResult::Passthrough { frames, backend_mac, service_ip, service_mac, .. } => {
                assert_eq!(frames.len(), 3);
                assert_eq!(backend_mac, POD_MAC);
                assert_eq!(service_ip, SVC_IP);
                assert_eq!(service_mac, SVC_MAC);
            }
            _ => panic!("expected Passthrough result"),
        }
    }

    #[test]
    fn update_backend_clears_ready_and_buffer() {
        let mut table = ServiceTable::new();
        table.create("svc1".into(), SVC_IP, SVC_MAC, default_policy(), None, None);
        table.update_backend("svc1", Some((POD_IP, POD_MAC)));
        table.mark_ready("svc1");

        // Service is ready — now update backend clears readiness.
        table.update_backend("svc1", None);

        let result = table.lookup_and_buffer(SVC_IP, FRAME, |_| true);
        assert!(matches!(result, Some((ServiceAction::Buffered, _))));
    }

    #[test]
    fn destroy_removes_service() {
        let mut table = ServiceTable::new();
        table.create("svc1".into(), SVC_IP, SVC_MAC, default_policy(), None, None);
        assert!(table.destroy("svc1"));
        assert!(table.lookup_and_buffer(SVC_IP, FRAME, |_| true).is_none());
    }

    #[test]
    fn activation_debounced() {
        let mut table = ServiceTable::new();
        table.create("svc1".into(), SVC_IP, SVC_MAC, default_policy(), None, None);

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
            SVC_MAC,
            ServicePolicy {
                buffer_frames: 2,
                timeout_ms: 30000,
                activator: None,
            },
            None,
            None,
        );

        table.lookup_and_buffer(SVC_IP, FRAME, |_| true);
        table.lookup_and_buffer(SVC_IP, FRAME, |_| true);
        let result = table.lookup_and_buffer(SVC_IP, FRAME, |_| true);
        assert!(matches!(result, Some((ServiceAction::Drop, _))));
    }

    /// Regression test for Bug 1: `update_backend` clears buffered frames.
    ///
    /// The orchestrator calls `update_backend` followed by `mark_ready`.
    /// Frames buffered *before* the backend is set should survive
    /// `update_backend` and be returned by `mark_ready`.
    ///
    /// **Expected to fail** until the bug is fixed: `update_backend` calls
    /// `buffer.clear()` unconditionally, so the 3 buffered frames are lost.
    #[test]
    fn update_backend_preserves_buffered_frames() {
        let mut table = ServiceTable::new();
        table.create("svc1".into(), SVC_IP, SVC_MAC, default_policy(), None, None);

        // Buffer 3 frames while there is no backend yet.
        for _ in 0..3 {
            let result = table.lookup_and_buffer(SVC_IP, FRAME, |_| true);
            assert!(matches!(result, Some((ServiceAction::Buffered, _))));
        }

        // Set the backend — this should NOT clear the buffer.
        table.update_backend("svc1", Some((POD_IP, POD_MAC)));

        // Mark ready — should return the 3 buffered frames.
        let result = table.mark_ready("svc1");
        match result.unwrap() {
            MarkReadyResult::Passthrough { frames, backend_mac, .. } => {
                assert_eq!(backend_mac, POD_MAC);
                assert_eq!(
                    frames.len(),
                    3,
                    "update_backend should not clear frames buffered before backend was set"
                );
            }
            _ => panic!("expected Passthrough result"),
        }
    }

    #[test]
    fn get_mac_returns_service_mac() {
        let mut table = ServiceTable::new();
        table.create("svc1".into(), SVC_IP, SVC_MAC, default_policy(), None, None);
        assert_eq!(table.get_mac(&SVC_IP), Some(SVC_MAC));
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

    /// Build a valid TCP SYN frame with vnet header using etherparse.
    fn make_tcp_frame_for_service(
        dst_mac: [u8; 6],
        src_mac: [u8; 6],
        src_ip: [u8; 4],
        dst_ip: [u8; 4],
        src_port: u16,
        dst_port: u16,
    ) -> Vec<u8> {
        use etherparse::PacketBuilder;

        let builder = PacketBuilder::ethernet2(src_mac, dst_mac)
            .ipv4(src_ip, dst_ip, 64)
            .tcp(src_port, dst_port, 1000, 65535);

        let mut eth_frame = Vec::new();
        builder.write(&mut eth_frame, &[]).unwrap();

        // Set SYN flag: eth(14) + ip(20) + tcp flags at byte 13
        let tcp_start = 14 + 20;
        eth_frame[tcp_start + 13] = 0x02; // SYN

        with_vnet_header(&eth_frame)
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
                service_mac: SVC_MAC,
                listen_ports: vec![80],
                tcp_buffer_size: 4096,
                listen_pool_size: 2,
            },
        );

        let mut table = ServiceTable::new();
        table.create(
            "svc1".into(),
            SVC_IP,
            SVC_MAC,
            ServicePolicy {
                buffer_frames: 64,
                timeout_ms: 30000,
                activator: Some(distvirt_worker_protocol::ActivatorConfig::Tcp {
                    ports: None,
                    tcp_only: false,
                    max_flows: 1024,
                }),
            },
            Some(instance),
            Some(sm),
        );

        // Feed a TCP SYN to the L4 path (after vnet header).
        let syn_frame = make_tcp_frame_for_service(
            SVC_MAC,
            [0x02, 0x00, 0x00, 0x00, 0x00, 0x0a],
            [10, 0, 0, 1],
            SVC_IP.octets(),
            12345,
            80,
        );
        let result = table.lookup_and_buffer(SVC_IP, &syn_frame, |_| true);
        assert!(
            matches!(result, Some((ServiceAction::L4Result { .. }, _))),
            "SYN should trigger L4Result"
        );

        // Set backend and mark ready.
        table.update_backend("svc1", Some((POD_IP, POD_MAC)));
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
                service_mac: SVC_MAC,
                listen_ports: vec![80],
                tcp_buffer_size: 4096,
                listen_pool_size: 2,
            },
        );

        let mut table = ServiceTable::new();
        table.create(
            "svc1".into(),
            SVC_IP,
            SVC_MAC,
            default_policy(),
            None,
            Some(sm),
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
        table.create("svc1".into(), SVC_IP, SVC_MAC, default_policy(), None, None);

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
            SVC_MAC,
            ServicePolicy {
                buffer_frames: 64,
                timeout_ms: 30000,
                activator: Some(distvirt_worker_protocol::ActivatorConfig::Tcp {
                    ports: None,
                    tcp_only: false,
                    max_flows: 1024,
                }),
            },
            Some(instance),
            None,
        );

        // Feed a TCP SYN frame via lookup_and_buffer.
        let syn_frame = make_tcp_frame_for_service(
            SVC_MAC,
            [0x02, 0x00, 0x00, 0x00, 0x00, 0x0a],
            [10, 0, 0, 1],
            SVC_IP.octets(),
            12345,
            80,
        );
        let result = table.lookup_and_buffer(SVC_IP, &syn_frame, |_| true);
        assert!(
            matches!(result, Some((ServiceAction::ActivatorActions { .. }, _))),
            "SYN should trigger activator actions"
        );

        // Set backend and mark ready.
        table.update_backend("svc1", Some((POD_IP, POD_MAC)));
        let ready_result = table.mark_ready("svc1");
        assert!(ready_result.is_some(), "mark_ready should return Some");

        match ready_result.unwrap() {
            MarkReadyResult::Passthrough { backend_mac, service_ip, service_mac, actions, .. } => {
                assert_eq!(backend_mac, POD_MAC);
                assert_eq!(service_ip, SVC_IP);
                assert_eq!(service_mac, SVC_MAC);
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
