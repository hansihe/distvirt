use std::collections::{HashMap, HashSet, VecDeque};
use std::net::Ipv4Addr;
use std::time::{Duration, Instant};

use distvirt_activator::types::Action;
use distvirt_worker_protocol::{BufferPolicy, EndpointKind, EndpointSpec, ServicePolicy};

use crate::packet::FabricPacket;
use super::service_activator::ServiceProcessor;

/// Side-effects from applying an endpoint sync/update.
#[derive(Debug)]
pub enum EndpointSyncEffect {
    /// Service became ready — caller should flush buffered frames.
    ServiceReady { service_id: String },
    /// Pod buffer should be flushed (pod became locally reachable).
    FlushPodBuffer { ip: Ipv4Addr },
}

/// What the fabric should do with a frame that matched an endpoint IP.
#[derive(Debug)]
pub enum EndpointAction {
    /// Forward to the ready backend pod (service DNAT path).
    ServiceForward { pod_ip: Ipv4Addr, service_ip: Ipv4Addr },
    /// Frame was accepted into the endpoint buffer.
    Buffered { service_id: Option<String> },
    /// Frame was dropped (buffer full or timed out).
    Drop { service_id: Option<String> },
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
    /// Forward to a remote worker via tunnel port.
    RemoteWorker { worker_id: String },
    /// No endpoint matches this IP.
    NotFound,
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
    /// L4 stream mode: outgoing frames + non-L4 actions via EndpointAction::L4Result.
    L4(EndpointAction),
}


/// Data returned by `flush_by_backend_ip` for each service whose buffer was drained.
pub struct ServiceFlushData {
    pub service_ip: Ipv4Addr,
    pub backend_ip: Ipv4Addr,
    pub frames: Vec<Vec<u8>>,
}

/// Lifecycle state of an endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EndpointState {
    /// No backend assigned; frames are buffered.
    Buffering,
    /// Backend assigned but not yet marked ready.
    Pending,
    /// Backend assigned and marked ready; frames are forwarded.
    Ready,
}

/// The backend variant for an endpoint.
enum EndpointBackend {
    /// Service VIP backed by a pod.
    Service {
        service_id: String,
        policy: ServicePolicy,
        backend_ip: Option<Ipv4Addr>,
        processor: ServiceProcessor,
    },
    /// Pod whose placement is not yet known (buffering frames).
    UnplacedPod {
        buffer_policy: BufferPolicy,
    },
    /// Pod located on a remote worker segment.
    RemoteSegment {
        worker_id: String,
    },
}

/// A single endpoint entry in the table.
struct Endpoint {
    ip: Ipv4Addr,
    state: EndpointState,
    buffer: VecDeque<Vec<u8>>,
    buffer_start: Option<Instant>,
    backend: EndpointBackend,
}

/// Table of endpoints indexed by IP for fast frame-path lookup.
pub struct EndpointTable {
    by_ip: HashMap<Ipv4Addr, Endpoint>,
    service_id_to_ip: HashMap<String, Ipv4Addr>,
    last_activation: HashMap<Ipv4Addr, Instant>,
    activation_debounce: Duration,
}

impl EndpointTable {
    pub fn new() -> Self {
        EndpointTable {
            by_ip: HashMap::new(),
            service_id_to_ip: HashMap::new(),
            last_activation: HashMap::new(),
            activation_debounce: Duration::from_secs(1),
        }
    }

    /// Mark a service as ready. Returns buffered frames / activator actions
    /// (L3 passthrough mode) or an L4Result (L4 stream mode).
    pub fn mark_service_ready(&mut self, service_id: &str) -> Option<MarkReadyResult> {
        let ip = match self.service_id_to_ip.get(service_id) {
            Some(ip) => *ip,
            None => return None,
        };
        let endpoint = self.by_ip.get_mut(&ip)?;

        let EndpointBackend::Service {
            service_id: ref svc_id,
            ref mut backend_ip,
            ref mut processor,
            ..
        } = endpoint.backend else {
            return None;
        };

        let Some(backend_ip_val) = *backend_ip else {
            log::warn!("service '{}': mark_ready called but no backend set", service_id);
            return None;
        };
        endpoint.state = EndpointState::Ready;

        log::debug!(
            "service '{}': mark_ready: buffer_len={}, has_stream_manager={}",
            svc_id, endpoint.buffer.len(),
            processor.has_stream_manager()
        );

        // L4/L3 activator path: delegate to processor.
        if let Some(svc_action) = processor.on_mark_ready(svc_id) {
            if processor.has_stream_manager() {
                return Some(MarkReadyResult::L4(svc_action));
            }
            // L3 activator: drain buffer and return Passthrough with actions.
            let frames: Vec<Vec<u8>> = endpoint.buffer.drain(..).collect();
            endpoint.buffer_start = None;
            let actions = match svc_action {
                EndpointAction::ActivatorActions { actions, .. } => actions,
                _ => Vec::new(),
            };

            log::debug!(
                "service '{}': mark_ready produced {} frames, {} actions",
                svc_id, frames.len(), actions.len()
            );

            let service_ip = endpoint.ip;
            return Some(MarkReadyResult::Passthrough { frames, backend_ip: backend_ip_val, service_ip, actions });
        }

        // Passthrough: drain buffer.
        let frames: Vec<Vec<u8>> = endpoint.buffer.drain(..).collect();
        endpoint.buffer_start = None;

        log::debug!(
            "service '{}': mark_ready produced {} frames, 0 actions",
            svc_id, frames.len()
        );

        let service_ip = endpoint.ip;
        Some(MarkReadyResult::Passthrough { frames, backend_ip: backend_ip_val, service_ip, actions: Vec::new() })
    }

    // -----------------------------------------------------------------------
    // Unified endpoint sync/update
    // -----------------------------------------------------------------------

    /// Full replacement of all endpoints from EndpointSpec list.
    /// Each worker derives its local view from `my_worker_id`.
    pub fn apply_endpoint_sync(
        &mut self,
        specs: Vec<EndpointSpec>,
        my_worker_id: &str,
        make_processor: &mut dyn FnMut(&str, &ServicePolicy, Ipv4Addr) -> ServiceProcessor,
    ) -> Vec<EndpointSyncEffect> {
        let mut effects = Vec::new();
        let new_ips: HashSet<Ipv4Addr> = specs.iter().map(|s| s.ip).collect();

        // Remove endpoints whose IP is not in the new set.
        let to_remove: Vec<Ipv4Addr> = self.by_ip.keys()
            .filter(|ip| !new_ips.contains(ip))
            .copied()
            .collect();
        for ip in to_remove {
            // Clean up service_id_to_ip mapping if this is a service
            if let Some(endpoint) = self.by_ip.get(&ip) {
                if let EndpointBackend::Service { ref service_id, .. } = endpoint.backend {
                    self.service_id_to_ip.remove(service_id);
                }
            }
            self.by_ip.remove(&ip);
            self.last_activation.remove(&ip);
        }

        // Upsert each spec.
        for spec in specs {
            effects.extend(self.apply_single_spec(spec, my_worker_id, make_processor));
        }

        effects
    }

    /// Incremental update: remove some IPs, upsert some specs.
    pub fn apply_endpoint_update(
        &mut self,
        upserted: Vec<EndpointSpec>,
        removed_ips: Vec<Ipv4Addr>,
        my_worker_id: &str,
        make_processor: &mut dyn FnMut(&str, &ServicePolicy, Ipv4Addr) -> ServiceProcessor,
    ) -> Vec<EndpointSyncEffect> {
        let mut effects = Vec::new();

        for ip in removed_ips {
            if let Some(endpoint) = self.by_ip.get(&ip) {
                if let EndpointBackend::Service { ref service_id, .. } = endpoint.backend {
                    self.service_id_to_ip.remove(service_id);
                }
            }
            self.by_ip.remove(&ip);
            self.last_activation.remove(&ip);
        }

        for spec in upserted {
            effects.extend(self.apply_single_spec(spec, my_worker_id, make_processor));
        }

        effects
    }

    /// Derive and upsert a single EndpointSpec.
    fn apply_single_spec(
        &mut self,
        spec: EndpointSpec,
        my_worker_id: &str,
        make_processor: &mut dyn FnMut(&str, &ServicePolicy, Ipv4Addr) -> ServiceProcessor,
    ) -> Vec<EndpointSyncEffect> {
        let mut effects = Vec::new();
        let ip = spec.ip;

        match spec.kind {
            EndpointKind::Pod { placement } => {
                match placement {
                    Some(ref p) if p.worker_id.as_ref() == my_worker_id => {
                        // Local pod — skip, handled by TAP/IpPortTable.
                        // Remove any stale endpoint entry if it exists.
                        if let Some(endpoint) = self.by_ip.remove(&ip) {
                            if let EndpointBackend::Service { ref service_id, .. } = endpoint.backend {
                                self.service_id_to_ip.remove(service_id);
                            }
                        }
                        self.last_activation.remove(&ip);
                    }
                    Some(ref p) => {
                        // Remote pod.
                        let was_buffering = self.by_ip.get(&ip)
                            .map(|ep| ep.state == EndpointState::Buffering && !ep.buffer.is_empty())
                            .unwrap_or(false);
                        self.by_ip.insert(ip, Endpoint {
                            ip,
                            state: EndpointState::Ready,
                            buffer: VecDeque::new(),
                            buffer_start: None,
                            backend: EndpointBackend::RemoteSegment {
                                worker_id: p.worker_id.0.clone(),
                            },
                        });
                        if was_buffering {
                            effects.push(EndpointSyncEffect::FlushPodBuffer { ip });
                        }
                    }
                    None => {
                        // Unplaced pod — buffer.
                        if !self.by_ip.contains_key(&ip) {
                            self.by_ip.insert(ip, Endpoint {
                                ip,
                                state: EndpointState::Buffering,
                                buffer: VecDeque::new(),
                                buffer_start: None,
                                backend: EndpointBackend::UnplacedPod {
                                    buffer_policy: BufferPolicy {
                                        buffer_frames: 64,
                                        timeout_ms: 30_000,
                                    },
                                },
                            });
                        }
                        // If already exists as UnplacedPod, keep buffer intact.
                    }
                }
            }
            EndpointKind::Service { service_id, policy, backend } => {
                let svc_id_str = service_id.0.clone();

                // Determine new state and backend_ip from the backend field.
                let (new_state, new_backend_ip) = match &backend {
                    None => (EndpointState::Buffering, None),
                    Some(be) if !be.ready => (EndpointState::Pending, Some(be.pod_ip)),
                    Some(be) => (EndpointState::Ready, Some(be.pod_ip)),
                };

                // Check if service already exists and can keep its processor.
                let existing = self.by_ip.get(&ip);
                let can_reuse_processor = existing.map(|ep| {
                    if let EndpointBackend::Service { service_id: ref existing_id, policy: ref existing_policy, .. } = ep.backend {
                        existing_id == &svc_id_str && existing_policy.activator == policy.activator
                    } else {
                        false
                    }
                }).unwrap_or(false);

                if can_reuse_processor {
                    // Update existing service endpoint in place.
                    let endpoint = self.by_ip.get_mut(&ip).unwrap();
                    let old_state = endpoint.state;
                    let EndpointBackend::Service {
                        ref mut backend_ip,
                        ref mut processor,
                        policy: ref mut existing_policy,
                        ..
                    } = endpoint.backend else {
                        unreachable!();
                    };

                    let old_backend_ip = *backend_ip;
                    *backend_ip = new_backend_ip;
                    *existing_policy = policy;
                    endpoint.state = new_state;

                    // Buffer preservation logic: clear when backend is removed or IP changes.
                    let should_clear = match (old_backend_ip, new_backend_ip) {
                        (_, None) => true,
                        (Some(old), Some(new)) if old != new => true,
                        _ => false,
                    };
                    if should_clear {
                        endpoint.buffer.clear();
                        endpoint.buffer_start = None;
                    }
                    if new_backend_ip.is_none() {
                        self.last_activation.remove(&ip);
                    }

                    processor.on_backend_update(
                        new_backend_ip.is_some(),
                        new_backend_ip,
                    );

                    // Check if transitioning to Ready.
                    if new_state == EndpointState::Ready && old_state != EndpointState::Ready {
                        effects.push(EndpointSyncEffect::ServiceReady { service_id: svc_id_str.clone() });
                    }
                } else {
                    // Create new service endpoint.
                    let processor = make_processor(&svc_id_str, &policy, ip);

                    // Remove old service_id mapping if different service was at this IP.
                    if let Some(old_ep) = self.by_ip.get(&ip) {
                        if let EndpointBackend::Service { service_id: ref old_id, .. } = old_ep.backend {
                            if old_id != &svc_id_str {
                                self.service_id_to_ip.remove(old_id);
                            }
                        }
                    }

                    self.by_ip.insert(ip, Endpoint {
                        ip,
                        state: new_state,
                        buffer: VecDeque::new(),
                        buffer_start: None,
                        backend: EndpointBackend::Service {
                            service_id: svc_id_str.clone(),
                            policy,
                            backend_ip: new_backend_ip,
                            processor,
                        },
                    });
                    self.service_id_to_ip.insert(svc_id_str.clone(), ip);

                    if new_state == EndpointState::Ready {
                        effects.push(EndpointSyncEffect::ServiceReady { service_id: svc_id_str });
                    }
                }
            }
        }

        effects
    }

    /// Drain buffered frames from an UnplacedPod endpoint.
    pub fn flush_pod_buffer(&mut self, ip: Ipv4Addr) -> Vec<Vec<u8>> {
        if let Some(endpoint) = self.by_ip.get_mut(&ip) {
            if matches!(endpoint.backend, EndpointBackend::UnplacedPod { .. }) {
                endpoint.buffer_start = None;
                return endpoint.buffer.drain(..).collect();
            }
        }
        Vec::new()
    }

    // -----------------------------------------------------------------------
    // Lookup
    // -----------------------------------------------------------------------

    /// Check if a destination IP belongs to an endpoint. If so, buffer or forward
    /// the frame and return the action + whether an activation event should fire.
    ///
    /// Returns `(NotFound, false)` if `dst_ip` is not an endpoint IP.
    ///
    /// `is_reachable` checks whether the backend IP is reachable (i.e. has a port
    /// in the `ip_port_table`).
    pub fn lookup_and_buffer<F>(&mut self, dst_ip: Ipv4Addr, frame: &[u8], is_reachable: F) -> (EndpointAction, bool)
    where
        F: Fn(&Ipv4Addr) -> bool,
    {
        let endpoint = match self.by_ip.get_mut(&dst_ip) {
            Some(ep) => ep,
            None => return (EndpointAction::NotFound, false),
        };
        let now = Instant::now();

        match &mut endpoint.backend {
            EndpointBackend::Service {
                service_id,
                backend_ip,
                processor,
                ..
            } => {
                // If ready with a backend and the backend IP is reachable, forward directly.
                if endpoint.state == EndpointState::Ready {
                    if let Some(pod_ip) = *backend_ip {
                        if is_reachable(&pod_ip) {
                            let service_ip = endpoint.ip;
                            return (EndpointAction::ServiceForward { pod_ip, service_ip }, false);
                        } else {
                            log::debug!(
                                "service '{}': ready but backend IP {} not reachable, falling through to buffer",
                                service_id, pod_ip
                            );
                        }
                    }
                }

                // L4/L3 activator path: delegate to processor.
                if !matches!(processor, ServiceProcessor::Passthrough) {
                    if let Some(fp) = FabricPacket::new(frame) {
                        if let Some(result) = processor.process_frame(
                            service_id,
                            fp.ip_packet(),
                            frame,
                        ) {
                            return (result, false);
                        }
                    }
                    // process_frame returned None on L3 error — fall through to buffering.
                }

                let svc_id = service_id.clone();

                // Not ready or no backend — check if we should activate (with debounce).
                let should_activate = self.check_activation_debounce(dst_ip, now);

                // Re-borrow endpoint after last_activation manipulation.
                let endpoint = self.by_ip.get_mut(&dst_ip).unwrap();
                let EndpointBackend::Service { ref policy, .. } = endpoint.backend else {
                    unreachable!();
                };

                let buffer_frames = policy.buffer_frames;
                let timeout_ms = policy.timeout_ms;

                let action = Self::try_buffer_frame(
                    endpoint,
                    frame,
                    buffer_frames,
                    timeout_ms,
                    now,
                );
                let service_id = Some(svc_id);
                match action {
                    BufferResult::Buffered => (EndpointAction::Buffered { service_id }, should_activate),
                    BufferResult::Dropped => (EndpointAction::Drop { service_id }, should_activate),
                }
            }

            EndpointBackend::RemoteSegment { worker_id } => {
                (EndpointAction::RemoteWorker { worker_id: worker_id.clone() }, false)
            }

            EndpointBackend::UnplacedPod { buffer_policy } => {
                let buffer_frames = buffer_policy.buffer_frames;
                let timeout_ms = buffer_policy.timeout_ms;

                let should_activate = self.check_activation_debounce(dst_ip, now);

                let endpoint = self.by_ip.get_mut(&dst_ip).unwrap();
                let action = Self::try_buffer_frame(endpoint, frame, buffer_frames, timeout_ms, now);
                match action {
                    BufferResult::Buffered => (EndpointAction::Buffered { service_id: None }, should_activate),
                    BufferResult::Dropped => (EndpointAction::Drop { service_id: None }, should_activate),
                }
            }
        }
    }

    /// Check activation debounce for an IP, returning true if activation should fire.
    fn check_activation_debounce(&mut self, ip: Ipv4Addr, now: Instant) -> bool {
        match self.last_activation.get(&ip) {
            Some(last) if now.duration_since(*last) < self.activation_debounce => false,
            _ => {
                self.last_activation.insert(ip, now);
                true
            }
        }
    }

    /// Try to accept a frame into the endpoint's buffer, applying capacity and
    /// timeout limits. Returns whether the frame was buffered or dropped.
    fn try_buffer_frame(
        endpoint: &mut Endpoint,
        frame: &[u8],
        buffer_frames: u32,
        timeout_ms: u32,
        now: Instant,
    ) -> BufferResult {
        if buffer_frames == 0 {
            return BufferResult::Dropped;
        }

        // Check timeout.
        if let Some(start) = endpoint.buffer_start {
            let timeout = Duration::from_millis(timeout_ms as u64);
            if now.duration_since(start) >= timeout {
                endpoint.buffer.clear();
                endpoint.buffer_start = None;
                return BufferResult::Dropped;
            }
        }

        // Check buffer capacity.
        if endpoint.buffer.len() >= buffer_frames as usize {
            return BufferResult::Dropped;
        }

        // Accept into buffer.
        if endpoint.buffer_start.is_none() {
            endpoint.buffer_start = Some(now);
        }
        endpoint.buffer.push_back(frame.to_vec());
        BufferResult::Buffered
    }

    // -----------------------------------------------------------------------
    // Service-specific helpers
    // -----------------------------------------------------------------------

    /// Look up NAT-relevant info for a service by its ID.
    /// Returns `(service_ip, backend_ip)`.
    pub fn get_service_nat_info(&self, service_id: &str) -> Option<(Ipv4Addr, Ipv4Addr)> {
        let ip = self.service_id_to_ip.get(service_id)?;
        let endpoint = self.by_ip.get(ip)?;
        let EndpointBackend::Service { backend_ip, .. } = &endpoint.backend else {
            return None;
        };
        Some((endpoint.ip, (*backend_ip)?))
    }

    /// Look up the service IP for a given service ID.
    pub fn get_service_ip(&self, service_id: &str) -> Option<Ipv4Addr> {
        self.service_id_to_ip.get(service_id).copied()
    }

    /// Handle a smoltcp timeout for a service IP.
    ///
    /// Calls `handle_timeout()` on the StreamManager, runs the activator loop,
    /// and returns the resulting `EndpointAction` (if the service has an L4 path).
    pub fn handle_timeout_for_ip(&mut self, ip: Ipv4Addr) -> Option<EndpointAction> {
        let endpoint = self.by_ip.get_mut(&ip)?;
        let EndpointBackend::Service { ref service_id, ref mut processor, .. } = endpoint.backend else {
            return None;
        };
        processor.handle_timeout(service_id)
    }

    /// Drain buffered frames from all ready endpoints whose backend IP matches `ip`.
    ///
    /// Used when a new port is added: the port's IP becomes reachable, so any
    /// endpoint buffers waiting for that IP can be flushed immediately.
    pub fn flush_by_backend_ip(&mut self, target_ip: &Ipv4Addr) -> Vec<ServiceFlushData> {
        let mut result = Vec::new();
        for (ip, endpoint) in self.by_ip.iter_mut() {
            let EndpointBackend::Service { ref service_id, ref backend_ip, .. } = endpoint.backend else {
                continue;
            };
            if endpoint.state == EndpointState::Ready && backend_ip.as_ref() == Some(target_ip) && !endpoint.buffer.is_empty() {
                log::info!(
                    "service '{}': flush_by_backend_ip draining {} frames for IP {}",
                    service_id, endpoint.buffer.len(), target_ip
                );
                let frames: Vec<Vec<u8>> = endpoint.buffer.drain(..).collect();
                endpoint.buffer_start = None;
                result.push(ServiceFlushData {
                    service_ip: *ip,
                    backend_ip: *target_ip,
                    frames,
                });
            }
        }
        if result.is_empty() {
            log::debug!(
                "flush_by_backend_ip: no ready endpoints with buffer for IP {}",
                target_ip
            );
        }
        result
    }
}

/// Internal result of the buffer acceptance helper.
enum BufferResult {
    Buffered,
    Dropped,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packet::with_fabric_header;
    use distvirt_worker_protocol::{EndpointKind, EndpointPodBackend, EndpointSpec, ServiceId};

    const SVC_IP: Ipv4Addr = Ipv4Addr::new(172, 16, 0, 2);
    const POD_IP: Ipv4Addr = Ipv4Addr::new(172, 16, 0, 130);
    const FRAME: &[u8] = &[0xDE, 0xAD, 0xBE, 0xEF];
    const OWN_WORKER: &str = "test-worker";

    fn default_policy() -> ServicePolicy {
        ServicePolicy {
            buffer_frames: 64,
            timeout_ms: 30000,
            activator: None,
        }
    }

    /// Default make_processor that returns Passthrough for all services.
    fn passthrough_processor(_: &str, _: &ServicePolicy, _: Ipv4Addr) -> ServiceProcessor {
        ServiceProcessor::Passthrough
    }

    /// Create a service endpoint with no backend (Buffering state) via apply_endpoint_sync.
    fn sync_create_service(
        table: &mut EndpointTable,
        service_id: &str,
        ip: Ipv4Addr,
        policy: ServicePolicy,
        make_processor: &mut dyn FnMut(&str, &ServicePolicy, Ipv4Addr) -> ServiceProcessor,
    ) -> Vec<EndpointSyncEffect> {
        table.apply_endpoint_sync(
            vec![EndpointSpec {
                ip,
                kind: EndpointKind::Service {
                    service_id: ServiceId::from(service_id),
                    policy,
                    backend: None,
                },
            }],
            OWN_WORKER,
            make_processor,
        )
    }

    /// Update a service's backend via apply_endpoint_update (sets Pending or Buffering).
    fn sync_update_backend(
        table: &mut EndpointTable,
        service_id: &str,
        ip: Ipv4Addr,
        policy: ServicePolicy,
        backend_ip: Option<Ipv4Addr>,
        make_processor: &mut dyn FnMut(&str, &ServicePolicy, Ipv4Addr) -> ServiceProcessor,
    ) -> Vec<EndpointSyncEffect> {
        let backend = backend_ip.map(|pod_ip| EndpointPodBackend {
            pod_ip,
            placement: None,
            ready: false,
        });
        table.apply_endpoint_update(
            vec![EndpointSpec {
                ip,
                kind: EndpointKind::Service {
                    service_id: ServiceId::from(service_id),
                    policy,
                    backend,
                },
            }],
            vec![],
            OWN_WORKER,
            make_processor,
        )
    }

    /// Remove a service by IP via apply_endpoint_update.
    fn sync_remove_service(
        table: &mut EndpointTable,
        ip: Ipv4Addr,
        make_processor: &mut dyn FnMut(&str, &ServicePolicy, Ipv4Addr) -> ServiceProcessor,
    ) -> Vec<EndpointSyncEffect> {
        table.apply_endpoint_update(vec![], vec![ip], OWN_WORKER, make_processor)
    }

    #[test]
    fn unknown_ip_returns_not_found() {
        let mut table = EndpointTable::new();
        let (action, _) = table.lookup_and_buffer(SVC_IP, FRAME, |_| true);
        assert!(matches!(action, EndpointAction::NotFound));
    }

    #[test]
    fn buffers_when_not_ready() {
        let mut table = EndpointTable::new();
        sync_create_service(&mut table, "svc1", SVC_IP, default_policy(), &mut passthrough_processor);

        let (action, activate) = table.lookup_and_buffer(SVC_IP, FRAME, |_ip| true);
        assert!(matches!(action, EndpointAction::Buffered { service_id: Some(_) }));
        assert!(activate);
    }

    #[test]
    fn forwards_when_ready() {
        let mut table = EndpointTable::new();
        sync_create_service(&mut table, "svc1", SVC_IP, default_policy(), &mut passthrough_processor);
        sync_update_backend(&mut table, "svc1", SVC_IP, default_policy(), Some(POD_IP), &mut passthrough_processor);
        table.mark_service_ready("svc1");

        let (action, activate) = table.lookup_and_buffer(SVC_IP, FRAME, |_ip| true);
        assert!(matches!(
            action,
            EndpointAction::ServiceForward { pod_ip, .. }
            if pod_ip == POD_IP
        ));
        assert!(!activate);
    }

    #[test]
    fn mark_ready_returns_buffered_frames() {
        let mut table = EndpointTable::new();
        sync_create_service(&mut table, "svc1", SVC_IP, default_policy(), &mut passthrough_processor);

        // Buffer some frames (no backend yet).
        for _ in 0..3 {
            table.lookup_and_buffer(SVC_IP, FRAME, |_ip| true);
        }

        // Set backend (Pending) — preserves buffer since None→Some.
        sync_update_backend(&mut table, "svc1", SVC_IP, default_policy(), Some(POD_IP), &mut passthrough_processor);

        let result = table.mark_service_ready("svc1");
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
        let mut table = EndpointTable::new();
        sync_create_service(&mut table, "svc1", SVC_IP, default_policy(), &mut passthrough_processor);
        sync_update_backend(&mut table, "svc1", SVC_IP, default_policy(), Some(POD_IP), &mut passthrough_processor);
        table.mark_service_ready("svc1");

        // Service is ready — now remove backend (Buffering state).
        sync_update_backend(&mut table, "svc1", SVC_IP, default_policy(), None, &mut passthrough_processor);

        let (action, _) = table.lookup_and_buffer(SVC_IP, FRAME, |_ip| true);
        assert!(matches!(action, EndpointAction::Buffered { .. }));
    }

    #[test]
    fn destroy_removes_service() {
        let mut table = EndpointTable::new();
        sync_create_service(&mut table, "svc1", SVC_IP, default_policy(), &mut passthrough_processor);
        sync_remove_service(&mut table, SVC_IP, &mut passthrough_processor);
        let (action, _) = table.lookup_and_buffer(SVC_IP, FRAME, |_| true);
        assert!(matches!(action, EndpointAction::NotFound));
    }

    #[test]
    fn activation_debounced() {
        let mut table = EndpointTable::new();
        sync_create_service(&mut table, "svc1", SVC_IP, default_policy(), &mut passthrough_processor);

        let (_, activate1) = table.lookup_and_buffer(SVC_IP, FRAME, |_| true);
        assert!(activate1);

        let (_, activate2) = table.lookup_and_buffer(SVC_IP, FRAME, |_| true);
        assert!(!activate2);
    }

    #[test]
    fn buffer_capacity_drops_excess() {
        let policy = ServicePolicy {
            buffer_frames: 2,
            timeout_ms: 30000,
            activator: None,
        };
        let mut table = EndpointTable::new();
        sync_create_service(&mut table, "svc1", SVC_IP, policy, &mut passthrough_processor);

        table.lookup_and_buffer(SVC_IP, FRAME, |_ip| true);
        table.lookup_and_buffer(SVC_IP, FRAME, |_ip| true);
        let (action, _) = table.lookup_and_buffer(SVC_IP, FRAME, |_ip| true);
        assert!(matches!(action, EndpointAction::Drop { .. }));
    }

    /// Regression test for Bug 1: setting a backend preserves buffered frames.
    #[test]
    fn update_backend_preserves_buffered_frames() {
        let mut table = EndpointTable::new();
        sync_create_service(&mut table, "svc1", SVC_IP, default_policy(), &mut passthrough_processor);

        // Buffer 3 frames while there is no backend yet.
        for _ in 0..3 {
            let (action, _) = table.lookup_and_buffer(SVC_IP, FRAME, |_ip| true);
            assert!(matches!(action, EndpointAction::Buffered { .. }));
        }

        // Set the backend — this should NOT clear the buffer.
        sync_update_backend(&mut table, "svc1", SVC_IP, default_policy(), Some(POD_IP), &mut passthrough_processor);

        // Mark ready — should return the 3 buffered frames.
        let result = table.mark_service_ready("svc1");
        match result.unwrap() {
            MarkReadyResult::Passthrough { frames, .. } => {
                assert_eq!(
                    frames.len(),
                    3,
                    "setting backend should not clear frames buffered before backend was set"
                );
            }
            _ => panic!("expected Passthrough result"),
        }
    }

    // --- Activator / L4 tests ---

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

    fn l4_tcp_policy() -> ServicePolicy {
        ServicePolicy {
            buffer_frames: 64,
            timeout_ms: 30000,
            activator: Some(distvirt_worker_protocol::ActivatorConfig::Tcp {
                ports: None,
                tcp_only: false,
                max_flows: 1024,
            }),
        }
    }

    #[test]
    #[ignore = "requires WASM activators — run with --include-ignored"]
    fn l4_mark_ready_processes_backend_available() {
        let (_runtime, instance) = try_load_tcp_activator()
            .expect("TCP activator WASM not built — run activators/build.sh");

        let sm = distvirt_activator::StreamManager::new(
            distvirt_activator::StreamManagerConfig {
                service_ip: SVC_IP,
                listen_ports: vec![80],
                tcp_buffer_size: 4096,
                listen_pool_size: 2,
            },
        );

        let mut table = EndpointTable::new();
        let mut make_l4 = {
            let mut instance_opt = Some(instance);
            let mut sm_opt = Some(sm);
            move |_: &str, _: &ServicePolicy, _: Ipv4Addr| -> ServiceProcessor {
                ServiceProcessor::L4 {
                    activator: Some(instance_opt.take().unwrap()),
                    stream_manager: sm_opt.take().unwrap(),
                }
            }
        };
        sync_create_service(&mut table, "svc1", SVC_IP, l4_tcp_policy(), &mut make_l4);

        // Feed a TCP SYN to the L4 path (after vnet header).
        let syn_frame = make_tcp_frame_for_service(
            [10, 0, 0, 1],
            SVC_IP.octets(),
            12345,
            80,
        );
        let (action, _) = table.lookup_and_buffer(SVC_IP, &syn_frame, |_ip| true);
        assert!(
            matches!(action, EndpointAction::L4Result { .. }),
            "SYN should trigger L4Result"
        );

        // Set backend and mark ready.
        sync_update_backend(&mut table, "svc1", SVC_IP, l4_tcp_policy(), Some(POD_IP), &mut passthrough_processor);
        let ready_result = table.mark_service_ready("svc1");
        assert!(ready_result.is_some(), "mark_service_ready should return Some");

        match ready_result.unwrap() {
            MarkReadyResult::L4(EndpointAction::L4Result { .. }) => {
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

        let mut table = EndpointTable::new();
        let mut make_l4 = {
            let mut sm_opt = Some(sm);
            move |_: &str, _: &ServicePolicy, _: Ipv4Addr| -> ServiceProcessor {
                ServiceProcessor::L4 {
                    activator: None,
                    stream_manager: sm_opt.take().unwrap(),
                }
            }
        };
        sync_create_service(&mut table, "svc1", SVC_IP, default_policy(), &mut make_l4);

        // handle_timeout_for_ip on a service with a StreamManager should return Some(L4Result).
        let result = table.handle_timeout_for_ip(SVC_IP);
        assert!(result.is_some(), "handle_timeout_for_ip should return Some for L4 service");
        assert!(
            matches!(result.unwrap(), EndpointAction::L4Result { .. }),
            "should return L4Result"
        );
    }

    #[test]
    fn handle_timeout_for_ip_returns_none_for_l3() {
        let mut table = EndpointTable::new();
        sync_create_service(&mut table, "svc1", SVC_IP, default_policy(), &mut passthrough_processor);

        // L3 service (no StreamManager) should return None.
        let result = table.handle_timeout_for_ip(SVC_IP);
        assert!(result.is_none(), "handle_timeout_for_ip should return None for L3 service");
    }

    #[test]
    #[ignore = "requires WASM activators — run with --include-ignored"]
    fn activator_mark_ready_returns_replay_actions() {
        let (_runtime, instance) = try_load_tcp_activator()
            .expect("TCP activator WASM not built — run activators/build.sh");

        let mut table = EndpointTable::new();
        let mut make_l3 = {
            let mut instance_opt = Some(instance);
            move |_: &str, _: &ServicePolicy, _: Ipv4Addr| -> ServiceProcessor {
                ServiceProcessor::L3 {
                    activator: instance_opt.take().unwrap(),
                    flow_tracker: distvirt_activator::FlowTracker::new(),
                }
            }
        };
        sync_create_service(&mut table, "svc1", SVC_IP, l4_tcp_policy(), &mut make_l3);

        // Feed a TCP SYN frame via lookup_and_buffer.
        let syn_frame = make_tcp_frame_for_service(
            [10, 0, 0, 1],
            SVC_IP.octets(),
            12345,
            80,
        );
        let (action, _) = table.lookup_and_buffer(SVC_IP, &syn_frame, |_ip| true);
        assert!(
            matches!(action, EndpointAction::ActivatorActions { .. }),
            "SYN should trigger activator actions"
        );

        // Set backend and mark ready.
        sync_update_backend(&mut table, "svc1", SVC_IP, l4_tcp_policy(), Some(POD_IP), &mut passthrough_processor);
        let ready_result = table.mark_service_ready("svc1");
        assert!(ready_result.is_some(), "mark_service_ready should return Some");

        match ready_result.unwrap() {
            MarkReadyResult::Passthrough { service_ip, actions, .. } => {
                assert_eq!(service_ip, SVC_IP);
                let replay_count = actions
                    .iter()
                    .filter(|a| matches!(a, Action::ReplayPacket(_)))
                    .count();
                assert!(replay_count > 0, "mark_service_ready should return ReplayPacket actions for buffered SYN");
            }
            _ => panic!("expected Passthrough result"),
        }
    }
}
