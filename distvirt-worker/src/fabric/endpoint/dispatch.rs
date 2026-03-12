use std::net::Ipv4Addr;
use std::time::{Duration, Instant};

use crate::packet::FabricPacket;
use crate::fabric::flow::FlowKey;
use super::{
    Endpoint, EndpointAction, EndpointBackend, EndpointState, EndpointTable,
    FlowStatusChange, ServiceProcessor,
};

/// Internal result of the buffer acceptance helper.
enum BufferResult {
    Buffered,
    Dropped,
}

/// Dispatch target for complex endpoint types that need table-level access.
enum BackendDispatch {
    Service,
    UnplacedPod,
    LocalPodPending,
}

// -----------------------------------------------------------------------
// Lookup — top-level dispatch
// -----------------------------------------------------------------------

impl EndpointTable {
    /// Check if a destination IP belongs to an endpoint. If so, buffer or forward
    /// the frame and return the action + whether an activation event should fire.
    ///
    /// Returns `(NotFound, false, None)` if `dst_ip` is not an endpoint IP.
    pub fn lookup_and_buffer(
        &mut self,
        dst_ip: Ipv4Addr,
        frame: &[u8],
        skip_flow_tracking: bool,
    ) -> (EndpointAction, bool, Option<FlowStatusChange>)
    {
        let endpoint = match self.by_ip.get_mut(&dst_ip) {
            Some(ep) => ep,
            None => return (EndpointAction::NotFound, false, None),
        };
        let now = Instant::now();

        // Track TCP flows for endpoints that have a flow tracker.
        let flow_change = if skip_flow_tracking {
            None
        } else {
            Self::track_flow(endpoint, frame)
        };

        // Fast path: stateless endpoint types return immediately.
        let dispatch = match &endpoint.backend {
            EndpointBackend::RemoteSegment { worker_id } => {
                return (EndpointAction::RemoteWorker { worker_id: worker_id.clone() }, false, None);
            }
            EndpointBackend::LocalAdapter { port_id } => {
                return (EndpointAction::LocalAdapter { port_id: *port_id }, false, flow_change);
            }
            EndpointBackend::LocalPod { port_id: Some(pid) } => {
                return (EndpointAction::LocalPod { port_id: *pid }, false, flow_change);
            }
            // Complex cases need table-level access (debounce, reachability).
            EndpointBackend::Service { .. } => BackendDispatch::Service,
            EndpointBackend::UnplacedPod { .. } => BackendDispatch::UnplacedPod,
            EndpointBackend::LocalPod { port_id: None } => BackendDispatch::LocalPodPending,
        };

        match dispatch {
            BackendDispatch::Service => self.dispatch_service(dst_ip, frame, flow_change, now),
            BackendDispatch::UnplacedPod => self.dispatch_unplaced_pod(dst_ip, frame, flow_change, now),
            BackendDispatch::LocalPodPending => self.dispatch_local_pod_pending(dst_ip, frame, flow_change, now),
        }
    }

    /// Feed a frame's TCP info to the endpoint's flow tracker (if present).
    /// Returns a `FlowStatusChange` if `has_active_flows` transitioned.
    ///
    /// Endpoint-scoped (takes `&mut Endpoint`) to enable per-entry locking.
    fn track_flow(endpoint: &mut Endpoint, frame: &[u8]) -> Option<FlowStatusChange> {
        let ft = endpoint.flow_tracker.as_mut()?;
        let fp = FabricPacket::new(frame)?;
        if fp.ip_protocol() != crate::packet::IP_PROTO_TCP {
            return None;
        }
        let (src_port, dst_port) = fp.transport_ports()?;
        let tcp_flags = fp.tcp_flags()?;

        let had_active = ft.has_active_flows();
        ft.track_packet(
            FlowKey {
                src_ip: fp.ipv4_src(),
                dst_ip: fp.ipv4_dst(),
                protocol: crate::packet::IP_PROTO_TCP,
                src_port,
                dst_port,
            },
            tcp_flags,
        );
        let has_active = ft.has_active_flows();

        if has_active != had_active {
            Some(FlowStatusChange {
                ip: endpoint.ip,
                service_id: endpoint.backend.service_id(),
                has_active_flows: has_active,
            })
        } else {
            None
        }
    }

    /// Check activation debounce for an endpoint, returning true if activation should fire.
    ///
    /// Takes `&mut Endpoint` instead of `&mut self` so that only the endpoint's
    /// own data is needed — enabling per-entry locking in the future.
    fn check_activation_debounce(endpoint: &mut Endpoint, debounce: Duration, now: Instant) -> bool {
        match endpoint.last_activation {
            Some(last) if now.duration_since(last) < debounce => false,
            _ => {
                endpoint.last_activation = Some(now);
                true
            }
        }
    }

    /// Try to accept a frame into the endpoint's buffer, applying capacity and
    /// timeout limits. Returns whether the frame was buffered or dropped.
    ///
    /// Endpoint-scoped (takes `&mut Endpoint`) to enable per-entry locking.
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
}

// -----------------------------------------------------------------------
// Per-type dispatch handlers
// -----------------------------------------------------------------------

impl EndpointTable {
    /// Dispatch a frame to a Service endpoint.
    ///
    /// Handles reachability checks (DNAT forward path), L3/L4 activator
    /// processing, and buffering when the backend isn't ready.
    ///
    /// The service endpoint borrow is dropped before checking pod reachability,
    /// so each lookup touches only one entry at a time — preparing for per-entry locking.
    fn dispatch_service(
        &mut self,
        dst_ip: Ipv4Addr,
        frame: &[u8],
        flow_change: Option<FlowStatusChange>,
        now: Instant,
    ) -> (EndpointAction, bool, Option<FlowStatusChange>) {
        // Phase 1: Read service endpoint state (single endpoint).
        let (state, backend_ip, svc_ip) = {
            let endpoint = self.by_ip.get(&dst_ip).unwrap();
            let EndpointBackend::Service {
                ref backend_ip,
                ..
            } = endpoint.backend else {
                unreachable!();
            };
            (endpoint.state, *backend_ip, endpoint.ip)
        };
        // Borrow on service endpoint is now dropped.

        // Phase 2: Check reachability of backend (separate endpoint lookup).
        // Structured as independent lookups to prepare for per-entry locking.
        if state == EndpointState::Ready {
            if let Some(pod_ip) = backend_ip {
                let reachable = if pod_ip != dst_ip {
                    self.by_ip.get(&pod_ip).map_or(false, |ep| match &ep.backend {
                        EndpointBackend::LocalPod { port_id } => port_id.is_some(),
                        EndpointBackend::RemoteSegment { .. } => true,
                        EndpointBackend::LocalAdapter { .. } => true,
                        _ => false,
                    })
                } else {
                    false
                };

                if reachable {
                    return (EndpointAction::ServiceForward { pod_ip, service_ip: svc_ip }, false, flow_change);
                }

                log::debug!(
                    "service endpoint {}: ready but backend IP {} not reachable, falling through to buffer",
                    dst_ip, pod_ip
                );

                return self.dispatch_service_buffer(dst_ip, frame, flow_change, now);
            }
        }

        // Not ready or no backend — try processor, then buffer.
        self.dispatch_service_buffer(dst_ip, frame, flow_change, now)
    }

    /// Service endpoint: try processor then buffer the frame.
    ///
    /// Used when the service isn't ready, has no backend, or the backend
    /// isn't reachable.
    fn dispatch_service_buffer(
        &mut self,
        dst_ip: Ipv4Addr,
        frame: &[u8],
        flow_change: Option<FlowStatusChange>,
        now: Instant,
    ) -> (EndpointAction, bool, Option<FlowStatusChange>) {
        let debounce = self.activation_debounce;
        let endpoint = self.by_ip.get_mut(&dst_ip).unwrap();
        let EndpointBackend::Service {
            ref service_id,
            ref mut processor,
            ref policy,
            ..
        } = endpoint.backend else {
            unreachable!();
        };

        // Try L4/L3 activator path.
        if !matches!(processor, ServiceProcessor::Passthrough) {
            if let Some(fp) = FabricPacket::new(frame) {
                if let Some(result) = processor.process_frame(
                    service_id,
                    fp.ip_packet(),
                    frame,
                ) {
                    return (result, false, None);
                }
            }
            // process_frame returned None on L3 error — fall through to buffering.
        }

        let svc_id = service_id.clone();
        let buffer_frames = policy.buffer_frames;
        let timeout_ms = policy.timeout_ms;
        let should_activate = Self::check_activation_debounce(endpoint, debounce, now);
        let action = Self::try_buffer_frame(endpoint, frame, buffer_frames, timeout_ms, now);
        let service_id = Some(svc_id);
        match action {
            BufferResult::Buffered => (EndpointAction::Buffered { service_id }, should_activate, flow_change),
            BufferResult::Dropped => (EndpointAction::Drop { service_id }, should_activate, flow_change),
        }
    }

    /// Dispatch a frame to an UnplacedPod endpoint (debounce + buffer).
    fn dispatch_unplaced_pod(
        &mut self,
        dst_ip: Ipv4Addr,
        frame: &[u8],
        flow_change: Option<FlowStatusChange>,
        now: Instant,
    ) -> (EndpointAction, bool, Option<FlowStatusChange>) {
        let debounce = self.activation_debounce;
        let endpoint = self.by_ip.get_mut(&dst_ip).unwrap();
        let EndpointBackend::UnplacedPod { buffer_policy } = &endpoint.backend else {
            unreachable!();
        };
        let buffer_frames = buffer_policy.buffer_frames;
        let timeout_ms = buffer_policy.timeout_ms;
        let should_activate = Self::check_activation_debounce(endpoint, debounce, now);
        let action = Self::try_buffer_frame(endpoint, frame, buffer_frames, timeout_ms, now);
        match action {
            BufferResult::Buffered => (EndpointAction::Buffered { service_id: None }, should_activate, flow_change),
            BufferResult::Dropped => (EndpointAction::Drop { service_id: None }, should_activate, flow_change),
        }
    }

    /// Dispatch a frame to a LocalPod endpoint that has no port yet (debounce + buffer).
    fn dispatch_local_pod_pending(
        &mut self,
        dst_ip: Ipv4Addr,
        frame: &[u8],
        flow_change: Option<FlowStatusChange>,
        now: Instant,
    ) -> (EndpointAction, bool, Option<FlowStatusChange>) {
        let debounce = self.activation_debounce;
        let endpoint = self.by_ip.get_mut(&dst_ip).unwrap();
        let should_activate = Self::check_activation_debounce(endpoint, debounce, now);
        let action = Self::try_buffer_frame(endpoint, frame, 64, 30_000, now);
        match action {
            BufferResult::Buffered => (EndpointAction::Buffered { service_id: None }, should_activate, flow_change),
            BufferResult::Dropped => (EndpointAction::Drop { service_id: None }, should_activate, flow_change),
        }
    }
}
