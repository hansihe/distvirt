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

// -----------------------------------------------------------------------
// Lookup
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
    ) -> (EndpointAction, bool, Option<FlowStatusChange>)
    {
        let endpoint = match self.by_ip.get_mut(&dst_ip) {
            Some(ep) => ep,
            None => return (EndpointAction::NotFound, false, None),
        };
        let now = Instant::now();

        // Track TCP flows for endpoints that have a flow tracker.
        let flow_change = Self::track_flow(endpoint, frame);

        match &mut endpoint.backend {
            EndpointBackend::Service {
                service_id,
                backend_ip,
                processor,
                ..
            } => {
                // If ready with a backend, check reachability inline (avoids borrow conflict).
                if endpoint.state == EndpointState::Ready {
                    if let Some(pod_ip) = *backend_ip {
                        // We need to check reachability of pod_ip, which may be a different
                        // entry in by_ip. Extract what we need first, then drop the borrow.
                        let svc_id = service_id.clone();
                        let svc_ip = endpoint.ip;

                        // Check reachability by looking up pod_ip in the table.
                        // If pod_ip == dst_ip, it's a self-reference (not meaningful), skip.
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
                            "service '{}': ready but backend IP {} not reachable, falling through to buffer",
                            svc_id, pod_ip
                        );

                        // Re-borrow endpoint after self access.
                        let endpoint = self.by_ip.get_mut(&dst_ip).unwrap();
                        let EndpointBackend::Service {
                            ref service_id,
                            ref mut processor,
                            ..
                        } = endpoint.backend else {
                            unreachable!();
                        };

                        // L4/L3 activator path: delegate to processor.
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
                        }

                        let svc_id = service_id.clone();
                        let should_activate = self.check_activation_debounce(dst_ip, now);
                        let endpoint = self.by_ip.get_mut(&dst_ip).unwrap();
                        let EndpointBackend::Service { ref policy, .. } = endpoint.backend else {
                            unreachable!();
                        };
                        let buffer_frames = policy.buffer_frames;
                        let timeout_ms = policy.timeout_ms;
                        let action = Self::try_buffer_frame(endpoint, frame, buffer_frames, timeout_ms, now);
                        let service_id = Some(svc_id);
                        return match action {
                            BufferResult::Buffered => (EndpointAction::Buffered { service_id }, should_activate, flow_change),
                            BufferResult::Dropped => (EndpointAction::Drop { service_id }, should_activate, flow_change),
                        };
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
                            return (result, false, None);
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
                    BufferResult::Buffered => (EndpointAction::Buffered { service_id }, should_activate, flow_change),
                    BufferResult::Dropped => (EndpointAction::Drop { service_id }, should_activate, flow_change),
                }
            }

            EndpointBackend::RemoteSegment { worker_id } => {
                (EndpointAction::RemoteWorker { worker_id: worker_id.clone() }, false, None)
            }

            EndpointBackend::UnplacedPod { buffer_policy } => {
                let buffer_frames = buffer_policy.buffer_frames;
                let timeout_ms = buffer_policy.timeout_ms;

                let should_activate = self.check_activation_debounce(dst_ip, now);

                let endpoint = self.by_ip.get_mut(&dst_ip).unwrap();
                let action = Self::try_buffer_frame(endpoint, frame, buffer_frames, timeout_ms, now);
                match action {
                    BufferResult::Buffered => (EndpointAction::Buffered { service_id: None }, should_activate, flow_change),
                    BufferResult::Dropped => (EndpointAction::Drop { service_id: None }, should_activate, flow_change),
                }
            }

            EndpointBackend::LocalAdapter { port_id } => {
                let port_id = *port_id;
                (EndpointAction::LocalAdapter { port_id }, false, flow_change)
            }

            EndpointBackend::LocalPod { port_id } => {
                match *port_id {
                    Some(pid) => {
                        (EndpointAction::LocalPod { port_id: pid }, false, flow_change)
                    }
                    None => {
                        // Pod launching — buffer frames.
                        let should_activate = self.check_activation_debounce(dst_ip, now);
                        let endpoint = self.by_ip.get_mut(&dst_ip).unwrap();
                        let action = Self::try_buffer_frame(endpoint, frame, 64, 30_000, now);
                        match action {
                            BufferResult::Buffered => (EndpointAction::Buffered { service_id: None }, should_activate, flow_change),
                            BufferResult::Dropped => (EndpointAction::Drop { service_id: None }, should_activate, flow_change),
                        }
                    }
                }
            }
        }
    }

    /// Feed a frame's TCP info to the endpoint's flow tracker (if present).
    /// Returns a `FlowStatusChange` if `has_active_flows` transitioned.
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
                has_active_flows: has_active,
            })
        } else {
            None
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
}
