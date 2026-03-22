use std::collections::HashMap;
use std::net::Ipv4Addr;

use distvirt_activator::types::Event;
use distvirt_activator::{
    ActivatorInstance, FlowTracker, StreamManager, StreamManagerOutput, is_l4_action,
    parse_frame_to_packet_info,
};
use distvirt_worker_protocol::ServiceId;

use super::EndpointAction;

/// Per-port processing mode.
pub(crate) enum PortMode {
    /// No activator — passthrough.
    Passthrough,
    /// L3 activator for this port.
    L3 { activator: ActivatorInstance },
    /// L4 — uses the shared StreamManager on ServiceProcessor.
    L4,
}

/// What to do with packets to ports not in port_routes.
pub(crate) enum DefaultPortMode {
    /// Drop unrecognized ports (service has activation).
    Drop,
    /// Pass through unrecognized ports (pure passthrough service).
    Passthrough,
}

/// Per-port routing service processor.
///
/// Incoming packets are routed by destination port to the appropriate
/// processing mode. L4 ports share a single StreamManager. L3 ports
/// each have their own activator instance.
pub(crate) struct ServiceProcessor {
    pub port_routes: HashMap<u16, PortMode>,
    pub default_mode: DefaultPortMode,
    pub stream_manager: Option<StreamManager>,
    pub flow_tracker: FlowTracker,
}

impl ServiceProcessor {
    /// Create a pure passthrough processor (no ports configured).
    pub fn passthrough() -> Self {
        ServiceProcessor {
            port_routes: HashMap::new(),
            default_mode: DefaultPortMode::Passthrough,
            stream_manager: None,
            flow_tracker: FlowTracker::new(),
        }
    }

    /// Process a frame through the appropriate per-port pipeline.
    ///
    /// Returns `None` for passthrough (caller falls through to buffering).
    pub fn process_frame(
        &mut self,
        service_id: ServiceId,
        ip_packet: &[u8],
        raw_frame: &[u8],
    ) -> Option<EndpointAction> {
        let dst_port = match extract_dst_port(ip_packet) {
            Some(port) => port,
            None => {
                // Can't determine port — use default mode
                return match self.default_mode {
                    DefaultPortMode::Passthrough => None,
                    DefaultPortMode::Drop => None,
                };
            }
        };

        let mode = match self.port_routes.get_mut(&dst_port) {
            Some(mode) => mode,
            None => {
                return match self.default_mode {
                    DefaultPortMode::Passthrough => None,
                    DefaultPortMode::Drop => None, // silently drop
                };
            }
        };

        match mode {
            PortMode::Passthrough => None,
            PortMode::L4 => {
                let stream_manager = self.stream_manager.as_mut()?;
                let sm_output = stream_manager.receive_frame(ip_packet);
                Some(process_l4_output(
                    service_id,
                    None, // TODO: per-port activator for L4
                    stream_manager,
                    sm_output,
                ))
            }
            PortMode::L3 { activator } => {
                if let Some(packet_info) =
                    parse_frame_to_packet_info(ip_packet, raw_frame, &mut self.flow_tracker)
                {
                    activator.push_event(Event::Packet(packet_info));
                }
                match activator.process_events() {
                    Ok(actions) => Some(EndpointAction::ActivatorActions {
                        actions,
                        service_id: service_id.to_owned(),
                    }),
                    Err(e) => {
                        log::error!("activator error for service '{}': {:#}", service_id, e);
                        None
                    }
                }
            }
        }
    }

    /// Handle mark_ready: push BackendAvailable and process pending events.
    pub fn on_mark_ready(&mut self, service_id: ServiceId) -> Option<EndpointAction> {
        // Push to all L3 activators
        for mode in self.port_routes.values_mut() {
            if let PortMode::L3 { activator } = mode {
                activator.push_event(Event::BackendAvailable(true));
            }
        }

        // Push to stream manager if present
        if let Some(ref mut sm) = self.stream_manager {
            let sm_output = sm.handle_timeout();
            return Some(process_l4_output(service_id, None, sm, sm_output));
        }

        // Collect L3 actions from all ports
        let mut all_actions = Vec::new();
        for mode in self.port_routes.values_mut() {
            if let PortMode::L3 { activator } = mode {
                match activator.process_events() {
                    Ok(actions) => all_actions.extend(actions),
                    Err(e) => {
                        log::error!("activator error for service '{}': {:#}", service_id, e);
                    }
                }
            }
        }

        if all_actions.is_empty() {
            None
        } else {
            Some(EndpointAction::ActivatorActions {
                actions: all_actions,
                service_id: service_id.to_owned(),
            })
        }
    }

    /// Push BackendAvailable event and update the stream manager backend.
    pub fn on_backend_update(&mut self, has_backend: bool, backend_ip: Option<Ipv4Addr>) {
        for mode in self.port_routes.values_mut() {
            if let PortMode::L3 { activator } = mode {
                activator.push_event(Event::BackendAvailable(has_backend));
            }
        }
        if let Some(ref mut sm) = self.stream_manager {
            sm.update_backend(backend_ip);
        }
    }

    /// Handle a smoltcp timeout (L4 only).
    pub fn handle_timeout(&mut self, service_id: ServiceId) -> Option<EndpointAction> {
        if let Some(ref mut sm) = self.stream_manager {
            let sm_output = sm.handle_timeout();
            Some(process_l4_output(service_id, None, sm, sm_output))
        } else {
            None
        }
    }

    /// Whether this is a pure passthrough processor (no activators).
    pub fn is_passthrough(&self) -> bool {
        self.port_routes.is_empty()
            || self.port_routes.values().all(|m| matches!(m, PortMode::Passthrough))
    }

    /// Whether this processor uses a stream manager (has L4 ports).
    pub fn has_stream_manager(&self) -> bool {
        self.stream_manager.is_some()
    }
}

/// Extract TCP/UDP destination port from an IP packet.
fn extract_dst_port(ip_packet: &[u8]) -> Option<u16> {
    if ip_packet.len() < 20 {
        return None;
    }
    let ihl = ((ip_packet[0] & 0x0F) as usize) * 4;
    let protocol = ip_packet[9];
    if ip_packet.len() < ihl + 4 {
        return None;
    }
    match protocol {
        6 | 17 => {
            // TCP or UDP: dst port at offset 2 in transport header
            let dst_port = u16::from_be_bytes([ip_packet[ihl + 2], ip_packet[ihl + 3]]);
            Some(dst_port)
        }
        _ => None,
    }
}

/// Process StreamManagerOutput through the activator event loop (bounded to 4 rounds).
fn process_l4_output(
    service_id: ServiceId,
    activator: Option<&mut ActivatorInstance>,
    stream_manager: &mut StreamManager,
    mut sm_output: StreamManagerOutput,
) -> EndpointAction {
    let mut all_non_l4_actions = Vec::new();

    if let Some(activator) = activator {
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
                    log::error!("activator error for service '{}': {:#}", service_id, e);
                    break;
                }
            };
            let mut new_events = Vec::new();
            for action in &actions {
                if is_l4_action(action) {
                    let out = stream_manager.execute_action(action);
                    new_events.extend(out.events);
                    sm_output.frames.extend(out.frames);
                }
            }
            all_non_l4_actions.extend(actions.into_iter().filter(|a| !is_l4_action(a)));
            sm_output.events = new_events;
        }

        if !sm_output.events.is_empty() || activator.has_pending_events() {
            log::warn!(
                "service '{}': L4 event loop hit 4-round cap with {} pending SM events and {} pending activator events",
                service_id,
                sm_output.events.len(),
                if activator.has_pending_events() {
                    "some"
                } else {
                    "no"
                },
            );
        }
    }

    let poll_delay = stream_manager.poll_delay();
    EndpointAction::L4Result {
        actions: all_non_l4_actions,
        frames: sm_output.frames,
        service_id: service_id.to_owned(),
        poll_delay,
    }
}
