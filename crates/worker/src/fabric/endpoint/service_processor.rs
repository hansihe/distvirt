use std::net::Ipv4Addr;

use distvirt_activator::types::Event;
use distvirt_activator::{
    ActivatorInstance, FlowTracker, StreamManager, StreamManagerOutput, is_l4_action,
    parse_frame_to_packet_info,
};
use distvirt_worker_protocol::ServiceId;

use super::EndpointAction;

/// Processing mode for a service entity.
pub(crate) enum ServiceProcessor {
    /// No activator — pure buffering/forwarding (passthrough).
    Passthrough,
    /// L3 activator: parse frames, push events, get actions back.
    L3 {
        activator: ActivatorInstance,
        flow_tracker: FlowTracker,
    },
    /// L4 stream manager: full TCP-level proxying with optional activator event loop.
    L4 {
        activator: Option<ActivatorInstance>,
        stream_manager: StreamManager,
    },
}

impl ServiceProcessor {
    /// Process a frame through the L4 or L3 activator pipeline.
    ///
    /// Returns `None` for `Passthrough` (caller falls through to buffering).
    pub fn process_frame(
        &mut self,
        service_id: ServiceId,
        ip_packet: &[u8],
        raw_frame: &[u8],
    ) -> Option<EndpointAction> {
        match self {
            ServiceProcessor::Passthrough => None,
            ServiceProcessor::L4 {
                activator,
                stream_manager,
            } => {
                let sm_output = stream_manager.receive_frame(ip_packet);
                Some(process_l4_output(
                    service_id,
                    activator.as_mut(),
                    stream_manager,
                    sm_output,
                ))
            }
            ServiceProcessor::L3 {
                activator,
                flow_tracker,
            } => {
                if let Some(packet_info) =
                    parse_frame_to_packet_info(ip_packet, raw_frame, flow_tracker)
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
                        // Fall through to passthrough buffering on error.
                        None
                    }
                }
            }
        }
    }

    /// Handle mark_ready: push BackendAvailable and process pending events.
    ///
    /// Returns `None` for `Passthrough`.
    pub fn on_mark_ready(&mut self, service_id: ServiceId) -> Option<EndpointAction> {
        match self {
            ServiceProcessor::Passthrough => None,
            ServiceProcessor::L4 {
                activator,
                stream_manager,
            } => {
                if let Some(act) = activator {
                    act.push_event(Event::BackendAvailable(true));
                }
                let sm_output = stream_manager.handle_timeout();
                Some(process_l4_output(
                    service_id,
                    activator.as_mut(),
                    stream_manager,
                    sm_output,
                ))
            }
            ServiceProcessor::L3 { activator, .. } => {
                activator.push_event(Event::BackendAvailable(true));
                let actions = match activator.process_events() {
                    Ok(a) => a,
                    Err(e) => {
                        log::error!("activator error for service '{}': {:#}", service_id, e);
                        Vec::new()
                    }
                };
                Some(EndpointAction::ActivatorActions {
                    actions,
                    service_id: service_id.to_owned(),
                })
            }
        }
    }

    /// Push BackendAvailable event and update the stream manager backend.
    pub fn on_backend_update(&mut self, has_backend: bool, backend_ip: Option<Ipv4Addr>) {
        match self {
            ServiceProcessor::Passthrough => {}
            ServiceProcessor::L4 {
                activator,
                stream_manager,
            } => {
                if let Some(act) = activator {
                    act.push_event(Event::BackendAvailable(has_backend));
                }
                stream_manager.update_backend(backend_ip);
            }
            ServiceProcessor::L3 { activator, .. } => {
                activator.push_event(Event::BackendAvailable(has_backend));
            }
        }
    }

    /// Handle a smoltcp timeout (L4 only).
    pub fn handle_timeout(&mut self, service_id: ServiceId) -> Option<EndpointAction> {
        match self {
            ServiceProcessor::L4 {
                activator,
                stream_manager,
            } => {
                let sm_output = stream_manager.handle_timeout();
                Some(process_l4_output(
                    service_id,
                    activator.as_mut(),
                    stream_manager,
                    sm_output,
                ))
            }
            _ => None,
        }
    }

    /// Whether this processor uses a stream manager (L4 mode).
    pub fn has_stream_manager(&self) -> bool {
        matches!(self, ServiceProcessor::L4 { .. })
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

        // Warn if the event loop didn't fully converge within 4 rounds.
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
