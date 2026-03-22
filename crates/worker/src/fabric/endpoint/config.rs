use std::collections::{HashSet, VecDeque};
use std::net::Ipv4Addr;

use distvirt_worker_protocol::{
    BufferPolicy, EndpointKind, EndpointSpec, ServiceId, ServicePolicy, WorkerId,
};

use super::ServiceProcessor;
use super::{Endpoint, EndpointBackend, EndpointState, EndpointSyncEffect, EndpointTable};
use crate::fabric::flow::FlowTracker;
use crate::fabric::port::PortId;

// -----------------------------------------------------------------------
// Unified endpoint sync/update
// -----------------------------------------------------------------------

impl EndpointTable {
    /// Full replacement of all endpoints from EndpointSpec list.
    /// Each worker derives its local view from `my_worker_id`.
    ///
    /// `adapter_port_id` is the port ID of the local adapter channel port
    /// (if any). Used for WireGuardPeer endpoints placed on this worker.
    pub fn apply_endpoint_sync(
        &mut self,
        specs: Vec<EndpointSpec>,
        my_worker_id: WorkerId,
        make_processor: &mut dyn FnMut(ServiceId, &ServicePolicy, Ipv4Addr) -> ServiceProcessor,
        adapter_port_id: Option<PortId>,
    ) -> Vec<EndpointSyncEffect> {
        let mut effects = Vec::new();
        let new_ips: HashSet<Ipv4Addr> = specs.iter().map(|s| s.ip).collect();

        // Remove endpoints whose IP is not in the new set.
        let to_remove: Vec<Ipv4Addr> = self
            .by_ip
            .keys()
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
        }

        // Upsert each spec.
        for spec in specs {
            effects.extend(self.apply_single_spec(
                spec,
                my_worker_id,
                make_processor,
                adapter_port_id,
            ));
        }

        effects
    }

    /// Incremental update: remove some IPs, upsert some specs.
    pub fn apply_endpoint_update(
        &mut self,
        upserted: Vec<EndpointSpec>,
        removed_ips: Vec<Ipv4Addr>,
        my_worker_id: WorkerId,
        make_processor: &mut dyn FnMut(ServiceId, &ServicePolicy, Ipv4Addr) -> ServiceProcessor,
        adapter_port_id: Option<PortId>,
    ) -> Vec<EndpointSyncEffect> {
        let mut effects = Vec::new();

        for ip in removed_ips {
            if let Some(endpoint) = self.by_ip.get(&ip) {
                if let EndpointBackend::Service { ref service_id, .. } = endpoint.backend {
                    self.service_id_to_ip.remove(service_id);
                }
            }
            self.by_ip.remove(&ip);
        }

        for spec in upserted {
            effects.extend(self.apply_single_spec(
                spec,
                my_worker_id,
                make_processor,
                adapter_port_id,
            ));
        }

        effects
    }

    /// Derive and upsert a single EndpointSpec.
    fn apply_single_spec(
        &mut self,
        spec: EndpointSpec,
        my_worker_id: WorkerId,
        make_processor: &mut dyn FnMut(ServiceId, &ServicePolicy, Ipv4Addr) -> ServiceProcessor,
        adapter_port_id: Option<PortId>,
    ) -> Vec<EndpointSyncEffect> {
        let mut effects = Vec::new();
        let ip = spec.ip;

        match spec.kind {
            EndpointKind::Pod { placement } => {
                match placement {
                    Some(ref p) if p.worker_id == my_worker_id => {
                        // Local pod — create/update LocalPod endpoint, preserving port_id if already attached.
                        let existing_port_id = self.by_ip.get(&ip).and_then(|ep| {
                            if let EndpointBackend::LocalPod { port_id } = &ep.backend {
                                *port_id
                            } else {
                                None
                            }
                        });
                        // Preserve buffer from prior UnplacedPod if transitioning.
                        let old_buffer: VecDeque<Vec<u8>> = self
                            .by_ip
                            .get_mut(&ip)
                            .filter(|ep| {
                                matches!(ep.backend, EndpointBackend::UnplacedPod { .. })
                                    && !ep.buffer.is_empty()
                            })
                            .map(|ep| ep.buffer.drain(..).collect())
                            .unwrap_or_default();
                        // Clean up old service mapping if needed.
                        if let Some(old) = self.by_ip.get(&ip) {
                            if let EndpointBackend::Service { ref service_id, .. } = old.backend {
                                self.service_id_to_ip.remove(service_id);
                            }
                        }
                        let (state, flow_tracker) = if existing_port_id.is_some() {
                            (EndpointState::Ready, Some(FlowTracker::new()))
                        } else {
                            (EndpointState::Pending, None)
                        };
                        log::info!(
                            "endpoint: {} -> LocalPod (state={:?}, port_id={:?}, buffered_frames={})",
                            ip, state, existing_port_id, old_buffer.len()
                        );
                        self.by_ip.insert(
                            ip,
                            Endpoint {
                                ip,
                                state,
                                buffer: old_buffer,
                                buffer_start: None,
                                backend: EndpointBackend::LocalPod {
                                    port_id: existing_port_id,
                                },
                                flow_tracker,
                                last_activation: None,
                            },
                        );
                    }
                    Some(ref p) => {
                        // Remote pod.
                        log::info!(
                            "endpoint: {} -> RemoteSegment (worker_id={})",
                            ip, p.worker_id
                        );
                        let was_buffering = self
                            .by_ip
                            .get(&ip)
                            .map(|ep| ep.state == EndpointState::Buffering && !ep.buffer.is_empty())
                            .unwrap_or(false);
                        // RemoteSegment endpoints start Ready with no buffer.
                        // This is intentional: they are dumb forwarders that relay
                        // frames to the remote worker; flow-control and buffer
                        // tracking only happens on the host worker that owns the pod.
                        self.by_ip.insert(
                            ip,
                            Endpoint {
                                ip,
                                state: EndpointState::Ready,
                                buffer: VecDeque::new(),
                                buffer_start: None,
                                backend: EndpointBackend::RemoteSegment {
                                    worker_id: p.worker_id,
                                },
                                flow_tracker: None,
                                last_activation: None,
                            },
                        );
                        if was_buffering {
                            effects.push(EndpointSyncEffect::FlushPodBuffer { ip });
                        }
                    }
                    None => {
                        // Unplaced pod — buffer.
                        log::info!("endpoint: {} -> UnplacedPod (buffering)", ip);
                        if !self.by_ip.contains_key(&ip) {
                            self.by_ip.insert(
                                ip,
                                Endpoint {
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
                                    flow_tracker: None,
                                    last_activation: None,
                                },
                            );
                        }
                        // If already exists as UnplacedPod, keep buffer intact.
                    }
                }
            }
            EndpointKind::WireGuardPeer { placement } => {
                match placement {
                    Some(ref p) if p.worker_id == my_worker_id => {
                        // Local adapter — create LocalAdapter endpoint.
                        log::info!("endpoint: {} -> LocalAdapter (WireGuardPeer, local)", ip);
                        let port_id = match adapter_port_id {
                            Some(id) => id,
                            None => {
                                log::warn!(
                                    "WireGuardPeer endpoint {} placed locally but no adapter port available",
                                    ip
                                );
                                return effects;
                            }
                        };
                        // Drain buffer from old endpoint if it was buffering.
                        let old_frames: Vec<Vec<u8>> = self
                            .by_ip
                            .get_mut(&ip)
                            .filter(|ep| !ep.buffer.is_empty())
                            .map(|ep| ep.buffer.drain(..).collect())
                            .unwrap_or_default();
                        // Clean up old endpoint.
                        if let Some(old) = self.by_ip.get(&ip) {
                            if let EndpointBackend::Service { ref service_id, .. } = old.backend {
                                self.service_id_to_ip.remove(service_id);
                            }
                        }
                        self.by_ip.insert(
                            ip,
                            Endpoint {
                                ip,
                                state: EndpointState::Ready,
                                buffer: VecDeque::new(),
                                buffer_start: None,
                                backend: EndpointBackend::LocalAdapter { port_id },
                                flow_tracker: None,
                                last_activation: None,
                            },
                        );
                        if !old_frames.is_empty() {
                            effects.push(EndpointSyncEffect::FlushAdapterBuffer {
                                ip,
                                port_id,
                                frames: old_frames,
                            });
                        }
                    }
                    Some(ref p) => {
                        // Remote peer — same as remote pod.
                        log::info!(
                            "endpoint: {} -> RemoteSegment (WireGuardPeer, worker_id={})",
                            ip, p.worker_id
                        );
                        let was_buffering = self
                            .by_ip
                            .get(&ip)
                            .map(|ep| ep.state == EndpointState::Buffering && !ep.buffer.is_empty())
                            .unwrap_or(false);
                        if let Some(old) = self.by_ip.get(&ip) {
                            if let EndpointBackend::Service { ref service_id, .. } = old.backend {
                                self.service_id_to_ip.remove(service_id);
                            }
                        }
                        self.by_ip.insert(
                            ip,
                            Endpoint {
                                ip,
                                state: EndpointState::Ready,
                                buffer: VecDeque::new(),
                                buffer_start: None,
                                backend: EndpointBackend::RemoteSegment {
                                    worker_id: p.worker_id,
                                },
                                flow_tracker: None,
                                last_activation: None,
                            },
                        );
                        if was_buffering {
                            effects.push(EndpointSyncEffect::FlushPodBuffer { ip });
                        }
                    }
                    None => {
                        // Unplaced peer — buffer.
                        log::info!("endpoint: {} -> UnplacedPod (WireGuardPeer, buffering)", ip);
                        if !self.by_ip.contains_key(&ip) {
                            self.by_ip.insert(
                                ip,
                                Endpoint {
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
                                    flow_tracker: None,
                                    last_activation: None,
                                },
                            );
                        }
                    }
                }
            }
            EndpointKind::Service {
                service_id,
                policy,
                backend,
            } => {
                // Determine new state and backend_ip from the backend field.
                let (new_state, new_backend_ip) = match &backend {
                    None => (EndpointState::Buffering, None),
                    Some(be) if !be.ready => (EndpointState::Pending, Some(be.pod_ip)),
                    Some(be) => (EndpointState::Ready, Some(be.pod_ip)),
                };

                // Check if service already exists and can keep its processor.
                let existing = self.by_ip.get(&ip);
                let can_reuse_processor = existing
                    .map(|ep| {
                        if let EndpointBackend::Service {
                            service_id: ref existing_id,
                            policy: ref existing_policy,
                            ..
                        } = ep.backend
                        {
                            *existing_id == service_id
                                && existing_policy.activator == policy.activator
                        } else {
                            false
                        }
                    })
                    .unwrap_or(false);

                log::info!(
                    "endpoint: {} -> Service (service_id={}, state={:?}, backend_ip={:?}, reuse={})",
                    ip, service_id, new_state, new_backend_ip, can_reuse_processor
                );

                if can_reuse_processor {
                    // Update existing service endpoint in place.
                    let endpoint = self.by_ip.get_mut(&ip).unwrap();
                    let old_state = endpoint.state;
                    let EndpointBackend::Service {
                        ref mut backend_ip,
                        ref mut processor,
                        policy: ref mut existing_policy,
                        ..
                    } = endpoint.backend
                    else {
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
                        endpoint.last_activation = None;
                        // If the flow tracker had active flows, notify the
                        // orchestrator so it can clear the demand contribution.
                        if let Some(ref ft) = endpoint.flow_tracker {
                            if ft.has_active_flows() {
                                effects.push(EndpointSyncEffect::FlowStatusChange {
                                    ip,
                                    service_id: Some(service_id),
                                    active: false,
                                });
                            }
                        }
                        endpoint.flow_tracker = None;
                    }

                    processor.on_backend_update(new_backend_ip.is_some(), new_backend_ip);

                    // Check if transitioning to Ready.
                    if new_state == EndpointState::Ready && old_state != EndpointState::Ready {
                        effects.push(EndpointSyncEffect::ServiceReady {
                            service_id: service_id,
                        });
                    }
                } else {
                    // Create new service endpoint.
                    let processor = make_processor(service_id, &policy, ip);

                    // Passthrough services get a FlowTracker immediately.
                    // This is safe because active() only counts
                    // Established/HalfClosed flows, not Opening (SYN-only).
                    let flow_tracker = if matches!(processor, ServiceProcessor::Passthrough) {
                        Some(FlowTracker::new())
                    } else {
                        None
                    };

                    // Remove old service_id mapping if different service was at this IP.
                    if let Some(old_ep) = self.by_ip.get(&ip) {
                        if let EndpointBackend::Service {
                            service_id: ref old_id,
                            ..
                        } = old_ep.backend
                        {
                            if *old_id != service_id {
                                self.service_id_to_ip.remove(old_id);
                            }
                        }
                    }

                    self.by_ip.insert(
                        ip,
                        Endpoint {
                            ip,
                            state: new_state,
                            buffer: VecDeque::new(),
                            buffer_start: None,
                            backend: EndpointBackend::Service {
                                service_id: service_id,
                                policy,
                                backend_ip: new_backend_ip,
                                processor,
                            },
                            flow_tracker,
                            last_activation: None,
                        },
                    );
                    self.service_id_to_ip.insert(service_id, ip);

                    if new_state == EndpointState::Ready {
                        effects.push(EndpointSyncEffect::ServiceReady {
                            service_id: service_id,
                        });
                    }
                }
            }
        }

        effects
    }
}
