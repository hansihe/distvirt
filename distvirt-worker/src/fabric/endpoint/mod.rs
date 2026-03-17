mod config;
mod dispatch;
pub(crate) mod service_processor;

use std::collections::{HashMap, VecDeque};
use std::net::Ipv4Addr;
use std::time::{Duration, Instant};

use distvirt_activator::types::Action;
use distvirt_worker_protocol::{ServiceId, ServicePolicy, WorkerId};
use tonic::service;

use super::flow::FlowTracker;
use super::port::PortId;
pub(crate) use service_processor::ServiceProcessor;

/// Side-effects from applying an endpoint sync/update.
#[derive(Debug)]
pub enum EndpointSyncEffect {
    /// Service became ready — caller should flush buffered frames.
    ServiceReady { service_id: ServiceId },
    /// Pod buffer should be flushed (pod became locally reachable).
    FlushPodBuffer { ip: Ipv4Addr },
    /// Adapter buffer should be flushed to the adapter port.
    FlushAdapterBuffer {
        ip: Ipv4Addr,
        port_id: PortId,
        frames: Vec<Vec<u8>>,
    },
    /// Flow status changed due to endpoint reconfiguration (e.g. flow tracker cleared).
    FlowStatusChange {
        ip: Ipv4Addr,
        service_id: Option<ServiceId>,
        active: bool,
    },
}

/// What the fabric should do with a frame that matched an endpoint IP.
#[derive(Debug)]
pub enum EndpointAction {
    /// Forward to the ready backend pod (service DNAT path).
    ServiceForward {
        pod_ip: Ipv4Addr,
        service_ip: Ipv4Addr,
    },
    /// Frame was accepted into the endpoint buffer.
    Buffered { service_id: Option<ServiceId> },
    /// Frame was dropped (buffer full or timed out).
    Drop { service_id: Option<ServiceId> },
    /// Activator processed the frame and returned actions for the fabric to execute.
    ActivatorActions {
        actions: Vec<Action>,
        service_id: ServiceId,
    },
    /// L4 stream manager processed the frame and produced outgoing frames + non-L4 actions.
    L4Result {
        actions: Vec<Action>,
        frames: Vec<Vec<u8>>,
        service_id: ServiceId,
        poll_delay: Option<Duration>,
    },
    /// Forward to a remote worker via tunnel port.
    RemoteWorker { worker_id: WorkerId },
    /// Forward to a local adapter (WireGuard, splice) via its channel port.
    LocalAdapter { port_id: PortId },
    /// Deliver to a local pod port.
    LocalPod { port_id: PortId },
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
        service_id: ServiceId,
        policy: ServicePolicy,
        backend_ip: Option<Ipv4Addr>,
        processor: ServiceProcessor,
    },
    /// Pod whose placement is not yet known (buffering frames).
    UnplacedPod {
        buffer_policy: distvirt_worker_protocol::BufferPolicy,
    },
    /// Pod located on a remote worker segment.
    RemoteSegment { worker_id: WorkerId },
    /// WireGuard peer or splice target connected locally via channel port.
    LocalAdapter { port_id: PortId },
    /// Pod placed on this worker. `port_id` is None while launching, Some once TAP attached.
    LocalPod { port_id: Option<PortId> },
}

impl EndpointBackend {
    fn service_id(&self) -> Option<ServiceId> {
        match self {
            EndpointBackend::Service { service_id, .. } => Some(*service_id),
            _ => None,
        }
    }
}

/// A single endpoint entry in the table.
struct Endpoint {
    ip: Ipv4Addr,
    state: EndpointState,
    buffer: VecDeque<Vec<u8>>,
    buffer_start: Option<Instant>,
    backend: EndpointBackend,
    /// TCP flow tracker for passthrough services and pod endpoints.
    /// `None` for activator-based services (they have their own demand signals).
    flow_tracker: Option<FlowTracker>,
    /// Per-endpoint activation debounce timestamp.
    /// Stored on the endpoint so that activation checks only need
    /// the endpoint's own data (preparing for per-entry locking).
    last_activation: Option<Instant>,
}

/// Optional flow status transition returned alongside an `EndpointAction`.
#[derive(Debug, Clone)]
pub struct FlowStatusChange {
    pub ip: Ipv4Addr,
    pub service_id: Option<ServiceId>,
    pub active: bool,
}

/// Table of endpoints indexed by IP for fast frame-path lookup.
///
/// Structured so that most operations only access a single endpoint entry.
/// This prepares for a future migration to per-entry locking (e.g. DashMap),
/// where the hot path (lookup_and_buffer) can proceed without a global lock.
pub struct EndpointTable {
    by_ip: HashMap<Ipv4Addr, Endpoint>,
    service_id_to_ip: HashMap<ServiceId, Ipv4Addr>,
    activation_debounce: Duration,
}

impl EndpointTable {
    pub fn new() -> Self {
        EndpointTable {
            by_ip: HashMap::new(),
            service_id_to_ip: HashMap::new(),
            activation_debounce: Duration::from_secs(1),
        }
    }

    /// Mark a service as ready. Returns buffered frames / activator actions
    /// (L3 passthrough mode) or an L4Result (L4 stream mode).
    pub fn mark_service_ready(&mut self, service_id: ServiceId) -> Option<MarkReadyResult> {
        let ip = match self.service_id_to_ip.get(&service_id) {
            Some(ip) => *ip,
            None => return None,
        };
        let endpoint = self.by_ip.get_mut(&ip)?;

        let EndpointBackend::Service {
            service_id: ref svc_id,
            ref mut backend_ip,
            ref mut processor,
            ..
        } = endpoint.backend
        else {
            return None;
        };

        let Some(backend_ip_val) = *backend_ip else {
            log::warn!(
                "service '{}': mark_ready called but no backend set",
                service_id
            );
            return None;
        };
        endpoint.state = EndpointState::Ready;

        log::debug!(
            "service '{}': mark_ready: buffer_len={}, has_stream_manager={}",
            svc_id,
            endpoint.buffer.len(),
            processor.has_stream_manager()
        );

        // L4/L3 activator path: delegate to processor.
        if let Some(svc_action) = processor.on_mark_ready(*svc_id) {
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
                svc_id,
                frames.len(),
                actions.len()
            );

            let service_ip = endpoint.ip;
            return Some(MarkReadyResult::Passthrough {
                frames,
                backend_ip: backend_ip_val,
                service_ip,
                actions,
            });
        }

        // Passthrough: drain buffer and enable flow tracking.
        let frames: Vec<Vec<u8>> = endpoint.buffer.drain(..).collect();
        endpoint.buffer_start = None;
        if endpoint.flow_tracker.is_none() {
            endpoint.flow_tracker = Some(FlowTracker::new());
        }

        log::debug!(
            "service '{}': mark_ready produced {} frames, 0 actions",
            svc_id,
            frames.len()
        );

        let service_ip = endpoint.ip;
        Some(MarkReadyResult::Passthrough {
            frames,
            backend_ip: backend_ip_val,
            service_ip,
            actions: Vec::new(),
        })
    }

    /// Drain buffered frames from an UnplacedPod or LocalPod endpoint.
    pub fn flush_pod_buffer(&mut self, ip: Ipv4Addr) -> Vec<Vec<u8>> {
        if let Some(endpoint) = self.by_ip.get_mut(&ip) {
            if matches!(
                endpoint.backend,
                EndpointBackend::UnplacedPod { .. } | EndpointBackend::LocalPod { .. }
            ) {
                endpoint.buffer_start = None;
                return endpoint.buffer.drain(..).collect();
            }
        }
        Vec::new()
    }

    // -----------------------------------------------------------------------
    // Service-specific helpers
    // -----------------------------------------------------------------------

    /// Look up NAT-relevant info for a service by its ID.
    /// Returns `(service_ip, backend_ip)`.
    pub fn get_service_nat_info(&self, service_id: ServiceId) -> Option<(Ipv4Addr, Ipv4Addr)> {
        let ip = self.service_id_to_ip.get(&service_id)?;
        let endpoint = self.by_ip.get(ip)?;
        let EndpointBackend::Service { backend_ip, .. } = &endpoint.backend else {
            return None;
        };
        Some((endpoint.ip, (*backend_ip)?))
    }

    /// Look up the service IP for a given service ID.
    pub fn get_service_ip(&self, service_id: ServiceId) -> Option<Ipv4Addr> {
        self.service_id_to_ip.get(&service_id).copied()
    }

    /// Handle a smoltcp timeout for a service IP.
    ///
    /// Calls `handle_timeout()` on the StreamManager, runs the activator loop,
    /// and returns the resulting `EndpointAction` (if the service has an L4 path).
    pub fn handle_timeout_for_ip(&mut self, ip: Ipv4Addr) -> Option<EndpointAction> {
        let endpoint = self.by_ip.get_mut(&ip)?;
        let EndpointBackend::Service {
            service_id,
            ref mut processor,
            ..
        } = endpoint.backend
        else {
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
            let EndpointBackend::Service {
                ref service_id,
                ref backend_ip,
                ..
            } = endpoint.backend
            else {
                continue;
            };
            if endpoint.state == EndpointState::Ready
                && backend_ip.as_ref() == Some(target_ip)
                && !endpoint.buffer.is_empty()
            {
                log::info!(
                    "service '{}': flush_by_backend_ip draining {} frames for IP {}",
                    service_id,
                    endpoint.buffer.len(),
                    target_ip
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

    /// Attach a port to a LocalPod endpoint. Returns buffered frames to flush.
    pub fn attach_port(&mut self, ip: Ipv4Addr, port_id: PortId) -> Result<Vec<Vec<u8>>, String> {
        let endpoint = self
            .by_ip
            .get_mut(&ip)
            .ok_or_else(|| format!("attach_port: no endpoint for IP {}", ip))?;
        match &mut endpoint.backend {
            EndpointBackend::LocalPod { port_id: pid } => {
                *pid = Some(port_id);
                endpoint.state = EndpointState::Ready;
                if endpoint.flow_tracker.is_none() {
                    endpoint.flow_tracker = Some(FlowTracker::new());
                }
                let frames: Vec<Vec<u8>> = endpoint.buffer.drain(..).collect();
                endpoint.buffer_start = None;
                Ok(frames)
            }
            _ => Err(format!("attach_port: endpoint for {} is not LocalPod", ip)),
        }
    }

    /// Detach a port from its endpoint (port removed/dropped).
    /// Scans all endpoints for matching port_id and resets to None.
    pub fn detach_port(&mut self, port_id: PortId) {
        for endpoint in self.by_ip.values_mut() {
            if let EndpointBackend::LocalPod {
                port_id: ref mut pid,
            } = endpoint.backend
            {
                if *pid == Some(port_id) {
                    *pid = None;
                    endpoint.state = EndpointState::Pending;
                    return;
                }
            }
        }
    }

    /// Get the port_id for a LocalPod or LocalAdapter endpoint.
    pub fn get_port_id(&self, ip: &Ipv4Addr) -> Option<PortId> {
        let endpoint = self.by_ip.get(ip)?;
        match &endpoint.backend {
            EndpointBackend::LocalPod { port_id } => *port_id,
            EndpointBackend::LocalAdapter { port_id } => Some(*port_id),
            _ => None,
        }
    }

    /// Check if a backend IP is reachable (has an attached local port).
    #[allow(dead_code)]
    pub fn is_backend_reachable(&self, ip: &Ipv4Addr) -> bool {
        match self.by_ip.get(ip) {
            Some(ep) => match &ep.backend {
                EndpointBackend::LocalPod { port_id } => port_id.is_some(),
                EndpointBackend::RemoteSegment { .. } => true,
                EndpointBackend::LocalAdapter { .. } => true,
                _ => false,
            },
            None => false,
        }
    }

    /// Run GC on all endpoint flow trackers.
    ///
    /// Returns flow status changes for any endpoints whose `active`
    /// transitioned due to expired flows.
    pub fn gc_flow_trackers(&mut self) -> Vec<FlowStatusChange> {
        let now = Instant::now();
        let mut changes = Vec::new();
        for endpoint in self.by_ip.values_mut() {
            if let Some(ref mut ft) = endpoint.flow_tracker {
                let had_active = ft.has_active_flows();
                ft.gc(now);
                let has_active = ft.has_active_flows();
                if has_active != had_active {
                    changes.push(FlowStatusChange {
                        ip: endpoint.ip,
                        service_id: endpoint.backend.service_id(),
                        active: has_active,
                    });
                }
            }
        }
        changes
    }
}

#[cfg(test)]
mod tests;
