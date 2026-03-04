mod forwarding;
pub(crate) mod gateway;
pub(crate) mod nat;
pub(crate) mod port;
pub(crate) mod route;
pub(crate) mod service;
pub(crate) mod service_activator;
pub(crate) mod switch;

pub use port::{ChannelPort, FabricPort, FramePort, Port, PortId};
pub use route::RouteTable;
pub use service::ServiceTable;
pub(crate) use forwarding::FabricContextInner;
pub(crate) use service::{MarkReadyResult, ServiceAction};
pub(crate) use service_activator::ServiceProcessor;
pub(crate) use gateway::{DnsRegistry, FabricGateway};

use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex, OnceLock};
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::mpsc;
use crate::tap::TapDevice;
use crate::task_handle::TaskHandle;
use distvirt_activator::types::{Action, BackendNeed as ActivatorBackendNeed, LogAction, LogLevel};
use crate::packet::{FabricPacket, ip_packet_src, ip_packet_protocol, ip_packet_transport_ports, rewrite_ipv4_dst};
use forwarding::{FabricContext, port_read_loop, gateway_ingress_task};
use switch::IpPortTable;

/// Convert activator BackendNeed to protocol BackendNeed.
pub(crate) fn convert_backend_need(need: &ActivatorBackendNeed) -> distvirt_worker_protocol::BackendNeed {
    match need {
        ActivatorBackendNeed::None => distvirt_worker_protocol::BackendNeed::None,
        ActivatorBackendNeed::Traffic => distvirt_worker_protocol::BackendNeed::Traffic,
        ActivatorBackendNeed::Active => distvirt_worker_protocol::BackendNeed::Active,
    }
}

/// Log an activator log action with appropriate level.
pub(crate) fn handle_log_action(service_id: &str, log_action: &LogAction) {
    let msg = format!("activator[{}]: {}", service_id, log_action.message);
    match log_action.level {
        LogLevel::Trace => log::trace!("{}", msg),
        LogLevel::Debug => log::debug!("{}", msg),
        LogLevel::Info => log::info!("{}", msg),
        LogLevel::Warn => log::warn!("{}", msg),
        LogLevel::Error => log::error!("{}", msg),
    }
}

/// Fabric-internal event emitted when the route table or service table is consulted.
#[derive(Debug, Clone)]
pub enum FabricEvent {
    /// A frame hit a placeholder route or no route was found for a routed IP.
    RouteMiss {
        dst_ip: Ipv4Addr,
    },
    /// A frame hit a service IP that has no ready backend.
    ServiceActivation { service_id: String, dst_ip: Ipv4Addr },
    /// An activator signaled a backend need level change.
    ServiceBackendNeed {
        service_id: String,
        #[allow(dead_code)]
        dst_ip: Ipv4Addr,
        need: distvirt_worker_protocol::BackendNeed,
    },
}

/// Shared port handle that can be used by any reader task to send frames.
type SharedPort<P> = Arc<P>;

/// The L3 fabric router.
///
/// Manages a set of ports and routes IP packets between them. Each port runs
/// a tokio task that reads packets and performs IP-based forwarding inline.
///
/// Port tasks are owned by the caller (returned as `TaskHandle`). Port map
/// cleanup is automatic via `PortGuard`.
pub struct Fabric<P: FramePort = Port> {
    ctx: FabricContext<P>,
    next_port_id: AtomicUsize,
    _gateway_ingress_task: Mutex<Option<TaskHandle<()>>>,
    _gc_task: Mutex<Option<TaskHandle<()>>>,
}

impl Fabric<Port> {
    /// Add a TAP device as a port, pre-register its IP, flush any buffered
    /// frames for `pod_ip`, and start the forwarding task.
    #[allow(dead_code)]
    pub fn add_port_with_ip(
        &self,
        tap: TapDevice,
        pod_ip: Ipv4Addr,
        guest_mac: [u8; 6],
    ) -> std::io::Result<(PortId, TaskHandle<()>)> {
        let port = Port::new(tap, guest_mac)?;
        Ok(self.add_port_inner(port, Some(pod_ip)))
    }
}

impl Fabric<FabricPort> {
    /// Add a TAP device as a port, wrapping it in `FabricPort::Tap`.
    pub fn add_tap_port(
        &self,
        tap: TapDevice,
        pod_ip: Ipv4Addr,
        guest_mac: [u8; 6],
    ) -> std::io::Result<(PortId, TaskHandle<()>)> {
        let port = Port::new(tap, guest_mac)?;
        Ok(self.add_port_inner(FabricPort::Tap(port), Some(pod_ip)))
    }
}

impl<P: FramePort> Fabric<P> {
    /// Create a new empty fabric.
    ///
    /// `subnet` and `prefix_len` define the fabric's pod subnet, used for
    /// determining whether to drop unknown in-subnet IPs vs forwarding to gateway.
    pub fn new(gateway_ip: Ipv4Addr, prefix_len: u8) -> Self {
        // Derive subnet base from gateway IP and prefix length.
        let mask = if prefix_len >= 32 { u32::MAX } else { !0u32 << (32 - prefix_len) };
        let subnet = Ipv4Addr::from(u32::from(gateway_ip) & mask);
        Fabric {
            ctx: FabricContext {
                inner: Arc::new(FabricContextInner {
                    ports: Mutex::new(HashMap::new()),
                    ip_port_table: Mutex::new(IpPortTable::new()),
                    route_table: Mutex::new(RouteTable::new()),
                    service_table: Mutex::new(ServiceTable::new()),
                    nat_table: Mutex::new(nat::NatTable::new()),
                    gateway_tx: OnceLock::new(),
                    event_tx: OnceLock::new(),
                    subnet,
                    prefix_len,
                    gateway_ip,
                }),
            },
            next_port_id: AtomicUsize::new(0),
            _gateway_ingress_task: Mutex::new(None),
            _gc_task: Mutex::new(None),
        }
    }

    /// Add a pre-constructed port and start its forwarding task.
    ///
    /// Returns the assigned port ID and a `TaskHandle` that owns the port's
    /// read loop task.
    #[allow(dead_code)]
    pub fn add_port_raw(&self, port: P) -> (PortId, TaskHandle<()>) {
        self.add_port_inner(port, None)
    }

    /// Add a pre-constructed port with an associated IP, flush buffered frames,
    /// and start the forwarding task.
    #[allow(dead_code)]
    pub fn add_port_raw_with_ip(&self, port: P, pod_ip: Ipv4Addr) -> (PortId, TaskHandle<()>) {
        self.add_port_inner(port, Some(pod_ip))
    }

    /// Shared implementation for all add_port variants.
    fn add_port_inner(&self, port: P, pod_ip: Option<Ipv4Addr>) -> (PortId, TaskHandle<()>) {
        let port_id = self.next_port_id.fetch_add(1, Ordering::Relaxed);

        let port = Arc::new(port);

        {
            let mut ports = self.ctx.inner.ports.lock().unwrap();
            ports.insert(port_id, Arc::clone(&port));
        }

        // Pre-register IP so the fabric can route to this port immediately.
        if let Some(ip) = pod_ip {
            let mut table = self.ctx.inner.ip_port_table.lock().unwrap();
            table.insert(ip, port_id);
        }

        // Flush service table buffers for any ready service whose backend IP
        // matches this port's IP. This handles the case where frames were
        // buffered because the backend IP wasn't reachable yet.
        if let Some(ip) = pod_ip {
            let service_flushes = {
                let mut st = self.ctx.inner.service_table.lock().unwrap();
                st.flush_by_backend_ip(&ip)
            };
            if !service_flushes.is_empty() {
                log::info!(
                    "fabric: add_port_inner: flushing {} service(s) for IP {}",
                    service_flushes.len(), ip
                );
            }
            for flush_data in service_flushes {
                self.flush_service_frames(
                    flush_data.frames,
                    flush_data.backend_ip,
                    flush_data.service_ip,
                );
            }
        }

        // Flush buffered frames if an IP was provided.
        if let Some(pod_ip) = pod_ip {
            let buffered = {
                let mut rt = self.ctx.inner.route_table.lock().unwrap();
                rt.flush_buffer(pod_ip)
            };
            if !buffered.is_empty() {
                let flush_port = Arc::clone(&port);
                let count = buffered.len();
                tokio::spawn(async move {
                    for frame in buffered {
                        if let Err(e) = flush_port.send_frame(&frame).await {
                            log::warn!("fabric: flush buffered frame error: {}", e);
                            break;
                        }
                    }
                    log::info!("fabric: flushed {} buffered frames to port {}", count, port_id);
                });
            }
        }

        let task = TaskHandle::spawn(port_read_loop(
            port_id,
            Arc::clone(&port),
            self.ctx.clone(),
        ));

        match pod_ip {
            Some(ip) => log::info!("fabric: added port {} with ip {}", port_id, ip),
            None => log::info!("fabric: added port {}", port_id),
        }

        (port_id, task)
    }

    /// Connect an output gateway to the fabric.
    ///
    /// `egress_tx` is stored so port read loops can forward gateway-destined frames.
    /// `ingress_rx` is consumed by a spawned task that injects returning frames
    /// back into the fabric via IP routing.
    pub fn set_gateway(
        &self,
        egress_tx: mpsc::Sender<Vec<u8>>,
        ingress_rx: mpsc::Receiver<Vec<u8>>,
    ) {
        let _ = self.ctx.inner.gateway_tx.set(egress_tx);

        *self._gateway_ingress_task.lock().unwrap() = Some(TaskHandle::spawn(gateway_ingress_task(
            ingress_rx,
            self.ctx.clone(),
        )));

        // Spawn periodic NAT table GC task.
        {
            let inner = Arc::clone(&self.ctx.inner);
            let gc_task = TaskHandle::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
                loop {
                    interval.tick().await;
                    {
                        let mut nat = inner.nat_table.lock().unwrap();
                        nat.gc(std::time::Duration::from_secs(300));
                    }
                }
            });
            *self._gc_task.lock().unwrap() = Some(gc_task);
        }

        log::info!("fabric: gateway connected");
    }

    /// Set the event channel for fabric events (route misses, etc.).
    pub fn set_event_channel(&self, tx: mpsc::Sender<FabricEvent>) {
        let _ = self.ctx.inner.event_tx.set(tx);
    }

    /// Get a shared reference to the fabric tables (ports, MAC, route, service).
    pub fn tables(&self) -> Arc<FabricContextInner<P>> {
        Arc::clone(&self.ctx.inner)
    }

    /// Send raw Ethernet frames (no vnet header) out to the port that owns
    /// the given destination IP. Prepends a zeroed vnet header before sending.
    pub fn send_l4_frames(&self, frames: Vec<Vec<u8>>) {
        forwarding::send_l4_frames(&frames, &self.ctx);
    }

    /// Dispatch activator actions from mark_ready (replay packets, log, backend need).
    ///
    /// Looks up the service IP from the service table, then delegates to
    /// `forwarding::dispatch_action` for each action.
    pub async fn dispatch_actions(
        &self,
        actions: &[Action],
        service_id: &str,
    ) {
        let dst_ip = {
            let st = self.ctx.inner.service_table.lock().unwrap();
            st.get_ip_by_id(service_id)
                .unwrap_or(std::net::Ipv4Addr::UNSPECIFIED)
        };
        for action in actions {
            forwarding::dispatch_action(
                action, service_id, dst_ip, &self.ctx,
            ).await;
        }
    }

    /// Flush buffered service frames to the backend port.
    ///
    /// Looks up the backend IP in the ip_port_table to find the port,
    /// rewrites the IP (DNAT) in each frame, and sends them.
    pub fn flush_service_frames(
        &self,
        frames: Vec<Vec<u8>>,
        backend_ip: std::net::Ipv4Addr,
        service_ip: std::net::Ipv4Addr,
    ) {
        let dst_port = match self.ctx.inner.resolve_ip(&backend_ip) {
            Some(p) => p,
            None => {
                log::warn!(
                    "fabric: flush_service_frames: backend IP {} not resolved, dropping {} frames",
                    backend_ip,
                    frames.len()
                );
                return;
            }
        };

        // Insert NAT entries for all flushed frames.
        {
            let mut nat = self.ctx.inner.nat_table.lock().unwrap();
            for frame in &frames {
                if let Some(fp) = FabricPacket::new(frame) {
                    let ip = fp.ip_packet();
                    if let Some(src_ip) = ip_packet_src(ip) {
                        let protocol = ip_packet_protocol(ip).unwrap_or(0);
                        let (src_port, dst_port_val) = ip_packet_transport_ports(ip).unwrap_or((0, 0));
                        let reverse_key = nat::NatFlowKey {
                            src_ip: backend_ip,
                            dst_ip: src_ip,
                            protocol,
                            src_port: dst_port_val,
                            dst_port: src_port,
                        };
                        let nat_entry = nat::NatEntry {
                            service_ip,
                            backend_ip,
                            last_seen: std::time::Instant::now(),
                        };
                        nat.insert(reverse_key, nat_entry);
                    }
                }
            }
        }

        let count = frames.len();
        tokio::spawn(async move {
            for mut frame in frames {
                // DNAT: rewrite dst IP from service_ip to backend_ip.
                rewrite_ipv4_dst(&mut frame, service_ip, backend_ip);
                match dst_port.send_frame(&frame).await {
                    Err(e) => {
                        log::warn!("fabric: flush_service_frames send error: {}", e);
                        break;
                    }
                    Ok(n) => {
                        log::debug!("fabric: flush_service_frames: sent {} bytes", n);
                    }
                }
            }
            log::info!("fabric: flushed {} service frames to backend", count);
        });
    }
}

#[cfg(test)]
mod tests;
