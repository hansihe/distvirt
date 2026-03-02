mod forwarding;
pub(crate) mod nat;
pub(crate) mod port;
pub(crate) mod route;
pub(crate) mod service;
pub(crate) mod switch;

pub use switch::GATEWAY_IP_STR;
pub use port::{ChannelPort, FabricPort, FramePort, Port, PortId};
pub use route::RouteTable;
pub use service::ServiceTable;
pub(crate) use forwarding::FabricContextInner;

use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;
use crate::tap::TapDevice;
use crate::task_handle::TaskHandle;
use distvirt_activator::types::{Action, BackendNeed as ActivatorBackendNeed, LogAction, LogLevel};
use forwarding::{FabricContext, port_read_loop, gateway_ingress_task};
use switch::{MacTable, VNET_HDR_SZ, format_mac, rewrite_dst_mac};

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
        #[allow(dead_code)]
        dst_ip: Ipv4Addr,
        dst_mac: [u8; 6],
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

/// The L2 fabric switch.
///
/// Manages a set of ports (TAP devices) and switches Ethernet frames between
/// them. Each port runs a tokio task that reads frames and performs MAC learning,
/// ARP responding, and forwarding inline.
///
/// Port tasks are owned by the caller (returned as `TaskHandle`). Port map
/// cleanup is automatic via `PortGuard`.
pub struct Fabric<P: FramePort = Port> {
    ctx: FabricContext<P>,
    next_port_id: PortId,
    _gateway_ingress_task: Option<TaskHandle<()>>,
}

impl Fabric<Port> {
    /// Add a TAP device as a port, pre-register its MAC, flush any buffered
    /// frames for `pod_ip`, and start the forwarding task.
    pub fn add_port_with_ip(
        &mut self,
        tap: TapDevice,
        pod_ip: Ipv4Addr,
        pod_mac: [u8; 6],
    ) -> std::io::Result<(PortId, TaskHandle<()>)> {
        let port = Port::new(tap)?;
        Ok(self.add_port_inner(port, Some(pod_ip), Some(pod_mac)))
    }
}

impl Fabric<FabricPort> {
    /// Add a TAP device as a port, wrapping it in `FabricPort::Tap`.
    pub fn add_tap_port(
        &mut self,
        tap: TapDevice,
        pod_ip: Ipv4Addr,
        pod_mac: [u8; 6],
    ) -> std::io::Result<(PortId, TaskHandle<()>)> {
        let port = Port::new(tap)?;
        Ok(self.add_port_inner(FabricPort::Tap(port), Some(pod_ip), Some(pod_mac)))
    }
}

impl<P: FramePort> Fabric<P> {
    /// Create a new empty fabric.
    pub fn new() -> Self {
        Fabric {
            ctx: FabricContext {
                inner: Arc::new(FabricContextInner {
                    ports: Mutex::new(HashMap::new()),
                    mac_table: Mutex::new(MacTable::new()),
                    route_table: Mutex::new(RouteTable::new()),
                    service_table: Mutex::new(ServiceTable::new()),
                    nat_table: Mutex::new(nat::NatTable::new()),
                }),
                gateway_tx: None,
                event_tx: None,
            },
            next_port_id: 0,
            _gateway_ingress_task: None,
        }
    }

    /// Add a pre-constructed port and start its forwarding task.
    ///
    /// Returns the assigned port ID and a `TaskHandle` that owns the port's
    /// read loop task.
    #[allow(dead_code)]
    pub fn add_port_raw(&mut self, port: P) -> (PortId, TaskHandle<()>) {
        self.add_port_inner(port, None, None)
    }

    /// Add a pre-constructed port with an associated IP, flush buffered frames,
    /// and start the forwarding task.
    #[allow(dead_code)]
    pub fn add_port_raw_with_ip(&mut self, port: P, pod_ip: Ipv4Addr) -> (PortId, TaskHandle<()>) {
        self.add_port_inner(port, Some(pod_ip), None)
    }

    /// Shared implementation for all add_port variants.
    fn add_port_inner(&mut self, port: P, pod_ip: Option<Ipv4Addr>, pod_mac: Option<[u8; 6]>) -> (PortId, TaskHandle<()>) {
        let port_id = self.next_port_id;
        self.next_port_id += 1;

        let port = Arc::new(port);

        {
            let mut ports = self.ctx.inner.ports.lock().unwrap();
            ports.insert(port_id, Arc::clone(&port));
        }

        // Pre-register MAC so the switch can forward to this port immediately.
        if let Some(mac) = pod_mac {
            let mut table = self.ctx.inner.mac_table.lock().unwrap();
            table.learn(mac, port_id);
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
    /// back into the fabric via MAC lookup or flooding.
    pub fn set_gateway(
        &mut self,
        egress_tx: mpsc::Sender<Vec<u8>>,
        ingress_rx: mpsc::Receiver<Vec<u8>>,
    ) {
        self.ctx.gateway_tx = Some(egress_tx);

        self._gateway_ingress_task = Some(TaskHandle::spawn(gateway_ingress_task(
            ingress_rx,
            self.ctx.clone(),
        )));

        // Spawn periodic MAC table + NAT table GC task.
        {
            let inner = Arc::clone(&self.ctx.inner);
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
                loop {
                    interval.tick().await;
                    {
                        let mut table = inner.mac_table.lock().unwrap();
                        table.gc(std::time::Duration::from_secs(300));
                    }
                    {
                        let mut nat = inner.nat_table.lock().unwrap();
                        nat.gc(std::time::Duration::from_secs(300));
                    }
                }
            });
        }

        log::info!("fabric: gateway connected");
    }

    /// Set the event channel for fabric events (route misses, etc.).
    pub fn set_event_channel(&mut self, tx: mpsc::Sender<FabricEvent>) {
        self.ctx.event_tx = Some(tx);
    }

    /// Get a shared reference to the fabric tables (ports, MAC, route, service).
    pub fn tables(&self) -> Arc<FabricContextInner<P>> {
        Arc::clone(&self.ctx.inner)
    }

    /// Send raw Ethernet frames (no vnet header) out to the port that owns
    /// the given destination MAC. Prepends a zeroed vnet header before sending.
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
    /// Looks up the backend MAC in the mac_table to find the port,
    /// rewrites the destination MAC and IP (DNAT) in each frame, and sends them.
    pub fn flush_service_frames(
        &self,
        frames: Vec<Vec<u8>>,
        backend_mac: [u8; 6],
        backend_ip: std::net::Ipv4Addr,
        service_ip: std::net::Ipv4Addr,
        service_mac: [u8; 6],
    ) {
        let dst_port = match self.ctx.inner.resolve_mac(&backend_mac) {
            Some(p) => p,
            None => {
                log::warn!(
                    "fabric: flush_service_frames: backend MAC {} not resolved, dropping {} frames",
                    format_mac(&backend_mac),
                    frames.len()
                );
                return;
            }
        };

        // Insert NAT entries for all flushed frames.
        {
            let mut nat = self.ctx.inner.nat_table.lock().unwrap();
            for frame in &frames {
                if let Some(ff) = switch::FabricFrame::new(frame) {
                    let eth = ff.eth_payload();
                    if let Some(src_ip) = switch::extract_ipv4_src(eth) {
                        let protocol = switch::extract_ip_protocol(eth).unwrap_or(0);
                        let (src_port, dst_port_val) = switch::extract_transport_ports(eth).unwrap_or((0, 0));
                        let reverse_key = nat::NatFlowKey {
                            src_ip: backend_ip,
                            dst_ip: src_ip,
                            protocol,
                            src_port: dst_port_val,
                            dst_port: src_port,
                        };
                        let nat_entry = nat::NatEntry {
                            service_ip,
                            service_mac,
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
                // Rewrite dst MAC in the frame (after vnet header).
                if frame.len() >= VNET_HDR_SZ + 6 {
                    rewrite_dst_mac(&mut frame, &backend_mac);
                }
                // DNAT: rewrite dst IP from service_ip to backend_ip.
                switch::rewrite_ipv4_dst(&mut frame, service_ip, backend_ip);
                if let Err(e) = dst_port.send_frame(&frame).await {
                    log::warn!("fabric: flush_service_frames send error: {}", e);
                    break;
                }
            }
            log::info!("fabric: flushed {} service frames to backend", count);
        });
    }
}

#[cfg(test)]
mod tests;
