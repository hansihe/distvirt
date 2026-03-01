pub(crate) mod dns;
mod forwarding;
pub(crate) mod gateway;
pub(crate) mod port;
pub(crate) mod route;
pub(crate) mod service;
pub(crate) mod switch;
pub(crate) mod tun;

pub use dns::DnsRegistry;
pub use gateway::FabricGateway;
pub use switch::GATEWAY_IP_STR;
pub use port::{FramePort, Port, PortId};
pub use route::RouteTable;
pub use service::ServiceTable;

use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;
use crate::tap::TapDevice;
use crate::task_handle::TaskHandle;
use distvirt_activator::types::{Action, BackendNeed as ActivatorBackendNeed, LogAction, LogLevel};
use forwarding::{port_read_loop, gateway_ingress_task};
use switch::{MacTable, VNET_HDR_SZ, format_mac};

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
    RouteMiss { dst_ip: Ipv4Addr, dst_mac: [u8; 6] },
    /// A frame hit a service IP that has no ready backend.
    ServiceActivation { service_id: String, dst_ip: Ipv4Addr },
    /// An activator signaled a backend need level change.
    ServiceBackendNeed {
        service_id: String,
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
    ports: Arc<Mutex<HashMap<PortId, SharedPort<P>>>>,
    mac_table: Arc<Mutex<MacTable>>,
    route_table: Arc<Mutex<RouteTable>>,
    service_table: Arc<Mutex<ServiceTable>>,
    next_port_id: PortId,
    gateway_tx: Option<mpsc::Sender<Vec<u8>>>,
    event_tx: Option<mpsc::Sender<FabricEvent>>,
    _gateway_ingress_task: Option<TaskHandle<()>>,
}

impl Fabric<Port> {
    /// Add a TAP device as a port and start its forwarding task.
    ///
    /// Returns the assigned port ID and a `TaskHandle` that owns the port's
    /// read loop task. The caller must keep the handle alive; dropping it
    /// aborts the task and the `PortGuard` cleans up the port map entry.
    pub fn add_port(&mut self, tap: TapDevice) -> std::io::Result<(PortId, TaskHandle<()>)> {
        let port_id = self.next_port_id;
        self.next_port_id += 1;

        let port = Arc::new(Port::new(tap)?);

        {
            let mut ports = self.ports.lock().unwrap();
            ports.insert(port_id, Arc::clone(&port));
        }

        let task = TaskHandle::spawn(port_read_loop(
            port_id,
            Arc::clone(&port),
            Arc::clone(&self.ports),
            Arc::clone(&self.mac_table),
            Arc::clone(&self.route_table),
            Arc::clone(&self.service_table),
            self.gateway_tx.clone(),
            self.event_tx.clone(),
        ));

        log::info!("fabric: added port {}", port_id);
        Ok((port_id, task))
    }

    /// Add a TAP device as a port, flush any buffered frames for `pod_ip`,
    /// and start the forwarding task.
    pub fn add_port_with_ip(
        &mut self,
        tap: TapDevice,
        pod_ip: Ipv4Addr,
    ) -> std::io::Result<(PortId, TaskHandle<()>)> {
        let port_id = self.next_port_id;
        self.next_port_id += 1;

        let port = Arc::new(Port::new(tap)?);

        {
            let mut ports = self.ports.lock().unwrap();
            ports.insert(port_id, Arc::clone(&port));
        }

        // Flush buffered frames for this IP and send them to the new port.
        let buffered = {
            let mut rt = self.route_table.lock().unwrap();
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

        let task = TaskHandle::spawn(port_read_loop(
            port_id,
            Arc::clone(&port),
            Arc::clone(&self.ports),
            Arc::clone(&self.mac_table),
            Arc::clone(&self.route_table),
            Arc::clone(&self.service_table),
            self.gateway_tx.clone(),
            self.event_tx.clone(),
        ));

        log::info!("fabric: added port {} with ip {}", port_id, pod_ip);
        Ok((port_id, task))
    }
}

impl<P: FramePort> Fabric<P> {
    /// Create a new empty fabric.
    pub fn new() -> Self {
        Fabric {
            ports: Arc::new(Mutex::new(HashMap::new())),
            mac_table: Arc::new(Mutex::new(MacTable::new())),
            route_table: Arc::new(Mutex::new(RouteTable::new())),
            service_table: Arc::new(Mutex::new(ServiceTable::new())),
            next_port_id: 0,
            gateway_tx: None,
            event_tx: None,
            _gateway_ingress_task: None,
        }
    }

    /// Add a pre-constructed port and start its forwarding task.
    ///
    /// Returns the assigned port ID and a `TaskHandle` that owns the port's
    /// read loop task.
    pub fn add_port_raw(&mut self, port: P) -> (PortId, TaskHandle<()>) {
        let port_id = self.next_port_id;
        self.next_port_id += 1;

        let port = Arc::new(port);

        {
            let mut ports = self.ports.lock().unwrap();
            ports.insert(port_id, Arc::clone(&port));
        }

        let task = TaskHandle::spawn(port_read_loop(
            port_id,
            Arc::clone(&port),
            Arc::clone(&self.ports),
            Arc::clone(&self.mac_table),
            Arc::clone(&self.route_table),
            Arc::clone(&self.service_table),
            self.gateway_tx.clone(),
            self.event_tx.clone(),
        ));

        log::info!("fabric: added port {}", port_id);
        (port_id, task)
    }

    /// Add a pre-constructed port with an associated IP, flush buffered frames,
    /// and start the forwarding task.
    pub fn add_port_raw_with_ip(&mut self, port: P, pod_ip: Ipv4Addr) -> (PortId, TaskHandle<()>) {
        let port_id = self.next_port_id;
        self.next_port_id += 1;

        let port = Arc::new(port);

        {
            let mut ports = self.ports.lock().unwrap();
            ports.insert(port_id, Arc::clone(&port));
        }

        let buffered = {
            let mut rt = self.route_table.lock().unwrap();
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

        let task = TaskHandle::spawn(port_read_loop(
            port_id,
            Arc::clone(&port),
            Arc::clone(&self.ports),
            Arc::clone(&self.mac_table),
            Arc::clone(&self.route_table),
            Arc::clone(&self.service_table),
            self.gateway_tx.clone(),
            self.event_tx.clone(),
        ));

        log::info!("fabric: added port {} with ip {}", port_id, pod_ip);
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
        self.gateway_tx = Some(egress_tx);

        let ports = Arc::clone(&self.ports);
        let mac_table = Arc::clone(&self.mac_table);
        let route_table = Arc::clone(&self.route_table);
        let service_table = Arc::clone(&self.service_table);
        let event_tx = self.event_tx.clone();

        self._gateway_ingress_task = Some(TaskHandle::spawn(gateway_ingress_task(
            ingress_rx,
            ports,
            mac_table,
            route_table,
            service_table,
            event_tx,
        )));

        log::info!("fabric: gateway connected");
    }

    /// Set the event channel for fabric events (route misses, etc.).
    pub fn set_event_channel(&mut self, tx: mpsc::Sender<FabricEvent>) {
        self.event_tx = Some(tx);
    }

    /// Get a reference to the route table.
    pub fn route_table(&self) -> Arc<Mutex<RouteTable>> {
        Arc::clone(&self.route_table)
    }

    /// Get a reference to the service table.
    pub fn service_table(&self) -> Arc<Mutex<ServiceTable>> {
        Arc::clone(&self.service_table)
    }

    /// Send raw Ethernet frames (no vnet header) out to the port that owns
    /// the given destination MAC. Prepends a zeroed vnet header before sending.
    pub fn send_l4_frames(&self, frames: Vec<Vec<u8>>) {
        if frames.is_empty() {
            return;
        }
        let mac_table = self.mac_table.lock().unwrap();
        let ports = self.ports.lock().unwrap();

        // Group frames by destination port for efficiency.
        for eth_frame in frames {
            if eth_frame.len() < 6 {
                continue;
            }
            let dst_mac: [u8; 6] = eth_frame[0..6].try_into().unwrap();
            if let Some(port_id) = mac_table.lookup(&dst_mac) {
                if let Some(port) = ports.get(&port_id) {
                    let port = Arc::clone(port);
                    let mut frame = vec![0u8; VNET_HDR_SZ];
                    frame.extend_from_slice(&eth_frame);
                    tokio::spawn(async move {
                        if let Err(e) = port.send_frame(&frame).await {
                            log::warn!("fabric: send_l4_frame error: {}", e);
                        }
                    });
                }
            } else {
                log::debug!(
                    "fabric: send_l4_frame: dst MAC {} not in mac_table",
                    format_mac(&dst_mac)
                );
            }
        }
    }

    /// Execute activator actions from mark_ready (replay packets, log, backend need).
    pub fn execute_service_actions(
        &self,
        actions: &[Action],
        service_id: &str,
    ) {
        let service_table = self.service_table.lock().unwrap();
        let dst_ip = service_table.get_ip_by_id(service_id)
            .unwrap_or(std::net::Ipv4Addr::UNSPECIFIED);
        let mac_table_inner = &*self.mac_table;
        let ports_inner = &*self.ports;
        for action in actions {
            match action {
                Action::ReplayPacket(raw_frame) => {
                    if let Some(pod_mac) = service_table.get_backend_mac_by_id(service_id) {
                        let mt = mac_table_inner.lock().unwrap();
                        if let Some(port_id) = mt.lookup(&pod_mac) {
                            let ports = ports_inner.lock().unwrap();
                            if let Some(port) = ports.get(&port_id) {
                                let port = Arc::clone(port);
                                let mut rewritten = raw_frame.clone();
                                if rewritten.len() >= VNET_HDR_SZ + 6 {
                                    rewritten[VNET_HDR_SZ..VNET_HDR_SZ + 6]
                                        .copy_from_slice(&pod_mac);
                                }
                                tokio::spawn(async move {
                                    if let Err(e) = port.send_frame(&rewritten).await {
                                        log::warn!("fabric: replay send error: {}", e);
                                    }
                                });
                            }
                        }
                    }
                }
                Action::SetBackendNeed(need) => {
                    let proto_need = convert_backend_need(need);
                    if let Some(ref tx) = self.event_tx {
                        let _ = tx.try_send(FabricEvent::ServiceBackendNeed {
                            service_id: service_id.to_string(),
                            dst_ip,
                            need: proto_need,
                        });
                    }
                }
                Action::Log(log_action) => {
                    handle_log_action(service_id, log_action);
                }
                _ => {}
            }
        }
    }

    /// Flush buffered service frames to the backend port.
    ///
    /// Looks up the backend MAC in the mac_table to find the port,
    /// rewrites the destination MAC in each frame, and sends them.
    pub fn flush_service_frames(&self, frames: Vec<Vec<u8>>, backend_mac: [u8; 6]) {
        let dst_port = {
            let mac_table = self.mac_table.lock().unwrap();
            let port_id = match mac_table.lookup(&backend_mac) {
                Some(id) => id,
                None => {
                    log::warn!(
                        "fabric: flush_service_frames: backend MAC {} not in mac_table, dropping {} frames",
                        format_mac(&backend_mac),
                        frames.len()
                    );
                    return;
                }
            };
            let ports = self.ports.lock().unwrap();
            match ports.get(&port_id) {
                Some(p) => Arc::clone(p),
                None => {
                    log::warn!(
                        "fabric: flush_service_frames: port {} gone, dropping {} frames",
                        port_id,
                        frames.len()
                    );
                    return;
                }
            }
        };

        let count = frames.len();
        tokio::spawn(async move {
            for mut frame in frames {
                // Rewrite dst MAC in the frame (after vnet header).
                if frame.len() >= VNET_HDR_SZ + 6 {
                    frame[VNET_HDR_SZ..VNET_HDR_SZ + 6].copy_from_slice(&backend_mac);
                }
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
