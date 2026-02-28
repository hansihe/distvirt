pub(crate) mod dns;
pub(crate) mod gateway;
pub(crate) mod port;
pub(crate) mod route;
pub(crate) mod switch;
pub(crate) mod tun;

pub use dns::DnsRegistry;
pub use gateway::FabricGateway;
pub use switch::GATEWAY_IP_STR;
pub use port::{FramePort, Port, PortId};
pub use route::RouteTable;

use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;
use crate::tap::TapDevice;
use crate::task_handle::TaskHandle;
use route::RouteAction;
use switch::{GATEWAY_MAC, MacTable, VNET_HDR_SZ, extract_ipv4_dst, format_mac, is_broadcast, parse_ethernet_header};

/// Fabric-internal event emitted when the route table is consulted.
#[derive(Debug, Clone)]
pub enum FabricEvent {
    /// A frame hit a placeholder route or no route was found for a routed IP.
    RouteMiss { dst_ip: Ipv4Addr, dst_mac: [u8; 6] },
}

/// Shared port handle that can be used by any reader task to send frames.
type SharedPort<P> = Arc<P>;

/// RAII guard that removes a port from the fabric's port map on drop.
///
/// Created at the top of each port read loop. Guarantees cleanup whether the
/// task exits normally, errors out, or is aborted.
struct PortGuard<P: FramePort> {
    port_id: PortId,
    ports: Arc<Mutex<HashMap<PortId, SharedPort<P>>>>,
}

impl<P: FramePort> Drop for PortGuard<P> {
    fn drop(&mut self) {
        let mut ports = self.ports.lock().unwrap();
        ports.remove(&self.port_id);
        log::info!("fabric: port {} removed (guard dropped)", self.port_id);
    }
}

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
        let event_tx = self.event_tx.clone();

        self._gateway_ingress_task = Some(TaskHandle::spawn(gateway_ingress_task(
            ingress_rx,
            ports,
            mac_table,
            route_table,
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
}

/// Per-port read loop: reads frames, learns MACs, responds to gateway ARP,
/// and forwards/floods frames to other ports.
async fn port_read_loop<P: FramePort>(
    port_id: PortId,
    port: SharedPort<P>,
    ports: Arc<Mutex<HashMap<PortId, SharedPort<P>>>>,
    mac_table: Arc<Mutex<MacTable>>,
    route_table: Arc<Mutex<RouteTable>>,
    gateway_tx: Option<mpsc::Sender<Vec<u8>>>,
    event_tx: Option<mpsc::Sender<FabricEvent>>,
) {
    // PortGuard removes this port from the map when this task exits for any reason.
    let _guard = PortGuard {
        port_id,
        ports: Arc::clone(&ports),
    };

    let mut buf = vec![0u8; VNET_HDR_SZ + 1514]; // vnet header + max Ethernet frame

    loop {
        let n = match port.recv_frame(&mut buf).await {
            Ok(0) => {
                log::info!("fabric: port {} EOF", port_id);
                break;
            }
            Ok(n) => n,
            Err(e) => {
                log::warn!("fabric: port {} recv error: {}", port_id, e);
                break;
            }
        };

        if n < VNET_HDR_SZ {
            continue; // too short to contain vnet header
        }

        let frame = &buf[..n];

        let (dst_mac, src_mac, ethertype) = match parse_ethernet_header(&frame[VNET_HDR_SZ..]) {
            Some(h) => h,
            None => continue, // runt frame
        };

        log::trace!(
            "fabric: port {} frame {} -> {} ethertype=0x{:04x} len={}",
            port_id,
            format_mac(&src_mac),
            format_mac(&dst_mac),
            ethertype,
            n,
        );

        // Learn source MAC.
        {
            let mut table = mac_table.lock().unwrap();
            table.learn(src_mac, port_id);
        }

        // Forward or flood.
        if is_broadcast(&dst_mac) || dst_mac[0] & 0x01 != 0 {
            // Broadcast/multicast: flood to all other ports and also to gateway.
            flood_frame(frame, port_id, &ports).await;
            if let Some(ref gw_tx) = gateway_tx {
                let _ = gw_tx.try_send(frame.to_vec());
            }
        } else if dst_mac == GATEWAY_MAC {
            // Gateway-destined frame: send to gateway via channel.
            if let Some(ref gw_tx) = gateway_tx {
                let _ = gw_tx.try_send(frame.to_vec());
            }
            continue;
        } else {
            // Unicast lookup.
            let dst_port_id = {
                let table = mac_table.lock().unwrap();
                table.lookup(&dst_mac)
            };

            if let Some(dst_id) = dst_port_id {
                if dst_id != port_id {
                    let dst_port = {
                        let ports = ports.lock().unwrap();
                        ports.get(&dst_id).cloned()
                    };
                    if let Some(dst_port) = dst_port {
                        if let Err(e) = dst_port.send_frame(frame).await {
                            log::warn!(
                                "fabric: send port {} -> {} error: {}",
                                port_id,
                                dst_id,
                                e
                            );
                        }
                    }
                }
            } else {
                // Unknown unicast: consult route table for IPv4 frames.
                handle_unknown_unicast(
                    frame, dst_mac, port_id, &ports, &route_table, &event_tx,
                ).await;
            }
        }
    }
}

/// Task that reads frames from the gateway ingress channel and forwards them
/// into the fabric via MAC lookup or flooding.
async fn gateway_ingress_task<P: FramePort>(
    mut ingress_rx: mpsc::Receiver<Vec<u8>>,
    ports: Arc<Mutex<HashMap<PortId, SharedPort<P>>>>,
    mac_table: Arc<Mutex<MacTable>>,
    route_table: Arc<Mutex<RouteTable>>,
    event_tx: Option<mpsc::Sender<FabricEvent>>,
) {
    while let Some(frame) = ingress_rx.recv().await {
        if frame.len() < VNET_HDR_SZ {
            continue;
        }
        let (dst_mac, _src_mac, _ethertype) = match parse_ethernet_header(&frame[VNET_HDR_SZ..]) {
            Some(h) => h,
            None => continue,
        };

        if is_broadcast(&dst_mac) || dst_mac[0] & 0x01 != 0 {
            // Broadcast/multicast: flood to all ports.
            // Use PortId::MAX as the "source" so no port is excluded.
            flood_frame(&frame, PortId::MAX, &ports).await;
        } else {
            let dst_port_id = {
                let table = mac_table.lock().unwrap();
                table.lookup(&dst_mac)
            };

            if let Some(dst_id) = dst_port_id {
                let dst_port = {
                    let ports = ports.lock().unwrap();
                    ports.get(&dst_id).cloned()
                };
                if let Some(dst_port) = dst_port {
                    if let Err(e) = dst_port.send_frame(&frame).await {
                        log::warn!("fabric: gateway ingress send to port {} error: {}", dst_id, e);
                    }
                }
            } else {
                // Unknown unicast: consult route table for IPv4 frames.
                handle_unknown_unicast(
                    &frame, dst_mac, PortId::MAX, &ports, &route_table, &event_tx,
                ).await;
            }
        }
    }

    log::info!("fabric: gateway ingress task ended");
}

/// Handle unknown unicast: consult the route table for IPv4 frames,
/// otherwise flood as before.
async fn handle_unknown_unicast<P: FramePort>(
    frame: &[u8],
    dst_mac: [u8; 6],
    src_port_id: PortId,
    ports: &Arc<Mutex<HashMap<PortId, SharedPort<P>>>>,
    route_table: &Arc<Mutex<RouteTable>>,
    event_tx: &Option<mpsc::Sender<FabricEvent>>,
) {
    // Try to extract IPv4 destination from the frame (skip vnet header).
    let eth_frame = &frame[VNET_HDR_SZ..];
    if let Some(dst_ip) = extract_ipv4_dst(eth_frame) {
        let (action, should_miss) = {
            let mut rt = route_table.lock().unwrap();
            rt.lookup_and_buffer(dst_ip, frame)
        };

        match action {
            RouteAction::Buffered => {
                log::trace!("fabric: frame to {} buffered", dst_ip);
            }
            RouteAction::Drop => {
                log::trace!("fabric: frame to {} dropped by route policy", dst_ip);
            }
            RouteAction::RemoteWorker { worker_id } => {
                log::debug!(
                    "fabric: frame to {} destined for remote worker {} (stub: dropping)",
                    dst_ip, worker_id
                );
            }
            RouteAction::NoRoute => {
                // No route entry — flood as before.
                flood_frame(frame, src_port_id, ports).await;
            }
        }

        if should_miss {
            if let Some(tx) = event_tx {
                let _ = tx.try_send(FabricEvent::RouteMiss {
                    dst_ip,
                    dst_mac,
                });
            }
        }
    } else {
        // Non-IPv4 unknown unicast: flood as before.
        flood_frame(frame, src_port_id, ports).await;
    }
}

/// Send a frame to all ports except the source port.
async fn flood_frame<P: FramePort>(
    frame: &[u8],
    src_port_id: PortId,
    ports: &Arc<Mutex<HashMap<PortId, SharedPort<P>>>>,
) {
    let targets: Vec<(PortId, SharedPort<P>)> = {
        let ports = ports.lock().unwrap();
        let mut result = Vec::new();
        for (id, port) in ports.iter() {
            if *id != src_port_id {
                result.push((*id, Arc::clone(port)));
            }
        }
        result
    };

    for (id, port) in targets {
        if let Err(e) = port.send_frame(frame).await {
            log::warn!("fabric: flood to port {} error: {}", id, e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use switch::ETH_HEADER_LEN;
    use tokio::sync::mpsc as tokio_mpsc;

    /// Channel-backed test double for FramePort.
    struct TestPort {
        /// Test injects frames here; recv_frame reads from this.
        rx: tokio::sync::Mutex<tokio_mpsc::Receiver<Vec<u8>>>,
        /// send_frame writes here; test reads captured frames from tx_out.
        tx: tokio_mpsc::Sender<Vec<u8>>,
    }

    struct TestPortHandle {
        /// Send frames into the port (simulates wire ingress).
        inject_tx: tokio_mpsc::Sender<Vec<u8>>,
        /// Receive frames that the fabric sent to this port.
        capture_rx: tokio::sync::Mutex<tokio_mpsc::Receiver<Vec<u8>>>,
    }

    fn make_test_port() -> (TestPort, TestPortHandle) {
        let (inject_tx, inject_rx) = tokio_mpsc::channel(64);
        let (capture_tx, capture_rx) = tokio_mpsc::channel(64);
        (
            TestPort {
                rx: tokio::sync::Mutex::new(inject_rx),
                tx: capture_tx,
            },
            TestPortHandle {
                inject_tx,
                capture_rx: tokio::sync::Mutex::new(capture_rx),
            },
        )
    }

    impl FramePort for TestPort {
        async fn recv_frame(&self, buf: &mut [u8]) -> std::io::Result<usize> {
            let mut rx = self.rx.lock().await;
            match rx.recv().await {
                Some(data) => {
                    let len = data.len().min(buf.len());
                    buf[..len].copy_from_slice(&data[..len]);
                    Ok(len)
                }
                None => Ok(0), // EOF
            }
        }

        async fn send_frame(&self, buf: &[u8]) -> std::io::Result<usize> {
            self.tx
                .send(buf.to_vec())
                .await
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "channel closed"))?;
            Ok(buf.len())
        }
    }

    /// Build a valid test frame: [vnet_hdr (10 bytes)][eth_hdr (14 bytes)][payload...]
    fn make_frame(dst_mac: [u8; 6], src_mac: [u8; 6], ethertype: u16, payload: &[u8]) -> Vec<u8> {
        let mut frame = vec![0u8; VNET_HDR_SZ]; // zeroed vnet header
        frame.extend_from_slice(&dst_mac);
        frame.extend_from_slice(&src_mac);
        frame.extend_from_slice(&ethertype.to_be_bytes());
        frame.extend_from_slice(payload);
        frame
    }

    /// Helper: try to receive a frame with a timeout. Returns None if no frame arrives.
    async fn try_recv(handle: &TestPortHandle) -> Option<Vec<u8>> {
        let mut rx = handle.capture_rx.lock().await;
        tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv())
            .await
            .ok()
            .flatten()
    }

    /// Helper: assert no frame arrives within timeout.
    async fn assert_no_frame(handle: &TestPortHandle) {
        assert!(try_recv(handle).await.is_none(), "expected no frame but got one");
    }

    // Some test MAC addresses
    const MAC_A: [u8; 6] = [0x02, 0x00, 0x00, 0x00, 0x00, 0x0a];
    const MAC_B: [u8; 6] = [0x02, 0x00, 0x00, 0x00, 0x00, 0x0b];
    const MAC_C: [u8; 6] = [0x02, 0x00, 0x00, 0x00, 0x00, 0x0c];
    const BROADCAST: [u8; 6] = [0xff; 6];
    const MULTICAST: [u8; 6] = [0x01, 0x00, 0x5e, 0x00, 0x00, 0x01];

    // --- Unicast forwarding tests ---

    #[tokio::test]
    async fn known_dst_delivers_to_correct_port() {
        let mut fabric: Fabric<TestPort> = Fabric::new();
        let (port0, handle0) = make_test_port();
        let (port1, handle1) = make_test_port();

        let (_id0, _task0) = fabric.add_port_raw(port0);
        let (_id1, _task1) = fabric.add_port_raw(port1);

        // Port 0 sends a frame with src=MAC_A, causing MAC_A to be learned on port 0.
        // Then port 1 sends a frame with dst=MAC_A, which should be delivered to port 0.
        let frame_learn = make_frame(MAC_B, MAC_A, 0x0800, &[0u8; 10]);
        handle0.inject_tx.send(frame_learn).await.unwrap();

        // Wait for learning to happen; the frame floods since MAC_B is unknown.
        let _ = try_recv(&handle1).await;

        // Now port 1 sends to MAC_A (known on port 0).
        let frame_to_a = make_frame(MAC_A, MAC_B, 0x0800, &[0u8; 10]);
        handle1.inject_tx.send(frame_to_a).await.unwrap();

        // Port 0 should receive the frame.
        let received = try_recv(&handle0).await;
        assert!(received.is_some(), "port 0 should receive frame destined to MAC_A");
    }

    #[tokio::test]
    async fn unknown_dst_floods_to_all_other_ports() {
        let mut fabric: Fabric<TestPort> = Fabric::new();
        let (port0, handle0) = make_test_port();
        let (port1, handle1) = make_test_port();
        let (port2, handle2) = make_test_port();

        let (_id0, _task0) = fabric.add_port_raw(port0);
        let (_id1, _task1) = fabric.add_port_raw(port1);
        let (_id2, _task2) = fabric.add_port_raw(port2);

        // Port 0 sends a frame to unknown MAC_C.
        let frame = make_frame(MAC_C, MAC_A, 0x0800, &[0u8; 10]);
        handle0.inject_tx.send(frame).await.unwrap();

        // Both port 1 and port 2 should receive the flooded frame.
        assert!(try_recv(&handle1).await.is_some(), "port 1 should receive flooded frame");
        assert!(try_recv(&handle2).await.is_some(), "port 2 should receive flooded frame");
    }

    #[tokio::test]
    async fn no_loopback_to_source_port() {
        let mut fabric: Fabric<TestPort> = Fabric::new();
        let (port0, handle0) = make_test_port();
        let (port1, _handle1) = make_test_port();

        let (_id0, _task0) = fabric.add_port_raw(port0);
        let (_id1, _task1) = fabric.add_port_raw(port1);

        // Port 0 sends a frame; it should never come back to port 0.
        let frame = make_frame(MAC_B, MAC_A, 0x0800, &[0u8; 10]);
        handle0.inject_tx.send(frame).await.unwrap();

        assert_no_frame(&handle0).await;
    }

    // --- Broadcast/multicast tests ---

    #[tokio::test]
    async fn broadcast_floods_to_all_other_ports_and_gateway() {
        let mut fabric: Fabric<TestPort> = Fabric::new();
        let (port0, handle0) = make_test_port();
        let (port1, handle1) = make_test_port();

        let (gw_tx, mut gw_rx) = tokio_mpsc::channel(64);
        let (_ingress_tx, ingress_rx) = tokio_mpsc::channel(64);
        fabric.set_gateway(gw_tx, ingress_rx);

        let (_id0, _task0) = fabric.add_port_raw(port0);
        let (_id1, _task1) = fabric.add_port_raw(port1);

        let frame = make_frame(BROADCAST, MAC_A, 0x0806, &[0u8; 10]);
        handle0.inject_tx.send(frame).await.unwrap();

        // Port 1 should receive the flooded frame.
        assert!(try_recv(&handle1).await.is_some(), "port 1 should get broadcast");

        // Gateway should also receive it.
        let gw_frame = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            gw_rx.recv(),
        )
        .await;
        assert!(gw_frame.is_ok() && gw_frame.unwrap().is_some(), "gateway should get broadcast");
    }

    #[tokio::test]
    async fn multicast_floods_to_all_other_ports_and_gateway() {
        let mut fabric: Fabric<TestPort> = Fabric::new();
        let (port0, handle0) = make_test_port();
        let (port1, handle1) = make_test_port();

        let (gw_tx, mut gw_rx) = tokio_mpsc::channel(64);
        let (_ingress_tx, ingress_rx) = tokio_mpsc::channel(64);
        fabric.set_gateway(gw_tx, ingress_rx);

        let (_id0, _task0) = fabric.add_port_raw(port0);
        let (_id1, _task1) = fabric.add_port_raw(port1);

        let frame = make_frame(MULTICAST, MAC_A, 0x0800, &[0u8; 10]);
        handle0.inject_tx.send(frame).await.unwrap();

        assert!(try_recv(&handle1).await.is_some(), "port 1 should get multicast");
        let gw_frame = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            gw_rx.recv(),
        )
        .await;
        assert!(gw_frame.is_ok() && gw_frame.unwrap().is_some(), "gateway should get multicast");
    }

    // --- Gateway routing tests ---

    #[tokio::test]
    async fn gateway_mac_dst_sent_to_gateway_only() {
        let mut fabric: Fabric<TestPort> = Fabric::new();
        let (port0, handle0) = make_test_port();
        let (port1, handle1) = make_test_port();

        let (gw_tx, mut gw_rx) = tokio_mpsc::channel(64);
        let (_ingress_tx, ingress_rx) = tokio_mpsc::channel(64);
        fabric.set_gateway(gw_tx, ingress_rx);

        let (_id0, _task0) = fabric.add_port_raw(port0);
        let (_id1, _task1) = fabric.add_port_raw(port1);

        let frame = make_frame(GATEWAY_MAC, MAC_A, 0x0800, &[0u8; 10]);
        handle0.inject_tx.send(frame).await.unwrap();

        // Gateway should receive it.
        let gw_frame = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            gw_rx.recv(),
        )
        .await;
        assert!(gw_frame.is_ok() && gw_frame.unwrap().is_some(), "gateway should get frame");

        // Port 1 should NOT receive it.
        assert_no_frame(&handle1).await;
    }

    // --- MAC learning tests ---

    #[tokio::test]
    async fn mac_learning_and_forwarding() {
        let mut fabric: Fabric<TestPort> = Fabric::new();
        let (port0, handle0) = make_test_port();
        let (port1, handle1) = make_test_port();
        let (port2, handle2) = make_test_port();

        let (_id0, _task0) = fabric.add_port_raw(port0);
        let (_id1, _task1) = fabric.add_port_raw(port1);
        let (_id2, _task2) = fabric.add_port_raw(port2);

        // Port 0 sends a frame with src=MAC_A (learn MAC_A on port 0).
        let frame1 = make_frame(BROADCAST, MAC_A, 0x0806, &[0u8; 10]);
        handle0.inject_tx.send(frame1).await.unwrap();

        // Drain the broadcast flood.
        let _ = try_recv(&handle1).await;
        let _ = try_recv(&handle2).await;

        // Port 1 sends a frame with dst=MAC_A → should go to port 0 only.
        let frame2 = make_frame(MAC_A, MAC_B, 0x0800, &[0u8; 10]);
        handle1.inject_tx.send(frame2).await.unwrap();

        assert!(try_recv(&handle0).await.is_some(), "port 0 should receive frame to MAC_A");
        assert_no_frame(&handle2).await;
    }

    #[tokio::test]
    async fn mac_migration() {
        let mut fabric: Fabric<TestPort> = Fabric::new();
        let (port0, handle0) = make_test_port();
        let (port1, handle1) = make_test_port();
        let (port2, handle2) = make_test_port();

        let (_id0, _task0) = fabric.add_port_raw(port0);
        let (_id1, _task1) = fabric.add_port_raw(port1);
        let (_id2, _task2) = fabric.add_port_raw(port2);

        // Learn MAC_A on port 0.
        let frame1 = make_frame(BROADCAST, MAC_A, 0x0806, &[0u8; 10]);
        handle0.inject_tx.send(frame1).await.unwrap();
        let _ = try_recv(&handle1).await;
        let _ = try_recv(&handle2).await;

        // Migrate MAC_A to port 1.
        let frame2 = make_frame(BROADCAST, MAC_A, 0x0806, &[0u8; 10]);
        handle1.inject_tx.send(frame2).await.unwrap();
        let _ = try_recv(&handle0).await;
        let _ = try_recv(&handle2).await;

        // Port 2 sends to MAC_A → should now go to port 1.
        let frame3 = make_frame(MAC_A, MAC_C, 0x0800, &[0u8; 10]);
        handle2.inject_tx.send(frame3).await.unwrap();

        assert!(try_recv(&handle1).await.is_some(), "port 1 should receive frame after migration");
        assert_no_frame(&handle0).await;
    }

    // --- Gateway ingress tests ---

    #[tokio::test]
    async fn gateway_ingress_known_unicast() {
        let mut fabric: Fabric<TestPort> = Fabric::new();
        let (port0, handle0) = make_test_port();
        let (port1, handle1) = make_test_port();

        let (gw_tx, _gw_rx) = tokio_mpsc::channel(64);
        let (ingress_tx, ingress_rx) = tokio_mpsc::channel(64);
        fabric.set_gateway(gw_tx, ingress_rx);

        let (_id0, _task0) = fabric.add_port_raw(port0);
        let (_id1, _task1) = fabric.add_port_raw(port1);

        // Learn MAC_A on port 0 by sending a frame from port 0.
        let frame_learn = make_frame(BROADCAST, MAC_A, 0x0806, &[0u8; 10]);
        handle0.inject_tx.send(frame_learn).await.unwrap();
        let _ = try_recv(&handle1).await;

        // Gateway sends a frame to MAC_A → should go to port 0 only.
        let gw_frame = make_frame(MAC_A, GATEWAY_MAC, 0x0800, &[0u8; 10]);
        ingress_tx.send(gw_frame).await.unwrap();

        assert!(try_recv(&handle0).await.is_some(), "port 0 should receive gateway ingress");
        assert_no_frame(&handle1).await;
    }

    #[tokio::test]
    async fn gateway_ingress_unknown_unicast_floods() {
        let mut fabric: Fabric<TestPort> = Fabric::new();
        let (port0, handle0) = make_test_port();
        let (port1, handle1) = make_test_port();

        let (gw_tx, _gw_rx) = tokio_mpsc::channel(64);
        let (ingress_tx, ingress_rx) = tokio_mpsc::channel(64);
        fabric.set_gateway(gw_tx, ingress_rx);

        let (_id0, _task0) = fabric.add_port_raw(port0);
        let (_id1, _task1) = fabric.add_port_raw(port1);

        // Gateway sends to unknown MAC_C → should flood to all ports.
        let gw_frame = make_frame(MAC_C, GATEWAY_MAC, 0x0800, &[0u8; 10]);
        ingress_tx.send(gw_frame).await.unwrap();

        assert!(try_recv(&handle0).await.is_some(), "port 0 should receive flooded frame");
        assert!(try_recv(&handle1).await.is_some(), "port 1 should receive flooded frame");
    }

    #[tokio::test]
    async fn gateway_ingress_broadcast_floods_to_all() {
        let mut fabric: Fabric<TestPort> = Fabric::new();
        let (port0, handle0) = make_test_port();
        let (port1, handle1) = make_test_port();

        let (gw_tx, _gw_rx) = tokio_mpsc::channel(64);
        let (ingress_tx, ingress_rx) = tokio_mpsc::channel(64);
        fabric.set_gateway(gw_tx, ingress_rx);

        let (_id0, _task0) = fabric.add_port_raw(port0);
        let (_id1, _task1) = fabric.add_port_raw(port1);

        let gw_frame = make_frame(BROADCAST, GATEWAY_MAC, 0x0806, &[0u8; 10]);
        ingress_tx.send(gw_frame).await.unwrap();

        assert!(try_recv(&handle0).await.is_some(), "port 0 should receive broadcast");
        assert!(try_recv(&handle1).await.is_some(), "port 1 should receive broadcast");
    }

    // --- Edge case tests ---

    #[tokio::test]
    async fn runt_frame_dropped() {
        let mut fabric: Fabric<TestPort> = Fabric::new();
        let (port0, handle0) = make_test_port();
        let (port1, handle1) = make_test_port();

        let (_id0, _task0) = fabric.add_port_raw(port0);
        let (_id1, _task1) = fabric.add_port_raw(port1);

        // Send a frame that is too short (< VNET_HDR_SZ + ETH_HEADER_LEN).
        let runt = vec![0u8; VNET_HDR_SZ + ETH_HEADER_LEN - 1];
        handle0.inject_tx.send(runt).await.unwrap();

        assert_no_frame(&handle1).await;
    }

    #[tokio::test]
    async fn flood_frame_with_empty_ports_no_panic() {
        let ports: Arc<Mutex<HashMap<PortId, SharedPort<TestPort>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let frame = make_frame(MAC_A, MAC_B, 0x0800, &[0u8; 10]);
        // Should not panic.
        flood_frame::<TestPort>(&frame, 0, &ports).await;
    }

    // --- Route-aware forwarding tests ---

    /// Build a valid IPv4 test frame with specific dst IP.
    /// Layout: [vnet_hdr(10)][eth_hdr(14)][ip_hdr(20)]
    fn make_ipv4_frame(dst_mac: [u8; 6], src_mac: [u8; 6], dst_ip: Ipv4Addr) -> Vec<u8> {
        let mut frame = vec![0u8; VNET_HDR_SZ]; // zeroed vnet header
        // Ethernet header
        frame.extend_from_slice(&dst_mac);
        frame.extend_from_slice(&src_mac);
        frame.extend_from_slice(&0x0800u16.to_be_bytes());
        // Minimal IP header (20 bytes)
        let mut ip_hdr = [0u8; 20];
        ip_hdr[0] = 0x45; // version=4, IHL=5
        // src IP at offset 12..16
        ip_hdr[12..16].copy_from_slice(&[10, 0, 0, 1]);
        // dst IP at offset 16..20
        let dst_octets = dst_ip.octets();
        ip_hdr[16..20].copy_from_slice(&dst_octets);
        frame.extend_from_slice(&ip_hdr);
        frame
    }

    /// Helper: try to receive a FabricEvent with timeout.
    async fn try_recv_event(rx: &mut tokio_mpsc::Receiver<FabricEvent>) -> Option<FabricEvent> {
        tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv())
            .await
            .ok()
            .flatten()
    }

    #[tokio::test]
    async fn placeholder_route_buffers_instead_of_flooding() {
        use distvirt_worker_protocol::{
            BufferPolicy, FabricRouteEntry, RouteDestination,
        };

        let mut fabric: Fabric<TestPort> = Fabric::new();
        let (event_tx, mut event_rx) = tokio_mpsc::channel(64);
        fabric.set_event_channel(event_tx);

        let pod_ip = Ipv4Addr::new(172, 16, 0, 10);

        // Add a placeholder route for pod_ip.
        {
            let rt_arc = fabric.route_table();
            let mut rt = rt_arc.lock().unwrap();
            rt.sync(vec![FabricRouteEntry {
                ip: pod_ip,
                mac: MAC_C,
                destination: RouteDestination::Placeholder {
                    buffer_policy: BufferPolicy {
                        hold_tcp_syn: false,
                        buffer_frames: 10,
                        timeout_ms: 5000,
                    },
                },
            }]);
        }

        let (port0, handle0) = make_test_port();
        let (port1, handle1) = make_test_port();

        let (_id0, _task0) = fabric.add_port_raw(port0);
        let (_id1, _task1) = fabric.add_port_raw(port1);

        // Port 0 sends an IPv4 frame to the placeholder IP with unknown dst MAC.
        let frame = make_ipv4_frame(MAC_C, MAC_A, pod_ip);
        handle0.inject_tx.send(frame).await.unwrap();

        // Port 1 should NOT receive the frame (it was buffered, not flooded).
        assert_no_frame(&handle1).await;

        // A route miss event should have been emitted.
        let event = try_recv_event(&mut event_rx).await;
        assert!(matches!(event, Some(FabricEvent::RouteMiss { dst_ip: ip, .. }) if ip == pod_ip));
    }

    #[tokio::test]
    async fn no_route_still_floods() {
        let mut fabric: Fabric<TestPort> = Fabric::new();

        let (port0, handle0) = make_test_port();
        let (port1, handle1) = make_test_port();
        let (port2, handle2) = make_test_port();

        let (_id0, _task0) = fabric.add_port_raw(port0);
        let (_id1, _task1) = fabric.add_port_raw(port1);
        let (_id2, _task2) = fabric.add_port_raw(port2);

        // Port 0 sends IPv4 frame to unknown MAC with no route entry.
        let pod_ip = Ipv4Addr::new(172, 16, 0, 99);
        let frame = make_ipv4_frame(MAC_C, MAC_A, pod_ip);
        handle0.inject_tx.send(frame).await.unwrap();

        // Both other ports should receive the flooded frame.
        assert!(try_recv(&handle1).await.is_some(), "port 1 should receive flooded frame");
        assert!(try_recv(&handle2).await.is_some(), "port 2 should receive flooded frame");
    }

    #[tokio::test]
    async fn buffered_frames_flushed_to_new_port() {
        use distvirt_worker_protocol::{
            BufferPolicy, FabricRouteEntry, RouteDestination,
        };

        let mut fabric: Fabric<TestPort> = Fabric::new();

        let pod_ip = Ipv4Addr::new(172, 16, 0, 10);

        // Add a placeholder route.
        {
            let rt_arc = fabric.route_table();
            let mut rt = rt_arc.lock().unwrap();
            rt.sync(vec![FabricRouteEntry {
                ip: pod_ip,
                mac: MAC_C,
                destination: RouteDestination::Placeholder {
                    buffer_policy: BufferPolicy {
                        hold_tcp_syn: false,
                        buffer_frames: 10,
                        timeout_ms: 5000,
                    },
                },
            }]);
        }

        let (port0, handle0) = make_test_port();
        let (_id0, _task0) = fabric.add_port_raw(port0);

        // Send 3 frames to the placeholder IP.
        for _ in 0..3 {
            let frame = make_ipv4_frame(MAC_C, MAC_A, pod_ip);
            handle0.inject_tx.send(frame).await.unwrap();
        }

        // Let the port read loop process the frames.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Now add a new port "for" that IP — buffered frames should be flushed to it.
        let (port_new, handle_new) = make_test_port();
        let (_id_new, _task_new) = fabric.add_port_raw_with_ip(port_new, pod_ip);

        // The new port should receive the 3 buffered frames.
        for i in 0..3 {
            let frame = try_recv(&handle_new).await;
            assert!(frame.is_some(), "new port should receive buffered frame {}", i);
        }
    }

    #[tokio::test]
    async fn route_miss_debounced_on_rapid_frames() {
        use distvirt_worker_protocol::{
            BufferPolicy, FabricRouteEntry, RouteDestination,
        };

        let mut fabric: Fabric<TestPort> = Fabric::new();
        let (event_tx, mut event_rx) = tokio_mpsc::channel(64);
        fabric.set_event_channel(event_tx);

        let pod_ip = Ipv4Addr::new(172, 16, 0, 10);

        {
            let rt_arc = fabric.route_table();
            let mut rt = rt_arc.lock().unwrap();
            rt.sync(vec![FabricRouteEntry {
                ip: pod_ip,
                mac: MAC_C,
                destination: RouteDestination::Placeholder {
                    buffer_policy: BufferPolicy {
                        hold_tcp_syn: false,
                        buffer_frames: 100,
                        timeout_ms: 5000,
                    },
                },
            }]);
        }

        let (port0, handle0) = make_test_port();
        let (_id0, _task0) = fabric.add_port_raw(port0);

        // Send multiple frames rapidly.
        for _ in 0..5 {
            let frame = make_ipv4_frame(MAC_C, MAC_A, pod_ip);
            handle0.inject_tx.send(frame).await.unwrap();
        }

        // Wait for processing.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Should get exactly one miss event (debounced).
        let event1 = try_recv_event(&mut event_rx).await;
        assert!(event1.is_some(), "should get one route miss event");

        // No second event within debounce window.
        let event2 = try_recv_event(&mut event_rx).await;
        assert!(event2.is_none(), "second miss should be debounced");
    }
}
