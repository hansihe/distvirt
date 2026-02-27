pub mod gateway;
pub mod port;
pub mod switch;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::tap::TapDevice;
use port::{Port, PortId};
use switch::{GATEWAY_MAC, MacTable, VNET_HDR_SZ, format_mac, is_broadcast, parse_ethernet_header};

/// Shared port handle that can be used by any reader task to send frames.
type SharedPort = Arc<Port>;

/// The L2 fabric switch.
///
/// Manages a set of ports (TAP devices) and switches Ethernet frames between
/// them. Each port runs a tokio task that reads frames and performs MAC learning,
/// ARP responding, and forwarding inline.
pub struct Fabric {
    ports: Arc<Mutex<HashMap<PortId, SharedPort>>>,
    mac_table: Arc<Mutex<MacTable>>,
    tasks: HashMap<PortId, JoinHandle<()>>,
    next_port_id: PortId,
    gateway_tx: Option<mpsc::Sender<Vec<u8>>>,
}

impl Fabric {
    /// Create a new empty fabric.
    pub fn new() -> Self {
        Fabric {
            ports: Arc::new(Mutex::new(HashMap::new())),
            mac_table: Arc::new(Mutex::new(MacTable::new())),
            tasks: HashMap::new(),
            next_port_id: 0,
            gateway_tx: None,
        }
    }

    /// Add a TAP device as a port and start its forwarding task.
    ///
    /// Returns the assigned port ID.
    pub fn add_port(&mut self, tap: TapDevice) -> std::io::Result<PortId> {
        let port_id = self.next_port_id;
        self.next_port_id += 1;

        let port = Arc::new(Port::new(tap)?);

        {
            let mut ports = self.ports.lock().unwrap();
            ports.insert(port_id, Arc::clone(&port));
        }

        let task = tokio::spawn(port_read_loop(
            port_id,
            Arc::clone(&port),
            Arc::clone(&self.ports),
            Arc::clone(&self.mac_table),
            self.gateway_tx.clone(),
        ));

        self.tasks.insert(port_id, task);

        log::info!("fabric: added port {}", port_id);
        Ok(port_id)
    }

    /// Remove a port and cancel its forwarding task.
    pub fn remove_port(&mut self, port_id: PortId) {
        if let Some(task) = self.tasks.remove(&port_id) {
            task.abort();
        }
        let mut ports = self.ports.lock().unwrap();
        ports.remove(&port_id);
        log::info!("fabric: removed port {}", port_id);
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

        tokio::spawn(gateway_ingress_task(ingress_rx, ports, mac_table));

        log::info!("fabric: gateway connected");
    }
}

impl Drop for Fabric {
    fn drop(&mut self) {
        for (_, task) in self.tasks.drain() {
            task.abort();
        }
    }
}

/// Per-port read loop: reads frames, learns MACs, responds to gateway ARP,
/// and forwards/floods frames to other ports.
async fn port_read_loop(
    port_id: PortId,
    port: SharedPort,
    ports: Arc<Mutex<HashMap<PortId, SharedPort>>>,
    mac_table: Arc<Mutex<MacTable>>,
    gateway_tx: Option<mpsc::Sender<Vec<u8>>>,
) {
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
                // Unknown unicast: flood to all other ports.
                flood_frame(frame, port_id, &ports).await;
            }
        }
    }
}

/// Task that reads frames from the gateway ingress channel and forwards them
/// into the fabric via MAC lookup or flooding.
async fn gateway_ingress_task(
    mut ingress_rx: mpsc::Receiver<Vec<u8>>,
    ports: Arc<Mutex<HashMap<PortId, SharedPort>>>,
    mac_table: Arc<Mutex<MacTable>>,
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
                // Unknown unicast: flood.
                flood_frame(&frame, PortId::MAX, &ports).await;
            }
        }
    }

    log::info!("fabric: gateway ingress task ended");
}

/// Send a frame to all ports except the source port.
async fn flood_frame(
    frame: &[u8],
    src_port_id: PortId,
    ports: &Arc<Mutex<HashMap<PortId, SharedPort>>>,
) {
    let targets: Vec<(PortId, SharedPort)> = {
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
