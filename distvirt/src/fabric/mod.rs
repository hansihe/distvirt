pub mod port;
pub mod switch;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::task::JoinHandle;

use crate::tap::TapDevice;
use port::{Port, PortId};
use switch::{
    MacTable, build_arp_reply, format_mac, is_arp_request_for_gateway, is_broadcast,
    parse_ethernet_header,
};

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
}

impl Fabric {
    /// Create a new empty fabric.
    pub fn new() -> Self {
        Fabric {
            ports: Arc::new(Mutex::new(HashMap::new())),
            mac_table: Arc::new(Mutex::new(MacTable::new())),
            tasks: HashMap::new(),
            next_port_id: 0,
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
) {
    let mut buf = vec![0u8; 1514]; // max Ethernet frame

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

        let frame = &buf[..n];

        let (dst_mac, src_mac, ethertype) = match parse_ethernet_header(frame) {
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

        // Handle ARP requests for the gateway.
        if is_arp_request_for_gateway(frame) {
            log::debug!(
                "fabric: port {} ARP request for gateway from {}",
                port_id,
                format_mac(&src_mac),
            );
            if let Some(reply) = build_arp_reply(frame) {
                if let Err(e) = port.send_frame(&reply).await {
                    log::warn!("fabric: port {} ARP reply send error: {}", port_id, e);
                }
            }
            continue;
        }

        // Forward or flood.
        if is_broadcast(&dst_mac) || dst_mac[0] & 0x01 != 0 {
            // Broadcast/multicast: flood to all other ports.
            flood_frame(frame, port_id, &ports).await;
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
