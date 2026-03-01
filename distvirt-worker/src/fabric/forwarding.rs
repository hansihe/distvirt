use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use distvirt_activator::types::Action;
use super::route::RouteAction;
use super::service::ServiceAction;
use super::switch::{GATEWAY_MAC, MacTable, VNET_HDR_SZ, extract_ipv4_dst, format_mac, is_broadcast, parse_ethernet_header};
use super::port::{FramePort, PortId};
use super::{FabricEvent, SharedPort, convert_backend_need, handle_log_action};

/// RAII guard that removes a port from the fabric's port map on drop.
///
/// Created at the top of each port read loop. Guarantees cleanup whether the
/// task exits normally, errors out, or is aborted.
pub(super) struct PortGuard<P: FramePort> {
    pub(super) port_id: PortId,
    pub(super) ports: Arc<Mutex<HashMap<PortId, SharedPort<P>>>>,
}

impl<P: FramePort> Drop for PortGuard<P> {
    fn drop(&mut self) {
        let mut ports = self.ports.lock().unwrap();
        ports.remove(&self.port_id);
        log::info!("fabric: port {} removed (guard dropped)", self.port_id);
    }
}

/// Per-port read loop: reads frames, learns MACs, responds to gateway ARP,
/// and forwards/floods frames to other ports.
pub(super) async fn port_read_loop<P: FramePort>(
    port_id: PortId,
    port: SharedPort<P>,
    ports: Arc<Mutex<HashMap<PortId, SharedPort<P>>>>,
    mac_table: Arc<Mutex<MacTable>>,
    route_table: Arc<Mutex<super::RouteTable>>,
    service_table: Arc<Mutex<super::ServiceTable>>,
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
            // Check if this is an ARP request for a service IP and reply.
            try_service_arp_reply(frame, &service_table, &port).await;
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
                // Unknown unicast: consult service table, then route table.
                handle_unknown_unicast(
                    frame, dst_mac, port_id, &ports, &mac_table, &route_table,
                    &service_table, &event_tx,
                ).await;
            }
        }
    }
}

/// Task that reads frames from the gateway ingress channel and forwards them
/// into the fabric via MAC lookup or flooding.
pub(super) async fn gateway_ingress_task<P: FramePort>(
    mut ingress_rx: mpsc::Receiver<Vec<u8>>,
    ports: Arc<Mutex<HashMap<PortId, SharedPort<P>>>>,
    mac_table: Arc<Mutex<MacTable>>,
    route_table: Arc<Mutex<super::RouteTable>>,
    service_table: Arc<Mutex<super::ServiceTable>>,
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
                // Unknown unicast: consult service table, then route table.
                handle_unknown_unicast(
                    &frame, dst_mac, PortId::MAX, &ports, &mac_table, &route_table,
                    &service_table, &event_tx,
                ).await;
            }
        }
    }

    log::info!("fabric: gateway ingress task ended");
}

/// Dispatch a single activator action: replay packets, set backend need, or log.
///
/// Shared by `handle_unknown_unicast` and `Fabric::execute_service_actions`.
pub(super) async fn dispatch_action<P: FramePort>(
    action: &Action,
    service_id: &str,
    dst_ip: Ipv4Addr,
    service_table: &Mutex<super::ServiceTable>,
    mac_table: &Mutex<MacTable>,
    ports: &Mutex<HashMap<PortId, SharedPort<P>>>,
    event_tx: &Option<mpsc::Sender<FabricEvent>>,
) {
    match action {
        Action::ReplayPacket(raw_frame) => {
            let backend_mac = {
                let st = service_table.lock().unwrap();
                st.get_backend_mac_by_id(service_id)
            };
            if let Some(pod_mac) = backend_mac {
                let dst_port = {
                    let mt = mac_table.lock().unwrap();
                    if let Some(port_id) = mt.lookup(&pod_mac) {
                        let p = ports.lock().unwrap();
                        p.get(&port_id).cloned()
                    } else {
                        None
                    }
                };
                if let Some(dst_port) = dst_port {
                    let mut rewritten = raw_frame.clone();
                    if rewritten.len() >= VNET_HDR_SZ + 6 {
                        rewritten[VNET_HDR_SZ..VNET_HDR_SZ + 6]
                            .copy_from_slice(&pod_mac);
                    }
                    if let Err(e) = dst_port.send_frame(&rewritten).await {
                        log::warn!("fabric: replay send error: {}", e);
                    }
                }
            }
        }
        Action::SetBackendNeed(need) => {
            let proto_need = convert_backend_need(need);
            if let Some(tx) = event_tx {
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
        Action::PacketDecision { .. } => {
            // Decision already handled by activator internally.
        }
        _ => {
            log::debug!("fabric: unhandled activator action in forwarding path");
        }
    }
}

/// Send L4 outgoing Ethernet frames (no vnet header) to the appropriate ports.
fn send_l4_frames<P: FramePort>(
    frames: &[Vec<u8>],
    mac_table: &Arc<Mutex<MacTable>>,
    ports: &Arc<Mutex<HashMap<PortId, SharedPort<P>>>>,
) {
    if frames.is_empty() {
        return;
    }
    let mac_table_ref = mac_table.lock().unwrap();
    let ports_ref = ports.lock().unwrap();
    for eth_frame in frames {
        if eth_frame.len() < 6 {
            continue;
        }
        let frame_dst_mac: [u8; 6] = eth_frame[0..6].try_into().unwrap();
        if let Some(port_id) = mac_table_ref.lookup(&frame_dst_mac) {
            if let Some(port) = ports_ref.get(&port_id) {
                let port = Arc::clone(port);
                let mut vnet_frame = vec![0u8; VNET_HDR_SZ];
                vnet_frame.extend_from_slice(eth_frame);
                tokio::spawn(async move {
                    if let Err(e) = port.send_frame(&vnet_frame).await {
                        log::warn!("fabric: L4 frame send error: {}", e);
                    }
                });
            }
        }
    }
}

/// Schedule a smoltcp poll timer for a service IP.
///
/// Spawns a tokio task that sleeps for `delay`, then calls
/// `handle_timeout_for_ip` on the service table to drive TCP state machine
/// timers (retransmissions, TIME_WAIT cleanup, FIN/FIN-ACK, etc.).
fn schedule_poll_timer<P: FramePort>(
    delay: Duration,
    ip: Ipv4Addr,
    service_table: Arc<Mutex<super::ServiceTable>>,
    mac_table: Arc<Mutex<MacTable>>,
    ports: Arc<Mutex<HashMap<PortId, SharedPort<P>>>>,
    event_tx: Option<mpsc::Sender<FabricEvent>>,
) {
    tokio::spawn(async move {
        tokio::time::sleep(delay).await;

        let result = {
            let mut st = service_table.lock().unwrap();
            st.handle_timeout_for_ip(ip)
        };

        if let Some(ServiceAction::L4Result { actions, frames, service_id, poll_delay }) = result {
            send_l4_frames(&frames, &mac_table, &ports);

            for action in &actions {
                dispatch_action(
                    action, &service_id, ip,
                    &service_table, &mac_table, &ports, &event_tx,
                ).await;
            }

            // Reschedule if smoltcp still needs polling.
            if let Some(next_delay) = poll_delay {
                schedule_poll_timer(
                    next_delay, ip,
                    service_table, mac_table, ports, event_tx,
                );
            }
        }
    });
}

/// Handle unknown unicast: consult service table first, then route table for IPv4 frames,
/// otherwise flood as before.
async fn handle_unknown_unicast<P: FramePort>(
    frame: &[u8],
    dst_mac: [u8; 6],
    src_port_id: PortId,
    ports: &Arc<Mutex<HashMap<PortId, SharedPort<P>>>>,
    mac_table: &Arc<Mutex<MacTable>>,
    route_table: &Arc<Mutex<super::RouteTable>>,
    service_table: &Arc<Mutex<super::ServiceTable>>,
    event_tx: &Option<mpsc::Sender<FabricEvent>>,
) {
    // Try to extract IPv4 destination from the frame (skip vnet header).
    let eth_frame = &frame[VNET_HDR_SZ..];
    if let Some(dst_ip) = extract_ipv4_dst(eth_frame) {
        // 1. Check service table first.
        let svc_result = {
            let mut st = service_table.lock().unwrap();
            st.lookup_and_buffer(dst_ip, frame)
        };

        if let Some((svc_action, should_activate)) = svc_result {
            // Get service_id for activation event (re-lock briefly).
            let service_id = if should_activate {
                let st = service_table.lock().unwrap();
                st.get_service_id(&dst_ip).map(String::from)
            } else {
                None
            };

            match svc_action {
                ServiceAction::Forward { pod_ip: _, pod_mac } => {
                    // Find port for backend MAC, rewrite dst MAC, send.
                    let dst_port = {
                        let mt = mac_table.lock().unwrap();
                        if let Some(port_id) = mt.lookup(&pod_mac) {
                            let p = ports.lock().unwrap();
                            p.get(&port_id).cloned()
                        } else {
                            None
                        }
                    };
                    if let Some(dst_port) = dst_port {
                        let mut rewritten = frame.to_vec();
                        if rewritten.len() >= VNET_HDR_SZ + 6 {
                            rewritten[VNET_HDR_SZ..VNET_HDR_SZ + 6].copy_from_slice(&pod_mac);
                        }
                        if let Err(e) = dst_port.send_frame(&rewritten).await {
                            log::warn!("fabric: service forward error: {}", e);
                        }
                    } else {
                        log::debug!(
                            "fabric: service forward to {} but backend MAC not in mac_table",
                            format_mac(&pod_mac)
                        );
                    }
                }
                ServiceAction::Buffered => {
                    log::trace!("fabric: frame to service {} buffered", dst_ip);
                }
                ServiceAction::Drop => {
                    log::trace!("fabric: frame to service {} dropped", dst_ip);
                }
                ServiceAction::ActivatorActions { actions, service_id } => {
                    for action in actions {
                        dispatch_action(
                            &action, &service_id, dst_ip,
                            service_table, mac_table, ports, event_tx,
                        ).await;
                    }
                }
                ServiceAction::L4Result { actions, frames, service_id, poll_delay } => {
                    // Send outgoing L4 frames.
                    send_l4_frames(&frames, mac_table, ports);
                    // Handle non-L4 actions (SetBackendNeed, Log, etc.)
                    for action in actions {
                        dispatch_action(
                            &action, &service_id, dst_ip,
                            service_table, mac_table, ports, event_tx,
                        ).await;
                    }
                    // Schedule smoltcp timer if needed.
                    if let Some(delay) = poll_delay {
                        schedule_poll_timer(
                            delay, dst_ip,
                            Arc::clone(service_table),
                            Arc::clone(mac_table),
                            Arc::clone(ports),
                            event_tx.clone(),
                        );
                    }
                }
            }

            if let Some(service_id) = service_id {
                if let Some(tx) = event_tx {
                    let _ = tx.try_send(FabricEvent::ServiceActivation {
                        service_id,
                        dst_ip,
                    });
                }
            }

            return;
        }

        // 2. Fall through to route table.
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

/// Check if a broadcast frame is an ARP request for a service IP. If so,
/// construct and send an ARP reply back to the source port.
async fn try_service_arp_reply<P: FramePort>(
    frame: &[u8],
    service_table: &Arc<Mutex<super::ServiceTable>>,
    src_port: &SharedPort<P>,
) {
    let eth_frame = &frame[VNET_HDR_SZ..];
    // Minimum ARP frame: 14 (eth header) + 28 (ARP for IPv4) = 42.
    if eth_frame.len() < 42 {
        return;
    }
    let ethertype = u16::from_be_bytes([eth_frame[12], eth_frame[13]]);
    if ethertype != 0x0806 {
        return;
    }
    // ARP operation at offset 20-21 (relative to eth_frame start at 14+6=20).
    let arp = &eth_frame[14..];
    let op = u16::from_be_bytes([arp[6], arp[7]]);
    if op != 1 {
        return; // Not a request.
    }
    // Target protocol address at ARP offset 24..28.
    let target_ip = Ipv4Addr::new(arp[24], arp[25], arp[26], arp[27]);

    let service_mac = {
        let st = service_table.lock().unwrap();
        st.get_mac(&target_ip)
    };

    let service_mac = match service_mac {
        Some(mac) => mac,
        None => return,
    };

    // Build ARP reply.
    let sender_mac: [u8; 6] = eth_frame[6..12].try_into().unwrap();
    let sender_ip: [u8; 4] = arp[14..18].try_into().unwrap();

    let mut reply = vec![0u8; VNET_HDR_SZ]; // vnet header (zeroed)
    // Ethernet header: dst=sender_mac, src=service_mac, ethertype=0x0806.
    reply.extend_from_slice(&sender_mac);
    reply.extend_from_slice(&service_mac);
    reply.extend_from_slice(&0x0806u16.to_be_bytes());
    // ARP payload.
    let mut arp_reply = [0u8; 28];
    arp_reply[0..2].copy_from_slice(&[0x00, 0x01]); // hardware type: Ethernet
    arp_reply[2..4].copy_from_slice(&[0x08, 0x00]); // protocol type: IPv4
    arp_reply[4] = 6; // hardware size
    arp_reply[5] = 4; // protocol size
    arp_reply[6..8].copy_from_slice(&[0x00, 0x02]); // operation: reply
    arp_reply[8..14].copy_from_slice(&service_mac); // sender hardware address
    arp_reply[14..18].copy_from_slice(&target_ip.octets()); // sender protocol address
    arp_reply[18..24].copy_from_slice(&sender_mac); // target hardware address
    arp_reply[24..28].copy_from_slice(&sender_ip); // target protocol address
    reply.extend_from_slice(&arp_reply);

    if let Err(e) = src_port.send_frame(&reply).await {
        log::warn!("fabric: service ARP reply send error: {}", e);
    } else {
        log::debug!(
            "fabric: sent ARP reply for service IP {} (MAC {})",
            target_ip,
            format_mac(&service_mac)
        );
    }
}

/// Send a frame to all ports except the source port.
pub(super) async fn flood_frame<P: FramePort>(
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
