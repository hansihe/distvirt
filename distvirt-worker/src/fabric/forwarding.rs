use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use distvirt_activator::types::Action;
use super::route::RouteAction;
use super::service::ServiceAction;
use super::nat::{NatEntry, NatFlowKey};
use super::switch::{FabricFrame, GATEWAY_MAC, MacTable, VNET_HDR_SZ, extract_ip_protocol, extract_ipv4_dst, extract_ipv4_src, extract_transport_ports, format_mac, is_broadcast, rewrite_dst_mac, rewrite_ipv4_dst, rewrite_ipv4_src, rewrite_src_mac, with_vnet_header};
use super::port::{FramePort, PortId};
use super::{FabricEvent, SharedPort, convert_backend_need, handle_log_action};

/// Shared fabric tables wrapped in a single Arc.
pub(crate) struct FabricContextInner<P: FramePort> {
    pub(crate) ports: Mutex<HashMap<PortId, SharedPort<P>>>,
    pub(crate) mac_table: Mutex<MacTable>,
    pub(crate) route_table: Mutex<super::RouteTable>,
    pub(crate) service_table: Mutex<super::ServiceTable>,
    pub(crate) nat_table: Mutex<super::nat::NatTable>,
}

impl<P: FramePort> FabricContextInner<P> {
    /// Resolve a MAC address to the port that owns it.
    ///
    /// Locks `mac_table` then `ports`; returns `None` if the MAC is unknown
    /// or the port has been removed.
    pub(crate) fn resolve_mac(&self, mac: &[u8; 6]) -> Option<SharedPort<P>> {
        let port_id = self.mac_table.lock().unwrap().lookup(mac)?;
        self.ports.lock().unwrap().get(&port_id).cloned()
    }
}

/// Shared fabric state passed to all forwarding functions.
///
/// The four table mutexes live behind a single `Arc` to reduce heap
/// allocations and reference-count bumps on clone. The sender fields
/// stay outside since they're set once before sharing and are cheap
/// to clone (each is a single internal Arc bump).
pub(super) struct FabricContext<P: FramePort> {
    pub(super) inner: Arc<FabricContextInner<P>>,
    pub(super) gateway_tx: Option<mpsc::Sender<Vec<u8>>>,
    pub(super) event_tx: Option<mpsc::Sender<FabricEvent>>,
}

impl<P: FramePort> Clone for FabricContext<P> {
    fn clone(&self) -> Self {
        FabricContext {
            inner: Arc::clone(&self.inner),
            gateway_tx: self.gateway_tx.clone(),
            event_tx: self.event_tx.clone(),
        }
    }
}

/// RAII guard that removes a port from the fabric's port map on drop.
///
/// Created at the top of each port read loop. Guarantees cleanup whether the
/// task exits normally, errors out, or is aborted.
pub(super) struct PortGuard<P: FramePort> {
    pub(super) port_id: PortId,
    pub(super) inner: Arc<FabricContextInner<P>>,
}

impl<P: FramePort> Drop for PortGuard<P> {
    fn drop(&mut self) {
        let mut ports = self.inner.ports.lock().unwrap();
        ports.remove(&self.port_id);
        log::info!("fabric: port {} removed (guard dropped)", self.port_id);
    }
}

/// Source of a frame being dispatched through the fabric.
enum FrameSource<'a, P: FramePort> {
    /// Frame from a local port: does MAC learning, gateway forwarding, ARP replies.
    Port { port_id: PortId, port: &'a SharedPort<P> },
    /// Frame from the gateway: no MAC learning, no gateway forwarding.
    Gateway,
}

/// Common frame dispatch logic shared by port read loops and gateway ingress.
///
/// Handles parsing, optional MAC learning, broadcast/multicast flooding,
/// gateway-MAC forwarding, unicast lookup with loopback avoidance, and
/// unknown-unicast fallback (service table, route table, flood).
async fn dispatch_frame<P: FramePort>(
    frame: &[u8],
    source: FrameSource<'_, P>,
    ctx: &FabricContext<P>,
) {
    let ff = match FabricFrame::new(frame) {
        Some(f) => f,
        None => return, // runt frame
    };
    let dst_mac = ff.dst_mac();
    let src_mac = ff.src_mac();

    let src_port_id = match &source {
        FrameSource::Port { port_id, .. } => *port_id,
        FrameSource::Gateway => PortId::MAX,
    };

    let source_label = match &source {
        FrameSource::Port { port_id, .. } => format!("port {}", port_id),
        FrameSource::Gateway => "gateway".to_string(),
    };

    if ff.ethertype() == 0x0800 {
        let eth = ff.eth_payload();
        let src_ip = extract_ipv4_src(eth);
        let dst_ip = extract_ipv4_dst(eth);
        log::debug!(
            "fabric: dispatch_frame from {} | {} -> {} IPv4 {:?} -> {:?} len={}",
            source_label, format_mac(&src_mac), format_mac(&dst_mac),
            src_ip, dst_ip, frame.len()
        );
    } else {
        log::trace!(
            "fabric: dispatch_frame from {} | {} -> {} ethertype=0x{:04x} len={}",
            source_label, format_mac(&src_mac), format_mac(&dst_mac),
            ff.ethertype(), frame.len()
        );
    }

    if let FrameSource::Port { port_id, .. } = &source {
        // Learn source MAC.
        let mut table = ctx.inner.mac_table.lock().unwrap();
        table.learn(src_mac, *port_id);
    }

    // Forward or flood.
    if is_broadcast(&dst_mac) || dst_mac[0] & 0x01 != 0 {
        // Broadcast/multicast: flood to all other ports.
        let num_ports = ctx.inner.ports.lock().unwrap().len();
        log::trace!("fabric: -> BROADCAST/MULTICAST flood (num_ports={})", num_ports);
        flood_frame(frame, src_port_id, &ctx.inner.ports).await;
        // Port sources also forward broadcasts to the gateway and check service ARP.
        if let FrameSource::Port { port, .. } = &source {
            if let Some(ref gw_tx) = ctx.gateway_tx {
                let _ = gw_tx.try_send(frame.to_vec());
            }
            try_service_arp_reply(frame, &ctx.inner.service_table, port).await;
        }
    } else if matches!(&source, FrameSource::Port { .. }) && dst_mac == GATEWAY_MAC {
        log::trace!("fabric: -> GATEWAY (dst_mac matches GATEWAY_MAC)");
        // Gateway-destined frame from a port: send to gateway via channel.
        if let Some(ref gw_tx) = ctx.gateway_tx {
            let _ = gw_tx.try_send(frame.to_vec());
        }
    } else {
        // Unicast lookup with loopback avoidance.
        // Gateway source uses PortId::MAX which never matches any real port.
        let dst_port_id = ctx.inner.mac_table.lock().unwrap().lookup(&dst_mac);

        log::trace!("fabric: -> UNICAST lookup dst_mac={} result={:?} src_port={}", format_mac(&dst_mac), dst_port_id, src_port_id);

        if let Some(dst_id) = dst_port_id {
            if dst_id != src_port_id {
                // Check NAT table for SNAT (return traffic from backend).
                let nat_match = {
                    let ff_ref = FabricFrame::new(frame).unwrap();
                    let eth = ff_ref.eth_payload();
                    let src_ip = extract_ipv4_src(eth);
                    let protocol = extract_ip_protocol(eth);
                    let ports = extract_transport_ports(eth);
                    let dst_ip_val = extract_ipv4_dst(eth);

                    if let (Some(s_ip), Some(d_ip), Some(proto)) = (src_ip, dst_ip_val, protocol) {
                        let (s_port, d_port) = ports.unwrap_or((0, 0));
                        let key = NatFlowKey {
                            src_ip: s_ip,
                            dst_ip: d_ip,
                            protocol: proto,
                            src_port: s_port,
                            dst_port: d_port,
                        };
                        let mut nat = ctx.inner.nat_table.lock().unwrap();
                        nat.lookup(&key).map(|e| (e.service_ip, e.service_mac))
                    } else {
                        None
                    }
                };

                let dst_port = ctx.inner.ports.lock().unwrap().get(&dst_id).cloned();
                if let Some(dst_port) = dst_port {
                    if let Some((svc_ip, svc_mac)) = nat_match {
                        // SNAT: rewrite src IP and src MAC from backend to service.
                        let ff_ref = FabricFrame::new(frame).unwrap();
                        let backend_ip = ff_ref.ipv4_src().unwrap();
                        log::debug!(
                            "fabric: SNAT return traffic {} -> {} (rewriting src {} -> {})",
                            src_port_id, dst_id, backend_ip, svc_ip
                        );
                        let mut rewritten = frame.to_vec();
                        rewrite_src_mac(&mut rewritten, &svc_mac);
                        rewrite_ipv4_src(&mut rewritten, backend_ip, svc_ip);
                        if let Err(e) = dst_port.send_frame(&rewritten).await {
                            log::warn!(
                                "fabric: send {} -> {} (SNAT) error: {}",
                                src_port_id, dst_id, e
                            );
                        }
                    } else {
                        log::trace!("fabric: -> forwarding to port {}", dst_id);
                        if let Err(e) = dst_port.send_frame(frame).await {
                            log::warn!(
                                "fabric: send {} -> {} error: {}",
                                src_port_id, dst_id, e
                            );
                        }
                    }
                }
            } else {
                log::trace!("fabric: -> LOOPBACK avoidance (src_port == dst_port = {})", dst_id);
            }
        } else {
            // Unknown unicast: consult service table, then route table.
            handle_unknown_unicast(frame, dst_mac, src_port_id, ctx).await;
        }
    }
}

/// Per-port read loop: reads frames and dispatches them through the fabric.
pub(super) async fn port_read_loop<P: FramePort>(
    port_id: PortId,
    port: SharedPort<P>,
    ctx: FabricContext<P>,
) {
    // PortGuard removes this port from the map when this task exits for any reason.
    let _guard = PortGuard {
        port_id,
        inner: Arc::clone(&ctx.inner),
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
        dispatch_frame(frame, FrameSource::Port { port_id, port: &port }, &ctx).await;
    }
}

/// Task that reads frames from the gateway ingress channel and forwards them
/// into the fabric via MAC lookup or flooding.
pub(super) async fn gateway_ingress_task<P: FramePort>(
    mut ingress_rx: mpsc::Receiver<Vec<u8>>,
    ctx: FabricContext<P>,
) {
    while let Some(frame) = ingress_rx.recv().await {
        if frame.len() < VNET_HDR_SZ {
            continue;
        }
        dispatch_frame(&frame, FrameSource::Gateway, &ctx).await;
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
    ctx: &FabricContext<P>,
) {
    match action {
        Action::ReplayPacket(raw_frame) => {
            log::debug!(
                "fabric: dispatching ReplayPacket for service '{}' (frame_len={})",
                service_id, raw_frame.len()
            );
            let nat_info = {
                let st = ctx.inner.service_table.lock().unwrap();
                st.get_nat_info_by_id(service_id)
            };
            if let Some((service_ip, service_mac, backend_ip, pod_mac)) = nat_info {
                if let Some(dst_port) = ctx.inner.resolve_mac(&pod_mac) {
                    let mut rewritten = raw_frame.clone();
                    if rewritten.len() >= VNET_HDR_SZ + 6 {
                        rewrite_dst_mac(&mut rewritten, &pod_mac);
                    }
                    // DNAT: rewrite dst IP from service_ip to backend_ip.
                    rewrite_ipv4_dst(&mut rewritten, service_ip, backend_ip);

                    // Insert reverse NAT entry.
                    if let Some(ff_rw) = FabricFrame::new(&rewritten) {
                        if let Some(src_ip) = ff_rw.ipv4_src() {
                            let eth = ff_rw.eth_payload();
                            let protocol = extract_ip_protocol(eth).unwrap_or(0);
                            let (src_port, dst_port_val) = extract_transport_ports(eth).unwrap_or((0, 0));
                            let reverse_key = NatFlowKey {
                                src_ip: backend_ip,
                                dst_ip: src_ip,
                                protocol,
                                src_port: dst_port_val,
                                dst_port: src_port,
                            };
                            let nat_entry = NatEntry {
                                service_ip,
                                service_mac,
                                backend_ip,
                                last_seen: std::time::Instant::now(),
                            };
                            ctx.inner.nat_table.lock().unwrap().insert(reverse_key, nat_entry);
                        }
                    }

                    if let Err(e) = dst_port.send_frame(&rewritten).await {
                        log::warn!("fabric: replay send error: {}", e);
                    } else {
                        log::debug!(
                            "fabric: ReplayPacket sent to backend MAC {} (DNAT {} -> {})",
                            format_mac(&pod_mac), service_ip, backend_ip
                        );
                    }
                } else {
                    log::warn!(
                        "fabric: ReplayPacket for service '{}': backend MAC {} not in mac_table, dropping",
                        service_id, format_mac(&pod_mac)
                    );
                }
            } else {
                log::warn!(
                    "fabric: ReplayPacket for service '{}': no NAT info (backend not set?), dropping",
                    service_id
                );
            }
        }
        Action::SetBackendNeed(need) => {
            let proto_need = convert_backend_need(need);
            if let Some(tx) = &ctx.event_tx {
                if let Err(e) = tx.send(FabricEvent::ServiceBackendNeed {
                    service_id: service_id.to_string(),
                    dst_ip,
                    need: proto_need,
                }).await {
                    log::warn!("fabric: failed to send ServiceBackendNeed for {}: {}", service_id, e);
                }
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
/// Prepends a zeroed vnet header before sending each frame.
pub(super) fn send_l4_frames<P: FramePort>(
    frames: &[Vec<u8>],
    ctx: &FabricContext<P>,
) {
    if frames.is_empty() {
        return;
    }
    for eth_frame in frames {
        if eth_frame.len() < 6 {
            continue;
        }
        let frame_dst_mac: [u8; 6] = eth_frame[0..6].try_into().unwrap();
        if let Some(port) = ctx.inner.resolve_mac(&frame_dst_mac) {
            let vnet_frame = with_vnet_header(eth_frame);
            tokio::spawn(async move {
                if let Err(e) = port.send_frame(&vnet_frame).await {
                    log::warn!("fabric: L4 frame send error: {}", e);
                }
            });
        } else {
            log::debug!(
                "fabric: send_l4_frame: dst MAC {} not in mac_table",
                format_mac(&frame_dst_mac)
            );
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
    ctx: FabricContext<P>,
) {
    tokio::spawn(async move {
        tokio::time::sleep(delay).await;

        let result = {
            let mut st = ctx.inner.service_table.lock().unwrap();
            st.handle_timeout_for_ip(ip)
        };

        if let Some(ServiceAction::L4Result { actions, frames, service_id, poll_delay }) = result {
            send_l4_frames(&frames, &ctx);

            for action in &actions {
                dispatch_action(action, &service_id, ip, &ctx).await;
            }

            // Reschedule if smoltcp still needs polling.
            if let Some(next_delay) = poll_delay {
                schedule_poll_timer(next_delay, ip, ctx);
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
    ctx: &FabricContext<P>,
) {
    // FabricFrame was already validated by dispatch_frame; unwrap is safe.
    let ff = FabricFrame::new(frame).unwrap();
    if let Some(dst_ip) = ff.ipv4_dst() {
        // 1. Check service table first.
        // Lock service_table before mac_table (consistent ordering).
        let svc_result = {
            let mut st = ctx.inner.service_table.lock().unwrap();
            let mac_table = ctx.inner.mac_table.lock().unwrap();
            st.lookup_and_buffer(dst_ip, frame, |mac| mac_table.lookup(mac).is_some())
        };

        if let Some((svc_action, should_activate)) = svc_result {
            // Get service_id for activation event (re-lock briefly).
            let service_id = if should_activate {
                let st = ctx.inner.service_table.lock().unwrap();
                st.get_service_id(&dst_ip).map(String::from)
            } else {
                None
            };

            match svc_action {
                ServiceAction::Forward { pod_ip, pod_mac, service_ip, service_mac } => {
                    log::debug!(
                        "fabric: service Forward {} -> {} (DNAT {} -> {})",
                        format_mac(&dst_mac), format_mac(&pod_mac), service_ip, pod_ip
                    );
                    // Find port for backend MAC, rewrite dst MAC + DNAT, send.
                    if let Some(dst_port) = ctx.inner.resolve_mac(&pod_mac) {
                        let mut rewritten = frame.to_vec();
                        if rewritten.len() >= VNET_HDR_SZ + 6 {
                            rewrite_dst_mac(&mut rewritten, &pod_mac);
                        }
                        // DNAT: rewrite dst IP from service_ip to pod_ip.
                        rewrite_ipv4_dst(&mut rewritten, service_ip, pod_ip);

                        // Insert reverse NAT entry for return traffic.
                        if let Some(ff_rw) = FabricFrame::new(&rewritten) {
                            if let Some(src_ip) = ff_rw.ipv4_src() {
                                let eth = ff_rw.eth_payload();
                                let protocol = extract_ip_protocol(eth).unwrap_or(0);
                                let (src_port, dst_port_val) = extract_transport_ports(eth).unwrap_or((0, 0));
                                let reverse_key = NatFlowKey {
                                    src_ip: pod_ip,
                                    dst_ip: src_ip,
                                    protocol,
                                    src_port: dst_port_val,
                                    dst_port: src_port,
                                };
                                let nat_entry = NatEntry {
                                    service_ip,
                                    service_mac,
                                    backend_ip: pod_ip,
                                    last_seen: std::time::Instant::now(),
                                };
                                ctx.inner.nat_table.lock().unwrap().insert(reverse_key, nat_entry);
                            }
                        }

                        if let Err(e) = dst_port.send_frame(&rewritten).await {
                            log::warn!("fabric: service forward error: {}", e);
                        }
                    } else {
                        log::warn!(
                            "fabric: service forward to {} but backend MAC {} not in mac_table",
                            dst_ip, format_mac(&pod_mac)
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
                    log::debug!(
                        "fabric: service '{}' activator returned {} actions",
                        service_id, actions.len()
                    );
                    for action in actions {
                        dispatch_action(&action, &service_id, dst_ip, ctx).await;
                    }
                }
                ServiceAction::L4Result { actions, frames, service_id, poll_delay } => {
                    // Send outgoing L4 frames.
                    send_l4_frames(&frames, ctx);
                    // Handle non-L4 actions (SetBackendNeed, Log, etc.)
                    for action in actions {
                        dispatch_action(&action, &service_id, dst_ip, ctx).await;
                    }
                    // Schedule smoltcp timer if needed.
                    if let Some(delay) = poll_delay {
                        schedule_poll_timer(delay, dst_ip, ctx.clone());
                    }
                }
            }

            if let Some(service_id) = service_id {
                if let Some(tx) = &ctx.event_tx {
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
            let mut rt = ctx.inner.route_table.lock().unwrap();
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
                flood_frame(frame, src_port_id, &ctx.inner.ports).await;
            }
        }

        if should_miss {
            if let Some(tx) = &ctx.event_tx {
                let _ = tx.try_send(FabricEvent::RouteMiss {
                    dst_ip,
                    dst_mac,
                });
            }
        }
    } else {
        // Non-IPv4 unknown unicast: flood as before.
        flood_frame(frame, src_port_id, &ctx.inner.ports).await;
    }
}

/// Check if a broadcast frame is an ARP request for a service IP. If so,
/// construct and send an ARP reply back to the source port.
async fn try_service_arp_reply<P: FramePort>(
    frame: &[u8],
    service_table: &Mutex<super::ServiceTable>,
    src_port: &SharedPort<P>,
) {
    // Frame was already validated as FabricFrame by dispatch_frame.
    let ff = FabricFrame::new(frame).unwrap();
    let eth_frame = ff.eth_payload();
    // Minimum ARP frame: 14 (eth header) + 28 (ARP for IPv4) = 42.
    if eth_frame.len() < 42 {
        return;
    }
    if ff.ethertype() != 0x0806 {
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
    let sender_mac = ff.src_mac();
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
    ports: &Mutex<HashMap<PortId, SharedPort<P>>>,
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
