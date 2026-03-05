use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use tokio::sync::mpsc;
use distvirt_activator::types::Action;
use super::route::RouteAction;
use super::service::ServiceAction;
use super::nat::{NatEntry, NatFlowKey};
use crate::packet::{FabricPacket, FABRIC_HDR_SZ, IP_PROTO_TCP, format_tcp_flags, ip_packet_dst, ip_packet_protocol, ip_packet_transport_ports, rewrite_ipv4_dst, rewrite_ipv4_src, with_fabric_header};
use super::switch::IpPortTable;
use super::port::{FramePort, PortId};
use super::{FabricEvent, SharedPort, convert_backend_need, handle_log_action};

/// Shared fabric tables wrapped in a single Arc.
pub(crate) struct FabricContextInner<P: FramePort> {
    pub(crate) ports: Mutex<HashMap<PortId, SharedPort<P>>>,
    pub(crate) ip_port_table: Mutex<IpPortTable>,
    pub(crate) route_table: Mutex<super::RouteTable>,
    pub(crate) service_table: Mutex<super::ServiceTable>,
    pub(crate) nat_table: Mutex<super::nat::NatTable>,
    pub(crate) gateway_tx: OnceLock<mpsc::Sender<Vec<u8>>>,
    pub(crate) event_tx: OnceLock<mpsc::Sender<FabricEvent>>,
    pub(crate) subnet: Ipv4Addr,
    pub(crate) prefix_len: u8,
    pub(crate) gateway_ip: Ipv4Addr,
    /// worker_id → port_id for tunnel ports (inter-worker forwarding).
    pub(crate) tunnel_ports: Mutex<HashMap<String, PortId>>,
}

impl<P: FramePort> FabricContextInner<P> {
    /// Resolve an IP address to the port that owns it.
    ///
    /// Locks `ip_port_table` then `ports`; returns `None` if the IP is unknown
    /// or the port has been removed.
    pub(crate) fn resolve_ip(&self, ip: &Ipv4Addr) -> Option<SharedPort<P>> {
        let table = self.ip_port_table.lock().expect("poisoned");
        let port_id = table.lookup(ip)?;
        drop(table);
        self.ports.lock().expect("poisoned").get(&port_id).cloned()
    }

    /// Check if a destination IP is within the fabric subnet.
    pub(crate) fn is_in_subnet(&self, ip: &Ipv4Addr) -> bool {
        let mask = if self.prefix_len >= 32 {
            u32::MAX
        } else {
            !0u32 << (32 - self.prefix_len)
        };
        let subnet_bits = u32::from(*ip) & mask;
        let our_bits = u32::from(self.subnet) & mask;
        subnet_bits == our_bits
    }
}

/// Shared fabric state passed to all forwarding functions.
///
/// Wraps a single `Arc<FabricContextInner>` containing all tables and
/// one-shot channel senders. Cheap to clone (single Arc bump).
pub(super) struct FabricContext<P: FramePort> {
    pub(super) inner: Arc<FabricContextInner<P>>,
}

impl<P: FramePort> Clone for FabricContext<P> {
    fn clone(&self) -> Self {
        FabricContext {
            inner: Arc::clone(&self.inner),
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
        {
            let mut table = self.inner.ip_port_table.lock().expect("poisoned");
            table.remove_by_port(self.port_id);
        }
        let mut ports = self.inner.ports.lock().expect("poisoned");
        ports.remove(&self.port_id);
        log::info!("fabric: port {} removed (guard dropped)", self.port_id);
    }
}

/// Source of a packet being dispatched through the fabric.
enum FrameSource<'a, P: FramePort> {
    /// Packet from a local port.
    Port { port_id: PortId, port: &'a SharedPort<P> },
    /// Packet from the gateway (TUN ingress).
    Gateway,
}

/// Send a fabric packet to a local pod port (L3 internal format).
async fn deliver_to_port<P: FramePort>(
    packet: &[u8],
    dst_port: &SharedPort<P>,
) {
    if let Err(e) = dst_port.send_frame(packet).await {
        log::warn!("fabric: deliver_to_port error: {}", e);
    }
}

/// L3 packet dispatch logic shared by port read loops and gateway ingress.
///
/// Routes all traffic by destination IP. Internal format is `[vnet][IP]`.
async fn dispatch_frame<P: FramePort>(
    packet: &[u8],
    source: FrameSource<'_, P>,
    ctx: &FabricContext<P>,
) {
    // 1. Parse and validate packet.
    let fp = match FabricPacket::new(packet) {
        Some(f) => f,
        None => return, // runt packet
    };

    // 2. Extract dst_ip.
    let dst_ip = fp.ipv4_dst();
    let src_ip = fp.ipv4_src();

    if log::log_enabled!(log::Level::Debug) {
        let source_label = match &source {
            FrameSource::Port { port_id, .. } => format!("port {}", port_id),
            FrameSource::Gateway => "gateway".to_string(),
        };
        let ip_pkt = fp.ip_packet();
        let tcp_info = if ip_packet_protocol(ip_pkt) == Some(IP_PROTO_TCP) {
            let flags = fp.tcp_flags().map(format_tcp_flags).unwrap_or_default();
            let ports = fp.transport_ports().map(|(s, d)| format!(" {}→{}", s, d)).unwrap_or_default();
            format!(" TCP{}{}", ports, flags)
        } else {
            String::new()
        };
        log::debug!(
            "fabric: dispatch_frame from {} | IPv4 {:?} -> {}{} len={}",
            source_label, src_ip, dst_ip, tcp_info, packet.len()
        );
    }

    // 3. Service VIP → DNAT path.
    {
        let svc_result = {
            let mut st = ctx.inner.service_table.lock().expect("poisoned");
            let ip_table = ctx.inner.ip_port_table.lock().expect("poisoned");
            st.lookup_and_buffer(dst_ip, packet, |ip: &Ipv4Addr| ip_table.contains_ip(ip))
        };

        if let Some((svc_action, should_activate)) = svc_result {
            let service_id = if should_activate {
                let st = ctx.inner.service_table.lock().expect("poisoned");
                st.get_service_id(&dst_ip).map(String::from)
            } else {
                None
            };

            match svc_action {
                ServiceAction::Forward { pod_ip, service_ip } => {
                    log::debug!(
                        "fabric: service Forward (DNAT {} -> {})",
                        service_ip, pod_ip
                    );
                    if let Some(dst_port) = ctx.inner.resolve_ip(&pod_ip) {
                        let mut rewritten = packet.to_vec();
                        rewrite_ipv4_dst(&mut rewritten, service_ip, pod_ip);

                        // Insert reverse NAT entry for return traffic.
                        if let Some(fp_rw) = FabricPacket::new(&rewritten) {
                            let ip_pkt = fp_rw.ip_packet();
                            let protocol = ip_packet_protocol(ip_pkt).unwrap_or(0);
                            let (src_port, dst_port_val) = ip_packet_transport_ports(ip_pkt).unwrap_or((0, 0));
                            let reverse_key = NatFlowKey {
                                src_ip: pod_ip,
                                dst_ip: fp_rw.ipv4_src(),
                                protocol,
                                src_port: dst_port_val,
                                dst_port: src_port,
                            };
                            let nat_entry = NatEntry {
                                service_ip,
                                backend_ip: pod_ip,
                                last_seen: std::time::Instant::now(),
                            };
                            ctx.inner.nat_table.lock().expect("poisoned").insert(reverse_key, nat_entry);
                        }

                        if let Err(e) = dst_port.send_frame(&rewritten).await {
                            log::warn!("fabric: service forward error: {}", e);
                        }
                    } else {
                        log::warn!(
                            "fabric: service forward to {} but backend IP {} not in ip_port_table",
                            dst_ip, pod_ip
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
                    send_l4_frames(&frames, ctx);
                    for action in actions {
                        dispatch_action(&action, &service_id, dst_ip, ctx).await;
                    }
                    if let Some(delay) = poll_delay {
                        schedule_poll_timer(delay, dst_ip, ctx.clone());
                    }
                }
            }

            if let Some(service_id) = service_id {
                if let Some(tx) = ctx.inner.event_tx.get() {
                    let _ = tx.try_send(FabricEvent::ServiceActivation {
                        service_id,
                        dst_ip,
                    });
                }
            }

            return;
        }
    }

    // 4. Local pod → SNAT check + send.
    if let Some(dst_port) = ctx.inner.resolve_ip(&dst_ip) {
        // Check NAT table for SNAT (return traffic from backend to client).
        let nat_match = {
            let ip_pkt = fp.ip_packet();
            let protocol = ip_packet_protocol(ip_pkt);
            let ports = ip_packet_transport_ports(ip_pkt);

            if let Some(proto) = protocol {
                let (s_port, d_port) = ports.unwrap_or((0, 0));
                let key = NatFlowKey {
                    src_ip,
                    dst_ip,
                    protocol: proto,
                    src_port: s_port,
                    dst_port: d_port,
                };
                let mut nat = ctx.inner.nat_table.lock().expect("poisoned");
                nat.lookup(&key).map(|e| e.service_ip)
            } else {
                None
            }
        };

        if let Some(svc_ip) = nat_match {
            // SNAT: rewrite src IP from backend to service.
            let backend_ip = src_ip;
            log::debug!(
                "fabric: SNAT return traffic (rewriting src {} -> {})",
                backend_ip, svc_ip
            );
            let mut rewritten = packet.to_vec();
            rewrite_ipv4_src(&mut rewritten, backend_ip, svc_ip);
            if let Err(e) = dst_port.send_frame(&rewritten).await {
                log::warn!("fabric: SNAT send error: {}", e);
            }
        } else {
            deliver_to_port(packet, &dst_port).await;
        }
        return;
    }

    // 5. Route table → buffer / remote worker / drop.
    let (action, should_miss) = {
        let mut rt = ctx.inner.route_table.lock().expect("poisoned");
        rt.lookup_and_buffer(dst_ip, packet)
    };

    match action {
        RouteAction::Buffered => {
            log::trace!("fabric: frame to {} buffered", dst_ip);
        }
        RouteAction::Drop => {
            log::trace!("fabric: frame to {} dropped by route policy", dst_ip);
        }
        RouteAction::RemoteWorker { worker_id } => {
            let port = {
                let tp = ctx.inner.tunnel_ports.lock().expect("poisoned");
                tp.get(&worker_id).and_then(|pid| {
                    ctx.inner.ports.lock().expect("poisoned").get(pid).cloned()
                })
            };
            if let Some(port) = port {
                if let Err(e) = port.send_frame(packet).await {
                    log::warn!("fabric: tunnel send to worker {} failed: {}", worker_id, e);
                }
            } else {
                log::debug!("fabric: no tunnel port for worker {}, dropping", worker_id);
            }
        }
        RouteAction::NoRoute => {
            // 6. dst_ip is the gateway itself or outside subnet → forward to gateway.
            //    Skip if the packet already came from the gateway to avoid loops.
            if dst_ip == ctx.inner.gateway_ip || !ctx.inner.is_in_subnet(&dst_ip) {
                if matches!(source, FrameSource::Gateway) {
                    log::debug!("fabric: dropping gateway-originated frame to {} (no route back)", dst_ip);
                } else if let Some(gw_tx) = ctx.inner.gateway_tx.get() {
                    let _ = gw_tx.try_send(packet.to_vec());
                } else {
                    log::debug!("fabric: dropping frame to {} (no gateway)", dst_ip);
                }
            } else {
                // 7. dst_ip in fabric subnet but no match → drop (pod doesn't exist).
                log::debug!("fabric: dropping frame to {} (in subnet, no route/port)", dst_ip);
            }
        }
    }

    if should_miss {
        if let Some(tx) = ctx.inner.event_tx.get() {
            let _ = tx.try_send(FabricEvent::RouteMiss { dst_ip });
        }
    }
}

/// Per-port read loop: reads packets and dispatches them through the fabric.
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

    let mut buf = vec![0u8; FABRIC_HDR_SZ + 1514]; // fabric header + max IP frame

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

        if n < FABRIC_HDR_SZ {
            continue; // too short to contain vnet header
        }

        let frame = &buf[..n];
        log::debug!("fabric: port {} received {} bytes (fabric_flags=0x{:02x})", port_id, n, frame[0]);
        dispatch_frame(frame, FrameSource::Port { port_id, port: &port }, &ctx).await;
    }
}

/// Task that reads packets from the gateway ingress channel and forwards them
/// into the fabric.
pub(super) async fn gateway_ingress_task<P: FramePort>(
    mut ingress_rx: mpsc::Receiver<Vec<u8>>,
    ctx: FabricContext<P>,
) {
    while let Some(frame) = ingress_rx.recv().await {
        if frame.len() < FABRIC_HDR_SZ {
            continue;
        }
        dispatch_frame(&frame, FrameSource::Gateway, &ctx).await;
    }

    log::info!("fabric: gateway ingress task ended");
}

/// Dispatch a single activator action: replay packets, set backend need, or log.
///
/// Shared by `dispatch_frame` service path and `Fabric::execute_service_actions`.
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
                let st = ctx.inner.service_table.lock().expect("poisoned");
                st.get_nat_info_by_id(service_id)
            };
            if let Some((service_ip, backend_ip)) = nat_info {
                if let Some(dst_port) = ctx.inner.resolve_ip(&backend_ip) {
                    let mut rewritten = raw_frame.clone();
                    // DNAT: rewrite dst IP from service_ip to backend_ip.
                    rewrite_ipv4_dst(&mut rewritten, service_ip, backend_ip);

                    // Insert reverse NAT entry.
                    if let Some(fp_rw) = FabricPacket::new(&rewritten) {
                        let ip_pkt = fp_rw.ip_packet();
                        let protocol = ip_packet_protocol(ip_pkt).unwrap_or(0);
                        let (src_port, dst_port_val) = ip_packet_transport_ports(ip_pkt).unwrap_or((0, 0));
                        let reverse_key = NatFlowKey {
                            src_ip: backend_ip,
                            dst_ip: fp_rw.ipv4_src(),
                            protocol,
                            src_port: dst_port_val,
                            dst_port: src_port,
                        };
                        let nat_entry = NatEntry {
                            service_ip,
                            backend_ip,
                            last_seen: std::time::Instant::now(),
                        };
                        ctx.inner.nat_table.lock().expect("poisoned").insert(reverse_key, nat_entry);
                    }

                    match dst_port.send_frame(&rewritten).await {
                        Err(e) => {
                            log::warn!("fabric: replay send error: {}", e);
                        }
                        Ok(n) => {
                            log::debug!(
                                "fabric: ReplayPacket sent {} bytes to backend IP {} (DNAT {} -> {})",
                                n, backend_ip, service_ip, backend_ip
                            );
                        }
                    }
                } else {
                    log::warn!(
                        "fabric: ReplayPacket for service '{}': backend IP {} not in ip_port_table, dropping",
                        service_id, backend_ip
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
            if let Some(tx) = ctx.inner.event_tx.get() {
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
            log::debug!("fabric: activator action for '{}': PacketDecision", service_id);
        }
        _ => {
            log::debug!("fabric: unhandled activator action in forwarding path");
        }
    }
}

/// Send L4 outgoing IP packets (from StreamManager) to the appropriate ports.
///
/// StreamManager emits raw IP packets. We prepend a fabric header and route
/// by destination IP.
pub(super) fn send_l4_frames<P: FramePort>(
    frames: &[Vec<u8>],
    ctx: &FabricContext<P>,
) {
    if frames.is_empty() {
        return;
    }
    for ip_packet in frames {
        if ip_packet.len() < 20 {
            continue;
        }
        let dst_ip = match ip_packet_dst(ip_packet) {
            Some(ip) => ip,
            None => continue,
        };
        if let Some(port) = ctx.inner.resolve_ip(&dst_ip) {
            let fabric_frame = with_fabric_header(0, 0, ip_packet);
            tokio::spawn(async move {
                if let Err(e) = port.send_frame(&fabric_frame).await {
                    log::warn!("fabric: L4 frame send error: {}", e);
                }
            });
        } else {
            log::debug!(
                "fabric: send_l4_frame: dst IP {} not in ip_port_table",
                dst_ip
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
            let mut st = ctx.inner.service_table.lock().expect("poisoned");
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
