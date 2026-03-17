use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use super::endpoint::EndpointAction;
use super::nat::{NatEntry, NatFlowKey};
use super::port::{FramePort, PortId};
use super::{FabricEvent, SharedPort, handle_log_action};
use crate::packet::{
    FABRIC_HDR_SZ, FabricPacket, IP_PROTO_TCP, format_tcp_flags, ip_packet_dst, ip_packet_protocol,
    ip_packet_transport_ports, rewrite_ipv4_dst, rewrite_ipv4_src, with_fabric_header,
};
use distvirt_activator::types::Action;
use distvirt_worker_protocol::{ServiceId, WorkerId};
use tokio::sync::mpsc;

/// Shared fabric tables wrapped in a single Arc.
pub(crate) struct FabricContextInner<P: FramePort> {
    pub(crate) ports: Mutex<HashMap<PortId, SharedPort<P>>>,
    pub(crate) endpoint_table: Mutex<super::EndpointTable>,
    pub(crate) nat_table: Mutex<super::nat::NatTable>,
    pub(crate) gateway_tx: OnceLock<mpsc::Sender<Vec<u8>>>,
    pub(crate) event_tx: OnceLock<mpsc::Sender<FabricEvent>>,
    pub(crate) subnet: Ipv4Addr,
    pub(crate) prefix_len: u8,
    pub(crate) gateway_ip: Ipv4Addr,
    /// worker_id → port_id for tunnel ports (inter-worker forwarding).
    pub(crate) tunnel_ports: Mutex<HashMap<WorkerId, PortId>>,
}

impl<P: FramePort> FabricContextInner<P> {
    /// Resolve an IP address to the port that owns it.
    ///
    /// Looks up via endpoint_table (LocalPod/LocalAdapter), then ports map.
    pub(crate) fn resolve_ip(&self, ip: &Ipv4Addr) -> Option<SharedPort<P>> {
        let port_id = {
            let et = self.endpoint_table.lock().expect("poisoned");
            et.get_port_id(ip)?
        };
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
            let mut et = self.inner.endpoint_table.lock().expect("poisoned");
            et.detach_port(self.port_id);
        }
        let mut ports = self.inner.ports.lock().expect("poisoned");
        ports.remove(&self.port_id);
        log::info!("fabric: port {} removed (guard dropped)", self.port_id);
    }
}

/// Source of a packet being dispatched through the fabric.
enum FrameSource<'a, P: FramePort> {
    /// Packet from a local port.
    Port {
        port_id: PortId,
        port: &'a SharedPort<P>,
    },
    /// Packet from the gateway (TUN ingress).
    Gateway,
    /// Internally generated frame (DNAT rewrite, replay).
    Internal,
}

/// Insert a reverse NAT entry so return traffic from the backend can be
/// SNATted back to the service VIP.
fn insert_reverse_nat<P: FramePort>(
    rewritten: &[u8],
    service_ip: Ipv4Addr,
    pod_ip: Ipv4Addr,
    ctx: &FabricContext<P>,
) {
    if let Some(fp_rw) = FabricPacket::new(rewritten) {
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
        ctx.inner
            .nat_table
            .lock()
            .expect("poisoned")
            .insert(reverse_key, nat_entry);
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
    let mut owned_packet: Option<Vec<u8>> = None;
    let mut skip_flow_tracking = false;

    for _iteration in 0..2 {
        let pkt = owned_packet.as_deref().unwrap_or(packet);

        // 1. Parse and validate packet.
        let fp = match FabricPacket::new(pkt) {
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
                FrameSource::Internal => "internal".to_string(),
            };
            let ip_pkt = fp.ip_packet();
            let tcp_info = if ip_packet_protocol(ip_pkt) == Some(IP_PROTO_TCP) {
                let flags = fp.tcp_flags().map(format_tcp_flags).unwrap_or_default();
                let ports = fp
                    .transport_ports()
                    .map(|(s, d)| format!(" {}→{}", s, d))
                    .unwrap_or_default();
                format!(" TCP{}{}", ports, flags)
            } else {
                String::new()
            };
            log::debug!(
                "fabric: dispatch_frame from {} | IPv4 {:?} -> {}{} len={}",
                source_label,
                src_ip,
                dst_ip,
                tcp_info,
                pkt.len()
            );
        }

        // 3. Endpoint table lookup (services + routes).
        let (ep_action, should_activate, flow_change) = {
            let mut st = ctx.inner.endpoint_table.lock().expect("poisoned");
            st.lookup_and_buffer(dst_ip, pkt, skip_flow_tracking)
        };

        // Emit flow status change if the tracker detected a transition.
        if let Some(change) = flow_change {
            if let Some(tx) = ctx.inner.event_tx.get() {
                let _ = tx.try_send(FabricEvent::EndpointDemand {
                    ip: change.ip,
                    service_id: change.service_id,
                    active: change.active,
                });
            }
        }

        match ep_action {
            EndpointAction::ServiceForward { pod_ip, service_ip } => {
                log::debug!(
                    "fabric: service Forward (DNAT {} -> {})",
                    service_ip,
                    pod_ip
                );
                let mut rewritten = pkt.to_vec();
                rewrite_ipv4_dst(&mut rewritten, service_ip, pod_ip);
                insert_reverse_nat(&rewritten, service_ip, pod_ip, ctx);

                // Loop back for a second dispatch pass to route the
                // rewritten packet through the normal path.
                owned_packet = Some(rewritten);
                skip_flow_tracking = true;

                if should_activate {
                    emit_activation(ctx, dst_ip, None).await;
                }
                continue;
            }
            EndpointAction::Buffered { service_id } => {
                log::trace!("fabric: frame to {} buffered", dst_ip);
                if should_activate {
                    emit_activation(ctx, dst_ip, service_id).await;
                }
                return;
            }
            EndpointAction::Drop { service_id } => {
                log::trace!("fabric: frame to {} dropped", dst_ip);
                if should_activate {
                    emit_activation(ctx, dst_ip, service_id).await;
                }
                return;
            }
            EndpointAction::ActivatorActions {
                actions,
                service_id,
            } => {
                log::debug!(
                    "fabric: service '{}' activator returned {} actions",
                    service_id,
                    actions.len()
                );
                for action in actions {
                    dispatch_action(&action, service_id, dst_ip, ctx).await;
                }
                if should_activate {
                    emit_activation(ctx, dst_ip, Some(service_id)).await;
                }
                return;
            }
            EndpointAction::L4Result {
                actions,
                frames,
                service_id,
                poll_delay,
            } => {
                send_l4_frames(&frames, ctx);
                for action in actions {
                    dispatch_action(&action, service_id, dst_ip, ctx).await;
                }
                if let Some(delay) = poll_delay {
                    schedule_poll_timer(delay, dst_ip, ctx.clone());
                }
                if should_activate {
                    emit_activation(ctx, dst_ip, Some(service_id)).await;
                }
                return;
            }
            EndpointAction::RemoteWorker { worker_id } => {
                let port = {
                    let tp = ctx.inner.tunnel_ports.lock().expect("poisoned");
                    tp.get(&worker_id)
                        .and_then(|pid| ctx.inner.ports.lock().expect("poisoned").get(pid).cloned())
                };
                if let Some(port) = port {
                    if let Err(e) = port.send_frame(pkt).await {
                        log::warn!("fabric: tunnel send to worker {} failed: {}", worker_id, e);
                    }
                } else {
                    log::debug!("fabric: no tunnel port for worker {}, dropping", worker_id);
                }
                return;
            }
            EndpointAction::LocalAdapter { port_id } => {
                let port = {
                    ctx.inner
                        .ports
                        .lock()
                        .expect("poisoned")
                        .get(&port_id)
                        .cloned()
                };
                if let Some(port) = port {
                    if let Err(e) = port.send_frame(pkt).await {
                        log::warn!("fabric: adapter send to port {} failed: {}", port_id, e);
                    }
                } else {
                    log::debug!("fabric: no adapter port {}, dropping", port_id);
                }
                return;
            }
            EndpointAction::LocalPod { port_id } => {
                let dst_port = {
                    ctx.inner
                        .ports
                        .lock()
                        .expect("poisoned")
                        .get(&port_id)
                        .cloned()
                };
                if let Some(dst_port) = dst_port {
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
                        let backend_ip = src_ip;
                        log::debug!(
                            "fabric: SNAT return traffic (rewriting src {} -> {})",
                            backend_ip,
                            svc_ip
                        );
                        let mut rewritten = pkt.to_vec();
                        rewrite_ipv4_src(&mut rewritten, backend_ip, svc_ip);
                        if let Err(e) = dst_port.send_frame(&rewritten).await {
                            log::warn!("fabric: SNAT send error: {}", e);
                        }
                    } else {
                        if let Err(e) = dst_port.send_frame(pkt).await {
                            log::warn!("fabric: deliver_to_port error: {}", e);
                        }
                    }
                } else {
                    log::debug!("fabric: LocalPod port {} not found, dropping", port_id);
                }
                return;
            }
            EndpointAction::NotFound => {
                // Gateway or drop.
                if dst_ip == ctx.inner.gateway_ip || !ctx.inner.is_in_subnet(&dst_ip) {
                    if matches!(source, FrameSource::Gateway | FrameSource::Internal) {
                        log::debug!(
                            "fabric: dropping non-port frame to {} (no route back)",
                            dst_ip
                        );
                    } else if let Some(gw_tx) = ctx.inner.gateway_tx.get() {
                        let _ = gw_tx.try_send(pkt.to_vec());
                    } else {
                        log::debug!("fabric: dropping frame to {} (no gateway)", dst_ip);
                    }
                } else {
                    log::debug!(
                        "fabric: dropping frame to {} (in subnet, no route/port)",
                        dst_ip
                    );
                }
                return;
            }
        }
    }
}

/// Emit an EndpointActivation event.
async fn emit_activation<P: FramePort>(
    ctx: &FabricContext<P>,
    dst_ip: Ipv4Addr,
    service_id: Option<ServiceId>,
) {
    if let Some(tx) = ctx.inner.event_tx.get() {
        let _ = tx.try_send(FabricEvent::EndpointActivation { dst_ip, service_id });
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
        log::debug!(
            "fabric: port {} received {} bytes (fabric_flags=0x{:02x})",
            port_id,
            n,
            frame[0]
        );
        dispatch_frame(
            frame,
            FrameSource::Port {
                port_id,
                port: &port,
            },
            &ctx,
        )
        .await;
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
    service_id: ServiceId,
    dst_ip: Ipv4Addr,
    ctx: &FabricContext<P>,
) {
    match action {
        Action::ReplayPacket(raw_frame) => {
            log::debug!(
                "fabric: dispatching ReplayPacket for service '{}' (frame_len={})",
                service_id,
                raw_frame.len()
            );
            let nat_info = {
                let st = ctx.inner.endpoint_table.lock().expect("poisoned");
                st.get_service_nat_info(service_id)
            };
            if let Some((service_ip, backend_ip)) = nat_info {
                let mut rewritten = raw_frame.clone();
                rewrite_ipv4_dst(&mut rewritten, service_ip, backend_ip);
                insert_reverse_nat(&rewritten, service_ip, backend_ip, ctx);
                Box::pin(dispatch_frame(&rewritten, FrameSource::Internal, ctx)).await;
            } else {
                log::warn!(
                    "fabric: ReplayPacket for service '{}': no NAT info (backend not set?), dropping",
                    service_id
                );
            }
        }
        Action::SetBackendNeed(need) => {
            use distvirt_activator::types::BackendNeed as ActivatorBackendNeed;
            if let Some(tx) = ctx.inner.event_tx.get() {
                match need {
                    ActivatorBackendNeed::Traffic => {
                        // Pulse signal — lossy (try_send), same as EndpointActivation.
                        let _ = tx.try_send(FabricEvent::EndpointActivation {
                            dst_ip,
                            service_id: Some(service_id),
                        });
                    }
                    ActivatorBackendNeed::Active => {
                        // Level signal — reliable (send).
                        if let Err(e) = tx
                            .send(FabricEvent::EndpointDemand {
                                ip: dst_ip,
                                service_id: Some(service_id),
                                active: true,
                            })
                            .await
                        {
                            log::warn!(
                                "fabric: failed to send EndpointDemand(active) for {}: {}",
                                service_id,
                                e
                            );
                        }
                    }
                    ActivatorBackendNeed::None => {
                        // Level signal — reliable (send).
                        if let Err(e) = tx
                            .send(FabricEvent::EndpointDemand {
                                ip: dst_ip,
                                service_id: Some(service_id),
                                active: false,
                            })
                            .await
                        {
                            log::warn!(
                                "fabric: failed to send EndpointDemand(inactive) for {}: {}",
                                service_id,
                                e
                            );
                        }
                    }
                }
            }
        }
        Action::Log(log_action) => {
            handle_log_action(service_id, log_action);
        }
        Action::PacketDecision { .. } => {
            // Decision already handled by activator internally.
            log::debug!(
                "fabric: activator action for '{}': PacketDecision",
                service_id
            );
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
pub(super) fn send_l4_frames<P: FramePort>(frames: &[Vec<u8>], ctx: &FabricContext<P>) {
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
            log::debug!("fabric: send_l4_frame: dst IP {} not reachable", dst_ip);
        }
    }
}

/// Schedule a smoltcp poll timer for a service IP.
///
/// Spawns a tokio task that sleeps for `delay`, then calls
/// `handle_timeout_for_ip` on the service table to drive TCP state machine
/// timers (retransmissions, TIME_WAIT cleanup, FIN/FIN-ACK, etc.).
fn schedule_poll_timer<P: FramePort>(delay: Duration, ip: Ipv4Addr, ctx: FabricContext<P>) {
    tokio::spawn(async move {
        tokio::time::sleep(delay).await;

        let result = {
            let mut st = ctx.inner.endpoint_table.lock().expect("poisoned");
            st.handle_timeout_for_ip(ip)
        };

        if let Some(EndpointAction::L4Result {
            actions,
            frames,
            service_id,
            poll_delay,
        }) = result
        {
            send_l4_frames(&frames, &ctx);

            for action in &actions {
                dispatch_action(action, service_id, ip, &ctx).await;
            }

            // Reschedule if smoltcp still needs polling.
            if let Some(next_delay) = poll_delay {
                schedule_poll_timer(next_delay, ip, ctx);
            }
        }
    });
}
