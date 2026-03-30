//! WireGuard ingress adapter using boringtun for userspace crypto.
//!
//! One `WireGuardAdapter` per worker owns a single UDP socket. Each WireGuard
//! peer maps to a namespace via its public key. The adapter bridges L3 (IP)
//! packets from peers into the L3 fabric.

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

use boringtun::noise::{Tunn, TunnResult, rate_limiter::RateLimiter};
use boringtun::x25519::{PublicKey, StaticSecret};
use tokio::net::UdpSocket;
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio::task::JoinHandle;

use crate::fabric::ChannelPort;
use crate::packet::{FABRIC_HDR_SZ, complete_checksum, ip_packet_dst, with_fabric_header};

use super::{AdapterPortHandle, IngressAdapter};
use async_trait::async_trait;

/// Maximum UDP datagram size (WireGuard max is ~65535 but typical MTU is much smaller).
const MAX_UDP_SIZE: usize = 65536;

/// Maximum encapsulated packet size.
const MAX_PACKET_SIZE: usize = 65536;

/// Per-peer WireGuard state.
struct PeerState {
    tunn: Mutex<Tunn>,
    namespace_id: String,
    peer_ip: Ipv4Addr,
    endpoint: RwLock<Option<SocketAddr>>,
}

/// Per-namespace channel state for sending frames into the fabric.
struct NamespaceChannel {
    adapter_tx: mpsc::Sender<Vec<u8>>,
    _egress_task: JoinHandle<()>,
    /// Incarnation ID from the orchestrator — used to guard against stale
    /// cleanup removing a newer channel for the same namespace name.
    id: u64,
}

/// Request to remove a namespace channel, sent from `DropGuard::drop`.
struct CleanupRequest {
    namespace_name: String,
    id: u64,
}

/// Shared mutable state for the WireGuard adapter.
struct WireGuardState {
    private_key: StaticSecret,
    peers_by_key: HashMap<[u8; 32], Arc<PeerState>>,
    peers_by_addr: HashMap<SocketAddr, Arc<PeerState>>,
    namespace_channels: HashMap<String, NamespaceChannel>,
    rate_limiter: Arc<RateLimiter>,
    /// Counter for Tunn index allocation.
    next_index: u32,
}

/// WireGuard ingress adapter.
///
/// Owns a UDP socket and manages peers that bridge external WireGuard clients
/// into the fabric's L3 network.
pub struct WireGuardAdapter {
    state: Arc<RwLock<WireGuardState>>,
    udp_socket: Arc<UdpSocket>,
    public_key: [u8; 32],
    cleanup_tx: mpsc::Sender<CleanupRequest>,
    _udp_recv_task: JoinHandle<()>,
    _timer_task: JoinHandle<()>,
    _cleanup_task: JoinHandle<()>,
}

impl WireGuardAdapter {
    /// Create a new WireGuard adapter bound to the given port.
    ///
    /// Generates a fresh X25519 keypair. The public key can be retrieved
    /// via [`Self::public_key()`].
    pub async fn new(listen_port: u16) -> anyhow::Result<Self> {
        let private_key = StaticSecret::from(rand::random::<[u8; 32]>());
        let public_key = PublicKey::from(&private_key);
        let rate_limiter = Arc::new(RateLimiter::new(&public_key, 100));

        let udp_socket = Arc::new(UdpSocket::bind(format!("0.0.0.0:{}", listen_port)).await?);
        log::info!(
            "wireguard: listening on UDP port {}",
            udp_socket.local_addr()?
        );

        let public_key_bytes = *public_key.as_bytes();

        let state = Arc::new(RwLock::new(WireGuardState {
            private_key,
            peers_by_key: HashMap::new(),
            peers_by_addr: HashMap::new(),
            namespace_channels: HashMap::new(),
            rate_limiter,
            next_index: 0,
        }));

        let udp_recv_task = tokio::spawn(Self::udp_recv_loop(
            Arc::clone(&state),
            Arc::clone(&udp_socket),
        ));

        let timer_task = tokio::spawn(Self::timer_loop(
            Arc::clone(&state),
            Arc::clone(&udp_socket),
        ));

        let (cleanup_tx, cleanup_rx) = mpsc::channel(64);
        let cleanup_task = tokio::spawn(Self::cleanup_loop(
            Arc::clone(&state),
            cleanup_rx,
        ));

        Ok(WireGuardAdapter {
            state,
            udp_socket,
            public_key: public_key_bytes,
            cleanup_tx,
            _udp_recv_task: udp_recv_task,
            _timer_task: timer_task,
            _cleanup_task: cleanup_task,
        })
    }

    /// The adapter's public key (reported to orchestrator, given to clients).
    pub fn public_key(&self) -> [u8; 32] {
        self.public_key
    }

    /// The actual UDP port the adapter is listening on.
    pub fn listen_port(&self) -> u16 {
        self.udp_socket
            .local_addr()
            .expect("bound socket has local addr")
            .port()
    }

    /// Add a WireGuard peer mapped to a namespace.
    pub async fn add_peer(
        &self,
        namespace_id: &distvirt_worker_protocol::NamespaceId,
        public_key: [u8; 32],
        peer_ip: Ipv4Addr,
        preshared_key: Option<[u8; 32]>,
    ) -> anyhow::Result<()> {
        let mut state = self.state.write().await;

        let index = state.next_index;
        state.next_index = state.next_index.wrapping_add(1);

        let peer_public = PublicKey::from(public_key);
        let static_private = state.private_key.clone();

        let tunn = Tunn::new(
            static_private,
            peer_public,
            preshared_key,
            None, // no persistent keepalive
            index,
            Some(Arc::clone(&state.rate_limiter)),
        );

        let peer = Arc::new(PeerState {
            tunn: Mutex::new(tunn),
            namespace_id: namespace_id.name.clone(),
            peer_ip,
            endpoint: RwLock::new(None),
        });

        log::info!(
            "wireguard: added peer pubkey={} ip={} namespace={}",
            hex::encode(public_key),
            peer_ip,
            namespace_id.name,
        );

        state.peers_by_key.insert(public_key, peer);
        Ok(())
    }

    /// Remove a WireGuard peer by public key.
    pub async fn remove_peer(&self, public_key: &[u8; 32]) -> anyhow::Result<()> {
        let mut state = self.state.write().await;
        if let Some(peer) = state.peers_by_key.remove(public_key) {
            // Remove from addr mapping.
            let endpoint = peer.endpoint.read().await;
            if let Some(addr) = *endpoint {
                state.peers_by_addr.remove(&addr);
            }
            log::info!("wireguard: removed peer pubkey={}", hex::encode(public_key));
        } else {
            log::warn!(
                "wireguard: remove_peer: unknown key {}",
                hex::encode(public_key)
            );
        }
        Ok(())
    }

    /// UDP receive loop: read datagrams and feed them through boringtun.
    async fn udp_recv_loop(state: Arc<RwLock<WireGuardState>>, socket: Arc<UdpSocket>) {
        let mut recv_buf = vec![0u8; MAX_UDP_SIZE];
        let mut dec_buf = vec![0u8; MAX_PACKET_SIZE];

        loop {
            let (n, src_addr) = match socket.recv_from(&mut recv_buf).await {
                Ok(r) => r,
                Err(e) => {
                    log::error!("wireguard: UDP recv error: {}", e);
                    continue;
                }
            };

            let datagram = &recv_buf[..n];
            log::trace!(
                "wireguard: received {} byte UDP packet from {}",
                n,
                src_addr
            );

            // Try to find peer by source address (fast path).
            let peer = {
                let state = state.read().await;
                state.peers_by_addr.get(&src_addr).cloned()
            };

            if let Some(peer) = peer {
                log::trace!("wireguard: matched known peer ip={}", peer.peer_ip);
                Self::handle_peer_packet(&state, &socket, &peer, src_addr, datagram, &mut dec_buf)
                    .await;
            } else {
                log::trace!("wireguard: unknown source, trying all peers");
                // Slow path: try all peers (handshake from new endpoint).
                Self::handle_unknown_source(&state, &socket, src_addr, datagram, &mut dec_buf)
                    .await;
            }
        }
    }

    /// Handle a packet from a known peer.
    async fn handle_peer_packet(
        state: &Arc<RwLock<WireGuardState>>,
        socket: &Arc<UdpSocket>,
        peer: &Arc<PeerState>,
        src_addr: SocketAddr,
        datagram: &[u8],
        dec_buf: &mut [u8],
    ) {
        let result = {
            let mut tunn = peer.tunn.lock().await;
            tunn.decapsulate(Some(src_addr.ip()), datagram, dec_buf)
        };

        Self::process_tunn_result(state, socket, peer, src_addr, result).await;
    }

    /// Try decapsulating against all peers when the source address is unknown.
    async fn handle_unknown_source(
        state: &Arc<RwLock<WireGuardState>>,
        socket: &Arc<UdpSocket>,
        src_addr: SocketAddr,
        datagram: &[u8],
        dec_buf: &mut [u8],
    ) {
        let peers: Vec<([u8; 32], Arc<PeerState>)> = {
            let s = state.read().await;
            s.peers_by_key
                .iter()
                .map(|(k, v)| (*k, Arc::clone(v)))
                .collect()
        };

        for (_key, peer) in &peers {
            let result = {
                let mut tunn = peer.tunn.lock().await;
                tunn.decapsulate(Some(src_addr.ip()), datagram, dec_buf)
            };

            match &result {
                TunnResult::Done
                | TunnResult::WriteToNetwork(_)
                | TunnResult::WriteToTunnelV4(..)
                | TunnResult::WriteToTunnelV6(..) => {
                    // This peer accepted the packet — update endpoint mapping.
                    Self::update_peer_endpoint(state, peer, src_addr).await;
                    Self::process_tunn_result(state, socket, peer, src_addr, result).await;
                    return;
                }
                TunnResult::Err(_) => {
                    // Not for this peer, try next.
                    continue;
                }
            }
        }

        log::trace!("wireguard: no peer matched packet from {}", src_addr);
    }

    /// Process a TunnResult from decapsulation.
    async fn process_tunn_result(
        state: &Arc<RwLock<WireGuardState>>,
        socket: &Arc<UdpSocket>,
        peer: &Arc<PeerState>,
        src_addr: SocketAddr,
        result: TunnResult<'_>,
    ) {
        match result {
            TunnResult::Done => {
                log::trace!("wireguard: decapsulate result: Done (from {})", src_addr);
            }
            TunnResult::Err(e) => {
                log::debug!("wireguard: decapsulate error from {}: {:?}", src_addr, e);
            }
            TunnResult::WriteToNetwork(data) => {
                // Response packet (handshake reply, cookie, etc.) — send back.
                log::debug!(
                    "wireguard: sending {} byte response to {} (handshake/cookie)",
                    data.len(),
                    src_addr
                );
                let data = data.to_vec();
                if let Err(e) = socket.send_to(&data, src_addr).await {
                    log::warn!("wireguard: failed to send response to {}: {}", src_addr, e);
                }
                // After sending a handshake response, there may be more to do.
                // Call decapsulate again with empty input to check.
                let mut cont_buf = vec![0u8; MAX_PACKET_SIZE];
                loop {
                    let cont_result = {
                        let mut tunn = peer.tunn.lock().await;
                        tunn.decapsulate(None, &[], &mut cont_buf)
                    };
                    match cont_result {
                        TunnResult::Done => break,
                        TunnResult::WriteToNetwork(data) => {
                            let data = data.to_vec();
                            if let Err(e) = socket.send_to(&data, src_addr).await {
                                log::warn!("wireguard: send continuation error: {}", e);
                            }
                        }
                        _ => break,
                    }
                }
            }
            TunnResult::WriteToTunnelV4(ip_packet, _addr) => {
                // Decrypted IP packet — inject into the fabric.
                let dst_ip = ip_packet_dst(ip_packet);

                log::trace!(
                    "wireguard: decrypted {} byte IP packet from peer ip={}, dst_ip={:?}, injecting into fabric ns={}",
                    ip_packet.len(),
                    peer.peer_ip,
                    dst_ip,
                    peer.namespace_id,
                );

                // Log TCP details for incoming packets too.
                if ip_packet.len() >= 40 && ip_packet[9] == 6 {
                    let ihl = (ip_packet[0] & 0x0f) as usize * 4;
                    if ip_packet.len() >= ihl + 14 {
                        let tcp_flags = ip_packet[ihl + 13];
                        let src_port = u16::from_be_bytes([ip_packet[ihl], ip_packet[ihl + 1]]);
                        let dst_port = u16::from_be_bytes([ip_packet[ihl + 2], ip_packet[ihl + 3]]);
                        let flag_str = format!(
                            "{}{}{}{}",
                            if tcp_flags & 0x02 != 0 { "SYN " } else { "" },
                            if tcp_flags & 0x10 != 0 { "ACK " } else { "" },
                            if tcp_flags & 0x04 != 0 { "RST " } else { "" },
                            if tcp_flags & 0x01 != 0 { "FIN " } else { "" },
                        );
                        log::debug!(
                            "wireguard: ingress TCP: {}.{}.*.*:{} -> {}.{}.*.*:{} flags=[{}]",
                            ip_packet[12],
                            ip_packet[13],
                            src_port,
                            ip_packet[16],
                            ip_packet[17],
                            dst_port,
                            flag_str.trim(),
                        );
                    }
                }

                // Prepend fabric header (no NEEDS_CSUM for decrypted packets).
                let frame = with_fabric_header(0, 0, ip_packet);
                let s = state.read().await;
                if let Some(ns_ch) = s.namespace_channels.get(&peer.namespace_id) {
                    if let Err(e) = ns_ch.adapter_tx.try_send(frame) {
                        log::warn!(
                            "wireguard: failed to send frame to fabric ns={}: {}",
                            peer.namespace_id,
                            e
                        );
                    }
                } else {
                    log::warn!(
                        "wireguard: no namespace channel for '{}', dropping {} byte packet (available: {:?})",
                        peer.namespace_id,
                        frame.len(),
                        s.namespace_channels.keys().collect::<Vec<_>>()
                    );
                }
            }
            TunnResult::WriteToTunnelV6(_ip_packet, _addr) => {
                // IPv6 not supported in the fabric currently.
                log::trace!("wireguard: dropping IPv6 packet");
            }
        }
    }

    /// Update the endpoint for a peer (roaming support).
    async fn update_peer_endpoint(
        state: &Arc<RwLock<WireGuardState>>,
        peer: &Arc<PeerState>,
        new_addr: SocketAddr,
    ) {
        let old_addr = {
            let ep = peer.endpoint.read().await;
            *ep
        };

        if old_addr == Some(new_addr) {
            return;
        }

        // Update the peer's endpoint.
        {
            let mut ep = peer.endpoint.write().await;
            *ep = Some(new_addr);
        }

        // Update the addr→peer mapping.
        let mut s = state.write().await;
        if let Some(old) = old_addr {
            s.peers_by_addr.remove(&old);
        }
        s.peers_by_addr.insert(new_addr, Arc::clone(peer));

        log::debug!(
            "wireguard: peer ip={} endpoint updated to {}",
            peer.peer_ip,
            new_addr
        );
    }

    /// Timer loop: call update_timers on all peers periodically.
    async fn timer_loop(state: Arc<RwLock<WireGuardState>>, socket: Arc<UdpSocket>) {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(250));
        let mut timer_buf = vec![0u8; MAX_PACKET_SIZE];

        loop {
            interval.tick().await;

            let peers: Vec<Arc<PeerState>> = {
                let s = state.read().await;
                s.peers_by_key.values().cloned().collect()
            };

            for peer in &peers {
                let result = {
                    let mut tunn = peer.tunn.lock().await;
                    tunn.update_timers(&mut timer_buf)
                };

                match result {
                    TunnResult::Done => {}
                    TunnResult::Err(e) => {
                        log::debug!(
                            "wireguard: timer error for peer ip={}: {:?}",
                            peer.peer_ip,
                            e
                        );
                    }
                    TunnResult::WriteToNetwork(data) => {
                        let endpoint = {
                            let ep = peer.endpoint.read().await;
                            *ep
                        };
                        if let Some(addr) = endpoint {
                            let data = data.to_vec();
                            if let Err(e) = socket.send_to(&data, addr).await {
                                log::warn!("wireguard: timer send error: {}", e);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    /// Cleanup loop: process namespace channel removal requests from DropGuards.
    ///
    /// By funnelling removals through a channel instead of spawning async tasks
    /// from `Drop`, cleanup is ordered and can check the incarnation ID to avoid
    /// removing a newer channel that replaced the one being dropped.
    async fn cleanup_loop(
        state: Arc<RwLock<WireGuardState>>,
        mut cleanup_rx: mpsc::Receiver<CleanupRequest>,
    ) {
        while let Some(req) = cleanup_rx.recv().await {
            let mut s = state.write().await;
            if let Some(ch) = s.namespace_channels.get(&req.namespace_name) {
                if ch.id == req.id {
                    if let Some(ch) = s.namespace_channels.remove(&req.namespace_name) {
                        ch._egress_task.abort();
                        log::info!(
                            "wireguard: removed namespace channel for '{}' (id={})",
                            req.namespace_name,
                            req.id,
                        );
                    }
                } else {
                    log::debug!(
                        "wireguard: skipping stale cleanup for '{}' (requested id={}, current id={})",
                        req.namespace_name,
                        req.id,
                        ch.id,
                    );
                }
            }
        }
    }

    /// Egress task: read frames from the fabric and encrypt+send to peers.
    async fn egress_loop(
        state: Arc<RwLock<WireGuardState>>,
        socket: Arc<UdpSocket>,
        mut adapter_rx: mpsc::Receiver<Vec<u8>>,
        namespace_id: String,
    ) {
        let mut enc_buf = vec![0u8; MAX_PACKET_SIZE];

        while let Some(mut frame) = adapter_rx.recv().await {
            // Complete any deferred checksum offload before extracting the IP packet.
            complete_checksum(&mut frame);

            // Extract IP packet after vnet header.
            if frame.len() <= FABRIC_HDR_SZ {
                log::trace!(
                    "wireguard: egress: frame too short ({} bytes), skipping",
                    frame.len()
                );
                continue;
            }
            let ip_packet = &frame[FABRIC_HDR_SZ..];

            log::trace!(
                "wireguard: egress loop received {} byte frame from fabric ns={} (IP payload {} bytes)",
                frame.len(),
                namespace_id,
                ip_packet.len(),
            );

            // Log vnet header + IP/TCP details for debugging.
            if ip_packet.len() >= 20 {
                let vnet_flags = frame[0];
                let ip_proto = ip_packet[9];
                let ip_total_len = u16::from_be_bytes([ip_packet[2], ip_packet[3]]);
                let src_ip = &ip_packet[12..16];
                let dst_ip_bytes = &ip_packet[16..20];
                if ip_proto == 6 && ip_packet.len() >= 40 {
                    // TCP: extract flags (byte 13 of TCP header, which starts at IHL*4)
                    let ihl = (ip_packet[0] & 0x0f) as usize * 4;
                    if ip_packet.len() >= ihl + 14 {
                        let tcp_flags = ip_packet[ihl + 13];
                        let src_port = u16::from_be_bytes([ip_packet[ihl], ip_packet[ihl + 1]]);
                        let dst_port = u16::from_be_bytes([ip_packet[ihl + 2], ip_packet[ihl + 3]]);
                        let tcp_csum =
                            u16::from_be_bytes([ip_packet[ihl + 16], ip_packet[ihl + 17]]);
                        let flag_str = format!(
                            "{}{}{}{}{}{}",
                            if tcp_flags & 0x02 != 0 { "SYN " } else { "" },
                            if tcp_flags & 0x10 != 0 { "ACK " } else { "" },
                            if tcp_flags & 0x04 != 0 { "RST " } else { "" },
                            if tcp_flags & 0x01 != 0 { "FIN " } else { "" },
                            if tcp_flags & 0x08 != 0 { "PSH " } else { "" },
                            if tcp_flags & 0x20 != 0 { "URG " } else { "" },
                        );
                        log::debug!(
                            "wireguard: egress TCP: vnet_flags=0x{:02x} {}.{}.{}.{}:{} -> {}.{}.{}.{}:{} flags=[{}] ip_len={} tcp_csum=0x{:04x}",
                            vnet_flags,
                            src_ip[0],
                            src_ip[1],
                            src_ip[2],
                            src_ip[3],
                            src_port,
                            dst_ip_bytes[0],
                            dst_ip_bytes[1],
                            dst_ip_bytes[2],
                            dst_ip_bytes[3],
                            dst_port,
                            flag_str.trim(),
                            ip_total_len,
                            tcp_csum,
                        );
                    }
                }
            }

            let dst_ip = match ip_packet_dst(ip_packet) {
                Some(ip) => ip,
                None => {
                    log::trace!("wireguard: egress: could not extract dst IP from packet");
                    continue;
                }
            };
            log::trace!("wireguard: egress: IPv4 packet dst_ip={}", dst_ip);

            // Find the peer by destination IP in this namespace.
            let peer = {
                let s = state.read().await;
                let mut found = None;
                for p in s.peers_by_key.values() {
                    if p.namespace_id == namespace_id && p.peer_ip == dst_ip {
                        found = Some(Arc::clone(p));
                        break;
                    }
                }
                found
            };

            let peer = match peer {
                Some(p) => p,
                None => {
                    log::debug!(
                        "wireguard: egress: no peer found for dst_ip={} in ns={}",
                        dst_ip,
                        namespace_id
                    );
                    continue;
                }
            };

            let endpoint = {
                let ep = peer.endpoint.read().await;
                *ep
            };

            let endpoint = match endpoint {
                Some(a) => a,
                None => {
                    log::debug!(
                        "wireguard: egress: peer ip={} has no endpoint yet",
                        peer.peer_ip
                    );
                    continue;
                }
            };

            let result = {
                let mut tunn = peer.tunn.lock().await;
                tunn.encapsulate(ip_packet, &mut enc_buf)
            };

            match result {
                TunnResult::WriteToNetwork(data) => {
                    log::trace!(
                        "wireguard: egress: sending {} byte encrypted packet to {}",
                        data.len(),
                        endpoint
                    );
                    let data = data.to_vec();
                    if let Err(e) = socket.send_to(&data, endpoint).await {
                        log::warn!("wireguard: egress send error: {}", e);
                    }
                }
                TunnResult::Err(e) => {
                    log::debug!("wireguard: egress: encapsulate error: {:?}", e);
                }
                other => {
                    log::debug!(
                        "wireguard: egress: unexpected encapsulate result: {:?}",
                        std::mem::discriminant(&other)
                    );
                }
            }
        }
    }
}

#[async_trait]
impl IngressAdapter for WireGuardAdapter {
    fn adapter_type(&self) -> &str {
        "wireguard"
    }

    async fn create_port(
        &self,
        namespace_id: &distvirt_worker_protocol::NamespaceId,
    ) -> anyhow::Result<(ChannelPort, AdapterPortHandle)> {
        let (port, adapter_tx, adapter_rx) = ChannelPort::new(256);

        let ns_id = namespace_id.name.clone();
        let egress_task = tokio::spawn(Self::egress_loop(
            Arc::clone(&self.state),
            Arc::clone(&self.udp_socket),
            adapter_rx,
            ns_id.clone(),
        ));

        let ns_incarnation = namespace_id.id;

        // Store the namespace channel in state.
        {
            let mut s = self.state.write().await;
            s.namespace_channels.insert(
                ns_id.clone(),
                NamespaceChannel {
                    adapter_tx,
                    _egress_task: egress_task,
                    id: ns_incarnation,
                },
            );
        }

        // Create a drop guard that sends a cleanup request on drop.
        let drop_guard = DropGuard {
            cleanup_tx: self.cleanup_tx.clone(),
            namespace_name: ns_id,
            id: ns_incarnation,
        };

        let handle = AdapterPortHandle {
            _drop_guard: Some(Box::new(drop_guard)),
        };

        Ok((port, handle))
    }
}

/// Drop guard that sends a cleanup request when the adapter port is dropped.
///
/// Instead of spawning an async task from `Drop` (which races with re-creation),
/// this sends a `CleanupRequest` on a channel. The cleanup loop checks the
/// incarnation ID before removing, preventing stale cleanup from affecting a
/// newer namespace channel.
struct DropGuard {
    cleanup_tx: mpsc::Sender<CleanupRequest>,
    namespace_name: String,
    id: u64,
}

impl Drop for DropGuard {
    fn drop(&mut self) {
        let _ = self.cleanup_tx.try_send(CleanupRequest {
            namespace_name: self.namespace_name.clone(),
            id: self.id,
        });
    }
}

/// Simple hex encoding for logging (avoids adding hex crate dependency).
mod hex {
    pub fn encode(bytes: impl AsRef<[u8]>) -> String {
        bytes
            .as_ref()
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::super::IngressAdapter;
    use super::*;
    use crate::fabric::port::FramePort;
    use distvirt_worker_protocol::NamespaceId;
    use crate::packet::FABRIC_HDR_SZ;
    use boringtun::noise::{Tunn, TunnResult};
    use boringtun::x25519::{PublicKey, StaticSecret};
    use std::net::{Ipv4Addr, SocketAddr};
    use tokio::net::UdpSocket;
    use tokio::sync::Mutex;

    const CLIENT_PRIVATE_KEY: [u8; 32] = [
        0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8, 0xa9, 0xaa, 0xab, 0xac, 0xad, 0xae, 0xaf,
        0xb0, 0xb1, 0xb2, 0xb3, 0xb4, 0xb5, 0xb6, 0xb7, 0xb8, 0xb9, 0xba, 0xbb, 0xbc, 0xbd, 0xbe,
        0xbf, 0xc0,
    ];

    const CLIENT2_PRIVATE_KEY: [u8; 32] = [
        0xc1, 0xc2, 0xc3, 0xc4, 0xc5, 0xc6, 0xc7, 0xc8, 0xc9, 0xca, 0xcb, 0xcc, 0xcd, 0xce, 0xcf,
        0xd0, 0xd1, 0xd2, 0xd3, 0xd4, 0xd5, 0xd6, 0xd7, 0xd8, 0xd9, 0xda, 0xdb, 0xdc, 0xdd, 0xde,
        0xdf, 0xe0,
    ];

    fn pubkey_bytes(private_key: &[u8; 32]) -> [u8; 32] {
        let secret = StaticSecret::from(*private_key);
        let public = PublicKey::from(&secret);
        public.to_bytes()
    }

    async fn make_adapter() -> WireGuardAdapter {
        WireGuardAdapter::new(0)
            .await
            .expect("failed to create adapter")
    }

    fn adapter_port(adapter: &WireGuardAdapter) -> u16 {
        adapter
            .udp_socket
            .local_addr()
            .expect("failed to get local addr")
            .port()
    }

    /// Build a minimal valid 20-byte IPv4 packet.
    fn make_ip_packet(src: Ipv4Addr, dst: Ipv4Addr) -> Vec<u8> {
        let mut pkt = vec![0u8; 20];
        pkt[0] = 0x45; // version 4, IHL 5
        pkt[2] = 0x00; // total length high
        pkt[3] = 0x14; // total length = 20
        pkt[8] = 0x40; // TTL = 64
        pkt[9] = 0x11; // protocol = UDP
        pkt[12..16].copy_from_slice(&src.octets());
        pkt[16..20].copy_from_slice(&dst.octets());
        pkt
    }

    fn create_client_tunn(client_key: &[u8; 32], server_pub: &[u8; 32]) -> Tunn {
        let client_secret = StaticSecret::from(*client_key);
        let server_public = PublicKey::from(*server_pub);
        Tunn::new(client_secret, server_public, None, None, 0, None)
    }

    /// Perform a WireGuard handshake: client initiates, exchanges messages
    /// with the adapter's UDP socket via loopback.
    async fn perform_handshake(tunn: &Mutex<Tunn>, socket: &UdpSocket, adapter_addr: SocketAddr) {
        let mut buf = vec![0u8; MAX_PACKET_SIZE];

        // Step 1: Client initiates handshake.
        let initiation = {
            let mut t = tunn.lock().await;
            t.format_handshake_initiation(&mut buf, false)
        };
        let init_data = match initiation {
            TunnResult::WriteToNetwork(data) => data.to_vec(),
            _ => panic!("expected handshake initiation WriteToNetwork"),
        };
        socket
            .send_to(&init_data, adapter_addr)
            .await
            .expect("send init failed");

        // Step 2: Receive response from adapter.
        let mut recv_buf = vec![0u8; MAX_PACKET_SIZE];
        let (n, _) = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            socket.recv_from(&mut recv_buf),
        )
        .await
        .expect("handshake response timed out")
        .expect("recv failed");

        // Step 3: Client processes server response.
        let mut dec_buf = vec![0u8; MAX_PACKET_SIZE];
        let result = {
            let mut t = tunn.lock().await;
            t.decapsulate(Some(adapter_addr.ip()), &recv_buf[..n], &mut dec_buf)
        };

        // The response may produce a follow-up WriteToNetwork (transport data confirmation).
        match result {
            TunnResult::Done => {}
            TunnResult::WriteToNetwork(data) => {
                let data = data.to_vec();
                socket
                    .send_to(&data, adapter_addr)
                    .await
                    .expect("send follow-up failed");
            }
            _ => panic!("unexpected TunnResult during handshake"),
        }

        // Small delay for the adapter to process the final handshake message.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // ── Category 1: Unit tests (peer management) ──

    #[tokio::test]
    async fn test_add_peer_success() {
        let adapter = make_adapter().await;
        let client_pub = pubkey_bytes(&CLIENT_PRIVATE_KEY);

        let result = adapter
            .add_peer(&NamespaceId::new("ns1", 0), client_pub, Ipv4Addr::new(10, 0, 0, 2), None)
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_add_peer_remove_peer_lifecycle() {
        let adapter = make_adapter().await;
        let client_pub = pubkey_bytes(&CLIENT_PRIVATE_KEY);

        adapter
            .add_peer(&NamespaceId::new("ns1", 0), client_pub, Ipv4Addr::new(10, 0, 0, 2), None)
            .await
            .expect("add_peer failed");

        adapter
            .remove_peer(&client_pub)
            .await
            .expect("remove_peer failed");

        // Remove again — should still return Ok.
        adapter
            .remove_peer(&client_pub)
            .await
            .expect("remove_peer of already-removed peer failed");
    }

    #[tokio::test]
    async fn test_multiple_peers_same_namespace() {
        let adapter = make_adapter().await;
        let pub1 = pubkey_bytes(&CLIENT_PRIVATE_KEY);
        let pub2 = pubkey_bytes(&CLIENT2_PRIVATE_KEY);

        adapter
            .add_peer(&NamespaceId::new("ns1", 0), pub1, Ipv4Addr::new(10, 0, 0, 2), None)
            .await
            .expect("add peer 1 failed");

        adapter
            .add_peer(&NamespaceId::new("ns1", 0), pub2, Ipv4Addr::new(10, 0, 0, 3), None)
            .await
            .expect("add peer 2 failed");

        let state = adapter.state.read().await;
        assert_eq!(state.peers_by_key.len(), 2);
    }

    #[tokio::test]
    async fn test_multiple_peers_different_namespaces() {
        let adapter = make_adapter().await;
        let pub1 = pubkey_bytes(&CLIENT_PRIVATE_KEY);
        let pub2 = pubkey_bytes(&CLIENT2_PRIVATE_KEY);

        adapter
            .add_peer(&NamespaceId::new("ns1", 0), pub1, Ipv4Addr::new(10, 0, 0, 2), None)
            .await
            .expect("add peer ns1 failed");

        adapter
            .add_peer(&NamespaceId::new("ns2", 0), pub2, Ipv4Addr::new(10, 0, 0, 3), None)
            .await
            .expect("add peer ns2 failed");

        let state = adapter.state.read().await;
        let peer1 = state.peers_by_key.get(&pub1).expect("peer1 missing");
        let peer2 = state.peers_by_key.get(&pub2).expect("peer2 missing");
        assert_eq!(peer1.namespace_id, "ns1");
        assert_eq!(peer2.namespace_id, "ns2");
    }

    #[tokio::test]
    async fn test_remove_nonexistent_peer() {
        let adapter = make_adapter().await;
        let unknown_key = [0xffu8; 32];
        let result = adapter.remove_peer(&unknown_key).await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_create_port() {
        let adapter = make_adapter().await;
        let (_port, _handle) = adapter
            .create_port(&NamespaceId::new("ns1", 0))
            .await
            .expect("create_port failed");

        // Verify namespace channel was registered.
        let state = adapter.state.read().await;
        assert!(state.namespace_channels.contains_key("ns1"));
    }

    // ── Category 2: Integration tests (real boringtun crypto) ──

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_wireguard_handshake() {
        let adapter = make_adapter().await;
        let server_pub = adapter.public_key();
        let client_pub = pubkey_bytes(&CLIENT_PRIVATE_KEY);
        let port = adapter_port(&adapter);

        // Create port + add peer.
        let (_port, _handle) = adapter
            .create_port(&NamespaceId::new("ns1", 0))
            .await
            .expect("create_port failed");
        adapter
            .add_peer(&NamespaceId::new("ns1", 0), client_pub, Ipv4Addr::new(10, 0, 0, 2), None)
            .await
            .expect("add_peer failed");

        // Client-side Tunn + UDP socket.
        let client_tunn = Mutex::new(create_client_tunn(&CLIENT_PRIVATE_KEY, &server_pub));
        let client_socket = UdpSocket::bind("127.0.0.1:0").await.expect("bind failed");

        let adapter_addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

        // Perform handshake — should complete without panic/timeout.
        perform_handshake(&client_tunn, &client_socket, adapter_addr).await;

        // Verify the peer's endpoint was set.
        let state = adapter.state.read().await;
        let peer = state.peers_by_key.get(&client_pub).expect("peer missing");
        let endpoint = peer.endpoint.read().await;
        assert!(
            endpoint.is_some(),
            "peer endpoint should be set after handshake"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_wireguard_ingress_ip_packet() {
        let adapter = make_adapter().await;
        let server_pub = adapter.public_key();
        let client_pub = pubkey_bytes(&CLIENT_PRIVATE_KEY);
        let port = adapter_port(&adapter);

        let (fabric_port, _handle) = adapter
            .create_port(&NamespaceId::new("ns1", 0))
            .await
            .expect("create_port failed");
        adapter
            .add_peer(&NamespaceId::new("ns1", 0), client_pub, Ipv4Addr::new(10, 0, 0, 2), None)
            .await
            .expect("add_peer failed");

        let client_tunn = Mutex::new(create_client_tunn(&CLIENT_PRIVATE_KEY, &server_pub));
        let client_socket = UdpSocket::bind("127.0.0.1:0").await.expect("bind failed");
        let adapter_addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

        // Complete handshake first.
        perform_handshake(&client_tunn, &client_socket, adapter_addr).await;

        // Client encrypts an IP packet and sends it.
        let ip_packet = make_ip_packet(Ipv4Addr::new(10, 0, 0, 2), Ipv4Addr::new(10, 0, 0, 1));
        let mut enc_buf = vec![0u8; MAX_PACKET_SIZE];
        let encrypted = {
            let mut t = client_tunn.lock().await;
            t.encapsulate(&ip_packet, &mut enc_buf)
        };
        let enc_data = match encrypted {
            TunnResult::WriteToNetwork(data) => data.to_vec(),
            _ => panic!("expected encapsulate to produce WriteToNetwork"),
        };
        client_socket
            .send_to(&enc_data, adapter_addr)
            .await
            .expect("send encrypted packet failed");

        // Read the frame from the fabric port.
        let mut frame_buf = vec![0u8; MAX_PACKET_SIZE];
        let frame_len = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            fabric_port.recv_frame(&mut frame_buf),
        )
        .await
        .expect("fabric port recv timed out")
        .expect("fabric port recv failed");

        let frame = &frame_buf[..frame_len];

        // Verify frame structure: fabric_hdr + ip_packet (L3, no Ethernet header).
        assert!(
            frame.len() >= FABRIC_HDR_SZ + 20,
            "frame too short: {} bytes",
            frame.len()
        );

        // Verify the IP payload after the vnet header matches what was sent.
        let extracted_ip = &frame[FABRIC_HDR_SZ..];
        assert_eq!(extracted_ip, &ip_packet[..]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_wireguard_egress_ip_packet() {
        let adapter = make_adapter().await;
        let server_pub = adapter.public_key();
        let client_pub = pubkey_bytes(&CLIENT_PRIVATE_KEY);
        let port = adapter_port(&adapter);

        let (fabric_port, _handle) = adapter
            .create_port(&NamespaceId::new("ns1", 0))
            .await
            .expect("create_port failed");
        adapter
            .add_peer(&NamespaceId::new("ns1", 0), client_pub, Ipv4Addr::new(10, 0, 0, 2), None)
            .await
            .expect("add_peer failed");

        let client_tunn = Mutex::new(create_client_tunn(&CLIENT_PRIVATE_KEY, &server_pub));
        let client_socket = UdpSocket::bind("127.0.0.1:0").await.expect("bind failed");
        let adapter_addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

        // Complete handshake first (sets the peer endpoint).
        perform_handshake(&client_tunn, &client_socket, adapter_addr).await;

        // Build fabric frame destined for the peer's IP (L3: fabric_hdr + IP).
        let ip_packet = make_ip_packet(
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 2), // peer's IP
        );
        let frame = with_fabric_header(0, 0, &ip_packet);

        // Send frame into the fabric port (fabric→adapter direction).
        fabric_port
            .send_frame(&frame)
            .await
            .expect("send_frame failed");

        // Client receives the encrypted UDP datagram.
        let mut recv_buf = vec![0u8; MAX_PACKET_SIZE];
        let (n, _) = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client_socket.recv_from(&mut recv_buf),
        )
        .await
        .expect("client recv timed out")
        .expect("client recv failed");

        // Client decrypts the datagram.
        let mut dec_buf = vec![0u8; MAX_PACKET_SIZE];
        let result = {
            let mut t = client_tunn.lock().await;
            t.decapsulate(Some(adapter_addr.ip()), &recv_buf[..n], &mut dec_buf)
        };

        match result {
            TunnResult::WriteToTunnelV4(decrypted, _addr) => {
                assert_eq!(
                    decrypted,
                    &ip_packet[..],
                    "decrypted IP packet should match original"
                );
            }
            _ => panic!("expected WriteToTunnelV4 from decapsulate"),
        }
    }
}
