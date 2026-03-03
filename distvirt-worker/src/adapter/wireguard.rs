//! WireGuard ingress adapter using boringtun for userspace crypto.
//!
//! One `WireGuardAdapter` per worker owns a single UDP socket. Each WireGuard
//! peer maps to a namespace via its public key. The adapter bridges L3 (IP)
//! packets from peers into the L2 fabric as Ethernet frames.

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

use boringtun::noise::{Tunn, TunnResult, rate_limiter::RateLimiter};
use boringtun::x25519::{PublicKey, StaticSecret};
use sha2::{Digest, Sha256};
use tokio::net::UdpSocket;
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio::task::JoinHandle;

use crate::packet::{
    BROADCAST_MAC, ETHERTYPE_ARP, build_arp_reply, complete_checksum, fabric_frame_ethertype,
    fabric_frame_to_ip, ip_packet_dst, ip_to_fabric_frame, parse_arp_request,
};
use crate::fabric::ChannelPort;

use super::{AdapterPortHandle, IngressAdapter};

/// Maximum UDP datagram size (WireGuard max is ~65535 but typical MTU is much smaller).
const MAX_UDP_SIZE: usize = 65536;

/// Maximum encapsulated packet size.
const MAX_PACKET_SIZE: usize = 65536;

/// Derive a locally-administered unicast MAC from a peer's public key.
///
/// Uses `[0x02, SHA-256(pubkey)[0..5]]` — the 0x02 prefix marks it as
/// locally administered and unicast per IEEE 802.
fn mac_from_pubkey(pubkey: &[u8; 32]) -> [u8; 6] {
    let hash = Sha256::digest(pubkey);
    [0x02, hash[0], hash[1], hash[2], hash[3], hash[4]]
}

/// Per-peer WireGuard state.
struct PeerState {
    tunn: Mutex<Tunn>,
    namespace_id: String,
    peer_ip: Ipv4Addr,
    peer_mac: [u8; 6],
    endpoint: RwLock<Option<SocketAddr>>,
}

/// Per-namespace channel state for sending frames into the fabric.
struct NamespaceChannel {
    adapter_tx: mpsc::Sender<Vec<u8>>,
    _egress_task: JoinHandle<()>,
}

/// Shared mutable state for the WireGuard adapter.
struct WireGuardState {
    private_key: StaticSecret,
    peers_by_key: HashMap<[u8; 32], Arc<PeerState>>,
    peers_by_addr: HashMap<SocketAddr, Arc<PeerState>>,
    namespace_channels: HashMap<String, NamespaceChannel>,
    /// IP → MAC mappings for local pods, keyed by (namespace_id, pod_ip).
    /// Used to resolve destination MACs when injecting frames into the fabric.
    pod_macs: HashMap<(String, Ipv4Addr), [u8; 6]>,
    rate_limiter: Arc<RateLimiter>,
    /// Counter for Tunn index allocation.
    next_index: u32,
}

/// WireGuard ingress adapter.
///
/// Owns a UDP socket and manages peers that bridge external WireGuard clients
/// into the fabric's L2 network.
pub struct WireGuardAdapter {
    state: Arc<RwLock<WireGuardState>>,
    udp_socket: Arc<UdpSocket>,
    _udp_recv_task: JoinHandle<()>,
    _timer_task: JoinHandle<()>,
}

impl WireGuardAdapter {
    /// Create a new WireGuard adapter bound to the given port.
    pub async fn new(listen_port: u16, private_key_bytes: &[u8]) -> anyhow::Result<Self> {
        let mut key_array = [0u8; 32];
        let len = private_key_bytes.len().min(32);
        key_array[..len].copy_from_slice(&private_key_bytes[..len]);
        let private_key = StaticSecret::from(key_array);

        let public_key = PublicKey::from(&private_key);
        let rate_limiter = Arc::new(RateLimiter::new(&public_key, 100));

        let udp_socket = Arc::new(
            UdpSocket::bind(format!("0.0.0.0:{}", listen_port)).await?,
        );
        log::info!(
            "wireguard: listening on UDP port {}",
            udp_socket.local_addr()?
        );

        let state = Arc::new(RwLock::new(WireGuardState {
            private_key: StaticSecret::from(key_array),
            peers_by_key: HashMap::new(),
            peers_by_addr: HashMap::new(),
            namespace_channels: HashMap::new(),
            pod_macs: HashMap::new(),
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

        Ok(WireGuardAdapter {
            state,
            udp_socket,
            _udp_recv_task: udp_recv_task,
            _timer_task: timer_task,
        })
    }

    /// Add a WireGuard peer mapped to a namespace.
    pub async fn add_peer(
        &self,
        namespace_id: &str,
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
        )
        .map_err(|e| anyhow::anyhow!("failed to create WireGuard tunnel: {}", e))?;

        let peer_mac = mac_from_pubkey(&public_key);

        let peer = Arc::new(PeerState {
            tunn: Mutex::new(tunn),
            namespace_id: namespace_id.to_string(),
            peer_ip,
            peer_mac,
            endpoint: RwLock::new(None),
        });

        log::info!(
            "wireguard: added peer pubkey={} ip={} namespace={} mac={:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
            hex::encode(public_key),
            peer_ip,
            namespace_id,
            peer_mac[0], peer_mac[1], peer_mac[2], peer_mac[3], peer_mac[4], peer_mac[5],
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

    /// Register a local pod's IP→MAC mapping so the adapter can resolve
    /// destination MACs when injecting frames into the fabric.
    pub async fn register_pod_mac(&self, namespace_id: &str, ip: Ipv4Addr, mac: [u8; 6]) {
        let mut state = self.state.write().await;
        state.pod_macs.insert((namespace_id.to_string(), ip), mac);
        log::info!(
            "wireguard: registered pod mac ns={} ip={} mac={:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
            namespace_id, ip, mac[0], mac[1], mac[2], mac[3], mac[4], mac[5],
        );
    }

    /// Unregister a local pod's IP→MAC mapping.
    pub async fn unregister_pod_mac(&self, namespace_id: &str, ip: Ipv4Addr) {
        let mut state = self.state.write().await;
        state.pod_macs.remove(&(namespace_id.to_string(), ip));
    }

    /// UDP receive loop: read datagrams and feed them through boringtun.
    async fn udp_recv_loop(
        state: Arc<RwLock<WireGuardState>>,
        socket: Arc<UdpSocket>,
    ) {
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
            log::trace!("wireguard: received {} byte UDP packet from {}", n, src_addr);

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
            s.peers_by_key.iter().map(|(k, v)| (*k, Arc::clone(v))).collect()
        };

        for (_key, peer) in &peers {
            let result = {
                let mut tunn = peer.tunn.lock().await;
                tunn.decapsulate(Some(src_addr.ip()), datagram, dec_buf)
            };

            match &result {
                TunnResult::Done | TunnResult::WriteToNetwork(_) | TunnResult::WriteToTunnelV4(..) | TunnResult::WriteToTunnelV6(..) => {
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
                log::debug!("wireguard: sending {} byte response to {} (handshake/cookie)", data.len(), src_addr);
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
                let s = state.read().await;

                // Resolve destination MAC from pod_macs table.
                // Use unicast MAC if known, otherwise fall back to broadcast.
                let dst_mac = dst_ip
                    .and_then(|ip| s.pod_macs.get(&(peer.namespace_id.clone(), ip)).copied())
                    .unwrap_or(BROADCAST_MAC);

                log::trace!(
                    "wireguard: decrypted {} byte IP packet from peer ip={}, dst_ip={:?}, dst_mac={:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}, injecting into fabric ns={}",
                    ip_packet.len(), peer.peer_ip, dst_ip,
                    dst_mac[0], dst_mac[1], dst_mac[2], dst_mac[3], dst_mac[4], dst_mac[5],
                    peer.namespace_id,
                );

                // Log TCP details for incoming packets too.
                if ip_packet.len() >= 40 && ip_packet[9] == 6 {
                    let ihl = (ip_packet[0] & 0x0f) as usize * 4;
                    if ip_packet.len() >= ihl + 14 {
                        let tcp_flags = ip_packet[ihl + 13];
                        let src_port = u16::from_be_bytes([ip_packet[ihl], ip_packet[ihl + 1]]);
                        let dst_port = u16::from_be_bytes([ip_packet[ihl + 2], ip_packet[ihl + 3]]);
                        let flag_str = format!("{}{}{}{}",
                            if tcp_flags & 0x02 != 0 { "SYN " } else { "" },
                            if tcp_flags & 0x10 != 0 { "ACK " } else { "" },
                            if tcp_flags & 0x04 != 0 { "RST " } else { "" },
                            if tcp_flags & 0x01 != 0 { "FIN " } else { "" },
                        );
                        log::debug!(
                            "wireguard: ingress TCP: {}.{}.*.*:{} -> {}.{}.*.*:{} flags=[{}]",
                            ip_packet[12], ip_packet[13],
                            src_port,
                            ip_packet[16], ip_packet[17],
                            dst_port,
                            flag_str.trim(),
                        );
                    }
                }

                let frame = ip_to_fabric_frame(ip_packet, &peer.peer_mac, &dst_mac);
                if let Some(ns_ch) = s.namespace_channels.get(&peer.namespace_id) {
                    if let Err(e) = ns_ch.adapter_tx.try_send(frame) {
                        log::warn!(
                            "wireguard: failed to send frame to fabric ns={}: {}",
                            peer.namespace_id,
                            e
                        );
                    }
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
    async fn timer_loop(
        state: Arc<RwLock<WireGuardState>>,
        socket: Arc<UdpSocket>,
    ) {
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
                        log::debug!("wireguard: timer error for peer ip={}: {:?}", peer.peer_ip, e);
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
            let ethertype_val = fabric_frame_ethertype(&frame);
            log::trace!(
                "wireguard: egress loop received {} byte frame from fabric ns={}, ethertype={:?}{}",
                frame.len(), namespace_id, ethertype_val.map(|e| format!("0x{:04x}", e)),
                if frame.len() >= 24 {
                    format!(", dst_mac={:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}, src_mac={:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
                        frame[10], frame[11], frame[12], frame[13], frame[14], frame[15],
                        frame[16], frame[17], frame[18], frame[19], frame[20], frame[21])
                } else { String::new() }
            );
            // Check for ARP requests first.
            if let Some(ethertype) = ethertype_val {
                if ethertype == ETHERTYPE_ARP {
                    log::trace!("wireguard: egress: ARP frame (ethertype 0x0806)");
                    if let Some((target_ip, sender_mac, sender_ip)) = parse_arp_request(&frame) {
                        // Check if the target IP matches any peer in this namespace.
                        let peer_mac = {
                            let s = state.read().await;
                            let mut found = None;
                            for peer in s.peers_by_key.values() {
                                if peer.namespace_id == namespace_id && peer.peer_ip == target_ip {
                                    found = Some(peer.peer_mac);
                                    break;
                                }
                            }
                            found
                        };

                        if let Some(peer_mac) = peer_mac {
                            let reply = build_arp_reply(
                                &sender_mac,
                                sender_ip,
                                &peer_mac,
                                target_ip,
                            );
                            // Send reply back into the fabric.
                            let s = state.read().await;
                            if let Some(ns_ch) = s.namespace_channels.get(&namespace_id) {
                                let _ = ns_ch.adapter_tx.try_send(reply);
                            }
                        }
                    }
                    continue;
                }
            }

            // IPv4 frame: extract IP packet and send to the appropriate peer.
            let ip_packet = match fabric_frame_to_ip(&frame) {
                Some(p) => p,
                None => {
                    log::trace!("wireguard: egress: frame is not IPv4, skipping");
                    continue;
                }
            };

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
                        let tcp_csum = u16::from_be_bytes([ip_packet[ihl + 16], ip_packet[ihl + 17]]);
                        let flag_str = format!("{}{}{}{}{}{}",
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
                            src_ip[0], src_ip[1], src_ip[2], src_ip[3], src_port,
                            dst_ip_bytes[0], dst_ip_bytes[1], dst_ip_bytes[2], dst_ip_bytes[3], dst_port,
                            flag_str.trim(), ip_total_len, tcp_csum,
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
                    log::debug!("wireguard: egress: no peer found for dst_ip={} in ns={}", dst_ip, namespace_id);
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
                    log::debug!("wireguard: egress: peer ip={} has no endpoint yet", peer.peer_ip);
                    continue;
                }
            };

            let result = {
                let mut tunn = peer.tunn.lock().await;
                tunn.encapsulate(ip_packet, &mut enc_buf)
            };

            match result {
                TunnResult::WriteToNetwork(data) => {
                    log::trace!("wireguard: egress: sending {} byte encrypted packet to {}", data.len(), endpoint);
                    let data = data.to_vec();
                    if let Err(e) = socket.send_to(&data, endpoint).await {
                        log::warn!("wireguard: egress send error: {}", e);
                    }
                }
                TunnResult::Err(e) => {
                    log::debug!("wireguard: egress: encapsulate error: {:?}", e);
                }
                other => {
                    log::debug!("wireguard: egress: unexpected encapsulate result: {:?}", std::mem::discriminant(&other));
                }
            }
        }
    }
}

impl IngressAdapter for WireGuardAdapter {
    fn adapter_type(&self) -> &str {
        "wireguard"
    }

    fn create_port(
        &self,
        namespace_id: &str,
    ) -> anyhow::Result<(ChannelPort, AdapterPortHandle)> {
        let (port, adapter_tx, adapter_rx) = ChannelPort::new(256);

        let ns_id = namespace_id.to_string();
        let egress_task = tokio::spawn(Self::egress_loop(
            Arc::clone(&self.state),
            Arc::clone(&self.udp_socket),
            adapter_rx,
            ns_id.clone(),
        ));

        // Store the namespace channel in state.
        // We need to block_in_place since we're in a sync fn but need async.
        let state = Arc::clone(&self.state);
        let ns_id_clone = ns_id.clone();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut s = state.write().await;
                s.namespace_channels.insert(
                    ns_id_clone,
                    NamespaceChannel {
                        adapter_tx,
                        _egress_task: egress_task,
                    },
                );
            });
        });

        // Create a drop guard that removes the namespace channel.
        let drop_state = Arc::clone(&self.state);
        let drop_ns_id = ns_id;
        let drop_guard = DropGuard {
            state: drop_state,
            namespace_id: drop_ns_id,
        };

        let handle = AdapterPortHandle {
            _drop_guard: Some(Box::new(drop_guard)),
        };

        Ok((port, handle))
    }
}

/// Drop guard that removes a namespace channel when the adapter port is dropped.
struct DropGuard {
    state: Arc<RwLock<WireGuardState>>,
    namespace_id: String,
}

impl Drop for DropGuard {
    fn drop(&mut self) {
        let state = self.state.clone();
        let ns_id = self.namespace_id.clone();
        // Best-effort async cleanup from Drop.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let mut s = state.write().await;
                if let Some(ch) = s.namespace_channels.remove(&ns_id) {
                    ch._egress_task.abort();
                    log::info!(
                        "wireguard: removed namespace channel for '{}'",
                        ns_id
                    );
                }
            });
        }
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
    use super::*;
    use crate::packet::{
        VNET_HDR_SZ, ETH_HDR_LEN, fabric_frame_to_ip, ip_to_fabric_frame,
    };
    use super::super::IngressAdapter;
    use crate::fabric::port::FramePort;
    use boringtun::noise::{Tunn, TunnResult};
    use boringtun::x25519::{PublicKey, StaticSecret};
    use sha2::{Digest, Sha256};
    use std::net::{Ipv4Addr, SocketAddr};
    use tokio::net::UdpSocket;
    use tokio::sync::Mutex;

    // Fixed key material (deterministic, no randomness needed).
    const SERVER_PRIVATE_KEY: [u8; 32] = [
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
        0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
        0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
        0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20,
    ];

    const CLIENT_PRIVATE_KEY: [u8; 32] = [
        0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8,
        0xa9, 0xaa, 0xab, 0xac, 0xad, 0xae, 0xaf, 0xb0,
        0xb1, 0xb2, 0xb3, 0xb4, 0xb5, 0xb6, 0xb7, 0xb8,
        0xb9, 0xba, 0xbb, 0xbc, 0xbd, 0xbe, 0xbf, 0xc0,
    ];

    const CLIENT2_PRIVATE_KEY: [u8; 32] = [
        0xc1, 0xc2, 0xc3, 0xc4, 0xc5, 0xc6, 0xc7, 0xc8,
        0xc9, 0xca, 0xcb, 0xcc, 0xcd, 0xce, 0xcf, 0xd0,
        0xd1, 0xd2, 0xd3, 0xd4, 0xd5, 0xd6, 0xd7, 0xd8,
        0xd9, 0xda, 0xdb, 0xdc, 0xdd, 0xde, 0xdf, 0xe0,
    ];

    fn pubkey_bytes(private_key: &[u8; 32]) -> [u8; 32] {
        let secret = StaticSecret::from(*private_key);
        let public = PublicKey::from(&secret);
        public.to_bytes()
    }

    async fn make_adapter(key: &[u8; 32]) -> WireGuardAdapter {
        WireGuardAdapter::new(0, key).await.expect("failed to create adapter")
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
        Tunn::new(
            client_secret,
            server_public,
            None,
            None,
            0,
            None,
        )
        .expect("failed to create client Tunn")
    }

    /// Perform a WireGuard handshake: client initiates, exchanges messages
    /// with the adapter's UDP socket via loopback.
    async fn perform_handshake(
        tunn: &Mutex<Tunn>,
        socket: &UdpSocket,
        adapter_addr: SocketAddr,
    ) {
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
        socket.send_to(&init_data, adapter_addr).await.expect("send init failed");

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
                socket.send_to(&data, adapter_addr).await.expect("send follow-up failed");
            }
            _ => panic!("unexpected TunnResult during handshake"),
        }

        // Small delay for the adapter to process the final handshake message.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // ── Category 1: Unit tests (peer management) ──

    #[test]
    fn test_mac_from_pubkey_determinism() {
        let key_a = pubkey_bytes(&CLIENT_PRIVATE_KEY);
        let key_b = pubkey_bytes(&CLIENT2_PRIVATE_KEY);

        let mac_a1 = mac_from_pubkey(&key_a);
        let mac_a2 = mac_from_pubkey(&key_a);
        let mac_b = mac_from_pubkey(&key_b);

        // Same input → same MAC.
        assert_eq!(mac_a1, mac_a2);
        // Different input → different MAC.
        assert_ne!(mac_a1, mac_b);
        // Locally-administered unicast prefix.
        assert_eq!(mac_a1[0], 0x02);
        assert_eq!(mac_b[0], 0x02);
    }

    #[test]
    fn test_mac_from_pubkey_format() {
        let key = pubkey_bytes(&CLIENT_PRIVATE_KEY);
        let mac = mac_from_pubkey(&key);
        let hash = Sha256::digest(&key);

        assert_eq!(mac[0], 0x02);
        assert_eq!(mac[1], hash[0]);
        assert_eq!(mac[2], hash[1]);
        assert_eq!(mac[3], hash[2]);
        assert_eq!(mac[4], hash[3]);
        assert_eq!(mac[5], hash[4]);
    }

    #[tokio::test]
    async fn test_add_peer_success() {
        let adapter = make_adapter(&SERVER_PRIVATE_KEY).await;
        let client_pub = pubkey_bytes(&CLIENT_PRIVATE_KEY);

        let result = adapter
            .add_peer("ns1", client_pub, Ipv4Addr::new(10, 0, 0, 2), None)
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_add_peer_remove_peer_lifecycle() {
        let adapter = make_adapter(&SERVER_PRIVATE_KEY).await;
        let client_pub = pubkey_bytes(&CLIENT_PRIVATE_KEY);

        adapter
            .add_peer("ns1", client_pub, Ipv4Addr::new(10, 0, 0, 2), None)
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
        let adapter = make_adapter(&SERVER_PRIVATE_KEY).await;
        let pub1 = pubkey_bytes(&CLIENT_PRIVATE_KEY);
        let pub2 = pubkey_bytes(&CLIENT2_PRIVATE_KEY);

        adapter
            .add_peer("ns1", pub1, Ipv4Addr::new(10, 0, 0, 2), None)
            .await
            .expect("add peer 1 failed");

        adapter
            .add_peer("ns1", pub2, Ipv4Addr::new(10, 0, 0, 3), None)
            .await
            .expect("add peer 2 failed");

        let state = adapter.state.read().await;
        assert_eq!(state.peers_by_key.len(), 2);
    }

    #[tokio::test]
    async fn test_multiple_peers_different_namespaces() {
        let adapter = make_adapter(&SERVER_PRIVATE_KEY).await;
        let pub1 = pubkey_bytes(&CLIENT_PRIVATE_KEY);
        let pub2 = pubkey_bytes(&CLIENT2_PRIVATE_KEY);

        adapter
            .add_peer("ns1", pub1, Ipv4Addr::new(10, 0, 0, 2), None)
            .await
            .expect("add peer ns1 failed");

        adapter
            .add_peer("ns2", pub2, Ipv4Addr::new(10, 0, 0, 3), None)
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
        let adapter = make_adapter(&SERVER_PRIVATE_KEY).await;
        let unknown_key = [0xffu8; 32];
        let result = adapter.remove_peer(&unknown_key).await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_create_port() {
        let adapter = make_adapter(&SERVER_PRIVATE_KEY).await;
        let (_port, _handle) = adapter
            .create_port("ns1")
            .expect("create_port failed");

        // Verify namespace channel was registered.
        let state = adapter.state.read().await;
        assert!(state.namespace_channels.contains_key("ns1"));
    }

    // ── Category 2: Integration tests (real boringtun crypto) ──

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_wireguard_handshake() {
        let adapter = make_adapter(&SERVER_PRIVATE_KEY).await;
        let server_pub = pubkey_bytes(&SERVER_PRIVATE_KEY);
        let client_pub = pubkey_bytes(&CLIENT_PRIVATE_KEY);
        let port = adapter_port(&adapter);

        // Create port + add peer.
        let (_port, _handle) = adapter.create_port("ns1").expect("create_port failed");
        adapter
            .add_peer("ns1", client_pub, Ipv4Addr::new(10, 0, 0, 2), None)
            .await
            .expect("add_peer failed");

        // Client-side Tunn + UDP socket.
        let client_tunn = Mutex::new(create_client_tunn(&CLIENT_PRIVATE_KEY, &server_pub));
        let client_socket = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("bind failed");

        let adapter_addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

        // Perform handshake — should complete without panic/timeout.
        perform_handshake(&client_tunn, &client_socket, adapter_addr).await;

        // Verify the peer's endpoint was set.
        let state = adapter.state.read().await;
        let peer = state.peers_by_key.get(&client_pub).expect("peer missing");
        let endpoint = peer.endpoint.read().await;
        assert!(endpoint.is_some(), "peer endpoint should be set after handshake");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_wireguard_ingress_ip_packet() {
        let adapter = make_adapter(&SERVER_PRIVATE_KEY).await;
        let server_pub = pubkey_bytes(&SERVER_PRIVATE_KEY);
        let client_pub = pubkey_bytes(&CLIENT_PRIVATE_KEY);
        let port = adapter_port(&adapter);

        let (fabric_port, _handle) = adapter.create_port("ns1").expect("create_port failed");
        adapter
            .add_peer("ns1", client_pub, Ipv4Addr::new(10, 0, 0, 2), None)
            .await
            .expect("add_peer failed");

        let client_tunn = Mutex::new(create_client_tunn(&CLIENT_PRIVATE_KEY, &server_pub));
        let client_socket = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("bind failed");
        let adapter_addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

        // Complete handshake first.
        perform_handshake(&client_tunn, &client_socket, adapter_addr).await;

        // Client encrypts an IP packet and sends it.
        let ip_packet = make_ip_packet(
            Ipv4Addr::new(10, 0, 0, 2),
            Ipv4Addr::new(10, 0, 0, 1),
        );
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

        // Verify frame structure: vnet_hdr + eth_hdr + ip_packet.
        assert!(
            frame.len() >= VNET_HDR_SZ + ETH_HDR_LEN + 20,
            "frame too short: {} bytes",
            frame.len()
        );

        // Verify ethertype is IPv4.
        let ethertype = u16::from_be_bytes([
            frame[VNET_HDR_SZ + 12],
            frame[VNET_HDR_SZ + 13],
        ]);
        assert_eq!(ethertype, 0x0800, "expected IPv4 ethertype");

        // Verify source MAC matches peer's derived MAC.
        let expected_mac = mac_from_pubkey(&client_pub);
        let src_mac = &frame[VNET_HDR_SZ + 6..VNET_HDR_SZ + 12];
        assert_eq!(src_mac, &expected_mac, "source MAC should match peer MAC");

        // Verify the extracted IP payload matches what was sent.
        let extracted_ip = fabric_frame_to_ip(frame).expect("failed to extract IP from frame");
        assert_eq!(extracted_ip, &ip_packet[..]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_wireguard_egress_ip_packet() {
        let adapter = make_adapter(&SERVER_PRIVATE_KEY).await;
        let server_pub = pubkey_bytes(&SERVER_PRIVATE_KEY);
        let client_pub = pubkey_bytes(&CLIENT_PRIVATE_KEY);
        let port = adapter_port(&adapter);

        let (fabric_port, _handle) = adapter.create_port("ns1").expect("create_port failed");
        adapter
            .add_peer("ns1", client_pub, Ipv4Addr::new(10, 0, 0, 2), None)
            .await
            .expect("add_peer failed");

        let client_tunn = Mutex::new(create_client_tunn(&CLIENT_PRIVATE_KEY, &server_pub));
        let client_socket = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("bind failed");
        let adapter_addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

        // Complete handshake first (sets the peer endpoint).
        perform_handshake(&client_tunn, &client_socket, adapter_addr).await;

        // Build fabric frame destined for the peer's IP.
        let ip_packet = make_ip_packet(
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 2), // peer's IP
        );
        let src_mac = [0x02, 0x11, 0x22, 0x33, 0x44, 0x55];
        let dst_mac = mac_from_pubkey(&client_pub);
        let frame = ip_to_fabric_frame(&ip_packet, &src_mac, &dst_mac);

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
                assert_eq!(decrypted, &ip_packet[..], "decrypted IP packet should match original");
            }
            _ => panic!("expected WriteToTunnelV4 from decapsulate"),
        }
    }
}
