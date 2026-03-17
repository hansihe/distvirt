//! UDP tunnel transport for inter-worker fabric forwarding.
//!
//! `TunnelTransport` owns a single UDP socket and multiplexes multiple
//! namespace segments over it using the `segment_id` field in the fabric
//! header.
//!
//! **Egress** (fabric → UDP): per-segment egress task reads from the
//! fabric-side `adapter_rx`, completes checksums, stamps `segment_id`,
//! and sends to the peer endpoint.
//!
//! **Ingress** (UDP → fabric): a single recv loop reads datagrams,
//! parses `segment_id` from the fabric header, and dispatches to the
//! matching namespace channel.
//!
//! When `encrypted = true`, all traffic is protected by Noise_IK (via
//! the `snow` crate). Each peer undergoes a 1-RTT handshake before
//! data frames flow. Initiation ordering is determined by comparing
//! the local and remote static public keys lexicographically — the
//! peer with the "smaller" key initiates. This ensures exactly one
//! side initiates without requiring out-of-band coordination.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use tokio::net::UdpSocket;
use tokio::sync::{mpsc, watch};

use super::port::ChannelPort;
use crate::packet::{FABRIC_HDR_SZ, complete_checksum};
use crate::task_handle::TaskHandle;

/// Noise protocol pattern string.
const NOISE_PATTERN: &str = "Noise_IK_25519_ChaChaPoly_BLAKE2s";

/// Create a snow Builder with the default crypto resolver.
fn noise_builder<'a>() -> snow::Builder<'a> {
    snow::Builder::new(NOISE_PATTERN.parse().unwrap())
}

/// Noise adds a 16-byte Poly1305 authentication tag.
const NOISE_TAG_LEN: usize = 16;

// ---------------------------------------------------------------------------
// Noise session state (per-peer)
// ---------------------------------------------------------------------------

/// Per-peer Noise session state.
enum NoiseSession {
    /// Handshake in progress.
    Handshaking(snow::HandshakeState),
    /// Handshake complete — transport-mode cipher.
    Transport(snow::TransportState),
}

// ---------------------------------------------------------------------------
// Shared transport state
// ---------------------------------------------------------------------------

/// State shared between the recv loop and the control API.
struct TunnelState {
    /// worker_id → peer endpoint address.
    peers: HashMap<String, PeerState>,
    /// segment_id → ingress channel for that namespace.
    segment_channels: HashMap<u16, mpsc::Sender<Vec<u8>>>,
    /// peer endpoint addr → worker_id (reverse lookup for recv loop).
    addr_to_worker: HashMap<SocketAddr, String>,
}

struct PeerState {
    endpoint: SocketAddr,
    /// Noise session, present only when encryption is enabled.
    noise: Option<NoiseSession>,
}

/// Encryption configuration for the transport.
struct EncryptionConfig {
    /// Our Noise static keypair.
    keypair: snow::Keypair,
}

/// A per-worker UDP tunnel transport.
///
/// Owns a UDP socket and a recv loop task. Create namespace-level tunnel
/// ports with [`create_namespace_port`](Self::create_namespace_port).
pub struct TunnelTransport {
    state: Arc<RwLock<TunnelState>>,
    udp_socket: Arc<UdpSocket>,
    _recv_task: TaskHandle<()>,
    encryption: Option<EncryptionConfig>,
    /// Sender to notify egress loops when handshake completes for a peer.
    handshake_done_tx: watch::Sender<u64>,
    /// Counter incremented each time a handshake completes, used to wake
    /// egress loops that are waiting for transport state.
    handshake_done_rx: watch::Receiver<u64>,
}

/// RAII handle returned by [`TunnelTransport::create_namespace_port`].
///
/// On drop, removes the segment channel from the transport state and
/// aborts the egress task.
pub struct TunnelPortHandle {
    segment_id: u16,
    state: Arc<RwLock<TunnelState>>,
    _egress_task: TaskHandle<()>,
}

impl Drop for TunnelPortHandle {
    fn drop(&mut self) {
        let mut st = self.state.write().expect("poisoned");
        st.segment_channels.remove(&self.segment_id);
        log::info!("tunnel: segment {} port handle dropped", self.segment_id);
    }
}

impl TunnelTransport {
    /// Bind a UDP socket and start the recv loop.
    ///
    /// When `encrypted` is true, a Noise static keypair is generated and
    /// all peer traffic will be encrypted with Noise_IK.
    pub async fn new(listen_addr: SocketAddr, encrypted: bool) -> io::Result<Self> {
        let udp_socket = Arc::new(UdpSocket::bind(listen_addr).await?);
        let state = Arc::new(RwLock::new(TunnelState {
            peers: HashMap::new(),
            segment_channels: HashMap::new(),
            addr_to_worker: HashMap::new(),
        }));

        let encryption = if encrypted {
            let builder = noise_builder();
            let keypair = builder.generate_keypair().map_err(|e| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("noise keypair generation failed: {}", e),
                )
            })?;
            Some(EncryptionConfig { keypair })
        } else {
            None
        };

        let (handshake_done_tx, handshake_done_rx) = watch::channel(0u64);

        let recv_task = TaskHandle::spawn(recv_loop(
            Arc::clone(&udp_socket),
            Arc::clone(&state),
            encrypted,
            handshake_done_tx.clone(),
        ));

        Ok(TunnelTransport {
            state,
            udp_socket,
            _recv_task: recv_task,
            encryption,
            handshake_done_tx,
            handshake_done_rx,
        })
    }

    /// The static public key for this transport (32 bytes), or `None` if
    /// encryption is disabled.
    pub fn public_key(&self) -> Option<&[u8]> {
        self.encryption
            .as_ref()
            .map(|e| e.keypair.public.as_slice())
    }

    /// Register a remote worker's endpoint address.
    ///
    /// When encryption is enabled, `peer_public_key` must be provided.
    /// `is_initiator` controls whether this side starts the Noise handshake.
    pub fn add_peer(
        &self,
        worker_id: String,
        endpoint: SocketAddr,
        peer_public_key: Option<&[u8; 32]>,
        is_initiator: bool,
    ) -> Result<(), io::Error> {
        let noise = match (&self.encryption, peer_public_key) {
            (Some(enc), Some(remote_pub)) => {
                let builder = noise_builder()
                    .local_private_key(&enc.keypair.private)
                    .map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::Other,
                            format!("noise: local_private_key failed: {}", e),
                        )
                    })?
                    .remote_public_key(remote_pub)
                    .map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::Other,
                            format!("noise: remote_public_key failed: {}", e),
                        )
                    })?;
                let hs = if is_initiator {
                    builder.build_initiator().map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::Other,
                            format!("noise: build_initiator failed: {}", e),
                        )
                    })?
                } else {
                    builder.build_responder().map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::Other,
                            format!("noise: build_responder failed: {}", e),
                        )
                    })?
                };
                Some(NoiseSession::Handshaking(hs))
            }
            _ => None,
        };

        {
            let mut st = self.state.write().expect("poisoned");
            st.addr_to_worker.insert(endpoint, worker_id.clone());
            st.peers
                .insert(worker_id.clone(), PeerState { endpoint, noise });
        }

        log::info!(
            "tunnel: added peer {} at {} (initiator={})",
            worker_id,
            endpoint,
            is_initiator
        );

        // If we're the initiator, send the first handshake message.
        if is_initiator && self.encryption.is_some() {
            self.send_handshake_msg(&worker_id);
        }

        Ok(())
    }

    /// Deregister a remote worker.
    pub fn remove_peer(&self, worker_id: &str) {
        let mut st = self.state.write().expect("poisoned");
        if let Some(peer) = st.peers.remove(worker_id) {
            st.addr_to_worker.remove(&peer.endpoint);
        }
        log::info!("tunnel: removed peer {}", worker_id);
    }

    /// Create a tunnel port for a specific namespace-peer pair.
    ///
    /// Returns a `ChannelPort` (to be added to the fabric) and a
    /// `TunnelPortHandle` that owns the egress task and cleans up on drop.
    ///
    /// `segment_id` identifies this namespace on the wire.
    /// `worker_id` identifies which peer to send egress traffic to.
    pub fn create_namespace_port(
        &self,
        worker_id: &str,
        segment_id: u16,
    ) -> io::Result<(ChannelPort, TunnelPortHandle)> {
        let endpoint = {
            let st = self.state.read().expect("poisoned");
            st.peers.get(worker_id).map(|p| p.endpoint).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("unknown peer: {}", worker_id),
                )
            })?
        };

        // Create a ChannelPort. The fabric writes to the port (fabric→adapter_rx),
        // and the egress loop reads from adapter_rx and sends over UDP.
        // The ingress side sends via adapter_tx into the fabric.
        let (port, adapter_tx, adapter_rx) = ChannelPort::new(256);

        // Register the segment channel for ingress demux.
        {
            let mut st = self.state.write().expect("poisoned");
            st.segment_channels.insert(segment_id, adapter_tx);
        }

        let encrypted = self.encryption.is_some();
        let worker_id_owned = worker_id.to_string();

        // Spawn egress loop.
        let egress_task = TaskHandle::spawn(egress_loop(
            adapter_rx,
            Arc::clone(&self.udp_socket),
            endpoint,
            segment_id,
            encrypted,
            Arc::clone(&self.state),
            worker_id_owned,
            self.handshake_done_rx.clone(),
        ));

        let handle = TunnelPortHandle {
            segment_id,
            state: Arc::clone(&self.state),
            _egress_task: egress_task,
        };

        log::info!(
            "tunnel: created namespace port for worker {} segment {}",
            worker_id,
            segment_id,
        );

        Ok((port, handle))
    }

    /// Get the local address of the UDP socket.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.udp_socket.local_addr()
    }

    /// Send the next handshake message for the given peer.
    fn send_handshake_msg(&self, worker_id: &str) {
        let mut st = self.state.write().expect("poisoned");
        let peer = match st.peers.get_mut(worker_id) {
            Some(p) => p,
            None => return,
        };
        let endpoint = peer.endpoint;

        if let Some(NoiseSession::Handshaking(ref mut hs)) = peer.noise {
            let mut buf = [0u8; 256];
            match hs.write_message(&[], &mut buf) {
                Ok(n) => {
                    let msg = buf[..n].to_vec();
                    let socket = Arc::clone(&self.udp_socket);
                    tokio::spawn(async move {
                        if let Err(e) = socket.send_to(&msg, endpoint).await {
                            log::warn!("tunnel: handshake send error: {}", e);
                        }
                    });
                    log::info!("tunnel: sent handshake message to {}", worker_id);
                }
                Err(e) => {
                    log::error!(
                        "tunnel: handshake write_message error for {}: {}",
                        worker_id,
                        e
                    );
                }
            }
        }
    }
}

/// Recv loop: read UDP datagrams, handle handshake or decrypt, parse
/// segment_id, dispatch to namespace channel.
async fn recv_loop(
    socket: Arc<UdpSocket>,
    state: Arc<RwLock<TunnelState>>,
    encrypted: bool,
    handshake_done_tx: watch::Sender<u64>,
) {
    // Max fabric frame: 3-byte header + 1500-byte MTU IP packet + some margin
    // + 16 bytes for Noise tag.
    let mut buf = [0u8; FABRIC_HDR_SZ + 1514 + NOISE_TAG_LEN];
    let mut decrypt_buf = [0u8; FABRIC_HDR_SZ + 1514];

    loop {
        let (n, peer_addr) = match socket.recv_from(&mut buf).await {
            Ok(r) => r,
            Err(e) => {
                log::warn!("tunnel: recv error: {}", e);
                continue;
            }
        };

        if encrypted {
            // Look up peer by source address.
            let worker_id = {
                let st = state.read().expect("poisoned");
                st.addr_to_worker.get(&peer_addr).cloned()
            };

            let worker_id = match worker_id {
                Some(id) => id,
                None => {
                    log::trace!("tunnel: datagram from unknown addr {}, dropping", peer_addr);
                    continue;
                }
            };

            // Check peer noise state.
            let mut st = state.write().expect("poisoned");
            let peer = match st.peers.get_mut(&worker_id) {
                Some(p) => p,
                None => continue,
            };

            match &mut peer.noise {
                Some(NoiseSession::Handshaking(hs)) => {
                    // Process incoming handshake message.
                    let mut payload_buf = [0u8; 256];
                    match hs.read_message(&buf[..n], &mut payload_buf) {
                        Ok(_) => {
                            log::info!("tunnel: processed handshake message from {}", worker_id);
                        }
                        Err(e) => {
                            log::error!(
                                "tunnel: handshake read_message error from {}: {}",
                                worker_id,
                                e
                            );
                            continue;
                        }
                    }

                    // If we need to send a response (responder after msg 1).
                    if !hs.is_handshake_finished() {
                        let mut resp_buf = [0u8; 256];
                        match hs.write_message(&[], &mut resp_buf) {
                            Ok(resp_n) => {
                                let msg = resp_buf[..resp_n].to_vec();
                                let sock = Arc::clone(&socket);
                                let ep = peer.endpoint;
                                tokio::spawn(async move {
                                    if let Err(e) = sock.send_to(&msg, ep).await {
                                        log::warn!("tunnel: handshake response send error: {}", e);
                                    }
                                });
                                log::info!("tunnel: sent handshake response to {}", worker_id);
                            }
                            Err(e) => {
                                log::error!(
                                    "tunnel: handshake write_message error for {}: {}",
                                    worker_id,
                                    e
                                );
                                continue;
                            }
                        }
                    }

                    // Check if handshake is now complete — transition to transport mode.
                    let finished = hs.is_handshake_finished();
                    if finished {
                        let Some(old) = peer.noise.take() else {
                            log::error!(
                                "tunnel: handshake finished but noise state missing for {}",
                                worker_id
                            );
                            continue;
                        };
                        if let NoiseSession::Handshaking(hs) = old {
                            match hs.into_transport_mode() {
                                Ok(transport) => {
                                    peer.noise = Some(NoiseSession::Transport(transport));
                                    log::info!(
                                        "tunnel: handshake with {} complete, transport mode active",
                                        worker_id
                                    );
                                    // Notify egress loops.
                                    let _ = handshake_done_tx.send_modify(|v| *v += 1);
                                }
                                Err(e) => {
                                    log::error!(
                                        "tunnel: into_transport_mode failed for {}: {}",
                                        worker_id,
                                        e
                                    );
                                    // peer.noise is now None — peer is broken but recv loop survives
                                }
                            }
                        }
                    }
                    continue;
                }
                Some(NoiseSession::Transport(ts)) => {
                    // Decrypt the datagram.
                    match ts.read_message(&buf[..n], &mut decrypt_buf) {
                        Ok(plaintext_len) => {
                            dispatch_frame(&st, &decrypt_buf[..plaintext_len]);
                        }
                        Err(e) => {
                            log::warn!("tunnel: decrypt error from {}: {}", worker_id, e);
                        }
                    }
                    continue;
                }
                None => {
                    // No encryption for this peer — shouldn't happen when
                    // transport is encrypted, but fall through to plaintext.
                }
            }

            // Drop write lock and fall through to plaintext path shouldn't happen.
            drop(st);
            continue;
        }

        // Plaintext mode.
        if n < FABRIC_HDR_SZ + 20 {
            // Too short for fabric header + minimal IP header.
            continue;
        }

        let st = state.read().expect("poisoned");
        dispatch_frame(&st, &buf[..n]);
    }
}

/// Parse segment_id from a plaintext frame and dispatch to the matching channel.
fn dispatch_frame(st: &TunnelState, frame: &[u8]) {
    if frame.len() < FABRIC_HDR_SZ + 20 {
        return;
    }

    // Parse segment_id from fabric header bytes [1..3] (network-endian u16).
    let segment_id = u16::from_be_bytes([frame[1], frame[2]]);

    if let Some(tx) = st.segment_channels.get(&segment_id) {
        let frame_vec = frame.to_vec();
        if tx.try_send(frame_vec).is_err() {
            log::debug!(
                "tunnel: ingress channel full/closed for segment {}, dropping",
                segment_id,
            );
        }
    } else {
        log::trace!("tunnel: unknown segment {}, dropping datagram", segment_id);
    }
}

/// Per-namespace egress loop: read frames from the fabric, complete checksums,
/// stamp segment_id, and send over UDP (encrypted if enabled).
async fn egress_loop(
    mut adapter_rx: mpsc::Receiver<Vec<u8>>,
    socket: Arc<UdpSocket>,
    endpoint: SocketAddr,
    segment_id: u16,
    encrypted: bool,
    state: Arc<RwLock<TunnelState>>,
    worker_id: String,
    mut handshake_done_rx: watch::Receiver<u64>,
) {
    let seg_bytes = segment_id.to_be_bytes();

    // If encrypted, wait until the peer's handshake has completed.
    if encrypted {
        loop {
            {
                let st = state.read().expect("poisoned");
                if let Some(peer) = st.peers.get(&worker_id) {
                    if matches!(&peer.noise, Some(NoiseSession::Transport(_))) {
                        break;
                    }
                } else {
                    // Peer removed, exit.
                    log::info!("tunnel: egress loop for segment {} peer gone", segment_id);
                    return;
                }
            }
            // Wait for a handshake completion notification.
            if handshake_done_rx.changed().await.is_err() {
                return;
            }
        }
        log::info!(
            "tunnel: egress loop for segment {} encryption ready",
            segment_id
        );
    }

    while let Some(mut frame) = adapter_rx.recv().await {
        if frame.len() < FABRIC_HDR_SZ {
            continue;
        }

        // Complete deferred checksum before leaving the fabric.
        complete_checksum(&mut frame);

        // Stamp segment_id into the fabric header.
        frame[1] = seg_bytes[0];
        frame[2] = seg_bytes[1];

        if encrypted {
            // Encrypt the frame.
            let mut encrypt_buf = vec![0u8; frame.len() + NOISE_TAG_LEN];
            let ciphertext_len = {
                let mut st = state.write().expect("poisoned");
                let peer = match st.peers.get_mut(&worker_id) {
                    Some(p) => p,
                    None => break,
                };
                match &mut peer.noise {
                    Some(NoiseSession::Transport(ts)) => {
                        match ts.write_message(&frame, &mut encrypt_buf) {
                            Ok(n) => n,
                            Err(e) => {
                                log::warn!(
                                    "tunnel: encrypt error for segment {}: {}",
                                    segment_id,
                                    e
                                );
                                continue;
                            }
                        }
                    }
                    _ => {
                        log::warn!(
                            "tunnel: peer {} not in transport mode, dropping frame",
                            worker_id
                        );
                        continue;
                    }
                }
            };

            if let Err(e) = socket
                .send_to(&encrypt_buf[..ciphertext_len], endpoint)
                .await
            {
                log::warn!("tunnel: egress send to {} failed: {}", endpoint, e);
            }
        } else {
            if let Err(e) = socket.send_to(&frame, endpoint).await {
                log::warn!("tunnel: egress send to {} failed: {}", endpoint, e,);
            }
        }
    }

    log::info!("tunnel: egress loop for segment {} ended", segment_id);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packet::FabricPacket;
    use crate::packet::with_fabric_header;

    /// Helper: build a minimal IPv4 fabric frame.
    fn make_ipv4_frame(src_ip: std::net::Ipv4Addr, dst_ip: std::net::Ipv4Addr) -> Vec<u8> {
        let mut ip_hdr = [0u8; 20];
        ip_hdr[0] = 0x45; // version=4, IHL=5
        ip_hdr[2..4].copy_from_slice(&20u16.to_be_bytes()); // total length
        ip_hdr[12..16].copy_from_slice(&src_ip.octets());
        ip_hdr[16..20].copy_from_slice(&dst_ip.octets());
        with_fabric_header(0, 0, &ip_hdr)
    }

    #[tokio::test]
    async fn test_tunnel_round_trip() {
        // Two transports on localhost, same segment_id, send A→B (plaintext).
        let transport_a = TunnelTransport::new("127.0.0.1:0".parse().unwrap(), false)
            .await
            .unwrap();
        let transport_b = TunnelTransport::new("127.0.0.1:0".parse().unwrap(), false)
            .await
            .unwrap();

        let addr_a = transport_a.local_addr().unwrap();
        let addr_b = transport_b.local_addr().unwrap();

        transport_a
            .add_peer("worker-b".into(), addr_b, None, false)
            .unwrap();
        transport_b
            .add_peer("worker-a".into(), addr_a, None, false)
            .unwrap();

        let segment_id = 42;

        let (port_a, _handle_a) = transport_a
            .create_namespace_port("worker-b", segment_id)
            .unwrap();
        let (_port_b, _handle_b) = transport_b
            .create_namespace_port("worker-a", segment_id)
            .unwrap();

        use super::super::port::FramePort;

        let frame = make_ipv4_frame(
            std::net::Ipv4Addr::new(10, 0, 0, 1),
            std::net::Ipv4Addr::new(10, 0, 0, 2),
        );

        // Send frame through port_a (fabric → egress → UDP → transport_b recv → ingress).
        port_a.send_frame(&frame).await.unwrap();

        // Read from port_b (ingress).
        let mut recv_buf = vec![0u8; 2048];
        let received = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            _port_b.recv_frame(&mut recv_buf),
        )
        .await
        .expect("timeout waiting for tunnel frame")
        .expect("recv_frame error");

        assert!(received >= FABRIC_HDR_SZ + 20);
        let recv_frame = &recv_buf[..received];

        // Verify segment_id was stamped.
        let seg = u16::from_be_bytes([recv_frame[1], recv_frame[2]]);
        assert_eq!(seg, segment_id);

        // Verify IP payload is intact.
        let fp = FabricPacket::new(recv_frame).unwrap();
        assert_eq!(fp.ipv4_src(), std::net::Ipv4Addr::new(10, 0, 0, 1));
        assert_eq!(fp.ipv4_dst(), std::net::Ipv4Addr::new(10, 0, 0, 2));
    }

    #[tokio::test]
    async fn test_tunnel_multi_segment_demux() {
        // One transport pair, two segment_ids (plaintext).
        let transport_a = TunnelTransport::new("127.0.0.1:0".parse().unwrap(), false)
            .await
            .unwrap();
        let transport_b = TunnelTransport::new("127.0.0.1:0".parse().unwrap(), false)
            .await
            .unwrap();

        let addr_a = transport_a.local_addr().unwrap();
        let addr_b = transport_b.local_addr().unwrap();

        transport_a
            .add_peer("worker-b".into(), addr_b, None, false)
            .unwrap();
        transport_b
            .add_peer("worker-a".into(), addr_a, None, false)
            .unwrap();

        let (port_a_seg1, _h1) = transport_a.create_namespace_port("worker-b", 1).unwrap();
        let (port_a_seg2, _h2) = transport_a.create_namespace_port("worker-b", 2).unwrap();
        let (_port_b_seg1, _h3) = transport_b.create_namespace_port("worker-a", 1).unwrap();
        let (_port_b_seg2, _h4) = transport_b.create_namespace_port("worker-a", 2).unwrap();

        use super::super::port::FramePort;

        let frame1 = make_ipv4_frame(
            std::net::Ipv4Addr::new(10, 0, 0, 1),
            std::net::Ipv4Addr::new(10, 0, 0, 2),
        );
        let frame2 = make_ipv4_frame(
            std::net::Ipv4Addr::new(10, 1, 0, 1),
            std::net::Ipv4Addr::new(10, 1, 0, 2),
        );

        // Send on segment 1.
        port_a_seg1.send_frame(&frame1).await.unwrap();
        // Send on segment 2.
        port_a_seg2.send_frame(&frame2).await.unwrap();

        // Receive on segment 1 — should get frame1.
        let mut buf1 = vec![0u8; 2048];
        let n1 = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            _port_b_seg1.recv_frame(&mut buf1),
        )
        .await
        .expect("timeout seg1")
        .expect("recv error seg1");

        let fp1 = FabricPacket::new(&buf1[..n1]).unwrap();
        assert_eq!(fp1.ipv4_src(), std::net::Ipv4Addr::new(10, 0, 0, 1));

        // Receive on segment 2 — should get frame2.
        let mut buf2 = vec![0u8; 2048];
        let n2 = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            _port_b_seg2.recv_frame(&mut buf2),
        )
        .await
        .expect("timeout seg2")
        .expect("recv error seg2");

        let fp2 = FabricPacket::new(&buf2[..n2]).unwrap();
        assert_eq!(fp2.ipv4_src(), std::net::Ipv4Addr::new(10, 1, 0, 1));
    }

    #[tokio::test]
    async fn test_tunnel_unknown_segment_dropped() {
        let transport = TunnelTransport::new("127.0.0.1:0".parse().unwrap(), false)
            .await
            .unwrap();
        let addr = transport.local_addr().unwrap();

        // Register segment 10 only.
        transport
            .add_peer("peer".into(), addr, None, false)
            .unwrap();
        let (_port, _handle) = transport.create_namespace_port("peer", 10).unwrap();

        // Send a datagram with segment_id=99 directly to the socket.
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let frame = with_fabric_header(0, 99, &{
            let mut ip = [0u8; 20];
            ip[0] = 0x45;
            ip[2..4].copy_from_slice(&20u16.to_be_bytes());
            ip
        });
        sender.send_to(&frame, addr).await.unwrap();

        // The registered port should NOT receive anything.
        use super::super::port::FramePort;
        let mut buf = vec![0u8; 2048];
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            _port.recv_frame(&mut buf),
        )
        .await;
        assert!(result.is_err(), "should timeout — unknown segment dropped");
    }

    // -------------------------------------------------------------------
    // Encrypted tunnel tests
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn test_tunnel_encrypted_round_trip() {
        // Two encrypted transports, Noise_IK handshake + encrypted data.
        let transport_a = TunnelTransport::new("127.0.0.1:0".parse().unwrap(), true)
            .await
            .unwrap();
        let transport_b = TunnelTransport::new("127.0.0.1:0".parse().unwrap(), true)
            .await
            .unwrap();

        let addr_a = transport_a.local_addr().unwrap();
        let addr_b = transport_b.local_addr().unwrap();

        let pub_a: [u8; 32] = transport_a.public_key().unwrap().try_into().unwrap();
        let pub_b: [u8; 32] = transport_b.public_key().unwrap().try_into().unwrap();

        // Determine initiator by lexicographic key comparison.
        let a_initiates = pub_a < pub_b;

        transport_a
            .add_peer("worker-b".into(), addr_b, Some(&pub_b), a_initiates)
            .unwrap();
        transport_b
            .add_peer("worker-a".into(), addr_a, Some(&pub_a), !a_initiates)
            .unwrap();

        // Give the handshake some time to complete.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let segment_id = 42;

        let (port_a, _handle_a) = transport_a
            .create_namespace_port("worker-b", segment_id)
            .unwrap();
        let (_port_b, _handle_b) = transport_b
            .create_namespace_port("worker-a", segment_id)
            .unwrap();

        use super::super::port::FramePort;

        let frame = make_ipv4_frame(
            std::net::Ipv4Addr::new(10, 0, 0, 1),
            std::net::Ipv4Addr::new(10, 0, 0, 2),
        );

        port_a.send_frame(&frame).await.unwrap();

        let mut recv_buf = vec![0u8; 2048];
        let received = tokio::time::timeout(
            std::time::Duration::from_millis(1000),
            _port_b.recv_frame(&mut recv_buf),
        )
        .await
        .expect("timeout waiting for encrypted tunnel frame")
        .expect("recv_frame error");

        assert!(received >= FABRIC_HDR_SZ + 20);
        let recv_frame = &recv_buf[..received];

        let seg = u16::from_be_bytes([recv_frame[1], recv_frame[2]]);
        assert_eq!(seg, segment_id);

        let fp = FabricPacket::new(recv_frame).unwrap();
        assert_eq!(fp.ipv4_src(), std::net::Ipv4Addr::new(10, 0, 0, 1));
        assert_eq!(fp.ipv4_dst(), std::net::Ipv4Addr::new(10, 0, 0, 2));
    }

    #[tokio::test]
    async fn test_tunnel_encrypted_multi_segment_demux() {
        let transport_a = TunnelTransport::new("127.0.0.1:0".parse().unwrap(), true)
            .await
            .unwrap();
        let transport_b = TunnelTransport::new("127.0.0.1:0".parse().unwrap(), true)
            .await
            .unwrap();

        let addr_a = transport_a.local_addr().unwrap();
        let addr_b = transport_b.local_addr().unwrap();

        let pub_a: [u8; 32] = transport_a.public_key().unwrap().try_into().unwrap();
        let pub_b: [u8; 32] = transport_b.public_key().unwrap().try_into().unwrap();

        let a_initiates = pub_a < pub_b;

        transport_a
            .add_peer("worker-b".into(), addr_b, Some(&pub_b), a_initiates)
            .unwrap();
        transport_b
            .add_peer("worker-a".into(), addr_a, Some(&pub_a), !a_initiates)
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let (port_a_seg1, _h1) = transport_a.create_namespace_port("worker-b", 1).unwrap();
        let (port_a_seg2, _h2) = transport_a.create_namespace_port("worker-b", 2).unwrap();
        let (_port_b_seg1, _h3) = transport_b.create_namespace_port("worker-a", 1).unwrap();
        let (_port_b_seg2, _h4) = transport_b.create_namespace_port("worker-a", 2).unwrap();

        use super::super::port::FramePort;

        let frame1 = make_ipv4_frame(
            std::net::Ipv4Addr::new(10, 0, 0, 1),
            std::net::Ipv4Addr::new(10, 0, 0, 2),
        );
        let frame2 = make_ipv4_frame(
            std::net::Ipv4Addr::new(10, 1, 0, 1),
            std::net::Ipv4Addr::new(10, 1, 0, 2),
        );

        port_a_seg1.send_frame(&frame1).await.unwrap();
        port_a_seg2.send_frame(&frame2).await.unwrap();

        let mut buf1 = vec![0u8; 2048];
        let n1 = tokio::time::timeout(
            std::time::Duration::from_millis(1000),
            _port_b_seg1.recv_frame(&mut buf1),
        )
        .await
        .expect("timeout seg1")
        .expect("recv error seg1");

        let fp1 = FabricPacket::new(&buf1[..n1]).unwrap();
        assert_eq!(fp1.ipv4_src(), std::net::Ipv4Addr::new(10, 0, 0, 1));

        let mut buf2 = vec![0u8; 2048];
        let n2 = tokio::time::timeout(
            std::time::Duration::from_millis(1000),
            _port_b_seg2.recv_frame(&mut buf2),
        )
        .await
        .expect("timeout seg2")
        .expect("recv error seg2");

        let fp2 = FabricPacket::new(&buf2[..n2]).unwrap();
        assert_eq!(fp2.ipv4_src(), std::net::Ipv4Addr::new(10, 1, 0, 1));
    }
}
