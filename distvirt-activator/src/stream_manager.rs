//! L4 stream management: translates between raw IP packets and stream
//! events/actions using a smoltcp TCP stack.
//!
//! The fabric feeds IP packets in, gets events and outgoing packets back.

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::Ipv4Addr;
use std::time::Duration;

use smoltcp::iface::{Config, Interface, SocketHandle, SocketSet};
use smoltcp::phy::{Checksum, Device, DeviceCapabilities, Medium, RxToken, TxToken};
use smoltcp::socket::tcp;
use smoltcp::time::Instant as SmolInstant;
use smoltcp::wire::{HardwareAddress, IpAddress, IpCidr, IpEndpoint, IpListenEndpoint};

use crate::types::{Action, Event, Stream};

// --- smoltcp phy::Device backed by VecDeque ---

struct FabricRxToken(Vec<u8>);

impl RxToken for FabricRxToken {
    fn consume<R, F>(self, f: F) -> R
    where
        F: FnOnce(&[u8]) -> R,
    {
        f(&self.0)
    }
}

struct FabricTxToken<'a>(&'a mut Vec<Vec<u8>>);

impl<'a> TxToken for FabricTxToken<'a> {
    fn consume<R, F>(self, len: usize, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        let mut buf = vec![0u8; len];
        let result = f(&mut buf);
        self.0.push(buf);
        result
    }
}

struct FabricDevice {
    rx_queue: VecDeque<Vec<u8>>,
    tx_queue: Vec<Vec<u8>>,
}

impl FabricDevice {
    fn new() -> Self {
        FabricDevice {
            rx_queue: VecDeque::new(),
            tx_queue: Vec::new(),
        }
    }
}

impl Device for FabricDevice {
    type RxToken<'a> = FabricRxToken where Self: 'a;
    type TxToken<'a> = FabricTxToken<'a> where Self: 'a;

    fn receive(
        &mut self,
        _timestamp: SmolInstant,
    ) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        let pkt = self.rx_queue.pop_front()?;
        Some((FabricRxToken(pkt), FabricTxToken(&mut self.tx_queue)))
    }

    fn transmit(&mut self, _timestamp: SmolInstant) -> Option<Self::TxToken<'_>> {
        Some(FabricTxToken(&mut self.tx_queue))
    }

    fn capabilities(&self) -> DeviceCapabilities {
        let mut caps = DeviceCapabilities::default();
        caps.medium = Medium::Ip;
        caps.max_transmission_unit = 1500;
        // Frames arrive from virtio with checksum offloading — skip verification
        // on receive but compute valid checksums on transmit.
        caps.checksum.ipv4 = Checksum::Tx;
        caps.checksum.udp = Checksum::Tx;
        caps.checksum.tcp = Checksum::Tx;
        caps.checksum.icmpv4 = Checksum::Tx;
        caps
    }
}

// --- StreamManager ---

/// Configuration for a StreamManager.
#[derive(Debug, Clone)]
pub struct StreamManagerConfig {
    /// The service virtual IP.
    pub service_ip: Ipv4Addr,
    /// TCP ports to listen on.
    pub listen_ports: Vec<u16>,
    /// Size of TCP receive/send buffers per socket.
    pub tcp_buffer_size: usize,
    /// Number of listening sockets to maintain per port.
    pub listen_pool_size: usize,
}

impl Default for StreamManagerConfig {
    fn default() -> Self {
        StreamManagerConfig {
            service_ip: Ipv4Addr::new(0, 0, 0, 0),
            listen_ports: vec![80],
            tcp_buffer_size: 65535,
            listen_pool_size: 4,
        }
    }
}

/// Direction of a stream: downstream (client→service) or upstream (service→backend).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamDirection {
    Downstream,
    Upstream,
}

/// Per-stream state.
struct StreamState {
    socket_handle: SocketHandle,
    direction: StreamDirection,
    opened_notified: bool,
    closed_notified: bool,
    paused: bool,
    /// For upstream connections, the local ephemeral port used.
    upstream_local_port: Option<u16>,
}

/// Output from StreamManager operations.
pub struct StreamManagerOutput {
    /// Events to deliver to the activator.
    pub events: Vec<Event>,
    /// Raw IP packets to send (no vnet header).
    pub frames: Vec<Vec<u8>>,
}

/// L4 stream manager: smoltcp-based TCP stack that translates between raw
/// Ethernet frames and stream events/actions.
pub struct StreamManager {
    device: FabricDevice,
    iface: Interface,
    sockets: SocketSet<'static>,
    /// Stream ID → state.
    streams: HashMap<Stream, StreamState>,
    /// Socket handle → stream ID (reverse lookup).
    handle_to_stream: HashMap<SocketHandle, Stream>,
    /// Listening socket handles (not yet associated with a stream).
    listeners: Vec<SocketHandle>,
    next_stream_id: Stream,
    config: StreamManagerConfig,
    base_instant: std::time::Instant,
    /// Backend IP for upstream connections.
    backend_ip: Option<Ipv4Addr>,
    /// Allocated upstream local ports to avoid collisions.
    upstream_ports: HashSet<u16>,
    /// Counter for upstream port allocation.
    next_upstream_port: u16,
}

impl StreamManager {
    /// Create a new StreamManager.
    pub fn new(config: StreamManagerConfig) -> Self {
        let base_instant = std::time::Instant::now();
        let mut device = FabricDevice::new();

        let iface_config = Config::new(HardwareAddress::Ip);
        let mut iface = Interface::new(
            iface_config,
            &mut device,
            SmolInstant::from_millis(0),
        );

        let ip = config.service_ip;
        iface.update_ip_addrs(|addrs| {
            addrs
                .push(IpCidr::new(
                    IpAddress::v4(ip.octets()[0], ip.octets()[1], ip.octets()[2], ip.octets()[3]),
                    24,
                ))
                .ok();
        });

        // Disable any routing — we only handle direct traffic.
        iface.routes_mut().update(|_routes| {});

        let mut sockets = SocketSet::new(vec![]);

        // Create listening socket pool.
        let mut listeners = Vec::new();
        for &port in &config.listen_ports {
            for _ in 0..config.listen_pool_size {
                let handle = Self::create_listener(&mut sockets, port, config.tcp_buffer_size);
                listeners.push(handle);
            }
        }

        StreamManager {
            device,
            iface,
            sockets,
            streams: HashMap::new(),
            handle_to_stream: HashMap::new(),
            listeners,
            next_stream_id: 1,
            config,
            base_instant,
            backend_ip: None,
            upstream_ports: HashSet::new(),
            next_upstream_port: 49152,
        }
    }

    /// Allocate a unique ephemeral port for upstream connections.
    fn alloc_upstream_port(&mut self) -> u16 {
        let range_start = 49152u16;
        let range_size = 16384u16;
        for _ in 0..range_size {
            let port = self.next_upstream_port;
            self.next_upstream_port = range_start + (self.next_upstream_port - range_start + 1) % range_size;
            if !self.upstream_ports.contains(&port) {
                self.upstream_ports.insert(port);
                return port;
            }
        }
        // All ports exhausted — fallback (should not happen in practice).
        log::error!("stream_manager: all ephemeral ports exhausted");
        self.next_upstream_port
    }

    fn smoltcp_now(&self) -> SmolInstant {
        SmolInstant::from_millis(self.base_instant.elapsed().as_millis() as i64)
    }

    fn now_from_instant(base: std::time::Instant) -> SmolInstant {
        SmolInstant::from_millis(base.elapsed().as_millis() as i64)
    }

    /// Create a listening TCP socket on the given port.
    fn create_listener(
        sockets: &mut SocketSet<'static>,
        port: u16,
        buffer_size: usize,
    ) -> SocketHandle {
        let rx_buf = tcp::SocketBuffer::new(vec![0u8; buffer_size]);
        let tx_buf = tcp::SocketBuffer::new(vec![0u8; buffer_size]);
        let mut socket = tcp::Socket::new(rx_buf, tx_buf);
        socket
            .listen(IpListenEndpoint { addr: None, port })
            .expect("listen should not fail on fresh socket");
        sockets.add(socket)
    }

    /// Allocate the next stream ID.
    fn alloc_stream_id(&mut self) -> Stream {
        let id = self.next_stream_id;
        self.next_stream_id += 1;
        id
    }

    /// Poll smoltcp and drain outgoing frames.
    fn poll_and_drain(&mut self, now: SmolInstant) -> Vec<Vec<u8>> {
        self.iface
            .poll(now, &mut self.device, &mut self.sockets);
        self.device.tx_queue.drain(..).collect()
    }

    /// Scan sockets and generate events for state changes.
    fn scan_sockets(&mut self) -> Vec<Event> {
        let mut events = Vec::new();

        // 1. Check listening sockets for new connections (transition to Established).
        let mut new_established = Vec::new();
        self.listeners.retain(|&handle| {
            let socket = self.sockets.get::<tcp::Socket>(handle);
            if socket.state() == tcp::State::Established
                || socket.state() == tcp::State::SynReceived
            {
                if socket.state() == tcp::State::Established {
                    new_established.push(handle);
                    return false; // Remove from listeners
                }
            }
            true
        });

        for handle in new_established {
            let stream_id = self.alloc_stream_id();
            // Determine the local port so we can re-create a listener for it.
            let local_port = {
                let socket = self.sockets.get::<tcp::Socket>(handle);
                socket.local_endpoint().unwrap().port
            };
            self.streams.insert(
                stream_id,
                StreamState {
                    socket_handle: handle,
                    direction: StreamDirection::Downstream,
                    opened_notified: true,
                    closed_notified: false,
                    paused: false,
                    upstream_local_port: None,
                },
            );
            self.handle_to_stream.insert(handle, stream_id);
            events.push(Event::StreamOpen(stream_id));

            // Replenish listener pool for this port.
            let new_listener =
                Self::create_listener(&mut self.sockets, local_port, self.config.tcp_buffer_size);
            self.listeners.push(new_listener);
        }

        // 2. Scan established streams for data and close events.
        // Collect stream IDs to avoid borrow conflicts with self.sockets.
        let stream_ids: Vec<(Stream, SocketHandle, StreamDirection, bool)> = self
            .streams
            .iter()
            .map(|(&id, s)| (id, s.socket_handle, s.direction, s.paused))
            .collect();

        let mut closed_streams = Vec::new();
        for (stream_id, handle, direction, paused) in stream_ids {
            // Read data if not paused.
            if !paused {
                let socket = self.sockets.get_mut::<tcp::Socket>(handle);
                if socket.can_recv() {
                    let data = socket
                        .recv(|buf| {
                            let data = buf.to_vec();
                            (buf.len(), data)
                        })
                        .ok()
                        .unwrap_or_default();
                    if !data.is_empty() {
                        match direction {
                            StreamDirection::Downstream => {
                                events.push(Event::StreamData {
                                    stream: stream_id,
                                    data,
                                });
                            }
                            StreamDirection::Upstream => {
                                events.push(Event::UpstreamData {
                                    stream: stream_id,
                                    data,
                                });
                            }
                        }
                    }
                }
            }

            let state = self.streams.get_mut(&stream_id).unwrap();
            let socket = self.sockets.get::<tcp::Socket>(handle);

            // Check for upstream connect completion.
            if state.direction == StreamDirection::Upstream && !state.opened_notified {
                match socket.state() {
                    tcp::State::Established => {
                        state.opened_notified = true;
                        events.push(Event::UpstreamConnectResult {
                            stream: stream_id,
                            ok: true,
                        });
                    }
                    tcp::State::Closed | tcp::State::TimeWait => {
                        state.opened_notified = true;
                        state.closed_notified = true;
                        events.push(Event::UpstreamConnectResult {
                            stream: stream_id,
                            ok: false,
                        });
                        closed_streams.push(stream_id);
                    }
                    _ => {} // Still connecting
                }
            }

            // Check for close.
            if !state.closed_notified {
                let is_closed = match socket.state() {
                    tcp::State::Closed
                    | tcp::State::TimeWait
                    | tcp::State::Closing
                    | tcp::State::LastAck => true,
                    tcp::State::CloseWait => {
                        // Remote closed, drain remaining data first
                        !socket.can_recv()
                    }
                    _ => false,
                };
                if is_closed {
                    state.closed_notified = true;
                    match state.direction {
                        StreamDirection::Downstream => {
                            events.push(Event::StreamClose(stream_id));
                        }
                        StreamDirection::Upstream => {
                            events.push(Event::UpstreamClose(stream_id));
                        }
                    }
                    closed_streams.push(stream_id);
                }
            }
        }

        // Clean up closed streams.
        for stream_id in closed_streams {
            if let Some(state) = self.streams.remove(&stream_id) {
                self.handle_to_stream.remove(&state.socket_handle);
                self.sockets.remove(state.socket_handle);
                // Release upstream local port for reuse.
                if let Some(port) = state.upstream_local_port {
                    self.upstream_ports.remove(&port);
                }
            }
        }

        events
    }

    /// Process a received IP packet. Returns events and outgoing packets.
    pub fn receive_frame(&mut self, ip_packet: &[u8]) -> StreamManagerOutput {
        let now = self.smoltcp_now();

        // Feed packet to smoltcp.
        self.device.rx_queue.push_back(ip_packet.to_vec());
        let frames = self.poll_and_drain(now);
        let events = self.scan_sockets();

        StreamManagerOutput { events, frames }
    }

    /// Execute an action from the activator. Returns events and outgoing frames.
    pub fn execute_action(&mut self, action: &Action) -> StreamManagerOutput {
        let now = self.smoltcp_now();

        match action {
            Action::DownstreamSend { stream, data } => {
                if let Some(state) = self.streams.get(stream) {
                    let socket = self.sockets.get_mut::<tcp::Socket>(state.socket_handle);
                    if socket.can_send() {
                        match socket.send_slice(data) {
                            Ok(sent) if sent < data.len() => {
                                log::warn!("stream_manager: downstream partial write: {}/{}", sent, data.len());
                            }
                            Err(e) => {
                                log::warn!("stream_manager: downstream send error: {:?}", e);
                            }
                            _ => {}
                        }
                    }
                }
            }
            Action::DownstreamClose(stream) => {
                if let Some(state) = self.streams.get(stream) {
                    let socket = self.sockets.get_mut::<tcp::Socket>(state.socket_handle);
                    socket.close();
                }
            }
            Action::PauseDownstream(stream) => {
                if let Some(state) = self.streams.get_mut(stream) {
                    state.paused = true;
                }
            }
            Action::ResumeDownstream(stream) => {
                if let Some(state) = self.streams.get_mut(stream) {
                    state.paused = false;
                }
            }
            Action::UpstreamConnect { port } => {
                if let Some(backend_ip) = self.backend_ip {
                    // Create a new TCP socket and connect to backend.
                    let rx_buf = tcp::SocketBuffer::new(vec![0u8; self.config.tcp_buffer_size]);
                    let tx_buf = tcp::SocketBuffer::new(vec![0u8; self.config.tcp_buffer_size]);
                    let socket = tcp::Socket::new(rx_buf, tx_buf);
                    let handle = self.sockets.add(socket);

                    let stream_id = self.alloc_stream_id();
                    let local_port = self.alloc_upstream_port();
                    self.streams.insert(
                        stream_id,
                        StreamState {
                            socket_handle: handle,
                            direction: StreamDirection::Upstream,
                            opened_notified: false,
                            closed_notified: false,
                            paused: false,
                            upstream_local_port: Some(local_port),
                        },
                    );
                    self.handle_to_stream.insert(handle, stream_id);

                    let remote = IpEndpoint::new(
                        IpAddress::v4(
                            backend_ip.octets()[0],
                            backend_ip.octets()[1],
                            backend_ip.octets()[2],
                            backend_ip.octets()[3],
                        ),
                        *port,
                    );
                    let socket = self.sockets.get_mut::<tcp::Socket>(handle);
                    let local = IpEndpoint::new(
                        IpAddress::v4(
                            self.config.service_ip.octets()[0],
                            self.config.service_ip.octets()[1],
                            self.config.service_ip.octets()[2],
                            self.config.service_ip.octets()[3],
                        ),
                        local_port,
                    );
                    if let Err(e) = socket.connect(&mut self.iface.context(), remote, local) {
                        log::warn!("stream_manager: upstream connect error: {:?}", e);
                    }
                } else {
                    log::warn!("stream_manager: upstream connect but no backend configured");
                }
            }
            Action::UpstreamSend { stream, data } => {
                if let Some(state) = self.streams.get(stream) {
                    if state.direction == StreamDirection::Upstream {
                        let socket = self.sockets.get_mut::<tcp::Socket>(state.socket_handle);
                        if socket.can_send() {
                            match socket.send_slice(data) {
                                Ok(sent) if sent < data.len() => {
                                    log::warn!("stream_manager: upstream partial write: {}/{}", sent, data.len());
                                }
                                Err(e) => {
                                    log::warn!("stream_manager: upstream send error: {:?}", e);
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            Action::UpstreamClose(stream) => {
                if let Some(state) = self.streams.get(stream) {
                    if state.direction == StreamDirection::Upstream {
                        let socket = self.sockets.get_mut::<tcp::Socket>(state.socket_handle);
                        socket.close();
                    }
                }
            }
            Action::PauseUpstream(stream) => {
                if let Some(state) = self.streams.get_mut(stream) {
                    if state.direction == StreamDirection::Upstream {
                        state.paused = true;
                    }
                }
            }
            Action::ResumeUpstream(stream) => {
                if let Some(state) = self.streams.get_mut(stream) {
                    if state.direction == StreamDirection::Upstream {
                        state.paused = false;
                    }
                }
            }
            _ => {
                // Non-L4 actions are ignored here.
            }
        }

        let frames = self.poll_and_drain(now);
        let events = self.scan_sockets();

        StreamManagerOutput { events, frames }
    }

    /// Handle a timeout (periodic polling). Returns events and outgoing frames.
    pub fn handle_timeout(&mut self) -> StreamManagerOutput {
        let now = self.smoltcp_now();
        let frames = self.poll_and_drain(now);
        let events = self.scan_sockets();
        StreamManagerOutput { events, frames }
    }

    /// Get the poll delay for the next timeout.
    pub fn poll_delay(&mut self) -> Option<Duration> {
        let now = Self::now_from_instant(self.base_instant);
        self.iface
            .poll_delay(now, &self.sockets)
            .map(|d| Duration::from_millis(d.total_millis() as u64))
    }

    /// Update the backend target for upstream connections.
    pub fn update_backend(&mut self, ip: Option<Ipv4Addr>) {
        self.backend_ip = ip;
    }
}

/// Helper to check if an action is an L4 stream action (handled by StreamManager).
pub fn is_l4_action(action: &Action) -> bool {
    matches!(
        action,
        Action::DownstreamSend { .. }
            | Action::DownstreamClose(_)
            | Action::PauseDownstream(_)
            | Action::ResumeDownstream(_)
            | Action::UpstreamConnect { .. }
            | Action::UpstreamSend { .. }
            | Action::UpstreamClose(_)
            | Action::PauseUpstream(_)
            | Action::ResumeUpstream(_)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config(ip: Ipv4Addr, ports: Vec<u16>) -> StreamManagerConfig {
        StreamManagerConfig {
            service_ip: ip,
            listen_ports: ports,
            tcp_buffer_size: 4096,
            listen_pool_size: 2,
        }
    }

    const SVC_IP: Ipv4Addr = Ipv4Addr::new(10, 0, 0, 1);
    const CLIENT_IP: Ipv4Addr = Ipv4Addr::new(10, 0, 0, 100);

    /// Compute the TCP header start offset by parsing the IHL field (IP packet, no Ethernet).
    fn tcp_start(packet: &[u8]) -> usize {
        let ihl = (packet[0] & 0x0f) as usize;
        ihl * 4
    }

    /// Check if a packet is an IPv4 TCP packet (raw IP, no Ethernet header).
    fn is_tcp_packet(packet: &[u8]) -> bool {
        packet.len() >= 40
            && (packet[0] >> 4) == 4  // IPv4
            && packet[9] == 6         // IP protocol TCP
    }

    /// Build TCP SYN IP packet using etherparse.
    fn make_tcp_syn(
        src_ip: Ipv4Addr,
        dst_ip: Ipv4Addr,
        src_port: u16,
        dst_port: u16,
        seq: u32,
    ) -> Vec<u8> {
        use etherparse::PacketBuilder;

        let builder = PacketBuilder::ipv4(src_ip.octets(), dst_ip.octets(), 64)
            .tcp(src_port, dst_port, seq, 65535);

        let mut packet = Vec::new();
        builder.write(&mut packet, &[]).unwrap();

        // Set SYN flag
        let ts = tcp_start(&packet);
        packet[ts + 13] = 0x02; // SYN only
        packet
    }

    /// Build a TCP ACK IP packet.
    fn make_tcp_ack(
        src_ip: Ipv4Addr,
        dst_ip: Ipv4Addr,
        src_port: u16,
        dst_port: u16,
        seq: u32,
        ack: u32,
    ) -> Vec<u8> {
        use etherparse::PacketBuilder;

        let builder = PacketBuilder::ipv4(src_ip.octets(), dst_ip.octets(), 64)
            .tcp(src_port, dst_port, seq, 65535);

        let mut packet = Vec::new();
        builder.write(&mut packet, &[]).unwrap();

        let ts = tcp_start(&packet);
        packet[ts + 13] = 0x10; // ACK only
        packet[ts + 8..ts + 12].copy_from_slice(&ack.to_be_bytes());
        packet
    }

    /// Build a TCP PSH+ACK IP packet with data payload.
    fn make_tcp_data(
        src_ip: Ipv4Addr,
        dst_ip: Ipv4Addr,
        src_port: u16,
        dst_port: u16,
        seq: u32,
        ack: u32,
        data: &[u8],
    ) -> Vec<u8> {
        use etherparse::PacketBuilder;

        let builder = PacketBuilder::ipv4(src_ip.octets(), dst_ip.octets(), 64)
            .tcp(src_port, dst_port, seq, 65535);

        let mut packet = Vec::new();
        builder.write(&mut packet, data).unwrap();

        let ts = tcp_start(&packet);
        packet[ts + 13] = 0x18; // PSH+ACK
        packet[ts + 8..ts + 12].copy_from_slice(&ack.to_be_bytes());
        packet
    }

    /// Build a TCP FIN+ACK IP packet.
    fn make_tcp_fin(
        src_ip: Ipv4Addr,
        dst_ip: Ipv4Addr,
        src_port: u16,
        dst_port: u16,
        seq: u32,
        ack: u32,
    ) -> Vec<u8> {
        use etherparse::PacketBuilder;

        let builder = PacketBuilder::ipv4(src_ip.octets(), dst_ip.octets(), 64)
            .tcp(src_port, dst_port, seq, 65535);

        let mut packet = Vec::new();
        builder.write(&mut packet, &[]).unwrap();

        let ts = tcp_start(&packet);
        packet[ts + 13] = 0x11; // FIN+ACK
        packet[ts + 8..ts + 12].copy_from_slice(&ack.to_be_bytes());
        packet
    }

    /// Extract seq and ack from a TCP IP packet.
    fn extract_tcp_seq_ack(packet: &[u8]) -> (u32, u32) {
        let ts = tcp_start(packet);
        let seq = u32::from_be_bytes(packet[ts + 4..ts + 8].try_into().unwrap());
        let ack = u32::from_be_bytes(packet[ts + 8..ts + 12].try_into().unwrap());
        (seq, ack)
    }

    /// Extract TCP flags from a raw IP packet.
    fn extract_tcp_flags(packet: &[u8]) -> u8 {
        let ts = tcp_start(packet);
        packet[ts + 13]
    }

    #[test]
    fn test_tcp_syn_produces_syn_ack() {
        let config = make_config(SVC_IP, vec![80]);
        let mut sm = StreamManager::new(config);

        let syn = make_tcp_syn(CLIENT_IP, SVC_IP, 12345, 80, 1000);
        let output = sm.receive_frame(&syn);

        // Should get at least one outgoing TCP packet (SYN-ACK).
        let tcp_packets: Vec<_> = output.frames.iter().filter(|f| is_tcp_packet(f)).collect();
        assert!(!tcp_packets.is_empty(), "SYN should produce SYN-ACK");
        let flags = extract_tcp_flags(tcp_packets[0]);
        assert_eq!(flags & 0x12, 0x12, "response should be SYN+ACK");
    }

    #[test]
    fn test_tcp_handshake_produces_stream_open() {
        let config = make_config(SVC_IP, vec![80]);
        let mut sm = StreamManager::new(config);

        // SYN
        let syn = make_tcp_syn(CLIENT_IP, SVC_IP, 12345, 80, 1000);
        let output = sm.receive_frame(&syn);
        let tcp_packets: Vec<_> = output.frames.iter().filter(|f| is_tcp_packet(f)).collect();
        assert!(!tcp_packets.is_empty(), "SYN should produce SYN-ACK");

        // Extract server seq from SYN-ACK.
        let (server_seq, _server_ack) = extract_tcp_seq_ack(tcp_packets[0]);

        // ACK (completes handshake)
        let ack = make_tcp_ack(
            CLIENT_IP,
            SVC_IP,
            12345,
            80,
            1001,
            server_seq + 1,
        );
        let output = sm.receive_frame(&ack);

        // Should get StreamOpen event.
        let stream_open = output
            .events
            .iter()
            .find(|e| matches!(e, Event::StreamOpen(_)));
        assert!(stream_open.is_some(), "completing handshake should produce StreamOpen");
    }

    /// Complete a TCP handshake and return (stream_id, server_seq).
    fn do_handshake(sm: &mut StreamManager) -> (Stream, u32) {
        let syn = make_tcp_syn(CLIENT_IP, SVC_IP, 12345, 80, 1000);
        let output = sm.receive_frame(&syn);
        let tcp_packets: Vec<_> = output.frames.iter().filter(|f| is_tcp_packet(f)).collect();
        assert!(!tcp_packets.is_empty(), "SYN should produce SYN-ACK");
        let (server_seq, _) = extract_tcp_seq_ack(tcp_packets[0]);

        let ack = make_tcp_ack(
            CLIENT_IP, SVC_IP,
            12345, 80, 1001, server_seq + 1,
        );
        let output = sm.receive_frame(&ack);
        let stream_id = output
            .events
            .iter()
            .find_map(|e| match e {
                Event::StreamOpen(s) => Some(*s),
                _ => None,
            })
            .expect("should have StreamOpen");
        (stream_id, server_seq)
    }

    #[test]
    fn test_tcp_data_produces_stream_data() {
        let config = make_config(SVC_IP, vec![80]);
        let mut sm = StreamManager::new(config);
        let (stream_id, server_seq) = do_handshake(&mut sm);

        let data_packet = make_tcp_data(
            CLIENT_IP, SVC_IP,
            12345, 80, 1001, server_seq + 1, b"hello",
        );
        let output = sm.receive_frame(&data_packet);

        let stream_data = output.events.iter().find(|e| {
            matches!(e, Event::StreamData { stream, data } if *stream == stream_id && data == b"hello")
        });
        assert!(stream_data.is_some(), "data packet should produce StreamData event");
    }

    #[test]
    fn test_tcp_fin_produces_stream_close() {
        let config = make_config(SVC_IP, vec![80]);
        let mut sm = StreamManager::new(config);
        let (stream_id, server_seq) = do_handshake(&mut sm);

        let fin = make_tcp_fin(
            CLIENT_IP, SVC_IP,
            12345, 80, 1001, server_seq + 1,
        );
        let output = sm.receive_frame(&fin);

        let stream_close = output
            .events
            .iter()
            .find(|e| matches!(e, Event::StreamClose(s) if *s == stream_id));
        assert!(stream_close.is_some(), "FIN should produce StreamClose event");
    }

    #[test]
    fn test_downstream_send_produces_data_frame() {
        let config = make_config(SVC_IP, vec![80]);
        let mut sm = StreamManager::new(config);
        let (stream_id, _) = do_handshake(&mut sm);

        let output = sm.execute_action(&Action::DownstreamSend {
            stream: stream_id,
            data: b"world".to_vec(),
        });

        let tcp_packets: Vec<_> = output.frames.iter().filter(|f| is_tcp_packet(f)).collect();
        assert!(!tcp_packets.is_empty(), "DownstreamSend should produce TCP packet");
        let flags = extract_tcp_flags(tcp_packets[0]);
        assert!(flags & 0x08 != 0 || flags & 0x10 != 0, "should be PSH and/or ACK");
    }

    #[test]
    fn test_downstream_close_produces_fin() {
        let config = make_config(SVC_IP, vec![80]);
        let mut sm = StreamManager::new(config);
        let (stream_id, _) = do_handshake(&mut sm);

        let output = sm.execute_action(&Action::DownstreamClose(stream_id));

        let tcp_packets: Vec<_> = output.frames.iter().filter(|f| is_tcp_packet(f)).collect();
        assert!(!tcp_packets.is_empty(), "DownstreamClose should produce FIN");
        let flags = extract_tcp_flags(tcp_packets[0]);
        assert!(flags & 0x01 != 0, "should have FIN flag set");
    }

    #[test]
    fn test_pause_stops_stream_data() {
        let config = make_config(SVC_IP, vec![80]);
        let mut sm = StreamManager::new(config);
        let (stream_id, server_seq) = do_handshake(&mut sm);

        sm.execute_action(&Action::PauseDownstream(stream_id));

        let data_packet = make_tcp_data(
            CLIENT_IP, SVC_IP,
            12345, 80, 1001, server_seq + 1, b"ignored",
        );
        let output = sm.receive_frame(&data_packet);
        let has_stream_data = output
            .events
            .iter()
            .any(|e| matches!(e, Event::StreamData { .. }));
        assert!(!has_stream_data, "paused stream should not produce StreamData");

        let _output = sm.execute_action(&Action::ResumeDownstream(stream_id));
        let _timeout_out = sm.handle_timeout();
    }

    #[test]
    fn test_partial_write_handled_gracefully() {
        let config = StreamManagerConfig {
            service_ip: SVC_IP,
            listen_ports: vec![80],
            tcp_buffer_size: 64, // Small buffer to trigger partial write
            listen_pool_size: 2,
        };
        let mut sm = StreamManager::new(config);
        let (stream_id, _) = do_handshake(&mut sm);

        // Try to send 256 bytes into a 64-byte buffer — should not panic.
        let big_data = vec![0xAA; 256];
        let output = sm.execute_action(&Action::DownstreamSend {
            stream: stream_id,
            data: big_data,
        });

        // Should produce some TCP packets (partial write handled).
        let tcp_packets: Vec<_> = output.frames.iter().filter(|f| is_tcp_packet(f)).collect();
        assert!(!tcp_packets.is_empty(), "partial write should still produce packets");
    }

    #[test]
    fn test_upstream_connect_sends_syn() {
        let config = make_config(SVC_IP, vec![80]);
        let mut sm = StreamManager::new(config);

        let backend_ip = Ipv4Addr::new(10, 0, 0, 200);

        sm.update_backend(Some(backend_ip));

        // Now issue an upstream connect.
        let output = sm.execute_action(&Action::UpstreamConnect { port: 8080 });

        // Should produce a TCP SYN packet.
        assert!(!output.frames.is_empty(), "UpstreamConnect should produce packets");
        let has_tcp = output.frames.iter().any(|f| is_tcp_packet(f));
        assert!(has_tcp, "should produce TCP SYN packet");
    }

    #[test]
    fn test_upstream_ports_unique() {
        let config = make_config(SVC_IP, vec![80]);
        let mut sm = StreamManager::new(config);

        let backend_ip = Ipv4Addr::new(10, 0, 0, 200);
        sm.update_backend(Some(backend_ip));

        let mut src_ports = std::collections::HashSet::new();
        for _ in 0..100 {
            let output = sm.execute_action(&Action::UpstreamConnect { port: 8080 });
            for packet in &output.frames {
                if is_tcp_packet(packet) {
                    let ts = tcp_start(packet);
                    let src_port = u16::from_be_bytes([packet[ts], packet[ts + 1]]);
                    src_ports.insert(src_port);
                }
            }
        }
        // All source ports should be unique.
        assert_eq!(src_ports.len(), 100, "all 100 upstream connections should have unique source ports");
    }

    #[test]
    fn test_poll_delay_and_handle_timeout_drives_fin_ack() {
        let config = make_config(SVC_IP, vec![80]);
        let mut sm = StreamManager::new(config);
        let (stream_id, server_seq) = do_handshake(&mut sm);

        // Client sends FIN.
        let fin = make_tcp_fin(
            CLIENT_IP, SVC_IP,
            12345, 80, 1001, server_seq + 1,
        );
        let output = sm.receive_frame(&fin);

        // Should get StreamClose event.
        let has_close = output.events.iter().any(|e| matches!(e, Event::StreamClose(s) if *s == stream_id));
        assert!(has_close, "FIN should produce StreamClose");

        // After processing FIN, smoltcp should want a timer (for TIME_WAIT or retransmit).
        // Calling handle_timeout should produce packets (ACK for the FIN at minimum).
        let timeout_output = sm.handle_timeout();
        let total_frames = output.frames.len() + timeout_output.frames.len();
        assert!(total_frames > 0, "FIN processing should produce at least one outgoing packet (ACK)");
    }

    #[test]
    fn test_poll_delay_returns_some_after_close() {
        let config = make_config(SVC_IP, vec![80]);
        let mut sm = StreamManager::new(config);
        let (_stream_id, _server_seq) = do_handshake(&mut sm);

        // Close from server side.
        sm.execute_action(&Action::DownstreamClose(_stream_id));

        // poll_delay should be Some after initiating close (FIN sent, waiting for ACK).
        let delay = sm.poll_delay();
        assert!(delay.is_some(), "poll_delay should be Some after initiating close, got None");
    }

    #[test]
    fn test_is_l4_action() {
        assert!(is_l4_action(&Action::DownstreamSend {
            stream: 1,
            data: vec![]
        }));
        assert!(is_l4_action(&Action::DownstreamClose(1)));
        assert!(is_l4_action(&Action::UpstreamConnect { port: 80 }));
        assert!(!is_l4_action(&Action::SetBackendNeed(
            crate::types::BackendNeed::None
        )));
        assert!(!is_l4_action(&Action::ReplayPacket(vec![])));
    }
}
