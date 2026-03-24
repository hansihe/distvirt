use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use anyhow::bail;
use smoltcp::iface::{Config, Interface, SocketHandle, SocketSet};
use smoltcp::phy::{self, Device, DeviceCapabilities, Medium};
use smoltcp::socket::{dns, tcp, udp};
use smoltcp::time::Instant as SmolInstant;
use smoltcp::wire::{IpAddress, IpCidr, IpEndpoint, Ipv4Address};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{mpsc, oneshot, Notify};

use super::{ConnectInfo, ProvisionedTunnel};
use crate::connection::Client;

// ── Channel-backed smoltcp device ──────────────────────────────────────────

struct ChannelDevice {
    rx_queue: VecDeque<Vec<u8>>,
    tx_queue: Vec<Vec<u8>>,
}

impl ChannelDevice {
    fn new() -> Self {
        ChannelDevice {
            rx_queue: VecDeque::new(),
            tx_queue: Vec::new(),
        }
    }
}

struct ChannelRxToken(Vec<u8>);

impl phy::RxToken for ChannelRxToken {
    fn consume<R, F>(self, f: F) -> R
    where
        F: FnOnce(&[u8]) -> R,
    {
        f(&self.0)
    }
}

struct ChannelTxToken<'a>(&'a mut Vec<Vec<u8>>);

impl<'a> phy::TxToken for ChannelTxToken<'a> {
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

impl Device for ChannelDevice {
    type RxToken<'a> = ChannelRxToken;
    type TxToken<'a> = ChannelTxToken<'a>;

    fn receive(&mut self, _timestamp: SmolInstant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        let pkt = self.rx_queue.pop_front()?;
        Some((ChannelRxToken(pkt), ChannelTxToken(&mut self.tx_queue)))
    }

    fn transmit(&mut self, _timestamp: SmolInstant) -> Option<Self::TxToken<'_>> {
        Some(ChannelTxToken(&mut self.tx_queue))
    }

    fn capabilities(&self) -> DeviceCapabilities {
        let mut caps = DeviceCapabilities::default();
        caps.medium = Medium::Ip;
        // WireGuard overhead: 32-byte header + 16-byte MAC = 48 bytes inside
        // the outer UDP/IP packet (28 bytes for IPv4). Account for this so
        // smoltcp generates packets that fit after encapsulation.
        caps.max_transmission_unit = 1420;
        caps
    }
}

// ── Per-socket shared state ────────────────────────────────────────────────

/// Shared state for a TCP socket, accessed by both the poll loop and the
/// TcpStream handle. Uses std::sync::Mutex since locks are held briefly.
struct TcpSocketState {
    /// Data received from the remote peer, ready for the caller to read.
    read_buf: VecDeque<u8>,
    /// Waker to notify when read data is available or the socket closes.
    read_waker: Option<Waker>,
    /// Data the caller wants to send, waiting to be drained into smoltcp.
    write_buf: VecDeque<u8>,
    /// Waker to notify when write buffer space is available.
    write_waker: Option<Waker>,
    /// Waker to notify when the shutdown completes.
    shutdown_waker: Option<Waker>,
    /// Remote end closed or error occurred — no more reads.
    read_closed: bool,
    /// We've initiated a close.
    write_closed: bool,
    /// The smoltcp socket.close() has been called.
    close_initiated: bool,
    /// The TCP close sequence has completed (socket reached Closed/TimeWait).
    close_complete: bool,
    /// An error occurred on this socket.
    error: Option<String>,
}

impl TcpSocketState {
    fn new() -> Self {
        TcpSocketState {
            read_buf: VecDeque::new(),
            read_waker: None,
            write_buf: VecDeque::new(),
            write_waker: None,
            shutdown_waker: None,
            read_closed: false,
            write_closed: false,
            close_initiated: false,
            close_complete: false,
            error: None,
        }
    }
}

/// Shared state for a UDP socket.
struct UdpSocketState {
    /// Received datagrams: (data, source address).
    recv_queue: VecDeque<(Vec<u8>, SocketAddr)>,
    /// Waker for recv_from.
    recv_waker: Option<Waker>,
    /// Datagrams to send: (data, destination address).
    send_queue: VecDeque<(Vec<u8>, SocketAddr)>,
    /// Waker for send_to (woken when send_queue drains).
    send_waker: Option<Waker>,
    /// An error occurred on this socket.
    error: Option<String>,
}

impl UdpSocketState {
    fn new() -> Self {
        UdpSocketState {
            recv_queue: VecDeque::new(),
            recv_waker: None,
            send_queue: VecDeque::new(),
            send_waker: None,
            error: None,
        }
    }
}

// Maximum write buffer before we apply backpressure.
const TCP_WRITE_BUF_LIMIT: usize = 65536;
// Maximum read buffer before we stop draining smoltcp (applies TCP window backpressure).
const TCP_READ_BUF_LIMIT: usize = 65536;
// Maximum queued datagrams.
const UDP_SEND_QUEUE_LIMIT: usize = 64;
// Maximum queued received datagrams before we start dropping.
const UDP_RECV_QUEUE_LIMIT: usize = 256;

// ── Commands sent to the poll loop ─────────────────────────────────────────

enum Command {
    ConnectTcp {
        addr: SocketAddr,
        reply: oneshot::Sender<anyhow::Result<(SocketHandle, Arc<Mutex<TcpSocketState>>)>>,
    },
    BindUdp {
        port: u16,
        reply: oneshot::Sender<anyhow::Result<(SocketHandle, Arc<Mutex<UdpSocketState>>)>>,
    },
    Resolve {
        name: String,
        reply: oneshot::Sender<anyhow::Result<Ipv4Addr>>,
    },
    Shutdown,
}

// ── Shared state between poll loop and socket handles ──────────────────────

struct Inner {
    /// Wake the poll loop when a socket has data to send.
    notify: Notify,
    /// Commands from socket constructors to the poll loop.
    cmd_tx: mpsc::UnboundedSender<Command>,
}

// ── Public types ───────────────────────────────────────────────────────────

/// A userspace WireGuard tunnel with its own IP stack (smoltcp).
///
/// No root privileges required. Spawns a background tokio task for
/// WireGuard packet forwarding and smoltcp polling.
///
/// Create via [`ProvisionedTunnel::into_userspace`].
pub struct UserspaceNetwork {
    inner: Arc<Inner>,
    task: Option<tokio::task::JoinHandle<anyhow::Result<()>>>,
    provisioned: ProvisionedTunnel,
}

impl ProvisionedTunnel {
    /// Materialize this tunnel as a userspace network using smoltcp.
    ///
    /// No OS privileges required. Spawns a background task that runs
    /// the WireGuard crypto and smoltcp IP stack.
    pub async fn into_userspace(self) -> anyhow::Result<UserspaceNetwork> {
        let (tunn, udp) = self.create_wg_tunnel().await?;

        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

        let inner = Arc::new(Inner {
            notify: Notify::new(),
            cmd_tx,
        });

        let task = tokio::spawn(poll_loop(
            tunn,
            udp,
            self.endpoint,
            self.client_ip,
            self.gateway_ip,
            self.prefix_len,
            cmd_rx,
            Arc::clone(&inner),
        ));

        Ok(UserspaceNetwork {
            inner,
            task: Some(task),
            provisioned: self,
        })
    }
}

impl UserspaceNetwork {
    /// Connection metadata.
    pub fn info(&self) -> ConnectInfo {
        self.provisioned.info()
    }

    /// The client's WireGuard public key.
    pub fn public_key(&self) -> &[u8; 32] {
        self.provisioned.public_key()
    }

    /// Resolve a hostname to an IPv4 address using the namespace's DNS server.
    pub async fn resolve(&self, name: &str) -> anyhow::Result<Ipv4Addr> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(Command::Resolve {
                name: name.to_string(),
                reply: reply_tx,
            })
            .map_err(|_| anyhow::anyhow!("tunnel shut down"))?;
        self.inner.notify.notify_one();
        reply_rx
            .await
            .map_err(|_| anyhow::anyhow!("tunnel shut down"))?
    }

    /// Open a TCP connection to a host:port inside the namespace.
    /// `host` may be an IP address or a hostname (resolved via the namespace DNS).
    pub async fn connect_tcp_host(&self, host: &str, port: u16) -> anyhow::Result<TcpStream> {
        let ip: Ipv4Addr = match host.parse() {
            Ok(ip) => ip,
            Err(_) => self.resolve(host).await?,
        };
        self.connect_tcp(SocketAddr::new(ip.into(), port)).await
    }

    /// Open a TCP connection to an address inside the namespace.
    pub async fn connect_tcp(&self, addr: SocketAddr) -> anyhow::Result<TcpStream> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(Command::ConnectTcp {
                addr,
                reply: reply_tx,
            })
            .map_err(|_| anyhow::anyhow!("tunnel shut down"))?;
        self.inner.notify.notify_one();

        let (handle, state) =
            reply_rx.await.map_err(|_| anyhow::anyhow!("tunnel shut down"))??;

        Ok(TcpStream {
            inner: Arc::clone(&self.inner),
            state,
            _handle: handle,
            peer_addr: addr,
        })
    }

    /// Bind a UDP socket inside the namespace on the given port.
    pub async fn bind_udp(&self, port: u16) -> anyhow::Result<UdpSocket> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(Command::BindUdp {
                port,
                reply: reply_tx,
            })
            .map_err(|_| anyhow::anyhow!("tunnel shut down"))?;
        self.inner.notify.notify_one();

        let (handle, state) =
            reply_rx.await.map_err(|_| anyhow::anyhow!("tunnel shut down"))??;

        Ok(UdpSocket {
            inner: Arc::clone(&self.inner),
            state,
            _handle: handle,
            port,
        })
    }

    /// Shut down the tunnel gracefully and disconnect from the namespace via gRPC.
    ///
    /// Sends a shutdown command to the poll loop, allowing it to drain
    /// pending TX data before exiting. Falls back to abort on send failure.
    pub async fn disconnect(
        mut self,
        client: &mut Client,
        namespace_id: &str,
    ) -> anyhow::Result<()> {
        let public_key = *self.provisioned.public_key();
        if let Some(task) = self.task.take() {
            // Request graceful shutdown; fall back to abort if channel is closed.
            if self.inner.cmd_tx.send(Command::Shutdown).is_ok() {
                self.inner.notify.notify_one();
                let _ = task.await;
            } else {
                task.abort();
                let _ = task.await;
            }
        }
        super::disconnect(client, namespace_id, &public_key).await
    }
}

impl Drop for UserspaceNetwork {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

// ── TCP stream ─────────────────────────────────────────────────────────────

/// A TCP stream over the userspace tunnel.
///
/// Implements [`AsyncRead`] and [`AsyncWrite`]. Multiple streams can
/// coexist on the same [`UserspaceNetwork`].
pub struct TcpStream {
    inner: Arc<Inner>,
    state: Arc<Mutex<TcpSocketState>>,
    _handle: SocketHandle,
    peer_addr: SocketAddr,
}

impl TcpStream {
    /// Remote address of this connection.
    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }
}

impl AsyncRead for TcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut state = self.state.lock().unwrap();

        if let Some(ref err) = state.error {
            return Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, err.clone())));
        }

        if !state.read_buf.is_empty() {
            let to_read = buf.remaining().min(state.read_buf.len());
            let (a, b) = state.read_buf.as_slices();
            if to_read <= a.len() {
                buf.put_slice(&a[..to_read]);
            } else {
                buf.put_slice(a);
                buf.put_slice(&b[..to_read - a.len()]);
            }
            state.read_buf.drain(..to_read);
            return Poll::Ready(Ok(()));
        }

        if state.read_closed {
            // EOF.
            return Poll::Ready(Ok(()));
        }

        // No data available — register waker and return Pending.
        state.read_waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl AsyncWrite for TcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut state = self.state.lock().unwrap();

        if let Some(ref err) = state.error {
            return Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, err.clone())));
        }

        if state.write_closed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "stream shut down",
            )));
        }

        if state.write_buf.len() >= TCP_WRITE_BUF_LIMIT {
            // Backpressure: wait until the poll loop drains some data.
            state.write_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }

        let available = TCP_WRITE_BUF_LIMIT - state.write_buf.len();
        let to_write = buf.len().min(available);
        state.write_buf.extend(&buf[..to_write]);

        // Wake the poll loop so it drains write_buf into smoltcp.
        drop(state);
        self.inner.notify.notify_one();

        Poll::Ready(Ok(to_write))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut state = self.state.lock().unwrap();

        if let Some(ref err) = state.error {
            return Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, err.clone())));
        }

        if state.write_buf.is_empty() {
            Poll::Ready(Ok(()))
        } else {
            state.write_waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut state = self.state.lock().unwrap();

        if let Some(ref err) = state.error {
            return Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, err.clone())));
        }

        if !state.write_closed {
            state.write_closed = true;
            // Wake poll loop to initiate the TCP close.
            drop(state);
            self.inner.notify.notify_one();
            return Poll::Pending;
        }

        // Already requested shutdown — wait for the TCP close to complete.
        if state.close_complete {
            Poll::Ready(Ok(()))
        } else {
            state.shutdown_waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

// ── UDP socket ─────────────────────────────────────────────────────────────

/// A UDP socket over the userspace tunnel.
pub struct UdpSocket {
    inner: Arc<Inner>,
    state: Arc<Mutex<UdpSocketState>>,
    _handle: SocketHandle,
    port: u16,
}

impl UdpSocket {
    /// The local port this socket is bound to.
    pub fn local_port(&self) -> u16 {
        self.port
    }

    /// Send a datagram to the given address.
    pub async fn send_to(&self, buf: &[u8], addr: SocketAddr) -> anyhow::Result<usize> {
        let len = buf.len();
        let mut data = Some(buf.to_vec());
        let state = Arc::clone(&self.state);
        let inner = Arc::clone(&self.inner);

        std::future::poll_fn(move |cx| {
            let mut s = state.lock().unwrap();

            if let Some(ref err) = s.error {
                return Poll::Ready(Err(anyhow::anyhow!("{}", err)));
            }

            if s.send_queue.len() >= UDP_SEND_QUEUE_LIMIT {
                s.send_waker = Some(cx.waker().clone());
                return Poll::Pending;
            }

            s.send_queue.push_back((data.take().unwrap(), addr));
            drop(s);
            inner.notify.notify_one();
            Poll::Ready(Ok(len))
        })
        .await
    }

    /// Receive a datagram, returning the number of bytes read and the source address.
    pub async fn recv_from(&self, buf: &mut [u8]) -> anyhow::Result<(usize, SocketAddr)> {
        let state = Arc::clone(&self.state);

        std::future::poll_fn(move |cx| {
            let mut s = state.lock().unwrap();

            if let Some(ref err) = s.error {
                return Poll::Ready(Err(anyhow::anyhow!("{}", err)));
            }

            if let Some((data, addr)) = s.recv_queue.pop_front() {
                let to_copy = data.len().min(buf.len());
                buf[..to_copy].copy_from_slice(&data[..to_copy]);
                // Return the actual datagram length so callers can detect
                // truncation (returned len > buf.len()).
                return Poll::Ready(Ok((data.len(), addr)));
            }

            s.recv_waker = Some(cx.waker().clone());
            Poll::Pending
        })
        .await
    }
}

// ── Helpers ────────────────────────────────────────────────────────────────

fn smol_now(epoch: std::time::Instant) -> SmolInstant {
    SmolInstant::from_millis(epoch.elapsed().as_millis() as i64)
}

fn smol_endpoint_to_socketaddr(ep: IpEndpoint) -> Option<SocketAddr> {
    let IpAddress::Ipv4(v4) = ep.addr;
    let octets = v4.octets();
    let ip = std::net::Ipv4Addr::new(octets[0], octets[1], octets[2], octets[3]);
    Some(SocketAddr::from((ip, ep.port)))
}

// ── Poll loop (background task) ────────────────────────────────────────────

async fn poll_loop(
    tunn: boringtun::noise::Tunn,
    udp: tokio::net::UdpSocket,
    endpoint: SocketAddr,
    client_ip: Ipv4Addr,
    gateway_ip: Ipv4Addr,
    prefix_len: u8,
    mut cmd_rx: mpsc::UnboundedReceiver<Command>,
    inner: Arc<Inner>,
) -> anyhow::Result<()> {
    use boringtun::noise::TunnResult;

    let mut tunn = tunn;
    let epoch = std::time::Instant::now();

    // Set up smoltcp interface.
    let mut device = ChannelDevice::new();
    let mut config = Config::new(smoltcp::wire::HardwareAddress::Ip);
    config.random_seed = rand::random();
    let mut iface = Interface::new(config, &mut device, smol_now(epoch));
    iface.update_ip_addrs(|addrs| {
        addrs
            .push(IpCidr::new(
                IpAddress::Ipv4(Ipv4Address::from(client_ip)),
                prefix_len,
            ))
            .unwrap();
    });

    let mut sockets = SocketSet::new(vec![]);

    // DNS socket for resolving hostnames via the namespace's gateway DNS server.
    let gateway_smoltcp = IpAddress::Ipv4(Ipv4Address::from(gateway_ip));
    let dns_socket = dns::Socket::new(&[gateway_smoltcp], vec![]);
    let dns_handle = sockets.add(dns_socket);
    let mut dns_pending: Vec<(dns::QueryHandle, oneshot::Sender<anyhow::Result<Ipv4Addr>>)> =
        Vec::new();

    // Tracked socket states, keyed by smoltcp SocketHandle.
    let mut tcp_states: HashMap<SocketHandle, Arc<Mutex<TcpSocketState>>> = HashMap::new();
    let mut udp_states: HashMap<SocketHandle, Arc<Mutex<UdpSocketState>>> = HashMap::new();

    let mut udp_buf = vec![0u8; 65536];
    let mut enc_buf = vec![0u8; 65536];
    let mut dec_buf = vec![0u8; 65536];
    let mut timer_buf = vec![0u8; 65536];

    let mut wg_timer = tokio::time::interval(std::time::Duration::from_millis(250));
    let mut shutting_down = false;

    let mut notified = std::pin::pin!(inner.notify.notified());
    loop {
        tokio::select! {
            // Process commands from connect_tcp / bind_udp.
            Some(cmd) = cmd_rx.recv() => {
                match cmd {
                    Command::ConnectTcp { addr, reply } => {
                        let result = (|| -> anyhow::Result<(SocketHandle, Arc<Mutex<TcpSocketState>>)> {
                            let rx_buf = tcp::SocketBuffer::new(vec![0u8; 65536]);
                            let tx_buf = tcp::SocketBuffer::new(vec![0u8; 65536]);
                            let mut socket = tcp::Socket::new(rx_buf, tx_buf);

                            let remote = match addr {
                                SocketAddr::V4(v4) => {
                                    (Ipv4Address::from(*v4.ip()), v4.port())
                                }
                                SocketAddr::V6(_) => bail!("IPv6 not supported"),
                            };

                            // Pick an ephemeral port that doesn't collide with existing sockets.
                            let local_port = {
                                let mut port = None;
                                for _ in 0..100 {
                                    let candidate = 49152 + (rand::random::<u16>() % 16384);
                                    let in_use = tcp_states.keys().any(|&h| {
                                        let s = sockets.get::<tcp::Socket>(h);
                                        s.local_endpoint().map_or(false, |ep| ep.port == candidate)
                                    });
                                    if !in_use {
                                        port = Some(candidate);
                                        break;
                                    }
                                }
                                port.ok_or_else(|| anyhow::anyhow!("no free ephemeral port"))?
                            };
                            socket.connect(
                                iface.context(),
                                remote,
                                local_port,
                            ).map_err(|e| anyhow::anyhow!("tcp connect: {}", e))?;

                            let handle = sockets.add(socket);
                            let state = Arc::new(Mutex::new(TcpSocketState::new()));
                            tcp_states.insert(handle, Arc::clone(&state));
                            Ok((handle, state))
                        })();
                        let _ = reply.send(result);
                    }
                    Command::BindUdp { port, reply } => {
                        let result = (|| -> anyhow::Result<(SocketHandle, Arc<Mutex<UdpSocketState>>)> {
                            let rx_buf = udp::PacketBuffer::new(
                                vec![udp::PacketMetadata::EMPTY; 64],
                                vec![0u8; 65536],
                            );
                            let tx_buf = udp::PacketBuffer::new(
                                vec![udp::PacketMetadata::EMPTY; 64],
                                vec![0u8; 65536],
                            );
                            let mut socket = udp::Socket::new(rx_buf, tx_buf);
                            socket.bind(port).map_err(|e| anyhow::anyhow!("udp bind: {}", e))?;
                            let handle = sockets.add(socket);
                            let state = Arc::new(Mutex::new(UdpSocketState::new()));
                            udp_states.insert(handle, Arc::clone(&state));
                            Ok((handle, state))
                        })();
                        let _ = reply.send(result);
                    }
                    Command::Resolve { name, reply } => {
                        let dns_socket = sockets.get_mut::<dns::Socket>(dns_handle);
                        match dns_socket.start_query(iface.context(), &name, smoltcp::wire::DnsQueryType::A) {
                            Ok(handle) => {
                                dns_pending.push((handle, reply));
                            }
                            Err(e) => {
                                let _ = reply.send(Err(anyhow::anyhow!("dns start_query: {}", e)));
                            }
                        }
                    }
                    Command::Shutdown => {
                        shutting_down = true;
                        cmd_rx.close();
                    }
                }
            }

            // Receive WireGuard packets from the UDP socket.
            result = udp.recv_from(&mut udp_buf) => {
                let (n, src) = result?;
                let datagram = &udp_buf[..n];

                let result = tunn.decapsulate(Some(src.ip()), datagram, &mut dec_buf);

                match result {
                    TunnResult::WriteToTunnelV4(ip_packet, _) => {
                        device.rx_queue.push_back(ip_packet.to_vec());
                    }
                    TunnResult::WriteToNetwork(data) => {
                        let data = data.to_vec();
                        udp.send_to(&data, endpoint).await?;
                        // Handshake continuation.
                        loop {
                            let cont = tunn.decapsulate(None, &[], &mut dec_buf);
                            match cont {
                                TunnResult::Done => break,
                                TunnResult::WriteToNetwork(data) => {
                                    let data = data.to_vec();
                                    udp.send_to(&data, endpoint).await?;
                                }
                                _ => break,
                            }
                        }
                    }
                    TunnResult::Done => {}
                    TunnResult::Err(e) => {
                        log::warn!("wg decapsulate error: {:?}", e);
                    }
                    _ => {}
                }
            }

            // WireGuard timer tick.
            _ = wg_timer.tick() => {
                let result = tunn.update_timers(&mut timer_buf);
                match result {
                    TunnResult::WriteToNetwork(data) => {
                        let data = data.to_vec();
                        udp.send_to(&data, endpoint).await?;
                    }
                    TunnResult::Err(e) => {
                        log::warn!("wg timer error: {:?}", e);
                    }
                    _ => {}
                }
            }

            // Woken by socket handles that have data to send.
            _ = &mut notified => {
                notified.set(inner.notify.notified());
            }
        }

        // ── Sync: write_buf → smoltcp (before poll) ────────────────────────

        // Drain caller write buffers into smoltcp TCP sockets.
        for (&handle, state) in &tcp_states {
            let mut s = state.lock().unwrap();
            let socket = sockets.get_mut::<tcp::Socket>(handle);

            // Drain write_buf into smoltcp's send buffer.
            if !s.write_buf.is_empty() && socket.can_send() {
                let (a, b) = s.write_buf.as_slices();
                let mut sent = 0;
                if !a.is_empty() {
                    match socket.send_slice(a) {
                        Ok(n) => sent += n,
                        Err(_) => {}
                    }
                }
                if sent == a.len() && !b.is_empty() {
                    match socket.send_slice(b) {
                        Ok(n) => sent += n,
                        Err(_) => {}
                    }
                }
                if sent > 0 {
                    s.write_buf.drain(..sent);
                    // Wake writer if it was backpressured.
                    if s.write_buf.len() < TCP_WRITE_BUF_LIMIT {
                        if let Some(waker) = s.write_waker.take() {
                            waker.wake();
                        }
                    }
                }
            }

            // Handle shutdown: close the smoltcp socket once write_buf is drained.
            if s.write_closed && !s.close_initiated && s.write_buf.is_empty() && socket.may_send() {
                socket.close();
                s.close_initiated = true;
            }
        }

        // Drain caller send queues into smoltcp UDP sockets.
        for (&handle, state) in &udp_states {
            let mut s = state.lock().unwrap();
            let socket = sockets.get_mut::<udp::Socket>(handle);

            while let Some((data, addr)) = s.send_queue.front() {
                let dest = match addr {
                    SocketAddr::V4(v4) => {
                        IpEndpoint::new(
                            IpAddress::Ipv4(Ipv4Address::from(*v4.ip())),
                            v4.port(),
                        )
                    }
                    SocketAddr::V6(_) => {
                        // Drop the datagram.
                        s.send_queue.pop_front();
                        continue;
                    }
                };

                if socket.can_send() {
                    match socket.send_slice(&data, dest) {
                        Ok(()) => {
                            s.send_queue.pop_front();
                        }
                        Err(_) => break,
                    }
                } else {
                    break;
                }
            }

            // Wake sender if we drained some space.
            if s.send_queue.len() < UDP_SEND_QUEUE_LIMIT {
                if let Some(waker) = s.send_waker.take() {
                    waker.wake();
                }
            }
        }

        // ── Poll smoltcp ───────────────────────────────────────────────────

        iface.poll(smol_now(epoch), &mut device, &mut sockets);

        // ── Check DNS query results ───────────────────────────────────────

        {
            let dns_socket = sockets.get_mut::<dns::Socket>(dns_handle);
            let pending = std::mem::take(&mut dns_pending);
            for (handle, reply) in pending {
                match dns_socket.get_query_result(handle) {
                    Ok(addrs) => {
                        let result = addrs
                            .iter()
                            .find_map(|addr| match addr {
                                IpAddress::Ipv4(v4) => Some(Ipv4Addr::from(v4.octets())),
                            })
                            .ok_or_else(|| anyhow::anyhow!("no IPv4 address in DNS response"));
                        let _ = reply.send(result);
                    }
                    Err(dns::GetQueryResultError::Pending) => {
                        dns_pending.push((handle, reply));
                    }
                    Err(dns::GetQueryResultError::Failed) => {
                        let _ = reply.send(Err(anyhow::anyhow!("DNS query failed")));
                    }
                }
            }
        }

        // ── Sync: smoltcp → read_buf / recv_queue (after poll) ─────────────

        // Read data from smoltcp TCP sockets into caller read buffers.
        for (&handle, state) in &tcp_states {
            let mut s = state.lock().unwrap();
            let socket = sockets.get_mut::<tcp::Socket>(handle);

            // Receive data (skip if read_buf is full to apply TCP window backpressure).
            if socket.can_recv() && s.read_buf.len() < TCP_READ_BUF_LIMIT {
                let budget = TCP_READ_BUF_LIMIT - s.read_buf.len();
                match socket.recv(|data| {
                    let take = data.len().min(budget);
                    s.read_buf.extend(data[..take].iter().copied());
                    (take, ())
                }) {
                    Ok(()) => {
                        if let Some(waker) = s.read_waker.take() {
                            waker.wake();
                        }
                    }
                    Err(_) => {}
                }
            }

            // Detect close / error.
            let sock_state = socket.state();
            if !s.read_closed {
                match sock_state {
                    tcp::State::CloseWait
                    | tcp::State::LastAck
                    | tcp::State::Closed
                    | tcp::State::Closing
                    | tcp::State::TimeWait => {
                        s.read_closed = true;
                        if let Some(waker) = s.read_waker.take() {
                            waker.wake();
                        }
                    }
                    _ => {}
                }
            }

            // Signal shutdown completion once the TCP close sequence finishes.
            if s.write_closed && !s.close_complete {
                match sock_state {
                    tcp::State::Closed | tcp::State::TimeWait => {
                        s.close_complete = true;
                        if let Some(waker) = s.shutdown_waker.take() {
                            waker.wake();
                        }
                    }
                    _ => {}
                }
            }
        }

        // Reap closed sockets whose handles have been dropped.
        tcp_states.retain(|&handle, state| {
            let socket = sockets.get_mut::<tcp::Socket>(handle);
            if socket.state() == tcp::State::Closed && Arc::strong_count(state) == 1 {
                sockets.remove(handle);
                return false;
            }
            true
        });
        udp_states.retain(|&handle, state| {
            if Arc::strong_count(state) == 1 {
                sockets.remove(handle);
                return false;
            }
            true
        });

        // Read datagrams from smoltcp UDP sockets into caller recv queues.
        for (&handle, state) in &udp_states {
            let mut s = state.lock().unwrap();
            let socket = sockets.get_mut::<udp::Socket>(handle);

            while socket.can_recv() && s.recv_queue.len() < UDP_RECV_QUEUE_LIMIT {
                match socket.recv() {
                    Ok((data, meta)) => {
                        if let Some(addr) = smol_endpoint_to_socketaddr(meta.endpoint) {
                            s.recv_queue.push_back((data.to_vec(), addr));
                        }
                    }
                    Err(_) => break,
                }
            }

            if !s.recv_queue.is_empty() {
                if let Some(waker) = s.recv_waker.take() {
                    waker.wake();
                }
            }
        }

        // ── Drain tx_queue: encrypt and send via WireGuard ─────────────────

        for pkt in device.tx_queue.drain(..) {
            let result = tunn.encapsulate(&pkt, &mut enc_buf);
            match result {
                TunnResult::WriteToNetwork(data) => {
                    udp.send_to(data, endpoint).await?;
                }
                TunnResult::Err(e) => {
                    log::warn!("wg encapsulate error: {:?}", e);
                }
                _ => {}
            }
        }

        // Exit after draining all pending TX if shutting down.
        if shutting_down && device.tx_queue.is_empty() {
            break;
        }
    }

    Ok(())
}
