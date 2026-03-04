use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

use tokio::io::unix::AsyncFd;
use tokio::sync::mpsc;

use crate::packet::{FABRIC_HDR_SZ, FLAG_NEEDS_CSUM, IP_PROTO_TCP, IP_PROTO_UDP};
use crate::tap::TapDevice;

/// Unique identifier for a port within the fabric.
pub type PortId = usize;

/// Trait abstracting async fabric packet send/receive.
///
/// Implemented by `Port` for real TAP devices and by test doubles in tests.
pub trait FramePort: Send + Sync + 'static {
    fn recv_frame(&self, buf: &mut [u8]) -> impl std::future::Future<Output = io::Result<usize>> + Send;
    fn send_frame(&self, buf: &[u8]) -> impl std::future::Future<Output = io::Result<usize>> + Send;
}

/// An async port wrapping a TapDevice's AF_PACKET socket.
///
/// Uses tokio's `AsyncFd` for readiness notification, then performs
/// non-blocking `recv`/`send` via libc.
pub struct Port {
    async_fd: AsyncFd<OwnedFd>,
    /// The underlying TapDevice (kept alive for Drop cleanup).
    _tap: TapDevice,
    /// Guest MAC address — used as the destination MAC when sending frames to the guest.
    /// TCP requires `pkt_type == PACKET_HOST`; using broadcast would set PACKET_BROADCAST
    /// and cause the guest kernel to silently drop TCP segments.
    guest_mac: [u8; 6],
}

impl Port {
    /// Create a new async port from a TapDevice.
    ///
    /// `guest_mac` is the MAC address configured on the guest's network interface.
    /// It is used as the destination MAC when injecting frames into the TAP device.
    /// Sets O_NONBLOCK on the socket fd before wrapping in AsyncFd.
    pub fn new(tap: TapDevice, guest_mac: [u8; 6]) -> io::Result<Self> {
        // Set non-blocking mode on the socket fd.
        let raw_fd = tap.socket.as_raw_fd();
        let flags = unsafe { libc::fcntl(raw_fd, libc::F_GETFL) };
        if flags < 0 {
            return Err(io::Error::last_os_error());
        }
        let ret = unsafe { libc::fcntl(raw_fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
        if ret < 0 {
            return Err(io::Error::last_os_error());
        }

        // We need to separate the OwnedFd from the TapDevice for AsyncFd,
        // but TapDevice must stay alive (it cleans up the TAP on Drop).
        // Use dup() to create a second fd for AsyncFd.
        let dup_fd = unsafe { libc::dup(raw_fd) };
        if dup_fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // Set non-blocking on the dup'd fd too.
        let flags2 = unsafe { libc::fcntl(dup_fd, libc::F_GETFL) };
        if flags2 >= 0 {
            unsafe { libc::fcntl(dup_fd, libc::F_SETFL, flags2 | libc::O_NONBLOCK) };
        }
        let owned_dup = unsafe { OwnedFd::from_raw_fd(dup_fd) };

        let async_fd = AsyncFd::new(owned_dup)?;

        Ok(Port {
            async_fd,
            _tap: tap,
            guest_mac,
        })
    }

    /// Size of the virtio-net header prepended by AF_PACKET with PACKET_VNET_HDR.
    const VNET_HDR_SZ: usize = 10;
    /// Size of an Ethernet header (dst MAC + src MAC + ethertype).
    const ETH_HDR_SZ: usize = 14;
    /// Total TAP overhead: vnet header + Ethernet header.
    const TAP_HDR_SZ: usize = Self::VNET_HDR_SZ + Self::ETH_HDR_SZ; // 24

    /// Fixed MAC used as the "gateway" source when sending frames to the guest.
    /// Must not collide with any guest MAC (guests typically use 02:00:00:00:00:XX).
    const GATEWAY_MAC: [u8; 6] = [0x02, 0xFB, 0x00, 0x00, 0x00, 0x01];

    /// ARP constants.
    const ETHERTYPE_ARP: u16 = 0x0806;
    const ETHERTYPE_IPV4: u16 = 0x0800;
    const ARP_REQUEST: u16 = 1;
    const ARP_REPLY: u16 = 2;

    /// Asynchronously receive raw bytes from the AF_PACKET socket.
    async fn recv_raw(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            let mut guard = self.async_fd.readable().await?;

            match guard.try_io(|inner| {
                let fd = inner.as_raw_fd();
                let n = unsafe {
                    libc::recv(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len(), 0)
                };
                if n < 0 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(n as usize)
                }
            }) {
                Ok(result) => return result,
                Err(_would_block) => continue,
            }
        }
    }

    /// Asynchronously send raw bytes to the AF_PACKET socket.
    async fn send_raw(&self, buf: &[u8]) -> io::Result<usize> {
        loop {
            let mut guard = self.async_fd.writable().await?;

            match guard.try_io(|inner| {
                let fd = inner.as_raw_fd();
                let n = unsafe {
                    libc::send(fd, buf.as_ptr() as *const libc::c_void, buf.len(), 0)
                };
                if n < 0 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(n as usize)
                }
            }) {
                Ok(result) => return result,
                Err(_would_block) => continue,
            }
        }
    }

    /// Build and send an ARP reply for an ARP request received from the guest.
    ///
    /// Acts as a proxy ARP responder: replies to all ARP requests with
    /// `GATEWAY_MAC`, so the guest can route all traffic through us.
    async fn handle_arp_request(&self, tap_frame: &[u8], n: usize) -> io::Result<()> {
        // ARP packet starts after vnet(10) + eth(14) = offset 24.
        // ARP for IPv4/Ethernet is 28 bytes.
        let arp_start = Self::TAP_HDR_SZ;
        if n < arp_start + 28 {
            return Ok(());
        }

        let arp = &tap_frame[arp_start..];
        // Check hardware type (1 = Ethernet) and protocol type (0x0800 = IPv4).
        let hw_type = u16::from_be_bytes([arp[0], arp[1]]);
        let proto_type = u16::from_be_bytes([arp[2], arp[3]]);
        let hw_len = arp[4];
        let proto_len = arp[5];
        let oper = u16::from_be_bytes([arp[6], arp[7]]);

        if hw_type != 1 || proto_type != 0x0800 || hw_len != 6 || proto_len != 4 || oper != Self::ARP_REQUEST {
            return Ok(());
        }

        // Extract sender MAC (arp[8..14]), sender IP (arp[14..18]), target IP (arp[24..28]).
        let sender_mac = &tap_frame[arp_start + 8..arp_start + 14];
        let sender_ip = &tap_frame[arp_start + 14..arp_start + 18];
        let target_ip = &tap_frame[arp_start + 24..arp_start + 28];

        // Build ARP reply: [vnet(10)][eth(14)][ARP(28)]
        let mut reply = vec![0u8; Self::VNET_HDR_SZ + Self::ETH_HDR_SZ + 28];

        // vnet header: all zeros (no checksum offload needed for ARP).
        // Ethernet header.
        let eth_start = Self::VNET_HDR_SZ;
        reply[eth_start..eth_start + 6].copy_from_slice(sender_mac); // dst = requester
        reply[eth_start + 6..eth_start + 12].copy_from_slice(&Self::GATEWAY_MAC); // src = us
        reply[eth_start + 12..eth_start + 14].copy_from_slice(&Self::ETHERTYPE_ARP.to_be_bytes());

        // ARP reply.
        let arp_out = &mut reply[Self::TAP_HDR_SZ..];
        arp_out[0..2].copy_from_slice(&1u16.to_be_bytes()); // hw type = Ethernet
        arp_out[2..4].copy_from_slice(&0x0800u16.to_be_bytes()); // proto type = IPv4
        arp_out[4] = 6; // hw len
        arp_out[5] = 4; // proto len
        arp_out[6..8].copy_from_slice(&Self::ARP_REPLY.to_be_bytes());
        // Sender = us (GATEWAY_MAC, target_ip).
        arp_out[8..14].copy_from_slice(&Self::GATEWAY_MAC);
        arp_out[14..18].copy_from_slice(target_ip);
        // Target = requester (sender_mac, sender_ip).
        arp_out[18..24].copy_from_slice(sender_mac);
        arp_out[24..28].copy_from_slice(sender_ip);

        self.send_raw(&reply).await?;
        Ok(())
    }
}

impl FramePort for Port {
    /// Receive a fabric-format packet from the TAP device.
    ///
    /// Reads `[vnet(10)][eth(14)][IP]` from the AF_PACKET socket,
    /// handles ARP requests internally, and returns `[fabric_hdr(3)][IP]`.
    async fn recv_frame(&self, buf: &mut [u8]) -> io::Result<usize> {
        let mut tap_buf = [0u8; Self::VNET_HDR_SZ + 1514]; // max Ethernet frame + vnet
        loop {
            let n = self.recv_raw(&mut tap_buf).await?;

            if n < Self::TAP_HDR_SZ {
                continue; // too short for vnet + ethernet header
            }

            // Extract ethertype from Ethernet header.
            let ethertype = u16::from_be_bytes([
                tap_buf[Self::VNET_HDR_SZ + 12],
                tap_buf[Self::VNET_HDR_SZ + 13],
            ]);

            if ethertype == Self::ETHERTYPE_ARP {
                // Handle ARP internally: reply with GATEWAY_MAC for any IP.
                if let Err(e) = self.handle_arp_request(&tap_buf, n).await {
                    log::warn!("fabric: port ARP reply error: {}", e);
                }
                continue; // ARP handled, wait for next frame
            }

            if ethertype != Self::ETHERTYPE_IPV4 {
                continue; // skip non-IPv4 (IPv6, etc.)
            }

            let ip_start = Self::TAP_HDR_SZ;
            let ip_len = n - ip_start;
            if ip_len < 20 {
                continue; // IP packet too short
            }

            let fabric_len = FABRIC_HDR_SZ + ip_len;
            if fabric_len > buf.len() {
                continue; // output buffer too small
            }

            // Extract NEEDS_CSUM from vnet header flags byte.
            let needs_csum = tap_buf[0] & 1; // VIRTIO_NET_HDR_F_NEEDS_CSUM

            // Write fabric header.
            buf[0] = if needs_csum != 0 { FLAG_NEEDS_CSUM } else { 0 };
            buf[1] = 0; // segment_id high byte
            buf[2] = 0; // segment_id low byte

            // Copy IP packet.
            buf[FABRIC_HDR_SZ..fabric_len].copy_from_slice(&tap_buf[ip_start..n]);

            return Ok(fabric_len);
        }
    }

    /// Send a fabric-format packet to the TAP device.
    ///
    /// Converts `[fabric_hdr(3)][IP]` to `[vnet(10)][eth(14)][IP]` for the guest.
    /// If NEEDS_CSUM is set, passes it through as a vnet checksum offload flag
    /// (AF_PACKET → TAP → virtio-net propagates NEEDS_CSUM correctly).
    async fn send_frame(&self, buf: &[u8]) -> io::Result<usize> {
        if buf.len() < FABRIC_HDR_SZ + 20 {
            return Ok(0);
        }

        let ip_packet = &buf[FABRIC_HDR_SZ..];

        // Build vnet header: pass through NEEDS_CSUM as virtio-net offload.
        let mut vnet_hdr = [0u8; Self::VNET_HDR_SZ];
        if buf[0] & FLAG_NEEDS_CSUM != 0 {
            vnet_hdr[0] = 1; // VIRTIO_NET_HDR_F_NEEDS_CSUM
            let ihl = (ip_packet[0] & 0x0f) as usize * 4;
            let protocol = ip_packet[9];
            let csum_offset: u16 = match protocol {
                IP_PROTO_TCP => 16,
                IP_PROTO_UDP => 6,
                _ => 0,
            };
            let csum_start = (Self::ETH_HDR_SZ + ihl) as u16;
            vnet_hdr[6..8].copy_from_slice(&csum_start.to_le_bytes());
            vnet_hdr[8..10].copy_from_slice(&csum_offset.to_le_bytes());
        }

        // Build TAP frame: [vnet(10)][eth(14)][IP].
        let total_len = Self::VNET_HDR_SZ + Self::ETH_HDR_SZ + ip_packet.len();
        let mut tap_frame = Vec::with_capacity(total_len);

        tap_frame.extend_from_slice(&vnet_hdr);

        // Ethernet header: dst=guest MAC, src=gateway MAC, ethertype=IPv4.
        tap_frame.extend_from_slice(&self.guest_mac); // dst
        tap_frame.extend_from_slice(&Self::GATEWAY_MAC); // src
        tap_frame.extend_from_slice(&Self::ETHERTYPE_IPV4.to_be_bytes());

        // IP packet.
        tap_frame.extend_from_slice(ip_packet);

        self.send_raw(&tap_frame).await
    }
}

/// A channel-backed port for adapter virtual interfaces.
///
/// The fabric side holds a `ChannelPort`; the adapter side holds the
/// opposite ends of the mpsc channels.
pub struct ChannelPort {
    rx: tokio::sync::Mutex<mpsc::Receiver<Vec<u8>>>,
    tx: mpsc::Sender<Vec<u8>>,
}

impl ChannelPort {
    /// Create a new channel port pair.
    ///
    /// Returns `(port, adapter_tx, adapter_rx)` where:
    /// - `port` is the fabric-side `ChannelPort` (implements `FramePort`)
    /// - `adapter_tx` sends frames *into* the fabric (adapter → fabric)
    /// - `adapter_rx` receives frames *from* the fabric (fabric → adapter)
    pub fn new(buffer_size: usize) -> (Self, mpsc::Sender<Vec<u8>>, mpsc::Receiver<Vec<u8>>) {
        // adapter→fabric direction
        let (adapter_tx, fabric_rx) = mpsc::channel(buffer_size);
        // fabric→adapter direction
        let (fabric_tx, adapter_rx) = mpsc::channel(buffer_size);

        let port = ChannelPort {
            rx: tokio::sync::Mutex::new(fabric_rx),
            tx: fabric_tx,
        };

        (port, adapter_tx, adapter_rx)
    }
}

impl FramePort for ChannelPort {
    async fn recv_frame(&self, buf: &mut [u8]) -> io::Result<usize> {
        let mut rx = self.rx.lock().await;
        match rx.recv().await {
            Some(frame) => {
                let len = frame.len().min(buf.len());
                buf[..len].copy_from_slice(&frame[..len]);
                Ok(len)
            }
            None => Err(io::Error::new(io::ErrorKind::BrokenPipe, "channel closed")),
        }
    }

    async fn send_frame(&self, buf: &[u8]) -> io::Result<usize> {
        let data = buf.to_vec();
        let len = data.len();
        self.tx
            .send(data)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "channel closed"))?;
        Ok(len)
    }
}

/// Enum dispatch for fabric ports: either a real TAP or a virtual channel.
pub enum FabricPort {
    Tap(Port),
    Virtual(ChannelPort),
}

impl FramePort for FabricPort {
    async fn recv_frame(&self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            FabricPort::Tap(p) => p.recv_frame(buf).await,
            FabricPort::Virtual(p) => p.recv_frame(buf).await,
        }
    }

    async fn send_frame(&self, buf: &[u8]) -> io::Result<usize> {
        match self {
            FabricPort::Tap(p) => p.send_frame(buf).await,
            FabricPort::Virtual(p) => p.send_frame(buf).await,
        }
    }
}
