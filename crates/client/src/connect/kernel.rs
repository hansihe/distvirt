use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::Mutex;

use super::platform::TunDevice;
use super::wireguard;

/// An active WireGuard tunnel using an OS TUN device.
///
/// Purely OS-level: owns the TUN device, WireGuard crypto state, and
/// UDP socket. Does not hold provisioning or server-side state.
pub struct KernelTunnel {
    tun: Arc<TunDevice>,
    tunn: Arc<Mutex<boringtun::noise::Tunn>>,
    udp: Arc<tokio::net::UdpSocket>,
    endpoint: SocketAddr,
}

impl KernelTunnel {
    /// Create a new kernel tunnel from pre-built components.
    pub fn new(
        tun: TunDevice,
        tunn: boringtun::noise::Tunn,
        udp: tokio::net::UdpSocket,
        endpoint: SocketAddr,
    ) -> Self {
        KernelTunnel {
            tun: Arc::new(tun),
            tunn: Arc::new(Mutex::new(tunn)),
            udp: Arc::new(udp),
            endpoint,
        }
    }

    /// The OS name of the TUN device (e.g. "tun0").
    pub fn tun_name(&self) -> &str {
        &self.tun.name
    }

    /// Run the packet forwarding loop. Returns on error (never returns Ok naturally).
    /// Caller is responsible for cancellation (e.g. `select!` with ctrl+c).
    pub async fn run(&self) -> anyhow::Result<()> {
        wireguard::run_tunnel(
            Arc::clone(&self.tun),
            Arc::clone(&self.tunn),
            Arc::clone(&self.udp),
            self.endpoint,
        )
        .await
    }
}
