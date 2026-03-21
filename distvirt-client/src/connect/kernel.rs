use std::sync::Arc;

use anyhow::Context;
use tokio::sync::Mutex;

use super::platform::{TunDevice, add_route, configure_interface, remove_route};
use super::wireguard;
use super::{ConnectInfo, ProvisionedTunnel};
use crate::connection::Client;

/// An active WireGuard tunnel using an OS TUN device.
///
/// Requires root or `CAP_NET_ADMIN`. Created via
/// [`ProvisionedTunnel::into_kernel`].
pub struct KernelTunnel {
    tun: Arc<TunDevice>,
    tunn: Arc<Mutex<boringtun::noise::Tunn>>,
    udp: Arc<tokio::net::UdpSocket>,
    provisioned: ProvisionedTunnel,
}

impl ProvisionedTunnel {
    /// Materialize this tunnel as an OS TUN device with kernel routing.
    ///
    /// Creates a TUN device, configures its IP address, and adds a route
    /// for the namespace subnet. Requires root or `CAP_NET_ADMIN`.
    ///
    /// Fails with `io::ErrorKind::PermissionDenied` if the process lacks
    /// privileges to create TUN devices.
    pub async fn into_kernel(self) -> anyhow::Result<KernelTunnel> {
        let tun = TunDevice::create().context("failed to create TUN device")?;

        configure_interface(&tun.name, &self.client_ip.to_string(), self.prefix_len)?;
        add_route(&self.subnet, &tun.name)?;

        let (tunn, udp) = self.create_wg_tunnel().await?;

        Ok(KernelTunnel {
            tun: Arc::new(tun),
            tunn: Arc::new(Mutex::new(tunn)),
            udp: Arc::new(udp),
            provisioned: self,
        })
    }
}

impl KernelTunnel {
    /// Connection metadata.
    pub fn info(&self) -> ConnectInfo {
        self.provisioned.info()
    }

    /// The OS name of the TUN device (e.g. "tun0").
    pub fn tun_name(&self) -> &str {
        &self.tun.name
    }

    /// The client's WireGuard public key.
    pub fn public_key(&self) -> &[u8; 32] {
        self.provisioned.public_key()
    }

    /// Run the packet forwarding loop. Returns on error (never returns Ok naturally).
    /// Caller is responsible for cancellation (e.g. `select!` with ctrl+c).
    pub async fn run(&self) -> anyhow::Result<()> {
        wireguard::run_tunnel(
            Arc::clone(&self.tun),
            Arc::clone(&self.tunn),
            Arc::clone(&self.udp),
            self.provisioned.endpoint,
        )
        .await
    }

    /// Cleanup: remove routes and call DisconnectNetwork gRPC.
    pub async fn disconnect(self, client: &mut Client, namespace_id: &str) -> anyhow::Result<()> {
        let _ = remove_route(&self.provisioned.subnet, &self.tun.name);
        super::disconnect(client, namespace_id, self.provisioned.public_key()).await
    }
}
