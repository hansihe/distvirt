pub mod kernel;
pub mod userspace;
pub mod platform;
pub mod wg_ops;
mod wireguard;

use std::net::{Ipv4Addr, SocketAddr};

use anyhow::Context;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use boringtun::noise::Tunn;
use boringtun::x25519::{PublicKey, StaticSecret};
use distvirt_client_protocol::*;
use tokio::net::UdpSocket;

use crate::connection::{Client, handle_grpc_error};

/// Info returned after a tunnel is provisioned.
pub struct ConnectInfo {
    pub client_ip: Ipv4Addr,
    pub gateway_ip: Ipv4Addr,
    pub subnet: String,
    pub endpoint: SocketAddr,
}

/// Network configuration for a WireGuard tunnel, independent of key material.
pub struct TunnelConfig {
    pub server_public_key: PublicKey,
    pub client_ip: Ipv4Addr,
    pub gateway_ip: Ipv4Addr,
    pub subnet: String,
    pub prefix_len: u8,
    pub endpoint: SocketAddr,
}

impl TunnelConfig {
    /// The client IP address assigned by the server.
    pub fn client_ip(&self) -> Ipv4Addr {
        self.client_ip
    }

    /// The subnet prefix length.
    pub fn prefix_len(&self) -> u8 {
        self.prefix_len
    }

    /// The subnet CIDR string (e.g. "10.0.0.0/24").
    pub fn subnet(&self) -> &str {
        &self.subnet
    }

    /// The gateway IP address.
    pub fn gateway_ip(&self) -> Ipv4Addr {
        self.gateway_ip
    }

    /// The WireGuard endpoint address.
    pub fn endpoint(&self) -> SocketAddr {
        self.endpoint
    }
}

/// A provisioned WireGuard tunnel, ready to be materialized as
/// either a kernel TUN tunnel or a userspace smoltcp tunnel.
///
/// Created by calling [`ProvisionedTunnel::connect`], which generates
/// an ephemeral WireGuard keypair and registers with the server via gRPC.
pub struct ProvisionedTunnel {
    private_key: StaticSecret,
    public_key: PublicKey,
    pub config: TunnelConfig,
}

impl ProvisionedTunnel {
    /// Generate an ephemeral WireGuard keypair, call ConnectNetwork gRPC,
    /// and return a provisioned tunnel ready for materialization.
    pub async fn connect(
        client: &mut Client,
        namespace_id: &str,
    ) -> anyhow::Result<Self> {
        let private_key = StaticSecret::from(rand::random::<[u8; 32]>());
        let public_key = PublicKey::from(&private_key);

        let resp = client
            .connect_network(ConnectNetworkRequest {
                namespace_id: namespace_id.to_string(),
                client_public_key: public_key.as_bytes().to_vec(),
            })
            .await
            .map_err(handle_grpc_error)?
            .into_inner();

        let server_public_key_bytes: [u8; 32] = resp
            .server_public_key
            .as_slice()
            .try_into()
            .context("server public key must be 32 bytes")?;
        let endpoint: SocketAddr = resp.endpoint.parse().context("invalid endpoint address")?;
        let client_ip: Ipv4Addr = resp.client_ip.parse().context("invalid client IP")?;
        let subnet = &resp.subnet;
        let prefix_len: u8 = subnet
            .split('/')
            .nth(1)
            .context("subnet missing /prefix_len")?
            .parse()
            .context("invalid prefix length in subnet")?;

        // Gateway is subnet base + 1 (e.g. 10.0.0.0/24 -> 10.0.0.1).
        let subnet_base: Ipv4Addr = subnet
            .split('/')
            .next()
            .unwrap()
            .parse()
            .context("invalid subnet base address")?;
        let gateway_ip = Ipv4Addr::from(u32::from(subnet_base) + 1);

        Ok(ProvisionedTunnel {
            private_key,
            public_key,
            config: TunnelConfig {
                server_public_key: PublicKey::from(server_public_key_bytes),
                client_ip,
                gateway_ip,
                subnet: subnet.clone(),
                prefix_len,
                endpoint,
            },
        })
    }

    /// The client's WireGuard public key.
    pub fn public_key(&self) -> &[u8; 32] {
        self.public_key.as_bytes()
    }

    /// The client IP address assigned by the server.
    pub fn client_ip(&self) -> Ipv4Addr {
        self.config.client_ip
    }

    /// The subnet prefix length.
    pub fn prefix_len(&self) -> u8 {
        self.config.prefix_len
    }

    /// The subnet CIDR string (e.g. "10.0.0.0/24").
    pub fn subnet(&self) -> &str {
        &self.config.subnet
    }

    /// The gateway IP address.
    pub fn gateway_ip(&self) -> Ipv4Addr {
        self.config.gateway_ip
    }

    /// The WireGuard endpoint address.
    pub fn endpoint(&self) -> SocketAddr {
        self.config.endpoint
    }

    pub fn info(&self) -> ConnectInfo {
        ConnectInfo {
            client_ip: self.config.client_ip,
            gateway_ip: self.config.gateway_ip,
            subnet: self.config.subnet.clone(),
            endpoint: self.config.endpoint,
        }
    }

    /// Generate a wg-quick compatible configuration string from this tunnel's
    /// provisioned parameters.
    pub fn to_wg_quick_config(&self) -> String {
        let private_key_b64 = BASE64.encode(self.private_key.to_bytes());
        let server_pub_b64 = BASE64.encode(self.config.server_public_key.as_bytes());
        format!(
            "[Interface]\n\
             PrivateKey = {}\n\
             Address = {}/{}\n\
             \n\
             [Peer]\n\
             PublicKey = {}\n\
             Endpoint = {}\n\
             AllowedIPs = {}\n\
             PersistentKeepalive = 25\n",
            private_key_b64, self.config.client_ip, self.config.prefix_len,
            server_pub_b64, self.config.endpoint, self.config.subnet
        )
    }

    /// Disconnect from the namespace via gRPC. Consumes self — call this
    /// only when tearing down the server-side tunnel state.
    pub async fn disconnect(self, client: &mut Client, namespace_id: &str) -> anyhow::Result<()> {
        disconnect_by_key(client, namespace_id, self.public_key()).await
    }

    /// Create the boringtun Tunn and bind a UDP socket.
    /// Shared setup used by both kernel and userspace paths.
    pub async fn create_wg_tunnel(&self) -> anyhow::Result<(Tunn, UdpSocket)> {
        let tunn = Tunn::new(
            self.private_key.clone(),
            self.config.server_public_key,
            None,
            Some(25), // persistent keepalive
            0,
            None,
        );
        let udp = UdpSocket::bind("0.0.0.0:0").await?;
        Ok((tunn, udp))
    }
}

/// Generate a wg-quick compatible config string without creating a tunnel.
pub async fn wg_quick_config(
    client: &mut Client,
    namespace_id: &str,
) -> anyhow::Result<String> {
    let provisioned = ProvisionedTunnel::connect(client, namespace_id).await?;
    Ok(provisioned.to_wg_quick_config())
}

/// Disconnect from the namespace via gRPC using a raw public key.
pub async fn disconnect_by_key(
    client: &mut Client,
    namespace_id: &str,
    public_key: &[u8; 32],
) -> anyhow::Result<()> {
    let result = client
        .disconnect_network(DisconnectNetworkRequest {
            namespace_id: namespace_id.to_string(),
            client_public_key: public_key.to_vec(),
        })
        .await;

    if let Err(e) = result {
        log::warn!("disconnect gRPC failed: {}", e);
    }

    Ok(())
}
