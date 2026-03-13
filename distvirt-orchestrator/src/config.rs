use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct OrchestratorConfig {
    pub grpc: GrpcConfig,
    pub workers: WorkersConfig,
    #[serde(default)]
    pub wireguard: WireguardConfig,
    #[serde(default)]
    pub tunnels: TunnelConfig,
    #[serde(default)]
    pub pools: Vec<PoolConfig>,
}

#[derive(Debug, Deserialize)]
pub struct PoolConfig {
    pub pool_id: String,
    pub path: String,
}

#[derive(Debug, Deserialize)]
pub struct GrpcConfig {
    pub listen: String,
    /// Shared secret for client authentication. Can also be set via --client-secret CLI flag.
    pub secret: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct WorkersConfig {
    pub listen: String,
    /// Shared secret for worker authentication. Can also be set via --worker-secret CLI flag.
    pub secret: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct WireguardConfig {
    #[serde(default = "default_wg_port")]
    pub listen_port: u16,
}

impl Default for WireguardConfig {
    fn default() -> Self {
        WireguardConfig {
            listen_port: default_wg_port(),
        }
    }
}

fn default_wg_port() -> u16 {
    51820
}

#[derive(Debug, Deserialize)]
pub struct TunnelConfig {
    #[serde(default = "default_tunnel_encrypted")]
    pub encrypted: bool,
}

impl Default for TunnelConfig {
    fn default() -> Self {
        TunnelConfig {
            encrypted: default_tunnel_encrypted(),
        }
    }
}

fn default_tunnel_encrypted() -> bool {
    true
}
