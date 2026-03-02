use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct OrchestratorConfig {
    pub grpc: GrpcConfig,
    pub workers: WorkersConfig,
    #[serde(default)]
    pub wireguard: WireguardConfig,
}

#[derive(Debug, Deserialize)]
pub struct GrpcConfig {
    pub listen: String,
}

#[derive(Debug, Deserialize)]
pub struct WorkersConfig {
    pub listen: String,
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
