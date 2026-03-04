pub mod wireguard;

use std::sync::Arc;

use distvirt_worker_protocol::AdapterConfig;

use crate::fabric::ChannelPort;
use wireguard::WireGuardAdapter;

/// RAII guard for an adapter virtual port.
///
/// Dropping this handle signals the adapter to tear down its end of the
/// virtual port (e.g. remove the namespace channel from the WireGuard adapter).
pub struct AdapterPortHandle {
    pub(crate) _drop_guard: Option<Box<dyn Send + Sync + 'static>>,
}

/// Trait for ingress adapters (WireGuard, ReverseProxy, OsRouting, etc.).
///
/// Each adapter can create virtual ports that plug into the fabric via
/// `ChannelPort`.
pub trait IngressAdapter: Send + Sync {
    /// Human-readable adapter type name (e.g. "wireguard").
    fn adapter_type(&self) -> &str;

    /// Create a virtual port for the given namespace.
    fn create_port(
        &self,
        namespace_id: &str,
    ) -> anyhow::Result<(ChannelPort, AdapterPortHandle)>;
}

/// Manages the set of configured ingress adapters.
///
/// Initialized from `AdapterConfig`s received during the worker handshake.
pub struct AdapterManager {
    adapters: Vec<Box<dyn IngressAdapter>>,
    /// Direct reference to the WireGuard adapter for peer management commands.
    wireguard: Option<Arc<WireGuardAdapter>>,
}

impl AdapterManager {
    /// Create an empty adapter manager (no adapters configured).
    pub fn empty() -> Self {
        AdapterManager {
            adapters: Vec::new(),
            wireguard: None,
        }
    }

    /// Create an adapter manager from handshake config.
    pub async fn new(configs: &[AdapterConfig]) -> Self {
        let mut adapters: Vec<Box<dyn IngressAdapter>> = Vec::new();
        let mut wireguard: Option<Arc<WireGuardAdapter>> = None;

        for config in configs {
            match config {
                AdapterConfig::WireGuard {
                    listen_port,
                    private_key,
                } => match WireGuardAdapter::new(*listen_port, private_key).await {
                    Ok(wg) => {
                        log::info!(
                            "adapter: WireGuard adapter initialized on port {}",
                            listen_port
                        );
                        let wg = Arc::new(wg);
                        wireguard = Some(Arc::clone(&wg));
                        adapters.push(Box::new(ArcAdapter(wg)));
                    }
                    Err(e) => {
                        log::error!("adapter: failed to create WireGuard adapter: {:#}", e);
                    }
                },
                AdapterConfig::ReverseProxy { .. } => {
                    log::warn!(
                        "adapter: ReverseProxy adapter not yet implemented, ignoring config"
                    );
                }
                AdapterConfig::OsRouting { .. } => {
                    log::warn!(
                        "adapter: OsRouting adapter not yet implemented, ignoring config"
                    );
                }
            }
        }

        AdapterManager { adapters, wireguard }
    }

    /// Create virtual ports for a namespace from all configured adapters.
    pub fn create_namespace_ports(
        &self,
        namespace_id: &str,
    ) -> Vec<(ChannelPort, AdapterPortHandle)> {
        let mut ports = Vec::new();
        for adapter in &self.adapters {
            match adapter.create_port(namespace_id) {
                Ok(pair) => ports.push(pair),
                Err(e) => {
                    log::warn!(
                        "adapter: {} failed to create port for namespace '{}': {:#}",
                        adapter.adapter_type(),
                        namespace_id,
                        e,
                    );
                }
            }
        }
        ports
    }

    /// Get the WireGuard adapter, if one is configured.
    pub fn wireguard(&self) -> Option<&WireGuardAdapter> {
        self.wireguard.as_deref()
    }

    /// List the types of all configured adapters.
    #[allow(dead_code)]
    pub fn available_types(&self) -> Vec<String> {
        self.adapters
            .iter()
            .map(|a| a.adapter_type().to_string())
            .collect()
    }
}

/// Wrapper to make `Arc<WireGuardAdapter>` implement `IngressAdapter`.
struct ArcAdapter(Arc<WireGuardAdapter>);

impl IngressAdapter for ArcAdapter {
    fn adapter_type(&self) -> &str {
        self.0.adapter_type()
    }

    fn create_port(
        &self,
        namespace_id: &str,
    ) -> anyhow::Result<(ChannelPort, AdapterPortHandle)> {
        self.0.create_port(namespace_id)
    }
}
