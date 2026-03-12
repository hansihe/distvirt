use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use distvirt_worker_protocol::NamespaceId;
use tokio::sync::mpsc;

use crate::fabric::gateway::{GatewayProvider, ChannelEgress};

/// Channel-based gateway provider for tests (no TUN device, no root required).
///
/// Each call to `create_egress` registers the namespace's traffic injection
/// handle so tests can send packets into the fabric gateway.
#[derive(Clone)]
pub struct SimGatewayProvider {
    inner: Arc<Mutex<HashMap<NamespaceId, Vec<SimNamespaceHandle>>>>,
}

struct SimNamespaceHandle {
    internet_tx: mpsc::Sender<Vec<u8>>,
    _internet_rx: mpsc::Receiver<Vec<u8>>,
}

impl GatewayProvider for SimGatewayProvider {
    type Egress = ChannelEgress;

    fn create_egress(
        &self,
        namespace_id: &NamespaceId,
        _gateway_ip: [u8; 4],
        _prefix_len: u8,
    ) -> anyhow::Result<ChannelEgress> {
        let (egress, internet_rx, internet_tx) = ChannelEgress::new(256);
        self.inner
            .lock()
            .unwrap()
            .entry(namespace_id.clone())
            .or_default()
            .push(SimNamespaceHandle {
                internet_tx,
                _internet_rx: internet_rx,
            });
        Ok(egress)
    }
}

impl SimGatewayProvider {
    pub fn new() -> Self {
        SimGatewayProvider {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Return a sender for the given namespace (the first registered worker's handle).
    pub fn get(&self, ns_id: &NamespaceId) -> Option<mpsc::Sender<Vec<u8>>> {
        self.inner
            .lock()
            .unwrap()
            .get(ns_id)
            .and_then(|v| v.first())
            .map(|h| h.internet_tx.clone())
    }
}
