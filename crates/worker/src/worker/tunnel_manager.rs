//! Control-plane glue between `WorkerRegistrySync` commands and the
//! `TunnelTransport` data plane.
//!
//! `TunnelManager` owns a single `TunnelTransport` (one UDP socket per worker)
//! and creates/removes tunnel ports as peers and namespaces come and go.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use crate::fabric::{Fabric, FabricPort, TunnelPortHandle, TunnelTransport};
use crate::task_handle::TaskHandle;
use distvirt_worker_protocol::{NamespaceId, WorkerId, WorkerPeerInfo};

/// Info about a namespace registered with the tunnel manager.
struct NamespaceInfo {
    _ns_id: NamespaceId,
    fabric: Arc<Fabric<FabricPort>>,
}

/// Per-peer state: which segments it shares, and active tunnel ports.
struct PeerState {
    endpoint: SocketAddr,
    segments: Vec<u16>,
    public_key: [u8; 32],
    /// segment_id → (RAII port handle, fabric read-loop task)
    namespace_ports: HashMap<u16, (TunnelPortHandle, TaskHandle<()>)>,
}

/// Manages tunnel ports across all namespaces and peers for a single worker.
pub(crate) struct TunnelManager {
    transport: TunnelTransport,
    /// worker_id → peer state
    peers: HashMap<WorkerId, PeerState>,
    /// segment_id → namespace info
    namespaces: HashMap<u16, NamespaceInfo>,
}

impl TunnelManager {
    /// Bind a UDP socket and create a new tunnel manager.
    ///
    /// When `encrypted` is true, a Noise static keypair is generated and
    /// all peer tunnels use Noise_IK encryption.
    pub(crate) async fn new(listen_addr: SocketAddr, encrypted: bool) -> std::io::Result<Self> {
        let transport = TunnelTransport::new(listen_addr, encrypted).await?;
        Ok(TunnelManager {
            transport,
            peers: HashMap::new(),
            namespaces: HashMap::new(),
        })
    }

    /// The local port the UDP socket is bound to.
    pub(crate) fn listen_port(&self) -> Option<u16> {
        self.transport.local_addr().ok().map(|a| a.port())
    }

    /// The transport's Noise static public key (32 bytes), or `None` if
    /// encryption is disabled.
    pub(crate) fn public_key(&self) -> Option<[u8; 32]> {
        self.transport
            .public_key()
            .map(|k| k.try_into().expect("public key should be 32 bytes"))
    }

    /// Handle a `WorkerRegistrySync` command: diff peers and reconcile tunnel ports.
    pub(crate) fn handle_registry_sync(&mut self, workers: Vec<WorkerPeerInfo>) {
        // Build set of incoming worker IDs.
        let incoming: HashMap<WorkerId, &WorkerPeerInfo> =
            workers.iter().map(|w| (w.worker_id, w)).collect();

        // Remove peers that are no longer in the registry.
        let stale: Vec<WorkerId> = self
            .peers
            .keys()
            .filter(|id| !incoming.contains_key(id))
            .cloned()
            .collect();
        for id in stale {
            self.remove_peer(id);
        }

        // Add/update peers.
        for info in &workers {
            let worker_id = info.worker_id;
            let endpoint: SocketAddr = match info.endpoint.parse() {
                Ok(addr) => addr,
                Err(e) => {
                    log::warn!(
                        "tunnel_manager: invalid endpoint '{}' for {}: {}",
                        info.endpoint,
                        worker_id,
                        e,
                    );
                    continue;
                }
            };

            let segments_changed = self.peers.get(&worker_id).map_or(true, |p| {
                p.segments != info.segments
                    || p.endpoint != endpoint
                    || p.public_key != info.public_key
            });

            if segments_changed {
                // Remove old state if exists, then re-add.
                self.remove_peer(worker_id);

                // Determine initiator by comparing our public key with
                // the remote peer's. The lexicographically lesser key initiates.
                let is_initiator = self
                    .transport
                    .public_key()
                    .map_or(false, |our_key| our_key < &info.public_key[..]);

                if let Err(e) = self.transport.add_peer(
                    worker_id,
                    endpoint,
                    Some(&info.public_key),
                    is_initiator,
                ) {
                    log::error!("tunnel: failed to add peer {}: {}", worker_id, e);
                    continue;
                }

                let mut peer = PeerState {
                    endpoint,
                    segments: info.segments.clone(),
                    public_key: info.public_key,
                    namespace_ports: HashMap::new(),
                };

                // Create ports for any overlapping namespaces.
                for &seg in &peer.segments {
                    if let Some(ns_info) = self.namespaces.get(&seg) {
                        if let Some((handle, task)) =
                            self.create_port(worker_id, seg, &ns_info.fabric)
                        {
                            peer.namespace_ports.insert(seg, (handle, task));
                        }
                    }
                }

                self.peers.insert(worker_id, peer);
            }
        }

        log::info!(
            "tunnel_manager: registry sync complete, {} peers active",
            self.peers.len()
        );
    }

    /// Called when a namespace with a segment_id is created.
    pub(crate) fn on_namespace_created(
        &mut self,
        ns_id: &NamespaceId,
        segment_id: u16,
        fabric: &Arc<Fabric<FabricPort>>,
    ) {
        self.namespaces.insert(
            segment_id,
            NamespaceInfo {
                _ns_id: ns_id.clone(),
                fabric: Arc::clone(fabric),
            },
        );

        // Create tunnel ports on all peers that share this segment.
        for (worker_id, peer) in &mut self.peers {
            if peer.segments.contains(&segment_id)
                && !peer.namespace_ports.contains_key(&segment_id)
            {
                if let Some((handle, task)) =
                    Self::create_port_static(&self.transport, *worker_id, segment_id, fabric)
                {
                    peer.namespace_ports.insert(segment_id, (handle, task));
                }
            }
        }

        log::info!(
            "tunnel_manager: namespace '{}' registered with segment {}",
            ns_id,
            segment_id
        );
    }

    /// Called when a namespace is destroyed — drops all tunnel ports for its segment.
    pub(crate) fn on_namespace_destroyed(&mut self, segment_id: u16) {
        self.namespaces.remove(&segment_id);

        for (_worker_id, peer) in &mut self.peers {
            // Dropping the TunnelPortHandle + TaskHandle cleans up automatically.
            peer.namespace_ports.remove(&segment_id);
        }

        log::info!(
            "tunnel_manager: segment {} removed, tunnel ports dropped",
            segment_id
        );
    }

    /// The underlying tunnel transport (for testing).
    #[cfg(test)]
    pub(crate) fn transport(&self) -> &TunnelTransport {
        &self.transport
    }

    /// Remove a peer and all its tunnel ports.
    fn remove_peer(&mut self, worker_id: WorkerId) {
        if self.peers.remove(&worker_id).is_some() {
            self.transport.remove_peer(worker_id);
            log::info!("tunnel_manager: removed peer {}", worker_id);
        }
    }

    /// Create a tunnel port and register it with the fabric.
    fn create_port(
        &self,
        worker_id: WorkerId,
        segment_id: u16,
        fabric: &Arc<Fabric<FabricPort>>,
    ) -> Option<(TunnelPortHandle, TaskHandle<()>)> {
        Self::create_port_static(&self.transport, worker_id, segment_id, fabric)
    }

    fn create_port_static(
        transport: &TunnelTransport,
        worker_id: WorkerId,
        segment_id: u16,
        fabric: &Arc<Fabric<FabricPort>>,
    ) -> Option<(TunnelPortHandle, TaskHandle<()>)> {
        match transport.create_namespace_port(worker_id, segment_id) {
            Ok((channel_port, handle)) => {
                let (_port_id, task) =
                    fabric.add_tunnel_port(worker_id, FabricPort::Virtual(channel_port));
                log::info!(
                    "tunnel_manager: created tunnel port for worker {} segment {}",
                    worker_id,
                    segment_id
                );
                Some((handle, task))
            }
            Err(e) => {
                log::error!(
                    "tunnel_manager: failed to create port for worker {} segment {}: {}",
                    worker_id,
                    segment_id,
                    e
                );
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    const WORKER_A: WorkerId = WorkerId(1);
    const WORKER_B: WorkerId = WorkerId(2);
    const WORKER_BAD: WorkerId = WorkerId(3);

    #[tokio::test]
    async fn listen_port_after_new() {
        let mgr = TunnelManager::new("127.0.0.1:0".parse().unwrap(), false)
            .await
            .unwrap();
        let port = mgr.listen_port();
        assert!(port.is_some());
        assert!(port.unwrap() > 0);
    }

    #[tokio::test]
    async fn public_key_unencrypted() {
        let mgr = TunnelManager::new("127.0.0.1:0".parse().unwrap(), false)
            .await
            .unwrap();
        assert!(mgr.public_key().is_none());
    }

    #[tokio::test]
    async fn public_key_encrypted() {
        let mgr = TunnelManager::new("127.0.0.1:0".parse().unwrap(), true)
            .await
            .unwrap();
        let key = mgr.public_key();
        assert!(key.is_some());
        assert_eq!(key.unwrap().len(), 32);
    }

    #[tokio::test]
    async fn registry_sync_empty() {
        let mut mgr = TunnelManager::new("127.0.0.1:0".parse().unwrap(), false)
            .await
            .unwrap();
        mgr.handle_registry_sync(vec![]);
        assert_eq!(mgr.peers.len(), 0);
    }

    #[tokio::test]
    async fn registry_sync_idempotent() {
        let mut mgr = TunnelManager::new("127.0.0.1:0".parse().unwrap(), false)
            .await
            .unwrap();
        let peers = vec![WorkerPeerInfo {
            worker_id: WORKER_A,
            endpoint: "127.0.0.1:9999".into(),
            segments: vec![1],
            public_key: [0u8; 32],
        }];
        mgr.handle_registry_sync(peers.clone());
        assert_eq!(mgr.peers.len(), 1);
        mgr.handle_registry_sync(peers);
        assert_eq!(mgr.peers.len(), 1);
    }

    #[tokio::test]
    async fn registry_sync_removes_stale() {
        let mut mgr = TunnelManager::new("127.0.0.1:0".parse().unwrap(), false)
            .await
            .unwrap();
        let peers_ab = vec![
            WorkerPeerInfo {
                worker_id: WORKER_A,
                endpoint: "127.0.0.1:9998".into(),
                segments: vec![1],
                public_key: [0u8; 32],
            },
            WorkerPeerInfo {
                worker_id: WORKER_B,
                endpoint: "127.0.0.1:9999".into(),
                segments: vec![1],
                public_key: [0u8; 32],
            },
        ];
        mgr.handle_registry_sync(peers_ab);
        assert_eq!(mgr.peers.len(), 2);

        // Sync with only A — B should be removed.
        let peers_a = vec![WorkerPeerInfo {
            worker_id: WORKER_A,
            endpoint: "127.0.0.1:9998".into(),
            segments: vec![1],
            public_key: [0u8; 32],
        }];
        mgr.handle_registry_sync(peers_a);
        assert_eq!(mgr.peers.len(), 1);
        assert!(mgr.peers.contains_key(&WORKER_A));
        assert!(!mgr.peers.contains_key(&WORKER_B));
    }

    #[tokio::test]
    async fn registry_sync_invalid_endpoint_skipped() {
        let mut mgr = TunnelManager::new("127.0.0.1:0".parse().unwrap(), false)
            .await
            .unwrap();
        let peers = vec![WorkerPeerInfo {
            worker_id: WORKER_BAD,
            endpoint: "not-a-valid-address".into(),
            segments: vec![1],
            public_key: [0u8; 32],
        }];
        mgr.handle_registry_sync(peers);
        // Invalid endpoint is skipped.
        assert_eq!(mgr.peers.len(), 0);
    }

    #[tokio::test]
    async fn namespace_then_peer_creates_port() {
        use crate::fabric::{Fabric, FabricPort};

        let mut mgr_a = TunnelManager::new("127.0.0.1:0".parse().unwrap(), false)
            .await
            .unwrap();
        let mut mgr_b = TunnelManager::new("127.0.0.1:0".parse().unwrap(), false)
            .await
            .unwrap();

        let addr_a = mgr_a.transport().local_addr().unwrap();
        let addr_b = mgr_b.transport().local_addr().unwrap();

        let segment_id = 42u16;
        let ns_id = NamespaceId::from("test-ns");

        // Create fabrics for each side.
        let fabric_a = Arc::new(Fabric::<FabricPort>::new(Ipv4Addr::new(10, 0, 0, 0), 24));
        let fabric_b = Arc::new(Fabric::<FabricPort>::new(Ipv4Addr::new(10, 0, 0, 0), 24));

        // Register namespace first on both sides.
        mgr_a.on_namespace_created(&ns_id, segment_id, &fabric_a);
        mgr_b.on_namespace_created(&ns_id, segment_id, &fabric_b);

        // Then sync peers — ports should be created.
        mgr_a.handle_registry_sync(vec![WorkerPeerInfo {
            worker_id: WORKER_B,
            endpoint: addr_b.to_string(),
            segments: vec![segment_id],
            public_key: [0u8; 32],
        }]);
        mgr_b.handle_registry_sync(vec![WorkerPeerInfo {
            worker_id: WORKER_A,
            endpoint: addr_a.to_string(),
            segments: vec![segment_id],
            public_key: [0u8; 32],
        }]);

        // Verify ports were created for the matching segment.
        assert!(
            mgr_a
                .peers
                .get(&WORKER_B)
                .unwrap()
                .namespace_ports
                .contains_key(&segment_id)
        );
        assert!(
            mgr_b
                .peers
                .get(&WORKER_A)
                .unwrap()
                .namespace_ports
                .contains_key(&segment_id)
        );
    }

    #[tokio::test]
    async fn peer_then_namespace_creates_port() {
        use crate::fabric::{Fabric, FabricPort};

        let mut mgr_a = TunnelManager::new("127.0.0.1:0".parse().unwrap(), false)
            .await
            .unwrap();
        let mut mgr_b = TunnelManager::new("127.0.0.1:0".parse().unwrap(), false)
            .await
            .unwrap();

        let addr_a = mgr_a.transport().local_addr().unwrap();
        let addr_b = mgr_b.transport().local_addr().unwrap();

        let segment_id = 42u16;
        let ns_id = NamespaceId::from("test-ns");

        let fabric_a = Arc::new(Fabric::<FabricPort>::new(Ipv4Addr::new(10, 0, 0, 0), 24));
        let fabric_b = Arc::new(Fabric::<FabricPort>::new(Ipv4Addr::new(10, 0, 0, 0), 24));

        // Sync peers first (before namespace exists).
        mgr_a.handle_registry_sync(vec![WorkerPeerInfo {
            worker_id: WORKER_B,
            endpoint: addr_b.to_string(),
            segments: vec![segment_id],
            public_key: [0u8; 32],
        }]);
        mgr_b.handle_registry_sync(vec![WorkerPeerInfo {
            worker_id: WORKER_A,
            endpoint: addr_a.to_string(),
            segments: vec![segment_id],
            public_key: [0u8; 32],
        }]);

        // No ports yet (namespace not registered).
        assert!(
            mgr_a
                .peers
                .get(&WORKER_B)
                .unwrap()
                .namespace_ports
                .is_empty()
        );

        // Register namespace — ports should be created.
        mgr_a.on_namespace_created(&ns_id, segment_id, &fabric_a);
        mgr_b.on_namespace_created(&ns_id, segment_id, &fabric_b);

        assert!(
            mgr_a
                .peers
                .get(&WORKER_B)
                .unwrap()
                .namespace_ports
                .contains_key(&segment_id)
        );
        assert!(
            mgr_b
                .peers
                .get(&WORKER_A)
                .unwrap()
                .namespace_ports
                .contains_key(&segment_id)
        );
    }

    #[tokio::test]
    async fn namespace_destroyed_removes_ports() {
        use crate::fabric::{Fabric, FabricPort};

        let mut mgr = TunnelManager::new("127.0.0.1:0".parse().unwrap(), false)
            .await
            .unwrap();

        let segment_id = 10u16;
        let ns_id = NamespaceId::from("test-ns");
        let fabric = Arc::new(Fabric::<FabricPort>::new(Ipv4Addr::new(10, 0, 0, 0), 24));

        mgr.on_namespace_created(&ns_id, segment_id, &fabric);
        mgr.handle_registry_sync(vec![WorkerPeerInfo {
            worker_id: WORKER_A,
            endpoint: "127.0.0.1:9999".into(),
            segments: vec![segment_id],
            public_key: [0u8; 32],
        }]);

        assert!(
            mgr.peers
                .get(&WORKER_A)
                .unwrap()
                .namespace_ports
                .contains_key(&segment_id)
        );

        mgr.on_namespace_destroyed(segment_id);

        assert!(
            !mgr.peers
                .get(&WORKER_A)
                .unwrap()
                .namespace_ports
                .contains_key(&segment_id)
        );
        assert!(!mgr.namespaces.contains_key(&segment_id));
    }
}
