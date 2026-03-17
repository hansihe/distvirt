use std::collections::HashMap;
use std::net::Ipv4Addr;

/// Per-peer state tracked by the WireGuard peer manager.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WgPeerInfo {
    pub client_ip: Ipv4Addr,
}

/// Side-effects produced by the peer manager. The caller converts these into
/// `NamespaceOutput` entries (worker commands, client events, etc.).
#[derive(Debug, Clone, PartialEq)]
pub enum WgPeerOutput {
    /// A new peer was added — emit `AddWireGuardPeer` to a worker.
    AddPeer {
        peer_public_key: [u8; 32],
        peer_ip: Ipv4Addr,
    },
    /// A peer was removed — emit `RemoveWireGuardPeer` to workers.
    RemovePeer { peer_public_key: [u8; 32] },
}

/// Result of a connect attempt.
pub enum ConnectResult {
    /// The peer was connected (new or idempotent). Returns (client_ip, outputs).
    Ok {
        client_ip: Ipv4Addr,
        outputs: Vec<WgPeerOutput>,
    },
    /// The connect failed.
    Error { message: String },
}

/// Manages WireGuard peer IP allocation and peer tracking for a single namespace.
pub struct WireGuardPeerManager {
    pub peers: HashMap<[u8; 32], WgPeerInfo>,
    pub next_host_offset: u16,
    subnet: Ipv4Addr,
    prefix_len: u8,
}

impl WireGuardPeerManager {
    pub fn new(subnet: Ipv4Addr, prefix_len: u8) -> Self {
        let next_host_offset = ((1u32 << (32 - prefix_len as u32)) - 2) as u16;
        WireGuardPeerManager {
            peers: HashMap::new(),
            next_host_offset,
            subnet,
            prefix_len,
        }
    }

    /// Subnet in CIDR notation, e.g. "172.16.0.0/24".
    pub fn subnet_cidr(&self) -> String {
        format!("{}/{}", self.subnet, self.prefix_len)
    }

    /// Connect a client. Returns the allocated client IP and any outputs, or an error.
    /// Idempotent: if the public key is already connected, returns the existing IP
    /// with no outputs.
    pub fn connect(&mut self, client_public_key: [u8; 32]) -> ConnectResult {
        // Idempotent: already connected.
        if let Some(peer) = self.peers.get(&client_public_key) {
            return ConnectResult::Ok {
                client_ip: peer.client_ip,
                outputs: vec![],
            };
        }

        // Allocate IP from top of subnet downward.
        if self.next_host_offset < 2 {
            return ConnectResult::Error {
                message: "no more WireGuard peer IPs available".to_string(),
            };
        }

        let subnet_u32 = u32::from(self.subnet);
        let client_ip = Ipv4Addr::from(subnet_u32 + self.next_host_offset as u32);
        self.next_host_offset -= 1;

        self.peers
            .insert(client_public_key, WgPeerInfo { client_ip });

        ConnectResult::Ok {
            client_ip,
            outputs: vec![WgPeerOutput::AddPeer {
                peer_public_key: client_public_key,
                peer_ip: client_ip,
            }],
        }
    }

    /// Disconnect a client. Returns outputs if the peer existed.
    pub fn disconnect(&mut self, client_public_key: [u8; 32]) -> Vec<WgPeerOutput> {
        if self.peers.remove(&client_public_key).is_some() {
            vec![WgPeerOutput::RemovePeer {
                peer_public_key: client_public_key,
            }]
        } else {
            vec![]
        }
    }
}
