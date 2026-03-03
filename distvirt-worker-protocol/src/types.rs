use std::fmt;
use std::net::Ipv4Addr;

use serde::{Deserialize, Serialize};

// --- ID Newtypes ---

macro_rules! define_id_newtype {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub String);

        impl From<String> for $name {
            fn from(s: String) -> Self { $name(s) }
        }
        impl From<&str> for $name {
            fn from(s: &str) -> Self { $name(s.to_string()) }
        }
        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str { &self.0 }
        }
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
        impl PartialEq<str> for $name {
            fn eq(&self, other: &str) -> bool { self.0 == other }
        }
        impl PartialEq<&str> for $name {
            fn eq(&self, other: &&str) -> bool { self.0 == *other }
        }
    };
}

define_id_newtype!(NamespaceId);
define_id_newtype!(WorkerId);
define_id_newtype!(PodId);
define_id_newtype!(ServiceId);
define_id_newtype!(SnapshotId);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NetworkConfig {
    pub subnet: Ipv4Addr,
    pub gateway: Ipv4Addr,
    pub prefix_len: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PodNetworkConfig {
    pub ip: Ipv4Addr,
    pub mac: [u8; 6],
    pub gateway: Ipv4Addr,
    pub netmask: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ContainerConfig {
    pub entrypoint: String,
    pub args: Vec<String>,
    pub env: Vec<String>,
    pub working_dir: Option<String>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub hostname: Option<String>,
    pub capture_output: bool,
    pub stdin: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ContainerSpec {
    pub container_id: String,
    pub image_ref: String,
    pub config: ContainerConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RegistryEntry {
    pub name: String,
    pub ip: Ipv4Addr,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BufferPolicy {
    pub buffer_frames: u32,
    pub timeout_ms: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ActivatorConfig {
    Tcp {
        ports: Option<Vec<u16>>,
        tcp_only: bool,
        max_flows: u32,
    },
    Http2 {},
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum BackendNeed {
    None,
    Traffic,
    Active,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ServicePolicy {
    pub buffer_frames: u32,
    pub timeout_ms: u32,
    pub activator: Option<ActivatorConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ServiceBackend {
    pub pod_ip: Ipv4Addr,
    pub pod_mac: [u8; 6],
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RouteDestination {
    RemoteWorker { worker_id: WorkerId },
    Placeholder { buffer_policy: BufferPolicy },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FabricRouteEntry {
    pub ip: Ipv4Addr,
    pub mac: [u8; 6],
    pub destination: RouteDestination,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutputStream {
    Stdout,
    Stderr,
}

// --- Handshake Types ---

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerHello {
    pub auth_token: String,
    pub capabilities: WorkerCapabilities,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerCapabilities {
    pub has_kvm: bool,
    pub has_containerd: bool,
    pub available_adapters: Vec<String>,
    pub max_pods: u32,
    pub available_memory_mb: u64,
    pub public_endpoint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerAccepted {
    pub worker_id: WorkerId,
    pub adapters: Vec<AdapterConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdapterConfig {
    WireGuard {
        listen_port: u16,
        private_key: Vec<u8>,
    },
    ReverseProxy {
        listen_port: u16,
        tls_cert: Vec<u8>,
        tls_key: Vec<u8>,
    },
    OsRouting {
        interface: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerReady {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WorkerCommand {
    CreateNamespace {
        namespace_id: NamespaceId,
        network: NetworkConfig,
    },
    DestroyNamespace {
        namespace_id: NamespaceId,
    },
    RegistrySync {
        namespace_id: NamespaceId,
        entries: Vec<RegistryEntry>,
    },
    RegistryUpdate {
        namespace_id: NamespaceId,
        added: Vec<RegistryEntry>,
        removed: Vec<String>,
    },
    LaunchPod {
        namespace_id: NamespaceId,
        pod_id: PodId,
        network: PodNetworkConfig,
        containers: Vec<ContainerSpec>,
    },
    StopPod {
        namespace_id: NamespaceId,
        pod_id: PodId,
        graceful: bool,
    },
    FabricRouteSync {
        namespace_id: NamespaceId,
        routes: Vec<FabricRouteEntry>,
    },
    FabricRouteUpdate {
        namespace_id: NamespaceId,
        added: Vec<FabricRouteEntry>,
        removed_ips: Vec<Ipv4Addr>,
    },
    CreateService {
        namespace_id: NamespaceId,
        service_id: ServiceId,
        ip: Ipv4Addr,
        mac: [u8; 6],
        policy: ServicePolicy,
    },
    UpdateServiceBackend {
        namespace_id: NamespaceId,
        service_id: ServiceId,
        backend: Option<ServiceBackend>,
    },
    ServiceReady {
        namespace_id: NamespaceId,
        service_id: ServiceId,
    },
    DestroyService {
        namespace_id: NamespaceId,
        service_id: ServiceId,
    },
    AddWireGuardPeer {
        namespace_id: NamespaceId,
        peer_public_key: [u8; 32],
        peer_ip: Ipv4Addr,
        preshared_key: Option<[u8; 32]>,
    },
    RemoveWireGuardPeer {
        peer_public_key: [u8; 32],
    },
    SuspendPod {
        namespace_id: NamespaceId,
        pod_id: PodId,
        snapshot_id: SnapshotId,
    },
    ResumePod {
        namespace_id: NamespaceId,
        pod_id: PodId,
        snapshot_id: SnapshotId,
        network: PodNetworkConfig,
    },
    DeleteSnapshot {
        snapshot_id: SnapshotId,
    },
    Shutdown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkerEvent {
    NamespaceCreated {
        namespace_id: NamespaceId,
    },
    PodRunning {
        namespace_id: NamespaceId,
        pod_id: PodId,
    },
    PodExited {
        namespace_id: NamespaceId,
        pod_id: PodId,
        exit_code: i32,
    },
    PodFailed {
        namespace_id: NamespaceId,
        pod_id: PodId,
        error: String,
    },
    NamespaceFailed {
        namespace_id: NamespaceId,
        error: String,
    },
    ShuttingDown,
    PodLogStreamError {
        namespace_id: NamespaceId,
        pod_id: PodId,
        container_id: String,
        phase: String,
        error: String,
    },
    FabricRouteMiss {
        namespace_id: NamespaceId,
        dst_ip: Ipv4Addr,
        dst_mac: [u8; 6],
    },
    ServiceActivation {
        namespace_id: NamespaceId,
        service_id: ServiceId,
        dst_ip: Ipv4Addr,
    },
    ServiceBackendNeed {
        namespace_id: NamespaceId,
        service_id: ServiceId,
        need: BackendNeed,
    },
    PodSuspended {
        namespace_id: NamespaceId,
        pod_id: PodId,
        snapshot_id: SnapshotId,
        snapshot_size_bytes: u64,
    },
    PodSuspendFailed {
        namespace_id: NamespaceId,
        pod_id: PodId,
        error: String,
    },
    NamespaceDestroyed {
        namespace_id: NamespaceId,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogStreamHeader {
    pub namespace_id: NamespaceId,
    pub pod_id: PodId,
    pub container_id: String,
}
