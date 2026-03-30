use std::fmt;
use std::net::Ipv4Addr;

use serde::{Deserialize, Serialize};

// --- String-based ID Newtypes ---

macro_rules! define_string_id_newtype {
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

// --- u64-based ID Newtypes ---

macro_rules! define_u64_id_newtype {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub u64);

        impl From<u64> for $name {
            fn from(v: u64) -> Self { $name(v) }
        }
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

/// Composite namespace identifier: a human-readable name plus a unique
/// incarnation ID assigned by the orchestrator. Two `NamespaceId` values
/// with the same `name` but different `id` values represent different
/// lifecycle incarnations of the same logical namespace.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NamespaceId {
    pub name: String,
    pub id: u64,
}

impl NamespaceId {
    pub fn new(name: impl Into<String>, id: u64) -> Self {
        NamespaceId {
            name: name.into(),
            id,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl fmt::Display for NamespaceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.name, self.id)
    }
}

define_string_id_newtype!(PoolId);
define_string_id_newtype!(ArtifactId);

define_u64_id_newtype!(WorkerId);
define_u64_id_newtype!(PodId);
define_u64_id_newtype!(ServiceId);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NetworkConfig {
    pub subnet: Ipv4Addr,
    pub gateway: Ipv4Addr,
    pub prefix_len: u8,
    pub segment_id: Option<u16>,
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
    pub command: Option<Vec<String>>,
    pub args: Option<Vec<String>>,
    pub env: Vec<String>,
    pub working_dir: Option<String>,
    pub user: Option<String>,
    pub hostname: Option<String>,
    pub capture_output: bool,
    pub stdin: bool,
    pub volume_mounts: Vec<VolumeMountSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VolumeMountSpec {
    pub name: String,
    pub mount_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum VolumeType {
    EmptyDir { size_mb: u64 },
    ConfigData { files: Vec<ConfigDataFile> },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VolumeSpec {
    pub name: String,
    pub volume_type: VolumeType,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConfigDataFile {
    pub path: String,
    pub content: String,
    pub mode: u32,
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
        max_flows: u32,
    },
    Http2,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ResourceValues {
    pub memory_mib: u64,
    pub vcpus: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ResourceRequirements {
    pub requests: Option<ResourceValues>,
    pub limits: Option<ResourceValues>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ServicePolicy {
    pub ports: Vec<PortConfig>,
    pub buffer_frames: u32,
    pub timeout_ms: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PortConfig {
    pub port: u16,
    pub target_port: u16,
    pub activator: Option<ActivatorConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ServiceBackend {
    pub pod_ip: Ipv4Addr,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EndpointPlacement {
    pub worker_id: WorkerId,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EndpointPodBackend {
    pub pod_ip: Ipv4Addr,
    pub placement: Option<EndpointPlacement>,
    pub ready: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum EndpointKind {
    Service {
        service_id: ServiceId,
        policy: ServicePolicy,
        backend: Option<EndpointPodBackend>,
    },
    Pod {
        placement: Option<EndpointPlacement>,
    },
    /// WireGuard peer endpoint. Placement indicates which worker hosts the
    /// WireGuard adapter for this peer.
    WireGuardPeer {
        placement: Option<EndpointPlacement>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EndpointSpec {
    pub ip: Ipv4Addr,
    pub kind: EndpointKind,
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
pub struct PoolInfo {
    pub pool_id: PoolId,
    pub path: String,
    pub capacity_bytes: u64,
    pub available_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerCapabilities {
    pub has_kvm: bool,
    pub has_containerd: bool,
    pub available_adapters: Vec<String>,
    pub max_pods: u32,
    pub available_memory_mb: u64,
    pub public_endpoint: String,
    pub pools: Vec<PoolInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerPeerInfo {
    pub worker_id: WorkerId,
    pub endpoint: String,
    pub public_key: [u8; 32],
    pub segments: Vec<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TunnelPeerStatus {
    Connected,
    Disconnected { error: String },
    HandshakeFailed { error: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerAccepted {
    pub worker_id: WorkerId,
    pub adapters: Vec<AdapterConfig>,
    pub tunnel_encrypted: bool,
    pub pools: Vec<PoolInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdapterConfig {
    WireGuard {
        listen_port: u16,
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
pub struct WorkerReady {
    pub tunnel_listen_port: Option<u16>,
    pub tunnel_public_key: Option<[u8; 32]>,
    pub transfer_listen_port: Option<u16>,
    pub wireguard_listen_port: Option<u16>,
    pub wireguard_public_key: Option<[u8; 32]>,
}

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
        resources: Option<ResourceRequirements>,
        volumes: Vec<VolumeSpec>,
    },
    StopPod {
        namespace_id: NamespaceId,
        pod_id: PodId,
        graceful: bool,
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
        artifact_id: ArtifactId,
        pool_id: PoolId,
    },
    ResumePod {
        namespace_id: NamespaceId,
        pod_id: PodId,
        artifact_id: ArtifactId,
        network: PodNetworkConfig,
        pool_id: PoolId,
    },
    DeleteArtifact {
        artifact_id: ArtifactId,
        pool_id: PoolId,
    },
    TransferArtifact {
        transfer_id: u64,
        source_artifact_id: ArtifactId,
        source_pool_id: PoolId,
        dest_artifact_id: ArtifactId,
        dest_pool_id: PoolId,
        dest_endpoint: Option<String>, // None = local copy
    },
    WorkerRegistrySync {
        workers: Vec<WorkerPeerInfo>,
    },
    EndpointSync {
        namespace_id: NamespaceId,
        endpoints: Vec<EndpointSpec>,
    },
    EndpointUpdate {
        namespace_id: NamespaceId,
        upserted: Vec<EndpointSpec>,
        removed_ips: Vec<Ipv4Addr>,
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
    PodSuspended {
        namespace_id: NamespaceId,
        pod_id: PodId,
        artifact_id: ArtifactId,
        artifact_size_bytes: u64,
        pool_id: PoolId,
    },
    PodSuspendFailed {
        namespace_id: NamespaceId,
        pod_id: PodId,
        error: String,
    },
    NamespaceDestroyed {
        namespace_id: NamespaceId,
    },
    TunnelStatus {
        peer_worker_id: WorkerId,
        status: TunnelPeerStatus,
    },
    WorkerCondition {
        key: String,
        active: bool,
        message: String,
    },
    PoolCapacityUpdate {
        pools: Vec<PoolInfo>,
    },
    ArtifactWriteStarted {
        namespace_id: NamespaceId,
        artifact_id: ArtifactId,
        pool_id: PoolId,
    },
    ArtifactWriteCommitted {
        namespace_id: NamespaceId,
        artifact_id: ArtifactId,
        pool_id: PoolId,
        size_bytes: u64,
    },
    ArtifactTransferReceived {
        transfer_id: u64,
        source_artifact_id: ArtifactId,
        source_pool_id: PoolId,
        dest_artifact_id: ArtifactId,
        dest_pool_id: PoolId,
        size_bytes: u64,
    },
    TransferFailed {
        transfer_id: u64,
        source_artifact_id: ArtifactId,
        source_pool_id: PoolId,
        dest_artifact_id: ArtifactId,
        dest_pool_id: PoolId,
        error: String,
    },
    PressureUpdate {
        cpu: PsiMetrics,
        memory: PsiMetrics,
        io: PsiMetrics,
    },
    EndpointDemandTraffic {
        namespace_id: NamespaceId,
        ip: Ipv4Addr,
        service_id: Option<ServiceId>,
    },
    EndpointDemandActive {
        namespace_id: NamespaceId,
        ip: Ipv4Addr,
        service_id: Option<ServiceId>,
        active: bool,
    },
    PodMemoryConstrained {
        namespace_id: NamespaceId,
        pod_id: PodId,
        reason: MemoryConstraintReason,
    },
    PodMemoryConstraintCleared {
        namespace_id: NamespaceId,
        pod_id: PodId,
    },
    PodOomKill {
        namespace_id: NamespaceId,
        pod_id: PodId,
        count: u64,
    },
}

/// Why the guest's memory control loop cannot resolve pressure.
/// Duplicated from guest-protocol per crate boundary convention.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemoryConstraintReason {
    BalloonExhausted,
    DeflationStalled,
}

/// PSI (Pressure Stall Information) metrics for a single resource dimension.
/// Values are percentages (0.0–100.0).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PsiMetrics {
    pub some_avg10: f64,
    pub some_avg60: f64,
    pub full_avg10: f64,
    pub full_avg60: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogStreamHeader {
    pub namespace_id: NamespaceId,
    pub pod_id: PodId,
    pub container_id: String,
}
