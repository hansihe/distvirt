use std::net::Ipv4Addr;

use serde::{Deserialize, Serialize};

/// Network configuration for a namespace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    pub subnet: Ipv4Addr,
    pub gateway: Ipv4Addr,
    pub prefix_len: u8,
}

/// Network configuration for a single pod within a namespace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PodNetworkConfig {
    pub ip: Ipv4Addr,
    pub mac: [u8; 6],
    pub gateway: Ipv4Addr,
    pub netmask: String,
}

/// Container execution configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerConfig {
    pub entrypoint: String,
    pub args: Vec<String>,
    pub env: Vec<String>,
    pub working_dir: Option<String>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub hostname: Option<String>,
    pub capture_output: bool,
}

/// Specification for a container within a pod.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerSpec {
    pub container_id: String,
    pub image_ref: String,
    pub config: ContainerConfig,
}

/// A service registry entry (name -> IP).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryEntry {
    pub name: String,
    pub ip: Ipv4Addr,
}

/// Buffer policy for placeholder routes (scale-to-zero / suspended pods).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BufferPolicy {
    /// Hold TCP SYN connections at the gateway level (no-op for now, stored for future use).
    pub hold_tcp_syn: bool,
    /// Maximum number of frames to buffer (0 = drop immediately).
    pub buffer_frames: u32,
    /// How long to buffer before giving up, in milliseconds.
    pub timeout_ms: u32,
}

/// Where a fabric route entry points.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RouteDestination {
    /// The pod is live on another worker.
    RemoteWorker { worker_id: String },
    /// The pod is not currently running (suspended, scaled-to-zero).
    Placeholder { buffer_policy: BufferPolicy },
}

/// A single entry in the fabric routing table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FabricRouteEntry {
    pub ip: Ipv4Addr,
    pub mac: [u8; 6],
    pub destination: RouteDestination,
}

/// Output stream identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutputStream {
    Stdout,
    Stderr,
}

/// Commands sent from the orchestrator to the worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkerCommand {
    CreateNamespace {
        namespace_id: String,
        network: NetworkConfig,
    },
    DestroyNamespace {
        namespace_id: String,
    },
    RegistrySync {
        namespace_id: String,
        entries: Vec<RegistryEntry>,
    },
    LaunchPod {
        namespace_id: String,
        pod_id: String,
        network: PodNetworkConfig,
        containers: Vec<ContainerSpec>,
    },
    StopPod {
        namespace_id: String,
        pod_id: String,
        graceful: bool,
    },
    FabricRouteSync {
        namespace_id: String,
        routes: Vec<FabricRouteEntry>,
    },
    FabricRouteUpdate {
        namespace_id: String,
        added: Vec<FabricRouteEntry>,
        removed_ips: Vec<Ipv4Addr>,
    },
    Shutdown,
}

/// Events emitted by the worker back to the orchestrator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkerEvent {
    NamespaceCreated {
        namespace_id: String,
    },
    PodRunning {
        namespace_id: String,
        pod_id: String,
    },
    PodExited {
        namespace_id: String,
        pod_id: String,
        exit_code: i32,
    },
    PodFailed {
        namespace_id: String,
        pod_id: String,
        error: String,
    },
    NamespaceFailed {
        namespace_id: String,
        error: String,
    },
    ShuttingDown,
    PodLogStreamError {
        namespace_id: String,
        pod_id: String,
        container_id: String,
        phase: String,
        error: String,
    },
    FabricRouteMiss {
        namespace_id: String,
        dst_ip: Ipv4Addr,
        dst_mac: [u8; 6],
    },
}

/// Header sent at the start of each log yamux stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogStreamHeader {
    pub namespace_id: String,
    pub pod_id: String,
    pub container_id: String,
}
