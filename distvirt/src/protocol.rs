use std::net::Ipv4Addr;

use crate::orchestrate::ContainerConfig;

/// Network configuration for a namespace.
pub struct NetworkConfig {
    pub subnet: Ipv4Addr,
    pub gateway: Ipv4Addr,
    pub prefix_len: u8,
}

/// Network configuration for a single pod within a namespace.
pub struct PodNetworkConfig {
    pub ip: Ipv4Addr,
    pub mac: [u8; 6],
    pub gateway: Ipv4Addr,
    pub netmask: String,
}

/// Specification for a container within a pod.
pub struct ContainerSpec {
    pub container_id: String,
    pub image_ref: String,
    pub config: ContainerConfig,
}

/// A service registry entry (name -> IP).
pub struct RegistryEntry {
    pub name: String,
    pub ip: Ipv4Addr,
}

/// Output stream identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputStream {
    Stdout,
    Stderr,
}

/// Commands sent from the orchestrator to the worker.
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
}

/// Events emitted by the worker back to the orchestrator.
#[derive(Debug)]
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
    PodOutput {
        namespace_id: String,
        pod_id: String,
        container_id: String,
        stream: OutputStream,
        data: Vec<u8>,
    },
    PodLogStreamError {
        namespace_id: String,
        pod_id: String,
        container_id: String,
        phase: String,
        error: String,
    },
}
