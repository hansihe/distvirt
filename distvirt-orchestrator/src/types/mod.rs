//! Orchestrator state-machine types.
//!
//! These types are the SM's canonical internal representation. They are intentionally
//! separate from both the worker protocol types and the client protocol types:
//!
//! - **SM types** (this module): owned by the state machine, may contain only a subset
//!   of the data present in outer types. The SM never sees wire formats directly.
//! - **Worker protocol types**: defined in `distvirt-worker-protocol`, re-exported here
//!   for convenience where they happen to match. The shell layer maps between SM outputs
//!   and wire commands.
//! - **Client protocol types**: defined in `distvirt-client-protocol`. The gRPC shell
//!   layer maps between SM events and client-facing protobuf messages.
//!
//! This decoupling is a feature — each layer can evolve independently, and the SM
//! remains testable without any wire format dependencies.

mod client;
mod namespace_io;
mod orchestrator_io;
mod specs;
mod states;

// --- Re-exports from protocol ---

pub use distvirt_worker_protocol::{
    ActivatorConfig, ArtifactId, ConfigDataFile, ContainerConfig, ContainerSpec, EndpointKind,
    EndpointPlacement, EndpointPodBackend, EndpointSpec, NamespaceId, NetworkConfig, PodId,
    PodNetworkConfig, PoolId, PoolInfo, PsiMetrics, RegistryEntry, ServiceBackend,
    ServicePolicy, VolumeSpec, VolumeType, VolumeMountSpec, WorkerCommand, WorkerId,
};

// Re-export SM's BackendNeed (no longer in the worker protocol).
pub use crate::sm::BackendNeed;

// Re-export all submodule types.
pub use client::*;
pub use namespace_io::*;
pub use orchestrator_io::*;
pub use specs::*;
pub use states::*;

use serde::{Deserialize, Serialize};

// --- Orchestrator-only ID Newtypes ---

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct WorkloadName(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ClientId(pub u64);

// --- Timer Keys ---

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TimerKey {
    IdleTimeout {
        service_id: distvirt_worker_protocol::ServiceId,
    },
    LaunchTimeout {
        workload_id: WorkloadName,
        pod_id: PodId,
    },
    SuspendTimeout {
        workload_id: WorkloadName,
        pod_id: PodId,
    },
    ResumeTimeout {
        workload_id: WorkloadName,
        pod_id: PodId,
    },
    RetryBackoffTimeout {
        workload_id: WorkloadName,
    },
}

// --- Pod Request ---

#[derive(Debug, Clone, PartialEq)]
pub struct PodRequest {
    pub workload_id: WorkloadName,
}
