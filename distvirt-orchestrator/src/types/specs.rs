use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceSpec {
    pub network: NetworkConfig,
    pub workloads: BTreeMap<WorkloadName, WorkloadSpec>,
    pub services: BTreeMap<String, ServiceSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ResourceValues {
    pub memory_mb: u64,
    pub vcpus: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ResourceRequirements {
    pub requests: Option<ResourceValues>,
    pub limits: Option<ResourceValues>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkloadSpec {
    pub containers: Vec<ContainerSpec>,
    pub network: PodNetworkConfig,
    /// If true, suspend the pod instead of stopping it when demand drops to zero.
    /// Enables fast resume from snapshot on re-activation.
    #[serde(default)]
    pub suspend_on_idle: bool,
    /// Resource requests and limits for this workload.
    #[serde(default)]
    pub resources: Option<ResourceRequirements>,
    /// Workload-level activation. If Some, workload is activation-based (starts dormant).
    /// If None, workload is always-on (starts immediately).
    #[serde(default)]
    pub activation: Option<WorkloadActivationSpec>,
}

/// Workload-level activation configuration. Only passthrough is valid.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkloadActivationSpec {
    pub idle_timeout: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ServiceSpec {
    pub workload_id: WorkloadName,
    pub ip: Ipv4Addr,
    pub policy: ServicePolicy,
    pub activation: Option<ActivationSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ActivationSpec {
    pub idle_timeout: Duration,
}
