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
    #[serde(alias = "memory_mb")]
    pub memory_mib: u64,
    pub vcpus: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ResourceRequirements {
    pub requests: Option<ResourceValues>,
    pub limits: Option<ResourceValues>,
}

/// Whether a workload runs as a long-lived service or a run-to-completion job.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum RunPolicy {
    /// Service: restart on completion, always try to maintain a running pod.
    #[default]
    Service,
    /// Job: run once to completion (exit 0 = done, non-zero = retry with backoff).
    Job,
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
    pub activation: Option<ActivationSpec>,
    /// Whether this workload runs as a service or a job.
    #[serde(default)]
    pub run_policy: RunPolicy,
    /// If true, the workload respects demand signals and starts dormant.
    /// If false, the workload is always-on regardless of demand.
    #[serde(default)]
    pub respects_demand: bool,
    /// Pod-scoped volumes. Mounted into containers via volume_mounts.
    #[serde(default)]
    pub volumes: Vec<VolumeSpec>,
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

/// Partial update to a namespace spec: upsert and/or remove individual resources.
#[derive(Debug, Clone)]
pub struct NamespacePatch {
    /// Workloads to create or replace.
    pub workloads: BTreeMap<WorkloadName, WorkloadSpec>,
    /// Services to create or replace.
    pub services: BTreeMap<String, ServiceSpec>,
    /// Workloads to remove by name.
    pub remove_workloads: Vec<WorkloadName>,
    /// Services to remove by name.
    pub remove_services: Vec<String>,
}
