use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::*;

// =============================================================================
// IP Allocation Types
// =============================================================================

/// Typed key for IP allocations — distinguishes workloads from services.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum IpResourceKey {
    Workload(WorkloadName),
    Service(String),
}

/// Whether an IP was auto-assigned by the orchestrator or manually specified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpAllocKind {
    Auto,
    Manual,
}

/// A single IP allocation with its type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IpAllocation {
    pub ip: Ipv4Addr,
    pub kind: IpAllocKind,
}

/// Full snapshot of all IP allocations in a namespace.
#[derive(Debug, Clone, Default)]
pub struct IpAllocResult {
    pub workload_ips: BTreeMap<WorkloadName, IpAllocation>,
    pub service_ips: BTreeMap<String, IpAllocation>,
}

// =============================================================================
// Input Types (from client, before IP allocation)
// =============================================================================

/// Workload spec as received from the client, before IP allocation.
#[derive(Debug, Clone)]
pub struct WorkloadSpecInput {
    pub explicit_ip: Option<Ipv4Addr>,
    pub containers: Vec<ContainerSpec>,
    pub suspend_on_idle: bool,
    pub resources: Option<ResourceRequirements>,
    pub activation: Option<ActivationSpec>,
    pub run_policy: RunPolicy,
    pub respects_demand: bool,
    pub volumes: Vec<VolumeSpec>,
    pub labels: BTreeMap<String, String>,
}

/// Service spec as received from the client, before IP allocation.
#[derive(Debug, Clone)]
pub struct ServiceSpecInput {
    pub workload_id: WorkloadName,
    pub explicit_ip: Option<Ipv4Addr>,
    pub ports: Vec<PortConfig>,
    pub has_activation: bool,
    pub idle_timeout: Duration,
    pub buffer_frames: u32,
    pub buffer_timeout_ms: u32,
    pub labels: BTreeMap<String, String>,
}

/// Full namespace spec as received from the client, before IP allocation.
#[derive(Debug, Clone)]
pub struct NamespaceSpecInput {
    pub network: NetworkConfig,
    pub workloads: BTreeMap<WorkloadName, WorkloadSpecInput>,
    pub services: BTreeMap<String, ServiceSpecInput>,
}

/// Partial update as received from the client, before IP allocation.
#[derive(Debug, Clone)]
pub struct NamespacePatchInput {
    pub workloads: BTreeMap<WorkloadName, WorkloadSpecInput>,
    pub services: BTreeMap<String, ServiceSpecInput>,
    pub remove_workloads: Vec<WorkloadName>,
    pub remove_services: Vec<String>,
}

impl NamespaceSpecInput {
    /// Convert a resolved NamespaceSpec into an input spec, dropping IPs so the
    /// orchestrator auto-assigns them. Useful for tests that build specs directly.
    pub fn from_resolved(spec: &NamespaceSpec) -> Self {
        NamespaceSpecInput {
            network: spec.network.clone(),
            workloads: spec.workloads.iter().map(|(name, wl)| {
                (name.clone(), WorkloadSpecInput {
                    explicit_ip: None,
                    containers: wl.containers.clone(),
                    suspend_on_idle: wl.suspend_on_idle,
                    resources: wl.resources.clone(),
                    activation: wl.activation.clone(),
                    run_policy: wl.run_policy.clone(),
                    respects_demand: wl.respects_demand,
                    volumes: wl.volumes.clone(),
                    labels: wl.labels.clone(),
                })
            }).collect(),
            services: spec.services.iter().map(|(name, svc)| {
                (name.clone(), ServiceSpecInput {
                    workload_id: svc.workload_id.clone(),
                    explicit_ip: None,
                    ports: svc.ports.clone(),
                    has_activation: svc.has_activation,
                    idle_timeout: svc.idle_timeout,
                    buffer_frames: svc.buffer_frames,
                    buffer_timeout_ms: svc.buffer_timeout_ms,
                    labels: svc.labels.clone(),
                })
            }).collect(),
        }
    }
}

impl NamespacePatchInput {
    /// Convert a resolved NamespacePatch into a patch input, dropping IPs. Useful for tests.
    pub fn from_resolved(patch: &NamespacePatch) -> Self {
        NamespacePatchInput {
            workloads: patch.workloads.iter().map(|(name, wl)| {
                (name.clone(), WorkloadSpecInput {
                    explicit_ip: None,
                    containers: wl.containers.clone(),
                    suspend_on_idle: wl.suspend_on_idle,
                    resources: wl.resources.clone(),
                    activation: wl.activation.clone(),
                    run_policy: wl.run_policy.clone(),
                    respects_demand: wl.respects_demand,
                    volumes: wl.volumes.clone(),
                    labels: wl.labels.clone(),
                })
            }).collect(),
            services: patch.services.iter().map(|(name, svc)| {
                (name.clone(), ServiceSpecInput {
                    workload_id: svc.workload_id.clone(),
                    explicit_ip: None,
                    ports: svc.ports.clone(),
                    has_activation: svc.has_activation,
                    idle_timeout: svc.idle_timeout,
                    buffer_frames: svc.buffer_frames,
                    buffer_timeout_ms: svc.buffer_timeout_ms,
                    labels: svc.labels.clone(),
                })
            }).collect(),
            remove_workloads: patch.remove_workloads.clone(),
            remove_services: patch.remove_services.clone(),
        }
    }
}

// =============================================================================
// Resolved Types (after IP allocation)
// =============================================================================

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
    /// User-defined labels for filtering and metadata.
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ServiceSpec {
    pub workload_id: WorkloadName,
    pub ip: Ipv4Addr,
    pub ports: Vec<PortConfig>,
    pub has_activation: bool,
    pub idle_timeout: Duration,
    pub buffer_frames: u32,
    pub buffer_timeout_ms: u32,
    /// User-defined labels for filtering and metadata.
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PortConfig {
    pub port: u16,
    pub target_port: u16,
    pub activator: Option<ActivatorKind>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ActivatorKind {
    Tcp { max_flows: u32 },
    Http2,
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
