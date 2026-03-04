use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceSpec {
    pub network: NetworkConfig,
    pub workloads: HashMap<WorkloadId, WorkloadSpec>,
    pub services: HashMap<ServiceId, ServiceSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkloadSpec {
    pub containers: Vec<ContainerSpec>,
    pub network: PodNetworkConfig,
    /// If true, suspend the pod instead of stopping it when demand drops to zero.
    /// Enables fast resume from snapshot on re-activation.
    #[serde(default)]
    pub suspend_on_idle: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ServiceSpec {
    pub workload_id: WorkloadId,
    pub ip: Ipv4Addr,
    pub mac: [u8; 6],
    pub policy: ServicePolicy,
    pub activation: Option<ActivationSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ActivationSpec {
    pub idle_timeout: Duration,
}
