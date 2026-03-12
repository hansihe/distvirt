use std::collections::BTreeMap;
use std::net::Ipv4Addr;

use crate::namespace::NamespaceStateMachine;
use crate::types::*;

// --- Test Helpers ---

pub(super) fn test_network_config() -> NetworkConfig {
    NetworkConfig {
        subnet: Ipv4Addr::new(172, 16, 0, 0),
        gateway: Ipv4Addr::new(172, 16, 0, 1),
        prefix_len: 24,
        segment_id: None,
    }
}

pub(super) fn test_pod_network_config() -> PodNetworkConfig {
    PodNetworkConfig {
        ip: Ipv4Addr::new(172, 16, 0, 10),
        mac: [0; 6],
        gateway: Ipv4Addr::new(172, 16, 0, 1),
        netmask: "255.255.255.0".into(),
    }
}

pub(super) fn test_service_policy() -> ServicePolicy {
    ServicePolicy {
        buffer_frames: 100,
        timeout_ms: 5000,
        activator: None,
    }
}

pub(super) fn test_container_spec() -> ContainerSpec {
    ContainerSpec {
        container_id: "main".into(),
        image_ref: "test-image:latest".into(),
        config: ContainerConfig {
            entrypoint: vec!["/bin/sh".into()],
            args: vec![],
            env: vec![],
            working_dir: None,
            uid: None,
            gid: None,
            hostname: None,
            capture_output: false,
            stdin: false,
        },
    }
}

pub(super) fn test_spec() -> NamespaceSpec {
    let mut workloads = BTreeMap::new();
    workloads.insert(
        WorkloadId("svc1".into()),
        WorkloadSpec {
            containers: vec![test_container_spec()],
            network: test_pod_network_config(),
            suspend_on_idle: false,
            resources: None,
        },
    );
    let mut services = BTreeMap::new();
    services.insert(
        ServiceId("svc1".into()),
        ServiceSpec {
            workload_id: WorkloadId("svc1".into()),
            ip: Ipv4Addr::new(172, 16, 0, 100),
            policy: test_service_policy(),
            activation: None,
        },
    );
    NamespaceSpec {
        network: test_network_config(),
        workloads,
        services,
    }
}

pub(super) fn test_spec_with_activation() -> NamespaceSpec {
    let mut workloads = BTreeMap::new();
    workloads.insert(
        WorkloadId("svc1".into()),
        WorkloadSpec {
            containers: vec![test_container_spec()],
            network: test_pod_network_config(),
            suspend_on_idle: false,
            resources: None,
        },
    );
    let mut services = BTreeMap::new();
    services.insert(
        ServiceId("svc1".into()),
        ServiceSpec {
            workload_id: WorkloadId("svc1".into()),
            ip: Ipv4Addr::new(172, 16, 0, 100),
            policy: test_service_policy(),
            activation: Some(ActivationSpec {
                idle_timeout: std::time::Duration::from_secs(30),
            }),
        },
    );
    NamespaceSpec {
        network: test_network_config(),
        workloads,
        services,
    }
}

pub(super) fn worker_id(n: u32) -> WorkerId {
    WorkerId(format!("worker-{}", n))
}

pub(super) fn client_id(n: u64) -> ClientId {
    ClientId(n)
}

pub(super) fn ns_id(name: &str) -> NamespaceId {
    NamespaceId(name.into())
}

pub(super) fn worker_caps() -> WorkerCapabilities {
    WorkerCapabilities {
        max_pods: 10,
        available_memory_mb: 1024,
        public_endpoint: String::new(),
        pools: vec![],
    }
}

pub(super) fn worker_caps_with_endpoint(endpoint: &str) -> WorkerCapabilities {
    WorkerCapabilities {
        max_pods: 10,
        available_memory_mb: 1024,
        public_endpoint: endpoint.to_string(),
        pools: vec![],
    }
}

pub(super) fn test_wg_config() -> WorkerWgConfig {
    WorkerWgConfig {
        listen_port: 51820,
        public_key: [0xab; 32],
    }
}

pub(super) fn svc_id() -> ServiceId {
    ServiceId("svc1".into())
}

pub(super) fn wl_id() -> WorkloadId {
    WorkloadId("svc1".into())
}

/// Create a namespace SM with Active status and one Active worker, ready for testing.
pub(super) fn active_namespace(spec: NamespaceSpec) -> NamespaceStateMachine {
    let mut ns = NamespaceStateMachine::new(ns_id("test"), spec, 1);
    ns.workers.insert(
        worker_id(1),
        NamespaceWorkerState {
            fabric_status: FabricStatus::Active,
            primary_pool_id: None,
            pressure_band: PressureBand::Normal,
        },
    );
    ns.status = NamespaceStatus::Active;
    ns
}

/// Simulate outer-layer scheduling: step the namespace, then for any pod_requests
/// emitted, pick the first active worker and inject LaunchPod.
/// Returns combined output.
pub(super) fn step_with_scheduling(
    ns: &mut NamespaceStateMachine,
    input: NamespaceInput,
    pod_counter: &mut u64,
) -> NamespaceOutput {
    let mut pt = PlacementTable::default();
    step_with_scheduling_pt(ns, input, pod_counter, &mut pt)
}

pub(super) fn step_with_scheduling_pt(
    ns: &mut NamespaceStateMachine,
    input: NamespaceInput,
    pod_counter: &mut u64,
    pt: &mut PlacementTable,
) -> NamespaceOutput {
    let mut out = ns.step(input, pt);
    let requests = std::mem::take(&mut out.pod_requests);
    for req in requests {
        // Pick first active worker.
        let wid = ns
            .workers
            .iter()
            .find(|(_, ws)| ws.fabric_status == FabricStatus::Active)
            .map(|(wid, _)| wid.clone());
        if let Some(wid) = wid {
            let pod_id = PodId(format!("pod-{}", *pod_counter));
            *pod_counter += 1;
            let launch_out = ns.step(NamespaceInput::LaunchPod {
                workload_id: req.workload_id,
                worker_id: wid,
                pod_id,
            }, pt);
            out.worker_commands.extend(launch_out.worker_commands);
            out.timers_set.extend(launch_out.timers_set);
            out.timers_cancel.extend(launch_out.timers_cancel);
        }
    }
    out
}

/// Trigger reconcile on an active namespace by stepping with UpdateSpec with same spec.
pub(super) fn reconcile_active_namespace(
    ns: &mut NamespaceStateMachine,
    pod_counter: &mut u64,
) -> NamespaceOutput {
    step_with_scheduling(
        ns,
        NamespaceInput::UpdateSpec {
            client_id: client_id(99),
            spec: ns.spec.clone(),
        },
        pod_counter,
    )
}

/// Helper to get the workload state for the default service.
pub(super) fn get_workload_state(ns: &NamespaceStateMachine) -> &WorkloadState {
    &ns.workloads[&wl_id()].state
}

/// Helper to get the service state for the default service.
pub(super) fn get_service_state(ns: &NamespaceStateMachine) -> &ServiceState {
    &ns.services[&svc_id()].state
}

/// Helper to extract pod_id from a workload in Launching state.
pub(super) fn get_launching_pod_id(ns: &NamespaceStateMachine) -> PodId {
    match get_workload_state(ns) {
        WorkloadState::Active { pod: PodSlot { pod_id, pod_state: PodState::Launching { .. }, .. }, .. } => pod_id.clone(),
        other => panic!("expected Launching, got {:?}", other),
    }
}

