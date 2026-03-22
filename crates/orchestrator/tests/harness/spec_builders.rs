use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use distvirt_orchestrator::types::*;
use distvirt_worker_protocol::{
    ContainerConfig, ContainerSpec, NetworkConfig, PodNetworkConfig,
};

static POD_OCTET: AtomicU8 = AtomicU8::new(10);
static SERVICE_OCTET: AtomicU8 = AtomicU8::new(100);

/// Extension methods for mutating NamespaceSpec in tests.
pub trait NamespaceSpecExt {
    /// Set the container image for a workload's first container.
    fn set_image(&mut self, wl_name: &str, image: &str) -> &mut Self;
}

impl NamespaceSpecExt for NamespaceSpec {
    fn set_image(&mut self, wl_name: &str, image: &str) -> &mut Self {
        self.workloads
            .get_mut(&WorkloadName(wl_name.to_string()))
            .unwrap_or_else(|| panic!("workload '{}' not found in spec", wl_name))
            .containers[0]
            .image_ref = image.to_string();
        self
    }
}

/// Auto-allocate a unique pod network config (for multi-namespace tests).
pub fn next_pod_network() -> PodNetworkConfig {
    let octet = POD_OCTET.fetch_add(1, Ordering::Relaxed);
    pod_network(octet)
}

/// Auto-allocate a unique service IP (for multi-namespace tests).
pub fn next_service_ip() -> Ipv4Addr {
    let octet = SERVICE_OCTET.fetch_add(1, Ordering::Relaxed);
    Ipv4Addr::new(172, 16, 0, octet)
}

pub fn default_network() -> NetworkConfig {
    NetworkConfig {
        subnet: Ipv4Addr::new(172, 16, 0, 0),
        gateway: Ipv4Addr::new(172, 16, 0, 1),
        prefix_len: 24,
        segment_id: None,
    }
}

pub fn pod_network(host_octet: u8) -> PodNetworkConfig {
    PodNetworkConfig {
        ip: Ipv4Addr::new(172, 16, 0, host_octet),
        mac: [0; 6],
        gateway: Ipv4Addr::new(172, 16, 0, 1),
        netmask: "255.255.255.0".to_string(),
    }
}

pub fn container_spec(image: &str) -> ContainerSpec {
    ContainerSpec {
        container_id: "main".to_string(),
        image_ref: image.to_string(),
        config: ContainerConfig {
            entrypoint: vec!["/bin/echo".to_string()],
            args: vec!["hello".to_string()],
            env: vec![],
            working_dir: None,
            uid: None,
            gid: None,
            hostname: None,
            capture_output: false,
            stdin: false,
            volume_mounts: vec![],
        },
    }
}

/// Always-on spec: 1 workload "echo" + 1 always-on service "echo-svc".
pub fn always_on_spec() -> NamespaceSpec {
    let wl_id = WorkloadName("echo".to_string());
    let svc_id = "echo-svc".to_string();

    let mut workloads = BTreeMap::new();
    workloads.insert(
        wl_id.clone(),
        WorkloadSpec {
            containers: vec![container_spec("docker.io/library/alpine:latest")],
            network: pod_network(10),
            suspend_on_idle: false,
            resources: None,
            activation: None,
            run_policy: Default::default(),
            respects_demand: false,
            volumes: vec![],
        },
    );

    let mut services = BTreeMap::new();
    services.insert(
        svc_id,
        ServiceSpec {
            workload_id: wl_id,
            ip: Ipv4Addr::new(172, 16, 0, 100),
            ports: vec![],
            has_activation: false,
            idle_timeout: Duration::ZERO,
            buffer_frames: 100,
            buffer_timeout_ms: 5000,
        },
    );

    NamespaceSpec {
        network: default_network(),
        workloads,
        services,
    }
}

/// Activation-based spec: 1 workload "web" with suspend_on_idle + 1 activation service "web-svc".
pub fn activation_spec(idle_timeout: Duration) -> NamespaceSpec {
    let wl_id = WorkloadName("web".to_string());
    let svc_id = "web-svc".to_string();

    let mut workloads = BTreeMap::new();
    workloads.insert(
        wl_id.clone(),
        WorkloadSpec {
            containers: vec![container_spec("docker.io/library/nginx:latest")],
            network: pod_network(10),
            suspend_on_idle: true,
            resources: None,
            activation: None,
            run_policy: Default::default(),
            respects_demand: true,
            volumes: vec![],
        },
    );

    let mut services = BTreeMap::new();
    services.insert(
        svc_id,
        ServiceSpec {
            workload_id: wl_id,
            ip: Ipv4Addr::new(172, 16, 0, 100),
            ports: vec![PortConfig {
                port: 80,
                target_port: 80,
                activator: Some(ActivatorKind::Tcp { max_flows: 100 }),
            }],
            has_activation: true,
            idle_timeout,
            buffer_frames: 100,
            buffer_timeout_ms: 5000,
        },
    );

    NamespaceSpec {
        network: default_network(),
        workloads,
        services,
    }
}

/// Activation-based spec without suspend: workload "web" with suspend_on_idle=false + activation service "web-svc".
pub fn activation_no_suspend_spec(idle_timeout: Duration) -> NamespaceSpec {
    let wl_id = WorkloadName("web".to_string());
    let svc_id = "web-svc".to_string();

    let mut workloads = BTreeMap::new();
    workloads.insert(
        wl_id.clone(),
        WorkloadSpec {
            containers: vec![container_spec("docker.io/library/nginx:latest")],
            network: pod_network(10),
            suspend_on_idle: false,
            resources: None,
            activation: None,
            run_policy: Default::default(),
            respects_demand: true,
            volumes: vec![],
        },
    );

    let mut services = BTreeMap::new();
    services.insert(
        svc_id,
        ServiceSpec {
            workload_id: wl_id,
            ip: Ipv4Addr::new(172, 16, 0, 100),
            ports: vec![PortConfig {
                port: 80,
                target_port: 80,
                activator: Some(ActivatorKind::Tcp { max_flows: 100 }),
            }],
            has_activation: true,
            idle_timeout,
            buffer_frames: 100,
            buffer_timeout_ms: 5000,
        },
    );

    NamespaceSpec {
        network: default_network(),
        workloads,
        services,
    }
}

/// 1 workload "shared" backed by 2 services "svc-a" and "svc-b" (both activation-based).
pub fn multi_service_spec() -> NamespaceSpec {
    let wl_id = WorkloadName("shared".to_string());

    let mut workloads = BTreeMap::new();
    workloads.insert(
        wl_id.clone(),
        WorkloadSpec {
            containers: vec![container_spec("docker.io/library/nginx:latest")],
            network: pod_network(10),
            suspend_on_idle: true,
            resources: None,
            activation: None,
            run_policy: Default::default(),
            respects_demand: true,
            volumes: vec![],
        },
    );

    let mut services = BTreeMap::new();
    services.insert(
        "svc-a".to_string(),
        ServiceSpec {
            workload_id: wl_id.clone(),
            ip: Ipv4Addr::new(172, 16, 0, 100),
            ports: vec![PortConfig {
                port: 80,
                target_port: 80,
                activator: Some(ActivatorKind::Tcp { max_flows: 100 }),
            }],
            has_activation: true,
            idle_timeout: Duration::from_secs(30),
            buffer_frames: 100,
            buffer_timeout_ms: 5000,
        },
    );
    services.insert(
        "svc-b".to_string(),
        ServiceSpec {
            workload_id: wl_id,
            ip: Ipv4Addr::new(172, 16, 0, 101),
            ports: vec![PortConfig {
                port: 80,
                target_port: 80,
                activator: Some(ActivatorKind::Tcp { max_flows: 100 }),
            }],
            has_activation: true,
            idle_timeout: Duration::from_secs(30),
            buffer_frames: 100,
            buffer_timeout_ms: 5000,
        },
    );

    NamespaceSpec {
        network: default_network(),
        workloads,
        services,
    }
}

/// Network config with a segment ID (required for multi-worker tunnel routing).
pub fn network_with_segment(segment_id: u16) -> NetworkConfig {
    NetworkConfig {
        subnet: Ipv4Addr::new(172, 16, 0, 0),
        gateway: Ipv4Addr::new(172, 16, 0, 1),
        prefix_len: 24,
        segment_id: Some(segment_id),
    }
}

/// Always-on spec with a segment ID on the network (for multi-worker/tunnel tests).
pub fn always_on_spec_with_segment(segment_id: u16) -> NamespaceSpec {
    let mut spec = always_on_spec();
    spec.network = network_with_segment(segment_id);
    spec
}

/// Activation-based spec with a segment ID on the network (for multi-worker route tests).
pub fn activation_spec_with_segment(idle_timeout: Duration, segment_id: u16) -> NamespaceSpec {
    let mut spec = activation_spec(idle_timeout);
    spec.network = network_with_segment(segment_id);
    spec
}

/// 1 workload "shared" backed by 2 always-on services "svc-a" and "svc-b" (no activation).
pub fn always_on_multi_service_spec() -> NamespaceSpec {
    let wl_id = WorkloadName("shared".to_string());

    let mut workloads = BTreeMap::new();
    workloads.insert(
        wl_id.clone(),
        WorkloadSpec {
            containers: vec![container_spec("docker.io/library/nginx:latest")],
            network: pod_network(10),
            suspend_on_idle: false,
            resources: None,
            activation: None,
            run_policy: Default::default(),
            respects_demand: false,
            volumes: vec![],
        },
    );

    let mut services = BTreeMap::new();
    services.insert(
        "svc-a".to_string(),
        ServiceSpec {
            workload_id: wl_id.clone(),
            ip: Ipv4Addr::new(172, 16, 0, 100),
            ports: vec![],
            has_activation: false,
            idle_timeout: Duration::ZERO,
            buffer_frames: 100,
            buffer_timeout_ms: 5000,
        },
    );
    services.insert(
        "svc-b".to_string(),
        ServiceSpec {
            workload_id: wl_id,
            ip: Ipv4Addr::new(172, 16, 0, 101),
            ports: vec![],
            has_activation: false,
            idle_timeout: Duration::ZERO,
            buffer_frames: 100,
            buffer_timeout_ms: 5000,
        },
    );

    NamespaceSpec {
        network: default_network(),
        workloads,
        services,
    }
}

/// 2 independent always-on workloads "echo-a" and "echo-b" with services.
pub fn always_on_two_workloads_spec() -> NamespaceSpec {
    let wl_a = WorkloadName("echo-a".to_string());
    let wl_b = WorkloadName("echo-b".to_string());

    let mut workloads = BTreeMap::new();
    workloads.insert(
        wl_a.clone(),
        WorkloadSpec {
            containers: vec![container_spec("docker.io/library/alpine:latest")],
            network: pod_network(10),
            suspend_on_idle: false,
            resources: None,
            activation: None,
            run_policy: Default::default(),
            respects_demand: false,
            volumes: vec![],
        },
    );
    workloads.insert(
        wl_b.clone(),
        WorkloadSpec {
            containers: vec![container_spec("docker.io/library/alpine:latest")],
            network: pod_network(11),
            suspend_on_idle: false,
            resources: None,
            activation: None,
            run_policy: Default::default(),
            respects_demand: false,
            volumes: vec![],
        },
    );

    let mut services = BTreeMap::new();
    services.insert(
        "svc-a".to_string(),
        ServiceSpec {
            workload_id: wl_a,
            ip: Ipv4Addr::new(172, 16, 0, 100),
            ports: vec![],
            has_activation: false,
            idle_timeout: Duration::ZERO,
            buffer_frames: 100,
            buffer_timeout_ms: 5000,
        },
    );
    services.insert(
        "svc-b".to_string(),
        ServiceSpec {
            workload_id: wl_b,
            ip: Ipv4Addr::new(172, 16, 0, 101),
            ports: vec![],
            has_activation: false,
            idle_timeout: Duration::ZERO,
            buffer_frames: 100,
            buffer_timeout_ms: 5000,
        },
    );

    NamespaceSpec {
        network: default_network(),
        workloads,
        services,
    }
}
