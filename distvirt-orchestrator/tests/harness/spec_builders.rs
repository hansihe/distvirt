use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use distvirt_orchestrator::types::*;
use distvirt_worker_protocol::{
    ActivatorConfig, ContainerConfig, ContainerSpec, NetworkConfig, PodNetworkConfig, ServicePolicy,
};

static POD_OCTET: AtomicU8 = AtomicU8::new(10);
static SERVICE_OCTET: AtomicU8 = AtomicU8::new(100);

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
        },
    }
}

/// Always-on spec: 1 workload "echo" + 1 always-on service "echo-svc".
pub fn always_on_spec() -> NamespaceSpec {
    let wl_id = WorkloadId("echo".to_string());
    let svc_id = ServiceId::from("echo-svc");

    let mut workloads = BTreeMap::new();
    workloads.insert(
        wl_id.clone(),
        WorkloadSpec {
            containers: vec![container_spec("docker.io/library/alpine:latest")],
            network: pod_network(10),
            suspend_on_idle: false,
            resources: None,
            activation: None,
        },
    );

    let mut services = BTreeMap::new();
    services.insert(
        svc_id,
        ServiceSpec {
            workload_id: wl_id,
            ip: Ipv4Addr::new(172, 16, 0, 100),
            policy: ServicePolicy {
                buffer_frames: 100,
                timeout_ms: 5000,
                activator: None,
            },
            activation: None,
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
    let wl_id = WorkloadId("web".to_string());
    let svc_id = ServiceId::from("web-svc");

    let mut workloads = BTreeMap::new();
    workloads.insert(
        wl_id.clone(),
        WorkloadSpec {
            containers: vec![container_spec("docker.io/library/nginx:latest")],
            network: pod_network(10),
            suspend_on_idle: true,
            resources: None,
            activation: None,
        },
    );

    let mut services = BTreeMap::new();
    services.insert(
        svc_id,
        ServiceSpec {
            workload_id: wl_id,
            ip: Ipv4Addr::new(172, 16, 0, 100),
            policy: ServicePolicy {
                buffer_frames: 100,
                timeout_ms: 5000,
                activator: Some(ActivatorConfig::Tcp {
                    ports: None,
                    tcp_only: true,
                    max_flows: 100,
                }),
            },
            activation: Some(ActivationSpec { idle_timeout }),
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
    let wl_id = WorkloadId("web".to_string());
    let svc_id = ServiceId::from("web-svc");

    let mut workloads = BTreeMap::new();
    workloads.insert(
        wl_id.clone(),
        WorkloadSpec {
            containers: vec![container_spec("docker.io/library/nginx:latest")],
            network: pod_network(10),
            suspend_on_idle: false,
            resources: None,
            activation: None,
        },
    );

    let mut services = BTreeMap::new();
    services.insert(
        svc_id,
        ServiceSpec {
            workload_id: wl_id,
            ip: Ipv4Addr::new(172, 16, 0, 100),
            policy: ServicePolicy {
                buffer_frames: 100,
                timeout_ms: 5000,
                activator: Some(ActivatorConfig::Tcp {
                    ports: None,
                    tcp_only: true,
                    max_flows: 100,
                }),
            },
            activation: Some(ActivationSpec { idle_timeout }),
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
    let wl_id = WorkloadId("shared".to_string());

    let mut workloads = BTreeMap::new();
    workloads.insert(
        wl_id.clone(),
        WorkloadSpec {
            containers: vec![container_spec("docker.io/library/nginx:latest")],
            network: pod_network(10),
            suspend_on_idle: true,
            resources: None,
            activation: None,
        },
    );

    let mut services = BTreeMap::new();
    services.insert(
        ServiceId::from("svc-a"),
        ServiceSpec {
            workload_id: wl_id.clone(),
            ip: Ipv4Addr::new(172, 16, 0, 100),
            policy: ServicePolicy {
                buffer_frames: 100,
                timeout_ms: 5000,
                activator: Some(ActivatorConfig::Tcp {
                    ports: None,
                    tcp_only: true,
                    max_flows: 100,
                }),
            },
            activation: Some(ActivationSpec {
                idle_timeout: Duration::from_secs(30),
            }),
        },
    );
    services.insert(
        ServiceId::from("svc-b"),
        ServiceSpec {
            workload_id: wl_id,
            ip: Ipv4Addr::new(172, 16, 0, 101),
            policy: ServicePolicy {
                buffer_frames: 100,
                timeout_ms: 5000,
                activator: Some(ActivatorConfig::Tcp {
                    ports: None,
                    tcp_only: true,
                    max_flows: 100,
                }),
            },
            activation: Some(ActivationSpec {
                idle_timeout: Duration::from_secs(30),
            }),
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
    let wl_id = WorkloadId("shared".to_string());

    let mut workloads = BTreeMap::new();
    workloads.insert(
        wl_id.clone(),
        WorkloadSpec {
            containers: vec![container_spec("docker.io/library/nginx:latest")],
            network: pod_network(10),
            suspend_on_idle: false,
            resources: None,
            activation: None,
        },
    );

    let mut services = BTreeMap::new();
    services.insert(
        ServiceId::from("svc-a"),
        ServiceSpec {
            workload_id: wl_id.clone(),
            ip: Ipv4Addr::new(172, 16, 0, 100),
            policy: ServicePolicy {
                buffer_frames: 100,
                timeout_ms: 5000,
                activator: None,
            },
            activation: None,
        },
    );
    services.insert(
        ServiceId::from("svc-b"),
        ServiceSpec {
            workload_id: wl_id,
            ip: Ipv4Addr::new(172, 16, 0, 101),
            policy: ServicePolicy {
                buffer_frames: 100,
                timeout_ms: 5000,
                activator: None,
            },
            activation: None,
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
    let wl_a = WorkloadId("echo-a".to_string());
    let wl_b = WorkloadId("echo-b".to_string());

    let mut workloads = BTreeMap::new();
    workloads.insert(
        wl_a.clone(),
        WorkloadSpec {
            containers: vec![container_spec("docker.io/library/alpine:latest")],
            network: pod_network(10),
            suspend_on_idle: false,
            resources: None,
            activation: None,
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
        },
    );

    let mut services = BTreeMap::new();
    services.insert(
        ServiceId::from("svc-a"),
        ServiceSpec {
            workload_id: wl_a,
            ip: Ipv4Addr::new(172, 16, 0, 100),
            policy: ServicePolicy {
                buffer_frames: 100,
                timeout_ms: 5000,
                activator: None,
            },
            activation: None,
        },
    );
    services.insert(
        ServiceId::from("svc-b"),
        ServiceSpec {
            workload_id: wl_b,
            ip: Ipv4Addr::new(172, 16, 0, 101),
            policy: ServicePolicy {
                buffer_frames: 100,
                timeout_ms: 5000,
                activator: None,
            },
            activation: None,
        },
    );

    NamespaceSpec {
        network: default_network(),
        workloads,
        services,
    }
}
