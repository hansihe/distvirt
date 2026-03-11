use std::collections::BTreeMap;
use std::net::Ipv4Addr;

use distvirt_orchestrator::types::*;
use distvirt_worker_protocol::{
    ContainerConfig, ContainerSpec, NetworkConfig, PodNetworkConfig, ServicePolicy,
};

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
