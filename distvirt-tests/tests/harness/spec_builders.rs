use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::time::Duration;

use distvirt_orchestrator::types::*;
use distvirt_worker_protocol::{
    ConfigDataFile, ContainerConfig, ContainerSpec, NetworkConfig, PodNetworkConfig, ServicePolicy,
    VolumeMountSpec, VolumeSpec, VolumeType,
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
            volume_mounts: vec![],
        },
    }
}

/// Workload with respects_demand=false, suspend_on_idle=false, NO services.
/// Matches the user scenario: `dv up` with a bare workload that should auto-start.
pub fn no_activation_spec() -> NamespaceSpec {
    let wl_id = WorkloadName("web".to_string());

    let mut workloads = BTreeMap::new();
    workloads.insert(
        wl_id,
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

    NamespaceSpec {
        network: default_network(),
        workloads,
        services: BTreeMap::new(),
    }
}

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

pub fn activation_spec(idle_timeout: Duration) -> NamespaceSpec {
    let wl_id = WorkloadName("web".to_string());
    let svc_id = "web-svc".to_string();

    let mut workloads = BTreeMap::new();
    workloads.insert(
        wl_id.clone(),
        WorkloadSpec {
            containers: vec![container_spec("docker.io/library/alpine:latest")],
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
            policy: ServicePolicy {
                buffer_frames: 100,
                timeout_ms: 5000,
                activator: None,
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

pub fn activation_no_suspend_spec(idle_timeout: Duration) -> NamespaceSpec {
    let wl_id = WorkloadName("web".to_string());
    let svc_id = "web-svc".to_string();

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
            policy: ServicePolicy {
                buffer_frames: 100,
                timeout_ms: 5000,
                activator: None,
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

pub fn two_workload_spec() -> NamespaceSpec {
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
            policy: ServicePolicy {
                buffer_frames: 100,
                timeout_ms: 5000,
                activator: None,
            },
            activation: None,
        },
    );
    services.insert(
        "svc-b".to_string(),
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

pub fn two_activation_workloads_spec(idle_timeout: Duration) -> NamespaceSpec {
    let wl_a = WorkloadName("wl-a".to_string());
    let wl_b = WorkloadName("wl-b".to_string());

    let mut workloads = BTreeMap::new();
    workloads.insert(
        wl_a.clone(),
        WorkloadSpec {
            containers: vec![container_spec("docker.io/library/alpine:latest")],
            network: pod_network(10),
            suspend_on_idle: true,
            resources: None,
            activation: None,
            run_policy: Default::default(),
            respects_demand: true,
            volumes: vec![],
        },
    );
    workloads.insert(
        wl_b.clone(),
        WorkloadSpec {
            containers: vec![container_spec("docker.io/library/alpine:latest")],
            network: pod_network(11),
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
            workload_id: wl_a,
            ip: Ipv4Addr::new(172, 16, 0, 100),
            policy: ServicePolicy {
                buffer_frames: 100,
                timeout_ms: 5000,
                activator: None,
            },
            activation: Some(ActivationSpec { idle_timeout }),
        },
    );
    services.insert(
        "svc-b".to_string(),
        ServiceSpec {
            workload_id: wl_b,
            ip: Ipv4Addr::new(172, 16, 0, 101),
            policy: ServicePolicy {
                buffer_frames: 100,
                timeout_ms: 5000,
                activator: None,
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

pub fn container_spec_with_mounts(image: &str, mounts: Vec<VolumeMountSpec>) -> ContainerSpec {
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
            volume_mounts: mounts,
        },
    }
}

/// Workload with an empty_dir volume mounted at /data.
pub fn empty_dir_spec() -> NamespaceSpec {
    let wl_id = WorkloadName("app".to_string());

    let mut workloads = BTreeMap::new();
    workloads.insert(
        wl_id,
        WorkloadSpec {
            containers: vec![container_spec_with_mounts(
                "docker.io/library/alpine:latest",
                vec![VolumeMountSpec {
                    name: "scratch".to_string(),
                    mount_path: "/data".to_string(),
                }],
            )],
            network: pod_network(10),
            suspend_on_idle: false,
            resources: None,
            activation: None,
            run_policy: Default::default(),
            respects_demand: false,
            volumes: vec![VolumeSpec {
                name: "scratch".to_string(),
                volume_type: VolumeType::EmptyDir { size_mb: 64 },
            }],
        },
    );

    NamespaceSpec {
        network: default_network(),
        workloads,
        services: BTreeMap::new(),
    }
}

/// Workload with a config_data volume containing files, mounted at /config.
pub fn config_data_spec() -> NamespaceSpec {
    let wl_id = WorkloadName("app".to_string());

    let mut workloads = BTreeMap::new();
    workloads.insert(
        wl_id,
        WorkloadSpec {
            containers: vec![container_spec_with_mounts(
                "docker.io/library/alpine:latest",
                vec![VolumeMountSpec {
                    name: "cfg".to_string(),
                    mount_path: "/config".to_string(),
                }],
            )],
            network: pod_network(10),
            suspend_on_idle: false,
            resources: None,
            activation: None,
            run_policy: Default::default(),
            respects_demand: false,
            volumes: vec![VolumeSpec {
                name: "cfg".to_string(),
                volume_type: VolumeType::ConfigData {
                    files: vec![
                        ConfigDataFile {
                            path: "app.toml".to_string(),
                            content: "[server]\nport = 8080\n".to_string(),
                        },
                        ConfigDataFile {
                            path: "secrets/db.env".to_string(),
                            content: "DB_HOST=localhost\n".to_string(),
                        },
                    ],
                },
            }],
        },
    );

    NamespaceSpec {
        network: default_network(),
        workloads,
        services: BTreeMap::new(),
    }
}

/// Workload with both an empty_dir and a config_data volume.
pub fn mixed_volumes_spec() -> NamespaceSpec {
    let wl_id = WorkloadName("app".to_string());

    let mut workloads = BTreeMap::new();
    workloads.insert(
        wl_id,
        WorkloadSpec {
            containers: vec![container_spec_with_mounts(
                "docker.io/library/alpine:latest",
                vec![
                    VolumeMountSpec {
                        name: "scratch".to_string(),
                        mount_path: "/data".to_string(),
                    },
                    VolumeMountSpec {
                        name: "cfg".to_string(),
                        mount_path: "/config".to_string(),
                    },
                ],
            )],
            network: pod_network(10),
            suspend_on_idle: false,
            resources: None,
            activation: None,
            run_policy: Default::default(),
            respects_demand: false,
            volumes: vec![
                VolumeSpec {
                    name: "scratch".to_string(),
                    volume_type: VolumeType::EmptyDir { size_mb: 64 },
                },
                VolumeSpec {
                    name: "cfg".to_string(),
                    volume_type: VolumeType::ConfigData {
                        files: vec![ConfigDataFile {
                            path: "app.conf".to_string(),
                            content: "key=value\n".to_string(),
                        }],
                    },
                },
            ],
        },
    );

    NamespaceSpec {
        network: default_network(),
        workloads,
        services: BTreeMap::new(),
    }
}

/// Activation-based workload with volumes (for suspend/resume testing).
pub fn activation_with_volumes_spec(idle_timeout: Duration) -> NamespaceSpec {
    let wl_id = WorkloadName("web".to_string());
    let svc_id = "web-svc".to_string();

    let mut workloads = BTreeMap::new();
    workloads.insert(
        wl_id.clone(),
        WorkloadSpec {
            containers: vec![container_spec_with_mounts(
                "docker.io/library/alpine:latest",
                vec![
                    VolumeMountSpec {
                        name: "scratch".to_string(),
                        mount_path: "/data".to_string(),
                    },
                    VolumeMountSpec {
                        name: "cfg".to_string(),
                        mount_path: "/config".to_string(),
                    },
                ],
            )],
            network: pod_network(10),
            suspend_on_idle: true,
            resources: None,
            activation: None,
            run_policy: Default::default(),
            respects_demand: true,
            volumes: vec![
                VolumeSpec {
                    name: "scratch".to_string(),
                    volume_type: VolumeType::EmptyDir { size_mb: 32 },
                },
                VolumeSpec {
                    name: "cfg".to_string(),
                    volume_type: VolumeType::ConfigData {
                        files: vec![ConfigDataFile {
                            path: "config.yaml".to_string(),
                            content: "debug: true\n".to_string(),
                        }],
                    },
                },
            ],
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
            activation: Some(ActivationSpec { idle_timeout }),
        },
    );

    NamespaceSpec {
        network: default_network(),
        workloads,
        services,
    }
}

pub fn multi_service_activation_spec(idle_timeout: Duration) -> NamespaceSpec {
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
            policy: ServicePolicy {
                buffer_frames: 100,
                timeout_ms: 5000,
                activator: None,
            },
            activation: Some(ActivationSpec { idle_timeout }),
        },
    );
    services.insert(
        "svc-b".to_string(),
        ServiceSpec {
            workload_id: wl_id,
            ip: Ipv4Addr::new(172, 16, 0, 101),
            policy: ServicePolicy {
                buffer_frames: 100,
                timeout_ms: 5000,
                activator: None,
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
