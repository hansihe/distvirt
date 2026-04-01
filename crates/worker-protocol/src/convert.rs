//! Conversion functions between Rust domain types and protobuf-generated types.

use std::net::Ipv4Addr;

use anyhow::{Context, bail};

use crate::proto;
use crate::types::*;

// --- Scalar Helpers ---

fn ipv4_to_u32(addr: &Ipv4Addr) -> u32 {
    u32::from_be_bytes(addr.octets())
}

fn u32_to_ipv4(raw: u32) -> Ipv4Addr {
    Ipv4Addr::from(raw.to_be_bytes())
}

fn mac_to_bytes(mac: &[u8; 6]) -> Vec<u8> {
    mac.to_vec()
}

fn bytes_to_mac(bytes: &[u8]) -> anyhow::Result<[u8; 6]> {
    bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid MAC length: {}", bytes.len()))
}

fn bytes_to_key32(bytes: &[u8]) -> anyhow::Result<[u8; 32]> {
    bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid key length: {} (expected 32)", bytes.len()))
}

// --- NamespaceId ---

fn ns_to_proto(ns: &NamespaceId) -> proto::NamespaceId {
    proto::NamespaceId {
        name: ns.name.clone(),
        id: ns.id,
    }
}

fn ns_from_proto(ns: Option<proto::NamespaceId>) -> anyhow::Result<NamespaceId> {
    let ns = ns.context("missing namespace_id")?;
    Ok(NamespaceId {
        name: ns.name,
        id: ns.id,
    })
}

// --- NetworkConfig ---

fn network_config_to_proto(val: &NetworkConfig) -> proto::NetworkConfig {
    proto::NetworkConfig {
        subnet: ipv4_to_u32(&val.subnet),
        gateway: ipv4_to_u32(&val.gateway),
        prefix_len: val.prefix_len as u32,
        segment_id: val.segment_id.map(|id| id as u32),
    }
}

fn network_config_from_proto(val: Option<proto::NetworkConfig>) -> anyhow::Result<NetworkConfig> {
    let val = val.context("missing network config")?;
    Ok(NetworkConfig {
        subnet: u32_to_ipv4(val.subnet),
        gateway: u32_to_ipv4(val.gateway),
        prefix_len: val.prefix_len as u8,
        segment_id: val.segment_id.map(|id| id as u16),
    })
}

// --- PodNetworkConfig ---

fn pod_network_to_proto(val: &PodNetworkConfig) -> proto::PodNetworkConfig {
    proto::PodNetworkConfig {
        ip: ipv4_to_u32(&val.ip),
        mac: mac_to_bytes(&val.mac),
        gateway: ipv4_to_u32(&val.gateway),
        netmask: val.netmask.clone(),
    }
}

fn pod_network_from_proto(val: Option<proto::PodNetworkConfig>) -> anyhow::Result<PodNetworkConfig> {
    let val = val.context("missing pod network config")?;
    Ok(PodNetworkConfig {
        ip: u32_to_ipv4(val.ip),
        mac: bytes_to_mac(&val.mac)?,
        gateway: u32_to_ipv4(val.gateway),
        netmask: val.netmask,
    })
}

// --- ContainerConfig ---

fn container_config_to_proto(val: &ContainerConfig) -> proto::ContainerConfig {
    proto::ContainerConfig {
        command: val.command.as_ref().map(|v| proto::OptionalStringList {
            values: v.clone(),
        }),
        args: val.args.as_ref().map(|v| proto::OptionalStringList {
            values: v.clone(),
        }),
        env: val.env.clone(),
        working_dir: val.working_dir.clone().unwrap_or_default(),
        user: val.user.clone().unwrap_or_default(),
        hostname: val.hostname.clone().unwrap_or_default(),
        capture_output: val.capture_output,
        stdin: val.stdin,
        volume_mounts: val.volume_mounts.iter().map(|m| proto::VolumeMountSpec {
            name: m.name.clone(),
            mount_path: m.mount_path.clone(),
        }).collect(),
    }
}

fn container_config_from_proto(val: Option<proto::ContainerConfig>) -> anyhow::Result<ContainerConfig> {
    let val = val.context("missing container config")?;
    Ok(ContainerConfig {
        command: val.command.map(|l| l.values),
        args: val.args.map(|l| l.values),
        env: val.env,
        working_dir: if val.working_dir.is_empty() { None } else { Some(val.working_dir) },
        user: if val.user.is_empty() { None } else { Some(val.user) },
        hostname: if val.hostname.is_empty() { None } else { Some(val.hostname) },
        capture_output: val.capture_output,
        stdin: val.stdin,
        volume_mounts: val.volume_mounts.into_iter().map(|m| VolumeMountSpec {
            name: m.name,
            mount_path: m.mount_path,
        }).collect(),
    })
}

// --- VolumeSpec ---

fn volume_spec_to_proto(val: &VolumeSpec) -> proto::VolumeSpec {
    let kind = match &val.volume_type {
        VolumeType::EmptyDir { size_mb } => {
            proto::volume_spec::Kind::EmptyDir(proto::volume_spec::EmptyDir {
                size_mb: *size_mb,
            })
        }
        VolumeType::ConfigData { files } => {
            proto::volume_spec::Kind::ConfigData(proto::volume_spec::ConfigData {
                files: files.iter().map(|f| proto::ConfigDataFile {
                    path: f.path.clone(),
                    content: f.content.clone(),
                    mode: f.mode,
                }).collect(),
            })
        }
    };
    proto::VolumeSpec {
        name: val.name.clone(),
        kind: Some(kind),
    }
}

fn volume_spec_from_proto(val: proto::VolumeSpec) -> anyhow::Result<VolumeSpec> {
    let kind = val.kind.context("missing volume kind")?;
    let volume_type = match kind {
        proto::volume_spec::Kind::EmptyDir(ed) => VolumeType::EmptyDir { size_mb: ed.size_mb },
        proto::volume_spec::Kind::ConfigData(cd) => VolumeType::ConfigData {
            files: cd.files.into_iter().map(|f| ConfigDataFile {
                path: f.path,
                content: f.content,
                mode: f.mode,
            }).collect(),
        },
    };
    Ok(VolumeSpec {
        name: val.name,
        volume_type,
    })
}

// --- ContainerSpec ---

fn container_spec_to_proto(val: &ContainerSpec) -> proto::ContainerSpec {
    proto::ContainerSpec {
        container_id: val.container_id.clone(),
        image_ref: val.image_ref.clone(),
        config: Some(container_config_to_proto(&val.config)),
    }
}

fn container_spec_from_proto(val: proto::ContainerSpec) -> anyhow::Result<ContainerSpec> {
    Ok(ContainerSpec {
        container_id: val.container_id,
        image_ref: val.image_ref,
        config: container_config_from_proto(val.config)?,
    })
}

// --- RegistryEntry ---

fn registry_entry_to_proto(val: &RegistryEntry) -> proto::RegistryEntry {
    proto::RegistryEntry {
        name: val.name.clone(),
        ip: ipv4_to_u32(&val.ip),
    }
}

fn registry_entry_from_proto(val: proto::RegistryEntry) -> RegistryEntry {
    RegistryEntry {
        name: val.name,
        ip: u32_to_ipv4(val.ip),
    }
}

// --- ResourceRequirements ---

fn resource_values_to_proto(val: &ResourceValues) -> proto::ResourceValues {
    proto::ResourceValues {
        memory_mib: val.memory_mib,
        vcpus: val.vcpus,
    }
}

fn resource_values_from_proto(val: proto::ResourceValues) -> ResourceValues {
    ResourceValues {
        memory_mib: val.memory_mib,
        vcpus: val.vcpus,
    }
}

fn resource_requirements_to_proto(val: &ResourceRequirements) -> proto::ResourceRequirements {
    proto::ResourceRequirements {
        requests: val.requests.as_ref().map(resource_values_to_proto),
        limits: val.limits.as_ref().map(resource_values_to_proto),
    }
}

fn resource_requirements_from_proto(val: proto::ResourceRequirements) -> ResourceRequirements {
    ResourceRequirements {
        requests: val.requests.map(resource_values_from_proto),
        limits: val.limits.map(resource_values_from_proto),
    }
}

// --- ServicePolicy / PortConfig / ActivatorConfig ---

fn activator_config_to_proto(val: &ActivatorConfig) -> proto::ActivatorConfig {
    let kind = match val {
        ActivatorConfig::Tcp { max_flows } => {
            proto::activator_config::Kind::Tcp(proto::activator_config::TcpActivator {
                max_flows: *max_flows,
            })
        }
        ActivatorConfig::Http2 => {
            proto::activator_config::Kind::Http2(proto::activator_config::Http2Activator {})
        }
    };
    proto::ActivatorConfig { kind: Some(kind) }
}

fn activator_config_from_proto(val: proto::ActivatorConfig) -> anyhow::Result<ActivatorConfig> {
    match val.kind.context("missing activator kind")? {
        proto::activator_config::Kind::Tcp(tcp) => Ok(ActivatorConfig::Tcp {
            max_flows: tcp.max_flows,
        }),
        proto::activator_config::Kind::Http2(_) => Ok(ActivatorConfig::Http2),
    }
}

fn port_config_to_proto(val: &PortConfig) -> proto::PortConfig {
    proto::PortConfig {
        port: val.port as u32,
        target_port: val.target_port as u32,
        activator: val.activator.as_ref().map(activator_config_to_proto),
    }
}

fn port_config_from_proto(val: proto::PortConfig) -> anyhow::Result<PortConfig> {
    Ok(PortConfig {
        port: val.port as u16,
        target_port: val.target_port as u16,
        activator: val.activator.map(activator_config_from_proto).transpose()?,
    })
}

fn service_policy_to_proto(val: &ServicePolicy) -> proto::ServicePolicy {
    proto::ServicePolicy {
        buffer_frames: val.buffer_frames,
        timeout_ms: val.timeout_ms,
        ports: val.ports.iter().map(port_config_to_proto).collect(),
    }
}

fn service_policy_from_proto(val: Option<proto::ServicePolicy>) -> anyhow::Result<ServicePolicy> {
    let val = val.context("missing service policy")?;
    Ok(ServicePolicy {
        buffer_frames: val.buffer_frames,
        timeout_ms: val.timeout_ms,
        ports: val.ports.into_iter().map(port_config_from_proto).collect::<anyhow::Result<_>>()?,
    })
}

// --- Endpoint Types ---

fn endpoint_placement_to_proto(val: &EndpointPlacement) -> proto::EndpointPlacement {
    proto::EndpointPlacement {
        worker_id: val.worker_id.0,
    }
}

fn endpoint_placement_from_proto(val: proto::EndpointPlacement) -> EndpointPlacement {
    EndpointPlacement {
        worker_id: WorkerId(val.worker_id),
    }
}

fn endpoint_pod_backend_to_proto(val: &EndpointPodBackend) -> proto::EndpointPodBackend {
    proto::EndpointPodBackend {
        pod_ip: ipv4_to_u32(&val.pod_ip),
        placement: val.placement.as_ref().map(endpoint_placement_to_proto),
        ready: val.ready,
    }
}

fn endpoint_pod_backend_from_proto(val: proto::EndpointPodBackend) -> EndpointPodBackend {
    EndpointPodBackend {
        pod_ip: u32_to_ipv4(val.pod_ip),
        placement: val.placement.map(endpoint_placement_from_proto),
        ready: val.ready,
    }
}

fn endpoint_spec_to_proto(val: &EndpointSpec) -> proto::EndpointSpec {
    let kind = match &val.kind {
        EndpointKind::Service { service_id, policy, backend } => {
            proto::endpoint_spec::Kind::Service(proto::endpoint_spec::ServiceEndpoint {
                service_id: service_id.0.to_string(),
                policy: Some(service_policy_to_proto(policy)),
                backend: backend.as_ref().map(endpoint_pod_backend_to_proto),
            })
        }
        EndpointKind::Pod { placement } => {
            proto::endpoint_spec::Kind::Pod(proto::endpoint_spec::PodEndpoint {
                placement: placement.as_ref().map(endpoint_placement_to_proto),
            })
        }
        EndpointKind::WireGuardPeer { placement } => {
            proto::endpoint_spec::Kind::WireguardPeer(proto::endpoint_spec::WireGuardPeerEndpoint {
                placement: placement.as_ref().map(endpoint_placement_to_proto),
            })
        }
    };
    proto::EndpointSpec {
        ip: ipv4_to_u32(&val.ip),
        kind: Some(kind),
    }
}

fn endpoint_spec_from_proto(val: proto::EndpointSpec) -> anyhow::Result<EndpointSpec> {
    let kind = match val.kind.context("missing endpoint kind")? {
        proto::endpoint_spec::Kind::Service(svc) => {
            let service_id = ServiceId(svc.service_id.parse::<u64>().context("invalid service_id")?);
            EndpointKind::Service {
                service_id,
                policy: service_policy_from_proto(svc.policy)?,
                backend: svc.backend.map(endpoint_pod_backend_from_proto),
            }
        }
        proto::endpoint_spec::Kind::Pod(pod) => {
            EndpointKind::Pod {
                placement: pod.placement.map(endpoint_placement_from_proto),
            }
        }
        proto::endpoint_spec::Kind::WireguardPeer(wg) => {
            EndpointKind::WireGuardPeer {
                placement: wg.placement.map(endpoint_placement_from_proto),
            }
        }
    };
    Ok(EndpointSpec {
        ip: u32_to_ipv4(val.ip),
        kind,
    })
}

// --- PoolInfo ---

fn pool_info_to_proto(val: &PoolInfo) -> proto::PoolInfo {
    proto::PoolInfo {
        pool_id: val.pool_id.0.clone(),
        path: val.path.clone(),
        capacity_bytes: val.capacity_bytes,
        available_bytes: val.available_bytes,
    }
}

fn pool_info_from_proto(val: proto::PoolInfo) -> PoolInfo {
    PoolInfo {
        pool_id: PoolId(val.pool_id),
        path: val.path,
        capacity_bytes: val.capacity_bytes,
        available_bytes: val.available_bytes,
    }
}

// --- WorkerPeerInfo ---

fn worker_peer_info_to_proto(val: &WorkerPeerInfo) -> proto::WorkerPeerInfo {
    proto::WorkerPeerInfo {
        worker_id: val.worker_id.0,
        endpoint: val.endpoint.clone(),
        public_key: val.public_key.to_vec(),
        segments: val.segments.iter().map(|s| *s as u32).collect(),
    }
}

fn worker_peer_info_from_proto(val: proto::WorkerPeerInfo) -> anyhow::Result<WorkerPeerInfo> {
    Ok(WorkerPeerInfo {
        worker_id: WorkerId(val.worker_id),
        endpoint: val.endpoint,
        public_key: bytes_to_key32(&val.public_key)?,
        segments: val.segments.into_iter().map(|s| s as u16).collect(),
    })
}

// --- AdapterConfig ---

fn adapter_config_to_proto(val: &AdapterConfig) -> proto::AdapterConfig {
    let kind = match val {
        AdapterConfig::WireGuard { listen_port } => {
            proto::adapter_config::Kind::Wireguard(proto::adapter_config::WireGuard {
                listen_port: *listen_port as u32,
            })
        }
        AdapterConfig::ReverseProxy { listen_port, tls_cert, tls_key } => {
            proto::adapter_config::Kind::ReverseProxy(proto::adapter_config::ReverseProxy {
                listen_port: *listen_port as u32,
                tls_cert: tls_cert.clone(),
                tls_key: tls_key.clone(),
            })
        }
        AdapterConfig::OsRouting { interface } => {
            proto::adapter_config::Kind::OsRouting(proto::adapter_config::OsRouting {
                interface: interface.clone(),
            })
        }
    };
    proto::AdapterConfig { kind: Some(kind) }
}

fn adapter_config_from_proto(val: proto::AdapterConfig) -> anyhow::Result<AdapterConfig> {
    match val.kind.context("missing adapter kind")? {
        proto::adapter_config::Kind::Wireguard(wg) => Ok(AdapterConfig::WireGuard {
            listen_port: wg.listen_port as u16,
        }),
        proto::adapter_config::Kind::ReverseProxy(rp) => Ok(AdapterConfig::ReverseProxy {
            listen_port: rp.listen_port as u16,
            tls_cert: rp.tls_cert,
            tls_key: rp.tls_key,
        }),
        proto::adapter_config::Kind::OsRouting(os) => Ok(AdapterConfig::OsRouting {
            interface: os.interface,
        }),
    }
}

// --- PsiMetrics ---

fn psi_metrics_to_proto(val: &PsiMetrics) -> proto::PsiMetrics {
    proto::PsiMetrics {
        some_avg10: val.some_avg10,
        some_avg60: val.some_avg60,
        full_avg10: val.full_avg10,
        full_avg60: val.full_avg60,
    }
}

fn psi_metrics_from_proto(val: Option<proto::PsiMetrics>) -> PsiMetrics {
    match val {
        Some(v) => PsiMetrics {
            some_avg10: v.some_avg10,
            some_avg60: v.some_avg60,
            full_avg10: v.full_avg10,
            full_avg60: v.full_avg60,
        },
        None => PsiMetrics::default(),
    }
}

// --- Handshake Messages ---

pub fn worker_hello_to_proto(val: &WorkerHello) -> proto::WorkerHello {
    proto::WorkerHello {
        auth_token: val.auth_token.clone(),
        capabilities: Some(worker_capabilities_to_proto(&val.capabilities)),
    }
}

pub fn worker_hello_from_proto(val: proto::WorkerHello) -> anyhow::Result<WorkerHello> {
    Ok(WorkerHello {
        auth_token: val.auth_token,
        capabilities: worker_capabilities_from_proto(val.capabilities.context("missing capabilities")?)?,
    })
}

fn worker_capabilities_to_proto(val: &WorkerCapabilities) -> proto::WorkerCapabilities {
    proto::WorkerCapabilities {
        has_kvm: val.has_kvm,
        has_containerd: val.has_containerd,
        available_adapters: val.available_adapters.clone(),
        max_pods: val.max_pods,
        available_memory_mb: val.available_memory_mb,
        public_endpoint: val.public_endpoint.clone(),
        tunnel_listen_port: None,
        tunnel_public_key: None,
        pools: val.pools.iter().map(pool_info_to_proto).collect(),
    }
}

fn worker_capabilities_from_proto(val: proto::WorkerCapabilities) -> anyhow::Result<WorkerCapabilities> {
    Ok(WorkerCapabilities {
        has_kvm: val.has_kvm,
        has_containerd: val.has_containerd,
        available_adapters: val.available_adapters,
        max_pods: val.max_pods,
        available_memory_mb: val.available_memory_mb,
        public_endpoint: val.public_endpoint,
        pools: val.pools.into_iter().map(pool_info_from_proto).collect(),
    })
}

pub fn worker_accepted_to_proto(val: &WorkerAccepted) -> proto::WorkerAccepted {
    proto::WorkerAccepted {
        worker_id: val.worker_id.0,
        adapters: val.adapters.iter().map(adapter_config_to_proto).collect(),
        tunnel_encrypted: val.tunnel_encrypted,
        pools: val.pools.iter().map(pool_info_to_proto).collect(),
    }
}

pub fn worker_accepted_from_proto(val: proto::WorkerAccepted) -> anyhow::Result<WorkerAccepted> {
    Ok(WorkerAccepted {
        worker_id: WorkerId(val.worker_id),
        adapters: val.adapters.into_iter().map(adapter_config_from_proto).collect::<anyhow::Result<_>>()?,
        tunnel_encrypted: val.tunnel_encrypted,
        pools: val.pools.into_iter().map(pool_info_from_proto).collect(),
    })
}

pub fn worker_ready_to_proto(val: &WorkerReady) -> proto::WorkerReady {
    proto::WorkerReady {
        tunnel_listen_port: val.tunnel_listen_port.map(|p| p as u32),
        tunnel_public_key: val.tunnel_public_key.map(|k| k.to_vec()),
        transfer_listen_port: val.transfer_listen_port.map(|p| p as u32),
        wireguard_listen_port: val.wireguard_listen_port.map(|p| p as u32),
        wireguard_public_key: val.wireguard_public_key.map(|k| k.to_vec()),
    }
}

pub fn worker_ready_from_proto(val: proto::WorkerReady) -> WorkerReady {
    WorkerReady {
        tunnel_listen_port: val.tunnel_listen_port.map(|p| p as u16),
        tunnel_public_key: val.tunnel_public_key.and_then(|k| <[u8; 32]>::try_from(k.as_slice()).ok()),
        transfer_listen_port: val.transfer_listen_port.map(|p| p as u16),
        wireguard_listen_port: val.wireguard_listen_port.map(|p| p as u16),
        wireguard_public_key: val.wireguard_public_key.and_then(|k| <[u8; 32]>::try_from(k.as_slice()).ok()),
    }
}

// --- WorkerCommand ---

pub fn worker_command_to_proto(cmd: &WorkerCommand) -> proto::WorkerCommand {
    use proto::worker_command::Command;

    let command = match cmd {
        WorkerCommand::CreateNamespace { namespace_id, network } => {
            Command::CreateNamespace(proto::CreateNamespaceCmd {
                namespace_id: Some(ns_to_proto(namespace_id)),
                network: Some(network_config_to_proto(network)),
            })
        }
        WorkerCommand::DestroyNamespace { namespace_id } => {
            Command::DestroyNamespace(proto::DestroyNamespaceCmd {
                namespace_id: Some(ns_to_proto(namespace_id)),
            })
        }
        WorkerCommand::RegistrySync { namespace_id, entries } => {
            Command::RegistrySync(proto::RegistrySyncCmd {
                namespace_id: Some(ns_to_proto(namespace_id)),
                entries: entries.iter().map(registry_entry_to_proto).collect(),
            })
        }
        WorkerCommand::RegistryUpdate { namespace_id, added, removed } => {
            Command::RegistryUpdate(proto::RegistryUpdateCmd {
                namespace_id: Some(ns_to_proto(namespace_id)),
                added: added.iter().map(registry_entry_to_proto).collect(),
                removed: removed.clone(),
            })
        }
        WorkerCommand::LaunchPod { namespace_id, pod_id, network, containers, resources, volumes } => {
            Command::LaunchPod(proto::LaunchPodCmd {
                namespace_id: Some(ns_to_proto(namespace_id)),
                pod_id: pod_id.0,
                network: Some(pod_network_to_proto(network)),
                containers: containers.iter().map(container_spec_to_proto).collect(),
                resources: resources.as_ref().map(resource_requirements_to_proto),
                volumes: volumes.iter().map(volume_spec_to_proto).collect(),
            })
        }
        WorkerCommand::StopPod { namespace_id, pod_id, graceful } => {
            Command::StopPod(proto::StopPodCmd {
                namespace_id: Some(ns_to_proto(namespace_id)),
                pod_id: pod_id.0,
                graceful: *graceful,
            })
        }
        WorkerCommand::AddWireGuardPeer { namespace_id, peer_public_key, peer_ip, preshared_key } => {
            Command::AddWireguardPeer(proto::AddWireGuardPeerCmd {
                namespace_id: Some(ns_to_proto(namespace_id)),
                peer_public_key: peer_public_key.to_vec(),
                peer_ip: ipv4_to_u32(peer_ip),
                preshared_key: preshared_key.map(|k| k.to_vec()),
            })
        }
        WorkerCommand::RemoveWireGuardPeer { peer_public_key } => {
            Command::RemoveWireguardPeer(proto::RemoveWireGuardPeerCmd {
                peer_public_key: peer_public_key.to_vec(),
            })
        }
        WorkerCommand::SuspendPod { namespace_id, pod_id, artifact_id, pool_id } => {
            Command::SuspendPod(proto::SuspendPodCmd {
                namespace_id: Some(ns_to_proto(namespace_id)),
                pod_id: pod_id.0,
                artifact_id: artifact_id.0.clone(),
                pool_id: pool_id.0.clone(),
            })
        }
        WorkerCommand::ResumePod { namespace_id, pod_id, artifact_id, network, pool_id } => {
            Command::ResumePod(proto::ResumePodCmd {
                namespace_id: Some(ns_to_proto(namespace_id)),
                pod_id: pod_id.0,
                artifact_id: artifact_id.0.clone(),
                network: Some(pod_network_to_proto(network)),
                pool_id: pool_id.0.clone(),
            })
        }
        WorkerCommand::DeleteArtifact { artifact_id, pool_id } => {
            Command::DeleteArtifact(proto::DeleteArtifactCmd {
                artifact_id: artifact_id.0.clone(),
                pool_id: pool_id.0.clone(),
            })
        }
        WorkerCommand::TransferArtifact { transfer_id, source_artifact_id, source_pool_id, dest_artifact_id, dest_pool_id, dest_endpoint } => {
            Command::TransferArtifact(proto::TransferArtifactCmd {
                transfer_id: *transfer_id,
                source_artifact_id: source_artifact_id.0.clone(),
                source_pool_id: source_pool_id.0.clone(),
                dest_artifact_id: dest_artifact_id.0.clone(),
                dest_pool_id: dest_pool_id.0.clone(),
                dest_endpoint: dest_endpoint.clone().unwrap_or_default(),
            })
        }
        WorkerCommand::WorkerRegistrySync { workers } => {
            Command::WorkerRegistrySync(proto::WorkerRegistrySyncCmd {
                workers: workers.iter().map(worker_peer_info_to_proto).collect(),
            })
        }
        WorkerCommand::EndpointSync { namespace_id, endpoints } => {
            Command::EndpointSync(proto::EndpointSyncCmd {
                namespace_id: Some(ns_to_proto(namespace_id)),
                endpoints: endpoints.iter().map(endpoint_spec_to_proto).collect(),
            })
        }
        WorkerCommand::EndpointUpdate { namespace_id, upserted, removed_ips } => {
            Command::EndpointUpdate(proto::EndpointUpdateCmd {
                namespace_id: Some(ns_to_proto(namespace_id)),
                upserted: upserted.iter().map(endpoint_spec_to_proto).collect(),
                removed_ips: removed_ips.iter().map(ipv4_to_u32).collect(),
            })
        }
        WorkerCommand::Shutdown => {
            Command::Shutdown(proto::ShutdownCmd {})
        }
    };

    proto::WorkerCommand { command: Some(command) }
}

pub fn worker_command_from_proto(msg: proto::WorkerCommand) -> anyhow::Result<WorkerCommand> {
    use proto::worker_command::Command;

    match msg.command.context("empty WorkerCommand")? {
        Command::CreateNamespace(r) => Ok(WorkerCommand::CreateNamespace {
            namespace_id: ns_from_proto(r.namespace_id)?,
            network: network_config_from_proto(r.network)?,
        }),
        Command::DestroyNamespace(r) => Ok(WorkerCommand::DestroyNamespace {
            namespace_id: ns_from_proto(r.namespace_id)?,
        }),
        Command::RegistrySync(r) => Ok(WorkerCommand::RegistrySync {
            namespace_id: ns_from_proto(r.namespace_id)?,
            entries: r.entries.into_iter().map(registry_entry_from_proto).collect(),
        }),
        Command::RegistryUpdate(r) => Ok(WorkerCommand::RegistryUpdate {
            namespace_id: ns_from_proto(r.namespace_id)?,
            added: r.added.into_iter().map(registry_entry_from_proto).collect(),
            removed: r.removed,
        }),
        Command::LaunchPod(r) => {
            let containers = r.containers.into_iter()
                .map(container_spec_from_proto)
                .collect::<anyhow::Result<_>>()?;
            let volumes = r.volumes.into_iter()
                .map(volume_spec_from_proto)
                .collect::<anyhow::Result<_>>()?;
            Ok(WorkerCommand::LaunchPod {
                namespace_id: ns_from_proto(r.namespace_id)?,
                pod_id: PodId(r.pod_id),
                network: pod_network_from_proto(r.network)?,
                containers,
                resources: r.resources.map(resource_requirements_from_proto),
                volumes,
            })
        }
        Command::StopPod(r) => Ok(WorkerCommand::StopPod {
            namespace_id: ns_from_proto(r.namespace_id)?,
            pod_id: PodId(r.pod_id),
            graceful: r.graceful,
        }),
        Command::AddWireguardPeer(r) => Ok(WorkerCommand::AddWireGuardPeer {
            namespace_id: ns_from_proto(r.namespace_id)?,
            peer_public_key: bytes_to_key32(&r.peer_public_key)?,
            peer_ip: u32_to_ipv4(r.peer_ip),
            preshared_key: r.preshared_key.as_deref().map(bytes_to_key32).transpose()?,
        }),
        Command::RemoveWireguardPeer(r) => Ok(WorkerCommand::RemoveWireGuardPeer {
            peer_public_key: bytes_to_key32(&r.peer_public_key)?,
        }),
        Command::SuspendPod(r) => Ok(WorkerCommand::SuspendPod {
            namespace_id: ns_from_proto(r.namespace_id)?,
            pod_id: PodId(r.pod_id),
            artifact_id: ArtifactId(r.artifact_id),
            pool_id: PoolId(r.pool_id),
        }),
        Command::ResumePod(r) => Ok(WorkerCommand::ResumePod {
            namespace_id: ns_from_proto(r.namespace_id)?,
            pod_id: PodId(r.pod_id),
            artifact_id: ArtifactId(r.artifact_id),
            network: pod_network_from_proto(r.network)?,
            pool_id: PoolId(r.pool_id),
        }),
        Command::DeleteArtifact(r) => Ok(WorkerCommand::DeleteArtifact {
            artifact_id: ArtifactId(r.artifact_id),
            pool_id: PoolId(r.pool_id),
        }),
        Command::TransferArtifact(r) => {
            let dest_endpoint = if r.dest_endpoint.is_empty() { None } else { Some(r.dest_endpoint) };
            Ok(WorkerCommand::TransferArtifact {
                transfer_id: r.transfer_id,
                source_artifact_id: ArtifactId(r.source_artifact_id),
                source_pool_id: PoolId(r.source_pool_id),
                dest_artifact_id: ArtifactId(r.dest_artifact_id),
                dest_pool_id: PoolId(r.dest_pool_id),
                dest_endpoint,
            })
        }
        Command::WorkerRegistrySync(r) => {
            let workers = r.workers.into_iter()
                .map(worker_peer_info_from_proto)
                .collect::<anyhow::Result<_>>()?;
            Ok(WorkerCommand::WorkerRegistrySync { workers })
        }
        Command::EndpointSync(r) => {
            let endpoints = r.endpoints.into_iter()
                .map(endpoint_spec_from_proto)
                .collect::<anyhow::Result<_>>()?;
            Ok(WorkerCommand::EndpointSync {
                namespace_id: ns_from_proto(r.namespace_id)?,
                endpoints,
            })
        }
        Command::EndpointUpdate(r) => {
            let upserted = r.upserted.into_iter()
                .map(endpoint_spec_from_proto)
                .collect::<anyhow::Result<_>>()?;
            Ok(WorkerCommand::EndpointUpdate {
                namespace_id: ns_from_proto(r.namespace_id)?,
                upserted,
                removed_ips: r.removed_ips.into_iter().map(u32_to_ipv4).collect(),
            })
        }
        Command::Shutdown(_) => Ok(WorkerCommand::Shutdown),
    }
}

// --- WorkerEvent ---

pub fn worker_event_to_proto(event: &WorkerEvent) -> proto::WorkerEvent {
    use proto::worker_event::Event;

    let evt = match event {
        WorkerEvent::NamespaceCreated { namespace_id } => {
            Event::NamespaceCreated(proto::NamespaceCreatedEvt {
                namespace_id: Some(ns_to_proto(namespace_id)),
            })
        }
        WorkerEvent::NamespaceFailed { namespace_id, error } => {
            Event::NamespaceFailed(proto::NamespaceFailedEvt {
                namespace_id: Some(ns_to_proto(namespace_id)),
                error: error.clone(),
            })
        }
        WorkerEvent::NamespaceDestroyed { namespace_id } => {
            Event::NamespaceDestroyed(proto::NamespaceDestroyedEvt {
                namespace_id: Some(ns_to_proto(namespace_id)),
            })
        }
        WorkerEvent::PodRunning { namespace_id, pod_id } => {
            Event::PodRunning(proto::PodRunningEvt {
                namespace_id: Some(ns_to_proto(namespace_id)),
                pod_id: pod_id.0,
            })
        }
        WorkerEvent::PodExited { namespace_id, pod_id, exit_code } => {
            Event::PodExited(proto::PodExitedEvt {
                namespace_id: Some(ns_to_proto(namespace_id)),
                pod_id: pod_id.0,
                exit_code: *exit_code,
            })
        }
        WorkerEvent::PodFailed { namespace_id, pod_id, error } => {
            Event::PodFailed(proto::PodFailedEvt {
                namespace_id: Some(ns_to_proto(namespace_id)),
                pod_id: pod_id.0,
                error: error.clone(),
            })
        }
        WorkerEvent::ShuttingDown => {
            Event::ShuttingDown(proto::ShuttingDownEvt {})
        }
        WorkerEvent::PodLogStreamError { namespace_id, pod_id, container_id, phase, error } => {
            Event::PodLogStreamError(proto::PodLogStreamErrorEvt {
                namespace_id: Some(ns_to_proto(namespace_id)),
                pod_id: pod_id.0,
                container_id: container_id.clone(),
                phase: phase.clone(),
                error: error.clone(),
            })
        }
        WorkerEvent::PodSuspended { namespace_id, pod_id, artifact_id, artifact_size_bytes, pool_id } => {
            Event::PodSuspended(proto::PodSuspendedEvt {
                namespace_id: Some(ns_to_proto(namespace_id)),
                pod_id: pod_id.0,
                artifact_id: artifact_id.0.clone(),
                artifact_size_bytes: *artifact_size_bytes,
                pool_id: pool_id.0.clone(),
            })
        }
        WorkerEvent::PodSuspendFailed { namespace_id, pod_id, error } => {
            Event::PodSuspendFailed(proto::PodSuspendFailedEvt {
                namespace_id: Some(ns_to_proto(namespace_id)),
                pod_id: pod_id.0,
                error: error.clone(),
            })
        }
        WorkerEvent::TunnelStatus { peer_worker_id, status } => {
            let status_oneof = match status {
                TunnelPeerStatus::Connected => {
                    proto::tunnel_status_evt::Status::Connected(proto::tunnel_status_evt::Connected {})
                }
                TunnelPeerStatus::Disconnected { error } => {
                    proto::tunnel_status_evt::Status::Disconnected(proto::tunnel_status_evt::Disconnected {
                        error: error.clone(),
                    })
                }
                TunnelPeerStatus::HandshakeFailed { error } => {
                    proto::tunnel_status_evt::Status::HandshakeFailed(proto::tunnel_status_evt::HandshakeFailed {
                        error: error.clone(),
                    })
                }
            };
            Event::TunnelStatus(proto::TunnelStatusEvt {
                peer_worker_id: peer_worker_id.0,
                status: Some(status_oneof),
            })
        }
        WorkerEvent::WorkerCondition { key, active, message } => {
            Event::WorkerCondition(proto::WorkerConditionEvt {
                key: key.clone(),
                active: *active,
                message: message.clone(),
            })
        }
        WorkerEvent::PoolCapacityUpdate { pools } => {
            Event::PoolCapacityUpdate(proto::PoolCapacityUpdateEvt {
                pools: pools.iter().map(pool_info_to_proto).collect(),
            })
        }
        WorkerEvent::ArtifactWriteStarted { namespace_id, artifact_id, pool_id } => {
            Event::ArtifactWriteStarted(proto::ArtifactWriteStartedEvt {
                namespace_id: Some(ns_to_proto(namespace_id)),
                artifact_id: artifact_id.0.clone(),
                pool_id: pool_id.0.clone(),
            })
        }
        WorkerEvent::ArtifactWriteCommitted { namespace_id, artifact_id, pool_id, size_bytes } => {
            Event::ArtifactWriteCommitted(proto::ArtifactWriteCommittedEvt {
                namespace_id: Some(ns_to_proto(namespace_id)),
                artifact_id: artifact_id.0.clone(),
                pool_id: pool_id.0.clone(),
                size_bytes: *size_bytes,
            })
        }
        WorkerEvent::ArtifactTransferReceived { transfer_id, source_artifact_id, source_pool_id, dest_artifact_id, dest_pool_id, size_bytes } => {
            Event::ArtifactTransferReceived(proto::ArtifactTransferReceivedEvt {
                transfer_id: *transfer_id,
                source_artifact_id: source_artifact_id.0.clone(),
                source_pool_id: source_pool_id.0.clone(),
                dest_artifact_id: dest_artifact_id.0.clone(),
                dest_pool_id: dest_pool_id.0.clone(),
                size_bytes: *size_bytes,
            })
        }
        WorkerEvent::TransferFailed { transfer_id, source_artifact_id, source_pool_id, dest_artifact_id, dest_pool_id, error } => {
            Event::TransferFailed(proto::TransferFailedEvt {
                transfer_id: *transfer_id,
                source_artifact_id: source_artifact_id.0.clone(),
                source_pool_id: source_pool_id.0.clone(),
                dest_artifact_id: dest_artifact_id.0.clone(),
                dest_pool_id: dest_pool_id.0.clone(),
                error: error.clone(),
            })
        }
        WorkerEvent::PressureUpdate { cpu, memory, io } => {
            Event::PressureUpdate(proto::PressureUpdateEvt {
                cpu: Some(psi_metrics_to_proto(cpu)),
                memory: Some(psi_metrics_to_proto(memory)),
                io: Some(psi_metrics_to_proto(io)),
            })
        }
        WorkerEvent::EndpointDemandTraffic { namespace_id, ip, service_id } => {
            Event::EndpointDemandTraffic(proto::EndpointDemandTrafficEvt {
                namespace_id: Some(ns_to_proto(namespace_id)),
                ip: ipv4_to_u32(ip),
                service_id: service_id.map(|s| s.0),
            })
        }
        WorkerEvent::EndpointDemandActive { namespace_id, ip, service_id, active } => {
            Event::EndpointDemandActive(proto::EndpointDemandActiveEvt {
                namespace_id: Some(ns_to_proto(namespace_id)),
                ip: ipv4_to_u32(ip),
                service_id: service_id.map(|s| s.0),
                active: *active,
            })
        }
        WorkerEvent::PodMemoryConstrained { namespace_id, pod_id, reason } => {
            let proto_reason = match reason {
                MemoryConstraintReason::BalloonExhausted => proto::MemoryConstraintReason::BalloonExhausted,
                MemoryConstraintReason::DeflationStalled => proto::MemoryConstraintReason::DeflationStalled,
            };
            Event::PodMemoryConstrained(proto::PodMemoryConstrainedEvt {
                namespace_id: Some(ns_to_proto(namespace_id)),
                pod_id: pod_id.0,
                reason: proto_reason.into(),
            })
        }
        WorkerEvent::PodMemoryConstraintCleared { namespace_id, pod_id } => {
            Event::PodMemoryConstraintCleared(proto::PodMemoryConstraintClearedEvt {
                namespace_id: Some(ns_to_proto(namespace_id)),
                pod_id: pod_id.0,
            })
        }
        WorkerEvent::PodOomKill { namespace_id, pod_id, count } => {
            Event::PodOomKill(proto::PodOomKillEvt {
                namespace_id: Some(ns_to_proto(namespace_id)),
                pod_id: pod_id.0,
                count: *count,
            })
        }
    };

    proto::WorkerEvent { event: Some(evt) }
}

pub fn worker_event_from_proto(msg: proto::WorkerEvent) -> anyhow::Result<WorkerEvent> {
    use proto::worker_event::Event;

    match msg.event.context("empty WorkerEvent")? {
        Event::NamespaceCreated(r) => Ok(WorkerEvent::NamespaceCreated {
            namespace_id: ns_from_proto(r.namespace_id)?,
        }),
        Event::NamespaceFailed(r) => Ok(WorkerEvent::NamespaceFailed {
            namespace_id: ns_from_proto(r.namespace_id)?,
            error: r.error,
        }),
        Event::NamespaceDestroyed(r) => Ok(WorkerEvent::NamespaceDestroyed {
            namespace_id: ns_from_proto(r.namespace_id)?,
        }),
        Event::PodRunning(r) => Ok(WorkerEvent::PodRunning {
            namespace_id: ns_from_proto(r.namespace_id)?,
            pod_id: PodId(r.pod_id),
        }),
        Event::PodExited(r) => Ok(WorkerEvent::PodExited {
            namespace_id: ns_from_proto(r.namespace_id)?,
            pod_id: PodId(r.pod_id),
            exit_code: r.exit_code,
        }),
        Event::PodFailed(r) => Ok(WorkerEvent::PodFailed {
            namespace_id: ns_from_proto(r.namespace_id)?,
            pod_id: PodId(r.pod_id),
            error: r.error,
        }),
        Event::ShuttingDown(_) => Ok(WorkerEvent::ShuttingDown),
        Event::PodLogStreamError(r) => Ok(WorkerEvent::PodLogStreamError {
            namespace_id: ns_from_proto(r.namespace_id)?,
            pod_id: PodId(r.pod_id),
            container_id: r.container_id,
            phase: r.phase,
            error: r.error,
        }),
        Event::PodSuspended(r) => Ok(WorkerEvent::PodSuspended {
            namespace_id: ns_from_proto(r.namespace_id)?,
            pod_id: PodId(r.pod_id),
            artifact_id: ArtifactId(r.artifact_id),
            artifact_size_bytes: r.artifact_size_bytes,
            pool_id: PoolId(r.pool_id),
        }),
        Event::PodSuspendFailed(r) => Ok(WorkerEvent::PodSuspendFailed {
            namespace_id: ns_from_proto(r.namespace_id)?,
            pod_id: PodId(r.pod_id),
            error: r.error,
        }),
        Event::TunnelStatus(r) => {
            let status = match r.status.context("missing tunnel status")? {
                proto::tunnel_status_evt::Status::Connected(_) => TunnelPeerStatus::Connected,
                proto::tunnel_status_evt::Status::Disconnected(d) => TunnelPeerStatus::Disconnected { error: d.error },
                proto::tunnel_status_evt::Status::HandshakeFailed(h) => TunnelPeerStatus::HandshakeFailed { error: h.error },
            };
            Ok(WorkerEvent::TunnelStatus {
                peer_worker_id: WorkerId(r.peer_worker_id),
                status,
            })
        }
        Event::WorkerCondition(r) => Ok(WorkerEvent::WorkerCondition {
            key: r.key,
            active: r.active,
            message: r.message,
        }),
        Event::PoolCapacityUpdate(r) => Ok(WorkerEvent::PoolCapacityUpdate {
            pools: r.pools.into_iter().map(pool_info_from_proto).collect(),
        }),
        Event::ArtifactWriteStarted(r) => Ok(WorkerEvent::ArtifactWriteStarted {
            namespace_id: ns_from_proto(r.namespace_id)?,
            artifact_id: ArtifactId(r.artifact_id),
            pool_id: PoolId(r.pool_id),
        }),
        Event::ArtifactWriteCommitted(r) => Ok(WorkerEvent::ArtifactWriteCommitted {
            namespace_id: ns_from_proto(r.namespace_id)?,
            artifact_id: ArtifactId(r.artifact_id),
            pool_id: PoolId(r.pool_id),
            size_bytes: r.size_bytes,
        }),
        Event::ArtifactTransferReceived(r) => Ok(WorkerEvent::ArtifactTransferReceived {
            transfer_id: r.transfer_id,
            source_artifact_id: ArtifactId(r.source_artifact_id),
            source_pool_id: PoolId(r.source_pool_id),
            dest_artifact_id: ArtifactId(r.dest_artifact_id),
            dest_pool_id: PoolId(r.dest_pool_id),
            size_bytes: r.size_bytes,
        }),
        Event::TransferFailed(r) => Ok(WorkerEvent::TransferFailed {
            transfer_id: r.transfer_id,
            source_artifact_id: ArtifactId(r.source_artifact_id),
            source_pool_id: PoolId(r.source_pool_id),
            dest_artifact_id: ArtifactId(r.dest_artifact_id),
            dest_pool_id: PoolId(r.dest_pool_id),
            error: r.error,
        }),
        Event::PressureUpdate(r) => Ok(WorkerEvent::PressureUpdate {
            cpu: psi_metrics_from_proto(r.cpu),
            memory: psi_metrics_from_proto(r.memory),
            io: psi_metrics_from_proto(r.io),
        }),
        Event::EndpointDemandTraffic(r) => Ok(WorkerEvent::EndpointDemandTraffic {
            namespace_id: ns_from_proto(r.namespace_id)?,
            ip: u32_to_ipv4(r.ip),
            service_id: r.service_id.map(ServiceId),
        }),
        Event::EndpointDemandActive(r) => Ok(WorkerEvent::EndpointDemandActive {
            namespace_id: ns_from_proto(r.namespace_id)?,
            ip: u32_to_ipv4(r.ip),
            service_id: r.service_id.map(ServiceId),
            active: r.active,
        }),
        Event::PodMemoryConstrained(r) => {
            let reason = match proto::MemoryConstraintReason::try_from(r.reason) {
                Ok(proto::MemoryConstraintReason::BalloonExhausted) => MemoryConstraintReason::BalloonExhausted,
                Ok(proto::MemoryConstraintReason::DeflationStalled) => MemoryConstraintReason::DeflationStalled,
                _ => bail!("unknown MemoryConstraintReason: {}", r.reason),
            };
            Ok(WorkerEvent::PodMemoryConstrained {
                namespace_id: ns_from_proto(r.namespace_id)?,
                pod_id: PodId(r.pod_id),
                reason,
            })
        }
        Event::PodMemoryConstraintCleared(r) => Ok(WorkerEvent::PodMemoryConstraintCleared {
            namespace_id: ns_from_proto(r.namespace_id)?,
            pod_id: PodId(r.pod_id),
        }),
        Event::PodOomKill(r) => Ok(WorkerEvent::PodOomKill {
            namespace_id: ns_from_proto(r.namespace_id)?,
            pod_id: PodId(r.pod_id),
            count: r.count,
        }),
    }
}

// --- LogStreamHeader ---

pub fn log_stream_header_to_proto(val: &LogStreamHeader) -> proto::LogStreamHeader {
    proto::LogStreamHeader {
        namespace_id: Some(ns_to_proto(&val.namespace_id)),
        pod_id: val.pod_id.0,
        container_id: val.container_id.clone(),
    }
}

pub fn log_stream_header_from_proto(val: proto::LogStreamHeader) -> anyhow::Result<LogStreamHeader> {
    Ok(LogStreamHeader {
        namespace_id: ns_from_proto(val.namespace_id)?,
        pod_id: PodId(val.pod_id),
        container_id: val.container_id,
    })
}
