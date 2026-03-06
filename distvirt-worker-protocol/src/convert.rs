//! Conversion functions between Rust types and Cap'n Proto readers/builders.
//!
//! Free functions rather than trait impls because capnp readers/builders have
//! complex lifetimes that make trait implementations awkward.

use std::net::Ipv4Addr;

use crate::types::*;
use crate::worker_protocol_capnp as schema;

// --- Scalar Helpers ---

pub fn write_ipv4(builder: &mut schema::ipv4_addr::Builder<'_>, addr: &Ipv4Addr) {
    builder.set_raw(u32::from_be_bytes(addr.octets()));
}

pub fn read_ipv4(reader: schema::ipv4_addr::Reader<'_>) -> Ipv4Addr {
    Ipv4Addr::from(reader.get_raw().to_be_bytes())
}

pub fn write_mac(builder: &mut schema::mac_addr::Builder<'_>, mac: &[u8; 6]) {
    builder.set_b0(mac[0]);
    builder.set_b1(mac[1]);
    builder.set_b2(mac[2]);
    builder.set_b3(mac[3]);
    builder.set_b4(mac[4]);
    builder.set_b5(mac[5]);
}

pub fn read_mac(reader: schema::mac_addr::Reader<'_>) -> [u8; 6] {
    [
        reader.get_b0(),
        reader.get_b1(),
        reader.get_b2(),
        reader.get_b3(),
        reader.get_b4(),
        reader.get_b5(),
    ]
}

// --- Config Structs ---

pub fn write_network_config(
    builder: &mut schema::network_config::Builder<'_>,
    val: &NetworkConfig,
) {
    write_ipv4(&mut builder.reborrow().init_subnet(), &val.subnet);
    write_ipv4(&mut builder.reborrow().init_gateway(), &val.gateway);
    builder.set_prefix_len(val.prefix_len);
    match val.segment_id {
        Some(id) => {
            builder.set_has_segment_id(true);
            builder.set_segment_id(id);
        }
        None => builder.set_has_segment_id(false),
    }
}

pub fn read_network_config(
    reader: schema::network_config::Reader<'_>,
) -> capnp::Result<NetworkConfig> {
    let segment_id = if reader.get_has_segment_id() {
        Some(reader.get_segment_id())
    } else {
        None
    };
    Ok(NetworkConfig {
        subnet: read_ipv4(reader.get_subnet()?),
        gateway: read_ipv4(reader.get_gateway()?),
        prefix_len: reader.get_prefix_len(),
        segment_id,
    })
}

pub fn write_pod_network_config(
    builder: &mut schema::pod_network_config::Builder<'_>,
    val: &PodNetworkConfig,
) {
    write_ipv4(&mut builder.reborrow().init_ip(), &val.ip);
    write_mac(&mut builder.reborrow().init_mac(), &val.mac);
    write_ipv4(&mut builder.reborrow().init_gateway(), &val.gateway);
    builder.set_netmask(&val.netmask);
}

pub fn read_pod_network_config(
    reader: schema::pod_network_config::Reader<'_>,
) -> capnp::Result<PodNetworkConfig> {
    Ok(PodNetworkConfig {
        ip: read_ipv4(reader.get_ip()?),
        mac: read_mac(reader.get_mac()?),
        gateway: read_ipv4(reader.get_gateway()?),
        netmask: reader.get_netmask()?.to_string()?,
    })
}

pub fn write_container_config(
    builder: &mut schema::container_config::Builder<'_>,
    val: &ContainerConfig,
) {
    {
        let mut ep = builder.reborrow().init_entrypoint(val.entrypoint.len() as u32);
        for (i, e) in val.entrypoint.iter().enumerate() {
            ep.set(i as u32, e);
        }
    }
    {
        let mut args = builder.reborrow().init_args(val.args.len() as u32);
        for (i, a) in val.args.iter().enumerate() {
            args.set(i as u32, a);
        }
    }
    {
        let mut env = builder.reborrow().init_env(val.env.len() as u32);
        for (i, e) in val.env.iter().enumerate() {
            env.set(i as u32, e);
        }
    }
    match &val.working_dir {
        Some(wd) => builder.set_working_dir(wd),
        None => builder.set_working_dir(""),
    }
    match val.uid {
        Some(uid) => {
            builder.set_has_uid(true);
            builder.set_uid(uid);
        }
        None => builder.set_has_uid(false),
    }
    match val.gid {
        Some(gid) => {
            builder.set_has_gid(true);
            builder.set_gid(gid);
        }
        None => builder.set_has_gid(false),
    }
    match &val.hostname {
        Some(h) => builder.set_hostname(h),
        None => builder.set_hostname(""),
    }
    builder.set_capture_output(val.capture_output);
    builder.set_stdin(val.stdin);
}

pub fn read_container_config(
    reader: schema::container_config::Reader<'_>,
) -> capnp::Result<ContainerConfig> {
    let args = reader.get_args()?;
    let mut args_vec = Vec::with_capacity(args.len() as usize);
    for i in 0..args.len() {
        args_vec.push(args.get(i)?.to_string()?);
    }
    let env = reader.get_env()?;
    let mut env_vec = Vec::with_capacity(env.len() as usize);
    for i in 0..env.len() {
        env_vec.push(env.get(i)?.to_string()?);
    }
    let wd = reader.get_working_dir()?.to_str()?;
    let hostname = reader.get_hostname()?.to_str()?;
    Ok(ContainerConfig {
        entrypoint: {
            let ep = reader.get_entrypoint()?;
            let mut ep_vec = Vec::with_capacity(ep.len() as usize);
            for i in 0..ep.len() {
                ep_vec.push(ep.get(i)?.to_string()?);
            }
            ep_vec
        },
        args: args_vec,
        env: env_vec,
        working_dir: if wd.is_empty() {
            None
        } else {
            Some(wd.to_string())
        },
        uid: if reader.get_has_uid() {
            Some(reader.get_uid())
        } else {
            None
        },
        gid: if reader.get_has_gid() {
            Some(reader.get_gid())
        } else {
            None
        },
        hostname: if hostname.is_empty() {
            None
        } else {
            Some(hostname.to_string())
        },
        capture_output: reader.get_capture_output(),
        stdin: reader.get_stdin(),
    })
}

pub fn write_container_spec(
    builder: &mut schema::container_spec::Builder<'_>,
    val: &ContainerSpec,
) {
    builder.set_container_id(&val.container_id);
    builder.set_image_ref(&val.image_ref);
    write_container_config(&mut builder.reborrow().init_config(), &val.config);
}

pub fn read_container_spec(
    reader: schema::container_spec::Reader<'_>,
) -> capnp::Result<ContainerSpec> {
    Ok(ContainerSpec {
        container_id: reader.get_container_id()?.to_string()?,
        image_ref: reader.get_image_ref()?.to_string()?,
        config: read_container_config(reader.get_config()?)?,
    })
}

pub fn write_registry_entry(
    builder: &mut schema::registry_entry::Builder<'_>,
    val: &RegistryEntry,
) {
    builder.set_name(&val.name);
    write_ipv4(&mut builder.reborrow().init_ip(), &val.ip);
}

pub fn read_registry_entry(
    reader: schema::registry_entry::Reader<'_>,
) -> capnp::Result<RegistryEntry> {
    Ok(RegistryEntry {
        name: reader.get_name()?.to_string()?,
        ip: read_ipv4(reader.get_ip()?),
    })
}


pub fn write_activator_config(
    mut builder: schema::activator_config::Builder<'_>,
    val: &ActivatorConfig,
) {
    match val {
        ActivatorConfig::Tcp {
            ports,
            tcp_only,
            max_flows,
        } => {
            let mut tcp = builder.init_tcp();
            match ports {
                Some(p) => {
                    tcp.set_has_ports(true);
                    let mut ports_builder = tcp.reborrow().init_ports(p.len() as u32);
                    for (i, port) in p.iter().enumerate() {
                        ports_builder.set(i as u32, *port);
                    }
                }
                None => tcp.set_has_ports(false),
            }
            tcp.set_tcp_only(*tcp_only);
            tcp.set_max_flows(*max_flows);
        }
        ActivatorConfig::Http2 {} => {
            builder.set_http2(());
        }
    }
}

pub fn read_activator_config(
    reader: schema::activator_config::Reader<'_>,
) -> capnp::Result<ActivatorConfig> {
    match reader.which()? {
        schema::activator_config::Tcp(tcp) => {
            let ports = if tcp.get_has_ports() {
                let p = tcp.get_ports()?;
                let mut v = Vec::with_capacity(p.len() as usize);
                for i in 0..p.len() {
                    v.push(p.get(i));
                }
                Some(v)
            } else {
                None
            };
            Ok(ActivatorConfig::Tcp {
                ports,
                tcp_only: tcp.get_tcp_only(),
                max_flows: tcp.get_max_flows(),
            })
        }
        schema::activator_config::Http2(()) => Ok(ActivatorConfig::Http2 {}),
    }
}

pub fn write_service_policy(
    builder: &mut schema::service_policy::Builder<'_>,
    val: &ServicePolicy,
) {
    builder.set_buffer_frames(val.buffer_frames);
    builder.set_timeout_ms(val.timeout_ms);
    match &val.activator {
        Some(ac) => {
            builder.set_has_activator(true);
            write_activator_config(builder.reborrow().init_activator(), ac);
        }
        None => builder.set_has_activator(false),
    }
}

pub fn read_service_policy(
    reader: schema::service_policy::Reader<'_>,
) -> capnp::Result<ServicePolicy> {
    let activator = if reader.get_has_activator() {
        Some(read_activator_config(reader.get_activator()?)?)
    } else {
        None
    };
    Ok(ServicePolicy {
        buffer_frames: reader.get_buffer_frames(),
        timeout_ms: reader.get_timeout_ms(),
        activator,
    })
}


// --- Endpoint Protocol Helpers ---

pub fn write_endpoint_placement(
    builder: &mut schema::endpoint_placement::Builder<'_>,
    val: &EndpointPlacement,
) {
    builder.set_worker_id(val.worker_id.as_ref());
}

pub fn read_endpoint_placement(
    reader: schema::endpoint_placement::Reader<'_>,
) -> capnp::Result<EndpointPlacement> {
    Ok(EndpointPlacement {
        worker_id: WorkerId::from(reader.get_worker_id()?.to_str()?),
    })
}

pub fn write_endpoint_pod_backend(
    builder: &mut schema::endpoint_pod_backend::Builder<'_>,
    val: &EndpointPodBackend,
) {
    write_ipv4(&mut builder.reborrow().init_pod_ip(), &val.pod_ip);
    builder.set_ready(val.ready);
    match &val.placement {
        Some(p) => {
            builder.set_has_placement(true);
            write_endpoint_placement(&mut builder.reborrow().init_placement(), p);
        }
        None => builder.set_has_placement(false),
    }
}

pub fn read_endpoint_pod_backend(
    reader: schema::endpoint_pod_backend::Reader<'_>,
) -> capnp::Result<EndpointPodBackend> {
    let placement = if reader.get_has_placement() {
        Some(read_endpoint_placement(reader.get_placement()?)?)
    } else {
        None
    };
    Ok(EndpointPodBackend {
        pod_ip: read_ipv4(reader.get_pod_ip()?),
        placement,
        ready: reader.get_ready(),
    })
}

pub fn write_endpoint_spec(
    builder: schema::endpoint_spec::Builder<'_>,
    val: &EndpointSpec,
) {
    let mut b = builder;
    write_ipv4(&mut b.reborrow().init_ip(), &val.ip);
    match &val.kind {
        EndpointKind::Service { service_id, policy, backend } => {
            let mut svc = b.init_service();
            svc.set_service_id(service_id.as_ref());
            write_service_policy(&mut svc.reborrow().init_policy(), policy);
            match backend {
                Some(be) => {
                    svc.set_has_backend(true);
                    write_endpoint_pod_backend(&mut svc.reborrow().init_backend(), be);
                }
                None => svc.set_has_backend(false),
            }
        }
        EndpointKind::Pod { placement } => {
            let mut pod = b.init_pod();
            match placement {
                Some(p) => {
                    pod.set_has_placement(true);
                    write_endpoint_placement(&mut pod.reborrow().init_placement(), p);
                }
                None => pod.set_has_placement(false),
            }
        }
        EndpointKind::WireGuardPeer { placement } => {
            let mut wg = b.init_wire_guard_peer();
            match placement {
                Some(p) => {
                    wg.set_has_placement(true);
                    write_endpoint_placement(&mut wg.reborrow().init_placement(), p);
                }
                None => wg.set_has_placement(false),
            }
        }
    }
}

pub fn read_endpoint_spec(
    reader: schema::endpoint_spec::Reader<'_>,
) -> capnp::Result<EndpointSpec> {
    let ip = read_ipv4(reader.get_ip()?);
    let kind = match reader.which()? {
        schema::endpoint_spec::Service(svc) => {
            let backend = if svc.get_has_backend() {
                Some(read_endpoint_pod_backend(svc.get_backend()?)?)
            } else {
                None
            };
            EndpointKind::Service {
                service_id: ServiceId::from(svc.get_service_id()?.to_str()?),
                policy: read_service_policy(svc.get_policy()?)?,
                backend,
            }
        }
        schema::endpoint_spec::Pod(pod) => {
            let placement = if pod.get_has_placement() {
                Some(read_endpoint_placement(pod.get_placement()?)?)
            } else {
                None
            };
            EndpointKind::Pod { placement }
        }
        schema::endpoint_spec::WireGuardPeer(wg) => {
            let placement = if wg.get_has_placement() {
                Some(read_endpoint_placement(wg.get_placement()?)?)
            } else {
                None
            };
            EndpointKind::WireGuardPeer { placement }
        }
    };
    Ok(EndpointSpec { ip, kind })
}

fn write_backend_need(val: &BackendNeed) -> schema::BackendNeed {
    match val {
        BackendNeed::None => schema::BackendNeed::None,
        BackendNeed::Traffic => schema::BackendNeed::Traffic,
        BackendNeed::Active => schema::BackendNeed::Active,
    }
}

fn read_backend_need(val: schema::BackendNeed) -> capnp::Result<BackendNeed> {
    match val {
        schema::BackendNeed::None => Ok(BackendNeed::None),
        schema::BackendNeed::Traffic => Ok(BackendNeed::Traffic),
        schema::BackendNeed::Active => Ok(BackendNeed::Active),
    }
}

// --- Handshake Messages ---

pub fn write_worker_hello(
    builder: &mut schema::worker_hello::Builder<'_>,
    val: &WorkerHello,
) {
    builder.set_auth_token(&val.auth_token);
    let mut caps = builder.reborrow().init_capabilities();
    write_worker_capabilities(&mut caps, &val.capabilities);
}

pub fn read_worker_hello(
    reader: schema::worker_hello::Reader<'_>,
) -> capnp::Result<WorkerHello> {
    Ok(WorkerHello {
        auth_token: reader.get_auth_token()?.to_string()?,
        capabilities: read_worker_capabilities(reader.get_capabilities()?)?,
    })
}

pub fn write_worker_capabilities(
    builder: &mut schema::worker_capabilities::Builder<'_>,
    val: &WorkerCapabilities,
) {
    builder.set_has_kvm(val.has_kvm);
    builder.set_has_containerd(val.has_containerd);
    let mut adapters = builder
        .reborrow()
        .init_available_adapters(val.available_adapters.len() as u32);
    for (i, a) in val.available_adapters.iter().enumerate() {
        adapters.set(i as u32, a);
    }
    builder.set_max_pods(val.max_pods);
    builder.set_available_memory_mb(val.available_memory_mb);
    builder.set_public_endpoint(&val.public_endpoint);
    let mut pools = builder.reborrow().init_pools(val.pools.len() as u32);
    for (i, pool) in val.pools.iter().enumerate() {
        let mut p = pools.reborrow().get(i as u32);
        p.set_pool_id(pool.pool_id.as_ref());
        p.set_path(&pool.path);
        p.set_capacity_bytes(pool.capacity_bytes);
        p.set_available_bytes(pool.available_bytes);
    }
}

pub fn read_worker_capabilities(
    reader: schema::worker_capabilities::Reader<'_>,
) -> capnp::Result<WorkerCapabilities> {
    let adapters = reader.get_available_adapters()?;
    let mut v = Vec::with_capacity(adapters.len() as usize);
    for i in 0..adapters.len() {
        v.push(adapters.get(i)?.to_string()?);
    }
    let pools_list = reader.get_pools()?;
    let mut pools = Vec::with_capacity(pools_list.len() as usize);
    for i in 0..pools_list.len() {
        let p = pools_list.get(i);
        pools.push(PoolInfo {
            pool_id: PoolId::from(p.get_pool_id()?.to_str()?),
            path: p.get_path()?.to_string()?,
            capacity_bytes: p.get_capacity_bytes(),
            available_bytes: p.get_available_bytes(),
        });
    }
    Ok(WorkerCapabilities {
        has_kvm: reader.get_has_kvm(),
        has_containerd: reader.get_has_containerd(),
        available_adapters: v,
        max_pods: reader.get_max_pods(),
        available_memory_mb: reader.get_available_memory_mb(),
        public_endpoint: reader.get_public_endpoint()?.to_string()?,
        pools,
    })
}

pub fn write_worker_ready(
    builder: &mut schema::worker_ready::Builder<'_>,
    val: &WorkerReady,
) {
    if let Some(port) = val.tunnel_listen_port {
        builder.set_has_tunnel_listen_port(true);
        builder.set_tunnel_listen_port(port);
    }
    if let Some(ref key) = val.tunnel_public_key {
        builder.set_has_tunnel_public_key(true);
        builder.set_tunnel_public_key(key);
    }
    if let Some(port) = val.transfer_listen_port {
        builder.set_has_transfer_listen_port(true);
        builder.set_transfer_listen_port(port);
    }
}

pub fn read_worker_ready(
    reader: schema::worker_ready::Reader<'_>,
) -> WorkerReady {
    let tunnel_listen_port = if reader.get_has_tunnel_listen_port() {
        Some(reader.get_tunnel_listen_port())
    } else {
        None
    };
    let tunnel_public_key = if reader.get_has_tunnel_public_key() {
        reader
            .get_tunnel_public_key()
            .ok()
            .and_then(|k| <[u8; 32]>::try_from(k).ok())
    } else {
        None
    };
    let transfer_listen_port = if reader.get_has_transfer_listen_port() {
        Some(reader.get_transfer_listen_port())
    } else {
        None
    };
    WorkerReady {
        tunnel_listen_port,
        tunnel_public_key,
        transfer_listen_port,
    }
}

pub fn write_worker_accepted(
    builder: &mut schema::worker_accepted::Builder<'_>,
    val: &WorkerAccepted,
) {
    builder.set_worker_id(val.worker_id.as_ref());
    builder.set_tunnel_encrypted(val.tunnel_encrypted);
    let mut adapters = builder
        .reborrow()
        .init_adapters(val.adapters.len() as u32);
    for (i, ac) in val.adapters.iter().enumerate() {
        write_adapter_config(adapters.reborrow().get(i as u32), ac);
    }
    let mut pools = builder.reborrow().init_pools(val.pools.len() as u32);
    for (i, pool) in val.pools.iter().enumerate() {
        let mut p = pools.reborrow().get(i as u32);
        p.set_pool_id(pool.pool_id.as_ref());
        p.set_path(&pool.path);
        p.set_capacity_bytes(pool.capacity_bytes);
        p.set_available_bytes(pool.available_bytes);
    }
}

pub fn read_worker_accepted(
    reader: schema::worker_accepted::Reader<'_>,
) -> capnp::Result<WorkerAccepted> {
    let adapters = reader.get_adapters()?;
    let mut v = Vec::with_capacity(adapters.len() as usize);
    for i in 0..adapters.len() {
        v.push(read_adapter_config(adapters.get(i))?);
    }
    let pools_list = reader.get_pools()?;
    let mut pools = Vec::with_capacity(pools_list.len() as usize);
    for i in 0..pools_list.len() {
        let p = pools_list.get(i);
        pools.push(PoolInfo {
            pool_id: PoolId::from(p.get_pool_id()?.to_str()?),
            path: p.get_path()?.to_string()?,
            capacity_bytes: p.get_capacity_bytes(),
            available_bytes: p.get_available_bytes(),
        });
    }
    Ok(WorkerAccepted {
        worker_id: WorkerId::from(reader.get_worker_id()?.to_str()?),
        adapters: v,
        tunnel_encrypted: reader.get_tunnel_encrypted(),
        pools,
    })
}

pub fn write_adapter_config(
    builder: schema::adapter_config::Builder<'_>,
    val: &AdapterConfig,
) {
    match val {
        AdapterConfig::WireGuard {
            listen_port,
            private_key,
        } => {
            let mut wg = builder.init_wireguard();
            wg.set_listen_port(*listen_port);
            wg.set_private_key(private_key);
        }
        AdapterConfig::ReverseProxy {
            listen_port,
            tls_cert,
            tls_key,
        } => {
            let mut rp = builder.init_reverse_proxy();
            rp.set_listen_port(*listen_port);
            rp.set_tls_cert(tls_cert);
            rp.set_tls_key(tls_key);
        }
        AdapterConfig::OsRouting { interface } => {
            let mut os = builder.init_os_routing();
            os.set_interface(interface);
        }
    }
}

pub fn read_adapter_config(
    reader: schema::adapter_config::Reader<'_>,
) -> capnp::Result<AdapterConfig> {
    match reader.which()? {
        schema::adapter_config::Wireguard(wg) => Ok(AdapterConfig::WireGuard {
            listen_port: wg.get_listen_port(),
            private_key: wg.get_private_key()?.to_vec(),
        }),
        schema::adapter_config::ReverseProxy(rp) => Ok(AdapterConfig::ReverseProxy {
            listen_port: rp.get_listen_port(),
            tls_cert: rp.get_tls_cert()?.to_vec(),
            tls_key: rp.get_tls_key()?.to_vec(),
        }),
        schema::adapter_config::OsRouting(os) => Ok(AdapterConfig::OsRouting {
            interface: os.get_interface()?.to_string()?,
        }),
    }
}

// --- WorkerPeerInfo ---

pub fn write_worker_peer_info(
    builder: &mut schema::worker_peer_info::Builder<'_>,
    val: &WorkerPeerInfo,
) {
    builder.set_worker_id(val.worker_id.as_ref());
    builder.set_endpoint(&val.endpoint);
    builder.set_public_key(&val.public_key);
    let mut segments = builder.reborrow().init_segments(val.segments.len() as u32);
    for (i, seg) in val.segments.iter().enumerate() {
        segments.set(i as u32, *seg);
    }
}

pub fn read_worker_peer_info(
    reader: schema::worker_peer_info::Reader<'_>,
) -> capnp::Result<WorkerPeerInfo> {
    let key_data = reader.get_public_key()?;
    let mut public_key = [0u8; 32];
    if key_data.len() >= 32 {
        public_key.copy_from_slice(&key_data[..32]);
    }
    let segments_list = reader.get_segments()?;
    let mut segments = Vec::with_capacity(segments_list.len() as usize);
    for i in 0..segments_list.len() {
        segments.push(segments_list.get(i));
    }
    Ok(WorkerPeerInfo {
        worker_id: WorkerId::from(reader.get_worker_id()?.to_str()?),
        endpoint: reader.get_endpoint()?.to_string()?,
        public_key,
        segments,
    })
}

// --- WorkerCommand ---

pub fn write_worker_command(
    mut builder: schema::worker_command::Builder<'_>,
    cmd: &WorkerCommand,
) {
    match cmd {
        WorkerCommand::CreateNamespace {
            namespace_id,
            network,
        } => {
            let mut b = builder.init_create_namespace();
            b.set_namespace_id(namespace_id.as_ref());
            write_network_config(&mut b.reborrow().init_network(), network);
        }
        WorkerCommand::DestroyNamespace { namespace_id } => {
            let mut b = builder.init_destroy_namespace();
            b.set_namespace_id(namespace_id.as_ref());
        }
        WorkerCommand::RegistrySync {
            namespace_id,
            entries,
        } => {
            let mut b = builder.init_registry_sync();
            b.set_namespace_id(namespace_id.as_ref());
            let mut list = b.reborrow().init_entries(entries.len() as u32);
            for (i, entry) in entries.iter().enumerate() {
                write_registry_entry(&mut list.reborrow().get(i as u32), entry);
            }
        }
        WorkerCommand::RegistryUpdate {
            namespace_id,
            added,
            removed,
        } => {
            let mut b = builder.init_registry_update();
            b.set_namespace_id(namespace_id.as_ref());
            {
                let mut list = b.reborrow().init_added(added.len() as u32);
                for (i, entry) in added.iter().enumerate() {
                    write_registry_entry(&mut list.reborrow().get(i as u32), entry);
                }
            }
            {
                let mut list = b.reborrow().init_removed(removed.len() as u32);
                for (i, name) in removed.iter().enumerate() {
                    list.set(i as u32, name);
                }
            }
        }
        WorkerCommand::LaunchPod {
            namespace_id,
            pod_id,
            network,
            containers,
        } => {
            let mut b = builder.init_launch_pod();
            b.set_namespace_id(namespace_id.as_ref());
            b.set_pod_id(pod_id.as_ref());
            write_pod_network_config(&mut b.reborrow().init_network(), network);
            let mut list = b.reborrow().init_containers(containers.len() as u32);
            for (i, spec) in containers.iter().enumerate() {
                write_container_spec(&mut list.reborrow().get(i as u32), spec);
            }
        }
        WorkerCommand::StopPod {
            namespace_id,
            pod_id,
            graceful,
        } => {
            let mut b = builder.init_stop_pod();
            b.set_namespace_id(namespace_id.as_ref());
            b.set_pod_id(pod_id.as_ref());
            b.set_graceful(*graceful);
        }
        WorkerCommand::AddWireGuardPeer {
            namespace_id,
            peer_public_key,
            peer_ip,
            preshared_key,
        } => {
            let mut b = builder.init_add_wire_guard_peer();
            b.set_namespace_id(namespace_id.as_ref());
            b.set_peer_public_key(peer_public_key);
            write_ipv4(&mut b.reborrow().init_peer_ip(), peer_ip);
            match preshared_key {
                Some(psk) => {
                    b.set_has_preshared_key(true);
                    b.set_preshared_key(psk);
                }
                None => b.set_has_preshared_key(false),
            }
        }
        WorkerCommand::RemoveWireGuardPeer { peer_public_key } => {
            let mut b = builder.init_remove_wire_guard_peer();
            b.set_peer_public_key(peer_public_key);
        }
        WorkerCommand::SuspendPod {
            namespace_id,
            pod_id,
            artifact_id,
            pool_id,
        } => {
            let mut b = builder.init_suspend_pod();
            b.set_namespace_id(namespace_id.as_ref());
            b.set_pod_id(pod_id.as_ref());
            b.set_snapshot_id(artifact_id.as_ref());
            b.set_pool_id(pool_id.as_ref());
        }
        WorkerCommand::ResumePod {
            namespace_id,
            pod_id,
            artifact_id,
            network,
            pool_id,
        } => {
            let mut b = builder.init_resume_pod();
            b.set_namespace_id(namespace_id.as_ref());
            b.set_pod_id(pod_id.as_ref());
            b.set_snapshot_id(artifact_id.as_ref());
            write_pod_network_config(&mut b.reborrow().init_network(), network);
            b.set_pool_id(pool_id.as_ref());
        }
        WorkerCommand::DeleteArtifact { artifact_id, pool_id } => {
            let mut b = builder.init_delete_snapshot();
            b.set_snapshot_id(artifact_id.as_ref());
            b.set_pool_id(pool_id.as_ref());
        }
        WorkerCommand::WorkerRegistrySync { workers } => {
            let mut b = builder.init_worker_registry_sync();
            let mut list = b.reborrow().init_workers(workers.len() as u32);
            for (i, peer) in workers.iter().enumerate() {
                write_worker_peer_info(&mut list.reborrow().get(i as u32), peer);
            }
        }
        WorkerCommand::TransferArtifact {
            transfer_id,
            source_artifact_id,
            source_pool_id,
            dest_artifact_id,
            dest_pool_id,
            dest_endpoint,
        } => {
            let mut b = builder.init_transfer_artifact();
            b.set_transfer_id(*transfer_id);
            b.set_source_artifact_id(source_artifact_id.as_ref());
            b.set_source_pool_id(source_pool_id.as_ref());
            b.set_dest_artifact_id(dest_artifact_id.as_ref());
            b.set_dest_pool_id(dest_pool_id.as_ref());
            match dest_endpoint {
                Some(ep) => b.set_dest_endpoint(ep),
                None => b.set_dest_endpoint(""),
            }
        }
        WorkerCommand::EndpointSync {
            namespace_id,
            endpoints,
        } => {
            let mut b = builder.init_endpoint_sync();
            b.set_namespace_id(namespace_id.as_ref());
            let mut list = b.reborrow().init_endpoints(endpoints.len() as u32);
            for (i, spec) in endpoints.iter().enumerate() {
                write_endpoint_spec(list.reborrow().get(i as u32), spec);
            }
        }
        WorkerCommand::EndpointUpdate {
            namespace_id,
            upserted,
            removed_ips,
        } => {
            let mut b = builder.init_endpoint_update();
            b.set_namespace_id(namespace_id.as_ref());
            {
                let mut list = b.reborrow().init_upserted(upserted.len() as u32);
                for (i, spec) in upserted.iter().enumerate() {
                    write_endpoint_spec(list.reborrow().get(i as u32), spec);
                }
            }
            {
                let mut list = b.reborrow().init_removed_ips(removed_ips.len() as u32);
                for (i, ip) in removed_ips.iter().enumerate() {
                    write_ipv4(&mut list.reborrow().get(i as u32), ip);
                }
            }
        }
        WorkerCommand::Shutdown => {
            builder.set_shutdown(());
        }
    }
}

pub fn read_worker_command(
    reader: schema::worker_command::Reader<'_>,
) -> capnp::Result<WorkerCommand> {
    use schema::worker_command::*;
    match reader.which()? {
        CreateNamespace(r) => {
            let r = r?;
            Ok(WorkerCommand::CreateNamespace {
                namespace_id: NamespaceId::from(r.get_namespace_id()?.to_str()?),
                network: read_network_config(r.get_network()?)?,
            })
        }
        DestroyNamespace(r) => {
            let r = r?;
            Ok(WorkerCommand::DestroyNamespace {
                namespace_id: NamespaceId::from(r.get_namespace_id()?.to_str()?),
            })
        }
        RegistrySync(r) => {
            let r = r?;
            let entries = r.get_entries()?;
            let mut v = Vec::with_capacity(entries.len() as usize);
            for i in 0..entries.len() {
                v.push(read_registry_entry(entries.get(i))?);
            }
            Ok(WorkerCommand::RegistrySync {
                namespace_id: NamespaceId::from(r.get_namespace_id()?.to_str()?),
                entries: v,
            })
        }
        RegistryUpdate(r) => {
            let r = r?;
            let added_list = r.get_added()?;
            let mut added = Vec::with_capacity(added_list.len() as usize);
            for i in 0..added_list.len() {
                added.push(read_registry_entry(added_list.get(i))?);
            }
            let removed_list = r.get_removed()?;
            let mut removed = Vec::with_capacity(removed_list.len() as usize);
            for i in 0..removed_list.len() {
                removed.push(removed_list.get(i)?.to_string()?);
            }
            Ok(WorkerCommand::RegistryUpdate {
                namespace_id: NamespaceId::from(r.get_namespace_id()?.to_str()?),
                added,
                removed,
            })
        }
        LaunchPod(r) => {
            let r = r?;
            let specs = r.get_containers()?;
            let mut containers = Vec::with_capacity(specs.len() as usize);
            for i in 0..specs.len() {
                containers.push(read_container_spec(specs.get(i))?);
            }
            Ok(WorkerCommand::LaunchPod {
                namespace_id: NamespaceId::from(r.get_namespace_id()?.to_str()?),
                pod_id: PodId::from(r.get_pod_id()?.to_str()?),
                network: read_pod_network_config(r.get_network()?)?,
                containers,
            })
        }
        StopPod(r) => {
            let r = r?;
            Ok(WorkerCommand::StopPod {
                namespace_id: NamespaceId::from(r.get_namespace_id()?.to_str()?),
                pod_id: PodId::from(r.get_pod_id()?.to_str()?),
                graceful: r.get_graceful(),
            })
        }
        // Deprecated command variants — removed from Rust types but still in Cap'n Proto schema.
        FabricRouteSync(_) | FabricRouteUpdate(_) | CreateService(_)
        | UpdateServiceBackend(_) | ServiceReady(_) | DestroyService(_) => {
            Err(capnp::Error::failed("received deprecated command variant".into()))
        }
        AddWireGuardPeer(r) => {
            let r = r?;
            let pubkey_data = r.get_peer_public_key()?;
            let mut peer_public_key = [0u8; 32];
            if pubkey_data.len() >= 32 {
                peer_public_key.copy_from_slice(&pubkey_data[..32]);
            }
            let preshared_key = if r.get_has_preshared_key() {
                let psk_data = r.get_preshared_key()?;
                let mut psk = [0u8; 32];
                if psk_data.len() >= 32 {
                    psk.copy_from_slice(&psk_data[..32]);
                }
                Some(psk)
            } else {
                None
            };
            Ok(WorkerCommand::AddWireGuardPeer {
                namespace_id: NamespaceId::from(r.get_namespace_id()?.to_str()?),
                peer_public_key,
                peer_ip: read_ipv4(r.get_peer_ip()?),
                preshared_key,
            })
        }
        RemoveWireGuardPeer(r) => {
            let r = r?;
            let pubkey_data = r.get_peer_public_key()?;
            let mut peer_public_key = [0u8; 32];
            if pubkey_data.len() >= 32 {
                peer_public_key.copy_from_slice(&pubkey_data[..32]);
            }
            Ok(WorkerCommand::RemoveWireGuardPeer { peer_public_key })
        }
        SuspendPod(r) => {
            let r = r?;
            Ok(WorkerCommand::SuspendPod {
                namespace_id: NamespaceId::from(r.get_namespace_id()?.to_str()?),
                pod_id: PodId::from(r.get_pod_id()?.to_str()?),
                artifact_id: ArtifactId::from(r.get_snapshot_id()?.to_str()?),
                pool_id: PoolId::from(r.get_pool_id()?.to_str()?),
            })
        }
        ResumePod(r) => {
            let r = r?;
            Ok(WorkerCommand::ResumePod {
                namespace_id: NamespaceId::from(r.get_namespace_id()?.to_str()?),
                pod_id: PodId::from(r.get_pod_id()?.to_str()?),
                artifact_id: ArtifactId::from(r.get_snapshot_id()?.to_str()?),
                network: read_pod_network_config(r.get_network()?)?,
                pool_id: PoolId::from(r.get_pool_id()?.to_str()?),
            })
        }
        DeleteSnapshot(r) => {
            let r = r?;
            Ok(WorkerCommand::DeleteArtifact {
                artifact_id: ArtifactId::from(r.get_snapshot_id()?.to_str()?),
                pool_id: PoolId::from(r.get_pool_id()?.to_str()?),
            })
        }
        WorkerRegistrySync(r) => {
            let r = r?;
            let workers_list = r.get_workers()?;
            let mut workers = Vec::with_capacity(workers_list.len() as usize);
            for i in 0..workers_list.len() {
                workers.push(read_worker_peer_info(workers_list.get(i))?);
            }
            Ok(WorkerCommand::WorkerRegistrySync { workers })
        }
        TransferArtifact(r) => {
            let r = r?;
            let ep = r.get_dest_endpoint()?.to_str()?;
            let dest_endpoint = if ep.is_empty() { None } else { Some(ep.to_string()) };
            Ok(WorkerCommand::TransferArtifact {
                transfer_id: r.get_transfer_id(),
                source_artifact_id: ArtifactId::from(r.get_source_artifact_id()?.to_str()?),
                source_pool_id: PoolId::from(r.get_source_pool_id()?.to_str()?),
                dest_artifact_id: ArtifactId::from(r.get_dest_artifact_id()?.to_str()?),
                dest_pool_id: PoolId::from(r.get_dest_pool_id()?.to_str()?),
                dest_endpoint,
            })
        }
        EndpointSync(r) => {
            let r = r?;
            let eps = r.get_endpoints()?;
            let mut endpoints = Vec::with_capacity(eps.len() as usize);
            for i in 0..eps.len() {
                endpoints.push(read_endpoint_spec(eps.get(i))?);
            }
            Ok(WorkerCommand::EndpointSync {
                namespace_id: NamespaceId::from(r.get_namespace_id()?.to_str()?),
                endpoints,
            })
        }
        EndpointUpdate(r) => {
            let r = r?;
            let ups = r.get_upserted()?;
            let mut upserted = Vec::with_capacity(ups.len() as usize);
            for i in 0..ups.len() {
                upserted.push(read_endpoint_spec(ups.get(i))?);
            }
            let ips_list = r.get_removed_ips()?;
            let mut removed_ips = Vec::with_capacity(ips_list.len() as usize);
            for i in 0..ips_list.len() {
                removed_ips.push(read_ipv4(ips_list.get(i)));
            }
            Ok(WorkerCommand::EndpointUpdate {
                namespace_id: NamespaceId::from(r.get_namespace_id()?.to_str()?),
                upserted,
                removed_ips,
            })
        }
        Shutdown(()) => Ok(WorkerCommand::Shutdown),
    }
}

// --- WorkerEvent ---

pub fn write_worker_event(
    mut builder: schema::worker_event::Builder<'_>,
    event: &WorkerEvent,
) {
    match event {
        WorkerEvent::NamespaceCreated { namespace_id } => {
            let mut b = builder.init_namespace_created();
            b.set_namespace_id(namespace_id.as_ref());
        }
        WorkerEvent::NamespaceFailed {
            namespace_id,
            error,
        } => {
            let mut b = builder.init_namespace_failed();
            b.set_namespace_id(namespace_id.as_ref());
            b.set_error(error);
        }
        WorkerEvent::NamespaceDestroyed { namespace_id } => {
            let mut b = builder.init_namespace_destroyed();
            b.set_namespace_id(namespace_id.as_ref());
        }
        WorkerEvent::PodRunning {
            namespace_id,
            pod_id,
        } => {
            let mut b = builder.init_pod_running();
            b.set_namespace_id(namespace_id.as_ref());
            b.set_pod_id(pod_id.as_ref());
        }
        WorkerEvent::PodExited {
            namespace_id,
            pod_id,
            exit_code,
        } => {
            let mut b = builder.init_pod_exited();
            b.set_namespace_id(namespace_id.as_ref());
            b.set_pod_id(pod_id.as_ref());
            b.set_exit_code(*exit_code);
        }
        WorkerEvent::PodFailed {
            namespace_id,
            pod_id,
            error,
        } => {
            let mut b = builder.init_pod_failed();
            b.set_namespace_id(namespace_id.as_ref());
            b.set_pod_id(pod_id.as_ref());
            b.set_error(error);
        }
        WorkerEvent::ShuttingDown => {
            builder.set_shutting_down(());
        }
        WorkerEvent::PodLogStreamError {
            namespace_id,
            pod_id,
            container_id,
            phase,
            error,
        } => {
            let mut b = builder.init_pod_log_stream_error();
            b.set_namespace_id(namespace_id.as_ref());
            b.set_pod_id(pod_id.as_ref());
            b.set_container_id(container_id);
            b.set_phase(phase);
            b.set_error(error);
        }
        WorkerEvent::ServiceBackendNeed {
            namespace_id,
            service_id,
            need,
        } => {
            let mut b = builder.init_service_backend_need();
            b.set_namespace_id(namespace_id.as_ref());
            b.set_service_id(service_id.as_ref());
            b.set_need(write_backend_need(need));
        }
        WorkerEvent::PodSuspended {
            namespace_id,
            pod_id,
            artifact_id,
            artifact_size_bytes,
            pool_id,
        } => {
            let mut b = builder.init_pod_suspended();
            b.set_namespace_id(namespace_id.as_ref());
            b.set_pod_id(pod_id.as_ref());
            b.set_snapshot_id(artifact_id.as_ref());
            b.set_snapshot_size_bytes(*artifact_size_bytes);
            b.set_pool_id(pool_id.as_ref());
        }
        WorkerEvent::PodSuspendFailed {
            namespace_id,
            pod_id,
            error,
        } => {
            let mut b = builder.init_pod_suspend_failed();
            b.set_namespace_id(namespace_id.as_ref());
            b.set_pod_id(pod_id.as_ref());
            b.set_error(error);
        }
        WorkerEvent::TunnelStatus {
            peer_worker_id,
            status,
        } => {
            let mut b = builder.init_tunnel_status();
            b.set_peer_worker_id(peer_worker_id.as_ref());
            match status {
                TunnelPeerStatus::Connected => {
                    b.set_connected(());
                }
                TunnelPeerStatus::Disconnected { error } => {
                    b.init_disconnected().set_error(error);
                }
                TunnelPeerStatus::HandshakeFailed { error } => {
                    b.init_handshake_failed().set_error(error);
                }
            }
        }
        WorkerEvent::WorkerCondition {
            key,
            active,
            message,
        } => {
            let mut b = builder.init_worker_condition();
            b.set_key(key);
            b.set_active(*active);
            b.set_message(message);
        }
        WorkerEvent::PoolCapacityUpdate { pools } => {
            let mut b = builder.init_pool_capacity_update();
            let mut list = b.reborrow().init_pools(pools.len() as u32);
            for (i, pool) in pools.iter().enumerate() {
                let mut p = list.reborrow().get(i as u32);
                p.set_pool_id(pool.pool_id.as_ref());
                p.set_path(&pool.path);
                p.set_capacity_bytes(pool.capacity_bytes);
                p.set_available_bytes(pool.available_bytes);
            }
        }
        WorkerEvent::ArtifactWriteStarted {
            namespace_id,
            artifact_id,
            pool_id,
        } => {
            let mut b = builder.init_artifact_write_started();
            b.set_namespace_id(namespace_id.as_ref());
            b.set_artifact_id(artifact_id.as_ref());
            b.set_pool_id(pool_id.as_ref());
        }
        WorkerEvent::ArtifactWriteCommitted {
            namespace_id,
            artifact_id,
            pool_id,
            size_bytes,
        } => {
            let mut b = builder.init_artifact_write_committed();
            b.set_namespace_id(namespace_id.as_ref());
            b.set_artifact_id(artifact_id.as_ref());
            b.set_pool_id(pool_id.as_ref());
            b.set_size_bytes(*size_bytes);
        }
        WorkerEvent::ArtifactTransferReceived {
            transfer_id,
            source_artifact_id,
            source_pool_id,
            dest_artifact_id,
            dest_pool_id,
            size_bytes,
        } => {
            let mut b = builder.init_artifact_transfer_received();
            b.set_transfer_id(*transfer_id);
            b.set_source_artifact_id(source_artifact_id.as_ref());
            b.set_source_pool_id(source_pool_id.as_ref());
            b.set_dest_artifact_id(dest_artifact_id.as_ref());
            b.set_dest_pool_id(dest_pool_id.as_ref());
            b.set_size_bytes(*size_bytes);
        }
        WorkerEvent::TransferFailed {
            transfer_id,
            source_artifact_id,
            source_pool_id,
            dest_artifact_id,
            dest_pool_id,
            error,
        } => {
            let mut b = builder.init_transfer_failed();
            b.set_transfer_id(*transfer_id);
            b.set_source_artifact_id(source_artifact_id.as_ref());
            b.set_source_pool_id(source_pool_id.as_ref());
            b.set_dest_artifact_id(dest_artifact_id.as_ref());
            b.set_dest_pool_id(dest_pool_id.as_ref());
            b.set_error(error);
        }
        WorkerEvent::PressureUpdate { cpu, memory, io } => {
            let mut b = builder.init_pressure_update();
            write_psi_metrics(b.reborrow().init_cpu(), cpu);
            write_psi_metrics(b.reborrow().init_memory(), memory);
            write_psi_metrics(b.reborrow().init_io(), io);
        }
        WorkerEvent::EndpointActivation {
            namespace_id,
            ip,
            service_id,
        } => {
            let mut b = builder.init_endpoint_activation();
            b.set_namespace_id(namespace_id.as_ref());
            write_ipv4(&mut b.reborrow().init_ip(), ip);
            match service_id {
                Some(sid) => {
                    b.set_has_service_id(true);
                    b.set_service_id(sid.as_ref());
                }
                None => b.set_has_service_id(false),
            }
        }
        WorkerEvent::EndpointFlowStatus {
            namespace_id,
            ip,
            has_active_flows,
        } => {
            let mut b = builder.init_endpoint_flow_status();
            b.set_namespace_id(namespace_id.as_ref());
            write_ipv4(&mut b.reborrow().init_ip(), ip);
            b.set_has_active_flows(*has_active_flows);
        }
    }
}

fn write_psi_metrics(mut b: schema::psi_metrics::Builder<'_>, m: &PsiMetrics) {
    b.set_some_avg10(m.some_avg10);
    b.set_some_avg60(m.some_avg60);
    b.set_full_avg10(m.full_avg10);
    b.set_full_avg60(m.full_avg60);
}

fn read_psi_metrics(r: schema::psi_metrics::Reader<'_>) -> PsiMetrics {
    PsiMetrics {
        some_avg10: r.get_some_avg10(),
        some_avg60: r.get_some_avg60(),
        full_avg10: r.get_full_avg10(),
        full_avg60: r.get_full_avg60(),
    }
}

pub fn read_worker_event(
    reader: schema::worker_event::Reader<'_>,
) -> capnp::Result<WorkerEvent> {
    use schema::worker_event::*;
    match reader.which()? {
        NamespaceCreated(r) => {
            let r = r?;
            Ok(WorkerEvent::NamespaceCreated {
                namespace_id: NamespaceId::from(r.get_namespace_id()?.to_str()?),
            })
        }
        NamespaceFailed(r) => {
            let r = r?;
            Ok(WorkerEvent::NamespaceFailed {
                namespace_id: NamespaceId::from(r.get_namespace_id()?.to_str()?),
                error: r.get_error()?.to_string()?,
            })
        }
        NamespaceDestroyed(r) => {
            let r = r?;
            Ok(WorkerEvent::NamespaceDestroyed {
                namespace_id: NamespaceId::from(r.get_namespace_id()?.to_str()?),
            })
        }
        PodRunning(r) => {
            let r = r?;
            Ok(WorkerEvent::PodRunning {
                namespace_id: NamespaceId::from(r.get_namespace_id()?.to_str()?),
                pod_id: PodId::from(r.get_pod_id()?.to_str()?),
            })
        }
        PodExited(r) => {
            let r = r?;
            Ok(WorkerEvent::PodExited {
                namespace_id: NamespaceId::from(r.get_namespace_id()?.to_str()?),
                pod_id: PodId::from(r.get_pod_id()?.to_str()?),
                exit_code: r.get_exit_code(),
            })
        }
        PodFailed(r) => {
            let r = r?;
            Ok(WorkerEvent::PodFailed {
                namespace_id: NamespaceId::from(r.get_namespace_id()?.to_str()?),
                pod_id: PodId::from(r.get_pod_id()?.to_str()?),
                error: r.get_error()?.to_string()?,
            })
        }
        ShuttingDown(()) => Ok(WorkerEvent::ShuttingDown),
        PodLogStreamError(r) => {
            let r = r?;
            Ok(WorkerEvent::PodLogStreamError {
                namespace_id: NamespaceId::from(r.get_namespace_id()?.to_str()?),
                pod_id: PodId::from(r.get_pod_id()?.to_str()?),
                container_id: r.get_container_id()?.to_string()?,
                phase: r.get_phase()?.to_string()?,
                error: r.get_error()?.to_string()?,
            })
        }
        // Deprecated event variant — removed from Rust types but still in Cap'n Proto schema.
        ServiceActivation(_) => {
            Err(capnp::Error::failed("received deprecated event variant: ServiceActivation".into()))
        }
        ServiceBackendNeed(r) => {
            let r = r?;
            Ok(WorkerEvent::ServiceBackendNeed {
                namespace_id: NamespaceId::from(r.get_namespace_id()?.to_str()?),
                service_id: ServiceId::from(r.get_service_id()?.to_str()?),
                need: read_backend_need(r.get_need()?)?,
            })
        }
        // Deprecated event variant — removed from Rust types but still in Cap'n Proto schema.
        FabricRouteMiss(_) => {
            Err(capnp::Error::failed("received deprecated event variant: FabricRouteMiss".into()))
        }
        PodSuspended(r) => {
            let r = r?;
            Ok(WorkerEvent::PodSuspended {
                namespace_id: NamespaceId::from(r.get_namespace_id()?.to_str()?),
                pod_id: PodId::from(r.get_pod_id()?.to_str()?),
                artifact_id: ArtifactId::from(r.get_snapshot_id()?.to_str()?),
                artifact_size_bytes: r.get_snapshot_size_bytes(),
                pool_id: PoolId::from(r.get_pool_id()?.to_str()?),
            })
        }
        PodSuspendFailed(r) => {
            let r = r?;
            Ok(WorkerEvent::PodSuspendFailed {
                namespace_id: NamespaceId::from(r.get_namespace_id()?.to_str()?),
                pod_id: PodId::from(r.get_pod_id()?.to_str()?),
                error: r.get_error()?.to_string()?,
            })
        }
        TunnelStatus(r) => {
            let r = r?;
            let peer_worker_id = WorkerId::from(r.get_peer_worker_id()?.to_str()?);
            let status = match r.which()? {
                schema::tunnel_status_evt::Connected(()) => TunnelPeerStatus::Connected,
                schema::tunnel_status_evt::Disconnected(d) => TunnelPeerStatus::Disconnected {
                    error: d.get_error()?.to_string()?,
                },
                schema::tunnel_status_evt::HandshakeFailed(h) => {
                    TunnelPeerStatus::HandshakeFailed {
                        error: h.get_error()?.to_string()?,
                    }
                }
            };
            Ok(WorkerEvent::TunnelStatus {
                peer_worker_id,
                status,
            })
        }
        WorkerCondition(r) => {
            let r = r?;
            Ok(WorkerEvent::WorkerCondition {
                key: r.get_key()?.to_string()?,
                active: r.get_active(),
                message: r.get_message()?.to_string()?,
            })
        }
        PoolCapacityUpdate(r) => {
            let r = r?;
            let pools_list = r.get_pools()?;
            let mut pools = Vec::with_capacity(pools_list.len() as usize);
            for i in 0..pools_list.len() {
                let p = pools_list.get(i);
                pools.push(PoolInfo {
                    pool_id: PoolId::from(p.get_pool_id()?.to_str()?),
                    path: p.get_path()?.to_string()?,
                    capacity_bytes: p.get_capacity_bytes(),
                    available_bytes: p.get_available_bytes(),
                });
            }
            Ok(WorkerEvent::PoolCapacityUpdate { pools })
        }
        ArtifactWriteStarted(r) => {
            let r = r?;
            Ok(WorkerEvent::ArtifactWriteStarted {
                namespace_id: NamespaceId::from(r.get_namespace_id()?.to_str()?),
                artifact_id: ArtifactId::from(r.get_artifact_id()?.to_str()?),
                pool_id: PoolId::from(r.get_pool_id()?.to_str()?),
            })
        }
        ArtifactWriteCommitted(r) => {
            let r = r?;
            Ok(WorkerEvent::ArtifactWriteCommitted {
                namespace_id: NamespaceId::from(r.get_namespace_id()?.to_str()?),
                artifact_id: ArtifactId::from(r.get_artifact_id()?.to_str()?),
                pool_id: PoolId::from(r.get_pool_id()?.to_str()?),
                size_bytes: r.get_size_bytes(),
            })
        }
        ArtifactTransferReceived(r) => {
            let r = r?;
            Ok(WorkerEvent::ArtifactTransferReceived {
                transfer_id: r.get_transfer_id(),
                source_artifact_id: ArtifactId::from(r.get_source_artifact_id()?.to_str()?),
                source_pool_id: PoolId::from(r.get_source_pool_id()?.to_str()?),
                dest_artifact_id: ArtifactId::from(r.get_dest_artifact_id()?.to_str()?),
                dest_pool_id: PoolId::from(r.get_dest_pool_id()?.to_str()?),
                size_bytes: r.get_size_bytes(),
            })
        }
        TransferFailed(r) => {
            let r = r?;
            Ok(WorkerEvent::TransferFailed {
                transfer_id: r.get_transfer_id(),
                source_artifact_id: ArtifactId::from(r.get_source_artifact_id()?.to_str()?),
                source_pool_id: PoolId::from(r.get_source_pool_id()?.to_str()?),
                dest_artifact_id: ArtifactId::from(r.get_dest_artifact_id()?.to_str()?),
                dest_pool_id: PoolId::from(r.get_dest_pool_id()?.to_str()?),
                error: r.get_error()?.to_string()?,
            })
        }
        PressureUpdate(r) => {
            let r = r?;
            Ok(WorkerEvent::PressureUpdate {
                cpu: read_psi_metrics(r.get_cpu()?),
                memory: read_psi_metrics(r.get_memory()?),
                io: read_psi_metrics(r.get_io()?),
            })
        }
        EndpointActivation(r) => {
            let r = r?;
            let service_id = if r.get_has_service_id() {
                Some(ServiceId::from(r.get_service_id()?.to_str()?))
            } else {
                None
            };
            Ok(WorkerEvent::EndpointActivation {
                namespace_id: NamespaceId::from(r.get_namespace_id()?.to_str()?),
                ip: read_ipv4(r.get_ip()?),
                service_id,
            })
        }
        EndpointFlowStatus(r) => {
            let r = r?;
            Ok(WorkerEvent::EndpointFlowStatus {
                namespace_id: NamespaceId::from(r.get_namespace_id()?.to_str()?),
                ip: read_ipv4(r.get_ip()?),
                has_active_flows: r.get_has_active_flows(),
            })
        }
    }
}

// --- LogStreamHeader ---

pub fn write_log_stream_header(
    builder: &mut schema::log_stream_header::Builder<'_>,
    val: &LogStreamHeader,
) {
    builder.set_namespace_id(val.namespace_id.as_ref());
    builder.set_pod_id(val.pod_id.as_ref());
    builder.set_container_id(&val.container_id);
}

pub fn read_log_stream_header(
    reader: schema::log_stream_header::Reader<'_>,
) -> capnp::Result<LogStreamHeader> {
    Ok(LogStreamHeader {
        namespace_id: NamespaceId::from(reader.get_namespace_id()?.to_str()?),
        pod_id: PodId::from(reader.get_pod_id()?.to_str()?),
        container_id: reader.get_container_id()?.to_string()?,
    })
}
