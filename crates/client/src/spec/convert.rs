use std::collections::{HashMap, HashSet};
use std::net::Ipv4Addr;

use distvirt_client_protocol::*;

use crate::errors::{SpecError, SpecErrors};
use super::helpers::{convert_buffer, convert_ports, ip_to_mac, parse_cidr, parse_duration_ms, resolve_resources};
use super::ip_alloc::IpAllocator;
use super::path::YamlPath;
use super::types::*;

// ---------------------------------------------------------------------------
// Conversion to proto NamespaceSpec
// ---------------------------------------------------------------------------

/// Convert a parsed native spec into (namespace_id, NamespaceSpec).
/// The namespace_id comes from metadata.name.
///
/// Runs multi-phase validation first, collecting all errors and reporting them
/// together so users can fix everything in one pass.
pub fn spec_to_namespace_spec(parsed: &super::parse::ParsedSpec) -> Result<(Option<String>, NamespaceSpec), SpecError> {
    let spec = &parsed.spec;
    let mut errs = SpecErrors::new();
    errs.add_source(&parsed.file_name, &parsed.source);
    let namespace_id = spec.metadata.as_ref().and_then(|m| m.name.clone());

    // --- Phase 1: Structural validation ---
    validate_structure(spec, &mut errs);

    // --- Phase 3: Activation validation ---
    validate_activation(spec, &mut errs);

    // --- Phase 4: Defaults validation ---
    validate_defaults(spec, &mut errs);

    let subnet_str = spec
        .network
        .as_ref()
        .map(|n| n.subnet.as_str())
        .unwrap_or("172.16.0.0/24");

    // Warnings for recognized-but-unsupported features
    if let Some(ref net) = spec.network {
        if net.gateway.is_some() {
            errs.warn(
                YamlPath::root().key("network").key("gateway"),
                "gateway is not yet supported; will be ignored",
            );
        }
    }

    // --- Phase 2: Network validation (subnet + IPs) ---
    let allocator = validate_network_and_build_allocator(spec, subnet_str, &mut errs);

    // If there are errors, bail now before attempting conversion
    if errs.has_errors() {
        errs.into_result()?;
        unreachable!();
    }

    // Log any warnings (they don't block conversion)
    errs.into_result()?;

    // --- Conversion (validation passed) ---
    let mut allocator = allocator.expect("allocator must exist when no errors");
    build_namespace_spec(spec, namespace_id, subnet_str, &mut allocator)
}

/// Phase 1: Structural validation
fn validate_structure(spec: &SpecFile, errs: &mut SpecErrors) {
    // apiVersion check
    if spec.api_version != "v1" {
        errs.error(
            YamlPath::root().key("apiVersion"),
            format!("unrecognized apiVersion '{}' (expected 'v1')", spec.api_version),
        );
    }

    // kind check
    if spec.kind != "Namespace" {
        errs.error(
            YamlPath::root().key("kind"),
            format!(
                "unsupported kind '{}' (expected 'Namespace')",
                spec.kind
            ),
        );
    }

    let workload_keys: HashSet<&str> = spec
        .workloads
        .as_ref()
        .map(|w| w.keys().map(|k| k.as_str()).collect())
        .unwrap_or_default();

    // Collect all service IDs to check for duplicates
    let mut all_service_ids: HashMap<&str, Vec<YamlPath>> = HashMap::new();

    if let Some(ref spec_workloads) = spec.workloads {
        for (wid, wl) in spec_workloads {
            let wl_path = YamlPath::root().key("workloads").key(wid);

            // Non-empty containers
            if wl.containers.is_empty() {
                errs.error(wl_path.key("containers"), "containers list is empty");
            }

            // Non-empty image on each container
            for (i, c) in wl.containers.iter().enumerate() {
                if c.image.is_empty() {
                    errs.error(
                        wl_path.key("containers").index(i).key("image"),
                        "image is empty",
                    );
                }
            }

            // Healthcheck warning
            if wl.healthcheck.is_some() {
                errs.warn(
                    wl_path.key("healthcheck"),
                    "healthcheck is not yet supported; will be ignored",
                );
            }

            // Volume validation
            if let Some(ref volumes) = wl.volumes {
                let mut vol_names: HashSet<&str> = HashSet::new();
                for (vi, vol) in volumes.iter().enumerate() {
                    let vol_path = wl_path.key("volumes").index(vi);

                    // Name must be non-empty
                    if vol.name.is_empty() {
                        errs.error(vol_path.key("name"), "volume name is empty");
                    }

                    // Duplicate volume name check
                    if !vol.name.is_empty() && !vol_names.insert(vol.name.as_str()) {
                        errs.error(
                            vol_path.key("name"),
                            format!("duplicate volume name '{}'", vol.name),
                        );
                    }

                    // Exactly one volume type
                    let type_count = vol.empty_dir.is_some() as u8
                        + vol.config_data.is_some() as u8;
                    if type_count == 0 {
                        errs.error(
                            vol_path.clone(),
                            "volume must specify exactly one type (empty_dir, config_data)",
                        );
                    } else if type_count > 1 {
                        errs.error(
                            vol_path.clone(),
                            "volume must specify exactly one type, but multiple were given",
                        );
                    }

                    // config_data validation
                    if let Some(ref cd) = vol.config_data {
                        if cd.files.is_empty() {
                            errs.error(vol_path.key("config_data").key("files"), "files list is empty");
                        }
                        for (fi, file) in cd.files.iter().enumerate() {
                            if file.path.is_empty() {
                                errs.error(
                                    vol_path.key("config_data").key("files").index(fi).key("path"),
                                    "file path is empty",
                                );
                            }
                        }
                    }
                }

                // Validate volume_mounts reference valid volume names
                for (ci, container) in wl.containers.iter().enumerate() {
                    if let Some(ref mounts) = container.volume_mounts {
                        for (mi, mount) in mounts.iter().enumerate() {
                            let mount_path = wl_path
                                .key("containers")
                                .index(ci)
                                .key("volume_mounts")
                                .index(mi);
                            if mount.mount_path.is_empty() {
                                errs.error(mount_path.key("mount_path"), "mount_path is empty");
                            }
                            if !vol_names.contains(mount.name.as_str()) {
                                errs.error(
                                    mount_path.key("name"),
                                    format!(
                                        "volume '{}' is not defined in workload '{}'",
                                        mount.name, wid
                                    ),
                                );
                            }
                        }
                    }
                }
            } else {
                // No volumes defined — check no container references volumes
                for (ci, container) in wl.containers.iter().enumerate() {
                    if let Some(ref mounts) = container.volume_mounts {
                        for (mi, mount) in mounts.iter().enumerate() {
                            errs.error(
                                wl_path
                                    .key("containers")
                                    .index(ci)
                                    .key("volume_mounts")
                                    .index(mi)
                                    .key("name"),
                                format!(
                                    "volume '{}' is not defined in workload '{}' (no volumes declared)",
                                    mount.name, wid
                                ),
                            );
                        }
                    }
                }
            }

            // Warn if activation is configured but respects_demand is false
            if !wl.respects_demand {
                if wl.activation.is_some() {
                    errs.warn(
                        wl_path.key("activation"),
                        "activation is configured but respects_demand is false; \
                         activation will have no effect on an always-on workload",
                    );
                }
            }

            // Track inline service IDs
            if let Some(ref inline_services) = wl.services {
                for sid in inline_services.keys() {
                    all_service_ids
                        .entry(sid.as_str())
                        .or_default()
                        .push(wl_path.key("services").key(sid));
                }

                // Warn if inline services have activation but workload is always-on
                if !wl.respects_demand {
                    for (sid, svc) in inline_services {
                        let has_activators = svc.ports.as_ref().map_or(false, |ports| {
                            ports.iter().any(|p| p.activator.is_some())
                        });
                        if has_activators {
                            errs.warn(
                                wl_path.key("services").key(sid).key("ports"),
                                "port activators are configured but the workload has \
                                 respects_demand: false; activation will have no effect \
                                 on an always-on workload",
                            );
                        }
                    }
                }
            }
        }
    }

    // Top-level services: check workload references and track IDs
    if let Some(ref top_services) = spec.services {
        for (sid, svc) in top_services {
            let svc_path = YamlPath::root().key("services").key(sid);

            if !workload_keys.contains(svc.workload.as_str()) {
                errs.error(
                    svc_path.clone(),
                    format!("workload '{}' does not exist", svc.workload),
                );
            }

            // Warn if service has activation but target workload is always-on
            let has_activators = svc.ports.as_ref().map_or(false, |ports| {
                ports.iter().any(|p| p.activator.is_some())
            });
            if has_activators {
                if let Some(ref spec_workloads) = spec.workloads {
                    if let Some(target_wl) = spec_workloads.get(&svc.workload) {
                        if !target_wl.respects_demand {
                            errs.warn(
                                svc_path.key("ports"),
                                format!(
                                    "port activators are configured but workload '{}' has \
                                     respects_demand: false; activation will have no effect \
                                     on an always-on workload",
                                    svc.workload
                                ),
                            );
                        }
                    }
                }
            }

            all_service_ids
                .entry(sid.as_str())
                .or_default()
                .push(svc_path);
        }
    }

    // Duplicate service IDs
    for (sid, locations) in &all_service_ids {
        if locations.len() > 1 {
            errs.error(
                locations[1].clone(),
                format!(
                    "duplicate service ID '{}' (also defined at {})",
                    sid, locations[0]
                ),
            );
        }
    }
}

/// Phase 2: Network validation — parse subnet, check explicit IPs, build allocator.
/// Returns the allocator if subnet parsing succeeded (None on subnet parse failure).
fn validate_network_and_build_allocator(
    spec: &SpecFile,
    subnet_str: &str,
    errs: &mut SpecErrors,
) -> Option<IpAllocator> {
    let allocator = match IpAllocator::new(subnet_str) {
        Ok(a) => a,
        Err(e) => {
            errs.error(YamlPath::root().key("network").key("subnet"), format!("invalid subnet: {}", e));
            return None;
        }
    };

    // Collect all explicit IPs for duplicate checking
    let mut explicit_ips: HashMap<String, Vec<YamlPath>> = HashMap::new();

    let mut check_ip = |ip_str: &str, path: YamlPath, errs: &mut SpecErrors| {
        match ip_str.parse::<Ipv4Addr>() {
            Ok(ip) => {
                // Check within subnet
                let ip_u32 = u32::from(ip);
                let (base_ip, prefix) = match parse_cidr(subnet_str) {
                    Ok(v) => v,
                    Err(_) => return, // already reported
                };
                let base = u32::from(base_ip);
                let first_host = base + 2;
                let host_bits = 32 - prefix as u32;
                let num_hosts = 1u32.checked_shl(host_bits).unwrap_or(0).saturating_sub(2);
                if ip_u32 < first_host || ip_u32 >= first_host + num_hosts {
                    errs.error(
                        path.clone(),
                        format!("IP {} is outside the subnet {}", ip_str, subnet_str),
                    );
                }
                explicit_ips
                    .entry(ip_str.to_string())
                    .or_default()
                    .push(path);
            }
            Err(_) => {
                errs.error(path, format!("'{}' is not a valid IPv4 address", ip_str));
            }
        }
    };

    if let Some(ref spec_workloads) = spec.workloads {
        for (wid, wl) in spec_workloads {
            let wl_path = YamlPath::root().key("workloads").key(wid);
            if let Some(ref ip) = wl.ip {
                check_ip(ip, wl_path.key("ip"), errs);
            }
            if let Some(ref inline_services) = wl.services {
                for (sid, svc) in inline_services {
                    if let Some(ref ip) = svc.ip {
                        check_ip(
                            ip,
                            wl_path.key("services").key(sid).key("ip"),
                            errs,
                        );
                    }
                }
            }
        }
    }
    if let Some(ref top_services) = spec.services {
        for (sid, svc) in top_services {
            if let Some(ref ip) = svc.ip {
                check_ip(ip, YamlPath::root().key("services").key(sid).key("ip"), errs);
            }
        }
    }

    // Duplicate explicit IP check
    for (ip, locations) in &explicit_ips {
        if locations.len() > 1 {
            errs.error(
                locations[1].clone(),
                format!(
                    "duplicate IP '{}' (also assigned at {})",
                    ip, locations[0]
                ),
            );
        }
    }

    // Check subnet capacity
    let total_items = count_total_items(spec);
    if total_items as u32 > allocator.num_hosts {
        errs.error(
            YamlPath::root().key("network").key("subnet"),
            format!(
                "subnet has {} usable addresses but spec requires {} (workloads + services)",
                allocator.num_hosts, total_items
            ),
        );
    }

    Some(allocator)
}

/// Phase 3: Activation validation
fn validate_activation(spec: &SpecFile, errs: &mut SpecErrors) {
    if let Some(ref spec_workloads) = spec.workloads {
        for (wid, wl) in spec_workloads {
            if let Some(ref activation) = wl.activation {
                let path = YamlPath::root().key("workloads").key(wid).key("activation");
                if let Some(ref idle_timeout) = activation.idle_timeout {
                    validate_duration(idle_timeout, path.key("idle_timeout"), errs);
                }
            }

            // Inline service ports
            if let Some(ref inline_services) = wl.services {
                for (sid, svc) in inline_services {
                    validate_service_ports(
                        &svc.ports,
                        &svc.idle_timeout,
                        &svc.buffer,
                        YamlPath::root()
                            .key("workloads")
                            .key(wid)
                            .key("services")
                            .key(sid),
                        errs,
                    );
                }
            }
        }
    }

    // Top-level service ports
    if let Some(ref top_services) = spec.services {
        for (sid, svc) in top_services {
            validate_service_ports(
                &svc.ports,
                &svc.idle_timeout,
                &svc.buffer,
                YamlPath::root().key("services").key(sid),
                errs,
            );
        }
    }
}

fn validate_service_ports(
    ports: &Option<Vec<SpecPort>>,
    idle_timeout: &Option<String>,
    buffer: &Option<SpecBuffer>,
    path: YamlPath,
    errs: &mut SpecErrors,
) {
    if let Some(idle_timeout) = idle_timeout {
        validate_duration(idle_timeout, path.key("idle_timeout"), errs);
    }

    if let Some(buf) = buffer {
        if let Some(ref timeout) = buf.timeout {
            validate_duration(timeout, path.key("buffer").key("timeout"), errs);
        }
    }

    if let Some(ports) = ports {
        let has_any_activator = ports.iter().any(|p| p.activator.is_some());
        let all_have_activator = ports.iter().all(|p| p.activator.is_some());

        if has_any_activator && !all_have_activator {
            errs.error(
                path.key("ports"),
                "mixed activated/passthrough ports are not allowed; \
                 all ports must have activators or none",
            );
        }

        for (i, port) in ports.iter().enumerate() {
            if port.port == 0 || port.port > 65535 {
                errs.error(
                    path.key("ports").index(i).key("port"),
                    format!("invalid port number {} (must be 1-65535)", port.port),
                );
            }
            if let Some(target) = port.target {
                if target == 0 || target > 65535 {
                    errs.error(
                        path.key("ports").index(i).key("target"),
                        format!("invalid target port {} (must be 1-65535)", target),
                    );
                }
            }
        }
    }
}

fn validate_duration(s: &str, path: YamlPath, errs: &mut SpecErrors) {
    if let Err(e) = parse_duration_ms(s) {
        errs.error(path, format!("invalid duration '{}' ({})", s, e));
    }
}

/// Phase 4: Defaults validation
fn validate_defaults(spec: &SpecFile, errs: &mut SpecErrors) {
    if let Some(ref defaults) = spec.defaults {
        if let Some(ref res) = defaults.resources {
            let path = YamlPath::root().key("defaults").key("resources");
            validate_resource_values(res.requests.as_ref(), path.key("requests"), errs);
            validate_resource_values(res.limits.as_ref(), path.key("limits"), errs);
        }
    }

    // Also validate workload-level resources
    if let Some(ref spec_workloads) = spec.workloads {
        for (wid, wl) in spec_workloads {
            if let Some(ref res) = wl.resources {
                let path = YamlPath::root().key("workloads").key(wid).key("resources");
                validate_resource_values(
                    res.requests.as_ref(),
                    path.key("requests"),
                    errs,
                );
                validate_resource_values(
                    res.limits.as_ref(),
                    path.key("limits"),
                    errs,
                );
            }
        }
    }
}

fn validate_resource_values(vals: Option<&SpecResourceValues>, path: YamlPath, errs: &mut SpecErrors) {
    if let Some(v) = vals {
        if let Some(mem) = v.memory_mb {
            if mem == 0 {
                errs.error(path.key("memory_mb"), "memory_mb must be > 0");
            }
        }
        if let Some(vcpus) = v.vcpus {
            if vcpus == 0 {
                errs.error(path.key("vcpus"), "vcpus must be > 0");
            }
        }
    }
}

fn count_total_items(spec: &SpecFile) -> usize {
    let mut count = 0;
    if let Some(ref wls) = spec.workloads {
        count += wls.len();
        for wl in wls.values() {
            if let Some(ref svcs) = wl.services {
                count += svcs.len();
            }
        }
    }
    if let Some(ref svcs) = spec.services {
        count += svcs.len();
    }
    count
}

/// Build the NamespaceSpec after validation has passed.
fn build_namespace_spec(
    spec: &SpecFile,
    namespace_id: Option<String>,
    subnet_str: &str,
    allocator: &mut IpAllocator,
) -> Result<(Option<String>, NamespaceSpec), SpecError> {
    // Reserve all explicit IPs
    if let Some(ref spec_workloads) = spec.workloads {
        for (_, wl) in spec_workloads {
            if let Some(ref ip) = wl.ip {
                let addr: Ipv4Addr = ip.parse().map_err(|_| SpecError::Validation {
                        message: format!("invalid IP: {ip}"),
                    })?;
                    allocator.reserve(addr)?;
            }
            if let Some(ref inline_services) = wl.services {
                for (_, svc) in inline_services {
                    if let Some(ref ip) = svc.ip {
                        let addr: Ipv4Addr = ip.parse().map_err(|_| SpecError::Validation {
                        message: format!("invalid IP: {ip}"),
                    })?;
                    allocator.reserve(addr)?;
                    }
                }
            }
        }
    }
    if let Some(ref top_services) = spec.services {
        for (_, svc) in top_services {
            if let Some(ref ip) = svc.ip {
                let addr: Ipv4Addr = ip.parse().map_err(|_| SpecError::Validation {
                        message: format!("invalid IP: {ip}"),
                    })?;
                    allocator.reserve(addr)?;
            }
        }
    }

    // Build workloads and services
    let mut workloads = HashMap::new();
    let mut services = HashMap::new();

    if let Some(ref spec_workloads) = spec.workloads {
        let mut wl_names: Vec<&String> = spec_workloads.keys().collect();
        wl_names.sort();

        for wid in wl_names {
            let wl = &spec_workloads[wid];
            let default_suspend = spec
                .defaults
                .as_ref()
                .and_then(|d| d.suspend_on_idle)
                .unwrap_or(true);
            let suspend_on_idle = wl.suspend_on_idle.unwrap_or(default_suspend);

            let pod_ip = match &wl.ip {
                Some(ip) => ip.clone(),
                None => allocator.assign(&format!("workload:{}", wid))?.to_string(),
            };
            let pod_mac = ip_to_mac(&pod_ip);

            let (requests, limits) = resolve_resources(&wl.resources, &spec.defaults);
            let resources = if requests.is_some() || limits.is_some() {
                Some(ResourceRequirements {
                    requests: requests.map(|r| ResourceValues {
                        memory_mb: r.memory_mb.unwrap_or(0),
                        vcpus: r.vcpus.unwrap_or(0),
                    }),
                    limits: limits.map(|l| ResourceValues {
                        memory_mb: l.memory_mb.unwrap_or(0),
                        vcpus: l.vcpus.unwrap_or(0),
                    }),
                })
            } else {
                None
            };

            let containers: Vec<ContainerSpec> = wl
                .containers
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    let name = c.name.clone().unwrap_or_else(|| {
                        if i == 0 {
                            "main".to_string()
                        } else {
                            format!("container-{}", i)
                        }
                    });
                    let volume_mounts = c
                        .volume_mounts
                        .as_ref()
                        .map(|mounts| {
                            mounts
                                .iter()
                                .map(|m| VolumeMountSpec {
                                    name: m.name.clone(),
                                    mount_path: m.mount_path.clone(),
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    ContainerSpec {
                        name,
                        image: c.image.clone(),
                        config: Some(ContainerConfig {
                            entrypoint: c.entrypoint.clone().unwrap_or_default(),
                            args: c.args.clone().unwrap_or_default(),
                            env: c.env.clone().unwrap_or_default(),
                            working_dir: c.working_dir.clone().unwrap_or_default(),
                            user: c.user.clone().unwrap_or_default(),
                            hostname: c.hostname.clone().unwrap_or_default(),
                            tty: c.tty,
                            volume_mounts,
                        }),
                    }
                })
                .collect();

            let volumes: Vec<VolumeSpec> = wl
                .volumes
                .as_ref()
                .map(|vols| {
                    vols.iter()
                        .map(|v| {
                            let volume_type = if let Some(ref ed) = v.empty_dir {
                                Some(volume_spec::VolumeType::EmptyDir(EmptyDirVolume {
                                    size_mb: ed.size_mb.unwrap_or(0),
                                }))
                            } else if let Some(ref cd) = v.config_data {
                                Some(volume_spec::VolumeType::ConfigData(ConfigDataVolume {
                                    files: cd
                                        .files
                                        .iter()
                                        .map(|f| ConfigDataFile {
                                            path: f.path.clone(),
                                            content: f.content.clone(),
                                        })
                                        .collect(),
                                }))
                            } else {
                                None // shouldn't happen after validation
                            };
                            VolumeSpec {
                                name: v.name.clone(),
                                volume_type,
                            }
                        })
                        .collect()
                })
                .unwrap_or_default();

            let wl_activation = wl.activation.as_ref().map(|a| {
                let idle_timeout_ms = a
                    .idle_timeout
                    .as_ref()
                    .map(|s| parse_duration_ms(s).expect("validated earlier"))
                    .unwrap_or(30_000); // default 30s
                ActivationSpec { idle_timeout_ms }
            });

            workloads.insert(
                wid.clone(),
                WorkloadSpec {
                    network: Some(PodNetworkConfig {
                        ip: pod_ip,
                        mac: pod_mac,
                    }),
                    containers,
                    suspend_on_idle,
                    resources,
                    activation: wl_activation,
                    respects_demand: wl.respects_demand,
                    volumes,
                    run_policy: 0, // SERVICE (default)
                },
            );

            if let Some(ref inline_services) = wl.services {
                let mut svc_names: Vec<&String> = inline_services.keys().collect();
                svc_names.sort();

                for sid in svc_names {
                    let svc = &inline_services[sid];
                    let svc_ip = match &svc.ip {
                        Some(ip) => ip.clone(),
                        None => allocator.assign(&format!("service:{}", sid))?.to_string(),
                    };
                    let svc_mac = ip_to_mac(&svc_ip);

                    let ports = convert_ports(&svc.ports);
                    let idle_timeout_ms = svc
                        .idle_timeout
                        .as_ref()
                        .map(|s| parse_duration_ms(s).expect("validated earlier"))
                        .unwrap_or(0);
                    let (buffer_frames, buffer_timeout_ms) =
                        convert_buffer(&svc.buffer);

                    services.insert(
                        sid.clone(),
                        ServiceSpec {
                            workload_id: wid.clone(),
                            network: Some(ServiceNetworkConfig {
                                ip: svc_ip,
                                mac: svc_mac,
                            }),
                            ports,
                            idle_timeout_ms,
                            buffer_frames,
                            buffer_timeout_ms,
                        },
                    );
                }
            }
        }
    }

    if let Some(ref top_services) = spec.services {
        let mut svc_names: Vec<&String> = top_services.keys().collect();
        svc_names.sort();

        for sid in svc_names {
            let svc = &top_services[sid];
            let svc_ip = match &svc.ip {
                Some(ip) => ip.clone(),
                None => allocator.assign(&format!("service:{}", sid))?.to_string(),
            };
            let svc_mac = ip_to_mac(&svc_ip);

            let ports = convert_ports(&svc.ports);
            let idle_timeout_ms = svc
                .idle_timeout
                .as_ref()
                .map(|s| parse_duration_ms(s).expect("validated earlier"))
                .unwrap_or(0);
            let (buffer_frames, buffer_timeout_ms) =
                convert_buffer(&svc.buffer);

            services.insert(
                sid.clone(),
                ServiceSpec {
                    workload_id: svc.workload.clone(),
                    network: Some(ServiceNetworkConfig {
                        ip: svc_ip,
                        mac: svc_mac,
                    }),
                    ports,
                    idle_timeout_ms,
                    buffer_frames,
                    buffer_timeout_ms,
                },
            );
        }
    }

    Ok((
        namespace_id,
        NamespaceSpec {
            network: Some(NetworkConfig {
                subnet: subnet_str.to_string(),
            }),
            workloads,
            services,
        },
    ))
}
