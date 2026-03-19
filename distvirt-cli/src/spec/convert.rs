use std::collections::{HashMap, HashSet};
use std::net::Ipv4Addr;

use distvirt_client_protocol::*;

use super::errors::SpecErrors;
use super::helpers::{convert_expose, ip_to_mac, parse_cidr, parse_duration_ms, resolve_activation, resolve_resources};
use super::ip_alloc::IpAllocator;
use super::types::*;

// ---------------------------------------------------------------------------
// Conversion to proto NamespaceSpec
// ---------------------------------------------------------------------------

/// Convert a parsed native spec into (namespace_id, NamespaceSpec).
/// The namespace_id comes from metadata.name.
///
/// Runs multi-phase validation first, collecting all errors and reporting them
/// together so users can fix everything in one pass.
pub fn spec_to_namespace_spec(spec: &SpecFile) -> anyhow::Result<(Option<String>, NamespaceSpec)> {
    let mut errs = SpecErrors::new();
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
                "network.gateway",
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
            "apiVersion",
            format!("unrecognized apiVersion '{}' (expected 'v1')", spec.api_version),
        );
    }

    // kind check
    if spec.kind != "Namespace" {
        errs.error(
            "kind",
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
    let mut all_service_ids: HashMap<&str, Vec<String>> = HashMap::new();

    if let Some(ref spec_workloads) = spec.workloads {
        for (wid, wl) in spec_workloads {
            let wl_path = format!("workloads.{}", wid);

            // Non-empty containers
            if wl.containers.is_empty() {
                errs.error(&format!("{}.containers", wl_path), "containers list is empty");
            }

            // Non-empty image on each container
            for (i, c) in wl.containers.iter().enumerate() {
                let c_name = c
                    .name
                    .as_deref()
                    .map(|n| n.to_string())
                    .unwrap_or_else(|| format!("[{}]", i));
                if c.image.is_empty() {
                    errs.error(
                        &format!("{}.containers.{}.image", wl_path, c_name),
                        "image is empty",
                    );
                }
            }

            // Healthcheck warning
            if wl.healthcheck.is_some() {
                errs.warn(
                    &format!("{}.healthcheck", wl_path),
                    "healthcheck is not yet supported; will be ignored",
                );
            }

            // Track inline service IDs
            if let Some(ref inline_services) = wl.services {
                for sid in inline_services.keys() {
                    all_service_ids
                        .entry(sid.as_str())
                        .or_default()
                        .push(format!("{}.services.{}", wl_path, sid));
                }
            }
        }
    }

    // Top-level services: check workload references and track IDs
    if let Some(ref top_services) = spec.services {
        for (sid, svc) in top_services {
            let svc_path = format!("services.{}", sid);

            if !workload_keys.contains(svc.workload.as_str()) {
                errs.error(
                    &svc_path,
                    format!("workload '{}' does not exist", svc.workload),
                );
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
                locations[1].as_str(),
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
            errs.error("network.subnet", format!("invalid subnet: {}", e));
            return None;
        }
    };

    // Collect all explicit IPs for duplicate checking
    let mut explicit_ips: HashMap<String, Vec<String>> = HashMap::new();

    let mut check_ip = |ip_str: &str, path: &str, errs: &mut SpecErrors| {
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
                        path,
                        format!("IP {} is outside the subnet {}", ip_str, subnet_str),
                    );
                }
                explicit_ips
                    .entry(ip_str.to_string())
                    .or_default()
                    .push(path.to_string());
            }
            Err(_) => {
                errs.error(path, format!("'{}' is not a valid IPv4 address", ip_str));
            }
        }
    };

    if let Some(ref spec_workloads) = spec.workloads {
        for (wid, wl) in spec_workloads {
            if let Some(ref ip) = wl.ip {
                check_ip(ip, &format!("workloads.{}.ip", wid), errs);
            }
            if let Some(ref inline_services) = wl.services {
                for (sid, svc) in inline_services {
                    if let Some(ref ip) = svc.ip {
                        check_ip(
                            ip,
                            &format!("workloads.{}.services.{}.ip", wid, sid),
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
                check_ip(ip, &format!("services.{}.ip", sid), errs);
            }
        }
    }

    // Duplicate explicit IP check
    for (ip, locations) in &explicit_ips {
        if locations.len() > 1 {
            errs.error(
                locations[1].as_str(),
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
            "network.subnet",
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
                let path = format!("workloads.{}.activation", wid);
                if activation.passthrough.is_none() {
                    errs.error(
                        &path,
                        "only passthrough activator is valid on workloads",
                    );
                } else if let Some(ref pt) = activation.passthrough {
                    validate_duration(
                        &pt.idle_timeout,
                        &format!("{}.passthrough.idle_timeout", path),
                        errs,
                    );
                }
            }

            // Inline service activations
            if let Some(ref inline_services) = wl.services {
                for (sid, svc) in inline_services {
                    if let Some(ref act) = svc.activation {
                        validate_service_activation(
                            act,
                            &format!("workloads.{}.services.{}.activation", wid, sid),
                            errs,
                        );
                    }
                }
            }
        }
    }

    // Top-level service activations
    if let Some(ref top_services) = spec.services {
        for (sid, svc) in top_services {
            if let Some(ref act) = svc.activation {
                validate_service_activation(
                    act,
                    &format!("services.{}.activation", sid),
                    errs,
                );
            }
        }
    }

    // Default activation
    if let Some(ref defaults) = spec.defaults {
        if let Some(ref act) = defaults.activation {
            validate_service_activation(act, "defaults.activation", errs);
        }
    }
}

fn validate_service_activation(act: &SpecActivation, path: &str, errs: &mut SpecErrors) {
    if act.postgres.is_some() {
        errs.warn(
            path,
            "postgres activator is not yet supported; will be ignored",
        );
    }

    if let Some(ref pt) = act.passthrough {
        validate_duration(
            &pt.idle_timeout,
            &format!("{}.passthrough.idle_timeout", path),
            errs,
        );
    }

    if let Some(ref tcp) = act.tcp {
        if let Some(ref idle_timeout) = tcp.idle_timeout {
            validate_duration(
                idle_timeout,
                &format!("{}.tcp.idle_timeout", path),
                errs,
            );
        }
        if let Some(ref ports) = tcp.ports {
            for (i, &port) in ports.iter().enumerate() {
                if port == 0 || port > 65535 {
                    errs.error(
                        &format!("{}.tcp.ports[{}]", path, i),
                        format!("invalid port number {} (must be 1-65535)", port),
                    );
                }
            }
        }
    }

    if let Some(ref buf) = act.buffer {
        if buf.frames.is_some() || buf.timeout.is_some() {
            errs.warn(
                &format!("{}.buffer", path),
                "buffer fields are not yet supported; will be ignored",
            );
        }
    }
}

fn validate_duration(s: &str, path: &str, errs: &mut SpecErrors) {
    if let Err(e) = parse_duration_ms(s) {
        errs.error(path, format!("invalid duration '{}' ({})", s, e));
    }
}

/// Phase 4: Defaults validation
fn validate_defaults(spec: &SpecFile, errs: &mut SpecErrors) {
    if let Some(ref defaults) = spec.defaults {
        if let Some(ref res) = defaults.resources {
            validate_resource_values(res.requests.as_ref(), "defaults.resources.requests", errs);
            validate_resource_values(res.limits.as_ref(), "defaults.resources.limits", errs);
        }
    }

    // Also validate workload-level resources
    if let Some(ref spec_workloads) = spec.workloads {
        for (wid, wl) in spec_workloads {
            if let Some(ref res) = wl.resources {
                let path = format!("workloads.{}.resources", wid);
                validate_resource_values(
                    res.requests.as_ref(),
                    &format!("{}.requests", path),
                    errs,
                );
                validate_resource_values(
                    res.limits.as_ref(),
                    &format!("{}.limits", path),
                    errs,
                );
            }
        }
    }
}

fn validate_resource_values(vals: Option<&SpecResourceValues>, path: &str, errs: &mut SpecErrors) {
    if let Some(v) = vals {
        if let Some(mem) = v.memory_mb {
            if mem == 0 {
                errs.error(&format!("{}.memory_mb", path), "memory_mb must be > 0");
            }
        }
        if let Some(vcpus) = v.vcpus {
            if vcpus == 0 {
                errs.error(&format!("{}.vcpus", path), "vcpus must be > 0");
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
) -> anyhow::Result<(Option<String>, NamespaceSpec)> {
    // Reserve all explicit IPs
    if let Some(ref spec_workloads) = spec.workloads {
        for (_, wl) in spec_workloads {
            if let Some(ref ip) = wl.ip {
                allocator.reserve(ip.parse()?)?;
            }
            if let Some(ref inline_services) = wl.services {
                for (_, svc) in inline_services {
                    if let Some(ref ip) = svc.ip {
                        allocator.reserve(ip.parse()?)?;
                    }
                }
            }
        }
    }
    if let Some(ref top_services) = spec.services {
        for (_, svc) in top_services {
            if let Some(ref ip) = svc.ip {
                allocator.reserve(ip.parse()?)?;
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
                        }),
                    }
                })
                .collect();

            let wl_activation = wl.activation.as_ref().and_then(|a| {
                a.passthrough.as_ref().map(|passthrough| {
                    let idle_timeout_ms = parse_duration_ms(&passthrough.idle_timeout)
                        .expect("validated earlier");
                    ActivationSpec {
                        activator: Some(ActivatorConfig {
                            activator: Some(activator_config::Activator::Passthrough(
                                PassthroughActivator { idle_timeout_ms },
                            )),
                        }),
                        buffer_policy: None,
                    }
                })
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

                    let activation = resolve_activation(&svc.activation, &spec.defaults);
                    let expose = convert_expose(&svc.expose);

                    services.insert(
                        sid.clone(),
                        ServiceSpec {
                            workload_id: wid.clone(),
                            network: Some(ServiceNetworkConfig {
                                ip: svc_ip,
                                mac: svc_mac,
                            }),
                            activation,
                            expose,
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

            let activation = resolve_activation(&svc.activation, &spec.defaults);
            let expose = convert_expose(&svc.expose);

            services.insert(
                sid.clone(),
                ServiceSpec {
                    workload_id: svc.workload.clone(),
                    network: Some(ServiceNetworkConfig {
                        ip: svc_ip,
                        mac: svc_mac,
                    }),
                    activation,
                    expose,
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
