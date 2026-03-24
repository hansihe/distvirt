use std::collections::HashMap;
use std::net::Ipv4Addr;

use crate::errors::SpecError;
use super::ip_alloc::IpAllocator;
use super::path::YamlPath;
use super::types::*;

// ---------------------------------------------------------------------------
// Resolve context — holds allocated IPs for expression resolution
// ---------------------------------------------------------------------------

/// Context for resolving `${...}` expressions in the spec.
///
/// Built by allocating IPs from the spec before conversion, so that
/// expressions like `${self.ip}` can be resolved in env vars and other
/// string fields.
pub struct ResolveContext {
    workload_ips: HashMap<String, String>,
    service_ips: HashMap<String, String>,
}

impl ResolveContext {
    /// Build a resolve context from a parsed spec by allocating IPs.
    ///
    /// This mirrors the IP allocation order in `build_namespace_spec` so that
    /// resolved IPs match the final proto output.
    fn from_spec(spec: &SpecFile) -> Result<Self, SpecError> {
        let subnet_str = spec
            .network
            .as_ref()
            .map(|n| n.subnet.as_str())
            .unwrap_or("172.16.0.0/24");

        let mut allocator = IpAllocator::new(subnet_str)?;
        let mut workload_ips = HashMap::new();
        let mut service_ips = HashMap::new();

        // Phase 1: Reserve all explicit IPs
        if let Some(ref workloads) = spec.workloads {
            for (_, wl) in workloads {
                if let Some(ref ip) = wl.ip {
                    let addr: Ipv4Addr = ip.parse().map_err(|_| SpecError::Validation {
                        message: format!("invalid IP: {ip}"),
                    })?;
                    allocator.reserve(addr)?;
                }
                if let Some(ref inline_services) = wl.services {
                    for (_, svc) in inline_services {
                        if let Some(ref ip) = svc.ip {
                            let addr: Ipv4Addr =
                                ip.parse().map_err(|_| SpecError::Validation {
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

        // Phase 2: Assign IPs in sorted order (matches build_namespace_spec)
        if let Some(ref workloads) = spec.workloads {
            let mut wl_names: Vec<&String> = workloads.keys().collect();
            wl_names.sort();

            for wid in wl_names {
                let wl = &workloads[wid];
                let ip = match &wl.ip {
                    Some(ip) => ip.clone(),
                    None => allocator
                        .assign(&format!("workload:{}", wid))?
                        .to_string(),
                };
                workload_ips.insert(wid.clone(), ip);

                if let Some(ref inline_services) = wl.services {
                    let mut svc_names: Vec<&String> = inline_services.keys().collect();
                    svc_names.sort();
                    for sid in svc_names {
                        let svc = &inline_services[sid];
                        let ip = match &svc.ip {
                            Some(ip) => ip.clone(),
                            None => allocator
                                .assign(&format!("service:{}", sid))?
                                .to_string(),
                        };
                        service_ips.insert(sid.clone(), ip);
                    }
                }
            }
        }

        if let Some(ref top_services) = spec.services {
            let mut svc_names: Vec<&String> = top_services.keys().collect();
            svc_names.sort();
            for sid in svc_names {
                let svc = &top_services[sid];
                let ip = match &svc.ip {
                    Some(ip) => ip.clone(),
                    None => allocator
                        .assign(&format!("service:{}", sid))?
                        .to_string(),
                };
                service_ips.insert(sid.clone(), ip);
            }
        }

        Ok(ResolveContext {
            workload_ips,
            service_ips,
        })
    }
}

// ---------------------------------------------------------------------------
// Ref resolution — walks the spec and resolves ${...} in string fields
// ---------------------------------------------------------------------------

/// Resolve `${...}` ref expressions in all string fields of the spec.
///
/// Resolves:
/// - `${self.ip}` — pod IP of the current workload
/// - `${workloads.<id>.ip}` — pod IP of another workload
/// - `${services.<id>.ip}` — virtual IP of a service
///
/// Fragment values (`${values.*}`) must already be resolved before this step
/// (they are handled by text-level substitution during include resolution).
pub fn resolve_refs(parsed: &mut super::parse::ParsedSpec) -> Result<(), SpecError> {
    // Build the resolve context by allocating IPs.
    // If this fails (e.g. invalid subnet), skip resolution and let the
    // validation phase in spec_to_namespace_spec report proper errors.
    let ctx = match ResolveContext::from_spec(&parsed.spec) {
        Ok(ctx) => ctx,
        Err(_) => return Ok(()),
    };

    let spec = &mut parsed.spec;

    if let Some(ref mut workloads) = spec.workloads {
        for (wid, wl) in workloads.iter_mut() {
            let wl_path = YamlPath::root().key("workloads").key(wid);

            // Containers
            for (ci, container) in wl.containers.iter_mut().enumerate() {
                let c_path = wl_path.key("containers").index(ci);

                resolve_string(&mut container.image, wid, &ctx, &c_path.key("image"))?;

                if let Some(ref mut command) = container.command {
                    for (i, s) in command.iter_mut().enumerate() {
                        resolve_string(s, wid, &ctx, &c_path.key("command").index(i))?;
                    }
                }
                if let Some(ref mut args) = container.args {
                    for (i, s) in args.iter_mut().enumerate() {
                        resolve_string(s, wid, &ctx, &c_path.key("args").index(i))?;
                    }
                }
                if let Some(ref mut env) = container.env {
                    for (k, v) in env.iter_mut() {
                        resolve_string(v, wid, &ctx, &c_path.key("env").key(k))?;
                    }
                }
                if let Some(ref mut wd) = container.working_dir {
                    resolve_string(wd, wid, &ctx, &c_path.key("working_dir"))?;
                }
                if let Some(ref mut user) = container.user {
                    resolve_string(user, wid, &ctx, &c_path.key("user"))?;
                }
                if let Some(ref mut hostname) = container.hostname {
                    resolve_string(hostname, wid, &ctx, &c_path.key("hostname"))?;
                }
            }

            // config_data volume content
            if let Some(ref mut volumes) = wl.volumes {
                for (vi, vol) in volumes.iter_mut().enumerate() {
                    if let Some(ref mut cd) = vol.config_data {
                        for (fi, f) in cd.files.iter_mut().enumerate() {
                            let f_path = wl_path
                                .key("volumes")
                                .index(vi)
                                .key("config_data")
                                .key("files")
                                .index(fi)
                                .key("content");
                            resolve_string(&mut f.content, wid, &ctx, &f_path)?;
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

/// Resolve `${...}` expressions in a single string, in-place.
fn resolve_string(
    s: &mut String,
    self_workload: &str,
    ctx: &ResolveContext,
    path: &YamlPath,
) -> Result<(), SpecError> {
    if !s.contains("${") {
        return Ok(());
    }
    *s = resolve_expressions(s, self_workload, ctx, path)?;
    Ok(())
}

/// Scan a string for `${...}` expressions and resolve them.
fn resolve_expressions(
    input: &str,
    self_workload: &str,
    ctx: &ResolveContext,
    path: &YamlPath,
) -> Result<String, SpecError> {
    let mut result = String::with_capacity(input.len());
    let mut chars = input.char_indices();

    while let Some((i, ch)) = chars.next() {
        if ch == '$' {
            if chars.clone().next().map(|(_, c)| c) == Some('{') {
                chars.next(); // consume '{'
                let mut expr = String::new();
                let mut found_close = false;
                for (_, c) in chars.by_ref() {
                    if c == '}' {
                        found_close = true;
                        break;
                    }
                    expr.push(c);
                }
                if !found_close {
                    return Err(SpecError::Validation {
                        message: format!(
                            "{} — unclosed expression starting at position {}",
                            path, i
                        ),
                    });
                }
                let resolved = resolve_single_expr(&expr, self_workload, ctx, path)?;
                result.push_str(&resolved);
            } else {
                result.push(ch);
            }
        } else {
            result.push(ch);
        }
    }

    Ok(result)
}

/// Resolve a single expression like `self.ip`, `workloads.db.ip`, `services.api.ip`.
fn resolve_single_expr(
    expr: &str,
    self_workload: &str,
    ctx: &ResolveContext,
    path: &YamlPath,
) -> Result<String, SpecError> {
    let parts: Vec<&str> = expr.splitn(3, '.').collect();

    match parts.as_slice() {
        ["self", "ip"] => ctx
            .workload_ips
            .get(self_workload)
            .cloned()
            .ok_or_else(|| SpecError::Validation {
                message: format!("{} — self.ip: workload '{}' not found", path, self_workload),
            }),
        ["workloads", name, "ip"] => ctx
            .workload_ips
            .get(*name)
            .cloned()
            .ok_or_else(|| SpecError::Validation {
                message: format!(
                    "{} — workloads.{}.ip: workload '{}' does not exist",
                    path, name, name
                ),
            }),
        ["services", name, "ip"] => ctx
            .service_ips
            .get(*name)
            .cloned()
            .ok_or_else(|| SpecError::Validation {
                message: format!(
                    "{} — services.{}.ip: service '{}' does not exist",
                    path, name, name
                ),
            }),
        ["values", ..] => Err(SpecError::Validation {
            message: format!(
                "{} — expression '${{{}}}' uses 'values.*' which is only available in fragments included via 'include'",
                path, expr
            ),
        }),
        _ => Err(SpecError::Validation {
            message: format!("{} — unknown expression '${{{}}}'", path, expr),
        }),
    }
}
