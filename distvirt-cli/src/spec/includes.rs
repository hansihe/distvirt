use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow::bail;
use std::fs;

use super::errors::SpecErrors;
use super::types::{SpecFile, SpecIncludeOverrides};

// ---------------------------------------------------------------------------
// Fragment include resolution
// ---------------------------------------------------------------------------

/// Resolve `include` entries in a namespace spec, loading and merging fragments.
/// `spec_path` is the path to the namespace spec file (used for relative path resolution).
pub fn resolve_includes(spec: &mut SpecFile, spec_path: &Path) -> anyhow::Result<()> {
    let includes = match spec.include.take() {
        Some(inc) if !inc.is_empty() => inc,
        _ => return Ok(()),
    };

    let spec_dir = spec_path
        .parent()
        .unwrap_or_else(|| Path::new("."));

    let mut errs = SpecErrors::new();

    for (idx, entry) in includes.iter().enumerate() {
        let fragment_path = spec_dir.join(&entry.path);
        let label = format!("include[{}] ({})", idx, entry.path);

        // Read fragment file
        let raw_yaml = match fs::read_to_string(&fragment_path) {
            Ok(s) => s,
            Err(e) => {
                errs.error(&label, format!("file not found: {}", e));
                continue;
            }
        };

        // Variable substitution
        let substituted = match substitute_variables(&raw_yaml, &entry.values, &label) {
            Ok(s) => s,
            Err(e) => {
                errs.error(&label, format!("{}", e));
                continue;
            }
        };

        // Parse fragment
        let mut fragment: SpecFile = match serde_yaml::from_str(&substituted) {
            Ok(f) => f,
            Err(e) => {
                errs.error(&label, format!("YAML parse error: {}", e));
                continue;
            }
        };

        // Validate fragment structure
        validate_fragment_structure(&fragment, &label, &mut errs);
        if errs.has_errors() {
            continue;
        }

        // Apply overrides
        if let Some(ref overrides) = entry.overrides {
            apply_overrides(&mut fragment, overrides);
        }

        // Merge workloads
        if let Some(fragment_workloads) = fragment.workloads.take() {
            let spec_workloads = spec.workloads.get_or_insert_with(HashMap::new);
            for (wid, wl) in fragment_workloads {
                if spec_workloads.contains_key(&wid) {
                    errs.error(
                        &format!("{} > workloads.{}", label, wid),
                        format!("duplicate workload ID '{}'", wid),
                    );
                } else {
                    spec_workloads.insert(wid, wl);
                }
            }
        }

        // Merge top-level services (validate workload refs within fragment)
        if let Some(fragment_services) = fragment.services.take() {
            let spec_services = spec.services.get_or_insert_with(HashMap::new);
            for (sid, svc) in fragment_services {
                if spec_services.contains_key(&sid) {
                    errs.error(
                        &format!("{} > services.{}", label, sid),
                        format!("duplicate service ID '{}'", sid),
                    );
                } else {
                    spec_services.insert(sid, svc);
                }
            }
        }
    }

    errs.into_result()
}

/// Replace `${VAR}` patterns in the YAML string with values from the map.
/// Errors on undefined variables.
fn substitute_variables(
    yaml: &str,
    values: &HashMap<String, String>,
    path_label: &str,
) -> anyhow::Result<String> {
    let mut result = String::with_capacity(yaml.len());
    let mut chars = yaml.char_indices();

    while let Some((i, ch)) = chars.next() {
        if ch == '$' {
            // Check if next char is '{'
            if chars.clone().next().map(|(_, c)| c) == Some('{') {
                // Consume '{'
                chars.next();
                // Find closing '}'
                let start = i;
                let mut var_name = String::new();
                let mut found_close = false;
                for (_, c) in chars.by_ref() {
                    if c == '}' {
                        found_close = true;
                        break;
                    }
                    var_name.push(c);
                }
                if !found_close {
                    bail!("{} — unclosed variable reference starting at position {}", path_label, start);
                }
                match values.get(&var_name) {
                    Some(val) => result.push_str(val),
                    None => bail!("{} — undefined variable '{}'", path_label, var_name),
                }
            } else {
                result.push(ch);
            }
        } else {
            result.push(ch);
        }
    }

    Ok(result)
}

/// Apply overrides to a fragment's containers.
fn apply_overrides(fragment: &mut SpecFile, overrides: &SpecIncludeOverrides) {
    if let Some(ref override_env) = overrides.env {
        if let Some(ref mut workloads) = fragment.workloads {
            for wl in workloads.values_mut() {
                for container in &mut wl.containers {
                    let env = container.env.get_or_insert_with(HashMap::new);
                    for (k, v) in override_env {
                        env.insert(k.clone(), v.clone());
                    }
                }
            }
        }
    }
}

/// Validate that a fragment has the expected structure.
fn validate_fragment_structure(spec: &SpecFile, label: &str, errs: &mut SpecErrors) {
    if spec.api_version != "v1" {
        errs.error(
            label,
            format!("unrecognized apiVersion '{}' (expected 'v1')", spec.api_version),
        );
    }
    if spec.kind != "WorkloadFragment" {
        errs.error(
            label,
            format!("expected kind 'WorkloadFragment', got '{}'", spec.kind),
        );
    }
    if spec.metadata.is_some() {
        errs.error(label, "fragments cannot have 'metadata'");
    }
    if spec.network.is_some() {
        errs.error(label, "fragments cannot have 'network'");
    }
    if spec.defaults.is_some() {
        errs.error(label, "fragments cannot have 'defaults'");
    }
    if spec.include.is_some() {
        errs.error(label, "fragments cannot have 'include' (no recursion)");
    }

    // Must have at least one workload
    match &spec.workloads {
        None => {
            errs.error(label, "fragment must have at least one workload");
        }
        Some(workloads) if workloads.is_empty() => {
            errs.error(label, "fragment must have at least one workload");
        }
        Some(workloads) => {
            let workload_keys: HashSet<&str> = workloads.keys().map(|k| k.as_str()).collect();

            for (wid, wl) in workloads {
                let wl_path = format!("{} > workloads.{}", label, wid);
                if wl.containers.is_empty() {
                    errs.error(
                        &format!("{}.containers", wl_path),
                        "containers list is empty",
                    );
                }
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
            }

            // Validate top-level service workload refs within fragment
            if let Some(ref services) = spec.services {
                for (sid, svc) in services {
                    if !workload_keys.contains(svc.workload.as_str()) {
                        errs.error(
                            &format!("{} > services.{}", label, sid),
                            format!(
                                "workload '{}' does not exist in this fragment",
                                svc.workload
                            ),
                        );
                    }
                }
            }
        }
    }
}
