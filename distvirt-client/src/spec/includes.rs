use std::collections::{HashMap, HashSet};
use std::path::Path;

use std::fs;

use crate::errors::{SourceId, SpecError, SpecErrors};
use super::path::YamlPath;
use super::types::{SpecFile, SpecIncludeOverrides};

// ---------------------------------------------------------------------------
// Fragment include resolution
// ---------------------------------------------------------------------------

/// Resolve `include` entries in a namespace spec, loading and merging fragments.
/// `spec_path` is the path to the namespace spec file (used for relative path resolution).
pub fn resolve_includes(parsed: &mut super::parse::ParsedSpec, spec_path: &Path) -> Result<(), SpecError> {
    let spec = &mut parsed.spec;
    let includes = match spec.include.take() {
        Some(inc) if !inc.is_empty() => inc,
        _ => return Ok(()),
    };

    let spec_dir = spec_path
        .parent()
        .unwrap_or_else(|| Path::new("."));

    let mut errs = SpecErrors::new();
    // Register the main spec source for any errors on the main file
    errs.add_source(&parsed.file_name, &parsed.source);

    for (idx, entry) in includes.iter().enumerate() {
        let fragment_path = spec_dir.join(&entry.path);
        let label = YamlPath::root().include_entry(idx, &entry.path);

        // Read fragment file
        let raw_yaml = match fs::read_to_string(&fragment_path) {
            Ok(s) => s,
            Err(e) => {
                errs.error(label.clone(), format!("file not found: {}", e));
                continue;
            }
        };

        // Variable substitution
        let substituted = match substitute_variables(&raw_yaml, &entry.values, &label) {
            Ok(s) => s,
            Err(e) => {
                errs.error(label.clone(), format!("{}", e));
                continue;
            }
        };

        // Parse fragment
        let mut fragment: SpecFile = match serde_saphyr::from_str(&substituted) {
            Ok(f) => f,
            Err(e) => {
                errs.error(label.clone(), format!("YAML parse error:\n{}", super::parse::render_yaml_error(e)));
                continue;
            }
        };

        // Register fragment source for span resolution
        let frag_source_id = errs.add_source(&entry.path, &substituted);

        // Validate fragment structure
        validate_fragment_structure(&fragment, &label, frag_source_id, &mut errs);
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
                    errs.error_in(
                        frag_source_id,
                        label.key("workloads").key(&wid),
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
                    errs.error_in(
                        frag_source_id,
                        label.key("services").key(&sid),
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
    path_label: &YamlPath,
) -> Result<String, SpecError> {
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
                    return Err(SpecError::Validation {
                        message: format!(
                            "{} — unclosed variable reference starting at position {}",
                            path_label, start
                        ),
                    });
                }
                match values.get(&var_name) {
                    Some(val) => result.push_str(val),
                    None => {
                        return Err(SpecError::Validation {
                            message: format!(
                                "{} — undefined variable '{}'",
                                path_label, var_name
                            ),
                        });
                    }
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
fn validate_fragment_structure(spec: &SpecFile, label: &YamlPath, source_id: SourceId, errs: &mut SpecErrors) {
    if spec.api_version != "v1" {
        errs.error_in(
            source_id,
            label.clone(),
            format!("unrecognized apiVersion '{}' (expected 'v1')", spec.api_version),
        );
    }
    if spec.kind != "WorkloadFragment" {
        errs.error_in(
            source_id,
            label.clone(),
            format!("expected kind 'WorkloadFragment', got '{}'", spec.kind),
        );
    }
    if spec.metadata.is_some() {
        errs.error_in(source_id, label.clone(), "fragments cannot have 'metadata'");
    }
    if spec.network.is_some() {
        errs.error_in(source_id, label.clone(), "fragments cannot have 'network'");
    }
    if spec.defaults.is_some() {
        errs.error_in(source_id, label.clone(), "fragments cannot have 'defaults'");
    }
    if spec.include.is_some() {
        errs.error_in(source_id, label.clone(), "fragments cannot have 'include' (no recursion)");
    }

    // Must have at least one workload
    match &spec.workloads {
        None => {
            errs.error_in(source_id, label.clone(), "fragment must have at least one workload");
        }
        Some(workloads) if workloads.is_empty() => {
            errs.error_in(source_id, label.clone(), "fragment must have at least one workload");
        }
        Some(workloads) => {
            let workload_keys: HashSet<&str> = workloads.keys().map(|k| k.as_str()).collect();

            for (wid, wl) in workloads {
                let wl_path = label.key("workloads").key(wid);
                if wl.containers.is_empty() {
                    errs.error_in(
                        source_id,
                        wl_path.key("containers"),
                        "containers list is empty",
                    );
                }
                for (i, c) in wl.containers.iter().enumerate() {
                    if c.image.is_empty() {
                        errs.error_in(
                            source_id,
                            wl_path.key("containers").index(i).key("image"),
                            "image is empty",
                        );
                    }
                }
            }

            // Validate top-level service workload refs within fragment
            if let Some(ref services) = spec.services {
                for (sid, svc) in services {
                    if !workload_keys.contains(svc.workload.as_str()) {
                        errs.error_in(
                            source_id,
                            label.key("services").key(sid),
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
