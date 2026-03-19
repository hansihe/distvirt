use std::path::Path;

use anyhow::Context;

use super::types::SpecFile;

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

/// Try to parse a file as a native distvirt spec.
/// Returns None if the file doesn't look like a native spec (no `kind` field
/// or kind is not Namespace/WorkloadFragment).
pub fn try_parse(path: &Path) -> anyhow::Result<Option<SpecFile>> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("reading spec file '{}'", path.display()))?;

    // Quick check: does it look like a native spec?
    let probe: serde_yaml::Value = serde_yaml::from_str(&contents)
        .with_context(|| format!("parsing YAML from '{}'", path.display()))?;

    let kind = probe.get("kind").and_then(|v| v.as_str());
    match kind {
        Some("Namespace") | Some("WorkloadFragment") => {}
        _ => return Ok(None),
    }

    let spec: SpecFile = serde_yaml::from_str(&contents)
        .with_context(|| format!("parsing native spec from '{}'", path.display()))?;

    Ok(Some(spec))
}
