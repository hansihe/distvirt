use std::path::Path;

use anyhow::Context;
use serde::Deserialize;

use super::types::SpecFile;

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct SpecProbe {
    kind: Option<String>,
}

/// A parsed spec file with its source content and file path preserved
/// for error reporting with source snippets.
pub struct ParsedSpec {
    pub spec: SpecFile,
    pub source: String,
    pub file_name: String,
}

/// Format a serde-saphyr error with source snippets when available.
pub(super) fn render_yaml_error(err: serde_saphyr::Error) -> String {
    err.render()
}

/// Try to parse a file as a native distvirt spec.
/// Returns None if the file doesn't look like a native spec (no `kind` field
/// or kind is not Namespace/WorkloadFragment).
pub fn try_parse(path: &Path) -> anyhow::Result<Option<ParsedSpec>> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("reading spec file '{}'", path.display()))?;

    // Quick check: does it look like a native spec?
    let probe: SpecProbe = serde_saphyr::from_str(&contents)
        .map_err(|e| anyhow::anyhow!("{}: {}", path.display(), render_yaml_error(e)))?;

    match probe.kind.as_deref() {
        Some("Namespace") | Some("WorkloadFragment") => {}
        _ => return Ok(None),
    }

    let spec: SpecFile = serde_saphyr::from_str(&contents)
        .map_err(|e| anyhow::anyhow!("{}: {}", path.display(), render_yaml_error(e)))?;

    Ok(Some(ParsedSpec {
        spec,
        source: contents,
        file_name: path.display().to_string(),
    }))
}
