use std::path::Path;

use anyhow::bail;
use distvirt_client_protocol::NamespaceSpec;

/// Find the spec file to use. Checks distvirt.yaml, distvirt.yml in the current directory.
pub fn find_default_file() -> anyhow::Result<std::path::PathBuf> {
    for candidate in &["distvirt.yaml", "distvirt.yml"] {
        let p = std::path::PathBuf::from(candidate);
        if p.exists() {
            return Ok(p);
        }
    }
    bail!(
        "no spec file found (looked for distvirt.yaml, distvirt.yml). Use -f to specify a file."
    )
}

/// Parse a spec file and return (optional namespace_id, NamespaceSpec).
pub fn parse_spec_file(file: &Path) -> anyhow::Result<(Option<String>, NamespaceSpec)> {
    if let Some(mut native) = crate::try_parse(file)? {
        crate::resolve_includes(&mut native, file)?;
        let (ns_id, proto_spec) = crate::spec_to_namespace_spec(&native)?;
        return Ok((ns_id, proto_spec));
    }

    bail!("failed to parse spec file '{}'", file.display())
}

/// Resolve namespace ID from explicit arg or spec file, and parse the spec.
pub fn resolve_spec(
    namespace_id: Option<&str>,
    file: Option<&Path>,
) -> anyhow::Result<(String, NamespaceSpec)> {
    let file = match file {
        Some(f) => f.to_path_buf(),
        None => find_default_file()?,
    };

    let (spec_ns_id, spec) = parse_spec_file(&file)?;

    let namespace_id = match namespace_id {
        Some(id) => id.to_string(),
        None => spec_ns_id.ok_or_else(|| {
            anyhow::anyhow!(
                "namespace ID required: specify as argument or set metadata.name in spec file"
            )
        })?,
    };

    Ok((namespace_id, spec))
}
