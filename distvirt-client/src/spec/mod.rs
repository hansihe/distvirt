pub(crate) mod convert;
pub(crate) mod helpers;
pub(crate) mod includes;
pub(crate) mod ip_alloc;
pub(crate) mod parse;
pub(crate) mod path;
pub(crate) mod snippet;
pub(crate) mod types;

use std::path::Path;

use distvirt_client_protocol::NamespaceSpec;

use crate::errors::SpecError;

/// Find the spec file to use. Checks distvirt.yaml, distvirt.yml in the current directory.
pub fn find_default_file() -> Result<std::path::PathBuf, SpecError> {
    for candidate in &["distvirt.yaml", "distvirt.yml"] {
        let p = std::path::PathBuf::from(candidate);
        if p.exists() {
            return Ok(p);
        }
    }
    Err(SpecError::Resolution {
        message: "no spec file found (looked for distvirt.yaml, distvirt.yml). Use -f to specify a file.".into(),
    })
}

/// Parse a spec file and return (optional namespace_id, NamespaceSpec).
pub fn parse_spec_file(file: &Path) -> Result<(Option<String>, NamespaceSpec), SpecError> {
    if let Some(mut native) = parse::try_parse(file)? {
        includes::resolve_includes(&mut native, file)?;
        let (ns_id, proto_spec) = convert::spec_to_namespace_spec(&native)?;
        return Ok((ns_id, proto_spec));
    }

    Err(SpecError::Resolution {
        message: format!("failed to parse spec file '{}'", file.display()),
    })
}

/// Resolve namespace ID from explicit arg or spec file, and parse the spec.
pub fn resolve_spec(
    namespace_id: Option<&str>,
    file: Option<&Path>,
) -> Result<(String, NamespaceSpec), SpecError> {
    let file = match file {
        Some(f) => f.to_path_buf(),
        None => find_default_file()?,
    };

    let (spec_ns_id, spec) = parse_spec_file(&file)?;

    let namespace_id = match namespace_id {
        Some(id) => id.to_string(),
        None => spec_ns_id.ok_or_else(|| SpecError::Resolution {
            message: "namespace ID required: specify as argument or set metadata.name in spec file".into(),
        })?,
    };

    Ok((namespace_id, spec))
}
