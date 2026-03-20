use std::path::Path;

use prost::Message;
use pyo3::prelude::*;

/// Parse a distvirt spec file and return the serialized protobuf NamespaceSpec bytes.
///
/// This wraps the same spec parsing pipeline used by the CLI:
/// try_parse → resolve_includes → spec_to_namespace_spec → serialize to proto bytes.
#[pyfunction]
#[pyo3(signature = (path, values=None))]
fn parse_spec(
    path: &str,
    values: Option<std::collections::HashMap<String, String>>,
) -> PyResult<(Option<String>, Vec<u8>)> {
    // TODO: Wire up `values` — currently values are only read from the YAML
    // include entries. Needs an API addition to distvirt-spec to accept
    // caller-provided values for variable substitution.
    let _ = values;

    let file_path = Path::new(path);

    let mut parsed = distvirt_client::try_parse(file_path)
        .map_err(to_value_error)?
        .ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "'{}' is not a native distvirt spec file",
                path
            ))
        })?;

    distvirt_client::resolve_includes(&mut parsed, file_path).map_err(to_value_error)?;

    let (ns_id, proto_spec) =
        distvirt_client::spec_to_namespace_spec(&parsed).map_err(to_value_error)?;

    let bytes = proto_spec.encode_to_vec();

    Ok((ns_id, bytes))
}

fn to_value_error(err: anyhow::Error) -> PyErr {
    pyo3::exceptions::PyValueError::new_err(format!("{err:#}"))
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_spec, m)?)?;
    Ok(())
}
