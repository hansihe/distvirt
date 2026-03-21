use std::path::Path;

use prost::Message;
use pyo3::create_exception;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use distvirt_client::model;
use distvirt_client_protocol as proto;

mod client;
mod network;
pub(crate) mod stream;
pub(crate) mod watcher;

// ---------------------------------------------------------------------------
// Python exception hierarchy (mirrors distvirt-client error types)
// ---------------------------------------------------------------------------

create_exception!(distvirt._core, DistvirtError, pyo3::exceptions::PyException);
create_exception!(distvirt._core, SpecError, DistvirtError);
create_exception!(distvirt._core, ConnectionError, DistvirtError);
create_exception!(distvirt._core, ApiError, DistvirtError);

// ---------------------------------------------------------------------------
// Spec parsing (existing)
// ---------------------------------------------------------------------------

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
        .map_err(|e: distvirt_client::SpecError| SpecError::new_err(format!("{e}")))?
        .ok_or_else(|| {
            SpecError::new_err(format!(
                "'{}' is not a native distvirt spec file",
                path
            ))
        })?;

    distvirt_client::resolve_includes(&mut parsed, file_path)
        .map_err(|e: distvirt_client::SpecError| SpecError::new_err(format!("{e}")))?;

    let (ns_id, proto_spec) =
        distvirt_client::spec_to_namespace_spec(&parsed)
            .map_err(|e: distvirt_client::SpecError| SpecError::new_err(format!("{e}")))?;

    let bytes = proto_spec.encode_to_vec();

    Ok((ns_id, bytes))
}

// ---------------------------------------------------------------------------
// Connection resolution
// ---------------------------------------------------------------------------

/// Resolve connection parameters using the same precedence as the CLI:
/// explicit args > env vars (DV_SERVER, DV_TOKEN) > credentials file > defaults.
///
/// Returns (server_url, optional_token).
#[pyfunction]
#[pyo3(signature = (server=None, token=None, context=None))]
fn resolve_connection(
    server: Option<String>,
    token: Option<String>,
    context: Option<String>,
) -> PyResult<(String, Option<String>)> {
    let params = distvirt_client::connection::resolve(
        distvirt_client::connection::ConnectionOverrides {
            server,
            token,
            context,
        },
    )
    .map_err(|e: distvirt_client::ConnectionError| ConnectionError::new_err(format!("{e}")))?;
    Ok((params.server, params.token))
}

// ---------------------------------------------------------------------------
// NamespaceModel
// ---------------------------------------------------------------------------

/// Live namespace state model backed by the same Rust implementation used by the CLI.
///
/// Bootstrap from a serialized NamespaceStatusReport, then apply serialized
/// NamespaceEvent messages to keep it up to date.
#[pyclass(name = "NamespaceModel")]
pub(crate) struct PyNamespaceModel {
    pub(crate) inner: model::NamespaceModel,
}

#[pymethods]
impl PyNamespaceModel {
    /// Create a model from a serialized NamespaceStatusReport (protobuf bytes).
    #[staticmethod]
    fn from_status_bytes(proto_bytes: &[u8]) -> PyResult<Self> {
        let status = proto::NamespaceStatusReport::decode(proto_bytes).map_err(|e| {
            ApiError::new_err(format!("failed to decode status: {e}"))
        })?;
        Ok(Self {
            inner: model::NamespaceModel::from_status(&status),
        })
    }

    /// Apply a serialized NamespaceEvent. Returns True if the model changed.
    fn apply_event_bytes(&mut self, proto_bytes: &[u8]) -> PyResult<bool> {
        let event = proto::NamespaceEvent::decode(proto_bytes).map_err(|e| {
            ApiError::new_err(format!("failed to decode event: {e}"))
        })?;
        Ok(self.inner.apply_event(&event).is_some())
    }

    #[getter]
    fn namespace_id(&self) -> &str {
        &self.inner.namespace_id
    }

    #[getter]
    fn namespace_state(&self) -> &str {
        self.inner.state.label()
    }

    /// Return list of workload IDs in the model.
    fn workload_ids(&self) -> Vec<String> {
        self.inner.workloads.keys().cloned().collect()
    }

    /// Return list of service IDs in the model.
    fn service_ids(&self) -> Vec<String> {
        self.inner.services.keys().cloned().collect()
    }

    /// Return the simplified state name for a workload, or None if not present.
    fn workload_state(&self, workload_id: &str) -> Option<String> {
        self.inner
            .workloads
            .get(workload_id)
            .map(|w| sdk_workload_state_name(&w.state))
    }

    /// Return a dict with full workload info, or None if not present.
    fn workload_info<'py>(
        &self,
        py: Python<'py>,
        workload_id: &str,
    ) -> PyResult<Option<Bound<'py, PyDict>>> {
        let Some(w) = self.inner.workloads.get(workload_id) else {
            return Ok(None);
        };
        let dict = PyDict::new(py);
        dict.set_item("workload_id", workload_id)?;
        dict.set_item("state", sdk_workload_state_name(&w.state))?;

        match &w.state {
            model::WorkloadState::Launching { pod_id, worker_id }
            | model::WorkloadState::Running { pod_id, worker_id }
            | model::WorkloadState::Suspending { pod_id, worker_id } => {
                dict.set_item("pod_id", pod_id)?;
                dict.set_item("worker_id", worker_id)?;
            }
            _ => {
                dict.set_item("pod_id", py.None())?;
                dict.set_item("worker_id", py.None())?;
            }
        }
        dict.set_item("spliced", w.spliced)?;
        dict.set_item("ip", w.ip.as_deref())?;
        dict.set_item("demanding_services", w.demanding_services)?;
        Ok(Some(dict))
    }

    /// Return the simplified state name for a service, or None if not present.
    fn service_state(&self, service_id: &str) -> Option<String> {
        self.inner
            .services
            .get(service_id)
            .map(|s| sdk_service_state_name(&s.state))
    }

    /// Return a dict with full service info, or None if not present.
    fn service_info<'py>(
        &self,
        py: Python<'py>,
        service_id: &str,
    ) -> PyResult<Option<Bound<'py, PyDict>>> {
        let Some(s) = self.inner.services.get(service_id) else {
            return Ok(None);
        };
        let dict = PyDict::new(py);
        dict.set_item("service_id", service_id)?;
        dict.set_item("workload_id", &s.workload_id)?;
        dict.set_item("state", sdk_service_state_name(&s.state))?;
        dict.set_item("ip", s.ip.as_deref())?;
        dict.set_item("activation_enabled", s.activation_enabled)?;
        dict.set_item("spliced", s.spliced)?;
        Ok(Some(dict))
    }
}

/// Map Rust WorkloadState to the simplified SDK state names used by Python matchers.
pub(crate) fn sdk_workload_state_name(state: &model::WorkloadState) -> String {
    match state {
        model::WorkloadState::Dormant | model::WorkloadState::WaitingForSpec => "dormant".into(),
        model::WorkloadState::Launching { .. } => "launching".into(),
        model::WorkloadState::Running { .. } => "running".into(),
        model::WorkloadState::Suspending { .. } => "suspending".into(),
        model::WorkloadState::Suspended => "suspended".into(),
        model::WorkloadState::RetryBackoff | model::WorkloadState::Failed { .. } => {
            "failed".into()
        }
        model::WorkloadState::Completed { .. } => "completed".into(),
    }
}

/// Map Rust ServiceState to the simplified SDK state names used by Python matchers.
pub(crate) fn sdk_service_state_name(state: &model::ServiceState) -> String {
    match state {
        model::ServiceState::Pending | model::ServiceState::Idle => "idle".into(),
        model::ServiceState::NeedBackend | model::ServiceState::Active { .. } => "active".into(),
    }
}

// ---------------------------------------------------------------------------
// Module
// ---------------------------------------------------------------------------

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_spec, m)?)?;
    m.add_function(wrap_pyfunction!(resolve_connection, m)?)?;
    m.add_class::<PyNamespaceModel>()?;
    m.add_class::<client::PyClient>()?;
    m.add_class::<watcher::PyNamespaceWatcher>()?;
    m.add_class::<stream::PyEventStream>()?;
    m.add_class::<stream::PyLogStream>()?;
    m.add_class::<network::PyUserspaceNetwork>()?;
    m.add_class::<network::PyTcpStream>()?;
    m.add_class::<network::PyUdpSocket>()?;

    // Exception hierarchy
    m.add("DistvirtError", m.py().get_type::<DistvirtError>())?;
    m.add("SpecError", m.py().get_type::<SpecError>())?;
    m.add("ConnectionError", m.py().get_type::<ConnectionError>())?;
    m.add("ApiError", m.py().get_type::<ApiError>())?;

    Ok(())
}
