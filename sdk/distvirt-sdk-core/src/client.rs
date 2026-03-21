use prost::Message;
use pyo3::prelude::*;

use distvirt_client::connection::{self, Client, ConnectionOverrides};
use distvirt_client::operations;
use distvirt_client::watcher::NamespaceWatcher;
use distvirt_client_protocol::NamespaceSpec;

use crate::stream::{PyEventStream, PyLogStream};
use crate::watcher::PyNamespaceWatcher;
use crate::{ApiError, ConnectionError};

fn map_conn_err(e: distvirt_client::ConnectionError) -> PyErr {
    ConnectionError::new_err(format!("{e}"))
}

fn map_api_err(e: distvirt_client::ApiError) -> PyErr {
    ApiError::new_err(format!("{e}"))
}

#[pyclass(name = "PyClient")]
pub struct PyClient {
    client: Option<Client>,
}

#[pymethods]
impl PyClient {
    /// Connect to a distvirt orchestrator. Returns a PyClient.
    #[staticmethod]
    #[pyo3(signature = (server=None, token=None, context=None))]
    fn connect<'py>(
        py: Python<'py>,
        server: Option<String>,
        token: Option<String>,
        context: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let params = connection::resolve(ConnectionOverrides {
                server,
                token,
                context,
            })
            .map_err(map_conn_err)?;

            let client = connection::connect(&params).await.map_err(map_conn_err)?;

            Ok(PyClient {
                client: Some(client),
            })
        })
    }

    /// Apply a namespace spec (create or patch). Returns "created" or "patched".
    fn apply<'py>(
        &self,
        py: Python<'py>,
        namespace_id: String,
        spec_bytes: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut client = self.take_client_ref()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let spec = NamespaceSpec::decode(&*spec_bytes)
                .map_err(|e| ApiError::new_err(format!("failed to decode spec: {e}")))?;
            let outcome = operations::apply(&mut client, &namespace_id, &spec)
                .await
                .map_err(map_api_err)?;
            Ok(match outcome {
                operations::ApplyOutcome::Created => "created".to_string(),
                operations::ApplyOutcome::Patched => "patched".to_string(),
            })
        })
    }

    /// Sync a namespace spec (create or full replace). Returns "created" or "synced".
    fn sync_ns<'py>(
        &self,
        py: Python<'py>,
        namespace_id: String,
        spec_bytes: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut client = self.take_client_ref()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let spec = NamespaceSpec::decode(&*spec_bytes)
                .map_err(|e| ApiError::new_err(format!("failed to decode spec: {e}")))?;
            let outcome = operations::sync(&mut client, &namespace_id, &spec)
                .await
                .map_err(map_api_err)?;
            Ok(match outcome {
                operations::SyncOutcome::Created => "created".to_string(),
                operations::SyncOutcome::Synced => "synced".to_string(),
            })
        })
    }

    /// Delete a namespace.
    fn down<'py>(&self, py: Python<'py>, namespace_id: String) -> PyResult<Bound<'py, PyAny>> {
        let mut client = self.take_client_ref()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            operations::down(&mut client, &namespace_id)
                .await
                .map_err(map_api_err)?;
            Ok(())
        })
    }

    /// Clone a namespace from source to target.
    fn clone_namespace<'py>(
        &self,
        py: Python<'py>,
        source: String,
        target: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut client = self.take_client_ref()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            operations::clone_namespace(&mut client, &source, &target)
                .await
                .map_err(map_api_err)?;
            Ok(())
        })
    }

    /// Deactivate a workload. Returns (deactivated: bool, reason: str).
    fn deactivate<'py>(
        &self,
        py: Python<'py>,
        namespace_id: String,
        workload_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut client = self.take_client_ref()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let outcome = operations::deactivate(&mut client, &namespace_id, &workload_id)
                .await
                .map_err(map_api_err)?;
            Ok((outcome.deactivated, outcome.reason))
        })
    }

    /// Get namespace status as serialized protobuf bytes.
    fn get_status<'py>(
        &self,
        py: Python<'py>,
        namespace_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut client = self.take_client_ref()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let status = operations::get_status(&mut client, &namespace_id)
                .await
                .map_err(map_api_err)?;
            Ok(status.encode_to_vec())
        })
    }

    /// List all namespaces. Returns list of (namespace_id, status_proto_bytes) tuples.
    fn list_namespaces<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let mut client = self.take_client_ref()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let resp = client
                .list_namespaces(distvirt_client_protocol::ListNamespacesRequest {})
                .await
                .map_err(|e| {
                    map_api_err(distvirt_client::connection::handle_grpc_error(e))
                })?;

            let namespaces = resp.into_inner().namespaces;
            let result: Vec<(String, Vec<u8>)> = namespaces
                .into_iter()
                .map(|ns| {
                    let id = ns.namespace_id.clone();
                    let bytes = ns.encode_to_vec();
                    (id, bytes)
                })
                .collect();
            Ok(result)
        })
    }

    /// Start a namespace watcher (subscribe + bootstrap model).
    fn start_watcher<'py>(
        &self,
        py: Python<'py>,
        namespace_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut client = self.take_client_ref()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let watcher = NamespaceWatcher::start(&mut client, &namespace_id)
                .await
                .map_err(map_api_err)?;
            Ok(PyNamespaceWatcher::new(watcher))
        })
    }

    /// Stream namespace events as an async iterator.
    #[pyo3(signature = (namespace_id, workload_ids=vec![], service_ids=vec![]))]
    fn stream_events<'py>(
        &self,
        py: Python<'py>,
        namespace_id: String,
        workload_ids: Vec<String>,
        service_ids: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut client = self.take_client_ref()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let stream =
                operations::stream_events(&mut client, &namespace_id, &workload_ids, &service_ids)
                    .await
                    .map_err(map_api_err)?;
            Ok(PyEventStream::new(stream))
        })
    }

    /// Stream logs from a namespace (optionally filtered to a workload).
    #[pyo3(signature = (namespace_id, workload_id=None))]
    fn stream_logs<'py>(
        &self,
        py: Python<'py>,
        namespace_id: String,
        workload_id: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut client = self.take_client_ref()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let stream =
                operations::stream_logs(&mut client, &namespace_id, workload_id.as_deref())
                    .await
                    .map_err(map_api_err)?;
            Ok(PyLogStream::new(stream))
        })
    }

    /// Close the client, dropping the inner gRPC connection.
    fn close(&mut self) {
        self.client = None;
    }
}

impl PyClient {
    pub(crate) fn take_client_ref(&self) -> PyResult<Client> {
        self.client
            .as_ref()
            .ok_or_else(|| ApiError::new_err("client is closed"))
            .cloned()
    }
}
