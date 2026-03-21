use std::sync::Arc;

use prost::Message;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio::sync::Mutex;
use tonic::Streaming;

use distvirt_client::connection::handle_grpc_error;
use distvirt_client_protocol as proto;

use crate::ApiError;

// ---------------------------------------------------------------------------
// EventStream
// ---------------------------------------------------------------------------

#[pyclass(name = "EventStream")]
pub struct PyEventStream {
    stream: Arc<Mutex<Option<Streaming<proto::NamespaceEvent>>>>,
}

impl PyEventStream {
    pub fn new(stream: Streaming<proto::NamespaceEvent>) -> Self {
        PyEventStream {
            stream: Arc::new(Mutex::new(Some(stream))),
        }
    }
}

#[pymethods]
impl PyEventStream {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let stream = Arc::clone(&self.stream);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = stream.lock().await;
            let s: &mut Streaming<proto::NamespaceEvent> = guard
                .as_mut()
                .ok_or_else(|| ApiError::new_err("event stream is closed"))?;

            match s.message().await {
                Ok(Some(event)) => {
                    let bytes = event.encode_to_vec();
                    Ok(Some(bytes))
                }
                Ok(None) => Err(pyo3::exceptions::PyStopAsyncIteration::new_err(())),
                Err(status) => {
                    let err = handle_grpc_error(status);
                    Err(ApiError::new_err(format!("{err}")))
                }
            }
        })
    }
}

// ---------------------------------------------------------------------------
// LogStream
// ---------------------------------------------------------------------------

#[pyclass(name = "LogStream")]
pub struct PyLogStream {
    stream: Arc<Mutex<Option<Streaming<proto::LogChunk>>>>,
}

impl PyLogStream {
    pub fn new(stream: Streaming<proto::LogChunk>) -> Self {
        PyLogStream {
            stream: Arc::new(Mutex::new(Some(stream))),
        }
    }
}

/// Carries LogChunk data across the async boundary, converted to Python dict
/// via IntoPyObject.
struct LogChunkData {
    workload_id: String,
    data: Vec<u8>,
    timestamp_ms: i64,
    container_id: String,
}

impl<'py> IntoPyObject<'py> for LogChunkData {
    type Target = PyDict;
    type Output = Bound<'py, PyDict>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let dict = PyDict::new(py);
        dict.set_item("workload_id", &self.workload_id)?;
        dict.set_item("data", &self.data[..])?;
        dict.set_item("timestamp_ms", self.timestamp_ms)?;
        dict.set_item("container_id", &self.container_id)?;
        Ok(dict)
    }
}

#[pymethods]
impl PyLogStream {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let stream = Arc::clone(&self.stream);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = stream.lock().await;
            let s: &mut Streaming<proto::LogChunk> = guard
                .as_mut()
                .ok_or_else(|| ApiError::new_err("log stream is closed"))?;

            match s.message().await {
                Ok(Some(chunk)) => Ok(Some(LogChunkData {
                    workload_id: chunk.workload_id,
                    data: chunk.data,
                    timestamp_ms: chunk.timestamp_unix_ms,
                    container_id: chunk.container_id,
                })),
                Ok(None) => Err(pyo3::exceptions::PyStopAsyncIteration::new_err(())),
                Err(status) => {
                    let err = handle_grpc_error(status);
                    Err(ApiError::new_err(format!("{err}")))
                }
            }
        })
    }
}
