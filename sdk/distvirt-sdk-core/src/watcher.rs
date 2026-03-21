use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio::sync::Mutex;

use distvirt_client::model::StateChange;
use distvirt_client::watcher::NamespaceWatcher;

use crate::{sdk_workload_state_name, sdk_service_state_name, ApiError, PyNamespaceModel};

#[pyclass(name = "NamespaceWatcher")]
pub struct PyNamespaceWatcher {
    watcher: Arc<Mutex<Option<NamespaceWatcher>>>,
}

impl PyNamespaceWatcher {
    pub fn new(watcher: NamespaceWatcher) -> Self {
        PyNamespaceWatcher {
            watcher: Arc::new(Mutex::new(Some(watcher))),
        }
    }
}

/// Carries a StateChange across the async boundary as plain Rust data.
struct StateChangeData {
    entries: Vec<(&'static str, StateChangeValue)>,
}

enum StateChangeValue {
    Str(String),
    StaticStr(&'static str),
    OptStr(Option<String>),
    U32(u32),
}

impl<'py> IntoPyObject<'py> for StateChangeData {
    type Target = PyDict;
    type Output = Bound<'py, PyDict>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let dict = PyDict::new(py);
        for (key, val) in self.entries {
            match val {
                StateChangeValue::Str(s) => dict.set_item(key, s)?,
                StateChangeValue::StaticStr(s) => dict.set_item(key, s)?,
                StateChangeValue::OptStr(s) => dict.set_item(key, s)?,
                StateChangeValue::U32(n) => dict.set_item(key, n)?,
            }
        }
        Ok(dict)
    }
}

fn state_change_to_data(change: &StateChange) -> StateChangeData {
    use StateChangeValue::*;

    let entries = match change {
        StateChange::WorkloadStateChanged {
            workload_id,
            old,
            new,
        } => vec![
            ("type", StaticStr("workload_state_changed")),
            ("workload_id", Str(workload_id.clone())),
            ("old_state", Str(sdk_workload_state_name(old))),
            ("new_state", Str(sdk_workload_state_name(new))),
        ],
        StateChange::WorkloadSpliced {
            workload_id,
            worker_id,
        } => vec![
            ("type", StaticStr("workload_spliced")),
            ("workload_id", Str(workload_id.clone())),
            ("worker_id", Str(worker_id.clone())),
        ],
        StateChange::WorkloadUnspliced { workload_id } => vec![
            ("type", StaticStr("workload_unspliced")),
            ("workload_id", Str(workload_id.clone())),
        ],
        StateChange::WorkloadDemandChanged {
            workload_id,
            demanding_services,
        } => vec![
            ("type", StaticStr("workload_demand_changed")),
            ("workload_id", Str(workload_id.clone())),
            ("demanding_services", U32(*demanding_services)),
        ],
        StateChange::PodCreated {
            pod_id,
            workload_id,
        } => vec![
            ("type", StaticStr("pod_created")),
            ("pod_id", Str(pod_id.clone())),
            ("workload_id", Str(workload_id.clone())),
        ],
        StateChange::PodStateChanged {
            pod_id,
            workload_id,
            new_state,
        } => vec![
            ("type", StaticStr("pod_state_changed")),
            ("pod_id", Str(pod_id.clone())),
            ("workload_id", Str(workload_id.clone())),
            ("new_state", StaticStr(new_state.label())),
        ],
        StateChange::PodReaped {
            pod_id,
            workload_id,
        } => vec![
            ("type", StaticStr("pod_reaped")),
            ("pod_id", Str(pod_id.clone())),
            ("workload_id", Str(workload_id.clone())),
        ],
        StateChange::Endpoint {
            endpoint_id,
            service_id,
            workload_id,
        } => vec![
            ("type", StaticStr("endpoint")),
            ("endpoint_id", Str(endpoint_id.clone())),
            ("service_id", OptStr(service_id.clone())),
            ("workload_id", OptStr(workload_id.clone())),
        ],
    };

    StateChangeData { entries }
}

#[pymethods]
impl PyNamespaceWatcher {
    /// Advance to the next state change. Returns a dict describing the change,
    /// or None when the stream ends.
    fn next<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let watcher = Arc::clone(&self.watcher);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = watcher.lock().await;
            let w = guard
                .as_mut()
                .ok_or_else(|| ApiError::new_err("watcher is closed"))?;

            match w.next().await {
                Ok(Some(change)) => Ok(Some(state_change_to_data(&change))),
                Ok(None) => Ok(None),
                Err(e) => Err(ApiError::new_err(format!("{e}"))),
            }
        })
    }

    /// Wait until a workload reaches the given state.
    fn wait_for_workload_state<'py>(
        &self,
        py: Python<'py>,
        workload_id: String,
        state: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let watcher = Arc::clone(&self.watcher);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = watcher.lock().await;
            let w = guard
                .as_mut()
                .ok_or_else(|| ApiError::new_err("watcher is closed"))?;

            w.wait_for(|model| {
                model
                    .workloads
                    .get(&workload_id)
                    .is_some_and(|wl| sdk_workload_state_name(&wl.state) == state)
            })
            .await
            .map_err(|e| ApiError::new_err(format!("{e}")))?;
            Ok(())
        })
    }

    /// Wait until a service reaches the given state.
    fn wait_for_service_state<'py>(
        &self,
        py: Python<'py>,
        service_id: String,
        state: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let watcher = Arc::clone(&self.watcher);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = watcher.lock().await;
            let w = guard
                .as_mut()
                .ok_or_else(|| ApiError::new_err("watcher is closed"))?;

            w.wait_for(|model| {
                model
                    .services
                    .get(&service_id)
                    .is_some_and(|svc| sdk_service_state_name(&svc.state) == state)
            })
            .await
            .map_err(|e| ApiError::new_err(format!("{e}")))?;
            Ok(())
        })
    }

    /// Return a snapshot of the current namespace model.
    fn model(&self) -> PyResult<PyNamespaceModel> {
        let watcher = Arc::clone(&self.watcher);
        let rt = pyo3_async_runtimes::tokio::get_runtime();
        let guard = rt.block_on(watcher.lock());
        let w = guard
            .as_ref()
            .ok_or_else(|| ApiError::new_err("watcher is closed"))?;
        Ok(PyNamespaceModel {
            inner: w.model().clone(),
        })
    }

    /// Close the watcher, dropping the event stream.
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let watcher = Arc::clone(&self.watcher);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = watcher.lock().await;
            guard.take();
            Ok(())
        })
    }
}
