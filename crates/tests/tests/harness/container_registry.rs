//! Shared registry of pod container handles for test control.
//!
//! When `GuestInitVmm` launches a pod, it registers a `BackendHandle` here
//! keyed by `PodId`. Tests retrieve handles via `TestCluster::pod_handle()`
//! which resolves namespace+workload → PodId and looks up the registry.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::Notify;

use distvirt_worker_protocol::PodId;
use guest_init::test_support::BackendHandle;

#[derive(Clone)]
pub struct ContainerRegistry {
    inner: Arc<Mutex<HashMap<PodId, BackendHandle>>>,
    notify: Arc<Notify>,
}

impl ContainerRegistry {
    pub fn new() -> Self {
        ContainerRegistry {
            inner: Arc::new(Mutex::new(HashMap::new())),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Register a pod's BackendHandle. Called by GuestInitVmm during launch.
    pub fn register(&self, pod_id: PodId, handle: BackendHandle) {
        self.inner.lock().insert(pod_id, handle);
        self.notify.notify_waiters();
    }

    /// Get handle if registered.
    pub fn get(&self, pod_id: &PodId) -> Option<BackendHandle> {
        self.inner.lock().get(pod_id).cloned()
    }

    /// Wait for a pod to appear in the registry, then return its handle.
    pub async fn wait_for(&self, pod_id: &PodId) -> BackendHandle {
        loop {
            if let Some(h) = self.get(pod_id) {
                return h;
            }
            self.notify.notified().await;
        }
    }
}
