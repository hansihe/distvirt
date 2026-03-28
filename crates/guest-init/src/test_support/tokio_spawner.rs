//! Tokio-based `LocalSpawner` implementation for test use.

use std::future::Future;

use crate::spawner::{LocalSpawner, TaskHandle};

/// `LocalSpawner` implementation using `tokio::spawn`.
///
/// For use in tests running on a tokio runtime. Since the supervisor is
/// Send-compatible, we can use regular `tokio::spawn` instead of `spawn_local`.
pub struct TokioSpawner;

/// Wrapper around `tokio::task::JoinHandle` that aborts the task on drop.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl LocalSpawner for TokioSpawner {
    fn spawn_local<F: Future<Output = ()> + Send + 'static>(&self, f: F) -> TaskHandle {
        let handle = tokio::spawn(f);
        TaskHandle::new(AbortOnDrop(handle))
    }
}
