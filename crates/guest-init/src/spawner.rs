use std::future::Future;

/// An opaque handle to a spawned task.
///
/// Dropping the handle cancels/aborts the underlying task. This provides
/// runtime-agnostic task lifecycle management:
/// - For `async-executor`: wraps `Task<()>` (cancelled on drop)
/// - For tokio: wraps `AbortOnDrop(JoinHandle)` (aborted on drop)
pub struct TaskHandle {
    _inner: Box<dyn std::any::Any + Send>,
}

impl TaskHandle {
    pub fn new<T: Send + 'static>(inner: T) -> Self {
        TaskHandle {
            _inner: Box::new(inner),
        }
    }
}

/// Trait for spawning futures on the async executor.
///
/// Returns a `TaskHandle` that cancels the spawned task on drop. This is
/// used for per-connection tasks (yamux driver, output drains, stdin relays)
/// that must be cancelled when the connection drops.
///
/// Production: wraps `async_executor::LocalExecutor::spawn()`.
/// Tests: wraps `tokio::spawn()` with abort-on-drop.
pub trait LocalSpawner {
    fn spawn_local<F: Future<Output = ()> + Send + 'static>(&self, f: F) -> TaskHandle;
}

impl LocalSpawner for async_executor::LocalExecutor<'_> {
    fn spawn_local<F: Future<Output = ()> + Send + 'static>(&self, f: F) -> TaskHandle {
        TaskHandle::new(self.spawn(f))
    }
}
