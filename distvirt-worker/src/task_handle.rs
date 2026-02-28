use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::task::JoinHandle;

/// A `JoinHandle` wrapper that aborts the task when dropped.
///
/// This ensures spawned tasks have a clear owner. When the owner goes away
/// (scope exit, struct drop, timeout expiry), the task is automatically
/// cancelled. This is analogous to Erlang process links.
///
/// To intentionally detach a task (let it run independently), call
/// [`detach()`](TaskHandle::detach) to get back the raw `JoinHandle`.
pub struct TaskHandle<T> {
    handle: Option<JoinHandle<T>>,
}

impl<T> TaskHandle<T> {
    /// Wrap an existing `JoinHandle`.
    pub fn new(handle: JoinHandle<T>) -> Self {
        TaskHandle {
            handle: Some(handle),
        }
    }

    /// Spawn a new tokio task and return a `TaskHandle` that owns it.
    pub fn spawn<F>(future: F) -> Self
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        Self::new(tokio::spawn(future))
    }

    /// Explicitly abort the task.
    pub fn abort(&self) {
        if let Some(ref handle) = self.handle {
            handle.abort();
        }
    }

    /// Detach the task, allowing it to run independently.
    ///
    /// Returns the underlying `JoinHandle`. The task will no longer be
    /// aborted when this `TaskHandle` is dropped.
    pub fn detach(mut self) -> JoinHandle<T> {
        self.handle.take().expect("handle already taken")
    }
}

impl<T> Future for TaskHandle<T> {
    type Output = Result<T, tokio::task::JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(self.handle.as_mut().expect("polled after completion")).poll(cx)
    }
}

impl<T> Drop for TaskHandle<T> {
    fn drop(&mut self) {
        if let Some(ref handle) = self.handle {
            handle.abort();
        }
    }
}
