use std::sync::atomic::{AtomicU64, Ordering};
/// Tracks activity and in-flight operations for convergence detection.
///
/// Used by the test harness to detect quiescence: the system has converged
/// when `activity_count()` stops changing AND `is_busy()` returns false.
///
/// Both the orchestrator shell and worker(s) share `ActivityTracker` instances.
/// The test harness's `converge()` checks all of them.
pub struct ActivityTracker {
    /// Monotonically increasing counter — bumped whenever something happens.
    activity: AtomicU64,
    /// Number of in-flight operations (volume prep, VM boot, etc.).
    busy: AtomicU64,
    /// Woken on every tick so waiters (like `converge()`) can re-check state.
    notify: tokio::sync::Notify,
}

impl ActivityTracker {
    pub fn new() -> Self {
        ActivityTracker {
            activity: AtomicU64::new(0),
            busy: AtomicU64::new(0),
            notify: tokio::sync::Notify::new(),
        }
    }

    /// Record that something happened. Causes `converge()` to keep waiting.
    pub fn tick(&self) {
        self.activity.fetch_add(1, Ordering::Relaxed);
        self.notify.notify_waiters();
    }

    /// Mark the start of a long-running operation. Returns an RAII guard
    /// that decrements the busy count on drop (even on panic/cancellation).
    pub fn busy_guard(&self) -> BusyGuard<'_> {
        self.busy.fetch_add(1, Ordering::Relaxed);
        self.tick();
        BusyGuard { tracker: self }
    }

    /// Current activity count (monotonically increasing).
    pub fn activity_count(&self) -> u64 {
        self.activity.load(Ordering::Relaxed)
    }

    /// Whether any operations are currently in flight.
    pub fn is_busy(&self) -> bool {
        self.busy.load(Ordering::Relaxed) > 0
    }

    /// Wait until the next tick. Useful for convergence loops that want to
    /// sleep until something changes rather than polling.
    pub async fn notified(&self) {
        self.notify.notified().await;
    }
}

impl Default for ActivityTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII guard that keeps the tracker in the "busy" state until dropped.
pub struct BusyGuard<'a> {
    tracker: &'a ActivityTracker,
}

impl Drop for BusyGuard<'_> {
    fn drop(&mut self) {
        self.tracker.busy.fetch_sub(1, Ordering::Relaxed);
        self.tracker.tick();
    }
}
