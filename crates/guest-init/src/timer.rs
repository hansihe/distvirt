//! Feature-gated sleep function.
//!
//! Uses `tokio::time::sleep` when `test-support` is enabled (integrates with
//! tokio's fake-time / `start_paused`), and `futures_timer::Delay` in
//! production (runtime-agnostic, no tokio dependency).

use std::time::Duration;

#[cfg(feature = "test-support")]
pub async fn sleep(duration: Duration) {
    tokio::time::sleep(duration).await;
}

#[cfg(not(feature = "test-support"))]
pub async fn sleep(duration: Duration) {
    futures_timer::Delay::new(duration).await;
}
