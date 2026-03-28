//! Test support utilities for guest-init integration testing.
//!
//! Requires the `test-support` feature flag.
//!
//! Provides:
//! - [`TokioSpawner`] — `LocalSpawner` impl for tokio's `spawn_local`
//! - [`TestContainerBackend`] — channel-based `ContainerBackend` for tests
//! - [`run_test_guest`] — convenience entry point for running guest-init in tests

mod tokio_spawner;
mod test_backend;

pub use tokio_spawner::TokioSpawner;
pub use test_backend::{TestContainerBackend, TestContainerSnapshot, ContainerHandle, BackendHandle};

use std::sync::Arc;
use parking_lot::Mutex;

use crate::buffer::EventBuffer;
use crate::config::{GuestConfig, ShutdownMode, TransportConfig};
use crate::container::ContainerManager;
use crate::platform::NullPlatform;
use crate::supervisor::run_supervisor;
use crate::transport::{BoxedStream, TransportListener};

/// Convenience entry point for running the guest-init supervisor in tests.
///
/// Creates a `TestContainerBackend` + `NullPlatform` + `TokioSpawner` and
/// runs `run_supervisor`. Returns a `BackendHandle` for test control.
///
/// The caller provides the transport channel sender — push `BoxedStream`s
/// into it to simulate host connections (including reconnects).
pub async fn run_test_guest(
    transport_rx: async_channel::Receiver<BoxedStream>,
) -> anyhow::Result<()> {
    let spawner = TokioSpawner;
    let platform = NullPlatform;

    let (backend, _handle) = TestContainerBackend::new();
    let containers = Arc::new(Mutex::new(ContainerManager::new(backend)));
    let event_buffer = EventBuffer::new();

    let config = GuestConfig {
        balloon_mib: None,
        transport: TransportConfig::Vsock { port: 0 },
        config_device: None,
        shutdown_mode: ShutdownMode::Reboot,
        shutdown_timeout: std::time::Duration::from_secs(2),
        shutdown_kill_timeout: std::time::Duration::from_millis(200),
    };

    let listener = TransportListener::Test(transport_rx);

    run_supervisor(&config, &platform, containers, &event_buffer, &listener, &spawner).await
}
