//! Guest-init library: runtime-agnostic supervisor core for distvirt guests.
//!
//! The supervisor manages container lifecycle inside a VM, communicating with
//! the host worker over a yamux-multiplexed transport (vsock or virtio-serial).
//!
//! # Test support
//!
//! Enable the `test-support` feature for:
//! - `TestContainerBackend` — channel-based container backend for testing
//! - `TokioSpawner` — `LocalSpawner` impl for tokio's `spawn_local`

pub mod buffer;
pub mod cgroup;
pub mod config;
pub(crate) mod config_drive;
pub mod container;
pub(crate) mod init;
pub mod memory;
pub(crate) mod net;
pub(crate) mod output;
pub mod platform;
pub(crate) mod session;
pub mod spawner;
pub mod supervisor;
pub(crate) mod timer;
pub mod transport;
pub(crate) mod util;
pub mod vsock;
pub(crate) mod yamux_driver;

#[cfg(feature = "test-support")]
pub mod test_support;
