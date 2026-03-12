pub(crate) mod adapter;
pub(crate) mod fabric;
pub mod fs;
pub mod image_provider;
pub mod io_session;
pub(crate) mod linux;
pub mod managed_vm;
pub(crate) mod oci;
pub mod packet;
pub mod sim_traffic;
pub mod task_handle;
pub mod resource_monitor;
pub mod vmm;
pub mod vsock_client;
pub mod worker;

// Re-export gateway provider types for external consumers.
pub use fabric::gateway::{GatewayProvider, TunGatewayProvider};
pub use fs::{Fs, TokioFs, SyncFs};
pub use resource_monitor::{ResourceMonitor, HostResourceMonitor, NullResourceMonitor};
