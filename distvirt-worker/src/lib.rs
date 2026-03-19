pub(crate) mod adapter;
pub(crate) mod fabric;
pub mod fs;
pub mod image_provider;
pub mod io_session;
pub(crate) mod linux;
pub mod managed_vm;
pub(crate) mod oci;
pub mod packet;
pub mod resource_monitor;
pub mod sim_traffic;
pub mod task_handle;
pub mod vmm;
pub mod volume;
pub mod vsock_client;
pub mod worker;

// Re-export gateway provider types for external consumers.
pub use fabric::gateway::{GatewayProvider, TunGatewayProvider};
pub use fs::{Fs, SyncFs, TokioFs};
pub use resource_monitor::{HostResourceMonitor, NullResourceMonitor, ResourceMonitor};
