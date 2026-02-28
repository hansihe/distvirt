//! Protocol activator runtime for distvirt.
//!
//! Provides WASM component loading, packet parsing, and per-service
//! activator instance management.

pub mod instance;
pub mod packet_parse;
pub mod runtime;
pub mod stream_manager;
pub mod types;

// Generate wasmtime component bindings from the WIT definition.
pub(crate) mod bindings {
    wasmtime::component::bindgen!({
        path: "wit/activator.wit",
        world: "activator",
    });
}

pub use instance::ActivatorInstance;
pub use packet_parse::{parse_frame_to_packet_info, FlowTracker};
pub use runtime::ActivatorRuntime;
pub use stream_manager::{StreamManager, StreamManagerConfig, StreamManagerOutput, is_l4_action};
pub use types::{Action, BackendNeed, Event, PacketInfo};
