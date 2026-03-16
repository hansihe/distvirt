//! Pure orchestration logic — no async, no channels, no I/O.
//!
//! Individual state machine cores, shared types, and the top-level
//! `OrchestratorCore` that composes them.

pub mod types;
pub mod namespace;
pub mod orchestrator;
pub(crate) mod scheduler;
pub mod timer_wheel;
pub mod worker_event;
pub mod worker_state;
