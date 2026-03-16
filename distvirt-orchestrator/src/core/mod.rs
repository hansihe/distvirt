//! Pure orchestration logic — no async, no channels, no I/O.
//!
//! Individual state machine cores, shared types, and the top-level
//! `OrchestratorCore` that composes them.

pub(crate) mod types;
pub(crate) mod namespace;
pub(crate) mod orchestrator;
pub(crate) mod scheduler;
pub(crate) mod worker_event;
pub(crate) mod worker_state;
