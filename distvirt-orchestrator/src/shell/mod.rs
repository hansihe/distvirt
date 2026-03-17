//! New shell implementations.
//!
//! The async shell wraps `crate::core::orchestrator::OrchestratorCore`
//! with tokio I/O (channels, timers, worker connections).

pub mod r#async;
pub mod sync;
