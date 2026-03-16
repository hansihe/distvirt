//! New shell implementations.
//!
//! The async shell wraps `crate::core::orchestrator::OrchestratorCore`
//! with tokio I/O (channels, timers, worker connections).

pub(crate) mod r#async;
pub mod sync;
