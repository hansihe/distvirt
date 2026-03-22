#[allow(dead_code)]
pub mod assertions;
#[allow(dead_code)]
pub mod spec_builders;
#[allow(dead_code)]
pub mod test_harness;

pub use distvirt_orchestrator::types::NamespacePatch;
pub use spec_builders::*;
pub use test_harness::TestHarness;

// Re-export MockWorkerConfig from the SyncShell module so scenario tests
// can use it without knowing the internal path.
pub mod mock_worker {
    pub use distvirt_orchestrator::shell::sync::MockWorkerConfig;
}
