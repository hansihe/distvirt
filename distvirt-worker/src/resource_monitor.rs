use std::future::Future;

use distvirt_worker_protocol::PsiMetrics;

use crate::worker::resources::read_all_psi;


/// Pluggable resource monitoring trait.
///
/// Production workers read real PSI data from `/proc/pressure/*`.
/// Test workers use `NullResourceMonitor` so orchestrator-injected
/// pressure values aren't overwritten by real host data.
pub trait ResourceMonitor: Send + Sync + 'static {
    fn read_psi() -> impl Future<Output = Option<(PsiMetrics, PsiMetrics, PsiMetrics)>> + Send;
}

/// Reads real host PSI data from `/proc/pressure/*`.
pub struct HostResourceMonitor;

impl ResourceMonitor for HostResourceMonitor {
    async fn read_psi() -> Option<(PsiMetrics, PsiMetrics, PsiMetrics)> {
        read_all_psi()
    }
}

/// Always returns `None` — worker never sends pressure updates.
pub struct NullResourceMonitor;

impl ResourceMonitor for NullResourceMonitor {
    async fn read_psi() -> Option<(PsiMetrics, PsiMetrics, PsiMetrics)> {
        None
    }
}
