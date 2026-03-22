use super::*;

// --- Pressure Band ---

/// Pressure band with hysteresis thresholds.
/// Enter at upper threshold, leave at lower to prevent oscillation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum PressureBand {
    #[default]
    Normal,
    Elevated,
    High,
    Critical,
}

impl PressureBand {
    /// Enter threshold for each band.
    pub fn enter_threshold(self) -> f32 {
        match self {
            PressureBand::Normal => 0.0,
            PressureBand::Elevated => 0.50,
            PressureBand::High => 0.80,
            PressureBand::Critical => 0.95,
        }
    }

    /// Adjust an idle timeout duration based on the current pressure band.
    /// Higher pressure → shorter timeout, with a 5-second floor.
    pub fn adjust_idle_timeout(self, configured: std::time::Duration) -> std::time::Duration {
        let floor = std::time::Duration::from_secs(5);
        match self {
            PressureBand::Normal => configured,
            PressureBand::Elevated => configured.mul_f64(0.75).max(floor),
            PressureBand::High => configured.mul_f64(0.25).max(floor),
            PressureBand::Critical => floor,
        }
    }

    /// Leave threshold for each band (lower than enter to provide hysteresis).
    pub fn leave_threshold(self) -> f32 {
        match self {
            PressureBand::Normal => 0.0,
            PressureBand::Elevated => 0.40,
            PressureBand::High => 0.70,
            PressureBand::Critical => 0.85,
        }
    }
}

// --- Domain Enums ---

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum NamespaceStatus {
    Creating,
    Active,
    Destroying,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FabricStatus {
    Creating,
    Active,
    Destroying,
}

// --- State Structs ---

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceWorkerState {
    pub fabric_status: FabricStatus,
    pub primary_pool_id: Option<PoolId>,
    pub pressure_band: PressureBand,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PodInfo {
    pub workload_id: WorkloadName,
    pub worker_id: WorkerId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerCondition {
    pub active: bool,
    pub message: String,
}
