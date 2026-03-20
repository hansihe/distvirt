//! Pressure scoring and hysteresis logic.
//!
//! Used by `core/worker_state.rs` and `core/scheduler/` to track per-worker
//! resource pressure across compute, memory, storage, and network dimensions.

use crate::types::PressureBand;

/// Normalized pressure per resource dimension, each 0.0–1.0.
/// Computed from available signals (pool utilization, pod memory accounting, PSI when available).
#[derive(Debug, Clone, PartialEq)]
pub struct WorkerPressure {
    pub compute: f32,
    pub memory: f32,
    pub storage: f32,
    pub network: f32,
}

impl Default for WorkerPressure {
    fn default() -> Self {
        WorkerPressure {
            compute: 0.0,
            memory: 0.0,
            storage: 0.0,
            network: 0.0,
        }
    }
}

/// Per-dimension hysteresis state.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PressureBands {
    pub compute: PressureBand,
    pub memory: PressureBand,
    pub storage: PressureBand,
    pub network: PressureBand,
}

impl PressureBands {
    /// The effective (max) band across all dimensions for scheduling decisions.
    pub fn max_band(&self) -> PressureBand {
        self.compute
            .max(self.memory)
            .max(self.storage)
            .max(self.network)
    }
}

/// Compute the new pressure band for a single dimension given the current band and raw score.
pub fn compute_band_with_hysteresis(current: PressureBand, score: f32) -> PressureBand {
    // Check if we should move to a higher band.
    if score >= PressureBand::Critical.enter_threshold() {
        return PressureBand::Critical;
    }
    if score >= PressureBand::High.enter_threshold() && current < PressureBand::High {
        return PressureBand::High;
    }
    if score >= PressureBand::Elevated.enter_threshold() && current < PressureBand::Elevated {
        return PressureBand::Elevated;
    }

    // Check if we should drop to a lower band (hysteresis: leave threshold is lower than enter).
    if current == PressureBand::Critical && score < PressureBand::Critical.leave_threshold() {
        if score >= PressureBand::High.enter_threshold() {
            return PressureBand::High;
        }
        if score >= PressureBand::Elevated.enter_threshold() {
            return PressureBand::Elevated;
        }
        return PressureBand::Normal;
    }
    if current == PressureBand::High && score < PressureBand::High.leave_threshold() {
        if score >= PressureBand::Elevated.enter_threshold() {
            return PressureBand::Elevated;
        }
        return PressureBand::Normal;
    }
    if current == PressureBand::Elevated && score < PressureBand::Elevated.leave_threshold() {
        return PressureBand::Normal;
    }

    current
}

impl WorkerPressure {
    /// Update pressure bands with hysteresis from raw scores.
    pub fn update_bands(&self, current: &PressureBands) -> PressureBands {
        PressureBands {
            compute: compute_band_with_hysteresis(current.compute, self.compute),
            memory: compute_band_with_hysteresis(current.memory, self.memory),
            storage: compute_band_with_hysteresis(current.storage, self.storage),
            network: compute_band_with_hysteresis(current.network, self.network),
        }
    }
}

/// Cached PSI metrics from the worker, used to compute pressure scores.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkerPsi {
    pub cpu: distvirt_worker_protocol::PsiMetrics,
    pub memory: distvirt_worker_protocol::PsiMetrics,
    pub io: distvirt_worker_protocol::PsiMetrics,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_band_from_zero() {
        assert_eq!(
            compute_band_with_hysteresis(PressureBand::Normal, 0.0),
            PressureBand::Normal
        );
    }

    #[test]
    fn test_enter_elevated() {
        assert_eq!(
            compute_band_with_hysteresis(PressureBand::Normal, 0.50),
            PressureBand::Elevated
        );
        assert_eq!(
            compute_band_with_hysteresis(PressureBand::Normal, 0.49),
            PressureBand::Normal
        );
    }

    #[test]
    fn test_enter_high() {
        assert_eq!(
            compute_band_with_hysteresis(PressureBand::Normal, 0.80),
            PressureBand::High
        );
        assert_eq!(
            compute_band_with_hysteresis(PressureBand::Elevated, 0.80),
            PressureBand::High
        );
    }

    #[test]
    fn test_enter_critical() {
        assert_eq!(
            compute_band_with_hysteresis(PressureBand::Normal, 0.95),
            PressureBand::Critical
        );
        assert_eq!(
            compute_band_with_hysteresis(PressureBand::High, 0.95),
            PressureBand::Critical
        );
    }

    #[test]
    fn test_hysteresis_elevated_stays() {
        assert_eq!(
            compute_band_with_hysteresis(PressureBand::Elevated, 0.45),
            PressureBand::Elevated
        );
    }

    #[test]
    fn test_hysteresis_elevated_leaves() {
        assert_eq!(
            compute_band_with_hysteresis(PressureBand::Elevated, 0.39),
            PressureBand::Normal
        );
    }

    #[test]
    fn test_hysteresis_high_stays() {
        assert_eq!(
            compute_band_with_hysteresis(PressureBand::High, 0.75),
            PressureBand::High
        );
    }

    #[test]
    fn test_hysteresis_high_leaves() {
        assert_eq!(
            compute_band_with_hysteresis(PressureBand::High, 0.69),
            PressureBand::Elevated
        );
    }

    #[test]
    fn test_hysteresis_critical_leaves() {
        assert_eq!(
            compute_band_with_hysteresis(PressureBand::Critical, 0.84),
            PressureBand::High
        );
        assert_eq!(
            compute_band_with_hysteresis(PressureBand::Critical, 0.60),
            PressureBand::Elevated
        );
        assert_eq!(
            compute_band_with_hysteresis(PressureBand::Critical, 0.30),
            PressureBand::Normal
        );
    }

    #[test]
    fn test_max_band() {
        let bands = PressureBands {
            compute: PressureBand::Normal,
            memory: PressureBand::High,
            storage: PressureBand::Elevated,
            network: PressureBand::Normal,
        };
        assert_eq!(bands.max_band(), PressureBand::High);
    }

    #[test]
    fn test_update_bands() {
        let pressure = WorkerPressure {
            compute: 0.0,
            memory: 0.55,
            storage: 0.90,
            network: 0.0,
        };
        let bands = pressure.update_bands(&PressureBands::default());
        assert_eq!(bands.compute, PressureBand::Normal);
        assert_eq!(bands.memory, PressureBand::Elevated);
        assert_eq!(bands.storage, PressureBand::High);
        assert_eq!(bands.network, PressureBand::Normal);
    }

    #[test]
    fn test_adjust_idle_timeout_normal() {
        let d = std::time::Duration::from_secs(60);
        assert_eq!(PressureBand::Normal.adjust_idle_timeout(d), d);
    }

    #[test]
    fn test_adjust_idle_timeout_elevated() {
        let d = std::time::Duration::from_secs(60);
        assert_eq!(
            PressureBand::Elevated.adjust_idle_timeout(d),
            std::time::Duration::from_secs(45),
        );
    }

    #[test]
    fn test_adjust_idle_timeout_high() {
        let d = std::time::Duration::from_secs(60);
        assert_eq!(
            PressureBand::High.adjust_idle_timeout(d),
            std::time::Duration::from_secs(15),
        );
    }

    #[test]
    fn test_adjust_idle_timeout_critical() {
        let d = std::time::Duration::from_secs(60);
        assert_eq!(
            PressureBand::Critical.adjust_idle_timeout(d),
            std::time::Duration::from_secs(5),
        );
    }

    #[test]
    fn test_adjust_idle_timeout_floor() {
        let d = std::time::Duration::from_secs(10);
        assert_eq!(
            PressureBand::High.adjust_idle_timeout(d),
            std::time::Duration::from_secs(5),
        );
        let d = std::time::Duration::from_secs(4);
        assert_eq!(
            PressureBand::Elevated.adjust_idle_timeout(d),
            std::time::Duration::from_secs(5),
        );
    }
}
