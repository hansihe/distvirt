use std::collections::BTreeMap;

use super::*;

// --- Artifact Placement ---

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArtifactStatus {
    Writing,
    Ready,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactPlacement {
    pub pool_id: PoolId,
    pub worker_id: WorkerId,
    pub status: ArtifactStatus,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlacementTable {
    placements: BTreeMap<ArtifactId, ArtifactPlacement>,
}

impl PlacementTable {
    pub fn insert(&mut self, artifact_id: ArtifactId, placement: ArtifactPlacement) {
        self.placements.insert(artifact_id, placement);
    }

    pub fn get(&self, artifact_id: &ArtifactId) -> Option<&ArtifactPlacement> {
        self.placements.get(artifact_id)
    }

    pub fn get_mut(&mut self, artifact_id: &ArtifactId) -> Option<&mut ArtifactPlacement> {
        self.placements.get_mut(artifact_id)
    }

    pub fn remove(&mut self, artifact_id: &ArtifactId) -> Option<ArtifactPlacement> {
        self.placements.remove(artifact_id)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&ArtifactId, &ArtifactPlacement)> {
        self.placements.iter()
    }

    pub fn remove_by_worker(
        &mut self,
        worker_id: &WorkerId,
    ) -> Vec<(ArtifactId, ArtifactPlacement)> {
        let to_remove: Vec<ArtifactId> = self
            .placements
            .iter()
            .filter(|(_, p)| p.worker_id == *worker_id)
            .map(|(id, _)| id.clone())
            .collect();
        to_remove
            .into_iter()
            .filter_map(|id| self.placements.remove(&id).map(|p| (id, p)))
            .collect()
    }
}

// --- Worker Pressure Score ---

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
        // Dropped below critical leave, check if still high.
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

/// Default memory per pod in MB (hardcoded until per-pod resource sizing is implemented).
pub const DEFAULT_POD_MEMORY_MB: u64 = 128;

/// Cached PSI metrics from the worker, used to compute pressure scores.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkerPsi {
    pub cpu: distvirt_worker_protocol::PsiMetrics,
    pub memory: distvirt_worker_protocol::PsiMetrics,
    pub io: distvirt_worker_protocol::PsiMetrics,
}

impl WorkerState {
    /// Recompute pressure scores from current capabilities and pod count.
    /// `pod_memory_committed_mb` is the total memory committed by active pods on this worker.
    ///
    /// When PSI data is available, it is used as the primary signal (max of PSI and static
    /// accounting). Without PSI (non-Linux), falls back to static accounting only.
    pub fn recompute_pressure(&mut self, pod_memory_committed_mb: u64) {
        // Static accounting fallback for memory.
        let memory_static = if self.capabilities.available_memory_mb > 0 {
            (pod_memory_committed_mb as f32 / self.capabilities.available_memory_mb as f32).min(1.0)
        } else {
            0.0
        };

        // Static accounting fallback for storage: max pool utilization.
        let storage_static = self
            .capabilities
            .pools
            .iter()
            .map(|p| {
                if p.capacity_bytes > 0 {
                    1.0 - (p.available_bytes as f32 / p.capacity_bytes as f32)
                } else {
                    0.0
                }
            })
            .fold(0.0f32, f32::max);

        let (compute, memory, storage) = if let Some(ref psi) = self.psi {
            // PSI available: use max(PSI, static) for memory/storage.
            // Compute has no static fallback — PSI is the only signal.
            let compute = (psi.cpu.some_avg10 as f32 / 100.0).clamp(0.0, 1.0);
            let memory = f32::max(
                (psi.memory.some_avg10 as f32 / 100.0).clamp(0.0, 1.0),
                memory_static,
            );
            let storage = f32::max(
                (psi.io.some_avg10 as f32 / 100.0).clamp(0.0, 1.0),
                storage_static,
            );
            (compute, memory, storage)
        } else {
            // No PSI: static accounting only, compute unknown (0.0).
            (0.0, memory_static, storage_static)
        };

        // Network pressure: 0.0 (future extension).
        let network = 0.0;

        self.pressure = WorkerPressure {
            compute,
            memory,
            storage,
            network,
        };
        self.pressure_bands = self.pressure.update_bands(&self.pressure_bands);
    }
}

#[cfg(test)]
mod pressure_tests {
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
        // Below enter threshold stays Normal.
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
        // At 0.45 — above leave (0.40) but below enter (0.50): stays Elevated.
        assert_eq!(
            compute_band_with_hysteresis(PressureBand::Elevated, 0.45),
            PressureBand::Elevated
        );
    }

    #[test]
    fn test_hysteresis_elevated_leaves() {
        // At 0.39 — below leave threshold (0.40): drops to Normal.
        assert_eq!(
            compute_band_with_hysteresis(PressureBand::Elevated, 0.39),
            PressureBand::Normal
        );
    }

    #[test]
    fn test_hysteresis_high_stays() {
        // At 0.75 — above leave (0.70) but below enter (0.80): stays High.
        assert_eq!(
            compute_band_with_hysteresis(PressureBand::High, 0.75),
            PressureBand::High
        );
    }

    #[test]
    fn test_hysteresis_high_leaves() {
        // At 0.69 — below leave threshold (0.70): drops.
        assert_eq!(
            compute_band_with_hysteresis(PressureBand::High, 0.69),
            PressureBand::Elevated
        );
    }

    #[test]
    fn test_hysteresis_critical_leaves() {
        // At 0.84 — below Critical leave (0.85): drops to High.
        assert_eq!(
            compute_band_with_hysteresis(PressureBand::Critical, 0.84),
            PressureBand::High
        );
        // At 0.60 — below High enter too: drops to Elevated.
        assert_eq!(
            compute_band_with_hysteresis(PressureBand::Critical, 0.60),
            PressureBand::Elevated
        );
        // At 0.30 — below Elevated enter: drops to Normal.
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
    fn test_recompute_pressure_memory() {
        let mut ws = WorkerState {
            capabilities: WorkerCapabilities {
                max_pods: 10,
                available_memory_mb: 1024,
                public_endpoint: String::new(),
                pools: vec![],
            },
            namespaces: std::collections::BTreeSet::new(),
            wg_config: None,
            tunnel_config: None,
            conditions: std::collections::BTreeMap::new(),
            transfer_listen_port: None,
            pressure: WorkerPressure::default(),
            pressure_bands: PressureBands::default(),
            psi: None,
        };

        // 512 MB committed out of 1024 → 0.5 → Elevated.
        ws.recompute_pressure(512);
        assert!((ws.pressure.memory - 0.5).abs() < 0.001);
        assert_eq!(ws.pressure_bands.memory, PressureBand::Elevated);

        // 900 MB committed → ~0.879 → High.
        ws.recompute_pressure(900);
        assert!((ws.pressure.memory - 0.879).abs() < 0.01);
        assert_eq!(ws.pressure_bands.memory, PressureBand::High);
    }

    #[test]
    fn test_recompute_pressure_storage() {
        let mut ws = WorkerState {
            capabilities: WorkerCapabilities {
                max_pods: 10,
                available_memory_mb: 1024,
                public_endpoint: String::new(),
                pools: vec![PoolInfo {
                    pool_id: PoolId("pool-1".into()),
                    path: String::new(),
                    capacity_bytes: 1000,
                    available_bytes: 100, // 90% used → 0.9 → High
                }],
            },
            namespaces: std::collections::BTreeSet::new(),
            wg_config: None,
            tunnel_config: None,
            conditions: std::collections::BTreeMap::new(),
            transfer_listen_port: None,
            pressure: WorkerPressure::default(),
            pressure_bands: PressureBands::default(),
            psi: None,
        };

        ws.recompute_pressure(0);
        assert!((ws.pressure.storage - 0.9).abs() < 0.001);
        assert_eq!(ws.pressure_bands.storage, PressureBand::High);
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
        // High band with 10s configured → 2.5s, but floor is 5s.
        let d = std::time::Duration::from_secs(10);
        assert_eq!(
            PressureBand::High.adjust_idle_timeout(d),
            std::time::Duration::from_secs(5),
        );
        // Elevated with 4s configured → 3s, but floor is 5s.
        let d = std::time::Duration::from_secs(4);
        assert_eq!(
            PressureBand::Elevated.adjust_idle_timeout(d),
            std::time::Duration::from_secs(5),
        );
    }

    #[test]
    fn test_psi_compute_pressure() {
        let mut ws = WorkerState {
            capabilities: WorkerCapabilities {
                max_pods: 10,
                available_memory_mb: 1024,
                public_endpoint: String::new(),
                pools: vec![],
            },
            namespaces: std::collections::BTreeSet::new(),
            wg_config: None,
            tunnel_config: None,
            conditions: std::collections::BTreeMap::new(),
            transfer_listen_port: None,
            pressure: WorkerPressure::default(),
            pressure_bands: PressureBands::default(),
            psi: Some(WorkerPsi {
                cpu: distvirt_worker_protocol::PsiMetrics {
                    some_avg10: 60.0, // 60% → 0.6 → Elevated
                    some_avg60: 40.0,
                    full_avg10: 0.0,
                    full_avg60: 0.0,
                },
                memory: distvirt_worker_protocol::PsiMetrics::default(),
                io: distvirt_worker_protocol::PsiMetrics::default(),
            }),
        };

        ws.recompute_pressure(0);
        assert!((ws.pressure.compute - 0.6).abs() < 0.001);
        assert_eq!(ws.pressure_bands.compute, PressureBand::Elevated);
    }

    #[test]
    fn test_psi_memory_max_with_static() {
        let mut ws = WorkerState {
            capabilities: WorkerCapabilities {
                max_pods: 10,
                available_memory_mb: 1024,
                public_endpoint: String::new(),
                pools: vec![],
            },
            namespaces: std::collections::BTreeSet::new(),
            wg_config: None,
            tunnel_config: None,
            conditions: std::collections::BTreeMap::new(),
            transfer_listen_port: None,
            pressure: WorkerPressure::default(),
            pressure_bands: PressureBands::default(),
            psi: Some(WorkerPsi {
                cpu: distvirt_worker_protocol::PsiMetrics::default(),
                memory: distvirt_worker_protocol::PsiMetrics {
                    some_avg10: 10.0, // 10% PSI
                    some_avg60: 5.0,
                    full_avg10: 0.0,
                    full_avg60: 0.0,
                },
                io: distvirt_worker_protocol::PsiMetrics::default(),
            }),
        };

        // Static accounting: 800/1024 ≈ 0.78 > PSI 0.10 → static wins.
        ws.recompute_pressure(800);
        assert!((ws.pressure.memory - 0.78125).abs() < 0.001);

        // Now PSI is higher: 90% PSI > 0.78 static → PSI wins.
        ws.psi.as_mut().unwrap().memory.some_avg10 = 90.0;
        ws.recompute_pressure(800);
        assert!((ws.pressure.memory - 0.9).abs() < 0.001);
        assert_eq!(ws.pressure_bands.memory, PressureBand::High);
    }

    #[test]
    fn test_no_psi_fallback_unchanged() {
        // Without PSI, compute is 0.0 (unchanged from before).
        let mut ws = WorkerState {
            capabilities: WorkerCapabilities {
                max_pods: 10,
                available_memory_mb: 1024,
                public_endpoint: String::new(),
                pools: vec![],
            },
            namespaces: std::collections::BTreeSet::new(),
            wg_config: None,
            tunnel_config: None,
            conditions: std::collections::BTreeMap::new(),
            transfer_listen_port: None,
            pressure: WorkerPressure::default(),
            pressure_bands: PressureBands::default(),
            psi: None,
        };

        ws.recompute_pressure(512);
        assert_eq!(ws.pressure.compute, 0.0);
        assert!((ws.pressure.memory - 0.5).abs() < 0.001);
    }
}

// --- Resource Leases ---

/// What a lease is reserving capacity for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseIntent {
    PodLaunch,
    PodResume { artifact_id: ArtifactId },
}

/// A capacity reservation for an in-flight pod operation (launch or resume).
/// Exists from dispatch until the pod reaches a terminal state (running, failed, exited, etc.).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    pub worker_id: WorkerId,
    pub intent: LeaseIntent,
    pub memory_mb: u64,
}

/// Tracks in-flight capacity reservations keyed by PodId.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LeaseTable {
    leases: BTreeMap<PodId, Lease>,
}

impl LeaseTable {
    /// Reserve capacity for a pod operation.
    pub fn grant(
        &mut self,
        pod_id: PodId,
        worker_id: WorkerId,
        intent: LeaseIntent,
        memory_mb: u64,
    ) {
        self.leases.insert(
            pod_id,
            Lease {
                worker_id,
                intent,
                memory_mb,
            },
        );
    }

    /// Release a lease. Returns the lease if it existed.
    pub fn release(&mut self, pod_id: &PodId) -> Option<Lease> {
        self.leases.remove(pod_id)
    }

    /// Release all leases for a given worker. Returns the released leases.
    pub fn release_worker_leases(&mut self, worker_id: &WorkerId) -> Vec<(PodId, Lease)> {
        let to_remove: Vec<PodId> = self
            .leases
            .iter()
            .filter(|(_, l)| l.worker_id == *worker_id)
            .map(|(id, _)| id.clone())
            .collect();
        to_remove
            .into_iter()
            .filter_map(|id| self.leases.remove(&id).map(|l| (id, l)))
            .collect()
    }

    /// Total leased memory in MB for a specific worker.
    pub fn leased_memory_mb(&self, worker_id: &WorkerId) -> u64 {
        self.leases
            .values()
            .filter(|l| l.worker_id == *worker_id)
            .map(|l| l.memory_mb)
            .sum()
    }

    /// Number of active leases for a specific worker.
    pub fn leased_pod_count(&self, worker_id: &WorkerId) -> usize {
        self.leases
            .values()
            .filter(|l| l.worker_id == *worker_id)
            .count()
    }

    /// Iterate over all leases.
    pub fn iter(&self) -> impl Iterator<Item = (&PodId, &Lease)> {
        self.leases.iter()
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
    pub workload_id: WorkloadId,
    pub worker_id: WorkerId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerTunnelConfig {
    pub listen_port: u16,
    pub public_key: [u8; 32],
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerCondition {
    pub active: bool,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerState {
    pub capabilities: WorkerCapabilities,
    pub namespaces: std::collections::BTreeSet<NamespaceId>,
    pub wg_config: Option<WorkerWgConfig>,
    pub tunnel_config: Option<WorkerTunnelConfig>,
    pub conditions: std::collections::BTreeMap<String, WorkerCondition>,
    pub transfer_listen_port: Option<u16>,
    pub pressure: WorkerPressure,
    pub pressure_bands: PressureBands,
    pub psi: Option<WorkerPsi>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerCapabilities {
    pub max_pods: u32,
    pub available_memory_mb: u64,
    pub public_endpoint: String,
    pub pools: Vec<PoolInfo>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerWgConfig {
    pub listen_port: u16,
    pub public_key: [u8; 32],
}

#[cfg(test)]
mod lease_tests {
    use super::*;

    #[test]
    fn test_grant_release_round_trip() {
        let mut table = LeaseTable::default();
        let pod_id = PodId("pod-1".into());
        let worker_id = WorkerId("w-0".into());

        table.grant(
            pod_id.clone(),
            worker_id.clone(),
            LeaseIntent::PodLaunch,
            128,
        );
        assert_eq!(table.leased_pod_count(&worker_id), 1);
        assert_eq!(table.leased_memory_mb(&worker_id), 128);

        let lease = table.release(&pod_id);
        assert!(lease.is_some());
        assert_eq!(lease.unwrap().worker_id, worker_id);
        assert_eq!(table.leased_pod_count(&worker_id), 0);
        assert_eq!(table.leased_memory_mb(&worker_id), 0);
    }

    #[test]
    fn test_double_release_returns_none() {
        let mut table = LeaseTable::default();
        let pod_id = PodId("pod-1".into());
        table.grant(
            pod_id.clone(),
            WorkerId("w-0".into()),
            LeaseIntent::PodLaunch,
            128,
        );
        table.release(&pod_id);
        assert!(table.release(&pod_id).is_none());
    }

    #[test]
    fn test_leased_memory_mb_sums_correctly() {
        let mut table = LeaseTable::default();
        let w0 = WorkerId("w-0".into());
        let w1 = WorkerId("w-1".into());

        table.grant(PodId("p-1".into()), w0.clone(), LeaseIntent::PodLaunch, 128);
        table.grant(PodId("p-2".into()), w0.clone(), LeaseIntent::PodLaunch, 256);
        table.grant(PodId("p-3".into()), w1.clone(), LeaseIntent::PodLaunch, 64);

        assert_eq!(table.leased_memory_mb(&w0), 384);
        assert_eq!(table.leased_memory_mb(&w1), 64);
    }

    #[test]
    fn test_release_worker_leases() {
        let mut table = LeaseTable::default();
        let w0 = WorkerId("w-0".into());
        let w1 = WorkerId("w-1".into());

        table.grant(PodId("p-1".into()), w0.clone(), LeaseIntent::PodLaunch, 128);
        table.grant(PodId("p-2".into()), w0.clone(), LeaseIntent::PodLaunch, 128);
        table.grant(PodId("p-3".into()), w1.clone(), LeaseIntent::PodLaunch, 128);

        let released = table.release_worker_leases(&w0);
        assert_eq!(released.len(), 2);
        assert_eq!(table.leased_pod_count(&w0), 0);
        assert_eq!(table.leased_pod_count(&w1), 1);
    }

    #[test]
    fn test_resume_intent() {
        let mut table = LeaseTable::default();
        let pod_id = PodId("pod-1".into());
        let artifact_id = ArtifactId("art-1".into());

        table.grant(
            pod_id.clone(),
            WorkerId("w-0".into()),
            LeaseIntent::PodResume {
                artifact_id: artifact_id.clone(),
            },
            128,
        );

        let lease = table.release(&pod_id).unwrap();
        assert_eq!(lease.intent, LeaseIntent::PodResume { artifact_id });
    }
}
