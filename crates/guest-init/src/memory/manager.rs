use std::time::{Duration, Instant};

use distvirt_guest_protocol::{ConstraintReason, GuestEvent};

use super::monitor::VIRTIO_BALLOON_PAGES_PER_MIB;
use crate::cgroup;

const INITIAL_STEP_MIB: u32 = 32;
const MAX_STEP_MIB: u32 = 256;

/// Memory reserved for kernel, page tables, slab caches, and PID 1.
/// Subtracted from VM memory when computing the cgroup limit ceiling.
pub const KERNEL_BUFFER_MIB: u32 = 64;

/// Gap between memory.high (throttle point) and memory.max (OOM point).
/// Gives the kernel room to reclaim before hitting the hard limit.
const HIGH_TO_MAX_GAP_MIB: u32 = 4;

/// Inflation step size in MiB.
const INFLATION_STEP_MIB: u32 = 16;

/// Minimum headroom above memory.current when inflating (8 MiB).
const INFLATION_HEADROOM_BYTES: u64 = 8 * 1024 * 1024;

/// Cooldown after deflation before inflation is allowed.
const INFLATION_COOLDOWN: Duration = Duration::from_secs(60);

/// Number of consecutive low-usage samples required before inflating.
const INFLATION_STREAK_REQUIRED: u32 = 4;

/// Usage ratio threshold: inflate when current < 75% of high.
const INFLATION_USAGE_RATIO: f64 = 0.75;

/// How long to wait for a balloon deflation response before declaring stall.
const DEFLATION_STALL_DEADLINE: Duration = Duration::from_millis(1000);

enum ConstraintState {
    Normal,
    WaitingForDeflation { sent_at: Instant },
    Constrained { reason: ConstraintReason },
}

pub struct MemoryManager {
    balloon_amount_mib: u32,
    step_size_mib: u32,
    vm_mem_mib: u32,

    /// Tracks in-flight deflation between sending BalloonSet and observing
    /// sysfs confirmation that the host actually released balloon pages.
    pending_deflation_mib: u32,

    current_cgroup_high: u64,
    current_cgroup_max: u64,

    // Inflation state
    low_usage_streak: u32,
    inflation_suppressed_until: Option<Instant>,

    constraint_state: ConstraintState,
}

impl MemoryManager {
    pub fn new(balloon_mib: u32, vm_mem_mib: u32) -> Self {
        MemoryManager {
            balloon_amount_mib: balloon_mib,
            step_size_mib: INITIAL_STEP_MIB,
            vm_mem_mib,
            pending_deflation_mib: 0,
            current_cgroup_high: 0,
            current_cgroup_max: 0,
            low_usage_streak: 0,
            inflation_suppressed_until: None,
            constraint_state: ConstraintState::Normal,
        }
    }

    /// Calculate the initial memory limits for containers:
    /// vm_mem - balloon - kernel buffer, returning (high_bytes, max_bytes).
    pub fn initial_limits(&mut self) -> (u64, u64) {
        let available_mib = self
            .vm_mem_mib
            .saturating_sub(self.balloon_amount_mib)
            .saturating_sub(KERNEL_BUFFER_MIB);
        let max_bytes = available_mib as u64 * 1024 * 1024;
        let high_bytes = max_bytes.saturating_sub(HIGH_TO_MAX_GAP_MIB as u64 * 1024 * 1024);
        self.current_cgroup_high = high_bytes;
        self.current_cgroup_max = max_bytes;
        (high_bytes, max_bytes)
    }

    /// Handle a memory pressure event. Computes a deflation step and returns
    /// events to send to the host. Does NOT touch cgroup limits —
    /// those are raised later when the balloon monitor confirms page release.
    pub fn handle_pressure(&mut self) -> Vec<GuestEvent> {
        // Reset inflation state — active pressure means we should not inflate.
        self.low_usage_streak = 0;
        self.inflation_suppressed_until = Some(Instant::now() + INFLATION_COOLDOWN);

        if self.balloon_amount_mib == 0 {
            log::warn!(
                "[balloon] pressure detected but balloon is already 0, cannot deflate further"
            );
            // Balloon exhausted — emit constrained event on transition only.
            if !matches!(
                self.constraint_state,
                ConstraintState::Constrained {
                    reason: ConstraintReason::BalloonExhausted
                }
            ) {
                self.constraint_state = ConstraintState::Constrained {
                    reason: ConstraintReason::BalloonExhausted,
                };
                return vec![GuestEvent::MemoryConstrained {
                    reason: ConstraintReason::BalloonExhausted,
                }];
            }
            return vec![];
        }

        // Adaptive step sizing based on pending deflation state.
        if self.pending_deflation_mib >= self.step_size_mib {
            // Previous deflation hasn't landed yet — host is still releasing.
            log::debug!(
                "[balloon] skipping: pending={} MiB >= step={} MiB",
                self.pending_deflation_mib,
                self.step_size_mib
            );
            return vec![];
        } else if self.pending_deflation_mib > 0 {
            // Partial delivery but still under pressure — step was too small.
            self.step_size_mib = (self.step_size_mib * 2).min(MAX_STEP_MIB);
            log::info!(
                "[balloon] pressure with partial pending={} MiB, doubling step to {} MiB",
                self.pending_deflation_mib,
                self.step_size_mib
            );
        } else {
            // pending == 0: previous deflation fully landed. Fresh pressure event.
            self.step_size_mib = INITIAL_STEP_MIB;
        }

        let step = self.step_size_mib.min(self.balloon_amount_mib);

        self.balloon_amount_mib -= step;
        self.pending_deflation_mib += step;

        log::info!(
            "[balloon] deflating by {} MiB, balloon={} MiB, pending={} MiB",
            step,
            self.balloon_amount_mib,
            self.pending_deflation_mib,
        );

        self.constraint_state = ConstraintState::WaitingForDeflation {
            sent_at: Instant::now(),
        };

        vec![GuestEvent::BalloonSet {
            amount_mib: self.balloon_amount_mib,
        }]
    }

    /// Called when the sysfs monitor observes a change in balloon num_pages.
    /// On deflation (pages decreased): raises cgroup limits by the actual released amount.
    /// On inflation (pages increased): logs confirmation only.
    pub fn on_balloon_pages_changed(
        &mut self,
        old_pages: u32,
        new_pages: u32,
        cgroup_path: &str,
    ) -> Vec<GuestEvent> {
        let old_mib = old_pages / VIRTIO_BALLOON_PAGES_PER_MIB;
        let new_mib = new_pages / VIRTIO_BALLOON_PAGES_PER_MIB;

        if new_pages < old_pages {
            // Deflation confirmed: host released balloon pages.
            let released_mib = old_mib.saturating_sub(new_mib);
            let released_bytes = released_mib as u64 * 1024 * 1024;

            // Decrement pending tracker.
            self.pending_deflation_mib = self.pending_deflation_mib.saturating_sub(released_mib);

            // Raise cgroup limits by the actual released amount.
            let new_max = self.current_cgroup_max.saturating_add(released_bytes);
            // When balloon is fully deflated there's no more memory to reclaim,
            // so remove the high/max gap to avoid pointless throttling.
            let new_high = if self.balloon_amount_mib == 0 {
                new_max
            } else {
                self.current_cgroup_high.saturating_add(released_bytes)
            };

            if let Err(e) = cgroup::set_memory_limits(cgroup_path, new_high, new_max) {
                log::warn!(
                    "[balloon] failed to raise cgroup limits after deflation: {:#}",
                    e
                );
                return vec![];
            }

            self.current_cgroup_high = new_high;
            self.current_cgroup_max = new_max;

            log::info!(
                "[balloon] deflation confirmed: released {} MiB, new limits high={} MiB, max={} MiB, pending={} MiB",
                released_mib,
                new_high / (1024 * 1024),
                new_max / (1024 * 1024),
                self.pending_deflation_mib,
            );

            // Check if constraint is now resolved.
            return self.check_constraint_cleared(cgroup_path);
        } else if new_pages > old_pages {
            // Inflation confirmed.
            let claimed_mib = new_mib.saturating_sub(old_mib);
            log::info!(
                "[balloon] inflation confirmed: host claimed {} MiB ({} -> {} pages)",
                claimed_mib,
                old_pages,
                new_pages,
            );
        }
        vec![]
    }

    /// Evaluate a PSI memory pressure event. Logs stats, resets inflation state.
    /// Deflates on `Full` as an emergency fallback (primary deflation is via
    /// `memory.events` high counter in balloon_task).
    pub fn handle_psi_event(
        &mut self,
        level: cgroup::PsiLevel,
        cgroup_path: &str,
    ) -> Vec<GuestEvent> {
        let is_full = matches!(level, cgroup::PsiLevel::Full);

        // Log stats.
        let fmt_bytes = |r: anyhow::Result<u64>, none: &str| -> String {
            match r {
                Ok(v) if v == u64::MAX => none.to_string(),
                Ok(v) => format!("{} MiB", v / (1024 * 1024)),
                Err(_) => "?".to_string(),
            }
        };
        let stats = format!(
            "current={}, high={}, max={}, swap={}",
            fmt_bytes(
                cgroup::read_cgroup_bytes(cgroup_path, "memory.current"),
                "?"
            ),
            fmt_bytes(cgroup::read_cgroup_bytes(cgroup_path, "memory.high"), "max"),
            fmt_bytes(cgroup::read_cgroup_bytes(cgroup_path, "memory.max"), "max"),
            fmt_bytes(
                cgroup::read_cgroup_bytes(cgroup_path, "memory.swap.current"),
                "?"
            ),
        );
        if is_full {
            log::error!("memory pressure FULL: {}", stats);
        } else {
            log::warn!("memory pressure (some): {}", stats);
        }

        // Reset inflation state on any pressure.
        self.low_usage_streak = 0;
        self.inflation_suppressed_until = Some(Instant::now() + INFLATION_COOLDOWN);

        // Emergency fallback: deflate on full pressure.
        if is_full {
            return self.handle_pressure();
        }

        vec![]
    }

    /// Periodically check if the workload is using much less memory than
    /// allocated, and if so, inflate the balloon to reclaim memory.
    ///
    /// Returns events including BalloonSet if inflation occurred, and
    /// MemoryConstraintCleared if constraint resolved.
    pub fn tick_inflation(&mut self, cgroup_path: &str) -> Vec<GuestEvent> {
        let mut events = Vec::new();

        // Check if constraint cleared due to low usage.
        if matches!(self.constraint_state, ConstraintState::Constrained { .. }) {
            if self.is_pressure_resolved(cgroup_path) {
                self.constraint_state = ConstraintState::Normal;
                events.push(GuestEvent::MemoryConstraintCleared);
            }
        }

        // Don't inflate while deflation is in-flight.
        if self.pending_deflation_mib > 0 {
            return events;
        }

        // Check cooldown.
        if let Some(until) = self.inflation_suppressed_until {
            if Instant::now() < until {
                return events;
            }
        }

        let current = match cgroup::read_cgroup_bytes(cgroup_path, "memory.current") {
            Ok(v) => v,
            Err(e) => {
                log::warn!("[inflate] failed to read memory.current: {:#}", e);
                return events;
            }
        };

        let high = match cgroup::read_cgroup_bytes(cgroup_path, "memory.high") {
            Ok(v) => v,
            Err(e) => {
                log::warn!("[inflate] failed to read memory.high: {:#}", e);
                return events;
            }
        };

        // Skip if high is "max" (unlimited).
        if high == u64::MAX {
            return events;
        }

        let ratio = current as f64 / high as f64;
        if ratio < INFLATION_USAGE_RATIO {
            self.low_usage_streak += 1;
        } else {
            self.low_usage_streak = 0;
            return events;
        }

        if self.low_usage_streak < INFLATION_STREAK_REQUIRED {
            return events;
        }

        let step_bytes = INFLATION_STEP_MIB as u64 * 1024 * 1024;

        // Don't lower high below current + headroom.
        let min_high = current.saturating_add(INFLATION_HEADROOM_BYTES);
        let new_high = high.saturating_sub(step_bytes);
        if new_high < min_high {
            log::debug!(
                "[inflate] skipping: new_high {} < min_high {}",
                new_high,
                min_high
            );
            self.low_usage_streak = 0;
            return events;
        }

        // Maintain high-to-max gap.
        let new_max = new_high.saturating_add(HIGH_TO_MAX_GAP_MIB as u64 * 1024 * 1024);

        if let Err(e) = cgroup::set_memory_limits(cgroup_path, new_high, new_max) {
            log::warn!("[inflate] failed to set cgroup limits: {:#}", e);
            return events;
        }

        self.current_cgroup_high = new_high;
        self.current_cgroup_max = new_max;
        self.balloon_amount_mib += INFLATION_STEP_MIB;
        self.low_usage_streak = 0;

        log::info!(
            "[inflate] inflated balloon by {} MiB, new balloon={} MiB (high={} MiB, current={} MiB)",
            INFLATION_STEP_MIB,
            self.balloon_amount_mib,
            new_high / (1024 * 1024),
            current / (1024 * 1024),
        );

        events.push(GuestEvent::BalloonSet {
            amount_mib: self.balloon_amount_mib,
        });
        events
    }

    /// Check the deflation deadline. Called when the deadline timer fires.
    pub fn check_deflation_deadline(&mut self, cgroup_path: &str) -> Vec<GuestEvent> {
        if let ConstraintState::WaitingForDeflation { sent_at } = self.constraint_state {
            if sent_at.elapsed() >= DEFLATION_STALL_DEADLINE {
                if self.is_pressure_resolved(cgroup_path) {
                    self.constraint_state = ConstraintState::Normal;
                } else {
                    self.constraint_state = ConstraintState::Constrained {
                        reason: ConstraintReason::DeflationStalled,
                    };
                    return vec![GuestEvent::MemoryConstrained {
                        reason: ConstraintReason::DeflationStalled,
                    }];
                }
            }
        }
        vec![]
    }

    /// Returns the next deadline instant, if any (for the task loop timer).
    pub fn next_deadline(&self) -> Option<Instant> {
        if let ConstraintState::WaitingForDeflation { sent_at } = self.constraint_state {
            Some(sent_at + DEFLATION_STALL_DEADLINE)
        } else {
            None
        }
    }

    /// Returns an OomKill event.
    pub fn handle_oom_kill(&self, count: u64) -> GuestEvent {
        GuestEvent::OomKill { count }
    }

    /// Check if memory pressure is resolved (current < high).
    fn is_pressure_resolved(&self, cgroup_path: &str) -> bool {
        let current = cgroup::read_cgroup_bytes(cgroup_path, "memory.current").unwrap_or(u64::MAX);
        let high = cgroup::read_cgroup_bytes(cgroup_path, "memory.high").unwrap_or(0);
        current < high
    }

    /// Check if constraint has cleared after a balloon/limit change.
    fn check_constraint_cleared(&mut self, cgroup_path: &str) -> Vec<GuestEvent> {
        match self.constraint_state {
            ConstraintState::Constrained { .. } => {
                if self.is_pressure_resolved(cgroup_path) {
                    self.constraint_state = ConstraintState::Normal;
                    vec![GuestEvent::MemoryConstraintCleared]
                } else {
                    vec![]
                }
            }
            ConstraintState::WaitingForDeflation { .. } => {
                if self.is_pressure_resolved(cgroup_path) {
                    self.constraint_state = ConstraintState::Normal;
                }
                vec![]
            }
            ConstraintState::Normal => vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use distvirt_guest_protocol::GuestEvent;

    const MIB: u64 = 1024 * 1024;

    fn make_mm(balloon_mib: u32, vm_mem_mib: u32) -> MemoryManager {
        MemoryManager::new(balloon_mib, vm_mem_mib)
    }

    #[test]
    fn initial_limits_correct() {
        let mut mm = make_mm(128, 512);
        let (high, max) = mm.initial_limits();
        // available = 512 - 128 - 64(kernel) = 320 MiB
        let expected_max = 320 * MIB;
        let expected_high = expected_max - HIGH_TO_MAX_GAP_MIB as u64 * MIB;
        assert_eq!(max, expected_max);
        assert_eq!(high, expected_high);
    }

    #[test]
    fn initial_limits_saturates_to_zero() {
        let mut mm = make_mm(500, 512);
        let (high, max) = mm.initial_limits();
        // available = 512 - 500 - 64 = 0 (saturating)
        assert_eq!(max, 0);
        assert_eq!(high, 0);
    }

    #[test]
    fn handle_pressure_deflates_by_initial_step() {
        let mut mm = make_mm(256, 512);
        mm.initial_limits();
        let events = mm.handle_pressure();
        match events.as_slice() {
            [GuestEvent::BalloonSet { amount_mib }] => {
                assert_eq!(*amount_mib, 256 - INITIAL_STEP_MIB);
            }
            other => panic!("expected [BalloonSet], got {:?}", other),
        }
        assert_eq!(mm.pending_deflation_mib, INITIAL_STEP_MIB);
    }

    #[test]
    fn handle_pressure_emits_constrained_when_balloon_zero() {
        let mut mm = make_mm(0, 512);
        mm.initial_limits();
        let events = mm.handle_pressure();
        assert!(matches!(
            events.as_slice(),
            [GuestEvent::MemoryConstrained {
                reason: ConstraintReason::BalloonExhausted
            }]
        ));
        // Second call should dedup (no event).
        assert!(mm.handle_pressure().is_empty());
    }

    #[test]
    fn handle_pressure_skips_when_pending_ge_step() {
        let mut mm = make_mm(256, 512);
        mm.initial_limits();
        // First pressure: deflate by 32
        mm.handle_pressure();
        assert_eq!(mm.pending_deflation_mib, INITIAL_STEP_MIB);
        // Second pressure with full pending: should skip
        assert!(mm.handle_pressure().is_empty());
    }

    #[test]
    fn handle_pressure_doubles_step_on_partial_pending() {
        let mut mm = make_mm(256, 512);
        mm.initial_limits();
        // First pressure: deflate by 32, pending=32
        mm.handle_pressure();
        // Simulate partial delivery
        mm.pending_deflation_mib = 16;
        // Second pressure: partial pending → step doubles to 64
        let events = mm.handle_pressure();
        match events.as_slice() {
            [GuestEvent::BalloonSet { amount_mib }] => {
                // balloon was 224 (256-32), step=64, now 224-64=160
                assert_eq!(*amount_mib, 160);
            }
            other => panic!("expected [BalloonSet], got {:?}", other),
        }
        assert_eq!(mm.step_size_mib, INITIAL_STEP_MIB * 2);
    }

    #[test]
    fn handle_pressure_clamps_step_to_balloon() {
        let mut mm = make_mm(16, 512);
        mm.initial_limits();
        // balloon=16, step=32 → step clamped to 16
        let events = mm.handle_pressure();
        match events.as_slice() {
            [GuestEvent::BalloonSet { amount_mib }] => {
                assert_eq!(*amount_mib, 0);
            }
            other => panic!("expected [BalloonSet], got {:?}", other),
        }
    }

    #[test]
    fn handle_pressure_resets_inflation_state() {
        let mut mm = make_mm(256, 512);
        mm.initial_limits();
        mm.low_usage_streak = 10;
        mm.handle_pressure();
        assert_eq!(mm.low_usage_streak, 0);
        assert!(mm.inflation_suppressed_until.is_some());
    }

    #[test]
    fn deflation_confirmed_clears_pending() {
        let mut mm = make_mm(256, 512);
        mm.initial_limits();
        // Deflate
        mm.handle_pressure();
        assert_eq!(mm.pending_deflation_mib, INITIAL_STEP_MIB);

        // Simulate what on_balloon_pages_changed does for pure state
        // (can't call it directly — it does cgroup I/O).
        let released_mib = INITIAL_STEP_MIB;
        mm.pending_deflation_mib = mm.pending_deflation_mib.saturating_sub(released_mib);
        let released_bytes = released_mib as u64 * MIB;
        let old_high = mm.current_cgroup_high;
        let old_max = mm.current_cgroup_max;
        mm.current_cgroup_high = mm.current_cgroup_high.saturating_add(released_bytes);
        mm.current_cgroup_max = mm.current_cgroup_max.saturating_add(released_bytes);

        assert_eq!(mm.pending_deflation_mib, 0);
        assert_eq!(mm.current_cgroup_high, old_high + released_bytes);
        assert_eq!(mm.current_cgroup_max, old_max + released_bytes);
    }
}
