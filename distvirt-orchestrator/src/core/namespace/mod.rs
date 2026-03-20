//! Per-namespace state: core logic, timer wheel, WG peers.

pub(crate) mod inner;
pub mod timer_wheel;
pub mod wg_peers;

// TODO: update for EndpointSm refactor
// #[cfg(test)]
// mod tests;

use std::time::Duration;

use timer_wheel::NamespaceTimerWheel;

use crate::adapter::timer::TimerConfig;
use crate::core::types::{NamespaceEffects, NamespaceOutput, OrchestratorToNamespace};
use crate::core::GlobalWorkerId;
use crate::id_registry::IdRegistry;
use crate::types::NamespaceId;

// Re-export for external use.
pub use inner::Namespace;

/// A self-contained namespace unit: namespace state + per-namespace timer wheel.
///
/// This is the unit of namespace processing in the split-core architecture.
/// The shell owns a `HashMap<NamespaceId, NamespaceUnit>` and delivers
/// `OrchestratorToNamespace` messages to it.
pub struct NamespaceUnit {
    namespace: Namespace,
    timer_wheel: NamespaceTimerWheel,
}

impl NamespaceUnit {
    pub fn new(
        namespace_id: NamespaceId,
        timer_config: TimerConfig,
        network: &distvirt_worker_protocol::NetworkConfig,
        id_registry: IdRegistry,
    ) -> Self {
        NamespaceUnit {
            namespace: Namespace::new(namespace_id, timer_config, network, id_registry),
            timer_wheel: NamespaceTimerWheel::new(),
        }
    }

    /// Process an orchestrator-to-namespace message.
    pub fn process(&mut self, input: OrchestratorToNamespace, now: Duration) -> NamespaceOutput {
        let ns_effects = self.namespace.process_event(input);
        self.convert_effects(ns_effects, now)
    }

    /// Advance the namespace's timer wheel to `now`, firing expired timers
    /// and feeding them back through the namespace. Returns accumulated output.
    pub fn advance_to(&mut self, now: Duration) -> NamespaceOutput {
        let mut output = NamespaceOutput::default();

        loop {
            let expired = self.timer_wheel.fire_expired(now);
            if expired.is_empty() {
                break;
            }

            for fired in expired {
                // Only fire if generation matches (timer wheel handles this).
                let ns_effects = self.namespace.fire_timer(&fired.identity);
                let timer_output = self.convert_effects(ns_effects, now);
                output.merge(timer_output);
            }
        }

        output
    }

    /// Returns the earliest deadline across pending timers for this namespace.
    pub fn next_deadline(&self) -> Option<Duration> {
        self.timer_wheel.next_deadline()
    }

    /// Convert `NamespaceEffects` to `NamespaceOutput`, absorbing timer
    /// actions into the per-namespace timer wheel.
    fn convert_effects(&mut self, effects: NamespaceEffects, now: Duration) -> NamespaceOutput {
        use crate::core::types::NamespaceToOrchestrator;

        // Absorb timer actions into the per-namespace timer wheel.
        if !effects.timer_actions.is_empty() {
            self.timer_wheel.absorb(effects.timer_actions, now);
        }

        // Convert scheduler messages to orchestrator messages.
        let to_orchestrator = effects
            .scheduler_messages
            .into_iter()
            .map(NamespaceToOrchestrator::SchedulerMessage)
            .collect();

        NamespaceOutput {
            to_orchestrator,
            worker_commands: effects.worker_commands,
            broadcast_commands: effects.broadcast_commands,
            observability_events: effects.observability_events,
        }
    }

    // =========================================================================
    // Accessors (delegate to namespace)
    // =========================================================================

    /// Get the set of active worker IDs.
    pub fn active_worker_ids(&self) -> impl Iterator<Item = GlobalWorkerId> + '_ {
        self.namespace.active_worker_ids()
    }

    /// Access the router (for inspecting workload/service/pod state in tests).
    pub fn router(&self) -> &crate::sm::DRouter {
        self.namespace.router()
    }

    /// Access the management adapter.
    pub fn management(&self) -> &crate::adapter::management::ManagementAdapter {
        self.namespace.management()
    }

    /// Access the WireGuard peer manager.
    pub fn wg_peers(&self) -> &wg_peers::WireGuardPeerManager {
        self.namespace.wg_peers()
    }

    /// Access the current namespace spec.
    pub fn current_spec(&self) -> Option<&crate::types::NamespaceSpec> {
        self.namespace.current_spec()
    }

    /// Build a status report for this namespace.
    pub fn status_report(&self) -> crate::types::NamespaceStatusReport {
        self.namespace.status_report()
    }

    /// Map a router-internal PodId to a protocol PodId.
    pub fn router_pod_to_proto(
        &self,
        router_pid: &crate::sm::PodId,
    ) -> Option<distvirt_worker_protocol::PodId> {
        self.namespace.router_pod_to_proto(router_pid)
    }
}
