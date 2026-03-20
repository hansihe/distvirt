//! Per-namespace state: core logic, boundary adapter, timer wheel, WG peers.

pub mod boundary;
pub(crate) mod inner;
pub mod timer_wheel;
pub mod wg_peers;

// TODO: update for EndpointSm refactor
// #[cfg(test)]
// mod tests;

use std::time::Duration;

use boundary::NamespaceWithBoundary;
use timer_wheel::NamespaceTimerWheel;

use crate::adapter::timer::TimerConfig;
use crate::core::types::{
    NamespaceCoreEvent, NamespaceEffects, NamespaceOutput, NamespaceToOrchestrator,
    OrchestratorToNamespace,
};
use crate::core::GlobalWorkerId;
use crate::id_registry::IdRegistry;
use crate::types::NamespaceId;

/// A self-contained namespace unit: boundary adapter + per-namespace timer wheel.
///
/// This is the unit of namespace processing in the split-core architecture.
/// The shell owns a `HashMap<NamespaceId, NamespaceUnit>` and delivers
/// `OrchestratorToNamespace` messages to it.
pub struct NamespaceUnit {
    boundary: NamespaceWithBoundary,
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
            boundary: NamespaceWithBoundary::new(namespace_id, timer_config, network, id_registry),
            timer_wheel: NamespaceTimerWheel::new(),
        }
    }

    /// Process an orchestrator-to-namespace message.
    pub fn process(&mut self, input: OrchestratorToNamespace, now: Duration) -> NamespaceOutput {
        let core_event = input.into_core_event();
        let ns_effects = self.boundary.process_event(core_event);
        self.convert_effects(ns_effects, now)
    }

    /// Advance the namespace's timer wheel to `now`, firing expired timers
    /// and feeding them back through the boundary. Returns accumulated output.
    pub fn advance_to(&mut self, now: Duration) -> NamespaceOutput {
        let mut output = NamespaceOutput::default();

        loop {
            let expired = self.timer_wheel.fire_expired(now);
            if expired.is_empty() {
                break;
            }

            for fired in expired {
                let ns_effects = self.boundary.process_event(NamespaceCoreEvent::TimerFired {
                    identity: fired.identity,
                    generation: fired.generation,
                });
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

    /// Convert internal `NamespaceEffects` to `NamespaceOutput`, absorbing timer
    /// actions into the per-namespace timer wheel.
    fn convert_effects(&mut self, effects: NamespaceEffects, now: Duration) -> NamespaceOutput {
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
    // Accessors (delegate to boundary)
    // =========================================================================

    /// Get the set of active worker IDs.
    pub fn active_worker_ids(&self) -> impl Iterator<Item = GlobalWorkerId> + '_ {
        self.boundary.active_worker_ids()
    }

    /// Access the router (for inspecting workload/service/pod state in tests).
    pub fn router(&self) -> &crate::sm::DRouter {
        self.boundary.router()
    }

    /// Access the management adapter.
    pub fn management(&self) -> &crate::adapter::management::ManagementAdapter {
        self.boundary.management()
    }

    /// Access the WireGuard peer manager.
    pub fn wg_peers(&self) -> &wg_peers::WireGuardPeerManager {
        self.boundary.wg_peers()
    }

    /// Access the current namespace spec.
    pub fn current_spec(&self) -> Option<&crate::types::NamespaceSpec> {
        self.boundary.current_spec()
    }

    /// Build a status report for this namespace.
    pub fn status_report(&self) -> crate::types::NamespaceStatusReport {
        self.boundary.status_report()
    }

    /// Map a router-internal PodId to a protocol PodId.
    pub fn router_pod_to_proto(
        &self,
        router_pid: &crate::sm::PodId,
    ) -> Option<distvirt_worker_protocol::PodId> {
        self.boundary.router_pod_to_proto(router_pid)
    }
}
