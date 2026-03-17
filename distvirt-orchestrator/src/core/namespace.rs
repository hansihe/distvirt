//! Pure namespace core — no async, no channels.
//!
//! Extracted from `task/namespace/mod.rs`. This module contains all the
//! pure state and logic for a single namespace, producing effects instead
//! of performing I/O directly.
//!
//! `NamespaceCore` operates exclusively with router-internal IDs. All
//! protocol ↔ router ID translation is handled by `NamespaceWithBoundary`
//! in `namespace_boundary.rs`.

use std::collections::{HashMap, HashSet};

use crate::adapter::endpoint_demand::EndpointDemandAdapter;
use crate::adapter::dns_registry::{DnsRegistryAction, DnsRegistryAdapter};
use crate::adapter::endpoint::{EndpointAction, EndpointAdapter};
use crate::adapter::management::ManagementAdapter;
use crate::adapter::pod_assignment::{PodAssignmentAction, PodAssignmentAdapter};
use crate::adapter::schedule_request::{ScheduleRequestAdapter, ScheduleRequestDelta};
use crate::adapter::timer::{TimerAction, TimerAdapter, TimerConfig};
use crate::core::ClientCommand;
use distvirt_sm_router::trace::PanicTracer;

use crate::sm::{
    AdminCmd, DRouter, DNS_REGISTRY, ENDPOINT, LeaseInfo, PodId, PodStatus, Router,
    SCHEDULE_REQUEST, ScheduleLeaseId, TIMER, WorkerId,
};
use crate::types::{NamespaceId, NamespaceSpec};

use super::types::{InternalNamespaceEffects, InternalNamespaceEvent, InternalSchedulerMessage, InternalWorkerEvent};

#[cfg(test)]
mod tests;

// =============================================================================
// Grouped state
// =============================================================================

/// All pure adapters owned by the namespace.
pub(crate) struct Adapters {
    timer: TimerAdapter,
    pod_assignment: PodAssignmentAdapter,
    schedule_request: ScheduleRequestAdapter,
    pub(crate) management: ManagementAdapter,
    endpoint_demand: EndpointDemandAdapter,
    pub(crate) endpoint: EndpointAdapter,
    pub(crate) dns_registry: DnsRegistryAdapter,
}

// =============================================================================
// Reconcile actions (sync output of reconcile phase)
// =============================================================================

struct ReconcileActions {
    timer_actions: Vec<TimerAction>,
    schedule_deltas: Vec<ScheduleRequestDelta>,
    pod_actions: Vec<PodAssignmentAction>,
    endpoint_actions: Vec<EndpointAction>,
    dns_registry_actions: Vec<DnsRegistryAction>,
}

// =============================================================================
// NamespaceCore
// =============================================================================

pub struct NamespaceCore {
    namespace_id: NamespaceId,
    router: DRouter,
    pub(crate) adapters: Adapters,

    leases: HashMap<PodId, ScheduleLeaseId>,

    /// Tracks which pods are assigned to each worker (for WorkerToPod edge management).
    worker_pod_edges: HashMap<WorkerId, HashSet<PodId>>,
    /// Reverse lookup: pod → assigned worker.
    pod_worker: HashMap<PodId, WorkerId>,

    pub(crate) current_spec: Option<NamespaceSpec>,
}

impl NamespaceCore {
    pub fn new(namespace_id: NamespaceId, timer_config: TimerConfig) -> Self {
        let mut router = Router::new_traced(16, PanicTracer::new());
        router.create_timer(TIMER);
        router.create_schedule_request(SCHEDULE_REQUEST);
        router.create_endpoint(ENDPOINT);
        router.create_dns_registry(DNS_REGISTRY);

        NamespaceCore {
            namespace_id,
            router,
            adapters: Adapters {
                timer: TimerAdapter::new(timer_config),
                pod_assignment: PodAssignmentAdapter::new(),
                schedule_request: ScheduleRequestAdapter::new(SCHEDULE_REQUEST),
                management: ManagementAdapter::new(),
                endpoint_demand: EndpointDemandAdapter::new(),
                endpoint: EndpointAdapter::new(ENDPOINT),
                dns_registry: DnsRegistryAdapter::new(DNS_REGISTRY),
            },
            leases: HashMap::new(),
            worker_pod_edges: HashMap::new(),
            pod_worker: HashMap::new(),
            current_spec: None,
        }
    }

    /// Top-level event processing: push event, propagate, reconcile loop.
    /// Returns all effects to be executed by the boundary layer.
    /// All IDs are router-internal.
    pub(crate) fn process_event(&mut self, event: InternalNamespaceEvent) -> InternalNamespaceEffects {
        let mut effects = InternalNamespaceEffects::default();

        // Phase 1: Push external event into router
        self.push_event(event, &mut effects);

        // Phase 2: Propagate
        self.router.propagate();

        // Phase 3+4: Reconcile and collect effects in a loop until stable.
        // The loop re-propagates only when an adapter signals that it wrote
        // back into the router (mutated_router == true).
        loop {
            let (actions, mutated_router) = self.reconcile();
            self.collect_effects(actions, &mut effects);
            if !mutated_router {
                break;
            }
            self.router.propagate();
        }

        effects
    }

    fn push_event(&mut self, event: InternalNamespaceEvent, _effects: &mut InternalNamespaceEffects) {
        match event {
            InternalNamespaceEvent::WorkerEvent { worker_id, event } => {
                match event {
                    InternalWorkerEvent::PodRunning { pod_id } => {
                        if self.router.get_pod(&pod_id).is_some() {
                            self.router.send_notify_pod_status(
                                worker_id,
                                pod_id,
                                PodStatus::Running,
                            );
                        }
                    }
                    InternalWorkerEvent::PodExited { pod_id, exit_code } => {
                        if self.router.get_pod(&pod_id).is_some() {
                            let status = if exit_code == 0 {
                                PodStatus::Finished
                            } else {
                                PodStatus::Failed
                            };
                            self.router.send_notify_pod_status(worker_id, pod_id, status);
                        }
                    }
                    InternalWorkerEvent::PodFailed { pod_id } => {
                        if self.router.get_pod(&pod_id).is_some() {
                            self.router.send_notify_pod_status(
                                worker_id,
                                pod_id,
                                PodStatus::Failed,
                            );
                        }
                    }
                    InternalWorkerEvent::PodSuspended { pod_id, artifact_id } => {
                        if self.router.get_pod(&pod_id).is_some() {
                            self.router.send_notify_pod_suspended(
                                worker_id,
                                pod_id,
                                artifact_id,
                            );
                        }
                    }
                    InternalWorkerEvent::PodSuspendFailed { pod_id } => {
                        if self.router.get_pod(&pod_id).is_some() {
                            self.router.send_notify_pod_status(
                                worker_id,
                                pod_id,
                                PodStatus::Failed,
                            );
                        }
                    }
                    InternalWorkerEvent::EndpointActivation { service_name } => {
                        self.adapters.management.send_activate_service(
                            &mut self.router,
                            &service_name,
                            true,
                        );
                    }
                    InternalWorkerEvent::EndpointDemand { service_id, active } => {
                        let need = if active {
                            crate::sm::BackendNeed::Active
                        } else {
                            crate::sm::BackendNeed::None
                        };
                        self.adapters.endpoint_demand.push_need(
                            &mut self.router,
                            worker_id,
                            service_id,
                            need,
                        );
                    }
                }
            }
            InternalNamespaceEvent::SchedulerGrant { worker_id, pod_id } => {
                self.apply_grant(worker_id, pod_id);
            }
            InternalNamespaceEvent::SchedulerRevoke { pod_id } => {
                if let Some(lease_id) = self.leases.remove(&pod_id) {
                    self.router.destroy_schedule_lease(lease_id);
                }
                self.remove_pod_from_worker(pod_id);
            }
            InternalNamespaceEvent::TimerFired { identity } => {
                self.adapters.timer.fire(&mut self.router, &identity);
            }
            InternalNamespaceEvent::WorkerActivated { worker_id, info } => {
                self.router.set_worker_info(worker_id, info);
            }
            InternalNamespaceEvent::WorkerDeactivated { worker_id } => {
                // Clean up WorkerToPod edge tracking for this worker.
                if let Some(pods) = self.worker_pod_edges.remove(&worker_id) {
                    for pod_id in pods {
                        self.pod_worker.remove(&pod_id);
                    }
                }

                self.adapters
                    .endpoint_demand
                    .remove_worker(&mut self.router, &worker_id);
                self.router.destroy_worker(worker_id);
            }
            InternalNamespaceEvent::ClientCommand(cmd) => {
                self.handle_client_command(cmd);
            }
        }
    }

    fn handle_client_command(&mut self, cmd: ClientCommand) {
        match cmd {
            ClientCommand::UpdateSpec(new_spec) => {
                self.adapters.management.apply_namespace_spec(
                    &mut self.router,
                    self.current_spec.as_ref(),
                    &new_spec,
                );

                self.current_spec = Some(new_spec);
            }
            ClientCommand::AdminRestart { workload_name } => {
                self.adapters.management.send_admin_command(
                    &mut self.router,
                    &workload_name,
                    AdminCmd::Restart,
                );
            }
            ClientCommand::Scavenge { workload_name } => {
                self.adapters.management.send_admin_command(
                    &mut self.router,
                    &workload_name,
                    AdminCmd::Scavenge,
                );
            }
            ClientCommand::ActivateService {
                service_name,
                active,
            } => {
                self.adapters.management.send_activate_service(
                    &mut self.router,
                    &service_name,
                    active,
                );
            }
        }
    }

    // =========================================================================
    // WorkerToPod edge management
    // =========================================================================

    /// Apply a scheduler grant: create a lease for the pod on the given worker.
    /// Returns false if the pod no longer exists in the router (stale grant).
    fn apply_grant(&mut self, router_worker_id: WorkerId, pod_id: PodId) -> bool {
        if self.router.get_pod(&pod_id).is_none() {
            return false;
        }
        let lease_id = self.router.create_schedule_lease();
        self.router.set_schedule_lease_lease(
            lease_id,
            LeaseInfo {
                worker_id: router_worker_id,
            },
        );
        self.router
            .set_pod_lease_edges(lease_id, vec![pod_id]);
        self.leases.insert(pod_id, lease_id);
        self.add_pod_to_worker(router_worker_id, pod_id);
        true
    }

    /// Add a pod to a worker's WorkerToPod edge set and update the router.
    fn add_pod_to_worker(&mut self, worker_id: WorkerId, pod_id: PodId) {
        self.pod_worker.insert(pod_id, worker_id);
        let pods = self.worker_pod_edges.entry(worker_id).or_default();
        pods.insert(pod_id);
        self.router
            .set_worker_assignment_edges(worker_id, pods.iter().copied().collect::<Vec<_>>());
    }

    /// Remove a pod from its assigned worker's WorkerToPod edge set and update the router.
    fn remove_pod_from_worker(&mut self, pod_id: PodId) {
        if let Some(worker_id) = self.pod_worker.remove(&pod_id) {
            if let Some(pods) = self.worker_pod_edges.get_mut(&worker_id) {
                pods.remove(&pod_id);
                self.router
                    .set_worker_assignment_edges(worker_id, pods.iter().copied().collect::<Vec<_>>());
            }
        }
    }

    /// Phase 3: Reconcile all adapters. Pure/sync — no I/O.
    /// Returns `(actions, mutated_router)` — the flag is `true` when any
    /// adapter wrote back into the router and a new propagation is needed.
    fn reconcile(&mut self) -> (ReconcileActions, bool) {
        let (timer_actions, timer_mut) = self.adapters.timer.reconcile(&mut self.router);
        let (schedule_deltas, sched_mut) = self.adapters.schedule_request.reconcile(&mut self.router);
        let (pod_actions, pod_mut) = self.adapters.pod_assignment.reconcile(&mut self.router);
        let (endpoint_actions, ep_mut) = self.adapters.endpoint.reconcile(&mut self.router);
        let (dns_registry_actions, dns_mut) = self.adapters.dns_registry.reconcile(&mut self.router);

        let mutated = timer_mut || sched_mut || pod_mut || ep_mut || dns_mut;

        (ReconcileActions {
            timer_actions,
            schedule_deltas,
            pod_actions,
            endpoint_actions,
            dns_registry_actions,
        }, mutated)
    }

    /// Phase 4: Translate reconcile actions into internal effects.
    fn collect_effects(&mut self, actions: ReconcileActions, effects: &mut InternalNamespaceEffects) {
        // Timer actions pass through directly.
        effects.timer_actions.extend(actions.timer_actions);

        // Schedule request deltas → internal scheduler messages.
        for delta in actions.schedule_deltas {
            match delta {
                ScheduleRequestDelta::Request { pod_id, request } => {
                    effects
                        .scheduler_messages
                        .push(InternalSchedulerMessage::RequestLease {
                            namespace_id: self.namespace_id.clone(),
                            pod_id,
                            resume_artifact: request.resume_artifact,
                        });
                }
                ScheduleRequestDelta::Drop { pod_id } => {
                    effects
                        .scheduler_messages
                        .push(InternalSchedulerMessage::DropRequest {
                            namespace_id: self.namespace_id.clone(),
                            pod_id,
                        });
                }
            }
        }

        // Pod assignment actions — spec now flows through the signal graph
        // (Workload::PodLaunchSpec → Pod::LaunchSpecInput → PodScheduleRequest → Worker port).
        effects.pod_actions.extend(actions.pod_actions);

        // Endpoint actions pass through directly (already router-level).
        effects.endpoint_actions.extend(actions.endpoint_actions);

        // DNS registry actions pass through directly.
        effects.dns_registry_actions.extend(actions.dns_registry_actions);
    }

    /// Create a new worker port in the router with the given ID.
    pub(crate) fn create_worker_port(&mut self, id: WorkerId) {
        self.router.create_worker(id);
    }

    // =========================================================================
    // Accessors
    // =========================================================================

    /// Access the router (for inspecting workload/service/pod state).
    pub fn router(&self) -> &DRouter {
        &self.router
    }

    /// Mutable access to the router (for test setup).
    pub(crate) fn router_mut(&mut self) -> &mut DRouter {
        &mut self.router
    }

    /// Access the management adapter (for looking up workloads/services by name).
    pub fn management(&self) -> &ManagementAdapter {
        &self.adapters.management
    }

    /// Access the current namespace spec.
    pub fn current_spec(&self) -> Option<&NamespaceSpec> {
        self.current_spec.as_ref()
    }

    /// Get the namespace ID.
    pub(crate) fn namespace_id(&self) -> &NamespaceId {
        &self.namespace_id
    }

}
