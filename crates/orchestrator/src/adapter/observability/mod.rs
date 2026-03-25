//! Observability adapter — derives structured events from router signal changes.
//!
//! This adapter is read-only: it drains incremental aggregator outputs from the
//! observability port but never writes back into the router. It runs last in the
//! reconcile loop and never sets `mutated_router`.

use distvirt_sm_router::IncrementalAggregator;

use crate::sm::{
    DRouter, EndpointId, ObservabilityId, ObservabilityPortInput, PodId, PodStatus, WlStatus,
    WorkerId, WorkloadId, endpoint::EndpointStatus,
};

// =============================================================================
// Event types
// =============================================================================

#[derive(Clone, Debug, PartialEq)]
pub enum ObservabilityEvent {
    Pod(PodObservabilityEvent),
    Workload(WorkloadObservabilityEvent),
    Endpoint(EndpointObservabilityEvent),
}

#[derive(Clone, Debug, PartialEq)]
pub struct PodObservabilityEvent {
    pub pod_id: PodId,
    pub event: PodEventKind,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PodEventKind {
    Created,
    StatusChanged { old: PodStatus, new: PodStatus },
    WorkerChanged { old: Option<WorkerId>, new: Option<WorkerId> },
    Reaped { last_status: PodStatus },
    MemoryConstrained {
        reason: distvirt_worker_protocol::MemoryConstraintReason,
    },
    MemoryConstraintCleared,
    OomKill { count: u64 },
}

#[derive(Clone, Debug, PartialEq)]
pub struct WorkloadObservabilityEvent {
    pub workload_id: WorkloadId,
    pub event: WorkloadEventKind,
}

#[derive(Clone, Debug, PartialEq)]
pub enum WorkloadEventKind {
    StatusChanged { old: WlStatus, new: WlStatus },
}

#[derive(Clone, Debug, PartialEq)]
pub struct EndpointObservabilityEvent {
    pub endpoint_id: EndpointId,
    pub event: EndpointEventKind,
}

#[derive(Clone, Debug, PartialEq)]
pub enum EndpointEventKind {
    StatusChanged { old: EndpointStatus, new: EndpointStatus },
    IdleTimerChanged { active: bool },
}

// =============================================================================
// Incremental aggregators
// =============================================================================

/// Pod status → observability events.
#[derive(Default)]
pub struct PodStatusObsAggregator;

impl IncrementalAggregator for PodStatusObsAggregator {
    type Input = (PodId, PodStatus);
    type Output = ObservabilityEvent;

    fn added(&self, (pod_id, _status): &(PodId, PodStatus)) -> Option<ObservabilityEvent> {
        Some(ObservabilityEvent::Pod(PodObservabilityEvent {
            pod_id: *pod_id,
            event: PodEventKind::Created,
        }))
    }

    fn removed(&self, (pod_id, last_status): &(PodId, PodStatus)) -> Option<ObservabilityEvent> {
        Some(ObservabilityEvent::Pod(PodObservabilityEvent {
            pod_id: *pod_id,
            event: PodEventKind::Reaped {
                last_status: last_status.clone(),
            },
        }))
    }

    fn changed(
        &self,
        (pod_id, old): &(PodId, PodStatus),
        (_pod_id, new): &(PodId, PodStatus),
    ) -> Option<ObservabilityEvent> {
        Some(ObservabilityEvent::Pod(PodObservabilityEvent {
            pod_id: *pod_id,
            event: PodEventKind::StatusChanged {
                old: old.clone(),
                new: new.clone(),
            },
        }))
    }
}

/// Pod assigned worker → observability events.
#[derive(Default)]
pub struct PodWorkerObsAggregator;

impl IncrementalAggregator for PodWorkerObsAggregator {
    type Input = (PodId, Option<WorkerId>);
    type Output = ObservabilityEvent;

    fn added(&self, _input: &(PodId, Option<WorkerId>)) -> Option<ObservabilityEvent> {
        // Pod creation is handled by PodStatusObsAggregator.
        None
    }

    fn removed(&self, _input: &(PodId, Option<WorkerId>)) -> Option<ObservabilityEvent> {
        // Pod removal is handled by PodStatusObsAggregator.
        None
    }

    fn changed(
        &self,
        (pod_id, old): &(PodId, Option<WorkerId>),
        (_pod_id, new): &(PodId, Option<WorkerId>),
    ) -> Option<ObservabilityEvent> {
        Some(ObservabilityEvent::Pod(PodObservabilityEvent {
            pod_id: *pod_id,
            event: PodEventKind::WorkerChanged {
                old: *old,
                new: *new,
            },
        }))
    }
}

/// Workload status → observability events.
#[derive(Default)]
pub struct WorkloadStatusObsAggregator;

impl IncrementalAggregator for WorkloadStatusObsAggregator {
    type Input = (WorkloadId, WlStatus);
    type Output = ObservabilityEvent;

    fn added(&self, _input: &(WorkloadId, WlStatus)) -> Option<ObservabilityEvent> {
        None
    }

    fn removed(&self, _input: &(WorkloadId, WlStatus)) -> Option<ObservabilityEvent> {
        None
    }

    fn changed(
        &self,
        (workload_id, old): &(WorkloadId, WlStatus),
        (_workload_id, new): &(WorkloadId, WlStatus),
    ) -> Option<ObservabilityEvent> {
        Some(ObservabilityEvent::Workload(WorkloadObservabilityEvent {
            workload_id: *workload_id,
            event: WorkloadEventKind::StatusChanged {
                old: old.clone(),
                new: new.clone(),
            },
        }))
    }
}

/// Endpoint status → observability events.
#[derive(Default)]
pub struct EndpointStatusObsAggregator;

impl IncrementalAggregator for EndpointStatusObsAggregator {
    type Input = (EndpointId, EndpointStatus);
    type Output = ObservabilityEvent;

    fn added(&self, _input: &(EndpointId, EndpointStatus)) -> Option<ObservabilityEvent> {
        None
    }

    fn removed(&self, _input: &(EndpointId, EndpointStatus)) -> Option<ObservabilityEvent> {
        None
    }

    fn changed(
        &self,
        (endpoint_id, old): &(EndpointId, EndpointStatus),
        (_endpoint_id, new): &(EndpointId, EndpointStatus),
    ) -> Option<ObservabilityEvent> {
        Some(ObservabilityEvent::Endpoint(EndpointObservabilityEvent {
            endpoint_id: *endpoint_id,
            event: EndpointEventKind::StatusChanged {
                old: old.clone(),
                new: new.clone(),
            },
        }))
    }
}

/// Endpoint idle timer → observability events.
#[derive(Default)]
pub struct EndpointIdleTimerObsAggregator;

impl IncrementalAggregator for EndpointIdleTimerObsAggregator {
    type Input = (EndpointId, bool);
    type Output = ObservabilityEvent;

    fn added(&self, _input: &(EndpointId, bool)) -> Option<ObservabilityEvent> {
        None
    }

    fn removed(&self, _input: &(EndpointId, bool)) -> Option<ObservabilityEvent> {
        None
    }

    fn changed(
        &self,
        (_endpoint_id, _old): &(EndpointId, bool),
        (endpoint_id, new): &(EndpointId, bool),
    ) -> Option<ObservabilityEvent> {
        Some(ObservabilityEvent::Endpoint(EndpointObservabilityEvent {
            endpoint_id: *endpoint_id,
            event: EndpointEventKind::IdleTimerChanged { active: *new },
        }))
    }
}

// =============================================================================
// Adapter
// =============================================================================

pub struct ObservabilityAdapter {
    observability_id: ObservabilityId,
}

impl ObservabilityAdapter {
    pub fn new(observability_id: ObservabilityId) -> Self {
        ObservabilityAdapter { observability_id }
    }

    /// Drain observability inputs from the router.
    /// Read-only — never mutates the router, so always returns `false` for mutated.
    pub fn reconcile(&mut self, router: &mut DRouter) -> Vec<ObservabilityEvent> {
        let inputs = router.drain_observability_inputs();
        inputs
            .into_iter()
            .filter(|(id, _)| *id == self.observability_id)
            .filter_map(|(_, input)| match input {
                ObservabilityPortInput::PodStatusInput(ev) => Some(ev),
                ObservabilityPortInput::PodWorkerInput(ev) => Some(ev),
                ObservabilityPortInput::WorkloadStatusInput(ev) => Some(ev),
                ObservabilityPortInput::EndpointStatusInput(ev) => Some(ev),
                ObservabilityPortInput::EndpointIdleTimerInput(ev) => Some(ev),
            })
            .collect()
    }
}
