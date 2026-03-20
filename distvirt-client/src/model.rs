//! Namespace object model — tracks live state of workloads and services.
//!
//! This model is shared between the CLI and the Python SDK. It can be
//! bootstrapped from a `NamespaceStatusReport` and then kept up to date
//! by applying `NamespaceEvent`s from an event stream.

use std::collections::HashMap;

use distvirt_client_protocol as proto;

// ---------------------------------------------------------------------------
// Workload
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum WorkloadState {
    Unknown,
    Dormant,
    WaitingForSpec,
    Launching { pod_id: String, worker_id: String },
    Running { pod_id: String, worker_id: String },
    Suspending { pod_id: String, worker_id: String },
    Suspended,
    RetryBackoff,
    Failed,
    Completed,
}

#[derive(Debug, Clone)]
pub struct WorkloadModel {
    pub workload_id: String,
    pub state: WorkloadState,
    pub spliced: bool,
    pub ip: Option<String>,
}

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum ServiceState {
    Unknown,
    Pending,
    Idle,
    NeedBackend,
    Active {
        pod_id: String,
        worker_id: String,
    },
}

#[derive(Debug, Clone)]
pub struct ServiceModel {
    pub service_id: String,
    pub workload_id: String,
    pub state: ServiceState,
    pub activation_enabled: bool,
    pub spliced: bool,
    pub ip: Option<String>,
    pub mac: Option<String>,
}

// ---------------------------------------------------------------------------
// Namespace
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum NamespaceState {
    Unknown,
    Creating,
    Active,
    Destroying,
}

#[derive(Debug, Clone)]
pub struct NamespaceModel {
    pub namespace_id: String,
    pub state: NamespaceState,
    pub workloads: HashMap<String, WorkloadModel>,
    pub services: HashMap<String, ServiceModel>,
}

impl NamespaceModel {
    pub fn new(namespace_id: String) -> Self {
        NamespaceModel {
            namespace_id,
            state: NamespaceState::Unknown,
            workloads: HashMap::new(),
            services: HashMap::new(),
        }
    }

    /// Bootstrap the model from a `NamespaceStatusReport`.
    pub fn apply_status(&mut self, status: &proto::NamespaceStatusReport) {
        self.state = match proto::NamespaceState::try_from(status.state) {
            Ok(proto::NamespaceState::Creating) => NamespaceState::Creating,
            Ok(proto::NamespaceState::Active) => NamespaceState::Active,
            Ok(proto::NamespaceState::Destroying) => NamespaceState::Destroying,
            _ => NamespaceState::Unknown,
        };

        self.workloads.clear();
        for (id, ws) in &status.workloads {
            self.workloads.insert(id.clone(), WorkloadModel {
                workload_id: id.clone(),
                state: convert_workload_state(ws.state.as_ref()),
                spliced: ws.spliced,
                ip: if ws.ip.is_empty() { None } else { Some(ws.ip.clone()) },
            });
        }

        self.services.clear();
        for (id, ss) in &status.services {
            self.services.insert(id.clone(), ServiceModel {
                service_id: id.clone(),
                workload_id: ss.workload_id.clone(),
                state: convert_service_state(ss.state.as_ref()),
                activation_enabled: ss.activation_enabled,
                spliced: ss.spliced,
                ip: if ss.ip.is_empty() { None } else { Some(ss.ip.clone()) },
                mac: if ss.mac.is_empty() { None } else { Some(ss.mac.clone()) },
            });
        }
    }

    /// Apply a single `NamespaceEvent` from the event stream.
    pub fn apply_event(&mut self, event: &proto::NamespaceEvent) {
        let Some(ref inner) = event.event else {
            return;
        };

        match inner {
            proto::namespace_event::Event::Workload(we) => {
                self.apply_workload_event(we);
            }
            proto::namespace_event::Event::Pod(pe) => {
                self.apply_pod_event(pe);
            }
            proto::namespace_event::Event::Endpoint(_ee) => {
                // Endpoint events don't mutate the high-level model for now.
                // They're useful for observability but the service state
                // transitions come through the status stream.
            }
        }
    }

    fn apply_workload_event(&mut self, event: &proto::WorkloadEvent) {
        let Some(ref inner) = event.event else {
            return;
        };

        let workload = self.workloads.get_mut(&event.workload_id);

        match inner {
            proto::workload_event::Event::Spliced(_) => {
                if let Some(w) = workload {
                    w.spliced = true;
                }
            }
            proto::workload_event::Event::Unspliced(_) => {
                if let Some(w) = workload {
                    w.spliced = false;
                }
            }
            proto::workload_event::Event::DemandChanged(_) => {
                // Demand info — doesn't change model state directly.
            }
        }
    }

    fn apply_pod_event(&mut self, event: &proto::PodEvent) {
        let workload = self.workloads.get_mut(&event.workload_id);
        let Some(w) = workload else { return };
        let Some(ref inner) = event.event else { return };

        match inner {
            proto::pod_event::Event::Created(_) => {}
            proto::pod_event::Event::Scheduled(e) => {
                w.state = WorkloadState::Launching {
                    pod_id: event.pod_id.clone(),
                    worker_id: e.worker_id.clone(),
                };
            }
            proto::pod_event::Event::Running(e) => {
                w.state = WorkloadState::Running {
                    pod_id: event.pod_id.clone(),
                    worker_id: e.worker_id.clone(),
                };
            }
            proto::pod_event::Event::Stopped(_) => {
                w.state = WorkloadState::Completed;
            }
            proto::pod_event::Event::Failed(_) => {
                w.state = WorkloadState::Failed;
            }
            proto::pod_event::Event::Suspending(e) => {
                w.state = WorkloadState::Suspending {
                    pod_id: event.pod_id.clone(),
                    worker_id: e.worker_id.clone(),
                };
            }
            proto::pod_event::Event::Suspended(_) => {
                w.state = WorkloadState::Suspended;
            }
            proto::pod_event::Event::SuspendFailed(_) => {
                // Revert to running? The proto doesn't make this clear.
                // For now leave state as-is.
            }
            proto::pod_event::Event::Resuming(e) => {
                w.state = WorkloadState::Launching {
                    pod_id: event.pod_id.clone(),
                    worker_id: e.worker_id.clone(),
                };
            }
            proto::pod_event::Event::Displaced(_) | proto::pod_event::Event::Reaped(_) => {
                w.state = WorkloadState::Dormant;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Proto → model conversions
// ---------------------------------------------------------------------------

fn convert_workload_state(state: Option<&proto::WorkloadState>) -> WorkloadState {
    let Some(state) = state else {
        return WorkloadState::Unknown;
    };
    let Some(ref inner) = state.state else {
        return WorkloadState::Unknown;
    };

    match inner {
        proto::workload_state::State::Dormant(_) => WorkloadState::Dormant,
        proto::workload_state::State::WaitingForSpec(_) => WorkloadState::WaitingForSpec,
        proto::workload_state::State::Launching(s) => WorkloadState::Launching {
            pod_id: s.pod_id.clone(),
            worker_id: s.worker_id.clone(),
        },
        proto::workload_state::State::Running(s) => WorkloadState::Running {
            pod_id: s.pod_id.clone(),
            worker_id: s.worker_id.clone(),
        },
        proto::workload_state::State::Suspending(s) => WorkloadState::Suspending {
            pod_id: s.pod_id.clone(),
            worker_id: s.worker_id.clone(),
        },
        proto::workload_state::State::Suspended(_) => WorkloadState::Suspended,
        proto::workload_state::State::RetryBackoff(_) => WorkloadState::RetryBackoff,
        proto::workload_state::State::Failed(_) => WorkloadState::Failed,
        proto::workload_state::State::Completed(_) => WorkloadState::Completed,
    }
}

fn convert_service_state(state: Option<&proto::ServiceState>) -> ServiceState {
    let Some(state) = state else {
        return ServiceState::Unknown;
    };
    let Some(ref inner) = state.state else {
        return ServiceState::Unknown;
    };

    match inner {
        proto::service_state::State::Pending(_) => ServiceState::Pending,
        proto::service_state::State::Idle(_) => ServiceState::Idle,
        proto::service_state::State::NeedBackend(_) => ServiceState::NeedBackend,
        proto::service_state::State::Active(s) => ServiceState::Active {
            pod_id: s.pod_id.clone(),
            worker_id: s.worker_id.clone(),
        },
    }
}
