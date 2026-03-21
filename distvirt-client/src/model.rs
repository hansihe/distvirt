//! Namespace object model — tracks live state of workloads, services, and pods.
//!
//! This model is shared between the CLI and the Python SDK. It can be
//! bootstrapped from a `NamespaceStatusReport` and then kept up to date
//! by applying `NamespaceEvent`s from an event stream.
//!
//! # State tracking contract
//!
//! - **Workload state** only changes on `WorkloadStateChanged` events.
//! - **Pod state** is tracked independently; pod events do NOT mutate workload state.
//! - **`apply_event`** returns a `StateChange` describing what mutated.

use std::collections::HashMap;

use distvirt_client_protocol as proto;

// ---------------------------------------------------------------------------
// Workload
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum WorkloadState {
    Dormant,
    WaitingForSpec,
    Launching { pod_id: String, worker_id: String },
    Running { pod_id: String, worker_id: String },
    Suspending { pod_id: String, worker_id: String },
    Suspended,
    RetryBackoff,
    Failed { exit_code: Option<i32>, reason: String },
    Completed { exit_code: i32 },
}

impl WorkloadState {
    pub fn label(&self) -> String {
        match self {
            WorkloadState::Dormant => "dormant".into(),
            WorkloadState::WaitingForSpec => "waiting".into(),
            WorkloadState::Launching { .. } => "launching".into(),
            WorkloadState::Running { .. } => "running".into(),
            WorkloadState::Suspending { .. } => "suspending".into(),
            WorkloadState::Suspended => "suspended".into(),
            WorkloadState::RetryBackoff => "retry-backoff".into(),
            WorkloadState::Failed { exit_code, reason } => {
                let mut s = "failed".to_string();
                if let Some(code) = exit_code {
                    s.push_str(&format!(" (exit {})", code));
                }
                if !reason.is_empty() {
                    s.push_str(&format!(": {}", reason));
                }
                s
            }
            WorkloadState::Completed { exit_code } => {
                format!("completed (exit {})", exit_code)
            }
        }
    }

    pub fn detail(&self) -> String {
        match self {
            WorkloadState::Dormant => "dormant".into(),
            WorkloadState::WaitingForSpec => "waiting for spec".into(),
            WorkloadState::Launching { pod_id, worker_id } => {
                format!("launching (pod {} on worker {})", pod_id, worker_id)
            }
            WorkloadState::Running { pod_id, worker_id } => {
                format!("running (pod {} on worker {})", pod_id, worker_id)
            }
            WorkloadState::Suspending { pod_id, worker_id } => {
                format!("suspending (pod {} on worker {})", pod_id, worker_id)
            }
            WorkloadState::Suspended => "suspended".into(),
            WorkloadState::RetryBackoff => "retry backoff".into(),
            WorkloadState::Failed { exit_code, reason } => {
                let mut s = "failed".to_string();
                if let Some(code) = exit_code {
                    s.push_str(&format!(" (exit code {})", code));
                }
                if !reason.is_empty() {
                    s.push_str(&format!(": {}", reason));
                }
                s
            }
            WorkloadState::Completed { exit_code } => {
                format!("completed (exit code {})", exit_code)
            }
        }
    }

    /// Returns true if this is a terminal state (Failed or Completed).
    pub fn is_terminal(&self) -> bool {
        matches!(self, WorkloadState::Failed { .. } | WorkloadState::Completed { .. })
    }
}

#[derive(Debug, Clone)]
pub struct Workload {
    pub state: WorkloadState,
    pub spliced: bool,
    pub ip: Option<String>,
    pub demanding_services: u32,
}

// ---------------------------------------------------------------------------
// Pod
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum PodState {
    Created,
    Scheduled { worker_id: String },
    Running { worker_id: String },
    Suspending { worker_id: String },
    Suspended { worker_id: String, snapshot_id: String },
    Stopped { exit_code: i32 },
    Failed { reason: String },
    Displaced,
    Reaped,
}

impl PodState {
    pub fn label(&self) -> &'static str {
        match self {
            PodState::Created => "created",
            PodState::Scheduled { .. } => "scheduled",
            PodState::Running { .. } => "running",
            PodState::Suspending { .. } => "suspending",
            PodState::Suspended { .. } => "suspended",
            PodState::Stopped { .. } => "stopped",
            PodState::Failed { .. } => "failed",
            PodState::Displaced => "displaced",
            PodState::Reaped => "reaped",
        }
    }

    /// Returns true if this is a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            PodState::Stopped { .. } | PodState::Failed { .. } | PodState::Displaced | PodState::Reaped
        )
    }
}

#[derive(Debug, Clone)]
pub struct Pod {
    pub workload_id: String,
    pub state: PodState,
}

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum BackendNeed {
    None,
    Traffic,
    Active,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ServiceState {
    Pending,
    Idle,
    NeedBackend,
    Active {
        pod_id: String,
        worker_id: String,
        backend_need: BackendNeed,
    },
}

impl ServiceState {
    pub fn label(&self) -> &'static str {
        match self {
            ServiceState::Pending => "pending",
            ServiceState::Idle => "idle",
            ServiceState::NeedBackend => "need-backend",
            ServiceState::Active { .. } => "active",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Service {
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
    Creating,
    Active,
    Destroying,
}

impl NamespaceState {
    pub fn label(&self) -> &'static str {
        match self {
            NamespaceState::Creating => "creating",
            NamespaceState::Active => "active",
            NamespaceState::Destroying => "destroying",
        }
    }
}

// ---------------------------------------------------------------------------
// State change (returned from apply_event)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum StateChange {
    WorkloadStateChanged {
        workload_id: String,
        old: WorkloadState,
        new: WorkloadState,
    },
    WorkloadSpliced {
        workload_id: String,
        worker_id: String,
    },
    WorkloadUnspliced {
        workload_id: String,
    },
    WorkloadDemandChanged {
        workload_id: String,
        demanding_services: u32,
    },
    PodCreated {
        pod_id: String,
        workload_id: String,
    },
    PodStateChanged {
        pod_id: String,
        workload_id: String,
        new_state: PodState,
    },
    PodReaped {
        pod_id: String,
        workload_id: String,
    },
    Endpoint {
        endpoint_id: String,
        service_id: Option<String>,
        workload_id: Option<String>,
    },
}

// ---------------------------------------------------------------------------
// Namespace model
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct NamespaceModel {
    pub namespace_id: String,
    pub state: NamespaceState,
    pub workloads: HashMap<String, Workload>,
    pub services: HashMap<String, Service>,
    pub pods: HashMap<String, Pod>,
}

impl NamespaceModel {
    /// Bootstrap the model from a `NamespaceStatusReport`.
    pub fn from_status(status: &proto::NamespaceStatusReport) -> Self {
        let state = match proto::NamespaceState::try_from(status.state) {
            Ok(proto::NamespaceState::Creating) => NamespaceState::Creating,
            Ok(proto::NamespaceState::Active) => NamespaceState::Active,
            Ok(proto::NamespaceState::Destroying) => NamespaceState::Destroying,
            _ => NamespaceState::Active, // default to active for unrecognized
        };

        let mut workloads = HashMap::new();
        for (id, ws) in &status.workloads {
            workloads.insert(
                id.clone(),
                Workload {
                    state: convert_workload_state(ws.state.as_ref()),
                    spliced: ws.spliced,
                    ip: if ws.ip.is_empty() {
                        None
                    } else {
                        Some(ws.ip.clone())
                    },
                    demanding_services: 0,
                },
            );
        }

        let mut services = HashMap::new();
        for (id, ss) in &status.services {
            services.insert(
                id.clone(),
                Service {
                    workload_id: ss.workload_id.clone(),
                    state: convert_service_state(ss.state.as_ref()),
                    activation_enabled: ss.activation_enabled,
                    spliced: ss.spliced,
                    ip: if ss.ip.is_empty() {
                        None
                    } else {
                        Some(ss.ip.clone())
                    },
                    mac: if ss.mac.is_empty() {
                        None
                    } else {
                        Some(ss.mac.clone())
                    },
                },
            );
        }

        let mut pods = HashMap::new();
        for (id, ps) in &status.pods {
            pods.insert(
                id.clone(),
                Pod {
                    workload_id: ps.workload_id.clone(),
                    state: convert_pod_state_from_enum(ps.state()),
                },
            );
        }

        NamespaceModel {
            namespace_id: status.namespace_id.clone(),
            state,
            workloads,
            services,
            pods,
        }
    }

    /// Apply a single `NamespaceEvent` from the event stream.
    /// Returns `Some(StateChange)` describing what mutated, or `None` if
    /// the event was not applicable (unknown entity, empty event, etc.)
    pub fn apply_event(&mut self, event: &proto::NamespaceEvent) -> Option<StateChange> {
        let inner = event.event.as_ref()?;

        match inner {
            proto::namespace_event::Event::Workload(we) => self.apply_workload_event(we),
            proto::namespace_event::Event::Pod(pe) => self.apply_pod_event(pe),
            proto::namespace_event::Event::Endpoint(ee) => {
                // Endpoint events don't mutate workload/service state in the model.
                // They're useful for observability. We pass them through as StateChange
                // so callers can react to them.
                Some(StateChange::Endpoint {
                    endpoint_id: ee.endpoint_id.clone(),
                    service_id: ee.service_id.clone(),
                    workload_id: ee.workload_id.clone(),
                })
            }
        }
    }

    fn apply_workload_event(&mut self, event: &proto::WorkloadEvent) -> Option<StateChange> {
        let inner = event.event.as_ref()?;

        match inner {
            proto::workload_event::Event::Spliced(s) => {
                if let Some(w) = self.workloads.get_mut(&event.workload_id) {
                    w.spliced = true;
                }
                Some(StateChange::WorkloadSpliced {
                    workload_id: event.workload_id.clone(),
                    worker_id: s.worker_id.clone(),
                })
            }
            proto::workload_event::Event::Unspliced(_) => {
                if let Some(w) = self.workloads.get_mut(&event.workload_id) {
                    w.spliced = false;
                }
                Some(StateChange::WorkloadUnspliced {
                    workload_id: event.workload_id.clone(),
                })
            }
            proto::workload_event::Event::DemandChanged(d) => {
                if let Some(w) = self.workloads.get_mut(&event.workload_id) {
                    w.demanding_services = d.demanding_services;
                }
                Some(StateChange::WorkloadDemandChanged {
                    workload_id: event.workload_id.clone(),
                    demanding_services: d.demanding_services,
                })
            }
            proto::workload_event::Event::StateChanged(sc) => {
                let new_state = convert_workload_state(sc.new_state.as_ref());
                let old_state = if let Some(w) = self.workloads.get_mut(&event.workload_id) {
                    let old = w.state.clone();
                    w.state = new_state.clone();
                    old
                } else {
                    convert_workload_state(sc.old_state.as_ref())
                };
                Some(StateChange::WorkloadStateChanged {
                    workload_id: event.workload_id.clone(),
                    old: old_state,
                    new: new_state,
                })
            }
        }
    }

    fn apply_pod_event(&mut self, event: &proto::PodEvent) -> Option<StateChange> {
        let inner = event.event.as_ref()?;

        match inner {
            proto::pod_event::Event::Created(_) => {
                self.pods.insert(
                    event.pod_id.clone(),
                    Pod {
                        workload_id: event.workload_id.clone(),
                        state: PodState::Created,
                    },
                );
                Some(StateChange::PodCreated {
                    pod_id: event.pod_id.clone(),
                    workload_id: event.workload_id.clone(),
                })
            }
            proto::pod_event::Event::Reaped(_) => {
                self.pods.remove(&event.pod_id);
                Some(StateChange::PodReaped {
                    pod_id: event.pod_id.clone(),
                    workload_id: event.workload_id.clone(),
                })
            }
            _ => {
                let new_state = match inner {
                    proto::pod_event::Event::Scheduled(e) => PodState::Scheduled {
                        worker_id: e.worker_id.clone(),
                    },
                    proto::pod_event::Event::Running(e) => PodState::Running {
                        worker_id: e.worker_id.clone(),
                    },
                    proto::pod_event::Event::Stopped(e) => PodState::Stopped {
                        exit_code: e.exit_code,
                    },
                    proto::pod_event::Event::Failed(e) => PodState::Failed {
                        reason: e.reason.clone(),
                    },
                    proto::pod_event::Event::Suspending(e) => PodState::Suspending {
                        worker_id: e.worker_id.clone(),
                    },
                    proto::pod_event::Event::Suspended(e) => PodState::Suspended {
                        worker_id: e.worker_id.clone(),
                        snapshot_id: e.snapshot_id.clone(),
                    },
                    proto::pod_event::Event::SuspendFailed(_) => {
                        // suspend_failed doesn't map to a PodState — the pod
                        // remains in its previous state. Skip model update.
                        return None;
                    }
                    proto::pod_event::Event::Resuming(e) => PodState::Running {
                        worker_id: e.worker_id.clone(),
                    },
                    proto::pod_event::Event::Displaced(_) => PodState::Displaced,
                    // Created and Reaped handled above
                    proto::pod_event::Event::Created(_) | proto::pod_event::Event::Reaped(_) => {
                        unreachable!()
                    }
                };

                if let Some(pod) = self.pods.get_mut(&event.pod_id) {
                    pod.state = new_state.clone();
                } else {
                    // Pod not in model (maybe we missed the Created event).
                    // Insert it.
                    self.pods.insert(
                        event.pod_id.clone(),
                        Pod {
                            workload_id: event.workload_id.clone(),
                            state: new_state.clone(),
                        },
                    );
                }

                Some(StateChange::PodStateChanged {
                    pod_id: event.pod_id.clone(),
                    workload_id: event.workload_id.clone(),
                    new_state,
                })
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Proto -> model conversions
// ---------------------------------------------------------------------------

fn convert_workload_state(state: Option<&proto::WorkloadState>) -> WorkloadState {
    let Some(state) = state else {
        return WorkloadState::Dormant;
    };
    let Some(ref inner) = state.state else {
        return WorkloadState::Dormant;
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
        proto::workload_state::State::Failed(f) => WorkloadState::Failed {
            exit_code: f.exit_code,
            reason: f.reason.clone(),
        },
        proto::workload_state::State::Completed(c) => WorkloadState::Completed {
            exit_code: c.exit_code,
        },
    }
}

fn convert_service_state(state: Option<&proto::ServiceState>) -> ServiceState {
    let Some(state) = state else {
        return ServiceState::Pending;
    };
    let Some(ref inner) = state.state else {
        return ServiceState::Pending;
    };

    match inner {
        proto::service_state::State::Pending(_) => ServiceState::Pending,
        proto::service_state::State::Idle(_) => ServiceState::Idle,
        proto::service_state::State::NeedBackend(_) => ServiceState::NeedBackend,
        proto::service_state::State::Active(s) => ServiceState::Active {
            pod_id: s.pod_id.clone(),
            worker_id: s.worker_id.clone(),
            backend_need: convert_backend_need(s.backend_need()),
        },
    }
}

fn convert_backend_need(need: proto::BackendNeed) -> BackendNeed {
    match need {
        proto::BackendNeed::None => BackendNeed::None,
        proto::BackendNeed::Traffic => BackendNeed::Traffic,
        proto::BackendNeed::Active => BackendNeed::Active,
        _ => BackendNeed::None,
    }
}

fn convert_pod_state_from_enum(state: proto::PodState) -> PodState {
    match state {
        proto::PodState::Launching => PodState::Created,
        proto::PodState::Running => PodState::Running {
            worker_id: String::new(),
        },
        proto::PodState::Suspending => PodState::Suspending {
            worker_id: String::new(),
        },
        proto::PodState::Suspended => PodState::Suspended {
            worker_id: String::new(),
            snapshot_id: String::new(),
        },
        proto::PodState::Resuming => PodState::Running {
            worker_id: String::new(),
        },
        proto::PodState::Finished => PodState::Stopped { exit_code: 0 },
        proto::PodState::Failed => PodState::Failed {
            reason: String::new(),
        },
        proto::PodState::Displaced => PodState::Displaced,
        _ => PodState::Created,
    }
}
