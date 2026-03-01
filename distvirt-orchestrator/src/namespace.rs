use std::collections::HashMap;

use crate::types::*;

pub struct NamespaceStateMachine {
    pub spec: NamespaceSpec,
    pub status: NamespaceStatus,
    pub services: HashMap<ServiceId, ServiceState>,
    pub pods: HashMap<PodId, PodInfo>,
    pub workers: HashMap<WorkerId, NamespaceWorkerState>,
}

impl NamespaceStateMachine {
    pub fn new(spec: NamespaceSpec) -> Self {
        let services = spec
            .services
            .keys()
            .map(|id| (id.clone(), ServiceState::Pending))
            .collect();

        NamespaceStateMachine {
            spec,
            status: NamespaceStatus::Creating,
            services,
            pods: HashMap::new(),
            workers: HashMap::new(),
        }
    }

    /// Pure state transition. No I/O.
    pub fn step(&mut self, input: NamespaceInput) -> NamespaceOutput {
        let mut out = NamespaceOutput::default();

        match input {
            NamespaceInput::WorkerEvent { worker_id, event } => {
                self.handle_worker_event(&worker_id, event, &mut out);
            }
            NamespaceInput::WorkerLost { worker_id } => {
                self.handle_worker_lost(&worker_id, &mut out);
            }
            NamespaceInput::TimerFired { timer_key } => {
                self.handle_timer_fired(&timer_key, &mut out);
            }
            NamespaceInput::UpdateSpec { client_id, spec } => {
                self.handle_update_spec(client_id, spec, &mut out);
            }
            NamespaceInput::Delete { client_id } => {
                self.handle_delete(client_id, &mut out);
            }
            NamespaceInput::GetStatus { client_id } => {
                self.handle_get_status(client_id, &mut out);
            }
            NamespaceInput::Splice {
                client_id,
                service_id,
                worker_id,
            } => {
                self.handle_splice(client_id, service_id, worker_id, &mut out);
            }
            NamespaceInput::Unsplice {
                client_id,
                service_id,
            } => {
                self.handle_unsplice(client_id, service_id, &mut out);
            }
            NamespaceInput::StreamLogs {
                client_id,
                service_id,
            } => {
                self.handle_stream_logs(client_id, service_id, &mut out);
            }
            NamespaceInput::CapacityAvailable => {
                self.handle_capacity_available(&mut out);
            }
        }

        out
    }

    // --- Stub handlers ---
    // Each returns default output with TODO for future implementation.

    fn handle_worker_event(
        &mut self,
        _worker_id: &WorkerId,
        _event: WorkerEvent,
        _out: &mut NamespaceOutput,
    ) {
        // TODO: apply worker event to state, reconcile affected services
    }

    fn handle_worker_lost(
        &mut self,
        worker_id: &WorkerId,
        _out: &mut NamespaceOutput,
    ) {
        // Remove the worker from our map.
        // TODO: mark all pods on this worker as lost, transition affected services
        self.workers.remove(worker_id);
    }

    fn handle_timer_fired(
        &mut self,
        _timer_key: &TimerKey,
        _out: &mut NamespaceOutput,
    ) {
        // TODO: handle idle timeout, launch timeout (as hints, not commands)
    }

    fn handle_update_spec(
        &mut self,
        client_id: ClientId,
        spec: NamespaceSpec,
        out: &mut NamespaceOutput,
    ) {
        self.spec = spec;
        // TODO: reconcile_all_services
        out.client_events.push((client_id, ClientEvent::Ok));
    }

    fn handle_delete(
        &mut self,
        client_id: ClientId,
        out: &mut NamespaceOutput,
    ) {
        self.status = NamespaceStatus::Destroying;
        // TODO: begin_destroy — send DestroyNamespace to all workers
        out.client_events.push((client_id, ClientEvent::Ok));
    }

    fn handle_get_status(
        &self,
        client_id: ClientId,
        out: &mut NamespaceOutput,
    ) {
        // TODO: build proper status report
        out.client_events.push((
            client_id,
            ClientEvent::Error {
                message: "GetStatus not yet implemented".to_string(),
            },
        ));
    }

    fn handle_splice(
        &mut self,
        client_id: ClientId,
        _service_id: ServiceId,
        _worker_id: WorkerId,
        out: &mut NamespaceOutput,
    ) {
        // TODO: implement splice flow
        out.client_events.push((client_id, ClientEvent::Ok));
    }

    fn handle_unsplice(
        &mut self,
        client_id: ClientId,
        _service_id: ServiceId,
        out: &mut NamespaceOutput,
    ) {
        // TODO: implement unsplice flow
        out.client_events.push((client_id, ClientEvent::Ok));
    }

    fn handle_stream_logs(
        &self,
        client_id: ClientId,
        _service_id: Option<ServiceId>,
        out: &mut NamespaceOutput,
    ) {
        // TODO: set up log streaming
        out.client_events.push((client_id, ClientEvent::Ok));
    }

    fn handle_capacity_available(&mut self, _out: &mut NamespaceOutput) {
        // TODO: reconcile_all_services — services in WaitingForCapacity retry worker selection
    }
}
