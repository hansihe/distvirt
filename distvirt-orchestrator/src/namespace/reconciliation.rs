use crate::service::ServiceInput;
use crate::types::*;
use crate::workload::WorkloadInput;

use super::output::PendingOutput;
use super::NamespaceStateMachine;

impl NamespaceStateMachine {
    pub(crate) fn reconcile_all_services(&mut self, placement_table: &mut PlacementTable, out: &mut NamespaceOutput) {
        if self.status == NamespaceStatus::Destroying {
            return;
        }
        let svc_ids: Vec<ServiceId> = self.spec.services.keys().cloned().collect();
        for svc_id in svc_ids {
            self.reconcile_service(&svc_id, out);
        }
        // After all services are reconciled, reconcile demand for all workloads.
        self.reconcile_all_demand(placement_table, out);
    }

    fn reconcile_service(&mut self, svc_id: &ServiceId, out: &mut NamespaceOutput) {
        let svc = match self.services.get(svc_id) {
            Some(s) => s,
            None => return,
        };
        let wl_id = svc.workload_id.clone();
        let has_activation = svc.has_activation;

        let wl = match self.workloads.get(&wl_id) {
            Some(w) => w,
            None => return,
        };

        let svc_state = &svc.state;
        let wl_state = &wl.state;

        match (svc_state, wl_state) {
            (ServiceState::Pending, WorkloadState::Dormant) => {
                if has_activation {
                    // Create the service on workers and move to Idle.
                    let ns_id = self.namespace_id.clone();
                    let svc_spec = self.spec.services.get(svc_id).cloned()
                        .expect("invariant: svc_id from reconcile_pair must exist in spec.services");
                    for wid in self.active_worker_ids() {
                        out.worker_commands.push((
                            wid,
                            WorkerCommand::CreateService {
                                namespace_id: ns_id.clone(),
                                service_id: svc_id.clone(),
                                ip: svc_spec.ip,
                                policy: svc_spec.policy.clone(),
                            },
                        ));
                    }
                    self.services.get_mut(svc_id)
                        .expect("invariant: svc_id from reconcile_pair must exist in services")
                        .state = ServiceState::Idle;
                } else {
                    // Always-on: move to NeedBackend. Demand reconciliation will handle waking the workload.
                    self.services.get_mut(svc_id)
                        .expect("invariant: svc_id from reconcile_pair must exist in services")
                        .state = ServiceState::NeedBackend;
                }
            }
            (ServiceState::NeedBackend, WorkloadState::Dormant) => {
                // Demand reconciliation will handle waking the workload (no-op here).
            }
            _ => {}
        }
    }

    /// Compute effective demand for a workload: count of services with wants_backend() + route_miss_wake.
    pub(crate) fn effective_demand(&self, workload_id: &WorkloadId) -> u32 {
        let service_demand: u32 = self
            .service_workload
            .iter()
            .filter(|(_, wl_id)| *wl_id == workload_id)
            .filter(|(svc_id, _)| {
                self.services
                    .get(svc_id)
                    .map(|svc| svc.wants_backend())
                    .unwrap_or(false)
            })
            .count() as u32;

        let route_miss: u32 = self
            .workloads
            .get(workload_id)
            .map(|wl| if wl.route_miss_wake { 1 } else { 0 })
            .unwrap_or(0);

        service_demand + route_miss
    }

    /// Reconcile demand for a single workload: compute effective_demand, compare to current_demand,
    /// send SetDemand if different.
    pub(crate) fn reconcile_demand(
        &mut self,
        workload_id: &WorkloadId,
        placement_table: &mut PlacementTable,
        out: &mut NamespaceOutput,
    ) {
        let new_demand = self.effective_demand(workload_id);
        let current = match self.workloads.get(workload_id) {
            Some(wl) => wl.current_demand,
            None => return,
        };

        if new_demand != current {
            out.events.push(SmNamespaceEvent::Workload {
                workload_id: workload_id.clone(),
                event: SmWorkloadEvent::DemandChanged {
                    demanding_services: new_demand,
                },
            });

            let wl_outputs = if let Some(wl) = self.workloads.get_mut(workload_id) {
                wl.step(
                    WorkloadInput::SetDemand { count: new_demand },
                    &self.namespace_id,
                )
            } else {
                return;
            };
            self.process_outputs(
                PendingOutput::Workload {
                    workload_id: workload_id.clone(),
                    outputs: wl_outputs,
                },
                placement_table,
                out,
            );
        }

        // Late-joiner: if workload is Running and any service is in NeedBackend,
        // send WorkloadReady so it can transition to Active.
        self.notify_late_joiner_services(workload_id, placement_table, out);
    }

    /// Notify services in NeedBackend state when the workload is already Running.
    /// This handles the "late-joiner" case: a service activates while the workload
    /// is already Running, so no BecameReady event is emitted.
    fn notify_late_joiner_services(
        &mut self,
        workload_id: &WorkloadId,
        placement_table: &mut PlacementTable,
        out: &mut NamespaceOutput,
    ) {
        // Check if workload is Running and extract pod_id/worker_id.
        let (pod_id, worker_id) = match self.workloads.get(workload_id) {
            Some(wl) => match &wl.state {
                WorkloadState::Running { pod_id, worker_id } => {
                    (pod_id.clone(), worker_id.clone())
                }
                _ => return,
            },
            None => return,
        };

        // Construct backend from workload spec.
        let backend = match self.spec.workloads.get(workload_id) {
            Some(wl_spec) => ServiceBackend {
                pod_ip: wl_spec.network.ip,
            },
            None => return,
        };

        // Find services in NeedBackend state for this workload.
        let svc_ids: Vec<ServiceId> = self
            .service_workload
            .iter()
            .filter(|(_, wl_id)| *wl_id == workload_id)
            .filter(|(svc_id, _)| {
                self.services
                    .get(svc_id)
                    .map(|svc| matches!(svc.state, ServiceState::NeedBackend))
                    .unwrap_or(false)
            })
            .map(|(sid, _)| sid.clone())
            .collect();

        for sid in svc_ids {
            if let Some(svc) = self.services.get_mut(&sid) {
                let svc_outputs = svc.step(
                    ServiceInput::WorkloadReady {
                        pod_id: pod_id.clone(),
                        worker_id: worker_id.clone(),
                        backend: backend.clone(),
                    },
                    &self.namespace_id,
                );
                if !svc_outputs.is_empty() {
                    self.process_outputs(
                        PendingOutput::Service {
                            service_id: sid.clone(),
                            workload_id: workload_id.clone(),
                            outputs: svc_outputs,
                        },
                        placement_table,
                        out,
                    );
                }
            }
        }
    }

    /// Reconcile demand for all workloads.
    pub(crate) fn reconcile_all_demand(
        &mut self,
        placement_table: &mut PlacementTable,
        out: &mut NamespaceOutput,
    ) {
        let wl_ids: Vec<WorkloadId> = self.workloads.keys().cloned().collect();
        for wl_id in wl_ids {
            self.reconcile_demand(&wl_id, placement_table, out);
        }
    }
}
