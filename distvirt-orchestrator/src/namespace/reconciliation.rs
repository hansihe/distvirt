use crate::service::ServiceInput;
use crate::types::*;
use crate::workload::WorkloadInput;

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
            (ServiceState::Pending, wl) => {
                let is_running = matches!(wl, WorkloadState::Running { .. });

                // Broadcast an endpoint update for this service.
                self.emit_endpoint_update_for_service(svc_id, out);

                if is_running {
                    // Workload already running — go straight to NeedBackend so
                    // reconcile_readiness (called from reconcile_demand) will
                    // deliver WorkloadReady → Active.
                    self.services.get_mut(svc_id)
                        .expect("invariant: svc_id from reconcile_pair must exist in services")
                        .state = ServiceState::NeedBackend;
                } else if has_activation {
                    // Direct state assignment (bypasses step()): initial Pending→Idle
                    // transition has no side effects to emit (no timers, no worker
                    // commands — endpoint state is synced via EndpointSync/EndpointUpdate).
                    self.services.get_mut(svc_id)
                        .expect("invariant: svc_id from reconcile_pair must exist in services")
                        .state = ServiceState::Idle;
                } else {
                    // Direct state assignment (bypasses step()): initial Pending→NeedBackend
                    // transition has no side effects. Demand reconciliation handles waking.
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
    pub fn effective_demand(&self, workload_id: &WorkloadId) -> u32 {
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
            .map(|wl| if wl.has_active_flows || wl.route_miss_wake { 1 } else { 0 })
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
            self.translate_workload_effects(workload_id, wl_outputs, placement_table, out);
        }

        // Reconcile readiness: sync service states based on workload state.
        self.reconcile_readiness(workload_id, out);
    }

    /// Reconcile service readiness based on workload state.
    ///
    /// - WorkloadReady: workload Running + service in NeedBackend → send WorkloadReady
    /// - WorkloadUnready: workload not Running + service Active → send WorkloadUnready
    ///   If `needs_successful_boot`, immediately re-activate activation services that
    ///   went Idle (preserves demand through worker loss / pod failure recovery).
    fn reconcile_readiness(
        &mut self,
        workload_id: &WorkloadId,
        out: &mut NamespaceOutput,
    ) {
        let wl = match self.workloads.get(workload_id) {
            Some(wl) => wl,
            None => return,
        };

        let is_running = matches!(wl.state, WorkloadState::Running { .. });
        let needs_boot = wl.needs_successful_boot;

        // Collect service IDs mapped to this workload.
        let svc_ids: Vec<ServiceId> = self
            .service_workload
            .iter()
            .filter(|(_, wl_id)| *wl_id == workload_id)
            .map(|(sid, _)| sid.clone())
            .collect();

        if is_running {
            // Extract pod_id/worker_id for WorkloadReady.
            let (pod_id, worker_id) = match &wl.state {
                WorkloadState::Running { pod_id, worker_id } => {
                    (pod_id.clone(), worker_id.clone())
                }
                _ => unreachable!(),
            };

            // Construct backend from workload spec.
            let backend = match self.spec.workloads.get(workload_id) {
                Some(wl_spec) => ServiceBackend {
                    pod_ip: wl_spec.network.ip,
                },
                None => return,
            };

            for sid in svc_ids {
                let svc = match self.services.get(&sid) {
                    Some(s) => s,
                    None => continue,
                };
                if !matches!(svc.state, ServiceState::NeedBackend) {
                    continue;
                }

                // Emit BackendReady observability event.
                out.events.push(SmNamespaceEvent::Service {
                    service_id: sid.clone(),
                    workload_id: workload_id.clone(),
                    event: SmServiceEvent::BackendReady,
                });

                let svc = self.services.get_mut(&sid).unwrap();
                let svc_outputs = svc.step(
                    ServiceInput::WorkloadReady {
                        pod_id: pod_id.clone(),
                        worker_id: worker_id.clone(),
                        backend: backend.clone(),
                    },
                    &self.namespace_id,
                );
                if !svc_outputs.is_empty() {
                    self.translate_service_effects(&sid, svc_outputs, out);
                }
            }
        } else {
            // Workload not running: send WorkloadUnready to Active services.
            // If needs_successful_boot, immediately re-activate activation services
            // that went Active → Idle (preserves demand through recovery).
            for sid in &svc_ids {
                let svc = match self.services.get(sid) {
                    Some(s) => s,
                    None => continue,
                };
                if !matches!(svc.state, ServiceState::Active { .. }) {
                    continue;
                }

                let svc = self.services.get_mut(sid).unwrap();
                let has_activation = svc.has_activation;
                let svc_outputs = svc.step(
                    ServiceInput::WorkloadUnready,
                    &self.namespace_id,
                );
                if !svc_outputs.is_empty() {
                    self.translate_service_effects(&sid, svc_outputs, out);
                }

                // If committed to booting and this activation service just went
                // Idle, re-activate it so it transitions Idle → NeedBackend and
                // preserves demand through reconciliation.
                let svc = self.services.get(sid).unwrap();
                if needs_boot && has_activation && matches!(svc.state, ServiceState::Idle) {
                    let svc = self.services.get_mut(sid).unwrap();
                    let svc_outputs = svc.step(
                        ServiceInput::ServiceActivation,
                        &self.namespace_id,
                    );
                    if !svc_outputs.is_empty() {
                        self.translate_service_effects(&sid, svc_outputs, out);
                    }
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
