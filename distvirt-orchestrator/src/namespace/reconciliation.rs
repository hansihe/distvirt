use crate::sm::service::ServiceInput;
use crate::types::*;
use crate::sm::workload::WorkloadInput;

use super::NamespaceStateMachine;

impl NamespaceStateMachine {
    pub(crate) fn reconcile_all_services(&mut self, placement_table: &mut PlacementTable, out: &mut NamespaceOutput) {
        if self.status == NamespaceStatus::Destroying {
            return;
        }

        // After all services are reconciled, reconcile demand for all workloads.
        self.reconcile_all_demand(placement_table, out);
    }

    /// Compute effective demand for a workload: count of services with wants_backend() + active_flows.
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

        let has_active_flows: u32 = if self.active_flows.contains(workload_id) { 1 } else { 0 };

        service_demand + has_active_flows
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

    /// Reconcile service readiness based on cached workload readiness.
    ///
    /// - Ready: service in NeedBackend → send WorkloadReady
    /// - Not ready: service Active → send WorkloadUnready
    ///   (service SM handles Active → NeedBackend directly when has_activation is true)
    fn reconcile_readiness(
        &mut self,
        workload_id: &WorkloadId,
        out: &mut NamespaceOutput,
    ) {
        let ready_info = self.workload_readiness.get(workload_id).cloned();

        // Collect service IDs mapped to this workload.
        let svc_ids: Vec<ServiceId> = self
            .service_workload
            .iter()
            .filter(|(_, wl_id)| *wl_id == workload_id)
            .map(|(sid, _)| sid.clone())
            .collect();

        if let Some(info) = ready_info {
            // Construct backend from workload spec.
            let backend = match self.spec.workloads.get(workload_id) {
                Some(wl_spec) => ServiceBackend {
                    pod_ip: wl_spec.network.ip,
                },
                None => return,
            };

            for sid in svc_ids {
                if !self.services.contains_key(&sid) {
                    continue;
                }

                let svc = self.services.get_mut(&sid).unwrap();
                let svc_outputs = svc.step(
                    ServiceInput::WorkloadReady {
                        pod_id: info.pod_id.clone(),
                        worker_id: info.worker_id.clone(),
                        backend: backend.clone(),
                    },
                    &self.namespace_id,
                );
                if !svc_outputs.is_empty() {
                    self.translate_service_effects(&sid, svc_outputs, out);
                }
            }
        } else {
            // Workload not ready: send WorkloadUnready to Active services.
            // No re-activation dance needed — service SM handles
            // Active → NeedBackend directly when has_activation is true.
            for sid in &svc_ids {
                if !self.services.contains_key(sid) {
                    continue;
                }

                let svc = self.services.get_mut(sid).unwrap();
                let svc_outputs = svc.step(
                    ServiceInput::WorkloadUnready,
                    &self.namespace_id,
                );
                if !svc_outputs.is_empty() {
                    self.translate_service_effects(&sid, svc_outputs, out);
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
