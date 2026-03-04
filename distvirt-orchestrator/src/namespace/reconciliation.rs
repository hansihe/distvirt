use crate::types::*;
use crate::workload::WorkloadInput;

use super::NamespaceStateMachine;

impl NamespaceStateMachine {
    pub(crate) fn reconcile_all_services(&mut self, out: &mut NamespaceOutput) {
        if self.status == NamespaceStatus::Destroying {
            return;
        }
        let svc_ids: Vec<ServiceId> = self.spec.services.keys().cloned().collect();
        for svc_id in svc_ids {
            self.reconcile_service(&svc_id, out);
        }
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
                    let svc_spec = self.spec.services.get(svc_id).cloned().unwrap();
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
                    self.services.get_mut(svc_id).unwrap().state = ServiceState::Idle;
                } else {
                    // Always-on: demand up immediately.
                    self.services.get_mut(svc_id).unwrap().state = ServiceState::NeedBackend;
                    // Step workload with DemandUp.
                    let wl = self.workloads.get_mut(&wl_id).unwrap();
                    let wl_outputs = wl.step(WorkloadInput::DemandUp, &self.namespace_id);
                    self.forward_workload_outputs(&wl_id, wl_outputs, out);
                }
            }
            (ServiceState::NeedBackend, WorkloadState::Dormant) => {
                // Workload is dormant but service needs backend - demand up.
                let wl = self.workloads.get_mut(&wl_id).unwrap();
                let wl_outputs = wl.step(WorkloadInput::DemandUp, &self.namespace_id);
                self.forward_workload_outputs(&wl_id, wl_outputs, out);
            }
            _ => {}
        }
    }
}
