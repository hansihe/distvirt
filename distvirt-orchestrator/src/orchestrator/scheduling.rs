use crate::types::*;

use super::Orchestrator;

impl Orchestrator {
    pub(crate) fn process_namespace_output(
        &mut self,
        namespace_id: NamespaceId,
        ns_out: NamespaceOutput,
        out: &mut OrchestratorOutput,
    ) {
        // Merge namespace output into top-level output.
        out.worker_commands
            .extend(ns_out.worker_commands.iter().cloned());
        out.timers_set.extend(ns_out.timers_set.iter().cloned());
        out.timers_cancel
            .extend(ns_out.timers_cancel.iter().cloned());

        let destroyed = ns_out.destroyed;
        let pod_requests = ns_out.pod_requests.clone();
        let resume_requests = ns_out.resume_requests.clone();

        if ns_out != NamespaceOutput::default() {
            out.namespace_outputs
                .push((namespace_id.clone(), ns_out));
        }

        // Process pod scheduling requests from the namespace.
        for req in pod_requests {
            if let Some(worker_id) = self.select_worker_for_pod(&namespace_id) {
                let pod_id = self.gen_pod_id();
                if let Some(ns) = self.namespaces.get_mut(&namespace_id) {
                    let launch_out = ns.step(NamespaceInput::LaunchPod {
                        workload_id: req.workload_id,
                        worker_id,
                        pod_id,
                    }, &mut self.placement_table);
                    // Recursively process outputs from LaunchPod (it won't emit more pod_requests).
                    out.worker_commands
                        .extend(launch_out.worker_commands.iter().cloned());
                    out.timers_set
                        .extend(launch_out.timers_set.iter().cloned());
                    out.timers_cancel
                        .extend(launch_out.timers_cancel.iter().cloned());
                    if launch_out != NamespaceOutput::default() {
                        out.namespace_outputs
                            .push((namespace_id.clone(), launch_out));
                    }
                }
            }
            // If no worker available, workload stays in WaitingForCapacity.
        }

        // Process resume requests from the namespace.
        for req in resume_requests {
            let pod_id = self.gen_pod_id();
            // Look up placement table to resolve worker_id for the artifact.
            let worker_id = self.placement_table
                .get(&req.artifact_id)
                .map(|p| p.worker_id.clone());
            let worker_id = match worker_id {
                Some(wid) => wid,
                None => continue,
            };
            if let Some(ns) = self.namespaces.get_mut(&namespace_id) {
                let resume_out = ns.step(NamespaceInput::ResumePod {
                    workload_id: req.workload_id,
                    worker_id,
                    pod_id,
                    artifact_id: req.artifact_id,
                }, &mut self.placement_table);
                // Recursively process outputs from ResumePod.
                out.worker_commands
                    .extend(resume_out.worker_commands.iter().cloned());
                out.timers_set
                    .extend(resume_out.timers_set.iter().cloned());
                out.timers_cancel
                    .extend(resume_out.timers_cancel.iter().cloned());
                if resume_out != NamespaceOutput::default() {
                    out.namespace_outputs
                        .push((namespace_id.clone(), resume_out));
                }
            }
        }

        // Schedule any workloads waiting for capacity. This is idempotent and covers:
        // NamespaceCreated (worker becomes Active), WorkerLost (workloads move to
        // WaitingForCapacity and may be schedulable on other workers), etc.
        self.schedule_waiting_pods(out);

        // If namespace is fully destroyed, remove it and clean up worker references.
        if destroyed {
            if let Some(ns) = self.namespaces.remove(&namespace_id) {
                self.free_segment_id(ns.segment_id);
            }
            for ws in self.workers.values_mut() {
                ws.namespaces.remove(&namespace_id);
            }
            // Push updated worker registry (segment sets changed).
            self.push_worker_registry(out);
        }
    }

    pub(crate) fn schedule_waiting_pods(&mut self, out: &mut OrchestratorOutput) {
        // Collect (namespace_id, workload_id) pairs for workloads waiting for capacity.
        // Skip namespaces in Destroying state.
        let waiting: Vec<(NamespaceId, WorkloadId)> = self
            .namespaces
            .iter()
            .filter(|(_, ns)| ns.status != NamespaceStatus::Destroying)
            .flat_map(|(ns_id, ns)| {
                ns.workloads
                    .iter()
                    .filter(|(_, wl)| matches!(wl.state, WorkloadState::WaitingForCapacity))
                    .map(move |(wl_id, _)| (ns_id.clone(), wl_id.clone()))
            })
            .collect();

        for (ns_id, wl_id) in waiting {
            if let Some(worker_id) = self.select_worker_for_pod(&ns_id) {
                let pod_id = self.gen_pod_id();
                if let Some(ns) = self.namespaces.get_mut(&ns_id) {
                    let launch_out = ns.step(NamespaceInput::LaunchPod {
                        workload_id: wl_id,
                        worker_id,
                        pod_id,
                    }, &mut self.placement_table);
                    out.worker_commands
                        .extend(launch_out.worker_commands.iter().cloned());
                    out.timers_set
                        .extend(launch_out.timers_set.iter().cloned());
                    out.timers_cancel
                        .extend(launch_out.timers_cancel.iter().cloned());
                    if launch_out != NamespaceOutput::default() {
                        out.namespace_outputs
                            .push((ns_id.clone(), launch_out));
                    }
                }
            }
        }
    }

    pub(crate) fn gen_pod_id(&mut self) -> PodId {
        let id = self.next_pod_id;
        self.next_pod_id += 1;
        PodId(format!("pod-{}", id))
    }

    pub(crate) fn select_worker_for_pod(&self, namespace_id: &NamespaceId) -> Option<WorkerId> {
        let ns = self.namespaces.get(namespace_id)?;
        ns.workers
            .iter()
            .filter(|(_, ws)| ws.fabric_status == FabricStatus::Active)
            .min_by_key(|(wid, _)| ns.pod_map.worker_pod_count(wid))
            .map(|(wid, _)| wid.clone())
    }
}
