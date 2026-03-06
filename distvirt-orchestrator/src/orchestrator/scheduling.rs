use crate::types::*;

use super::Orchestrator;

impl Orchestrator {
    pub(crate) fn process_namespace_output(
        &mut self,
        namespace_id: NamespaceId,
        ns_out: NamespaceOutput,
        out: &mut OrchestratorOutput,
    ) {
        let destroyed = ns_out.destroyed;
        let pod_requests = ns_out.pod_requests.clone();
        let resume_requests = ns_out.resume_requests.clone();

        // Merge namespace output into top-level output.
        out.merge_namespace(namespace_id.clone(), ns_out);

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
                    out.merge_namespace(namespace_id.clone(), launch_out);
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
                out.merge_namespace(namespace_id.clone(), resume_out);
            }
        }

        // Schedule any workloads waiting for capacity. This is idempotent and covers:
        // NamespaceCreated (worker becomes Active), WorkerLost (workloads move to
        // WaitingForCapacity and may be schedulable on other workers), etc.
        self.schedule_waiting_pods(out);

        // Recompute pressure for all workers after pod count may have changed.
        self.recompute_all_worker_pressure();

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
        // BTreeMap iteration is sorted, so the result is deterministic.
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
                    out.merge_namespace(ns_id.clone(), launch_out);
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
            // Hard constraints: must be active, must not be at High or Critical pressure.
            .filter(|(wid, ws)| {
                if ws.fabric_status != FabricStatus::Active {
                    return false;
                }
                // Check global worker pressure bands.
                if let Some(global_ws) = self.workers.get(*wid) {
                    global_ws.pressure_bands.max_band() < PressureBand::High
                } else {
                    false
                }
            })
            // Soft preferences: lowest pressure band first, then fewest pods, then WorkerId for determinism.
            .min_by_key(|(wid, _)| {
                let band = self.workers.get(*wid)
                    .map(|ws| ws.pressure_bands.max_band())
                    .unwrap_or(PressureBand::Critical);
                (band, ns.pod_map.worker_pod_count(wid), (*wid).clone())
            })
            .map(|(wid, _)| wid.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::namespace::NamespaceStateMachine;
    use crate::pod_map::PodMap;
    use std::collections::{BTreeMap, BTreeSet};
    use std::net::Ipv4Addr;

    fn test_ns_spec() -> NamespaceSpec {
        NamespaceSpec {
            network: NetworkConfig {
                subnet: Ipv4Addr::new(172, 16, 0, 0),
                gateway: Ipv4Addr::new(172, 16, 0, 1),
                prefix_len: 24,
                segment_id: None,
            },
            workloads: BTreeMap::new(),
            services: BTreeMap::new(),
        }
    }

    fn test_worker_state(ns_id: &NamespaceId) -> WorkerState {
        let mut namespaces = BTreeSet::new();
        namespaces.insert(ns_id.clone());
        WorkerState {
            capabilities: WorkerCapabilities {
                max_pods: 10,
                available_memory_mb: 1024,
                public_endpoint: String::new(),
                pools: vec![],
            },
            namespaces,
            wg_config: None,
            tunnel_config: None,
            conditions: BTreeMap::new(),
            transfer_listen_port: None,
            pressure: WorkerPressure::default(),
            pressure_bands: PressureBands::default(),
        }
    }

    fn setup_orchestrator(worker_count: usize) -> (Orchestrator, NamespaceId, Vec<WorkerId>) {
        let ns_id = NamespaceId::from("test-ns");
        let mut orch = Orchestrator::new();

        let mut ns = NamespaceStateMachine::new(ns_id.clone(), test_ns_spec(), 1);
        let mut worker_ids = Vec::new();

        for i in 0..worker_count {
            let wid = WorkerId(format!("w-{}", i));
            ns.workers.insert(wid.clone(), NamespaceWorkerState {
                fabric_status: FabricStatus::Active,
                primary_pool_id: None,
                pressure_band: PressureBand::Normal,
            });
            orch.workers.insert(wid.clone(), test_worker_state(&ns_id));
            worker_ids.push(wid);
        }

        orch.namespaces.insert(ns_id.clone(), ns);
        (orch, ns_id, worker_ids)
    }

    fn insert_pods(pod_map: &mut PodMap, count: usize, prefix: &str, worker_id: &WorkerId) {
        for i in 0..count {
            pod_map.insert(
                PodId(format!("{}-{}", prefix, i)),
                PodInfo {
                    workload_id: WorkloadId("wl".into()),
                    worker_id: worker_id.clone(),
                },
            );
        }
    }

    #[test]
    fn test_select_worker_prefers_normal_over_elevated() {
        let (mut orch, ns_id, workers) = setup_orchestrator(2);

        // w-0: Elevated memory pressure
        orch.workers.get_mut(&workers[0]).unwrap().pressure_bands.memory = PressureBand::Elevated;
        // w-1: Normal (default)

        let selected = orch.select_worker_for_pod(&ns_id).unwrap();
        assert_eq!(selected, workers[1], "should prefer Normal over Elevated");
    }

    #[test]
    fn test_select_worker_excludes_high_pressure() {
        let (mut orch, ns_id, workers) = setup_orchestrator(2);

        // w-0: High memory pressure
        orch.workers.get_mut(&workers[0]).unwrap().pressure_bands.memory = PressureBand::High;
        // w-1: Normal

        let selected = orch.select_worker_for_pod(&ns_id).unwrap();
        assert_eq!(selected, workers[1], "should exclude High pressure worker");
    }

    #[test]
    fn test_select_worker_excludes_critical_pressure() {
        let (mut orch, ns_id, workers) = setup_orchestrator(2);

        // w-0: Critical storage pressure
        orch.workers.get_mut(&workers[0]).unwrap().pressure_bands.storage = PressureBand::Critical;
        // w-1: Normal

        let selected = orch.select_worker_for_pod(&ns_id).unwrap();
        assert_eq!(selected, workers[1], "should exclude Critical pressure worker");
    }

    #[test]
    fn test_select_worker_none_when_all_high() {
        let (mut orch, ns_id, workers) = setup_orchestrator(2);

        // Both workers at High pressure
        orch.workers.get_mut(&workers[0]).unwrap().pressure_bands.memory = PressureBand::High;
        orch.workers.get_mut(&workers[1]).unwrap().pressure_bands.compute = PressureBand::Critical;

        let selected = orch.select_worker_for_pod(&ns_id);
        assert!(selected.is_none(), "should return None when all workers are at High+ pressure");
    }

    #[test]
    fn test_select_worker_elevated_is_fallback() {
        let (mut orch, ns_id, workers) = setup_orchestrator(2);

        // w-0: High (excluded), w-1: Elevated (allowed)
        orch.workers.get_mut(&workers[0]).unwrap().pressure_bands.memory = PressureBand::High;
        orch.workers.get_mut(&workers[1]).unwrap().pressure_bands.memory = PressureBand::Elevated;

        let selected = orch.select_worker_for_pod(&ns_id).unwrap();
        assert_eq!(selected, workers[1], "Elevated worker should be selected when High is excluded");
    }

    #[test]
    fn test_select_worker_pod_count_tiebreaker() {
        let (mut orch, ns_id, workers) = setup_orchestrator(2);

        // Both Normal pressure, but w-0 has 3 pods, w-1 has 1 pod
        let ns = orch.namespaces.get_mut(&ns_id).unwrap();
        insert_pods(&mut ns.pod_map, 3, "a", &workers[0]);
        insert_pods(&mut ns.pod_map, 1, "b", &workers[1]);

        let selected = orch.select_worker_for_pod(&ns_id).unwrap();
        assert_eq!(selected, workers[1], "should prefer worker with fewer pods at same pressure");
    }

    #[test]
    fn test_select_worker_pressure_trumps_pod_count() {
        let (mut orch, ns_id, workers) = setup_orchestrator(2);

        // w-0: Normal pressure, 5 pods
        // w-1: Elevated pressure, 0 pods
        let ns = orch.namespaces.get_mut(&ns_id).unwrap();
        insert_pods(&mut ns.pod_map, 5, "a", &workers[0]);
        orch.workers.get_mut(&workers[1]).unwrap().pressure_bands.memory = PressureBand::Elevated;

        let selected = orch.select_worker_for_pod(&ns_id).unwrap();
        assert_eq!(selected, workers[0], "should prefer Normal worker even with more pods over Elevated");
    }

    #[test]
    fn test_select_worker_skips_inactive() {
        let (mut orch, ns_id, workers) = setup_orchestrator(2);

        // w-0 is not active
        let ns = orch.namespaces.get_mut(&ns_id).unwrap();
        ns.workers.get_mut(&workers[0]).unwrap().fabric_status = FabricStatus::Creating;

        let selected = orch.select_worker_for_pod(&ns_id).unwrap();
        assert_eq!(selected, workers[1], "should skip inactive worker");
    }

    #[test]
    fn test_select_worker_max_band_across_dimensions() {
        let (mut orch, ns_id, workers) = setup_orchestrator(2);

        // w-0: Normal on all dimensions
        // w-1: Normal memory but High on storage -> max_band is High -> excluded
        orch.workers.get_mut(&workers[1]).unwrap().pressure_bands.storage = PressureBand::High;

        let selected = orch.select_worker_for_pod(&ns_id).unwrap();
        assert_eq!(selected, workers[0], "High on any dimension should exclude the worker");
    }
}
