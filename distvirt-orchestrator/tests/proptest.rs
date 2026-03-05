use proptest::prelude::*;
use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::time::Duration;

use distvirt_orchestrator::namespace::NamespaceStateMachine;
use distvirt_orchestrator::orchestrator::Orchestrator;
use distvirt_orchestrator::types::*;

// --- Arbitrary Generators ---

fn arb_service_id() -> impl Strategy<Value = ServiceId> {
    prop_oneof![
        Just(ServiceId("svc1".into())),
        Just(ServiceId("svc2".into())),
        Just(ServiceId("svc3".into())),
    ]
}

fn arb_workload_id() -> impl Strategy<Value = WorkloadId> {
    prop_oneof![
        Just(WorkloadId("svc1".into())),
        Just(WorkloadId("svc2".into())),
        Just(WorkloadId("svc3".into())),
    ]
}

fn arb_worker_id() -> impl Strategy<Value = WorkerId> {
    prop_oneof![
        Just(WorkerId("worker-1".into())),
        Just(WorkerId("worker-2".into())),
    ]
}

fn arb_client_id() -> impl Strategy<Value = ClientId> {
    (1..=3u64).prop_map(ClientId)
}

fn arb_pod_id() -> impl Strategy<Value = PodId> {
    prop_oneof![
        Just(PodId("pod-1".into())),
        Just(PodId("pod-2".into())),
        Just(PodId("pod-3".into())),
    ]
}

fn arb_timer_key() -> impl Strategy<Value = TimerKey> {
    prop_oneof![
        arb_service_id().prop_map(|sid| TimerKey::IdleTimeout { service_id: sid }),
        (arb_workload_id(), arb_pod_id()).prop_map(|(wlid, pid)| TimerKey::LaunchTimeout {
            workload_id: wlid,
            pod_id: pid
        }),
    ]
}

fn arb_backend_need() -> impl Strategy<Value = BackendNeed> {
    prop_oneof![
        Just(BackendNeed::None),
        Just(BackendNeed::Traffic),
        Just(BackendNeed::Active),
    ]
}

fn arb_worker_event() -> impl Strategy<Value = WorkerEvent> {
    prop_oneof![
        Just(WorkerEvent::NamespaceCreated),
        Just(WorkerEvent::NamespaceFailed {
            error: "test failure".into(),
        }),
        Just(WorkerEvent::NamespaceDestroyed),
        arb_service_id().prop_map(|sid| WorkerEvent::ServiceActivation { service_id: sid }),
        (arb_service_id(), arb_backend_need()).prop_map(|(sid, need)| {
            WorkerEvent::ServiceBackendNeed {
                service_id: sid,
                need,
            }
        }),
        arb_pod_id().prop_map(|pid| WorkerEvent::PodRunning { pod_id: pid }),
        arb_pod_id().prop_map(|pid| WorkerEvent::PodExited {
            pod_id: pid,
            exit_code: 0,
        }),
        arb_pod_id().prop_map(|pid| WorkerEvent::PodFailed {
            pod_id: pid,
            error: "test failure".into(),
        }),
    ]
}

fn arb_namespace_spec() -> impl Strategy<Value = NamespaceSpec> {
    prop_oneof![
        Just(single_service_spec()),
        Just(multi_service_spec()),
        Just(activation_only_spec()),
    ]
}

fn arb_namespace_input() -> impl Strategy<Value = NamespaceInput> {
    prop_oneof![
        (arb_worker_id(), arb_worker_event()).prop_map(|(wid, event)| {
            NamespaceInput::WorkerEvent {
                worker_id: wid,
                event,
            }
        }),
        arb_worker_id().prop_map(|wid| NamespaceInput::WorkerLost { worker_id: wid }),
        arb_timer_key().prop_map(|tk| NamespaceInput::TimerFired { timer_key: tk }),
        arb_client_id().prop_map(|cid| NamespaceInput::Delete { client_id: cid }),
        arb_client_id().prop_map(|cid| NamespaceInput::GetStatus { client_id: cid }),
        (arb_client_id(), arb_namespace_spec()).prop_map(|(cid, spec)| {
            NamespaceInput::UpdateSpec {
                client_id: cid,
                spec,
            }
        }),
        (arb_workload_id(), arb_worker_id(), arb_pod_id()).prop_map(|(wlid, wid, pid)| {
            NamespaceInput::LaunchPod {
                workload_id: wlid,
                worker_id: wid,
                pod_id: pid,
            }
        }),
    ]
}

fn test_network_config() -> NetworkConfig {
    NetworkConfig {
        subnet: Ipv4Addr::new(172, 16, 0, 0),
        gateway: Ipv4Addr::new(172, 16, 0, 1),
        prefix_len: 24,
        segment_id: None,
    }
}

fn test_pod_network_config() -> PodNetworkConfig {
    PodNetworkConfig {
        ip: Ipv4Addr::new(172, 16, 0, 10),
        mac: [0; 6],
        gateway: Ipv4Addr::new(172, 16, 0, 1),
        netmask: "255.255.255.0".into(),
    }
}

fn test_service_policy() -> ServicePolicy {
    ServicePolicy {
        buffer_frames: 100,
        timeout_ms: 5000,
        activator: None,
    }
}

fn test_container_spec() -> ContainerSpec {
    ContainerSpec {
        container_id: "main".into(),
        image_ref: "test-image:latest".into(),
        config: ContainerConfig {
            entrypoint: vec!["/bin/sh".into()],
            args: vec![],
            env: vec![],
            working_dir: None,
            uid: None,
            gid: None,
            hostname: None,
            capture_output: false,
            stdin: false,
        },
    }
}

fn single_service_spec() -> NamespaceSpec {
    let mut workloads = HashMap::new();
    workloads.insert(
        WorkloadId("svc1".into()),
        WorkloadSpec {
            containers: vec![test_container_spec()],
            network: test_pod_network_config(),
            suspend_on_idle: false,
        },
    );
    let mut services = HashMap::new();
    services.insert(
        ServiceId("svc1".into()),
        ServiceSpec {
            workload_id: WorkloadId("svc1".into()),
            ip: Ipv4Addr::new(172, 16, 0, 100),

            policy: test_service_policy(),
            activation: None,
        },
    );
    NamespaceSpec {
        network: test_network_config(),
        workloads,
        services,
    }
}

fn multi_service_spec() -> NamespaceSpec {
    let mut workloads = HashMap::new();
    workloads.insert(
        WorkloadId("svc1".into()),
        WorkloadSpec {
            containers: vec![test_container_spec()],
            network: test_pod_network_config(),
            suspend_on_idle: false,
        },
    );
    workloads.insert(
        WorkloadId("svc2".into()),
        WorkloadSpec {
            containers: vec![test_container_spec()],
            suspend_on_idle: false,
            network: PodNetworkConfig {
                ip: Ipv4Addr::new(172, 16, 0, 11),
                mac: [0; 6],
                gateway: Ipv4Addr::new(172, 16, 0, 1),
                netmask: "255.255.255.0".into(),
            },
        },
    );
    let mut services = HashMap::new();
    services.insert(
        ServiceId("svc1".into()),
        ServiceSpec {
            workload_id: WorkloadId("svc1".into()),
            ip: Ipv4Addr::new(172, 16, 0, 100),
            policy: test_service_policy(),
            activation: None,
        },
    );
    services.insert(
        ServiceId("svc2".into()),
        ServiceSpec {
            workload_id: WorkloadId("svc2".into()),
            ip: Ipv4Addr::new(172, 16, 0, 101),
            policy: test_service_policy(),
            activation: Some(ActivationSpec {
                idle_timeout: Duration::from_secs(30),
            }),
        },
    );
    NamespaceSpec {
        network: test_network_config(),
        workloads,
        services,
    }
}

fn activation_only_spec() -> NamespaceSpec {
    let mut workloads = HashMap::new();
    workloads.insert(
        WorkloadId("svc1".into()),
        WorkloadSpec {
            containers: vec![test_container_spec()],
            network: test_pod_network_config(),
            suspend_on_idle: false,
        },
    );
    let mut services = HashMap::new();
    services.insert(
        ServiceId("svc1".into()),
        ServiceSpec {
            workload_id: WorkloadId("svc1".into()),
            ip: Ipv4Addr::new(172, 16, 0, 100),

            policy: test_service_policy(),
            activation: Some(ActivationSpec {
                idle_timeout: Duration::from_secs(30),
            }),
        },
    );
    NamespaceSpec {
        network: test_network_config(),
        workloads,
        services,
    }
}

// --- Invariant Checkers ---

fn check_namespace_invariants(ns: &NamespaceStateMachine, output: &NamespaceOutput) {
    // No commands sent to workers not in our workers map.
    for (wid, _) in &output.worker_commands {
        assert!(
            ns.workers.contains_key(wid),
            "Command sent to unknown worker {:?}",
            wid
        );
    }

    // Every pod in the pods map should reference a known workload.
    for (pid, pod_info) in ns.pod_map.iter() {
        assert!(
            ns.workloads.contains_key(&pod_info.workload_id),
            "Pod {:?} references unknown workload {:?}",
            pid,
            pod_info.workload_id
        );
    }

    // Workloads in Launching/Running must reference pods in the pods map.
    for (wl_id, wl) in &ns.workloads {
        match &wl.state {
            WorkloadState::Launching {
                pod_id, worker_id, ..
            } => {
                assert!(
                    ns.pod_map.contains(pod_id),
                    "Workload {:?} in Launching references unknown pod {:?}",
                    wl_id,
                    pod_id
                );
                assert!(
                    ns.workers.contains_key(worker_id),
                    "Workload {:?} in Launching references unknown worker {:?}",
                    wl_id,
                    worker_id
                );
            }
            WorkloadState::Running {
                pod_id, worker_id, ..
            } => {
                assert!(
                    ns.pod_map.contains(pod_id),
                    "Workload {:?} in Running references unknown pod {:?}",
                    wl_id,
                    pod_id
                );
                assert!(
                    ns.workers.contains_key(worker_id),
                    "Workload {:?} in Running references unknown worker {:?}",
                    wl_id,
                    worker_id
                );
            }
            _ => {}
        }
    }

    // Services in Active must have a workload that is Running.
    for (sid, svc) in &ns.services {
        if let ServiceState::Active { pod_id, worker_id, .. } = &svc.state {
            let wl = ns.workloads.get(&svc.workload_id);
            assert!(
                matches!(
                    wl.map(|w| &w.state),
                    Some(WorkloadState::Running { .. })
                ),
                "Service {:?} is Active but workload {:?} is not Running",
                sid,
                svc.workload_id
            );
            // Pod and worker should match the workload's.
            if let Some(wl) = wl {
                if let WorkloadState::Running { pod_id: wl_pid, worker_id: wl_wid, .. } = &wl.state {
                    assert_eq!(pod_id, wl_pid, "Service {:?} pod_id doesn't match workload", sid);
                    assert_eq!(worker_id, wl_wid, "Service {:?} worker_id doesn't match workload", sid);
                }
            }
        }
    }

    // service_workload consistency: every key exists in services, every value in workloads.
    for (svc_id, wl_id) in &ns.service_workload {
        assert!(
            ns.services.contains_key(svc_id),
            "service_workload references unknown service {:?}",
            svc_id
        );
        assert!(
            ns.workloads.contains_key(wl_id),
            "service_workload references unknown workload {:?}",
            wl_id
        );
    }

    // current_demand consistency: for each workload, current_demand should equal the
    // number of services with wants_backend() + route_miss_wake.
    for (wl_id, wl) in &ns.workloads {
        let service_demand: u32 = ns
            .service_workload
            .iter()
            .filter(|(_, w)| *w == wl_id)
            .filter(|(svc_id, _)| {
                ns.services.get(svc_id).map(|s| s.wants_backend()).unwrap_or(false)
            })
            .count() as u32;
        let route_miss: u32 = if wl.route_miss_wake { 1 } else { 0 };
        let expected_demand = service_demand + route_miss;
        assert_eq!(
            wl.current_demand, expected_demand,
            "Workload {:?} current_demand={} but expected {} (services={}, route_miss={})",
            wl_id, wl.current_demand, expected_demand, service_demand, route_miss
        );
    }

    // Workers in Destroying namespace must have fabric_status == Destroying.
    if ns.status == NamespaceStatus::Destroying {
        for (wid, ws) in &ns.workers {
            assert!(
                ws.fabric_status == FabricStatus::Destroying,
                "Worker {:?} in Destroying namespace has fabric_status {:?}, expected Destroying",
                wid,
                ws.fabric_status
            );
        }
    }

    // Services in Destroying namespace never have new pods launched.
    if ns.status == NamespaceStatus::Destroying {
        assert!(
            output.pod_requests.is_empty(),
            "Destroying namespace emitted pod requests"
        );
        // No LaunchPod commands should be emitted
        for (_, cmd) in &output.worker_commands {
            assert!(
                !matches!(cmd, WorkerCommand::LaunchPod { .. }),
                "Destroying namespace emitted LaunchPod command"
            );
        }
    }
}

fn check_orchestrator_invariants(orch: &Orchestrator, output: &OrchestratorOutput) {
    // No worker commands sent to unknown workers.
    for (wid, _) in &output.worker_commands {
        assert!(
            orch.workers.contains_key(wid),
            "Command sent to unknown worker {:?}",
            wid
        );
    }
}

// --- Proptest Harnesses ---

// --- PodMap proptest ---

use distvirt_orchestrator::pod_map::PodMap;

#[derive(Debug, Clone)]
enum PodMapOp {
    Insert(PodId, WorkerId),
    Remove(PodId),
    RemoveWorker(WorkerId),
    Clear,
}

fn arb_pod_map_op() -> impl Strategy<Value = PodMapOp> {
    prop_oneof![
        (arb_pod_id(), arb_worker_id()).prop_map(|(p, w)| PodMapOp::Insert(p, w)),
        arb_pod_id().prop_map(PodMapOp::Remove),
        arb_worker_id().prop_map(PodMapOp::RemoveWorker),
        Just(PodMapOp::Clear),
    ]
}

fn check_pod_map_consistency(map: &PodMap, shadow: &HashMap<PodId, (WorkerId, WorkloadId)>) {
    // Length matches.
    assert_eq!(map.len(), shadow.len(), "PodMap len mismatch");

    // Every shadow entry is in the map with correct worker.
    for (pid, (wid, wlid)) in shadow {
        let info = map.get(pid).unwrap_or_else(|| panic!("pod {:?} missing from PodMap", pid));
        assert_eq!(&info.worker_id, wid);
        assert_eq!(&info.workload_id, wlid);
    }

    // Worker counts match.
    let mut counts: HashMap<WorkerId, usize> = HashMap::new();
    for (_pid, (wid, _)) in shadow {
        *counts.entry(wid.clone()).or_default() += 1;
    }
    for (wid, expected) in &counts {
        assert_eq!(
            map.worker_pod_count(wid),
            *expected,
            "worker_pod_count mismatch for {:?}",
            wid
        );
    }
}

proptest! {
    #[test]
    fn pod_map_shadow_consistency(ops in prop::collection::vec(arb_pod_map_op(), 0..200)) {
        let mut map = PodMap::new();
        let mut shadow: HashMap<PodId, (WorkerId, WorkloadId)> = HashMap::new();
        let dummy_wl = WorkloadId("wl".into());

        for op in ops {
            match op {
                PodMapOp::Insert(pid, wid) => {
                    if shadow.contains_key(&pid) {
                        // Skip duplicate — would panic in debug mode.
                        continue;
                    }
                    shadow.insert(pid.clone(), (wid.clone(), dummy_wl.clone()));
                    map.insert(pid, PodInfo {
                        worker_id: wid,
                        workload_id: dummy_wl.clone(),
                    });
                }
                PodMapOp::Remove(pid) => {
                    shadow.remove(&pid);
                    map.remove(&pid);
                }
                PodMapOp::RemoveWorker(wid) => {
                    shadow.retain(|_, (w, _)| *w != wid);
                    map.remove_worker_pods(&wid);
                }
                PodMapOp::Clear => {
                    shadow.clear();
                    map.clear();
                }
            }
            check_pod_map_consistency(&map, &shadow);
        }
    }

    #[test]
    fn namespace_invariants_hold(inputs in prop::collection::vec(arb_namespace_input(), 0..100)) {
        let mut ns = NamespaceStateMachine::new(NamespaceId("prop-ns".into()), single_service_spec(), 1);
        let mut pt = PlacementTable::default();
        for input in inputs {
            let output = ns.step(input, &mut pt);
            check_namespace_invariants(&ns, &output);
        }
    }

    #[test]
    fn namespace_invariants_hold_multi_service(inputs in prop::collection::vec(arb_namespace_input(), 0..100)) {
        let mut ns = NamespaceStateMachine::new(NamespaceId("prop-ns".into()), multi_service_spec(), 1);
        let mut pt = PlacementTable::default();
        for input in inputs {
            let output = ns.step(input, &mut pt);
            check_namespace_invariants(&ns, &output);
        }
    }

    #[test]
    fn orchestrator_no_panic(
        num_steps in 1..50usize,
        seed in any::<u64>(),
    ) {
        // Simple fuzz: create an orchestrator, do random operations, never panic.
        let mut orch = Orchestrator::new();
        let mut rng_state = seed;

        for _ in 0..num_steps {
            // Simple PRNG for deterministic test.
            rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let choice = (rng_state >> 32) % 7;

            let input = match choice {
                0 => OrchestratorInput::ClientConnected {
                    client_id: ClientId(rng_state % 3),
                },
                1 => OrchestratorInput::ClientDisconnected {
                    client_id: ClientId(rng_state % 3),
                },
                2 => OrchestratorInput::WorkerConnected {
                    worker_id: WorkerId(format!("w-{}", rng_state % 3)),
                    capabilities: WorkerCapabilities {
                        max_pods: 10,
                        available_memory_mb: 1024,
                        public_endpoint: String::new(),
                        pools: vec![],
                    },
                    wg_config: None,
                    tunnel_config: None,
                },
                3 => OrchestratorInput::WorkerDisconnected {
                    worker_id: WorkerId(format!("w-{}", rng_state % 3)),
                },
                4 => OrchestratorInput::ClientCommand {
                    client_id: ClientId(rng_state % 3),
                    command: ClientCommand::CreateNamespace {
                        namespace_id: NamespaceId(format!("ns-{}", rng_state % 5)),
                        spec: single_service_spec(),
                    },
                },
                5 => OrchestratorInput::ClientCommand {
                    client_id: ClientId(rng_state % 3),
                    command: ClientCommand::DeleteNamespace {
                        namespace_id: NamespaceId(format!("ns-{}", rng_state % 5)),
                    },
                },
                _ => {
                    // NamespaceFailed event routed to a namespace.
                    OrchestratorInput::NamespaceInput {
                        namespace_id: NamespaceId(format!("ns-{}", rng_state % 5)),
                        input: NamespaceInput::WorkerEvent {
                            worker_id: WorkerId(format!("w-{}", rng_state % 3)),
                            event: WorkerEvent::NamespaceFailed {
                                error: "test".into(),
                            },
                        },
                    }
                }
            };

            let output = orch.step(input);
            check_orchestrator_invariants(&orch, &output);
        }
    }
}
