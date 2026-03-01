use proptest::prelude::*;
use std::collections::HashMap;
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
        (arb_service_id(), arb_pod_id()).prop_map(|(sid, pid)| TimerKey::LaunchTimeout {
            service_id: sid,
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
        Just(WorkerEvent::NamespaceDestroyed),
        arb_service_id().prop_map(|sid| WorkerEvent::ServiceCreated { service_id: sid }),
        arb_service_id().prop_map(|sid| WorkerEvent::ServiceActivation { service_id: sid }),
        (arb_service_id(), arb_backend_need()).prop_map(|(sid, need)| {
            WorkerEvent::ServiceBackendNeed {
                service_id: sid,
                need,
            }
        }),
        arb_pod_id().prop_map(|pid| WorkerEvent::PodRunning { pod_id: pid }),
        arb_pod_id().prop_map(|pid| WorkerEvent::PodExited { pod_id: pid }),
        arb_pod_id().prop_map(|pid| WorkerEvent::PodFailed {
            pod_id: pid,
            reason: "test failure".into(),
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
        (arb_service_id(), arb_worker_id(), arb_pod_id()).prop_map(|(sid, wid, pid)| {
            NamespaceInput::LaunchPod {
                service_id: sid,
                worker_id: wid,
                pod_id: pid,
            }
        }),
    ]
}

fn single_service_spec() -> NamespaceSpec {
    let mut services = HashMap::new();
    services.insert(
        ServiceId("svc1".into()),
        ServiceSpec {
            image: "test-image:latest".into(),
            activation: None,
        },
    );
    NamespaceSpec { services }
}

fn multi_service_spec() -> NamespaceSpec {
    let mut services = HashMap::new();
    services.insert(
        ServiceId("svc1".into()),
        ServiceSpec {
            image: "test-image:latest".into(),
            activation: None,
        },
    );
    services.insert(
        ServiceId("svc2".into()),
        ServiceSpec {
            image: "test-image:latest".into(),
            activation: Some(ActivationSpec {
                idle_timeout: Duration::from_secs(30),
            }),
        },
    );
    NamespaceSpec { services }
}

fn activation_only_spec() -> NamespaceSpec {
    let mut services = HashMap::new();
    services.insert(
        ServiceId("svc1".into()),
        ServiceSpec {
            image: "test-image:latest".into(),
            activation: Some(ActivationSpec {
                idle_timeout: Duration::from_secs(30),
            }),
        },
    );
    NamespaceSpec { services }
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

    // Every pod in the pods map should reference a known service.
    for (pid, pod_info) in &ns.pods {
        assert!(
            ns.spec.services.contains_key(&pod_info.service_id)
                || ns.services.contains_key(&pod_info.service_id),
            "Pod {:?} references unknown service {:?}",
            pid,
            pod_info.service_id
        );
    }

    // Services in Launching/Active must reference pods in the pods map.
    for (sid, svc) in &ns.services {
        match svc {
            ServiceState::Launching {
                pod_id, worker_id, ..
            } => {
                assert!(
                    ns.pods.contains_key(pod_id),
                    "Service {:?} in Launching references unknown pod {:?}",
                    sid,
                    pod_id
                );
                assert!(
                    ns.workers.contains_key(worker_id),
                    "Service {:?} in Launching references unknown worker {:?}",
                    sid,
                    worker_id
                );
            }
            ServiceState::Active {
                pod_id, worker_id, ..
            } => {
                assert!(
                    ns.pods.contains_key(pod_id),
                    "Service {:?} in Active references unknown pod {:?}",
                    sid,
                    pod_id
                );
                assert!(
                    ns.workers.contains_key(worker_id),
                    "Service {:?} in Active references unknown worker {:?}",
                    sid,
                    worker_id
                );
            }
            _ => {}
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

proptest! {
    #[test]
    fn namespace_invariants_hold(inputs in prop::collection::vec(arb_namespace_input(), 0..100)) {
        let mut ns = NamespaceStateMachine::new(NamespaceId("prop-ns".into()), single_service_spec());
        for input in inputs {
            let output = ns.step(input);
            check_namespace_invariants(&ns, &output);
        }
    }

    #[test]
    fn namespace_invariants_hold_multi_service(inputs in prop::collection::vec(arb_namespace_input(), 0..100)) {
        let mut ns = NamespaceStateMachine::new(NamespaceId("prop-ns".into()), multi_service_spec());
        for input in inputs {
            let output = ns.step(input);
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
                    },
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
                    // NamespaceDestroyed event routed to a namespace.
                    OrchestratorInput::NamespaceInput {
                        namespace_id: NamespaceId(format!("ns-{}", rng_state % 5)),
                        input: NamespaceInput::WorkerEvent {
                            worker_id: WorkerId(format!("w-{}", rng_state % 3)),
                            event: WorkerEvent::NamespaceDestroyed,
                        },
                    }
                }
            };

            let output = orch.step(input);
            check_orchestrator_invariants(&orch, &output);
        }
    }
}
