use std::collections::BTreeMap;

use crate::namespace::NamespaceStateMachine;
use crate::orchestrator::Orchestrator;
use crate::types::*;

use super::helpers::*;

// --- Namespace State Machine Tests ---

#[test]
fn test_namespace_new_initializes_services() {
    let spec = test_spec();
    let ns = NamespaceStateMachine::new(ns_id("test"), spec, 1);

    assert_eq!(ns.status, NamespaceStatus::Creating);
    assert_eq!(ns.services.len(), 1);
    assert!(ns.services.contains_key(&svc_id()));
    assert!(matches!(get_service_state(&ns), ServiceState::Pending));
    assert!(matches!(get_workload_state(&ns), WorkloadState::Dormant));
    assert!(ns.pod_map.is_empty());
    assert!(ns.workers.is_empty());
}

#[test]
fn test_namespace_update_spec() {
    let mut ns = NamespaceStateMachine::new(ns_id("test"), test_spec(), 1);
    let mut pt = PlacementTable::default();
    let new_spec = test_spec_with_activation();
    let out = ns.step(NamespaceInput::UpdateSpec {
        client_id: client_id(1),
        spec: new_spec.clone(),
    }, &mut pt);

    assert_eq!(ns.spec, new_spec);
    assert!(out
        .client_events
        .iter()
        .any(|(_, ev)| *ev == ClientEvent::Ok));
}

#[test]
fn test_namespace_delete() {
    let mut ns = NamespaceStateMachine::new(ns_id("test"), test_spec(), 1);
    let mut pt = PlacementTable::default();
    let out = ns.step(NamespaceInput::Delete {
        client_id: client_id(1),
    }, &mut pt);

    assert_eq!(ns.status, NamespaceStatus::Destroying);
    assert!(out.destroyed);
    assert!(out
        .client_events
        .iter()
        .any(|(_, ev)| *ev == ClientEvent::Ok));
}

#[test]
fn test_namespace_worker_lost_removes_worker() {
    let mut ns = NamespaceStateMachine::new(ns_id("test"), test_spec(), 1);
    let mut pt = PlacementTable::default();
    ns.workers.insert(
        worker_id(1),
        NamespaceWorkerState {
            fabric_status: FabricStatus::Active,
            primary_pool_id: None,
            pressure_band: PressureBand::Normal,
        },
    );

    ns.step(NamespaceInput::WorkerLost {
        worker_id: worker_id(1),
    }, &mut pt);

    assert!(!ns.workers.contains_key(&worker_id(1)));
}

#[test]
fn test_namespace_get_status() {
    let ns_name = ns_id("test");
    let mut ns = NamespaceStateMachine::new(ns_name.clone(), test_spec(), 1);
    let mut pt = PlacementTable::default();
    let out = ns.step(NamespaceInput::GetStatus {
        client_id: client_id(1),
    }, &mut pt);

    assert!(out.client_events.iter().any(|(cid, ev)| {
        *cid == client_id(1)
            && matches!(ev, ClientEvent::NamespaceStatus { namespace_id, status }
                if *namespace_id == ns_name && status.status == NamespaceStatus::Creating)
    }));
}

// --- ListNamespaces Tests ---

#[test]
fn test_list_namespaces_returns_all() {
    let mut orch = Orchestrator::new();

    // Create two namespaces.
    orch.step(OrchestratorInput::ClientCommand {
        client_id: client_id(1),
        command: ClientCommand::CreateNamespace {
            namespace_id: ns_id("ns1"),
            spec: test_spec(),
        },
    });
    orch.step(OrchestratorInput::ClientCommand {
        client_id: client_id(1),
        command: ClientCommand::CreateNamespace {
            namespace_id: ns_id("ns2"),
            spec: test_spec_with_activation(),
        },
    });

    let out = orch.step(OrchestratorInput::ClientCommand {
        client_id: client_id(1),
        command: ClientCommand::ListNamespaces,
    });

    let list = out
        .client_events
        .iter()
        .find_map(|(_, ev)| match ev {
            ClientEvent::NamespaceList { namespaces } => Some(namespaces),
            _ => None,
        })
        .expect("should have NamespaceList event");

    assert_eq!(list.len(), 2);
    let ids: std::collections::HashSet<_> = list.iter().map(|r| &r.namespace_id).collect();
    assert!(ids.contains(&ns_id("ns1")));
    assert!(ids.contains(&ns_id("ns2")));
}

#[test]
fn test_list_namespaces_empty() {
    let mut orch = Orchestrator::new();
    let out = orch.step(OrchestratorInput::ClientCommand {
        client_id: client_id(1),
        command: ClientCommand::ListNamespaces,
    });

    let list = out
        .client_events
        .iter()
        .find_map(|(_, ev)| match ev {
            ClientEvent::NamespaceList { namespaces } => Some(namespaces),
            _ => None,
        })
        .expect("should have NamespaceList event");

    assert!(list.is_empty());
}

// --- UpdateSpec Service Removal Tests ---

#[test]
fn test_update_spec_removes_service() {
    let mut ns = active_namespace(test_spec());
    let mut pt = PlacementTable::default();
    let mut pod_counter = 0u64;

    // Reconcile to get the service launched.
    reconcile_active_namespace(&mut ns, &mut pod_counter);
    let pod_id = get_launching_pod_id(&ns);
    ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::PodRunning { pod_id: pod_id.clone() },
    }, &mut pt);
    assert!(matches!(get_workload_state(&ns), WorkloadState::Running { .. }));

    // Update spec with no services/workloads — svc1 should be removed.
    let empty_spec = NamespaceSpec {
        network: test_network_config(),
        workloads: BTreeMap::new(),
        services: BTreeMap::new(),
    };
    let out = ns.step(NamespaceInput::UpdateSpec {
        client_id: client_id(1),
        spec: empty_spec,
    }, &mut pt);

    assert!(ns.services.is_empty());
    assert!(ns.workloads.is_empty());
    assert!(ns.pod_map.is_empty());
    assert!(out.worker_commands.iter().any(|(_, cmd)| matches!(
        cmd,
        WorkerCommand::StopPod { .. }
    )));
}

#[test]
fn test_update_spec_removes_launching_service() {
    let mut ns = active_namespace(test_spec());
    let mut pt = PlacementTable::default();
    let mut pod_counter = 0u64;

    // Reconcile to get the service launching.
    reconcile_active_namespace(&mut ns, &mut pod_counter);
    assert!(matches!(get_workload_state(&ns), WorkloadState::Launching { .. }));

    // Update spec with no services — should stop the launching pod and cancel the launch timer.
    let empty_spec = NamespaceSpec {
        network: test_network_config(),
        workloads: BTreeMap::new(),
        services: BTreeMap::new(),
    };
    let out = ns.step(NamespaceInput::UpdateSpec {
        client_id: client_id(1),
        spec: empty_spec,
    }, &mut pt);

    assert!(ns.services.is_empty());
    assert!(ns.workloads.is_empty());
    assert!(ns.pod_map.is_empty());
    assert!(out.worker_commands.iter().any(|(_, cmd)| matches!(
        cmd,
        WorkerCommand::StopPod { .. }
    )));
    assert!(!out.timers_cancel.is_empty());
}

// --- Lifecycle Tests ---

#[test]
fn test_namespace_created_activates_namespace() {
    let mut ns = NamespaceStateMachine::new(ns_id("test"), test_spec(), 1);
    ns.workers.insert(
        worker_id(1),
        NamespaceWorkerState {
            fabric_status: FabricStatus::Creating,
            primary_pool_id: None,
            pressure_band: PressureBand::Normal,
        },
    );
    let mut pod_counter = 0u64;

    let out = step_with_scheduling(
        &mut ns,
        NamespaceInput::WorkerEvent {
            worker_id: worker_id(1),
            event: WorkerEvent::NamespaceCreated,
        },
        &mut pod_counter,
    );

    assert_eq!(ns.status, NamespaceStatus::Active);
    assert_eq!(
        ns.workers[&worker_id(1)].fabric_status,
        FabricStatus::Active
    );

    // Always-on service should have been launched (CreateService + LaunchPod via scheduling).
    assert!(out.worker_commands.iter().any(|(_, cmd)| matches!(
        cmd,
        WorkerCommand::CreateService { .. }
    )));
    assert!(out
        .worker_commands
        .iter()
        .any(|(_, cmd)| matches!(cmd, WorkerCommand::LaunchPod { .. })));
}

#[test]
fn test_activation_service_lifecycle() {
    let mut ns = active_namespace(test_spec_with_activation());
    let mut pod_counter = 0u64;

    // Reconcile: activation service goes Pending → Idle with CreateService on workers.
    let out = reconcile_active_namespace(&mut ns, &mut pod_counter);
    assert!(matches!(get_service_state(&ns), ServiceState::Idle));
    assert!(out.worker_commands.iter().any(|(_, cmd)| matches!(
        cmd,
        WorkerCommand::CreateService { .. }
    )));

    // ServiceActivation → should request pod scheduling then launch.
    let out = step_with_scheduling(
        &mut ns,
        NamespaceInput::WorkerEvent {
            worker_id: worker_id(1),
            event: WorkerEvent::ServiceActivation {
                service_id: svc_id(),
            },
        },
        &mut pod_counter,
    );
    assert!(matches!(
        get_workload_state(&ns),
        WorkloadState::Launching { .. }
    ));
    assert!(out
        .worker_commands
        .iter()
        .any(|(_, cmd)| matches!(cmd, WorkerCommand::LaunchPod { .. })));

    // Get the pod_id from the Launching state.
    let pod_id = get_launching_pod_id(&ns);

    // PodRunning → Active.
    let mut pt = PlacementTable::default();
    let out = ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::PodRunning {
            pod_id: pod_id.clone(),
        },
    }, &mut pt);
    assert!(matches!(
        get_workload_state(&ns),
        WorkloadState::Running { .. }
    ));
    assert!(matches!(
        get_service_state(&ns),
        ServiceState::Active { .. }
    ));
    assert!(out.worker_commands.iter().any(|(_, cmd)| matches!(
        cmd,
        WorkerCommand::UpdateServiceBackend {
            backend: Some(_),
            ..
        }
    )));
    assert!(out.worker_commands.iter().any(|(_, cmd)| matches!(
        cmd,
        WorkerCommand::ServiceReady { .. }
    )));

    // BackendNeed::None → idle timer set.
    let out = ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::ServiceBackendNeed {
            service_id: svc_id(),
            need: BackendNeed::None,
        },
    }, &mut pt);
    assert!(!out.timers_set.is_empty());
    let idle_timer = out.timers_set[0].0.clone();

    // Idle timer fires → service back to Idle, workload goes Dormant.
    let out = ns.step(NamespaceInput::TimerFired {
        timer_key: idle_timer,
    }, &mut pt);
    assert!(matches!(get_service_state(&ns), ServiceState::Idle));
    assert!(matches!(get_workload_state(&ns), WorkloadState::Dormant));
    // The idle timeout causes DemandDown which stops the pod via the workload SM.
    assert!(out
        .worker_commands
        .iter()
        .any(|(_, cmd)| matches!(cmd, WorkerCommand::StopPod { .. })
            || matches!(cmd, WorkerCommand::UpdateServiceBackend { backend: None, .. })));
}

#[test]
fn test_always_on_service_lifecycle() {
    let mut ns = active_namespace(test_spec());
    let mut pt = PlacementTable::default();
    let mut pod_counter = 0u64;

    // Reconcile: always-on service goes Pending → NeedBackend, workload goes WaitingForCapacity → Launching.
    let _out = reconcile_active_namespace(&mut ns, &mut pod_counter);
    assert!(matches!(
        get_workload_state(&ns),
        WorkloadState::Launching { .. }
    ));

    let pod_id = get_launching_pod_id(&ns);

    // PodRunning → Running.
    let _out = ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::PodRunning {
            pod_id: pod_id.clone(),
        },
    }, &mut pt);
    assert!(matches!(
        get_workload_state(&ns),
        WorkloadState::Running { .. }
    ));
    assert!(matches!(
        get_service_state(&ns),
        ServiceState::Active { .. }
    ));

    // PodExited → should re-launch (always-on) via pod request.
    let _out = step_with_scheduling(
        &mut ns,
        NamespaceInput::WorkerEvent {
            worker_id: worker_id(1),
            event: WorkerEvent::PodExited {
                pod_id: pod_id.clone(),
                exit_code: 0,
            },
        },
        &mut pod_counter,
    );
    assert!(matches!(
        get_workload_state(&ns),
        WorkloadState::Launching { .. }
    ));
}

#[test]
fn test_worker_loss_during_active_service() {
    let mut ns = active_namespace(test_spec_with_activation());
    let mut pt = PlacementTable::default();
    let mut pod_counter = 0u64;

    // Get to Active state.
    reconcile_active_namespace(&mut ns, &mut pod_counter);
    step_with_scheduling(
        &mut ns,
        NamespaceInput::WorkerEvent {
            worker_id: worker_id(1),
            event: WorkerEvent::ServiceActivation {
                service_id: svc_id(),
            },
        },
        &mut pod_counter,
    );
    let pod_id = get_launching_pod_id(&ns);
    ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::PodRunning { pod_id },
    }, &mut pt);
    assert!(matches!(
        get_workload_state(&ns),
        WorkloadState::Running { .. }
    ));

    // Worker lost → workload enters WaitingForCapacity (demand preserved),
    // service stays NeedBackend (re-activated to wait for recovery).
    // Namespace goes to Creating (no workers left).
    let _out = ns.step(NamespaceInput::WorkerLost {
        worker_id: worker_id(1),
    }, &mut pt);
    assert!(matches!(get_service_state(&ns), ServiceState::NeedBackend));
    assert!(matches!(get_workload_state(&ns), WorkloadState::WaitingForCapacity));
    assert_eq!(ns.status, NamespaceStatus::Creating);
    assert!(ns.workers.is_empty());
    assert!(ns.pod_map.is_empty());
}

#[test]
fn test_launch_timeout() {
    let mut ns = active_namespace(test_spec_with_activation());
    let mut pt = PlacementTable::default();
    let mut pod_counter = 0u64;

    // Get to Launching state.
    reconcile_active_namespace(&mut ns, &mut pod_counter);
    step_with_scheduling(
        &mut ns,
        NamespaceInput::WorkerEvent {
            worker_id: worker_id(1),
            event: WorkerEvent::ServiceActivation {
                service_id: svc_id(),
            },
        },
        &mut pod_counter,
    );
    let (pod_id, launch_timeout) = match get_workload_state(&ns) {
        WorkloadState::Launching {
            pod_id,
            launch_timeout,
            ..
        } => (pod_id.clone(), launch_timeout.clone()),
        other => panic!("expected Launching, got {:?}", other),
    };

    // Launch timeout fires → workload enters WaitingForCapacity (demand preserved),
    // service stays NeedBackend (re-activated to wait for recovery).
    let out = ns.step(NamespaceInput::TimerFired {
        timer_key: launch_timeout,
    }, &mut pt);
    assert!(matches!(get_service_state(&ns), ServiceState::NeedBackend));
    assert!(matches!(get_workload_state(&ns), WorkloadState::WaitingForCapacity));
    assert!(!ns.pod_map.contains(&pod_id));
    assert!(out
        .worker_commands
        .iter()
        .any(|(_, cmd)| matches!(cmd, WorkerCommand::StopPod { .. })));
}

#[test]
fn test_delete_single_worker_stateful() {
    let mut ns = active_namespace(test_spec());
    let mut pt = PlacementTable::default();
    let mut pod_counter = 0u64;

    // Get to Active with a running pod.
    reconcile_active_namespace(&mut ns, &mut pod_counter);
    let pod_id = get_launching_pod_id(&ns);
    ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::PodRunning { pod_id },
    }, &mut pt);

    // Delete — stateful: workers stay in map with Destroying status.
    let out = ns.step(NamespaceInput::Delete {
        client_id: client_id(1),
    }, &mut pt);
    assert_eq!(ns.status, NamespaceStatus::Destroying);
    assert!(!out.destroyed); // Not destroyed yet, waiting for worker confirmation.
    assert!(!ns.workers.is_empty());
    assert!(ns.pod_map.is_empty()); // Pods cleared.
    assert!(ns.workers[&worker_id(1)].fabric_status == FabricStatus::Destroying);
    assert!(out
        .worker_commands
        .iter()
        .any(|(_, cmd)| matches!(cmd, WorkerCommand::DestroyNamespace { .. })));

    // Worker confirms destruction.
    let out = ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::NamespaceDestroyed,
    }, &mut pt);
    assert!(out.destroyed);
    assert!(ns.workers.is_empty());
}

#[test]
fn test_delete_multi_worker_stateful() {
    let mut ns = active_namespace(test_spec());
    let mut pt = PlacementTable::default();
    // Add a second worker.
    ns.workers.insert(
        worker_id(2),
        NamespaceWorkerState {
            fabric_status: FabricStatus::Active,
            primary_pool_id: None,
            pressure_band: PressureBand::Normal,
        },
    );

    // Delete.
    let out = ns.step(NamespaceInput::Delete {
        client_id: client_id(1),
    }, &mut pt);
    assert_eq!(ns.status, NamespaceStatus::Destroying);
    assert!(!out.destroyed);
    assert_eq!(ns.workers.len(), 2);

    // First worker confirms.
    let out = ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::NamespaceDestroyed,
    }, &mut pt);
    assert!(!out.destroyed); // Still waiting for worker-2.
    assert_eq!(ns.workers.len(), 1);

    // Second worker confirms.
    let out = ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(2),
        event: WorkerEvent::NamespaceDestroyed,
    }, &mut pt);
    assert!(out.destroyed);
    assert!(ns.workers.is_empty());
}

#[test]
fn test_delete_worker_lost_during_teardown() {
    let mut ns = active_namespace(test_spec());
    let mut pt = PlacementTable::default();
    let mut pod_counter = 0u64;

    reconcile_active_namespace(&mut ns, &mut pod_counter);
    let pod_id = get_launching_pod_id(&ns);
    ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::PodRunning { pod_id },
    }, &mut pt);

    // Delete.
    ns.step(NamespaceInput::Delete {
        client_id: client_id(1),
    }, &mut pt);
    assert_eq!(ns.status, NamespaceStatus::Destroying);

    // Worker disconnects instead of confirming.
    let out = ns.step(NamespaceInput::WorkerLost {
        worker_id: worker_id(1),
    }, &mut pt);
    assert!(out.destroyed);
    assert!(ns.workers.is_empty());
}

#[test]
fn test_delete_no_workers_immediate() {
    // Namespace with no workers -> immediate destroy.
    let mut ns = NamespaceStateMachine::new(ns_id("test"), test_spec(), 1);
    let mut pt = PlacementTable::default();
    ns.status = NamespaceStatus::Active;

    let out = ns.step(NamespaceInput::Delete {
        client_id: client_id(1),
    }, &mut pt);
    assert_eq!(ns.status, NamespaceStatus::Destroying);
    assert!(out.destroyed);
    assert!(ns.workers.is_empty());
}

#[test]
fn test_idle_timer_cancelled_on_traffic() {
    let mut ns = active_namespace(test_spec_with_activation());
    let mut pt = PlacementTable::default();
    let mut pod_counter = 0u64;

    // Get to Active.
    reconcile_active_namespace(&mut ns, &mut pod_counter);
    step_with_scheduling(
        &mut ns,
        NamespaceInput::WorkerEvent {
            worker_id: worker_id(1),
            event: WorkerEvent::ServiceActivation {
                service_id: svc_id(),
            },
        },
        &mut pod_counter,
    );
    let pod_id = get_launching_pod_id(&ns);
    ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::PodRunning { pod_id },
    }, &mut pt);

    // BackendNeed::None → idle timer set.
    let out = ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::ServiceBackendNeed {
            service_id: svc_id(),
            need: BackendNeed::None,
        },
    }, &mut pt);
    assert!(!out.timers_set.is_empty());

    // BackendNeed::Traffic → idle timer cancelled.
    let out = ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::ServiceBackendNeed {
            service_id: svc_id(),
            need: BackendNeed::Traffic,
        },
    }, &mut pt);
    assert!(!out.timers_cancel.is_empty());
}

// --- Destroy Lifecycle Tests ---

#[test]
fn test_destroying_namespace_ignores_activation() {
    let mut ns = active_namespace(test_spec_with_activation());
    let mut pt = PlacementTable::default();
    let mut pod_counter = 0u64;

    // Reconcile to get service to Idle.
    reconcile_active_namespace(&mut ns, &mut pod_counter);
    assert!(matches!(get_service_state(&ns), ServiceState::Idle));

    // Delete namespace.
    ns.step(NamespaceInput::Delete {
        client_id: client_id(1),
    }, &mut pt);
    assert_eq!(ns.status, NamespaceStatus::Destroying);
    // Workers stay in map with Destroying status.
    assert!(!ns.workers.is_empty());
    assert!(ns.workers[&worker_id(1)].fabric_status == FabricStatus::Destroying);

    // Activation events during Destroying are ignored.
    let out = ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::ServiceActivation {
            service_id: svc_id(),
        },
    }, &mut pt);
    assert!(out.pod_requests.is_empty());
    assert!(out.worker_commands.is_empty());
}

#[test]
fn test_update_spec_rejected_during_teardown() {
    let mut ns = active_namespace(test_spec());
    let mut pt = PlacementTable::default();
    ns.step(NamespaceInput::Delete {
        client_id: client_id(1),
    }, &mut pt);
    assert_eq!(ns.status, NamespaceStatus::Destroying);

    let out = ns.step(NamespaceInput::UpdateSpec {
        client_id: client_id(2),
        spec: test_spec(),
    }, &mut pt);
    assert!(out.client_events.iter().any(|(_, ev)| matches!(
        ev,
        ClientEvent::Error { .. }
    )));
}

#[test]
fn test_stateful_destroy_with_running_pod() {
    let mut ns = active_namespace(test_spec());
    let mut pt = PlacementTable::default();
    let mut pod_counter = 0u64;

    // Get service to Active.
    reconcile_active_namespace(&mut ns, &mut pod_counter);
    let pod_id = get_launching_pod_id(&ns);
    ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::PodRunning {
            pod_id: pod_id.clone(),
        },
    }, &mut pt);
    assert!(matches!(get_workload_state(&ns), WorkloadState::Running { .. }));

    // Delete namespace — stateful: not immediately destroyed.
    let out = ns.step(NamespaceInput::Delete {
        client_id: client_id(1),
    }, &mut pt);
    assert_eq!(ns.status, NamespaceStatus::Destroying);
    assert!(!out.destroyed);
    assert!(!ns.workers.is_empty());
    assert!(ns.pod_map.is_empty()); // Pods cleared, workloads reset.
    assert!(matches!(get_workload_state(&ns), WorkloadState::Dormant));

    // Worker confirms.
    let out = ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::NamespaceDestroyed,
    }, &mut pt);
    assert!(out.destroyed);
    assert!(ns.workers.is_empty());
}

#[test]
fn test_stateful_destroy_from_orchestrator() {
    let mut orch = Orchestrator::new();

    // Connect worker, create namespace, activate it.
    orch.step(OrchestratorInput::WorkerConnected {
        worker_id: worker_id(1),
        capabilities: worker_caps(),
        wg_config: None,
        tunnel_config: None,
    });
    orch.step(OrchestratorInput::ClientCommand {
        client_id: client_id(1),
        command: ClientCommand::CreateNamespace {
            namespace_id: ns_id("ns1"),
            spec: test_spec(),
        },
    });

    // NamespaceCreated — namespace becomes Active.
    orch.step(OrchestratorInput::NamespaceInput {
        namespace_id: ns_id("ns1"),
        input: NamespaceInput::WorkerEvent {
            worker_id: worker_id(1),
            event: WorkerEvent::NamespaceCreated,
        },
    });
    assert_eq!(
        orch.namespaces.get(&ns_id("ns1")).unwrap().status,
        NamespaceStatus::Active
    );

    // Delete namespace — stateful: still in map, waiting for worker confirmation.
    orch.step(OrchestratorInput::ClientCommand {
        client_id: client_id(1),
        command: ClientCommand::DeleteNamespace {
            namespace_id: ns_id("ns1"),
        },
    });
    assert!(orch.namespaces.contains_key(&ns_id("ns1")));
    assert_eq!(
        orch.namespaces.get(&ns_id("ns1")).unwrap().status,
        NamespaceStatus::Destroying
    );

    // Worker confirms destruction.
    orch.step(OrchestratorInput::NamespaceInput {
        namespace_id: ns_id("ns1"),
        input: NamespaceInput::WorkerEvent {
            worker_id: worker_id(1),
            event: WorkerEvent::NamespaceDestroyed,
        },
    });
    assert!(!orch.namespaces.contains_key(&ns_id("ns1")));
    // Worker should no longer reference the namespace.
    assert!(!orch.workers[&worker_id(1)]
        .namespaces
        .contains(&ns_id("ns1")));
}

// --- NamespaceFailed Tests ---

#[test]
fn test_namespace_failed_treats_like_worker_loss() {
    let mut ns = active_namespace(test_spec_with_activation());
    let mut pt = PlacementTable::default();
    let mut pod_counter = 0u64;

    // Get to Active state with a running pod.
    reconcile_active_namespace(&mut ns, &mut pod_counter);
    step_with_scheduling(
        &mut ns,
        NamespaceInput::WorkerEvent {
            worker_id: worker_id(1),
            event: WorkerEvent::ServiceActivation {
                service_id: svc_id(),
            },
        },
        &mut pod_counter,
    );
    let pod_id = get_launching_pod_id(&ns);
    ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::PodRunning { pod_id },
    }, &mut pt);
    assert!(matches!(
        get_workload_state(&ns),
        WorkloadState::Running { .. }
    ));

    // NamespaceFailed should act like worker loss: workload enters WaitingForCapacity
    // (demand preserved), service stays NeedBackend.
    ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::NamespaceFailed {
            error: "gateway crashed".into(),
        },
    }, &mut pt);
    assert!(ns.workers.is_empty());
    assert!(ns.pod_map.is_empty());
    assert!(matches!(get_service_state(&ns), ServiceState::NeedBackend));
    assert!(matches!(get_workload_state(&ns), WorkloadState::WaitingForCapacity));
    assert_eq!(ns.status, NamespaceStatus::Creating);
}

// --- DestroyService Tests ---

#[test]
fn test_destroy_service_on_spec_update() {
    let mut ns = active_namespace(test_spec_with_activation());
    let mut pt = PlacementTable::default();
    let mut pod_counter = 0u64;

    // Reconcile to get service to Idle (CreateService sent).
    reconcile_active_namespace(&mut ns, &mut pod_counter);
    assert!(matches!(get_service_state(&ns), ServiceState::Idle));

    // Update spec with no services — should emit DestroyService.
    let empty_spec = NamespaceSpec {
        network: test_network_config(),
        workloads: BTreeMap::new(),
        services: BTreeMap::new(),
    };
    let out = ns.step(NamespaceInput::UpdateSpec {
        client_id: client_id(1),
        spec: empty_spec,
    }, &mut pt);

    assert!(out.worker_commands.iter().any(|(_, cmd)| matches!(
        cmd,
        WorkerCommand::DestroyService { .. }
    )));
}

// --- RegistrySync Tests ---

#[test]
fn test_registry_sync_on_namespace_active() {
    let mut ns = NamespaceStateMachine::new(ns_id("test"), test_spec(), 1);
    let mut pt = PlacementTable::default();
    ns.workers.insert(
        worker_id(1),
        NamespaceWorkerState {
            fabric_status: FabricStatus::Creating,
            primary_pool_id: None,
            pressure_band: PressureBand::Normal,
        },
    );

    let out = ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::NamespaceCreated,
    }, &mut pt);

    assert_eq!(ns.status, NamespaceStatus::Active);
    // Should emit RegistrySync.
    assert!(out.worker_commands.iter().any(|(_, cmd)| matches!(
        cmd,
        WorkerCommand::RegistrySync { .. }
    )));
}

// --- Outer Layer Scheduling Tests ---

#[test]
fn test_outer_layer_scheduling_picks_worker() {
    let mut ns = active_namespace(test_spec());
    let mut pt = PlacementTable::default();

    // Reconcile — workload should go to WaitingForCapacity with a PodRequest.
    let out = ns.step(NamespaceInput::UpdateSpec {
        client_id: client_id(99),
        spec: ns.spec.clone(),
    }, &mut pt);
    assert!(matches!(
        get_workload_state(&ns),
        WorkloadState::WaitingForCapacity
    ));
    assert_eq!(out.pod_requests.len(), 1);
    assert_eq!(out.pod_requests[0].workload_id, wl_id());

    // Inject LaunchPod (simulating outer layer).
    let pod_id = PodId("pod-0".into());
    let out = ns.step(NamespaceInput::LaunchPod {
        workload_id: wl_id(),
        worker_id: worker_id(1),
        pod_id: pod_id.clone(),
    }, &mut pt);
    assert!(matches!(
        get_workload_state(&ns),
        WorkloadState::Launching { .. }
    ));
    assert!(out
        .worker_commands
        .iter()
        .any(|(_, cmd)| matches!(cmd, WorkerCommand::LaunchPod { .. })));
}

#[test]
fn test_waiting_for_capacity_no_workers() {
    let mut ns = NamespaceStateMachine::new(ns_id("test"), test_spec(), 1);
    let mut pt = PlacementTable::default();

    // Make namespace Active with no workers.
    ns.status = NamespaceStatus::Active;

    // Reconcile — workload should emit PodRequest but stay WaitingForCapacity.
    let out = ns.step(NamespaceInput::UpdateSpec {
        client_id: client_id(99),
        spec: ns.spec.clone(),
    }, &mut pt);
    assert!(matches!(
        get_workload_state(&ns),
        WorkloadState::WaitingForCapacity
    ));
    assert_eq!(out.pod_requests.len(), 1);

    // Add a worker and inject LaunchPod.
    ns.workers.insert(
        worker_id(1),
        NamespaceWorkerState {
            fabric_status: FabricStatus::Active,
            primary_pool_id: None,
            pressure_band: PressureBand::Normal,
        },
    );
    let pod_id = PodId("pod-0".into());
    let out = ns.step(NamespaceInput::LaunchPod {
        workload_id: wl_id(),
        worker_id: worker_id(1),
        pod_id: pod_id.clone(),
    }, &mut pt);
    assert!(matches!(
        get_workload_state(&ns),
        WorkloadState::Launching { .. }
    ));
    assert!(out.worker_commands.iter().any(|(wid, cmd)| {
        *wid == worker_id(1) && matches!(cmd, WorkerCommand::LaunchPod { .. })
    }));
}

#[test]
fn test_full_activation_lifecycle_through_orchestrator() {
    let mut orch = Orchestrator::new();

    // Connect worker.
    orch.step(OrchestratorInput::WorkerConnected {
        worker_id: worker_id(1),
        capabilities: worker_caps(),
        wg_config: None,
        tunnel_config: None,
    });

    // Create namespace with activation service.
    orch.step(OrchestratorInput::ClientCommand {
        client_id: client_id(1),
        command: ClientCommand::CreateNamespace {
            namespace_id: ns_id("ns1"),
            spec: test_spec_with_activation(),
        },
    });

    // NamespaceCreated — namespace becomes Active, activation service goes Pending→Idle.
    let out = orch.step(OrchestratorInput::NamespaceInput {
        namespace_id: ns_id("ns1"),
        input: NamespaceInput::WorkerEvent {
            worker_id: worker_id(1),
            event: WorkerEvent::NamespaceCreated,
        },
    });
    let ns = orch.namespaces.get(&ns_id("ns1")).unwrap();
    assert_eq!(ns.status, NamespaceStatus::Active);
    assert!(matches!(
        ns.services[&svc_id()].state,
        ServiceState::Idle
    ));
    assert!(out.worker_commands.iter().any(|(_, cmd)| matches!(
        cmd,
        WorkerCommand::CreateService { .. }
    )));

    // Activation event — outer layer should schedule pod.
    let out = orch.step(OrchestratorInput::NamespaceInput {
        namespace_id: ns_id("ns1"),
        input: NamespaceInput::WorkerEvent {
            worker_id: worker_id(1),
            event: WorkerEvent::ServiceActivation {
                service_id: svc_id(),
            },
        },
    });
    let ns = orch.namespaces.get(&ns_id("ns1")).unwrap();
    assert!(matches!(
        ns.workloads[&wl_id()].state,
        WorkloadState::Launching { .. }
    ));
    assert!(out
        .worker_commands
        .iter()
        .any(|(_, cmd)| matches!(cmd, WorkerCommand::LaunchPod { .. })));

    // Get pod_id from Launching state.
    let pod_id = match &ns.workloads[&wl_id()].state {
        WorkloadState::Launching { pod_id, .. } => pod_id.clone(),
        _ => panic!("expected Launching"),
    };

    // PodRunning — Active.
    let out = orch.step(OrchestratorInput::NamespaceInput {
        namespace_id: ns_id("ns1"),
        input: NamespaceInput::WorkerEvent {
            worker_id: worker_id(1),
            event: WorkerEvent::PodRunning {
                pod_id: pod_id.clone(),
            },
        },
    });
    let ns = orch.namespaces.get(&ns_id("ns1")).unwrap();
    assert!(matches!(
        ns.workloads[&wl_id()].state,
        WorkloadState::Running { .. }
    ));
    assert!(matches!(
        ns.services[&svc_id()].state,
        ServiceState::Active { .. }
    ));
    assert!(out.worker_commands.iter().any(|(_, cmd)| matches!(
        cmd,
        WorkerCommand::UpdateServiceBackend {
            backend: Some(_),
            ..
        }
    )));
    assert!(out.worker_commands.iter().any(|(_, cmd)| matches!(
        cmd,
        WorkerCommand::ServiceReady { .. }
    )));
}

