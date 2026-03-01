use std::collections::HashMap;

use crate::namespace::NamespaceStateMachine;
use crate::orchestrator::Orchestrator;
use crate::types::*;

// --- Test Helpers ---

fn test_spec() -> NamespaceSpec {
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

fn test_spec_with_activation() -> NamespaceSpec {
    let mut services = HashMap::new();
    services.insert(
        ServiceId("svc1".into()),
        ServiceSpec {
            image: "test-image:latest".into(),
            activation: Some(ActivationSpec {
                idle_timeout: std::time::Duration::from_secs(30),
            }),
        },
    );
    NamespaceSpec { services }
}

fn worker_id(n: u32) -> WorkerId {
    WorkerId(format!("worker-{}", n))
}

fn client_id(n: u64) -> ClientId {
    ClientId(n)
}

fn ns_id(name: &str) -> NamespaceId {
    NamespaceId(name.into())
}

fn worker_caps() -> WorkerCapabilities {
    WorkerCapabilities {
        max_pods: 10,
        available_memory_mb: 1024,
    }
}

/// Create a namespace SM with Active status and one Active worker, ready for testing.
fn active_namespace(spec: NamespaceSpec) -> NamespaceStateMachine {
    let mut ns = NamespaceStateMachine::new(ns_id("test"), spec);
    ns.workers.insert(
        worker_id(1),
        NamespaceWorkerState {
            fabric_status: FabricStatus::Active,
            pods: std::collections::HashSet::new(),
        },
    );
    ns.status = NamespaceStatus::Active;
    ns
}

// --- Orchestrator Tests ---

#[test]
fn test_create_namespace() {
    let mut orch = Orchestrator::new();
    let out = orch.step(OrchestratorInput::ClientCommand {
        client_id: client_id(1),
        command: ClientCommand::CreateNamespace {
            namespace_id: ns_id("ns1"),
            spec: test_spec(),
        },
    });

    assert!(orch.namespaces.contains_key(&ns_id("ns1")));
    assert!(out
        .client_events
        .iter()
        .any(|(cid, ev)| *cid == client_id(1) && *ev == ClientEvent::Ok));
}

#[test]
fn test_create_namespace_duplicate() {
    let mut orch = Orchestrator::new();
    orch.step(OrchestratorInput::ClientCommand {
        client_id: client_id(1),
        command: ClientCommand::CreateNamespace {
            namespace_id: ns_id("ns1"),
            spec: test_spec(),
        },
    });

    let out = orch.step(OrchestratorInput::ClientCommand {
        client_id: client_id(1),
        command: ClientCommand::CreateNamespace {
            namespace_id: ns_id("ns1"),
            spec: test_spec(),
        },
    });

    assert!(out.client_events.iter().any(|(_, ev)| matches!(
        ev,
        ClientEvent::Error { .. }
    )));
}

#[test]
fn test_delete_namespace() {
    let mut orch = Orchestrator::new();
    orch.step(OrchestratorInput::ClientCommand {
        client_id: client_id(1),
        command: ClientCommand::CreateNamespace {
            namespace_id: ns_id("ns1"),
            spec: test_spec(),
        },
    });

    let out = orch.step(OrchestratorInput::ClientCommand {
        client_id: client_id(1),
        command: ClientCommand::DeleteNamespace {
            namespace_id: ns_id("ns1"),
        },
    });

    // Namespace still exists but is in Destroying status.
    let ns = orch.namespaces.get(&ns_id("ns1")).unwrap();
    assert_eq!(ns.status, NamespaceStatus::Destroying);

    // Client got Ok through the namespace output.
    let has_ok = out.namespace_outputs.iter().any(|(_, ns_out)| {
        ns_out
            .client_events
            .iter()
            .any(|(_, ev)| *ev == ClientEvent::Ok)
    });
    assert!(has_ok);
}

#[test]
fn test_delete_nonexistent_namespace() {
    let mut orch = Orchestrator::new();
    let out = orch.step(OrchestratorInput::ClientCommand {
        client_id: client_id(1),
        command: ClientCommand::DeleteNamespace {
            namespace_id: ns_id("nope"),
        },
    });

    assert!(out.client_events.iter().any(|(_, ev)| matches!(
        ev,
        ClientEvent::Error { .. }
    )));
}

#[test]
fn test_worker_connect_disconnect() {
    let mut orch = Orchestrator::new();

    orch.step(OrchestratorInput::WorkerConnected {
        worker_id: worker_id(1),
        capabilities: worker_caps(),
    });
    assert!(orch.workers.contains_key(&worker_id(1)));

    orch.step(OrchestratorInput::WorkerDisconnected {
        worker_id: worker_id(1),
    });
    assert!(!orch.workers.contains_key(&worker_id(1)));
}

#[test]
fn test_client_connect_disconnect() {
    let mut orch = Orchestrator::new();

    orch.step(OrchestratorInput::ClientConnected {
        client_id: client_id(1),
    });
    assert!(orch.clients.contains(&client_id(1)));

    orch.step(OrchestratorInput::ClientDisconnected {
        client_id: client_id(1),
    });
    assert!(!orch.clients.contains(&client_id(1)));
}

#[test]
fn test_worker_lost_fans_out() {
    let mut orch = Orchestrator::new();

    // Connect a worker and create a namespace.
    orch.step(OrchestratorInput::WorkerConnected {
        worker_id: worker_id(1),
        capabilities: worker_caps(),
    });
    orch.step(OrchestratorInput::ClientCommand {
        client_id: client_id(1),
        command: ClientCommand::CreateNamespace {
            namespace_id: ns_id("ns1"),
            spec: test_spec(),
        },
    });

    // The orchestrator should have assigned worker-1 to ns1.
    let ns = orch.namespaces.get(&ns_id("ns1")).unwrap();
    assert!(ns.workers.contains_key(&worker_id(1)));

    // Disconnect the worker — should fan out WorkerLost.
    orch.step(OrchestratorInput::WorkerDisconnected {
        worker_id: worker_id(1),
    });

    // The namespace should have removed the worker from its map.
    let ns = orch.namespaces.get(&ns_id("ns1")).unwrap();
    assert!(!ns.workers.contains_key(&worker_id(1)));
}

#[test]
fn test_namespace_step_routing() {
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

    // Update ns1 spec — should only affect ns1.
    let new_spec = test_spec_with_activation();
    orch.step(OrchestratorInput::ClientCommand {
        client_id: client_id(1),
        command: ClientCommand::UpdateNamespace {
            namespace_id: ns_id("ns1"),
            spec: new_spec.clone(),
        },
    });

    let ns1 = orch.namespaces.get(&ns_id("ns1")).unwrap();
    assert_eq!(ns1.spec, new_spec);

    // ns2 should be unchanged.
    let ns2 = orch.namespaces.get(&ns_id("ns2")).unwrap();
    assert!(ns2.spec.services[&ServiceId("svc1".into())]
        .activation
        .is_some());
}

// --- Orchestrator Integration Tests ---

#[test]
fn test_create_namespace_assigns_worker() {
    let mut orch = Orchestrator::new();

    // Connect a worker first.
    orch.step(OrchestratorInput::WorkerConnected {
        worker_id: worker_id(1),
        capabilities: worker_caps(),
    });

    // Create a namespace.
    let out = orch.step(OrchestratorInput::ClientCommand {
        client_id: client_id(1),
        command: ClientCommand::CreateNamespace {
            namespace_id: ns_id("ns1"),
            spec: test_spec(),
        },
    });

    // Worker should be assigned to the namespace.
    let ns = orch.namespaces.get(&ns_id("ns1")).unwrap();
    assert!(ns.workers.contains_key(&worker_id(1)));
    assert_eq!(
        ns.workers[&worker_id(1)].fabric_status,
        FabricStatus::Creating
    );

    // Worker should know about the namespace.
    assert!(orch.workers[&worker_id(1)]
        .namespaces
        .contains(&ns_id("ns1")));

    // Should have emitted CreateNamespace command.
    assert!(out.worker_commands.iter().any(|(wid, cmd)| {
        *wid == worker_id(1)
            && matches!(cmd, WorkerCommand::CreateNamespace { namespace_id } if *namespace_id == ns_id("ns1"))
    }));
}

#[test]
fn test_worker_connects_assigns_to_workerless_namespace() {
    let mut orch = Orchestrator::new();

    // Create namespace without any workers.
    orch.step(OrchestratorInput::ClientCommand {
        client_id: client_id(1),
        command: ClientCommand::CreateNamespace {
            namespace_id: ns_id("ns1"),
            spec: test_spec(),
        },
    });

    // Namespace should have no workers.
    assert!(orch.namespaces.get(&ns_id("ns1")).unwrap().workers.is_empty());

    // Connect a worker.
    let out = orch.step(OrchestratorInput::WorkerConnected {
        worker_id: worker_id(1),
        capabilities: worker_caps(),
    });

    // Worker should be assigned to the namespace.
    let ns = orch.namespaces.get(&ns_id("ns1")).unwrap();
    assert!(ns.workers.contains_key(&worker_id(1)));

    // Should have emitted CreateNamespace command.
    assert!(out.worker_commands.iter().any(|(wid, cmd)| {
        *wid == worker_id(1)
            && matches!(cmd, WorkerCommand::CreateNamespace { namespace_id } if *namespace_id == ns_id("ns1"))
    }));
}

// --- Namespace State Machine Tests ---

#[test]
fn test_namespace_new_initializes_services() {
    let spec = test_spec();
    let ns = NamespaceStateMachine::new(ns_id("test"), spec);

    assert_eq!(ns.status, NamespaceStatus::Creating);
    assert_eq!(ns.services.len(), 1);
    assert_eq!(
        ns.services[&ServiceId("svc1".into())],
        ServiceState::Pending
    );
    assert!(ns.pods.is_empty());
    assert!(ns.workers.is_empty());
}

#[test]
fn test_namespace_update_spec() {
    let mut ns = NamespaceStateMachine::new(ns_id("test"), test_spec());
    let new_spec = test_spec_with_activation();
    let out = ns.step(NamespaceInput::UpdateSpec {
        client_id: client_id(1),
        spec: new_spec.clone(),
    });

    assert_eq!(ns.spec, new_spec);
    assert!(out
        .client_events
        .iter()
        .any(|(_, ev)| *ev == ClientEvent::Ok));
}

#[test]
fn test_namespace_delete() {
    let mut ns = NamespaceStateMachine::new(ns_id("test"), test_spec());
    let out = ns.step(NamespaceInput::Delete {
        client_id: client_id(1),
    });

    assert_eq!(ns.status, NamespaceStatus::Destroying);
    assert!(out
        .client_events
        .iter()
        .any(|(_, ev)| *ev == ClientEvent::Ok));
}

#[test]
fn test_namespace_worker_lost_removes_worker() {
    let mut ns = NamespaceStateMachine::new(ns_id("test"), test_spec());
    ns.workers.insert(
        worker_id(1),
        NamespaceWorkerState {
            fabric_status: FabricStatus::Active,
            pods: std::collections::HashSet::new(),
        },
    );

    ns.step(NamespaceInput::WorkerLost {
        worker_id: worker_id(1),
    });

    assert!(!ns.workers.contains_key(&worker_id(1)));
}

#[test]
fn test_namespace_get_status() {
    let ns_name = ns_id("test");
    let mut ns = NamespaceStateMachine::new(ns_name.clone(), test_spec());
    let out = ns.step(NamespaceInput::GetStatus {
        client_id: client_id(1),
    });

    assert!(out.client_events.iter().any(|(cid, ev)| {
        *cid == client_id(1)
            && matches!(ev, ClientEvent::NamespaceStatus { namespace_id, status }
                if *namespace_id == ns_name && status.status == NamespaceStatus::Creating)
    }));
}

// --- Lifecycle Tests ---

#[test]
fn test_namespace_created_activates_namespace() {
    let mut ns = NamespaceStateMachine::new(ns_id("test"), test_spec());
    ns.workers.insert(
        worker_id(1),
        NamespaceWorkerState {
            fabric_status: FabricStatus::Creating,
            pods: std::collections::HashSet::new(),
        },
    );

    let out = ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::NamespaceCreated,
    });

    assert_eq!(ns.status, NamespaceStatus::Active);
    assert_eq!(
        ns.workers[&worker_id(1)].fabric_status,
        FabricStatus::Active
    );

    // Always-on service should have been launched (CreateService + LaunchPod).
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
    let svc_id = ServiceId("svc1".into());

    // Reconcile: activation service goes Pending → Idle with CreateService on workers.
    let out = ns.step(NamespaceInput::CapacityAvailable);
    assert_eq!(ns.services[&svc_id], ServiceState::Idle);
    assert!(out.worker_commands.iter().any(|(_, cmd)| matches!(
        cmd,
        WorkerCommand::CreateService { .. }
    )));

    // ServiceActivation → should launch pod.
    let out = ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::ServiceActivation {
            service_id: svc_id.clone(),
        },
    });
    assert!(matches!(
        ns.services[&svc_id],
        ServiceState::Launching { .. }
    ));
    assert!(out
        .worker_commands
        .iter()
        .any(|(_, cmd)| matches!(cmd, WorkerCommand::LaunchPod { .. })));

    // Get the pod_id from the Launching state.
    let pod_id = match &ns.services[&svc_id] {
        ServiceState::Launching { pod_id, .. } => pod_id.clone(),
        _ => panic!("expected Launching"),
    };

    // PodRunning → Active.
    let out = ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::PodRunning {
            pod_id: pod_id.clone(),
        },
    });
    assert!(matches!(
        ns.services[&svc_id],
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
            service_id: svc_id.clone(),
            need: BackendNeed::None,
        },
    });
    assert!(!out.timers_set.is_empty());
    let idle_timer = out.timers_set[0].0.clone();

    // Idle timer fires → back to Idle.
    let out = ns.step(NamespaceInput::TimerFired {
        timer_key: idle_timer,
    });
    assert_eq!(ns.services[&svc_id], ServiceState::Idle);
    assert!(out
        .worker_commands
        .iter()
        .any(|(_, cmd)| matches!(cmd, WorkerCommand::StopPod { .. })));
}

#[test]
fn test_always_on_service_lifecycle() {
    let mut ns = active_namespace(test_spec());
    let svc_id = ServiceId("svc1".into());

    // Reconcile: always-on service goes Pending → Launching.
    let _out = ns.step(NamespaceInput::CapacityAvailable);
    assert!(matches!(
        ns.services[&svc_id],
        ServiceState::Launching { .. }
    ));

    let pod_id = match &ns.services[&svc_id] {
        ServiceState::Launching { pod_id, .. } => pod_id.clone(),
        _ => panic!("expected Launching"),
    };

    // PodRunning → Active.
    let _out = ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::PodRunning {
            pod_id: pod_id.clone(),
        },
    });
    assert!(matches!(
        ns.services[&svc_id],
        ServiceState::Active { .. }
    ));

    // PodExited → should re-launch (always-on).
    let _out = ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::PodExited {
            pod_id: pod_id.clone(),
        },
    });
    assert!(matches!(
        ns.services[&svc_id],
        ServiceState::Launching { .. }
    ));
}

#[test]
fn test_worker_loss_during_active_service() {
    let mut ns = active_namespace(test_spec_with_activation());
    let svc_id = ServiceId("svc1".into());

    // Get to Active state.
    ns.step(NamespaceInput::CapacityAvailable);
    ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::ServiceActivation {
            service_id: svc_id.clone(),
        },
    });
    let pod_id = match &ns.services[&svc_id] {
        ServiceState::Launching { pod_id, .. } => pod_id.clone(),
        _ => panic!("expected Launching"),
    };
    ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::PodRunning { pod_id },
    });
    assert!(matches!(
        ns.services[&svc_id],
        ServiceState::Active { .. }
    ));

    // Worker lost → service should go to Idle, namespace to Creating (no workers).
    let _out = ns.step(NamespaceInput::WorkerLost {
        worker_id: worker_id(1),
    });
    assert_eq!(ns.services[&svc_id], ServiceState::Idle);
    assert_eq!(ns.status, NamespaceStatus::Creating);
    assert!(ns.workers.is_empty());
    assert!(ns.pods.is_empty());
}

#[test]
fn test_launch_timeout() {
    let mut ns = active_namespace(test_spec_with_activation());
    let svc_id = ServiceId("svc1".into());

    // Get to Launching state.
    ns.step(NamespaceInput::CapacityAvailable);
    ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::ServiceActivation {
            service_id: svc_id.clone(),
        },
    });
    let (pod_id, launch_timeout) = match &ns.services[&svc_id] {
        ServiceState::Launching {
            pod_id,
            launch_timeout,
            ..
        } => (pod_id.clone(), launch_timeout.clone()),
        _ => panic!("expected Launching"),
    };

    // Launch timeout fires → should stop pod and go to Idle (activation service).
    let out = ns.step(NamespaceInput::TimerFired {
        timer_key: launch_timeout,
    });
    assert_eq!(ns.services[&svc_id], ServiceState::Idle);
    assert!(!ns.pods.contains_key(&pod_id));
    assert!(out
        .worker_commands
        .iter()
        .any(|(_, cmd)| matches!(cmd, WorkerCommand::StopPod { .. })));
}

#[test]
fn test_delete_stops_pods_and_destroys() {
    let mut ns = active_namespace(test_spec());
    let svc_id = ServiceId("svc1".into());

    // Get to Active with a running pod.
    ns.step(NamespaceInput::CapacityAvailable);
    let pod_id = match &ns.services[&svc_id] {
        ServiceState::Launching { pod_id, .. } => pod_id.clone(),
        _ => panic!("expected Launching"),
    };
    ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::PodRunning { pod_id },
    });

    // Delete.
    let out = ns.step(NamespaceInput::Delete {
        client_id: client_id(1),
    });
    assert_eq!(ns.status, NamespaceStatus::Destroying);
    assert!(out
        .worker_commands
        .iter()
        .any(|(_, cmd)| matches!(cmd, WorkerCommand::StopPod { .. })));
    assert!(out
        .worker_commands
        .iter()
        .any(|(_, cmd)| matches!(cmd, WorkerCommand::DestroyNamespace { .. })));
}

#[test]
fn test_idle_timer_cancelled_on_traffic() {
    let mut ns = active_namespace(test_spec_with_activation());
    let svc_id = ServiceId("svc1".into());

    // Get to Active.
    ns.step(NamespaceInput::CapacityAvailable);
    ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::ServiceActivation {
            service_id: svc_id.clone(),
        },
    });
    let pod_id = match &ns.services[&svc_id] {
        ServiceState::Launching { pod_id, .. } => pod_id.clone(),
        _ => panic!("expected Launching"),
    };
    ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::PodRunning { pod_id },
    });

    // BackendNeed::None → idle timer set.
    let out = ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::ServiceBackendNeed {
            service_id: svc_id.clone(),
            need: BackendNeed::None,
        },
    });
    assert!(!out.timers_set.is_empty());

    // BackendNeed::Traffic → idle timer cancelled.
    let out = ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::ServiceBackendNeed {
            service_id: svc_id.clone(),
            need: BackendNeed::Traffic,
        },
    });
    assert!(!out.timers_cancel.is_empty());
}
