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

    // Manually register the worker in the namespace's worker map
    // (normally done by namespace reconciliation, but we're testing the fan-out).
    let ns = orch.namespaces.get_mut(&ns_id("ns1")).unwrap();
    ns.workers.insert(
        worker_id(1),
        NamespaceWorkerState {
            fabric_status: FabricStatus::Active,
            pods: std::collections::HashSet::new(),
        },
    );
    // Also register the namespace in the worker state.
    orch.workers
        .get_mut(&worker_id(1))
        .unwrap()
        .namespaces
        .insert(ns_id("ns1"));

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

// --- Namespace State Machine Tests ---

#[test]
fn test_namespace_new_initializes_services() {
    let spec = test_spec();
    let ns = NamespaceStateMachine::new(spec);

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
    let mut ns = NamespaceStateMachine::new(test_spec());
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
    let mut ns = NamespaceStateMachine::new(test_spec());
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
    let mut ns = NamespaceStateMachine::new(test_spec());
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
