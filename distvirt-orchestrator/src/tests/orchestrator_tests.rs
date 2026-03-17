use crate::orchestrator::Orchestrator;
use crate::types::*;

use super::helpers::*;

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
    assert!(
        out.client_events
            .iter()
            .any(|(cid, ev)| *cid == client_id(1) && *ev == ClientEvent::Ok)
    );
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

    assert!(
        out.client_events
            .iter()
            .any(|(_, ev)| matches!(ev, ClientEvent::Error { .. }))
    );
}

#[test]
fn test_delete_namespace_no_workers() {
    // Delete with no workers assigned -> immediate destroy.
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

    // No workers, so namespace is immediately destroyed.
    assert!(!orch.namespaces.contains_key(&ns_id("ns1")));

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

    assert!(
        out.client_events
            .iter()
            .any(|(_, ev)| matches!(ev, ClientEvent::Error { .. }))
    );
}

#[test]
fn test_worker_connect_disconnect() {
    let mut orch = Orchestrator::new();

    orch.step(OrchestratorInput::WorkerConnected {
        worker_id: worker_id(1),
        capabilities: worker_caps(),
        wg_config: None,
        tunnel_config: None,
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
    assert!(
        ns2.spec.services[&ServiceId("svc1".into())]
            .activation
            .is_some()
    );
}

// --- Orchestrator Integration Tests ---

#[test]
fn test_create_namespace_assigns_worker() {
    let mut orch = Orchestrator::new();

    // Connect a worker first.
    orch.step(OrchestratorInput::WorkerConnected {
        worker_id: worker_id(1),
        capabilities: worker_caps(),
        wg_config: None,
        tunnel_config: None,
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
    assert!(
        orch.workers[&worker_id(1)]
            .namespaces
            .contains(&ns_id("ns1"))
    );

    // Should have emitted CreateNamespace command with network.
    assert!(out.worker_commands.iter().any(|(wid, cmd)| {
        *wid == worker_id(1)
            && matches!(cmd, WorkerCommand::CreateNamespace { namespace_id, .. } if *namespace_id == ns_id("ns1"))
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
    assert!(
        orch.namespaces
            .get(&ns_id("ns1"))
            .unwrap()
            .workers
            .is_empty()
    );

    // Connect a worker.
    let out = orch.step(OrchestratorInput::WorkerConnected {
        worker_id: worker_id(1),
        capabilities: worker_caps(),
        wg_config: None,
        tunnel_config: None,
    });

    // Worker should be assigned to the namespace.
    let ns = orch.namespaces.get(&ns_id("ns1")).unwrap();
    assert!(ns.workers.contains_key(&worker_id(1)));

    // Should have emitted CreateNamespace command.
    assert!(out.worker_commands.iter().any(|(wid, cmd)| {
        *wid == worker_id(1)
            && matches!(cmd, WorkerCommand::CreateNamespace { namespace_id, .. } if *namespace_id == ns_id("ns1"))
    }));
}
