use std::net::Ipv4Addr;

use crate::namespace::NamespaceStateMachine;
use crate::orchestrator::Orchestrator;
use crate::types::*;

use super::helpers::*;

// --- WireGuard Connect/Disconnect: Namespace SM Tests ---

#[test]
fn test_wg_connect_happy_path() {
    let mut ns = active_namespace(test_spec());
    let pubkey = [0x01; 32];

    let out = ns.step(NamespaceInput::Connect {
        client_id: client_id(1),
        client_public_key: pubkey,
        worker_wg_public_key: [0xab; 32],
        worker_endpoint: "1.2.3.4:51820".to_string(),
    });

    // Should get ConnectResult with IP 172.16.0.254.
    assert!(out.client_events.iter().any(|(cid, ev)| {
        *cid == client_id(1)
            && matches!(ev, ClientEvent::ConnectResult {
                client_ip,
                subnet,
                server_public_key,
                endpoint,
            } if client_ip == "172.16.0.254"
                && subnet == "172.16.0.0/24"
                && *server_public_key == [0xab; 32]
                && endpoint == "1.2.3.4:51820")
    }));

    // Should emit AddWireGuardPeer to worker_id(1).
    assert!(out.worker_commands.iter().any(|(wid, cmd)| {
        *wid == worker_id(1)
            && matches!(cmd, WorkerCommand::AddWireGuardPeer {
                peer_public_key,
                peer_ip,
                ..
            } if *peer_public_key == pubkey && *peer_ip == Ipv4Addr::new(172, 16, 0, 254))
    }));

    // Peer should be tracked.
    assert!(ns.wg_peer_manager.peers.contains_key(&pubkey));
    assert_eq!(ns.wg_peer_manager.next_host_offset, 253);
}

#[test]
fn test_wg_connect_idempotent() {
    let mut ns = active_namespace(test_spec());
    let pubkey = [0x01; 32];

    // First connect.
    ns.step(NamespaceInput::Connect {
        client_id: client_id(1),
        client_public_key: pubkey,
        worker_wg_public_key: [0xab; 32],
        worker_endpoint: "1.2.3.4:51820".to_string(),
    });
    assert_eq!(ns.wg_peer_manager.next_host_offset, 253);

    // Second connect with same key.
    let out = ns.step(NamespaceInput::Connect {
        client_id: client_id(2),
        client_public_key: pubkey,
        worker_wg_public_key: [0xab; 32],
        worker_endpoint: "1.2.3.4:51820".to_string(),
    });

    // Should return same IP, no new AddWireGuardPeer, offset unchanged.
    assert!(out.client_events.iter().any(|(cid, ev)| {
        *cid == client_id(2)
            && matches!(ev, ClientEvent::ConnectResult { client_ip, .. } if client_ip == "172.16.0.254")
    }));
    assert!(out.worker_commands.is_empty());
    assert_eq!(ns.wg_peer_manager.next_host_offset, 253);
}

#[test]
fn test_wg_connect_multiple_peers() {
    let mut ns = active_namespace(test_spec());
    let pubkey_a = [0x01; 32];
    let pubkey_b = [0x02; 32];

    let out_a = ns.step(NamespaceInput::Connect {
        client_id: client_id(1),
        client_public_key: pubkey_a,
        worker_wg_public_key: [0xab; 32],
        worker_endpoint: "1.2.3.4:51820".to_string(),
    });
    let out_b = ns.step(NamespaceInput::Connect {
        client_id: client_id(2),
        client_public_key: pubkey_b,
        worker_wg_public_key: [0xab; 32],
        worker_endpoint: "1.2.3.4:51820".to_string(),
    });

    // First gets .254, second gets .253.
    assert!(out_a.client_events.iter().any(|(_, ev)| {
        matches!(ev, ClientEvent::ConnectResult { client_ip, .. } if client_ip == "172.16.0.254")
    }));
    assert!(out_b.client_events.iter().any(|(_, ev)| {
        matches!(ev, ClientEvent::ConnectResult { client_ip, .. } if client_ip == "172.16.0.253")
    }));

    assert_eq!(ns.wg_peer_manager.peers.len(), 2);
    assert_eq!(ns.wg_peer_manager.next_host_offset, 252);
}

#[test]
fn test_wg_connect_namespace_not_active() {
    // Namespace in Creating state (before NamespaceCreated).
    let mut ns = NamespaceStateMachine::new(ns_id("test"), test_spec(), 1);
    assert_eq!(ns.status, NamespaceStatus::Creating);

    let out = ns.step(NamespaceInput::Connect {
        client_id: client_id(1),
        client_public_key: [0x01; 32],
        worker_wg_public_key: [0xab; 32],
        worker_endpoint: "1.2.3.4:51820".to_string(),
    });

    assert!(out.client_events.iter().any(|(cid, ev)| {
        *cid == client_id(1)
            && matches!(ev, ClientEvent::Error { message } if message == "namespace is not active")
    }));
    assert!(ns.wg_peer_manager.peers.is_empty());
}

#[test]
fn test_wg_connect_ip_exhaustion() {
    let mut ns = active_namespace(test_spec());
    ns.wg_peer_manager.next_host_offset = 1; // No IPs left (< 2).

    let out = ns.step(NamespaceInput::Connect {
        client_id: client_id(1),
        client_public_key: [0x01; 32],
        worker_wg_public_key: [0xab; 32],
        worker_endpoint: "1.2.3.4:51820".to_string(),
    });

    assert!(out.client_events.iter().any(|(cid, ev)| {
        *cid == client_id(1)
            && matches!(ev, ClientEvent::Error { message } if message == "no more WireGuard peer IPs available")
    }));
    assert!(ns.wg_peer_manager.peers.is_empty());
}

#[test]
fn test_wg_disconnect_known_peer() {
    let mut ns = active_namespace(test_spec());
    let pubkey = [0x01; 32];

    // Connect first.
    ns.step(NamespaceInput::Connect {
        client_id: client_id(1),
        client_public_key: pubkey,
        worker_wg_public_key: [0xab; 32],
        worker_endpoint: "1.2.3.4:51820".to_string(),
    });
    assert!(ns.wg_peer_manager.peers.contains_key(&pubkey));

    // Disconnect.
    let out = ns.step(NamespaceInput::Disconnect {
        client_id: client_id(1),
        client_public_key: pubkey,
    });

    assert!(out.client_events.iter().any(|(cid, ev)| {
        *cid == client_id(1) && *ev == ClientEvent::Ok
    }));
    assert!(out.worker_commands.iter().any(|(wid, cmd)| {
        *wid == worker_id(1)
            && matches!(cmd, WorkerCommand::RemoveWireGuardPeer { peer_public_key } if *peer_public_key == pubkey)
    }));
    assert!(!ns.wg_peer_manager.peers.contains_key(&pubkey));
}

#[test]
fn test_wg_disconnect_unknown_peer() {
    let mut ns = active_namespace(test_spec());
    let pubkey = [0x01; 32];

    // Disconnect without prior connect.
    let out = ns.step(NamespaceInput::Disconnect {
        client_id: client_id(1),
        client_public_key: pubkey,
    });

    assert!(out.client_events.iter().any(|(cid, ev)| {
        *cid == client_id(1) && *ev == ClientEvent::Ok
    }));
    // No RemoveWireGuardPeer should be emitted.
    assert!(!out.worker_commands.iter().any(|(_, cmd)| matches!(
        cmd,
        WorkerCommand::RemoveWireGuardPeer { .. }
    )));
}

#[test]
fn test_wg_connect_after_disconnect() {
    let mut ns = active_namespace(test_spec());
    let pubkey = [0x01; 32];

    // Connect → gets .254.
    let out1 = ns.step(NamespaceInput::Connect {
        client_id: client_id(1),
        client_public_key: pubkey,
        worker_wg_public_key: [0xab; 32],
        worker_endpoint: "1.2.3.4:51820".to_string(),
    });
    assert!(out1.client_events.iter().any(|(_, ev)| {
        matches!(ev, ClientEvent::ConnectResult { client_ip, .. } if client_ip == "172.16.0.254")
    }));

    // Disconnect.
    ns.step(NamespaceInput::Disconnect {
        client_id: client_id(1),
        client_public_key: pubkey,
    });

    // Reconnect same key → gets NEW IP (.253, offset doesn't go back).
    let out2 = ns.step(NamespaceInput::Connect {
        client_id: client_id(1),
        client_public_key: pubkey,
        worker_wg_public_key: [0xab; 32],
        worker_endpoint: "1.2.3.4:51820".to_string(),
    });
    assert!(out2.client_events.iter().any(|(_, ev)| {
        matches!(ev, ClientEvent::ConnectResult { client_ip, .. } if client_ip == "172.16.0.253")
    }));
    assert_eq!(ns.wg_peer_manager.next_host_offset, 252);
}

// --- WireGuard Connect/Disconnect: Orchestrator-Level Tests ---

/// Helper: set up orchestrator with a worker (with WG config) and an active namespace.
fn setup_orch_with_wg() -> Orchestrator {
    let mut orch = Orchestrator::new();

    // Connect client.
    orch.step(OrchestratorInput::ClientConnected {
        client_id: client_id(1),
    });

    // Connect worker with endpoint and WG config.
    orch.step(OrchestratorInput::WorkerConnected {
        worker_id: worker_id(1),
        capabilities: worker_caps_with_endpoint("1.2.3.4"),
        wg_config: Some(test_wg_config()),
        tunnel_config: None,
    });

    // Create namespace (worker auto-assigned, status=Creating).
    orch.step(OrchestratorInput::ClientCommand {
        client_id: client_id(1),
        command: ClientCommand::CreateNamespace {
            namespace_id: ns_id("ns1"),
            spec: test_spec(),
        },
    });

    // Simulate NamespaceCreated to activate.
    orch.step(OrchestratorInput::NamespaceInput {
        namespace_id: ns_id("ns1"),
        input: NamespaceInput::WorkerEvent {
            worker_id: worker_id(1),
            event: WorkerEvent::NamespaceCreated,
        },
    });

    assert_eq!(
        orch.namespaces[&ns_id("ns1")].status,
        NamespaceStatus::Active
    );
    orch
}

#[test]
fn test_wg_connect_through_orchestrator() {
    let mut orch = setup_orch_with_wg();
    let pubkey = [0x01; 32];

    let out = orch.step(OrchestratorInput::ClientCommand {
        client_id: client_id(1),
        command: ClientCommand::Connect {
            namespace_id: ns_id("ns1"),
            client_public_key: pubkey,
        },
    });

    // ConnectResult is in namespace_outputs (routed through namespace SM).
    let has_connect_result = out.namespace_outputs.iter().any(|(_, ns_out)| {
        ns_out.client_events.iter().any(|(cid, ev)| {
            *cid == client_id(1)
                && matches!(ev, ClientEvent::ConnectResult {
                    endpoint,
                    server_public_key,
                    client_ip,
                    subnet,
                } if endpoint == "1.2.3.4:51820"
                    && *server_public_key == [0xab; 32]
                    && client_ip == "172.16.0.254"
                    && subnet == "172.16.0.0/24")
        })
    });
    assert!(has_connect_result);

    // AddWireGuardPeer should be in worker_commands.
    assert!(out.worker_commands.iter().any(|(wid, cmd)| {
        *wid == worker_id(1)
            && matches!(cmd, WorkerCommand::AddWireGuardPeer {
                peer_public_key,
                peer_ip,
                ..
            } if *peer_public_key == pubkey && *peer_ip == Ipv4Addr::new(172, 16, 0, 254))
    }));
}

#[test]
fn test_wg_connect_namespace_not_found() {
    let mut orch = setup_orch_with_wg();

    let out = orch.step(OrchestratorInput::ClientCommand {
        client_id: client_id(1),
        command: ClientCommand::Connect {
            namespace_id: ns_id("nonexistent"),
            client_public_key: [0x01; 32],
        },
    });

    assert!(out.client_events.iter().any(|(cid, ev)| {
        *cid == client_id(1)
            && matches!(ev, ClientEvent::Error { message } if message == "namespace not found")
    }));
}

#[test]
fn test_wg_connect_no_wg_config() {
    let mut orch = Orchestrator::new();

    orch.step(OrchestratorInput::ClientConnected {
        client_id: client_id(1),
    });

    // Connect worker WITHOUT WG config.
    orch.step(OrchestratorInput::WorkerConnected {
        worker_id: worker_id(1),
        capabilities: worker_caps_with_endpoint("1.2.3.4"),
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

    orch.step(OrchestratorInput::NamespaceInput {
        namespace_id: ns_id("ns1"),
        input: NamespaceInput::WorkerEvent {
            worker_id: worker_id(1),
            event: WorkerEvent::NamespaceCreated,
        },
    });

    let out = orch.step(OrchestratorInput::ClientCommand {
        client_id: client_id(1),
        command: ClientCommand::Connect {
            namespace_id: ns_id("ns1"),
            client_public_key: [0x01; 32],
        },
    });

    assert!(out.client_events.iter().any(|(cid, ev)| {
        *cid == client_id(1)
            && matches!(ev, ClientEvent::Error { message } if message == "worker does not have WireGuard configured")
    }));
}

#[test]
fn test_wg_connect_no_public_endpoint() {
    let mut orch = Orchestrator::new();

    orch.step(OrchestratorInput::ClientConnected {
        client_id: client_id(1),
    });

    // Connect worker with empty public_endpoint but with WG config.
    orch.step(OrchestratorInput::WorkerConnected {
        worker_id: worker_id(1),
        capabilities: worker_caps(), // empty public_endpoint
        wg_config: Some(test_wg_config()),
        tunnel_config: None,
    });

    orch.step(OrchestratorInput::ClientCommand {
        client_id: client_id(1),
        command: ClientCommand::CreateNamespace {
            namespace_id: ns_id("ns1"),
            spec: test_spec(),
        },
    });

    orch.step(OrchestratorInput::NamespaceInput {
        namespace_id: ns_id("ns1"),
        input: NamespaceInput::WorkerEvent {
            worker_id: worker_id(1),
            event: WorkerEvent::NamespaceCreated,
        },
    });

    let out = orch.step(OrchestratorInput::ClientCommand {
        client_id: client_id(1),
        command: ClientCommand::Connect {
            namespace_id: ns_id("ns1"),
            client_public_key: [0x01; 32],
        },
    });

    assert!(out.client_events.iter().any(|(cid, ev)| {
        *cid == client_id(1)
            && matches!(ev, ClientEvent::Error { message } if message == "worker has no public endpoint")
    }));
}

// --- DeactivateWorkload Tests ---

#[test]
fn test_deactivate_workload_active_idle() {
    // Set up: namespace with activation service, activate it, get it running, then
    // report BackendNeed::None so the service is Active but idle.
    let mut orch = Orchestrator::new();

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
            spec: test_spec_with_activation(),
        },
    });

    // Namespace becomes active.
    orch.step(OrchestratorInput::NamespaceInput {
        namespace_id: ns_id("ns1"),
        input: NamespaceInput::WorkerEvent {
            worker_id: worker_id(1),
            event: WorkerEvent::NamespaceCreated,
        },
    });

    // Trigger activation.
    orch.step(OrchestratorInput::NamespaceInput {
        namespace_id: ns_id("ns1"),
        input: NamespaceInput::WorkerEvent {
            worker_id: worker_id(1),
            event: WorkerEvent::ServiceActivation {
                service_id: svc_id(),
            },
        },
    });

    // Get pod_id from launching state.
    let pod_id = match &orch.namespaces[&ns_id("ns1")].workloads[&wl_id()].state {
        WorkloadState::Launching { pod_id, .. } => pod_id.clone(),
        _ => panic!("expected Launching"),
    };

    // Pod running.
    orch.step(OrchestratorInput::NamespaceInput {
        namespace_id: ns_id("ns1"),
        input: NamespaceInput::WorkerEvent {
            worker_id: worker_id(1),
            event: WorkerEvent::PodRunning {
                pod_id: pod_id.clone(),
            },
        },
    });

    // Backend need goes to None (service is idle but Active).
    orch.step(OrchestratorInput::NamespaceInput {
        namespace_id: ns_id("ns1"),
        input: NamespaceInput::WorkerEvent {
            worker_id: worker_id(1),
            event: WorkerEvent::ServiceBackendNeed {
                service_id: svc_id(),
                need: BackendNeed::None,
            },
        },
    });

    // Verify service is Active with backend_need == None.
    let ns = orch.namespaces.get(&ns_id("ns1")).unwrap();
    assert!(matches!(
        ns.services[&svc_id()].state,
        ServiceState::Active {
            backend_need: BackendNeed::None,
            ..
        }
    ));

    // Deactivate.
    let out = orch.step(OrchestratorInput::ClientCommand {
        client_id: client_id(1),
        command: ClientCommand::DeactivateWorkload {
            namespace_id: ns_id("ns1"),
            workload_id: wl_id(),
        },
    });

    // Should succeed — check in namespace_outputs for client events.
    let has_result = out.namespace_outputs.iter().any(|(_, ns_out)| {
        ns_out.client_events.iter().any(|(cid, ev)| {
            *cid == client_id(1)
                && matches!(ev, ClientEvent::DeactivateWorkloadResult { deactivated: true, .. })
        })
    });
    assert!(has_result);

    // Service should be Idle, workload should be stopping.
    let ns = orch.namespaces.get(&ns_id("ns1")).unwrap();
    assert!(matches!(
        ns.services[&svc_id()].state,
        ServiceState::Idle
    ));
    // Workload demand should have dropped.
    assert_eq!(ns.workloads[&wl_id()].demand_count, 0);
}

#[test]
fn test_deactivate_workload_with_demand_refused() {
    let mut orch = Orchestrator::new();

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
            spec: test_spec_with_activation(),
        },
    });

    orch.step(OrchestratorInput::NamespaceInput {
        namespace_id: ns_id("ns1"),
        input: NamespaceInput::WorkerEvent {
            worker_id: worker_id(1),
            event: WorkerEvent::NamespaceCreated,
        },
    });

    // Activate.
    orch.step(OrchestratorInput::NamespaceInput {
        namespace_id: ns_id("ns1"),
        input: NamespaceInput::WorkerEvent {
            worker_id: worker_id(1),
            event: WorkerEvent::ServiceActivation {
                service_id: svc_id(),
            },
        },
    });

    let pod_id = match &orch.namespaces[&ns_id("ns1")].workloads[&wl_id()].state {
        WorkloadState::Launching { pod_id, .. } => pod_id.clone(),
        _ => panic!("expected Launching"),
    };

    orch.step(OrchestratorInput::NamespaceInput {
        namespace_id: ns_id("ns1"),
        input: NamespaceInput::WorkerEvent {
            worker_id: worker_id(1),
            event: WorkerEvent::PodRunning { pod_id },
        },
    });

    // Service is Active with BackendNeed::Active (initial state after WorkloadReady).
    // Try to deactivate — should be refused.
    let out = orch.step(OrchestratorInput::ClientCommand {
        client_id: client_id(1),
        command: ClientCommand::DeactivateWorkload {
            namespace_id: ns_id("ns1"),
            workload_id: wl_id(),
        },
    });

    let has_result = out.namespace_outputs.iter().any(|(_, ns_out)| {
        ns_out.client_events.iter().any(|(cid, ev)| {
            *cid == client_id(1)
                && matches!(ev, ClientEvent::DeactivateWorkloadResult { deactivated: false, reason } if reason.contains("active demand"))
        })
    });
    assert!(has_result);
}

#[test]
fn test_deactivate_workload_not_found() {
    let mut orch = Orchestrator::new();

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
            spec: test_spec_with_activation(),
        },
    });

    orch.step(OrchestratorInput::NamespaceInput {
        namespace_id: ns_id("ns1"),
        input: NamespaceInput::WorkerEvent {
            worker_id: worker_id(1),
            event: WorkerEvent::NamespaceCreated,
        },
    });

    // Deactivate a nonexistent workload.
    let out = orch.step(OrchestratorInput::ClientCommand {
        client_id: client_id(1),
        command: ClientCommand::DeactivateWorkload {
            namespace_id: ns_id("ns1"),
            workload_id: WorkloadId("nonexistent".into()),
        },
    });

    let has_result = out.namespace_outputs.iter().any(|(_, ns_out)| {
        ns_out.client_events.iter().any(|(cid, ev)| {
            *cid == client_id(1)
                && matches!(ev, ClientEvent::DeactivateWorkloadResult { deactivated: false, reason } if reason.contains("not found"))
        })
    });
    assert!(has_result);
}
