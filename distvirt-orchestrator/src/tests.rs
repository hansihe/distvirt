use std::collections::HashMap;
use std::net::Ipv4Addr;

use crate::namespace::NamespaceStateMachine;
use crate::orchestrator::Orchestrator;
use crate::types::*;

// --- Test Helpers ---

fn test_network_config() -> NetworkConfig {
    NetworkConfig {
        subnet: Ipv4Addr::new(172, 16, 0, 0),
        gateway: Ipv4Addr::new(172, 16, 0, 1),
        prefix_len: 24,
    }
}

fn test_pod_network_config() -> PodNetworkConfig {
    PodNetworkConfig {
        ip: Ipv4Addr::new(172, 16, 0, 10),
        mac: [0x02, 0x00, 0x00, 0x00, 0x00, 0x10],
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
            entrypoint: "/bin/sh".into(),
            args: vec![],
            env: vec![],
            working_dir: None,
            uid: None,
            gid: None,
            hostname: None,
            capture_output: false,
        },
    }
}

fn test_spec() -> NamespaceSpec {
    let mut workloads = HashMap::new();
    workloads.insert(
        WorkloadId("svc1".into()),
        WorkloadSpec {
            containers: vec![test_container_spec()],
            network: test_pod_network_config(),
        },
    );
    let mut services = HashMap::new();
    services.insert(
        ServiceId("svc1".into()),
        ServiceSpec {
            workload_id: WorkloadId("svc1".into()),
            ip: Ipv4Addr::new(172, 16, 0, 100),
            mac: [0x02, 0x00, 0x00, 0x00, 0x01, 0x00],
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

fn test_spec_with_activation() -> NamespaceSpec {
    let mut workloads = HashMap::new();
    workloads.insert(
        WorkloadId("svc1".into()),
        WorkloadSpec {
            containers: vec![test_container_spec()],
            network: test_pod_network_config(),
        },
    );
    let mut services = HashMap::new();
    services.insert(
        ServiceId("svc1".into()),
        ServiceSpec {
            workload_id: WorkloadId("svc1".into()),
            ip: Ipv4Addr::new(172, 16, 0, 100),
            mac: [0x02, 0x00, 0x00, 0x00, 0x01, 0x00],
            policy: test_service_policy(),
            activation: Some(ActivationSpec {
                idle_timeout: std::time::Duration::from_secs(30),
            }),
        },
    );
    NamespaceSpec {
        network: test_network_config(),
        workloads,
        services,
    }
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

fn svc_id() -> ServiceId {
    ServiceId("svc1".into())
}

fn wl_id() -> WorkloadId {
    WorkloadId("svc1".into())
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

/// Simulate outer-layer scheduling: step the namespace, then for any pod_requests
/// emitted, pick the first active worker and inject LaunchPod.
/// Returns combined output.
fn step_with_scheduling(
    ns: &mut NamespaceStateMachine,
    input: NamespaceInput,
    pod_counter: &mut u64,
) -> NamespaceOutput {
    let mut out = ns.step(input);
    let requests = std::mem::take(&mut out.pod_requests);
    for req in requests {
        // Pick first active worker.
        let wid = ns
            .workers
            .iter()
            .find(|(_, ws)| ws.fabric_status == FabricStatus::Active)
            .map(|(wid, _)| wid.clone());
        if let Some(wid) = wid {
            let pod_id = PodId(format!("pod-{}", *pod_counter));
            *pod_counter += 1;
            let launch_out = ns.step(NamespaceInput::LaunchPod {
                workload_id: req.workload_id,
                worker_id: wid,
                pod_id,
            });
            out.worker_commands.extend(launch_out.worker_commands);
            out.timers_set.extend(launch_out.timers_set);
            out.timers_cancel.extend(launch_out.timers_cancel);
        }
    }
    out
}

/// Trigger reconcile on an active namespace by stepping with UpdateSpec with same spec.
fn reconcile_active_namespace(
    ns: &mut NamespaceStateMachine,
    pod_counter: &mut u64,
) -> NamespaceOutput {
    step_with_scheduling(
        ns,
        NamespaceInput::UpdateSpec {
            client_id: client_id(99),
            spec: ns.spec.clone(),
        },
        pod_counter,
    )
}

/// Helper to get the workload state for the default service.
fn get_workload_state(ns: &NamespaceStateMachine) -> &WorkloadState {
    &ns.workloads[&wl_id()].state
}

/// Helper to get the service state for the default service.
fn get_service_state(ns: &NamespaceStateMachine) -> &ServiceState {
    &ns.services[&svc_id()].state
}

/// Helper to extract pod_id from a workload in Launching state.
fn get_launching_pod_id(ns: &NamespaceStateMachine) -> PodId {
    match get_workload_state(ns) {
        WorkloadState::Launching { pod_id, .. } => pod_id.clone(),
        other => panic!("expected Launching, got {:?}", other),
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
            && matches!(cmd, WorkerCommand::CreateNamespace { namespace_id, .. } if *namespace_id == ns_id("ns1"))
    }));
}

// --- Namespace State Machine Tests ---

#[test]
fn test_namespace_new_initializes_services() {
    let spec = test_spec();
    let ns = NamespaceStateMachine::new(ns_id("test"), spec);

    assert_eq!(ns.status, NamespaceStatus::Creating);
    assert_eq!(ns.services.len(), 1);
    assert!(ns.services.contains_key(&svc_id()));
    assert!(matches!(get_service_state(&ns), ServiceState::Pending));
    assert!(matches!(get_workload_state(&ns), WorkloadState::Dormant));
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
    assert!(out.destroyed);
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
    let mut pod_counter = 0u64;

    // Reconcile to get the service launched.
    reconcile_active_namespace(&mut ns, &mut pod_counter);
    let pod_id = get_launching_pod_id(&ns);
    ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::PodRunning { pod_id: pod_id.clone() },
    });
    assert!(matches!(get_workload_state(&ns), WorkloadState::Running { .. }));

    // Update spec with no services/workloads — svc1 should be removed.
    let empty_spec = NamespaceSpec {
        network: test_network_config(),
        workloads: HashMap::new(),
        services: HashMap::new(),
    };
    let out = ns.step(NamespaceInput::UpdateSpec {
        client_id: client_id(1),
        spec: empty_spec,
    });

    assert!(ns.services.is_empty());
    assert!(ns.workloads.is_empty());
    assert!(ns.pods.is_empty());
    assert!(out.worker_commands.iter().any(|(_, cmd)| matches!(
        cmd,
        WorkerCommand::StopPod { .. }
    )));
}

#[test]
fn test_update_spec_removes_launching_service() {
    let mut ns = active_namespace(test_spec());
    let mut pod_counter = 0u64;

    // Reconcile to get the service launching.
    reconcile_active_namespace(&mut ns, &mut pod_counter);
    assert!(matches!(get_workload_state(&ns), WorkloadState::Launching { .. }));

    // Update spec with no services — should stop the launching pod and cancel the launch timer.
    let empty_spec = NamespaceSpec {
        network: test_network_config(),
        workloads: HashMap::new(),
        services: HashMap::new(),
    };
    let out = ns.step(NamespaceInput::UpdateSpec {
        client_id: client_id(1),
        spec: empty_spec,
    });

    assert!(ns.services.is_empty());
    assert!(ns.workloads.is_empty());
    assert!(ns.pods.is_empty());
    assert!(out.worker_commands.iter().any(|(_, cmd)| matches!(
        cmd,
        WorkerCommand::StopPod { .. }
    )));
    assert!(!out.timers_cancel.is_empty());
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
    let out = ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::PodRunning {
            pod_id: pod_id.clone(),
        },
    });
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
    });
    assert!(!out.timers_set.is_empty());
    let idle_timer = out.timers_set[0].0.clone();

    // Idle timer fires → service back to Idle, workload goes Dormant.
    let out = ns.step(NamespaceInput::TimerFired {
        timer_key: idle_timer,
    });
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
    });
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
    });
    assert!(matches!(
        get_workload_state(&ns),
        WorkloadState::Running { .. }
    ));

    // Worker lost → service should go to Idle, workload to Dormant, namespace to Creating.
    let _out = ns.step(NamespaceInput::WorkerLost {
        worker_id: worker_id(1),
    });
    assert!(matches!(get_service_state(&ns), ServiceState::Idle));
    assert!(matches!(get_workload_state(&ns), WorkloadState::Dormant));
    assert_eq!(ns.status, NamespaceStatus::Creating);
    assert!(ns.workers.is_empty());
    assert!(ns.pods.is_empty());
}

#[test]
fn test_launch_timeout() {
    let mut ns = active_namespace(test_spec_with_activation());
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

    // Launch timeout fires → should stop pod and go to Idle (activation service).
    let out = ns.step(NamespaceInput::TimerFired {
        timer_key: launch_timeout,
    });
    assert!(matches!(get_service_state(&ns), ServiceState::Idle));
    assert!(matches!(get_workload_state(&ns), WorkloadState::Dormant));
    assert!(!ns.pods.contains_key(&pod_id));
    assert!(out
        .worker_commands
        .iter()
        .any(|(_, cmd)| matches!(cmd, WorkerCommand::StopPod { .. })));
}

#[test]
fn test_delete_single_worker_stateful() {
    let mut ns = active_namespace(test_spec());
    let mut pod_counter = 0u64;

    // Get to Active with a running pod.
    reconcile_active_namespace(&mut ns, &mut pod_counter);
    let pod_id = get_launching_pod_id(&ns);
    ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::PodRunning { pod_id },
    });

    // Delete — stateful: workers stay in map with Destroying status.
    let out = ns.step(NamespaceInput::Delete {
        client_id: client_id(1),
    });
    assert_eq!(ns.status, NamespaceStatus::Destroying);
    assert!(!out.destroyed); // Not destroyed yet, waiting for worker confirmation.
    assert!(!ns.workers.is_empty());
    assert!(ns.pods.is_empty()); // Pods cleared.
    assert!(ns.workers[&worker_id(1)].fabric_status == FabricStatus::Destroying);
    assert!(out
        .worker_commands
        .iter()
        .any(|(_, cmd)| matches!(cmd, WorkerCommand::DestroyNamespace { .. })));

    // Worker confirms destruction.
    let out = ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::NamespaceDestroyed,
    });
    assert!(out.destroyed);
    assert!(ns.workers.is_empty());
}

#[test]
fn test_delete_multi_worker_stateful() {
    let mut ns = active_namespace(test_spec());
    // Add a second worker.
    ns.workers.insert(
        worker_id(2),
        NamespaceWorkerState {
            fabric_status: FabricStatus::Active,
            pods: std::collections::HashSet::new(),
        },
    );

    // Delete.
    let out = ns.step(NamespaceInput::Delete {
        client_id: client_id(1),
    });
    assert_eq!(ns.status, NamespaceStatus::Destroying);
    assert!(!out.destroyed);
    assert_eq!(ns.workers.len(), 2);

    // First worker confirms.
    let out = ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::NamespaceDestroyed,
    });
    assert!(!out.destroyed); // Still waiting for worker-2.
    assert_eq!(ns.workers.len(), 1);

    // Second worker confirms.
    let out = ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(2),
        event: WorkerEvent::NamespaceDestroyed,
    });
    assert!(out.destroyed);
    assert!(ns.workers.is_empty());
}

#[test]
fn test_delete_worker_lost_during_teardown() {
    let mut ns = active_namespace(test_spec());
    let mut pod_counter = 0u64;

    reconcile_active_namespace(&mut ns, &mut pod_counter);
    let pod_id = get_launching_pod_id(&ns);
    ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::PodRunning { pod_id },
    });

    // Delete.
    ns.step(NamespaceInput::Delete {
        client_id: client_id(1),
    });
    assert_eq!(ns.status, NamespaceStatus::Destroying);

    // Worker disconnects instead of confirming.
    let out = ns.step(NamespaceInput::WorkerLost {
        worker_id: worker_id(1),
    });
    assert!(out.destroyed);
    assert!(ns.workers.is_empty());
}

#[test]
fn test_delete_no_workers_immediate() {
    // Namespace with no workers -> immediate destroy.
    let mut ns = NamespaceStateMachine::new(ns_id("test"), test_spec());
    ns.status = NamespaceStatus::Active;

    let out = ns.step(NamespaceInput::Delete {
        client_id: client_id(1),
    });
    assert_eq!(ns.status, NamespaceStatus::Destroying);
    assert!(out.destroyed);
    assert!(ns.workers.is_empty());
}

#[test]
fn test_idle_timer_cancelled_on_traffic() {
    let mut ns = active_namespace(test_spec_with_activation());
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
    });

    // BackendNeed::None → idle timer set.
    let out = ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::ServiceBackendNeed {
            service_id: svc_id(),
            need: BackendNeed::None,
        },
    });
    assert!(!out.timers_set.is_empty());

    // BackendNeed::Traffic → idle timer cancelled.
    let out = ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::ServiceBackendNeed {
            service_id: svc_id(),
            need: BackendNeed::Traffic,
        },
    });
    assert!(!out.timers_cancel.is_empty());
}

// --- Destroy Lifecycle Tests ---

#[test]
fn test_destroying_namespace_ignores_activation() {
    let mut ns = active_namespace(test_spec_with_activation());
    let mut pod_counter = 0u64;

    // Reconcile to get service to Idle.
    reconcile_active_namespace(&mut ns, &mut pod_counter);
    assert!(matches!(get_service_state(&ns), ServiceState::Idle));

    // Delete namespace.
    ns.step(NamespaceInput::Delete {
        client_id: client_id(1),
    });
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
    });
    assert!(out.pod_requests.is_empty());
    assert!(out.worker_commands.is_empty());
}

#[test]
fn test_update_spec_rejected_during_teardown() {
    let mut ns = active_namespace(test_spec());
    ns.step(NamespaceInput::Delete {
        client_id: client_id(1),
    });
    assert_eq!(ns.status, NamespaceStatus::Destroying);

    let out = ns.step(NamespaceInput::UpdateSpec {
        client_id: client_id(2),
        spec: test_spec(),
    });
    assert!(out.client_events.iter().any(|(_, ev)| matches!(
        ev,
        ClientEvent::Error { .. }
    )));
}

#[test]
fn test_stateful_destroy_with_running_pod() {
    let mut ns = active_namespace(test_spec());
    let mut pod_counter = 0u64;

    // Get service to Active.
    reconcile_active_namespace(&mut ns, &mut pod_counter);
    let pod_id = get_launching_pod_id(&ns);
    ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::PodRunning {
            pod_id: pod_id.clone(),
        },
    });
    assert!(matches!(get_workload_state(&ns), WorkloadState::Running { .. }));

    // Delete namespace — stateful: not immediately destroyed.
    let out = ns.step(NamespaceInput::Delete {
        client_id: client_id(1),
    });
    assert_eq!(ns.status, NamespaceStatus::Destroying);
    assert!(!out.destroyed);
    assert!(!ns.workers.is_empty());
    assert!(ns.pods.is_empty()); // Pods cleared, workloads reset.
    assert!(matches!(get_workload_state(&ns), WorkloadState::Dormant));

    // Worker confirms.
    let out = ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::NamespaceDestroyed,
    });
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
    });
    assert!(matches!(
        get_workload_state(&ns),
        WorkloadState::Running { .. }
    ));

    // NamespaceFailed should act like worker loss.
    ns.step(NamespaceInput::WorkerEvent {
        worker_id: worker_id(1),
        event: WorkerEvent::NamespaceFailed {
            error: "gateway crashed".into(),
        },
    });
    assert!(ns.workers.is_empty());
    assert!(ns.pods.is_empty());
    assert!(matches!(get_service_state(&ns), ServiceState::Idle));
    assert!(matches!(get_workload_state(&ns), WorkloadState::Dormant));
    assert_eq!(ns.status, NamespaceStatus::Creating);
}

// --- DestroyService Tests ---

#[test]
fn test_destroy_service_on_spec_update() {
    let mut ns = active_namespace(test_spec_with_activation());
    let mut pod_counter = 0u64;

    // Reconcile to get service to Idle (CreateService sent).
    reconcile_active_namespace(&mut ns, &mut pod_counter);
    assert!(matches!(get_service_state(&ns), ServiceState::Idle));

    // Update spec with no services — should emit DestroyService.
    let empty_spec = NamespaceSpec {
        network: test_network_config(),
        workloads: HashMap::new(),
        services: HashMap::new(),
    };
    let out = ns.step(NamespaceInput::UpdateSpec {
        client_id: client_id(1),
        spec: empty_spec,
    });

    assert!(out.worker_commands.iter().any(|(_, cmd)| matches!(
        cmd,
        WorkerCommand::DestroyService { .. }
    )));
}

// --- RegistrySync Tests ---

#[test]
fn test_registry_sync_on_namespace_active() {
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

    // Reconcile — workload should go to WaitingForCapacity with a PodRequest.
    let out = ns.step(NamespaceInput::UpdateSpec {
        client_id: client_id(99),
        spec: ns.spec.clone(),
    });
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
    });
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
    let mut ns = NamespaceStateMachine::new(ns_id("test"), test_spec());

    // Make namespace Active with no workers.
    ns.status = NamespaceStatus::Active;

    // Reconcile — workload should emit PodRequest but stay WaitingForCapacity.
    let out = ns.step(NamespaceInput::UpdateSpec {
        client_id: client_id(99),
        spec: ns.spec.clone(),
    });
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
            pods: std::collections::HashSet::new(),
        },
    );
    let pod_id = PodId("pod-0".into());
    let out = ns.step(NamespaceInput::LaunchPod {
        workload_id: wl_id(),
        worker_id: worker_id(1),
        pod_id: pod_id.clone(),
    });
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
