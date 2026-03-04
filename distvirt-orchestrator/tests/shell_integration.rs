use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::time::Duration;

use distvirt_orchestrator::shell::OrchestratorShell;
use distvirt_orchestrator::types::*;
use distvirt_worker_protocol::{
    ContainerConfig, ContainerSpec, OrchestratorConnection, PodNetworkConfig, ServicePolicy,
    WorkerCapabilities, WorkerCommand, WorkerConnection, WorkerEvent, WorkerHello,
};

/// Mock worker command loop. Runs after the handshake is complete.
async fn mock_worker_loop(mut conn: WorkerConnection) {
    loop {
        match conn.recv_command().await {
            Ok(WorkerCommand::CreateNamespace { namespace_id, .. }) => {
                conn.send_event(&WorkerEvent::NamespaceCreated { namespace_id })
                    .await
                    .unwrap();
            }
            Ok(WorkerCommand::LaunchPod {
                namespace_id,
                pod_id,
                ..
            }) => {
                conn.send_event(&WorkerEvent::PodRunning {
                    namespace_id,
                    pod_id,
                })
                .await
                .unwrap();
            }
            Ok(WorkerCommand::StopPod {
                namespace_id,
                pod_id,
                ..
            }) => {
                conn.send_event(&WorkerEvent::PodExited {
                    namespace_id,
                    pod_id,
                    exit_code: 0,
                })
                .await
                .unwrap();
            }
            Ok(WorkerCommand::Shutdown) => {
                conn.send_event(&WorkerEvent::ShuttingDown).await.unwrap();
                break;
            }
            Ok(_) => { /* no-op for RegistrySync, CreateService, etc. */ }
            Err(_) => break,
        }
    }
}

/// Perform the worker-side handshake on an accepted connection.
async fn mock_worker_handshake(conn: &mut WorkerConnection) {
    conn.send_hello(&WorkerHello {
        auth_token: "test".to_string(),
        capabilities: WorkerCapabilities {
            has_kvm: true,
            has_containerd: true,
            available_adapters: vec![],
            max_pods: 10,
            available_memory_mb: 1024,
            public_endpoint: String::new(),
        },
    })
    .await
    .unwrap();
    let _accepted = conn.recv_accepted().await.unwrap();
    conn.send_ready().await.unwrap();
}

fn test_spec() -> NamespaceSpec {
    let wl_id = WorkloadId("echo".to_string());
    let svc_id = ServiceId::from("echo-svc");

    let mut workloads = HashMap::new();
    workloads.insert(
        wl_id.clone(),
        WorkloadSpec {
            containers: vec![ContainerSpec {
                container_id: "main".to_string(),
                image_ref: "docker.io/library/alpine:latest".to_string(),
                config: ContainerConfig {
                    entrypoint: vec!["/bin/echo".to_string()],
                    args: vec!["hello".to_string()],
                    env: vec![],
                    working_dir: None,
                    uid: None,
                    gid: None,
                    hostname: None,
                    capture_output: false,
                    stdin: false,
                },
            }],
            network: PodNetworkConfig {
                ip: Ipv4Addr::new(172, 16, 0, 10),
                mac: [0x02, 0x00, 0x00, 0x00, 0x00, 0x10],
                gateway: Ipv4Addr::new(172, 16, 0, 1),
                netmask: "255.255.255.0".to_string(),
            },
            suspend_on_idle: false,
        },
    );

    let mut services = HashMap::new();
    services.insert(
        svc_id,
        ServiceSpec {
            workload_id: wl_id,
            ip: Ipv4Addr::new(172, 16, 0, 100),
            mac: [0x02, 0x00, 0x00, 0x00, 0x01, 0x00],
            policy: ServicePolicy {
                buffer_frames: 100,
                timeout_ms: 5000,
                activator: None,
            },
            activation: None, // always-on
        },
    );

    NamespaceSpec {
        network: distvirt_worker_protocol::NetworkConfig {
            subnet: Ipv4Addr::new(172, 16, 0, 0),
            gateway: Ipv4Addr::new(172, 16, 0, 1),
            prefix_len: 24,
        },
        workloads,
        services,
    }
}

#[tokio::test]
async fn test_always_on_service_full_lifecycle() {
    let (orch_half, worker_half) = tokio::io::duplex(64 * 1024);

    // Spawn mock worker — accept + handshake + command loop.
    // Must run concurrently because yamux needs both sides driving
    // the connection for the handshake to complete.
    let _worker_handle = tokio::spawn(async move {
        let mut conn = WorkerConnection::accept(worker_half).await.unwrap();
        mock_worker_handshake(&mut conn).await;
        mock_worker_loop(conn).await;
    });

    // Connect orchestrator side and perform handshake.
    let orch_conn = OrchestratorConnection::connect(orch_half).await.unwrap();

    let mut shell = OrchestratorShell::new(51820);
    let worker_id = shell.add_worker(orch_conn).await.unwrap();
    assert_eq!(worker_id, WorkerId::from("w-1"));

    // Create namespace — this produces a CreateNamespace command which is
    // the FIRST write to the control stream. The yamux SYN frame is sent,
    // the mock worker's accept() completes, and the protocol starts flowing.
    shell
        .client_command(
            ClientId(1),
            ClientCommand::CreateNamespace {
                namespace_id: NamespaceId::from("ns-test"),
                spec: test_spec(),
            },
        )
        .await;

    // Let the mock worker respond and events propagate.
    // Flow: CreateNamespace -> NamespaceCreated -> (reconcile: RegistrySync + CreateService + LaunchPod)
    //       -> PodRunning -> (UpdateServiceBackend + ServiceReady)
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        shell.drain().await;

        // Early exit once stable.
        let orch = shell.orchestrator();
        if let Some(ns) = orch.namespaces.get(&NamespaceId::from("ns-test")) {
            let wl = ns.workloads.get(&WorkloadId("echo".to_string()));
            if matches!(wl.map(|w| &w.state), Some(WorkloadState::Running { .. })) {
                break;
            }
        }
    }

    // Assert orchestrator state.
    let orch = shell.orchestrator();
    let ns = orch
        .namespaces
        .get(&NamespaceId::from("ns-test"))
        .expect("namespace should exist");

    assert_eq!(
        ns.status,
        NamespaceStatus::Active,
        "namespace should be Active"
    );

    let wl = ns
        .workloads
        .get(&WorkloadId("echo".to_string()))
        .expect("workload should exist");
    assert!(
        matches!(wl.state, WorkloadState::Running { .. }),
        "workload should be Running, got {:?}",
        wl.state
    );

    let svc = ns
        .services
        .get(&ServiceId::from("echo-svc"))
        .expect("service should exist");
    assert!(
        matches!(svc.state, ServiceState::Active { .. }),
        "service should be Active, got {:?}",
        svc.state
    );
}
