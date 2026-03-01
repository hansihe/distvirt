use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::time::Duration;

use distvirt_orchestrator::shell::OrchestratorShell;
use distvirt_orchestrator::types::*;
use distvirt_worker_protocol::{
    ContainerConfig, ContainerSpec, OrchestratorConnection, PodNetworkConfig, ServicePolicy,
    WorkerCommand, WorkerConnection, WorkerEvent,
};

/// Mock worker that auto-responds to commands.
/// Accepts the connection internally so it can be spawned before connect().
async fn mock_worker(
    transport: impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static,
) {
    let mut conn = WorkerConnection::accept(transport).await.unwrap();
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
                    entrypoint: "/bin/echo".to_string(),
                    args: vec!["hello".to_string()],
                    env: vec![],
                    working_dir: None,
                    uid: None,
                    gid: None,
                    hostname: None,
                    capture_output: false,
                },
            }],
            network: PodNetworkConfig {
                ip: Ipv4Addr::new(172, 16, 0, 10),
                mac: [0x02, 0x00, 0x00, 0x00, 0x00, 0x10],
                gateway: Ipv4Addr::new(172, 16, 0, 1),
                netmask: "255.255.255.0".to_string(),
            },
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

    // Spawn mock worker with accept inside — yamux only sends the stream-open
    // frame when data is first written, so accept must run concurrently with
    // the first send_command.
    let _worker_handle = tokio::spawn(mock_worker(worker_half));

    // Connect orchestrator side (yamux Client opens outbound control stream,
    // but the SYN frame is deferred until the first write).
    let orch_conn = OrchestratorConnection::connect(orch_half).await.unwrap();

    // Set up shell and add worker. WorkerConnected produces no commands,
    // so nothing is written to the control stream yet.
    let mut shell = OrchestratorShell::new();
    shell
        .add_worker(
            WorkerId::from("w-1"),
            WorkerCapabilities {
                max_pods: 10,
                available_memory_mb: 1024,
            },
            orch_conn,
        )
        .await;

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
