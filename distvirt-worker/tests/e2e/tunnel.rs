use std::net::Ipv4Addr;
use std::path::PathBuf;

use distvirt_worker_protocol::{
    ContainerConfig, ContainerSpec, EndpointKind, EndpointPlacement, EndpointSpec, NetworkConfig,
    OrchestratorConnection, PodId, PodNetworkConfig, WorkerAccepted, WorkerCommand,
    WorkerConnection, WorkerEvent, WorkerHello, WorkerId, WorkerPeerInfo, WorkerReady,
};

use super::common::*;

/// Spawn a worker and return the orchestrator connection, the WorkerHello,
/// the WorkerReady (containing tunnel_listen_port), and the worker task handle.
async fn setup_worker(
    worker_id: WorkerId,
) -> anyhow::Result<(
    OrchestratorConnection,
    WorkerHello,
    WorkerReady,
    tokio::task::JoinHandle<anyhow::Result<()>>,
)> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let kernel = manifest_dir.join("../guest-image/result-kernel/vmlinux");
    let rootfs = manifest_dir.join("../guest-image/result-rootfs");

    assert!(kernel.exists(), "kernel not found at {}", kernel.display());
    assert!(rootfs.exists(), "rootfs not found at {}", rootfs.display());

    let firecracker_bin = std::env::var("FIRECRACKER_BIN").unwrap_or_else(|_| "firecracker".into());
    let vmm = distvirt_worker::vmm::firecracker::Firecracker::new(firecracker_bin);

    let containerd_socket = std::env::var("CONTAINERD_SOCKET")
        .unwrap_or_else(|_| "/run/containerd/containerd.sock".into());
    let image_provider =
        distvirt_worker::image_provider::containerd_overlayfs::ContainerdOverlayfsProvider {
            socket: containerd_socket,
            namespace: "default".into(),
            docker_config: None,
        };

    let (orch_half, worker_half) = tokio::io::duplex(64 * 1024);

    let worker_handle = tokio::spawn(async move {
        let conn = WorkerConnection::accept(worker_half).await.unwrap();
        let worker = distvirt_worker::worker::Worker::<_, _, _, distvirt_worker::TokioFs>::new(
            kernel,
            rootfs,
            vmm,
            image_provider,
            None,
            String::new(),
            distvirt_worker::TunGatewayProvider,
        );
        worker.run(conn, "test-secret".to_string()).await
    });

    let mut conn = OrchestratorConnection::connect(orch_half).await?;

    let hello = conn.recv_hello().await?;
    eprintln!(
        "e2e: worker '{}' capabilities: {:?}",
        worker_id, hello.capabilities
    );

    conn.send_accepted(&WorkerAccepted {
        worker_id,
        adapters: vec![],
        tunnel_encrypted: true,
        pools: vec![],
    })
    .await?;

    let ready = conn.recv_ready().await?;
    eprintln!("e2e: worker '{}' handshake complete", worker_id);

    Ok((conn, hello, ready, worker_handle))
}

/// Test cross-worker traffic over UDP tunnel.
///
/// Two workers each create a namespace on the same segment. Worker-A runs a ping
/// to pod-B's IP, which is routed through the tunnel to worker-B. Worker-B runs a
/// long-lived sleep so the fabric can deliver the ICMP traffic.
#[tokio::test]
async fn test_cross_worker_tunnel_ping() -> anyhow::Result<()> {
    if !should_run() {
        return Ok(());
    }

    let _ = env_logger::try_init();

    // --- Start two workers ---
    let (mut conn_a, _hello_a, ready_a, handle_a) = setup_worker(WorkerId(1)).await?;
    let (mut conn_b, _hello_b, ready_b, handle_b) = setup_worker(WorkerId(2)).await?;

    let port_a = ready_a
        .tunnel_listen_port
        .expect("worker-a should have tunnel_listen_port");
    let port_b = ready_b
        .tunnel_listen_port
        .expect("worker-b should have tunnel_listen_port");
    let pubkey_a = ready_a
        .tunnel_public_key
        .expect("worker-a should have tunnel_public_key");
    let pubkey_b = ready_b
        .tunnel_public_key
        .expect("worker-b should have tunnel_public_key");

    eprintln!(
        "e2e: worker-a tunnel port={}, worker-b tunnel port={}",
        port_a, port_b
    );

    let segment_id = 1u16;

    // --- Create namespaces with same segment_id ---
    conn_a
        .send_command(&WorkerCommand::CreateNamespace {
            namespace_id: "ns-tunnel".into(),
            network: NetworkConfig {
                subnet: Ipv4Addr::new(10, 0, 0, 0),
                gateway: Ipv4Addr::new(10, 0, 0, 1),
                prefix_len: 24,
                segment_id: Some(segment_id),
            },
        })
        .await?;

    conn_b
        .send_command(&WorkerCommand::CreateNamespace {
            namespace_id: "ns-tunnel".into(),
            network: NetworkConfig {
                subnet: Ipv4Addr::new(10, 0, 0, 0),
                gateway: Ipv4Addr::new(10, 0, 0, 1),
                prefix_len: 24,
                segment_id: Some(segment_id),
            },
        })
        .await?;

    recv_until(&mut conn_a, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::NamespaceCreated { .. })
    })
    .await?;
    recv_until(&mut conn_b, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::NamespaceCreated { .. })
    })
    .await?;

    // --- WorkerRegistrySync: tell each worker about the other ---
    conn_a
        .send_command(&WorkerCommand::WorkerRegistrySync {
            workers: vec![WorkerPeerInfo {
                worker_id: WorkerId(2),
                endpoint: format!("127.0.0.1:{}", port_b),
                public_key: pubkey_b,
                segments: vec![segment_id],
            }],
        })
        .await?;

    conn_b
        .send_command(&WorkerCommand::WorkerRegistrySync {
            workers: vec![WorkerPeerInfo {
                worker_id: WorkerId(1),
                endpoint: format!("127.0.0.1:{}", port_a),
                public_key: pubkey_a,
                segments: vec![segment_id],
            }],
        })
        .await?;

    // --- EndpointSync: route pod-B's IP via worker-b on worker-a, and vice versa ---
    let pod_a_ip = Ipv4Addr::new(10, 0, 0, 2);
    let pod_b_ip = Ipv4Addr::new(10, 0, 0, 3);

    conn_a
        .send_command(&WorkerCommand::EndpointSync {
            namespace_id: "ns-tunnel".into(),
            endpoints: vec![EndpointSpec {
                ip: pod_b_ip,
                kind: EndpointKind::Pod {
                    placement: Some(EndpointPlacement {
                        worker_id: WorkerId(2),
                    }),
                },
            }],
        })
        .await?;

    conn_b
        .send_command(&WorkerCommand::EndpointSync {
            namespace_id: "ns-tunnel".into(),
            endpoints: vec![EndpointSpec {
                ip: pod_a_ip,
                kind: EndpointKind::Pod {
                    placement: Some(EndpointPlacement {
                        worker_id: WorkerId(1),
                    }),
                },
            }],
        })
        .await?;

    // Register local pod endpoints before launching
    let pod_b_network = PodNetworkConfig {
        ip: pod_b_ip,
        mac: [0x02, 0x00, 0x00, 0x00, 0x00, 0x03],
        gateway: Ipv4Addr::new(10, 0, 0, 1),
        netmask: "255.255.255.0".into(),
    };
    register_pod_endpoint(&mut conn_b, "ns-tunnel", &pod_b_network, WorkerId(2)).await?;

    // --- Launch pod-B: a long-running process so the fabric has a destination ---
    conn_b
        .send_command(&WorkerCommand::LaunchPod {
            namespace_id: "ns-tunnel".into(),
            pod_id: PodId(2),
            network: PodNetworkConfig {
                ip: pod_b_ip,
                mac: [0x02, 0x00, 0x00, 0x00, 0x00, 0x03],
                gateway: Ipv4Addr::new(10, 0, 0, 1),
                netmask: "255.255.255.0".into(),
            },
            containers: vec![ContainerSpec {
                container_id: "ctr-b".into(),
                image_ref: "docker.io/library/distvirt-test-containers:latest".into(),
                config: ContainerConfig {
                    entrypoint: vec!["/bin/test-containers".into()],
                    args: vec!["sleep".into()],
                    env: vec![],
                    working_dir: None,
                    uid: None,
                    gid: None,
                    hostname: None,
                    capture_output: false,
                    stdin: false,
                    volume_mounts: vec![],
                },
            }],
            resources: None,
            volumes: vec![],
        })
        .await?;

    recv_until(
        &mut conn_b,
        EVENT_TIMEOUT,
        |e| matches!(e, WorkerEvent::PodRunning { pod_id, .. } if *pod_id == PodId(2)),
    )
    .await?;
    eprintln!("e2e: pod-b running on worker-b");

    let pod_a_network = PodNetworkConfig {
        ip: pod_a_ip,
        mac: [0x02, 0x00, 0x00, 0x00, 0x00, 0x02],
        gateway: Ipv4Addr::new(10, 0, 0, 1),
        netmask: "255.255.255.0".into(),
    };
    register_pod_endpoint(&mut conn_a, "ns-tunnel", &pod_a_network, WorkerId(1)).await?;

    // --- Launch pod-A: ping pod-B's IP through the tunnel ---
    conn_a
        .send_command(&WorkerCommand::LaunchPod {
            namespace_id: "ns-tunnel".into(),
            pod_id: PodId(1),
            network: PodNetworkConfig {
                ip: pod_a_ip,
                mac: [0x02, 0x00, 0x00, 0x00, 0x00, 0x02],
                gateway: Ipv4Addr::new(10, 0, 0, 1),
                netmask: "255.255.255.0".into(),
            },
            containers: vec![ContainerSpec {
                container_id: "ctr-a".into(),
                image_ref: "docker.io/library/alpine:latest".into(),
                config: ContainerConfig {
                    entrypoint: vec!["/bin/ping".into()],
                    args: vec![
                        "-c".into(),
                        "3".into(),
                        "-W".into(),
                        "5".into(),
                        pod_b_ip.to_string(),
                    ],
                    env: vec![],
                    working_dir: None,
                    uid: None,
                    gid: None,
                    hostname: None,
                    capture_output: true,
                    stdin: false,
                    volume_mounts: vec![],
                },
            }],
            resources: None,
            volumes: vec![],
        })
        .await?;

    recv_until(
        &mut conn_a,
        EVENT_TIMEOUT,
        |e| matches!(e, WorkerEvent::PodRunning { pod_id, .. } if *pod_id == PodId(1)),
    )
    .await?;
    eprintln!("e2e: pod-a running on worker-a, pinging pod-b");

    // Drain log stream for pod-a output
    let log_str = drain_log_stream(&mut conn_a).await?;
    eprintln!("e2e: pod-a ping output:\n{}", log_str);

    // Wait for pod-a to exit
    let event = recv_until(
        &mut conn_a,
        EVENT_TIMEOUT,
        |e| matches!(e, WorkerEvent::PodExited { pod_id, .. } if *pod_id == PodId(1)),
    )
    .await?;

    // Assert ping succeeded (exit code 0)
    match &event {
        WorkerEvent::PodExited { exit_code, .. } => {
            assert_eq!(
                *exit_code, 0,
                "ping should succeed (exit code 0), got {}. Output:\n{}",
                exit_code, log_str
            );
        }
        other => panic!("expected PodExited, got {:?}", other),
    }

    eprintln!("e2e: cross-worker tunnel ping succeeded!");

    // --- Teardown: force-kill both workers (no need for graceful shutdown in this test) ---
    force_shutdown_worker(handle_a).await;
    force_shutdown_worker(handle_b).await;

    Ok(())
}
