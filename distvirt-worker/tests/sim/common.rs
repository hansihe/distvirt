use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::time::Duration;

use distvirt_worker_protocol::{
    ContainerConfig, ContainerSpec, EndpointKind, EndpointPlacement, EndpointSpec, NetworkConfig,
    OrchestratorConnection, PodNetworkConfig, WorkerAccepted, WorkerCommand, WorkerConnection,
    WorkerEvent, WorkerId,
};

use distvirt_worker::image_provider::stub::StubImageProvider;
use distvirt_worker::vmm::guest_sim::ContainerBehavior;
use distvirt_worker::vmm::test_vmm::{CrashHandle, TestVmm};
use tokio::sync::mpsc;

pub const EVENT_TIMEOUT: Duration = Duration::from_secs(10);

pub async fn setup() -> anyhow::Result<(
    OrchestratorConnection,
    tokio::task::JoinHandle<anyhow::Result<()>>,
)> {
    setup_with_behavior(ContainerBehavior::ExitImmediately(0)).await
}

pub async fn setup_with_behavior(
    behavior: ContainerBehavior,
) -> anyhow::Result<(
    OrchestratorConnection,
    tokio::task::JoinHandle<anyhow::Result<()>>,
)> {
    let _ = env_logger::try_init();

    let vmm = TestVmm::new(behavior);
    let image_provider = StubImageProvider;

    let (orch_half, worker_half) = tokio::io::duplex(64 * 1024);

    let worker_handle = tokio::spawn(async move {
        let conn = WorkerConnection::accept(worker_half).await.unwrap();
        let worker = distvirt_worker::worker::Worker::<_, _, _, distvirt_worker::SyncFs>::new(
            PathBuf::from("/dev/null"),
            PathBuf::from("/dev/null"),
            vmm,
            image_provider,
            None,
            String::new(),
            distvirt_worker::sim_traffic::SimGatewayProvider::new(),
        );
        worker.run(conn, "test-secret".to_string()).await
    });

    let mut conn = OrchestratorConnection::connect(orch_half).await?;

    let _hello = conn.recv_hello().await?;
    conn.send_accepted(&WorkerAccepted {
        worker_id: WorkerId::from("sim-worker"),
        adapters: vec![],
        tunnel_encrypted: false,
        pools: vec![],
    })
    .await?;
    let _ready = conn.recv_ready().await?;

    Ok((conn, worker_handle))
}

pub async fn recv_event_timeout(
    conn: &mut OrchestratorConnection,
    timeout: Duration,
) -> anyhow::Result<WorkerEvent> {
    tokio::time::timeout(timeout, conn.recv_event())
        .await
        .map_err(|_| anyhow::anyhow!("timed out waiting for event"))?
}

pub async fn recv_until<F: Fn(&WorkerEvent) -> bool>(
    conn: &mut OrchestratorConnection,
    timeout: Duration,
    pred: F,
) -> anyhow::Result<WorkerEvent> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline - tokio::time::Instant::now();
        let event = recv_event_timeout(conn, remaining).await?;
        if pred(&event) {
            return Ok(event);
        }
        eprintln!("(skipping event: {:?})", event);
    }
}

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

pub async fn shutdown_worker(
    conn: &mut OrchestratorConnection,
    worker_handle: tokio::task::JoinHandle<anyhow::Result<()>>,
) -> anyhow::Result<()> {
    conn.send_command(&WorkerCommand::Shutdown).await?;

    match tokio::time::timeout(SHUTDOWN_TIMEOUT, worker_handle).await {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(e))) => panic!("worker exited with error: {:#}", e),
        Ok(Err(e)) => panic!("worker task panicked: {}", e),
        Err(_) => panic!("worker did not shut down within {:?}", SHUTDOWN_TIMEOUT),
    }
}

pub fn test_network_config() -> NetworkConfig {
    NetworkConfig {
        subnet: Ipv4Addr::new(10, 0, 0, 0),
        gateway: Ipv4Addr::new(10, 0, 0, 1),
        prefix_len: 24,
        segment_id: None,
    }
}

pub fn test_pod_network_config() -> PodNetworkConfig {
    PodNetworkConfig {
        ip: Ipv4Addr::new(10, 0, 0, 2),
        mac: [0x02, 0x00, 0x00, 0x00, 0x00, 0x01],
        gateway: Ipv4Addr::new(10, 0, 0, 1),
        netmask: "255.255.255.0".into(),
    }
}

pub fn test_pod_network_config_with_ip(last_octet: u8) -> PodNetworkConfig {
    PodNetworkConfig {
        ip: Ipv4Addr::new(10, 0, 0, last_octet),
        mac: [0x02, 0x00, 0x00, 0x00, 0x00, last_octet],
        gateway: Ipv4Addr::new(10, 0, 0, 1),
        netmask: "255.255.255.0".into(),
    }
}

pub fn test_network_config_with_subnet(third_octet: u8) -> NetworkConfig {
    NetworkConfig {
        subnet: Ipv4Addr::new(10, 0, third_octet, 0),
        gateway: Ipv4Addr::new(10, 0, third_octet, 1),
        prefix_len: 24,
        segment_id: None,
    }
}

pub fn test_pod_network_config_for_subnet(third_octet: u8, last_octet: u8) -> PodNetworkConfig {
    PodNetworkConfig {
        ip: Ipv4Addr::new(10, 0, third_octet, last_octet),
        mac: [0x02, 0x00, 0x00, third_octet, 0x00, last_octet],
        gateway: Ipv4Addr::new(10, 0, third_octet, 1),
        netmask: "255.255.255.0".into(),
    }
}

pub async fn create_namespace(
    conn: &mut OrchestratorConnection,
    ns_id: &str,
    network: NetworkConfig,
) -> anyhow::Result<()> {
    conn.send_command(&WorkerCommand::CreateNamespace {
        namespace_id: ns_id.into(),
        network,
    })
    .await?;

    recv_until(
        conn,
        EVENT_TIMEOUT,
        |e| matches!(e, WorkerEvent::NamespaceCreated { namespace_id } if namespace_id == ns_id),
    )
    .await?;

    Ok(())
}

pub async fn launch_pod(
    conn: &mut OrchestratorConnection,
    ns_id: &str,
    pod_id: &str,
    pod_net: &PodNetworkConfig,
) -> anyhow::Result<()> {
    register_pod_endpoint(conn, ns_id, pod_net, "sim-worker").await?;

    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: ns_id.into(),
        pod_id: pod_id.into(),
        network: pod_net.clone(),
        containers: vec![ContainerSpec {
            container_id: "ctr-sim".into(),
            image_ref: "stub://ignored".into(),
            config: ContainerConfig {
                entrypoint: vec!["/bin/true".into()],
                args: vec![],
                env: vec![],
                working_dir: None,
                uid: None,
                gid: None,
                hostname: None,
                capture_output: true,
                stdin: false,
            },
        }],
        resources: None,
    })
    .await?;

    recv_until(conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodRunning { namespace_id, pod_id: pid }
            if namespace_id == ns_id && pid == pod_id)
    })
    .await?;

    Ok(())
}

pub async fn setup_with_crash_handles(
    behavior: ContainerBehavior,
) -> anyhow::Result<(
    OrchestratorConnection,
    tokio::task::JoinHandle<anyhow::Result<()>>,
    mpsc::UnboundedReceiver<CrashHandle>,
)> {
    let _ = env_logger::try_init();

    let (vmm, crash_handle_rx) = TestVmm::with_crash_handles(behavior);
    let image_provider = StubImageProvider;

    let (orch_half, worker_half) = tokio::io::duplex(64 * 1024);

    let worker_handle = tokio::spawn(async move {
        let conn = WorkerConnection::accept(worker_half).await.unwrap();
        let worker = distvirt_worker::worker::Worker::<_, _, _, distvirt_worker::SyncFs>::new(
            PathBuf::from("/dev/null"),
            PathBuf::from("/dev/null"),
            vmm,
            image_provider,
            None,
            String::new(),
            distvirt_worker::sim_traffic::SimGatewayProvider::new(),
        );
        worker.run(conn, "test-secret".to_string()).await
    });

    let mut conn = OrchestratorConnection::connect(orch_half).await?;

    let _hello = conn.recv_hello().await?;
    conn.send_accepted(&WorkerAccepted {
        worker_id: WorkerId::from("sim-worker"),
        adapters: vec![],
        tunnel_encrypted: false,
        pools: vec![],
    })
    .await?;
    let _ready = conn.recv_ready().await?;

    Ok((conn, worker_handle, crash_handle_rx))
}

pub async fn register_pod_endpoint(
    conn: &mut OrchestratorConnection,
    namespace_id: &str,
    pod_network: &PodNetworkConfig,
    worker_id: &str,
) -> anyhow::Result<()> {
    conn.send_command(&WorkerCommand::EndpointUpdate {
        namespace_id: namespace_id.into(),
        upserted: vec![EndpointSpec {
            ip: pod_network.ip,
            kind: EndpointKind::Pod {
                placement: Some(EndpointPlacement {
                    worker_id: WorkerId::from(worker_id),
                }),
            },
        }],
        removed_ips: vec![],
    })
    .await?;
    Ok(())
}
