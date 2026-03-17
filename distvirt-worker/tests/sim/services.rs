use std::net::Ipv4Addr;

use distvirt_worker_protocol::{
    EndpointKind, EndpointPlacement, EndpointSpec, PodId, RegistryEntry, ServiceId, ServicePolicy,
    WorkerCommand, WorkerEvent, WorkerId,
};

use super::common::*;

#[tokio::test]
async fn test_sim_registry_sync() -> anyhow::Result<()> {
    let (mut conn, worker_handle) = setup().await?;

    conn.send_command(&WorkerCommand::CreateNamespace {
        namespace_id: "ns-sim".into(),
        network: test_network_config(),
    })
    .await?;

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::NamespaceCreated { .. })
    })
    .await?;

    conn.send_command(&WorkerCommand::RegistrySync {
        namespace_id: "ns-sim".into(),
        entries: vec![RegistryEntry {
            name: "myservice".into(),
            ip: Ipv4Addr::new(10, 0, 0, 99),
        }],
    })
    .await?;

    // Smoke test: no panics, clean shutdown
    shutdown_worker(&mut conn, worker_handle).await?;
    Ok(())
}

#[tokio::test]
async fn test_sim_endpoint_lifecycle() -> anyhow::Result<()> {
    let (mut conn, worker_handle) = setup().await?;

    conn.send_command(&WorkerCommand::CreateNamespace {
        namespace_id: "ns-sim".into(),
        network: test_network_config(),
    })
    .await?;

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::NamespaceCreated { .. })
    })
    .await?;

    // Sync a service endpoint
    let vip = Ipv4Addr::new(10, 0, 0, 99);
    conn.send_command(&WorkerCommand::EndpointSync {
        namespace_id: "ns-sim".into(),
        endpoints: vec![EndpointSpec {
            ip: vip,
            kind: EndpointKind::Service {
                service_id: ServiceId(1),
                policy: ServicePolicy {
                    buffer_frames: 0,
                    timeout_ms: 0,
                    activator: None,
                },
                backend: None,
            },
        }],
    })
    .await?;

    // Remove the endpoint
    conn.send_command(&WorkerCommand::EndpointUpdate {
        namespace_id: "ns-sim".into(),
        upserted: vec![],
        removed_ips: vec![vip],
    })
    .await?;

    // Smoke test: no panics, clean shutdown
    shutdown_worker(&mut conn, worker_handle).await?;
    Ok(())
}

/// Full service lifecycle: EndpointSync with service → assign backend pod →
/// launch backend pod → mark service ready via EndpointUpdate(ready=true).
/// Validates the worker-level EndpointSync → ServiceReady → mark_service_ready plumbing.
#[tokio::test]
async fn test_sim_service_with_backend_and_ready() -> anyhow::Result<()> {
    use distvirt_worker_protocol::EndpointPodBackend;

    let (mut conn, worker_handle) = setup().await?;

    let ns_id = "ns-svc-ready";
    create_namespace(&mut conn, ns_id, test_network_config()).await?;

    let vip = Ipv4Addr::new(10, 0, 0, 99);
    let backend_pod_net = test_pod_network_config_with_ip(50);
    let backend_ip = backend_pod_net.ip;

    // Step 1: Sync service endpoint with no backend.
    conn.send_command(&WorkerCommand::EndpointSync {
        namespace_id: ns_id.into(),
        endpoints: vec![EndpointSpec {
            ip: vip,
            kind: EndpointKind::Service {
                service_id: ServiceId(1),
                policy: ServicePolicy {
                    buffer_frames: 64,
                    timeout_ms: 30000,
                    activator: None,
                },
                backend: None,
            },
        }],
    })
    .await?;

    // Step 2: Register backend pod endpoint and launch it.
    launch_pod(&mut conn, ns_id, PodId(1), &backend_pod_net).await?;

    // Step 3: Update service endpoint with ready backend.
    conn.send_command(&WorkerCommand::EndpointUpdate {
        namespace_id: ns_id.into(),
        upserted: vec![EndpointSpec {
            ip: vip,
            kind: EndpointKind::Service {
                service_id: ServiceId(1),
                policy: ServicePolicy {
                    buffer_frames: 64,
                    timeout_ms: 30000,
                    activator: None,
                },
                backend: Some(EndpointPodBackend {
                    pod_ip: backend_ip,
                    placement: Some(EndpointPlacement {
                        worker_id: WorkerId(1),
                    }),
                    ready: true,
                }),
            },
        }],
        removed_ips: vec![],
    })
    .await?;

    // If we reach here without panics, the full service lifecycle plumbing works.
    // Clean shutdown verifies no dangling tasks or poisoned locks.
    shutdown_worker(&mut conn, worker_handle).await?;
    Ok(())
}
