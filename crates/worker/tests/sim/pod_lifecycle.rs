use std::collections::HashSet;

use distvirt_worker::vmm::guest_sim::ContainerBehavior;
use distvirt_worker_protocol::{ContainerConfig, ContainerSpec, PodId, WorkerCommand, WorkerEvent, WorkerId};

use super::common::*;

#[allow(dead_code)]
fn default_container_spec() -> ContainerSpec {
    ContainerSpec {
        container_id: "ctr-sim".into(),
        image_ref: "stub://ignored".into(),
        config: ContainerConfig {
            command: Some(vec!["/bin/true".into()]),
            args: Some(vec![]),
            env: vec![],
            working_dir: None,
            user: None,
            hostname: None,
            capture_output: true,
            stdin: false,
            volume_mounts: vec![],
        },
    }
}

#[tokio::test]
async fn test_sim_pod_lifecycle() -> anyhow::Result<()> {
    let (mut conn, worker_handle, _pool_id) = setup().await?;

    // Create namespace
    conn.send_command(&WorkerCommand::CreateNamespace {
        namespace_id: "ns-sim".into(),
        network: test_network_config(),
    })
    .await?;

    let event = recv_event_timeout(&mut conn, EVENT_TIMEOUT).await?;
    assert!(
        matches!(&event, WorkerEvent::NamespaceCreated { namespace_id } if namespace_id == "ns-sim"),
        "expected NamespaceCreated, got {:?}",
        event
    );

    // Register pod endpoint before launch
    register_pod_endpoint(
        &mut conn,
        "ns-sim",
        &test_pod_network_config(),
        WorkerId(1),
    )
    .await?;

    // Launch pod
    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-sim".into(),
        pod_id: PodId(1),
        network: test_pod_network_config(),
        containers: vec![ContainerSpec {
            container_id: "ctr-sim".into(),
            image_ref: "stub://ignored".into(),
            config: ContainerConfig {
                command: Some(vec!["/bin/true".into()]),
                args: Some(vec![]),
                env: vec![],
                working_dir: None,
                user: None,
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

    // Wait for PodRunning
    let event = recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodRunning { .. })
    })
    .await?;
    assert!(
        matches!(&event, WorkerEvent::PodRunning { namespace_id, pod_id }
            if namespace_id == "ns-sim" && *pod_id == PodId(1)),
        "unexpected event: {:?}",
        event
    );

    // Wait for PodExited
    let event = recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodExited { .. })
    })
    .await?;
    assert!(
        matches!(&event, WorkerEvent::PodExited { exit_code, .. } if *exit_code == 0),
        "expected exit_code 0, got {:?}",
        event
    );

    // Graceful shutdown
    shutdown_worker(&mut conn, worker_handle).await?;

    Ok(())
}

#[tokio::test]
async fn test_sim_pod_exit_code() -> anyhow::Result<()> {
    let (mut conn, worker_handle, _pool_id) =
        setup_with_behavior(ContainerBehavior::ExitImmediately(42)).await?;

    conn.send_command(&WorkerCommand::CreateNamespace {
        namespace_id: "ns-sim".into(),
        network: test_network_config(),
    })
    .await?;

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::NamespaceCreated { .. })
    })
    .await?;

    register_pod_endpoint(
        &mut conn,
        "ns-sim",
        &test_pod_network_config(),
        WorkerId(1),
    )
    .await?;

    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-sim".into(),
        pod_id: PodId(1),
        network: test_pod_network_config(),
        containers: vec![ContainerSpec {
            container_id: "ctr-sim".into(),
            image_ref: "stub://ignored".into(),
            config: ContainerConfig {
                command: Some(vec!["/bin/false".into()]),
                args: Some(vec![]),
                env: vec![],
                working_dir: None,
                user: None,
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

    let event = recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodExited { .. })
    })
    .await?;
    assert!(
        matches!(&event, WorkerEvent::PodExited { exit_code, .. } if *exit_code == 42),
        "expected exit_code 42, got {:?}",
        event
    );

    shutdown_worker(&mut conn, worker_handle).await?;
    Ok(())
}

#[tokio::test]
async fn test_sim_stop_pod_graceful() -> anyhow::Result<()> {
    let (mut conn, worker_handle, _pool_id) =
        setup_with_behavior(ContainerBehavior::RunUntilSignaled).await?;

    conn.send_command(&WorkerCommand::CreateNamespace {
        namespace_id: "ns-sim".into(),
        network: test_network_config(),
    })
    .await?;

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::NamespaceCreated { .. })
    })
    .await?;

    register_pod_endpoint(
        &mut conn,
        "ns-sim",
        &test_pod_network_config(),
        WorkerId(1),
    )
    .await?;

    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-sim".into(),
        pod_id: PodId(1),
        network: test_pod_network_config(),
        containers: vec![ContainerSpec {
            container_id: "ctr-sim".into(),
            image_ref: "stub://ignored".into(),
            config: ContainerConfig {
                command: Some(vec!["/bin/sleep".into()]),
                args: Some(vec!["infinity".into()]),
                env: vec![],
                working_dir: None,
                user: None,
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

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodRunning { .. })
    })
    .await?;

    conn.send_command(&WorkerCommand::StopPod {
        namespace_id: "ns-sim".into(),
        pod_id: PodId(1),
        graceful: true,
    })
    .await?;

    // Graceful stop cancels the supervisor token, which emits PodExited with -1.
    let event = recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodExited { .. })
    })
    .await?;
    assert!(
        matches!(&event, WorkerEvent::PodExited { exit_code, .. } if *exit_code == -1),
        "expected exit_code -1 (cancelled), got {:?}",
        event
    );

    shutdown_worker(&mut conn, worker_handle).await?;
    Ok(())
}

#[tokio::test]
async fn test_sim_stop_pod_force() -> anyhow::Result<()> {
    let (mut conn, worker_handle, _pool_id) =
        setup_with_behavior(ContainerBehavior::RunUntilSignaled).await?;

    conn.send_command(&WorkerCommand::CreateNamespace {
        namespace_id: "ns-sim".into(),
        network: test_network_config(),
    })
    .await?;

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::NamespaceCreated { .. })
    })
    .await?;

    register_pod_endpoint(
        &mut conn,
        "ns-sim",
        &test_pod_network_config(),
        WorkerId(1),
    )
    .await?;

    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-sim".into(),
        pod_id: PodId(1),
        network: test_pod_network_config(),
        containers: vec![ContainerSpec {
            container_id: "ctr-sim".into(),
            image_ref: "stub://ignored".into(),
            config: ContainerConfig {
                command: Some(vec!["/bin/sleep".into()]),
                args: Some(vec!["infinity".into()]),
                env: vec![],
                working_dir: None,
                user: None,
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

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodRunning { .. })
    })
    .await?;

    conn.send_command(&WorkerCommand::StopPod {
        namespace_id: "ns-sim".into(),
        pod_id: PodId(1),
        graceful: false,
    })
    .await?;

    // Force stop aborts the supervisor directly — no PodExited event is emitted.
    // Verify the worker is still healthy by performing a clean shutdown.
    shutdown_worker(&mut conn, worker_handle).await?;
    Ok(())
}

#[tokio::test]
async fn test_sim_destroy_namespace() -> anyhow::Result<()> {
    let (mut conn, worker_handle, _pool_id) =
        setup_with_behavior(ContainerBehavior::RunUntilSignaled).await?;

    conn.send_command(&WorkerCommand::CreateNamespace {
        namespace_id: "ns-sim".into(),
        network: test_network_config(),
    })
    .await?;

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::NamespaceCreated { .. })
    })
    .await?;

    register_pod_endpoint(
        &mut conn,
        "ns-sim",
        &test_pod_network_config(),
        WorkerId(1),
    )
    .await?;

    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-sim".into(),
        pod_id: PodId(1),
        network: test_pod_network_config(),
        containers: vec![ContainerSpec {
            container_id: "ctr-sim".into(),
            image_ref: "stub://ignored".into(),
            config: ContainerConfig {
                command: Some(vec!["/bin/sleep".into()]),
                args: Some(vec!["infinity".into()]),
                env: vec![],
                working_dir: None,
                user: None,
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

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodRunning { .. })
    })
    .await?;

    conn.send_command(&WorkerCommand::DestroyNamespace {
        namespace_id: "ns-sim".into(),
    })
    .await?;

    // Expect pod termination (PodExited or PodFailed)
    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(
            e,
            WorkerEvent::PodExited { .. } | WorkerEvent::PodFailed { .. }
        )
    })
    .await?;

    // Expect NamespaceDestroyed
    let event = recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::NamespaceDestroyed { .. })
    })
    .await?;
    assert!(
        matches!(&event, WorkerEvent::NamespaceDestroyed { namespace_id } if namespace_id == "ns-sim"),
        "expected NamespaceDestroyed for ns-sim, got {:?}",
        event
    );

    shutdown_worker(&mut conn, worker_handle).await?;
    Ok(())
}

// --- Tier 2 tests ---

#[tokio::test]
async fn test_sim_multiple_pods_same_namespace() -> anyhow::Result<()> {
    let (mut conn, worker_handle, _pool_id) = setup().await?;

    create_namespace(&mut conn, "ns-sim", test_network_config()).await?;

    let net1 = test_pod_network_config_with_ip(2);
    let net2 = test_pod_network_config_with_ip(3);

    // Register and launch both pods without waiting between them
    register_pod_endpoint(&mut conn, "ns-sim", &net1, WorkerId(1)).await?;
    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-sim".into(),
        pod_id: PodId(1),
        network: net1,
        containers: vec![default_container_spec()],
        resources: None,
        volumes: vec![],
    })
    .await?;

    register_pod_endpoint(&mut conn, "ns-sim", &net2, WorkerId(1)).await?;
    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-sim".into(),
        pod_id: PodId(2),
        network: net2,
        containers: vec![default_container_spec()],
        resources: None,
        volumes: vec![],
    })
    .await?;

    // Collect 2 PodRunning and 2 PodExited events (in any order)
    let mut running_pods = HashSet::new();
    let mut exited_pods = HashSet::new();
    let deadline = tokio::time::Instant::now() + EVENT_TIMEOUT;
    while running_pods.len() < 2 || exited_pods.len() < 2 {
        let remaining = deadline - tokio::time::Instant::now();
        let event = recv_event_timeout(&mut conn, remaining).await?;
        match &event {
            WorkerEvent::PodRunning { pod_id, .. } => {
                running_pods.insert(*pod_id);
            }
            WorkerEvent::PodExited {
                pod_id, exit_code, ..
            } => {
                assert_eq!(*exit_code, 0, "expected exit_code 0, got {:?}", event);
                exited_pods.insert(*pod_id);
            }
            _ => { /* skip other events like PressureUpdate */ }
        }
    }
    assert!(running_pods.contains(&PodId(1)), "pod-1 should have run");
    assert!(running_pods.contains(&PodId(2)), "pod-2 should have run");
    assert!(exited_pods.contains(&PodId(1)), "pod-1 should have exited");
    assert!(exited_pods.contains(&PodId(2)), "pod-2 should have exited");

    shutdown_worker(&mut conn, worker_handle).await?;
    Ok(())
}

#[tokio::test]
async fn test_sim_multiple_namespaces() -> anyhow::Result<()> {
    let (mut conn, worker_handle, _pool_id) =
        setup_with_behavior(ContainerBehavior::RunUntilSignaled).await?;

    // Create two namespaces with different subnets
    create_namespace(&mut conn, "ns-a", test_network_config_with_subnet(0)).await?;
    create_namespace(&mut conn, "ns-b", test_network_config_with_subnet(1)).await?;

    let net_a = test_pod_network_config_for_subnet(0, 2);
    let net_b = test_pod_network_config_for_subnet(1, 2);

    launch_pod(&mut conn, "ns-a", PodId(1), &net_a).await?;
    launch_pod(&mut conn, "ns-b", PodId(2), &net_b).await?;

    // Destroy ns-a only
    conn.send_command(&WorkerCommand::DestroyNamespace {
        namespace_id: "ns-a".into(),
    })
    .await?;

    // Expect pod termination for ns-a
    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodExited { namespace_id, .. } | WorkerEvent::PodFailed { namespace_id, .. }
            if namespace_id == "ns-a")
    })
    .await?;

    // Expect NamespaceDestroyed for ns-a
    recv_until(
        &mut conn,
        EVENT_TIMEOUT,
        |e| matches!(e, WorkerEvent::NamespaceDestroyed { namespace_id } if namespace_id == "ns-a"),
    )
    .await?;

    // ns-b pod should still be running — stop it gracefully
    conn.send_command(&WorkerCommand::StopPod {
        namespace_id: "ns-b".into(),
        pod_id: PodId(2),
        graceful: true,
    })
    .await?;

    recv_until(
        &mut conn,
        EVENT_TIMEOUT,
        |e| matches!(e, WorkerEvent::PodExited { namespace_id, .. } if namespace_id == "ns-b"),
    )
    .await?;

    shutdown_worker(&mut conn, worker_handle).await?;
    Ok(())
}

#[tokio::test]
async fn test_sim_rapid_create_destroy() -> anyhow::Result<()> {
    let (mut conn, worker_handle, _pool_id) =
        setup_with_behavior(ContainerBehavior::RunUntilSignaled).await?;

    create_namespace(&mut conn, "ns-sim", test_network_config()).await?;

    let pod_net = test_pod_network_config();
    register_pod_endpoint(&mut conn, "ns-sim", &pod_net, WorkerId(1)).await?;

    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-sim".into(),
        pod_id: PodId(1),
        network: pod_net,
        containers: vec![default_container_spec()],
        resources: None,
        volumes: vec![],
    })
    .await?;

    // Immediately destroy the namespace before waiting for PodRunning
    conn.send_command(&WorkerCommand::DestroyNamespace {
        namespace_id: "ns-sim".into(),
    })
    .await?;

    // Should eventually get pod termination and namespace destroyed
    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(
            e,
            WorkerEvent::PodExited { .. } | WorkerEvent::PodFailed { .. }
        )
    })
    .await?;

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::NamespaceDestroyed { namespace_id } if namespace_id == "ns-sim")
    })
    .await?;

    shutdown_worker(&mut conn, worker_handle).await?;
    Ok(())
}

#[tokio::test]
async fn test_sim_stop_during_launch() -> anyhow::Result<()> {
    let (mut conn, worker_handle, _pool_id) =
        setup_with_behavior(ContainerBehavior::RunUntilSignaled).await?;

    create_namespace(&mut conn, "ns-sim", test_network_config()).await?;

    let pod_net = test_pod_network_config();
    register_pod_endpoint(&mut conn, "ns-sim", &pod_net, WorkerId(1)).await?;

    conn.send_command(&WorkerCommand::LaunchPod {
        namespace_id: "ns-sim".into(),
        pod_id: PodId(1),
        network: pod_net,
        containers: vec![default_container_spec()],
        resources: None,
        volumes: vec![],
    })
    .await?;

    // Immediately stop before waiting for PodRunning
    conn.send_command(&WorkerCommand::StopPod {
        namespace_id: "ns-sim".into(),
        pod_id: PodId(1),
        graceful: true,
    })
    .await?;

    // Should get PodExited with -1 (cancelled)
    let event = recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodExited { .. })
    })
    .await?;
    assert!(
        matches!(&event, WorkerEvent::PodExited { exit_code, .. } if *exit_code == -1),
        "expected exit_code -1 (cancelled), got {:?}",
        event
    );

    shutdown_worker(&mut conn, worker_handle).await?;
    Ok(())
}

#[tokio::test]
async fn test_sim_double_stop() -> anyhow::Result<()> {
    let (mut conn, worker_handle, _pool_id) =
        setup_with_behavior(ContainerBehavior::RunUntilSignaled).await?;

    create_namespace(&mut conn, "ns-sim", test_network_config()).await?;

    let pod_net = test_pod_network_config();
    launch_pod(&mut conn, "ns-sim", PodId(1), &pod_net).await?;

    // First stop
    conn.send_command(&WorkerCommand::StopPod {
        namespace_id: "ns-sim".into(),
        pod_id: PodId(1),
        graceful: true,
    })
    .await?;

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodExited { .. })
    })
    .await?;

    // Second stop — should be silently ignored, no panic
    conn.send_command(&WorkerCommand::StopPod {
        namespace_id: "ns-sim".into(),
        pod_id: PodId(1),
        graceful: true,
    })
    .await?;

    // Verify worker is still healthy
    shutdown_worker(&mut conn, worker_handle).await?;
    Ok(())
}

#[tokio::test]
async fn test_sim_commands_after_shutdown() -> anyhow::Result<()> {
    let (mut conn, worker_handle, _pool_id) = setup().await?;

    create_namespace(&mut conn, "ns-sim", test_network_config()).await?;

    // Send shutdown
    conn.send_command(&WorkerCommand::Shutdown).await?;

    // Try sending more commands — these write to the duplex but are never read.
    // We tolerate send failures since the worker may have closed its half.
    let _ = conn
        .send_command(&WorkerCommand::CreateNamespace {
            namespace_id: "ns-extra".into(),
            network: test_network_config(),
        })
        .await;

    // Worker should exit cleanly
    match tokio::time::timeout(EVENT_TIMEOUT, worker_handle).await {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(e))) => panic!("worker exited with error: {:#}", e),
        Ok(Err(e)) => panic!("worker task panicked: {}", e),
        Err(_) => panic!("worker did not shut down in time"),
    }

    Ok(())
}
