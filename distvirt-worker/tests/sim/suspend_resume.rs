use distvirt_worker::vmm::guest_sim::ContainerBehavior;
use distvirt_worker_protocol::{ArtifactId, PodId, WorkerCommand, WorkerEvent, WorkerId};

use super::common::*;

#[tokio::test]
async fn test_sim_suspend_resume() -> anyhow::Result<()> {
    let (mut conn, worker_handle, pool_id) =
        setup_with_behavior(ContainerBehavior::RunUntilSignaled).await?;

    create_namespace(&mut conn, "ns-sim", test_network_config()).await?;

    let pod_net = test_pod_network_config();
    launch_pod(&mut conn, "ns-sim", PodId(1), &pod_net).await?;

    // Suspend the pod.
    conn.send_command(&WorkerCommand::SuspendPod {
        namespace_id: "ns-sim".into(),
        pod_id: PodId(1),
        artifact_id: ArtifactId::from("snap-1"),
        pool_id: pool_id.clone(),
    })
    .await?;

    // Expect ArtifactWriteStarted.
    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::ArtifactWriteStarted { artifact_id, .. } if artifact_id == "snap-1")
    })
    .await?;

    // Expect ArtifactWriteCommitted.
    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::ArtifactWriteCommitted { artifact_id, .. } if artifact_id == "snap-1")
    })
    .await?;

    // Expect PodSuspended.
    recv_until(
        &mut conn,
        EVENT_TIMEOUT,
        |e| matches!(e, WorkerEvent::PodSuspended { pod_id, .. } if *pod_id == PodId(1)),
    )
    .await?;

    // Resume the pod.
    conn.send_command(&WorkerCommand::ResumePod {
        namespace_id: "ns-sim".into(),
        pod_id: PodId(1),
        artifact_id: ArtifactId::from("snap-1"),
        network: pod_net.clone(),
        pool_id: pool_id.clone(),
    })
    .await?;

    // Expect PodRunning after resume.
    recv_until(
        &mut conn,
        EVENT_TIMEOUT,
        |e| matches!(e, WorkerEvent::PodRunning { pod_id, .. } if *pod_id == PodId(1)),
    )
    .await?;

    // Stop the resumed pod.
    conn.send_command(&WorkerCommand::StopPod {
        namespace_id: "ns-sim".into(),
        pod_id: PodId(1),
        graceful: true,
    })
    .await?;

    recv_until(
        &mut conn,
        EVENT_TIMEOUT,
        |e| matches!(e, WorkerEvent::PodExited { pod_id, .. } if *pod_id == PodId(1)),
    )
    .await?;

    shutdown_worker(&mut conn, worker_handle).await?;
    Ok(())
}

#[tokio::test]
async fn test_sim_suspend_then_destroy_namespace() -> anyhow::Result<()> {
    let (mut conn, worker_handle, pool_id) =
        setup_with_behavior(ContainerBehavior::RunUntilSignaled).await?;

    create_namespace(&mut conn, "ns-sim", test_network_config()).await?;

    let pod_net = test_pod_network_config();
    launch_pod(&mut conn, "ns-sim", PodId(1), &pod_net).await?;

    // Suspend the pod.
    conn.send_command(&WorkerCommand::SuspendPod {
        namespace_id: "ns-sim".into(),
        pod_id: PodId(1),
        artifact_id: ArtifactId::from("snap-2"),
        pool_id: pool_id.clone(),
    })
    .await?;

    // Wait for PodSuspended.
    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodSuspended { .. })
    })
    .await?;

    // Destroy namespace after suspend.
    conn.send_command(&WorkerCommand::DestroyNamespace {
        namespace_id: "ns-sim".into(),
    })
    .await?;

    // Expect NamespaceDestroyed.
    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::NamespaceDestroyed { namespace_id } if namespace_id == "ns-sim")
    })
    .await?;

    shutdown_worker(&mut conn, worker_handle).await?;
    Ok(())
}

#[tokio::test]
async fn test_sim_resume_unknown_artifact() -> anyhow::Result<()> {
    let (mut conn, worker_handle, pool_id) =
        setup_with_behavior(ContainerBehavior::RunUntilSignaled).await?;

    create_namespace(&mut conn, "ns-sim", test_network_config()).await?;

    let pod_net = test_pod_network_config();
    register_pod_endpoint(&mut conn, "ns-sim", &pod_net, WorkerId(1)).await?;

    // Resume with a bogus artifact_id that doesn't exist on disk.
    conn.send_command(&WorkerCommand::ResumePod {
        namespace_id: "ns-sim".into(),
        pod_id: PodId(1),
        artifact_id: ArtifactId::from("nonexistent-snap"),
        network: pod_net,
        pool_id: pool_id.clone(),
    })
    .await?;

    // Expect PodFailed because metadata.json won't be found.
    let event = recv_until(
        &mut conn,
        EVENT_TIMEOUT,
        |e| matches!(e, WorkerEvent::PodFailed { pod_id, .. } if *pod_id == PodId(1)),
    )
    .await?;
    assert!(
        matches!(&event, WorkerEvent::PodFailed { error, .. } if error.contains("metadata.json")),
        "expected metadata.json error, got {:?}",
        event
    );

    shutdown_worker(&mut conn, worker_handle).await?;
    Ok(())
}

#[tokio::test]
async fn test_sim_re_suspend_after_resume() -> anyhow::Result<()> {
    let (mut conn, worker_handle, pool_id) =
        setup_with_behavior(ContainerBehavior::RunUntilSignaled).await?;

    create_namespace(&mut conn, "ns-sim", test_network_config()).await?;

    let pod_net = test_pod_network_config();
    launch_pod(&mut conn, "ns-sim", PodId(1), &pod_net).await?;

    // First suspend.
    conn.send_command(&WorkerCommand::SuspendPod {
        namespace_id: "ns-sim".into(),
        pod_id: PodId(1),
        artifact_id: ArtifactId::from("snap-a"),
        pool_id: pool_id.clone(),
    })
    .await?;

    recv_until(&mut conn, EVENT_TIMEOUT, |e| {
        matches!(e, WorkerEvent::PodSuspended { .. })
    })
    .await?;

    // First resume.
    conn.send_command(&WorkerCommand::ResumePod {
        namespace_id: "ns-sim".into(),
        pod_id: PodId(1),
        artifact_id: ArtifactId::from("snap-a"),
        network: pod_net.clone(),
        pool_id: pool_id.clone(),
    })
    .await?;

    recv_until(
        &mut conn,
        EVENT_TIMEOUT,
        |e| matches!(e, WorkerEvent::PodRunning { pod_id, .. } if *pod_id == PodId(1)),
    )
    .await?;

    // Second suspend.
    conn.send_command(&WorkerCommand::SuspendPod {
        namespace_id: "ns-sim".into(),
        pod_id: PodId(1),
        artifact_id: ArtifactId::from("snap-b"),
        pool_id: pool_id.clone(),
    })
    .await?;

    recv_until(
        &mut conn,
        EVENT_TIMEOUT,
        |e| matches!(e, WorkerEvent::PodSuspended { artifact_id, .. } if artifact_id == "snap-b"),
    )
    .await?;

    // Second resume.
    conn.send_command(&WorkerCommand::ResumePod {
        namespace_id: "ns-sim".into(),
        pod_id: PodId(1),
        artifact_id: ArtifactId::from("snap-b"),
        network: pod_net.clone(),
        pool_id: pool_id.clone(),
    })
    .await?;

    recv_until(
        &mut conn,
        EVENT_TIMEOUT,
        |e| matches!(e, WorkerEvent::PodRunning { pod_id, .. } if *pod_id == PodId(1)),
    )
    .await?;

    // Clean stop.
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

    shutdown_worker(&mut conn, worker_handle).await?;
    Ok(())
}
