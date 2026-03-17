use distvirt_worker::vmm::guest_sim::ContainerBehavior;
use distvirt_worker_protocol::{WorkerCommand, WorkerEvent};

use super::common::*;

#[tokio::test]
async fn test_sim_vm_crash() -> anyhow::Result<()> {
    let (mut conn, worker_handle, mut crash_rx) =
        setup_with_crash_handles(ContainerBehavior::RunUntilSignaled).await?;

    create_namespace(&mut conn, "ns-sim", test_network_config()).await?;

    let pod_net = test_pod_network_config();
    launch_pod(&mut conn, "ns-sim", "pod-sim", &pod_net).await?;

    // Get the crash handle for the launched VM.
    let crash_handle = crash_rx.recv().await.expect("should receive crash handle");

    // Crash the VM.
    crash_handle.crash();

    // Expect PodFailed with VM exit message.
    let event = recv_until(
        &mut conn,
        EVENT_TIMEOUT,
        |e| matches!(e, WorkerEvent::PodFailed { pod_id, .. } if pod_id == "pod-sim"),
    )
    .await?;
    assert!(
        matches!(&event, WorkerEvent::PodFailed { error, .. }
            if error.contains("VM process exited unexpectedly")),
        "expected VM crash error, got {:?}",
        event
    );

    shutdown_worker(&mut conn, worker_handle).await?;
    Ok(())
}

#[tokio::test]
async fn test_sim_worker_health_after_crash() -> anyhow::Result<()> {
    let (mut conn, worker_handle, mut crash_rx) =
        setup_with_crash_handles(ContainerBehavior::RunUntilSignaled).await?;

    create_namespace(&mut conn, "ns-sim", test_network_config()).await?;

    let pod_net = test_pod_network_config();
    launch_pod(&mut conn, "ns-sim", "pod-sim-1", &pod_net).await?;

    // Crash the first VM.
    let crash_handle = crash_rx.recv().await.expect("should receive crash handle");
    crash_handle.crash();

    // Wait for PodFailed.
    recv_until(
        &mut conn,
        EVENT_TIMEOUT,
        |e| matches!(e, WorkerEvent::PodFailed { pod_id, .. } if pod_id == "pod-sim-1"),
    )
    .await?;

    // Launch a new pod in the same namespace — worker should still be healthy.
    let pod_net2 = test_pod_network_config_with_ip(3);
    launch_pod(&mut conn, "ns-sim", "pod-sim-2", &pod_net2).await?;

    // The RunUntilSignaled pod is running; stop it gracefully.
    conn.send_command(&WorkerCommand::StopPod {
        namespace_id: "ns-sim".into(),
        pod_id: "pod-sim-2".into(),
        graceful: true,
    })
    .await?;

    recv_until(
        &mut conn,
        EVENT_TIMEOUT,
        |e| matches!(e, WorkerEvent::PodExited { pod_id, .. } if pod_id == "pod-sim-2"),
    )
    .await?;

    shutdown_worker(&mut conn, worker_handle).await?;
    Ok(())
}
