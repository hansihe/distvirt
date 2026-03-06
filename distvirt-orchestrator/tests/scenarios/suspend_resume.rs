use std::time::Duration;

use crate::harness::mock_worker::MockWorkerConfig;
use crate::harness::*;
use distvirt_orchestrator::types::*;
use distvirt_worker_protocol::{WorkerCommand, WorkerEvent};

/// Full cycle: activate → run → idle → suspend → re-activate → resume → running.
/// Verify the resume path uses ResumePod (not LaunchPod).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_resume_from_suspended() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;
    h.assert_workload_dormant("ns", "web");

    // Activate → running → idle → suspend
    h.run_activation_suspend_cycle("ns", "web-svc", "web").await;

    // Re-activate → should resume (not cold launch)
    h.activate_service("ns", "web-svc").await;

    // Verify ResumePod was sent (not LaunchPod after the first one)
    h.assert_worker_command_count(&w1, "ResumePod", 1, |c| matches!(c, WorkerCommand::ResumePod { .. }));
}

/// Use suspend_hang handler. Activate → run → idle → suspending → advance past SUSPEND_TIMEOUT.
/// Workload should fall back (StopPod issued).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_suspend_timeout_fallback_to_stop() {
    let config = MockWorkerConfig::with_suspend_hang().add_pool();
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(config).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;

    // Activate → running
    h.activate_service("ns", "web-svc").await;

    // Idle → begin suspending
    h.deactivate_service("ns", "web-svc").await;
    h.advance_past_idle_timeout("ns", "web-svc").await;
    // Should be in Suspending (handler doesn't respond)
    h.assert_workload_suspending("ns", "web");

    // Advance past suspend timeout (30s)
    h.advance_time(Duration::from_secs(31)).await;

    // After suspend timeout, the orchestrator issues StopPod and the workload
    // transitions to Dormant (demand is 0, service went idle).
    h.assert_workload_dormant("ns", "web");

    // StopPod should have been issued
    let stop_count = h.worker_command_count(&w1, |c| matches!(c, WorkerCommand::StopPod { .. }));
    assert!(stop_count >= 1, "expected StopPod after suspend timeout");
}

/// Worker returns PodSuspendFailed. Workload should transition appropriately.
/// StopPod should be issued.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_suspend_failure_fallback_to_stop() {
    let config = MockWorkerConfig::with_suspend_failure().add_pool();
    let mut h = TestHarness::new();
    let _w1 = h.add_worker_with(config).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;

    // Activate → running → idle → suspend attempt (fails immediately)
    h.activate_service("ns", "web-svc").await;
    h.deactivate_service("ns", "web-svc").await;
    h.advance_past_idle_timeout("ns", "web-svc").await;

    // PodSuspendFailed triggers StopPod fallback. With demand at 0 (service went idle),
    // the workload transitions to Dormant.
    h.assert_workload_dormant("ns", "web");
}

/// Use activation_no_suspend_spec. Activate → run → idle → stop (not suspend).
/// Re-activate → cold start (LaunchPod, not ResumePod).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_activation_no_suspend_cold_start() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_no_suspend_spec(timeout)).await;
    h.converge().await;
    h.assert_workload_dormant("ns", "web");

    // Activate → running → idle → stop (not suspend, because suspend_on_idle=false)
    h.run_activation_stop_cycle("ns", "web-svc", "web").await;

    // Re-activate → cold start
    h.activate_service("ns", "web-svc").await;

    // Verify LaunchPod was used both times (not ResumePod)
    h.assert_worker_command_count(&w1, "LaunchPod", 2, |c| matches!(c, WorkerCommand::LaunchPod { .. }));
    h.assert_worker_command_count(&w1, "ResumePod", 0, |c| matches!(c, WorkerCommand::ResumePod { .. }));
}

/// Test: Pod crashes (exits unexpectedly) while in the Suspending state.
#[tokio::test(start_paused = true)]
async fn test_pod_exit_during_suspend() {
    let mut h = TestHarness::new();

    // Worker with suspend hang (no response to SuspendPod) + pool.
    let w1 = h
        .add_worker_with(MockWorkerConfig::with_suspend_hang().add_pool())
        .await;
    h.converge().await;

    let spec = activation_spec(Duration::from_secs(30));
    h.create_namespace("ns1", spec).await;
    h.converge().await;
    h.assert_namespace_status("ns1", NamespaceStatus::Active);

    // Activate → running → idle → suspending (handler hangs)
    h.activate_service("ns1", "web-svc").await;
    h.deactivate_service("ns1", "web-svc").await;
    h.advance_past_idle_timeout("ns1", "web-svc").await;
    h.assert_workload_suspending("ns1", "web");

    // Get the pod_id from the workload state so we can inject the right event.
    let pod_id = {
        let state = h.workload_state("ns1", "web");
        match state {
            WorkloadState::Suspending { pod_id, .. } => {
                pod_id.clone()
            }
            _ => panic!("expected Suspending state"),
        }
    };

    // Pod crashes while suspending: inject PodFailed.
    h.worker(&w1).send_event(WorkerEvent::PodFailed {
        namespace_id: "ns1".into(),
        pod_id: pod_id.clone(),
        error: "VM crashed during suspend".to_string(),
    });
    h.converge().await;

    // A pod crash during an intentional deactivation should not count as a failure.
    h.assert_workload_dormant("ns1", "web");

    // Verify failure counter was NOT incremented.
    let ns = h.namespace("ns1");
    let wl = ns.workloads.get(&WorkloadId("web".to_string())).unwrap();
    assert_eq!(
        wl.consecutive_failures, 0,
        "pod crash during suspend should not increment consecutive_failures"
    );
}

/// Test: Pod exits with exit_code during suspend (PodExited, not PodFailed).
#[tokio::test(start_paused = true)]
async fn test_pod_exited_during_suspend() {
    let mut h = TestHarness::new();

    let w1 = h
        .add_worker_with(MockWorkerConfig::with_suspend_hang().add_pool())
        .await;
    h.converge().await;

    let spec = activation_spec(Duration::from_secs(30));
    h.create_namespace("ns1", spec).await;
    h.converge().await;

    // Activate → running → idle → suspending (handler hangs)
    h.activate_service("ns1", "web-svc").await;
    h.deactivate_service("ns1", "web-svc").await;
    h.advance_past_idle_timeout("ns1", "web-svc").await;
    h.assert_workload_suspending("ns1", "web");

    let pod_id = {
        let state = h.workload_state("ns1", "web");
        match state {
            WorkloadState::Suspending { pod_id, .. } => {
                pod_id.clone()
            }
            _ => panic!("expected Suspending state"),
        }
    };

    // Inject PodExited (exit_code: 1) while suspending.
    h.worker(&w1).send_event(WorkerEvent::PodExited {
        namespace_id: "ns1".into(),
        pod_id,
        exit_code: 1,
    });
    h.converge().await;

    h.assert_workload_dormant("ns1", "web");

    // Verify failure counter was NOT incremented.
    let ns = h.namespace("ns1");
    let wl = ns.workloads.get(&WorkloadId("web".to_string())).unwrap();
    assert_eq!(
        wl.consecutive_failures, 0,
        "pod exit during suspend should not increment consecutive_failures"
    );
}

/// When a namespace is deleted while a workload is mid-resume (Resuming state),
/// the ForceDeactivate sets PendingIntent::Deactivate. When the resume completes
/// (PodRunning), the workload should stop rather than entering Running.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_delete_during_resume() {
    let mut h = TestHarness::new();

    // Use a handler that hangs on ResumePod AND DestroyNamespace (no response)
    let config = MockWorkerConfig {
        handler: Some(Box::new(|cmd| match cmd {
            distvirt_worker_protocol::WorkerCommand::ResumePod { .. } => Some(vec![]),
            distvirt_worker_protocol::WorkerCommand::DestroyNamespace { .. } => Some(vec![]),
            _ => None,
        })),
        ..MockWorkerConfig::with_pool()
    };
    let w1 = h.add_worker_with(config).await;

    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;
    h.assert_workload_dormant("ns", "web");

    // Activate → Running → idle → suspend
    h.run_activation_suspend_cycle("ns", "web-svc", "web").await;

    // Re-activate → should start resume (which will hang).
    // Low-level: need to trigger resume without asserting Running (it hangs)
    let svc_ip = h.service_ip("ns", "web-svc");
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: distvirt_worker_protocol::ServiceId::from("web-svc"),
        dst_ip: svc_ip,
    });
    h.converge().await;
    h.assert_workload_resuming("ns", "web");

    // Get the pod_id before deleting namespace.
    let pod_id = match h.workload_state("ns", "web") {
        WorkloadState::Resuming { pod_id, .. } => pod_id.clone(),
        other => panic!("expected Resuming, got {:?}", other),
    };

    // Delete the namespace while mid-resume — sets PendingIntent::Deactivate.
    h.delete_namespace("ns").await;
    h.converge().await;

    h.assert_namespace_status("ns", NamespaceStatus::Destroying);

    // Complete the resume by injecting PodRunning.
    h.worker(&w1).send_event(WorkerEvent::PodRunning {
        namespace_id: "ns".into(),
        pod_id,
    });
    h.converge().await;

    h.assert_namespace_status("ns", NamespaceStatus::Destroying);

    // Inject NamespaceDestroyed to complete the destroy cycle.
    h.worker(&w1).send_event(WorkerEvent::NamespaceDestroyed {
        namespace_id: "ns".into(),
    });
    h.converge().await;

    h.assert_namespace_absent("ns");
}

/// Spec change (image update) during resume sets PendingIntent::Restart.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_spec_change_during_resume() {
    let mut h = TestHarness::new();

    let config = MockWorkerConfig {
        handler: Some(Box::new(|cmd| match cmd {
            distvirt_worker_protocol::WorkerCommand::ResumePod { .. } => Some(vec![]),
            _ => None,
        })),
        ..MockWorkerConfig::with_pool()
    };
    let w1 = h.add_worker_with(config).await;

    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;
    h.assert_workload_dormant("ns", "web");

    // Activate → Running → idle → suspend
    h.run_activation_suspend_cycle("ns", "web-svc", "web").await;

    // Re-activate → Resuming (hangs).
    // Low-level: need to trigger resume without asserting Running (it hangs)
    let svc_ip = h.service_ip("ns", "web-svc");
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: distvirt_worker_protocol::ServiceId::from("web-svc"),
        dst_ip: svc_ip,
    });
    h.converge().await;
    h.assert_workload_resuming("ns", "web");

    let pod_id = match h.workload_state("ns", "web") {
        WorkloadState::Resuming { pod_id, .. } => pod_id.clone(),
        other => panic!("expected Resuming, got {:?}", other),
    };

    // Update spec with new image while resuming.
    let mut new_spec = activation_spec(timeout);
    new_spec
        .workloads
        .get_mut(&WorkloadId("web".to_string()))
        .unwrap()
        .containers[0]
        .image_ref = "docker.io/library/nginx:v2".to_string();
    h.update_namespace("ns", new_spec).await;
    h.converge().await;

    // Still resuming (handler hangs).
    h.assert_workload_resuming("ns", "web");

    // Complete the resume.
    h.worker(&w1).send_event(WorkerEvent::PodRunning {
        namespace_id: "ns".into(),
        pod_id: pod_id.clone(),
    });
    h.converge().await;

    // The Restart intent should have stopped the old pod and relaunched.
    h.assert_workload_running("ns", "web");
    let new_pod_id = h.workload_state("ns", "web").pod_id().unwrap();
    assert_ne!(
        *new_pod_id, pod_id,
        "pod should have been replaced (stopped + relaunched) due to Restart intent"
    );

    // Verify StopPod was issued for the old pod.
    let commands = h.worker(&w1).commands();
    let stop_count = commands
        .iter()
        .filter(|cmd| matches!(cmd, distvirt_worker_protocol::WorkerCommand::StopPod { pod_id: pid, .. } if *pid == pod_id))
        .count();
    assert!(
        stop_count >= 1,
        "should have issued StopPod for the old pod after Restart intent"
    );
}
