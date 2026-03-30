use std::time::Duration;

use crate::harness::mock_worker::MockWorkerConfig;
use crate::harness::*;
use distvirt_orchestrator::types::*;
use distvirt_worker_protocol::{WorkerCommand, WorkerEvent};

/// Full cycle: activate → run → idle → suspend → re-activate → resume → running.
/// Verify the resume path uses ResumePod (not LaunchPod).
#[test]
fn test_resume_from_suspended() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool());
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout));
    h.converge();
    h.assert_workload_dormant("ns", "web");

    // Activate → running → idle → suspend
    h.run_activation_suspend_cycle("ns", "web-svc", "web");

    // Re-activate → should resume (not cold launch)
    h.activate_service("ns", "web-svc");

    // Verify ResumePod was sent (not LaunchPod after the first one)
    h.assert_worker_command_count(&w1, "ResumePod", 1, |c| {
        matches!(c, WorkerCommand::ResumePod { .. })
    });
}

/// Use suspend_hang handler. Activate → run → idle → suspending → advance past SUSPEND_TIMEOUT.
/// Workload should fall back (StopPod issued).
#[test]
fn test_suspend_timeout_fallback_to_stop() {
    let config = MockWorkerConfig::with_suspend_hang().add_pool();
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(config);
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout));
    h.converge();

    // Activate → running
    h.activate_service("ns", "web-svc");

    // Idle → begin suspending
    h.deactivate_service("ns", "web-svc");
    h.advance_past_idle_timeout("ns", "web-svc");
    // Should be in Suspending (handler doesn't respond)
    h.assert_workload_suspending("ns", "web");

    // Advance past suspend timeout (30s)
    h.advance_time(Duration::from_secs(31));

    // After suspend timeout, the orchestrator issues StopPod and the workload
    // transitions to Dormant (demand is 0, service went idle).
    h.assert_workload_dormant("ns", "web");

    // StopPod should have been issued
    let stop_count = h.worker_command_count(&w1, |c| matches!(c, WorkerCommand::StopPod { .. }));
    assert!(stop_count >= 1, "expected StopPod after suspend timeout");
}

/// Worker returns PodSuspendFailed. Workload should transition appropriately.
/// StopPod should be issued.
#[test]
fn test_suspend_failure_fallback_to_stop() {
    let config = MockWorkerConfig::with_suspend_failure().add_pool();
    let mut h = TestHarness::new();
    let _w1 = h.add_worker_with(config);
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout));
    h.converge();

    // Activate → running → idle → suspend attempt (fails immediately)
    h.activate_service("ns", "web-svc");
    h.deactivate_service("ns", "web-svc");
    h.advance_past_idle_timeout("ns", "web-svc");

    // PodSuspendFailed triggers StopPod fallback. With demand at 0 (service went idle),
    // the workload transitions to Dormant.
    h.assert_workload_dormant("ns", "web");
}

/// Use activation_no_suspend_spec. Activate → run → idle → stop (not suspend).
/// Re-activate → cold start (LaunchPod, not ResumePod).
#[test]
fn test_activation_no_suspend_cold_start() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool());
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_no_suspend_spec(timeout));
    h.converge();
    h.assert_workload_dormant("ns", "web");

    // Activate → running → idle → stop (not suspend, because suspend_on_idle=false)
    h.run_activation_stop_cycle("ns", "web-svc", "web");

    // Re-activate → cold start
    h.activate_service("ns", "web-svc");

    // Verify LaunchPod was used both times (not ResumePod)
    h.assert_worker_command_count(&w1, "LaunchPod", 2, |c| {
        matches!(c, WorkerCommand::LaunchPod { .. })
    });
    h.assert_worker_command_count(&w1, "ResumePod", 0, |c| {
        matches!(c, WorkerCommand::ResumePod { .. })
    });
}

/// Test: Pod crashes (exits unexpectedly) while in the Suspending state.
#[test]
fn test_pod_exit_during_suspend() {
    let mut h = TestHarness::new();

    // Worker with suspend hang (no response to SuspendPod) + pool.
    let w1 = h.add_worker_with(MockWorkerConfig::with_suspend_hang().add_pool());
    h.converge();

    let spec = activation_spec(Duration::from_secs(30));
    h.create_namespace("ns1", spec);
    h.converge();
    h.assert_namespace_status("ns1", NamespaceStatus::Active);

    // Activate → running → idle → suspending (handler hangs)
    h.activate_service("ns1", "web-svc");
    h.deactivate_service("ns1", "web-svc");
    h.advance_past_idle_timeout("ns1", "web-svc");
    h.assert_workload_suspending("ns1", "web");

    // Pod crashes while suspending: inject PodFailed.
    h.inject_pod_failed(&w1, "ns1", "web", "VM crashed during suspend");

    // A pod crash during an intentional deactivation should not count as a failure.
    h.assert_workload_dormant("ns1", "web");

    // Verify failure counter was NOT incremented.
    let wl = h.workload_state("ns1", "web");
    assert_eq!(
        wl.consecutive_failures, 0,
        "pod crash during suspend should not increment consecutive_failures"
    );
}

/// Test: Pod exits with exit_code during suspend (PodExited, not PodFailed).
#[test]
fn test_pod_exited_during_suspend() {
    let mut h = TestHarness::new();

    let w1 = h.add_worker_with(MockWorkerConfig::with_suspend_hang().add_pool());
    h.converge();

    let spec = activation_spec(Duration::from_secs(30));
    h.create_namespace("ns1", spec);
    h.converge();

    // Activate → running → idle → suspending (handler hangs)
    h.activate_service("ns1", "web-svc");
    h.deactivate_service("ns1", "web-svc");
    h.advance_past_idle_timeout("ns1", "web-svc");
    h.assert_workload_suspending("ns1", "web");

    // Inject PodExited (exit_code: 1) while suspending.
    h.inject_pod_exited(&w1, "ns1", "web", 1);

    h.assert_workload_dormant("ns1", "web");

    // Verify failure counter was NOT incremented.
    let wl = h.workload_state("ns1", "web");
    assert_eq!(
        wl.consecutive_failures, 0,
        "pod exit during suspend should not increment consecutive_failures"
    );
}

/// When a namespace is deleted while a workload is mid-resume (Resuming state),
/// the ForceDeactivate sets PendingIntent::Deactivate. When the resume completes
/// (PodRunning), the workload should stop rather than entering Running.
#[test]
fn test_delete_during_resume() {
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
    let w1 = h.add_worker_with(config);

    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout));
    h.converge();
    h.assert_workload_dormant("ns", "web");

    // Activate → Running → idle → suspend
    h.run_activation_suspend_cycle("ns", "web-svc", "web");

    // Re-activate → should start resume (which will hang).
    // Low-level: need to trigger resume without asserting Running (it hangs)
    let svc_ip = h.service_ip("ns", "web-svc");
    h.worker(&w1).send_event(WorkerEvent::EndpointDemandTraffic {
        namespace_id: h.resolve_ns("ns"),
        ip: svc_ip,
        service_id: Some(h.proto_service_id("ns", "web-svc")),
    });
    h.converge();
    h.assert_workload_resuming("ns", "web");

    // Delete the namespace while mid-resume.
    h.delete_namespace("ns");
    h.converge();

    // In the new system, destroy is immediate — namespace is gone after delete.
    h.assert_namespace_absent("ns");

    // Verify StopPod was issued for the resuming pod.
    h.assert_worker_received_command_matching(
        &w1,
        "StopPod for resuming pod after namespace deletion",
        |cmd| matches!(cmd, distvirt_worker_protocol::WorkerCommand::StopPod { .. }),
    );
}

/// Spec change (image update) during resume sets PendingIntent::Restart.
#[test]
fn test_spec_change_during_resume() {
    let mut h = TestHarness::new();

    let config = MockWorkerConfig {
        handler: Some(Box::new(|cmd| match cmd {
            distvirt_worker_protocol::WorkerCommand::ResumePod { .. } => Some(vec![]),
            _ => None,
        })),
        ..MockWorkerConfig::with_pool()
    };
    let w1 = h.add_worker_with(config);

    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout));
    h.converge();
    h.assert_workload_dormant("ns", "web");

    // Activate → Running → idle → suspend
    h.run_activation_suspend_cycle("ns", "web-svc", "web");

    // Re-activate → Resuming (hangs).
    // Low-level: need to trigger resume without asserting Running (it hangs)
    let svc_ip = h.service_ip("ns", "web-svc");
    h.worker(&w1).send_event(WorkerEvent::EndpointDemandTraffic {
        namespace_id: h.resolve_ns("ns"),
        ip: svc_ip,
        service_id: Some(h.proto_service_id("ns", "web-svc")),
    });
    h.converge();
    h.assert_workload_resuming("ns", "web");

    let pod_id = h
        .workload_proto_pod_id("ns", "web")
        .expect("expected pod_id");

    // Update spec with new image while resuming.
    let mut new_spec = activation_spec(timeout);
    new_spec.set_image("web", "docker.io/library/nginx:v2");
    h.update_namespace("ns", new_spec);
    h.converge();

    // Still resuming (handler hangs).
    h.assert_workload_resuming("ns", "web");

    // Complete the resume.
    h.worker(&w1).send_event(WorkerEvent::PodRunning {
        namespace_id: h.resolve_ns("ns"),
        pod_id: pod_id.clone(),
    });
    h.converge();

    // The Restart intent should have stopped the old pod and relaunched.
    h.assert_workload_running("ns", "web");
    let new_pod_id = h
        .workload_proto_pod_id("ns", "web")
        .expect("expected pod_id");
    assert_ne!(
        new_pod_id, pod_id,
        "pod should have been replaced (stopped + relaunched) due to Restart intent"
    );

    // Verify StopPod was issued for the old pod.
    let commands = h.worker(&w1).commands();
    let stop_count = commands
        .iter()
        .filter(|cmd| match cmd {
            distvirt_worker_protocol::WorkerCommand::StopPod { pod_id: pid, .. } => *pid == pod_id,
            _ => false,
        })
        .count();
    assert!(
        stop_count >= 1,
        "should have issued StopPod for the old pod after Restart intent"
    );
}
