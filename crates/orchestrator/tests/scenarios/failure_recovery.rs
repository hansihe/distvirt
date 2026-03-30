use std::time::Duration;

use crate::harness::mock_worker::MockWorkerConfig;
use crate::harness::*;
use distvirt_worker_protocol::{WorkerCommand, WorkerEvent};

// =============================================================================
// Tests from retry_failure.rs
// =============================================================================

/// Worker returns PodFailed on LaunchPod. After converge, workload enters RetryBackoff.
/// Advance time through each backoff. After 5 failures → Failed state.
#[test]
fn test_pod_launch_failure_retries() {
    let mut h = TestHarness::new();
    h.add_worker_with(MockWorkerConfig::with_launch_failure());
    h.create_namespace("ns", always_on_spec());
    h.converge();

    // After first failure: RetryBackoff (consecutive_failures=1)
    h.assert_workload_retry_backoff("ns", "echo");

    // Advance through backoffs: 1s, 2s, 4s, 8s for attempts 2-5
    for attempt in 2..=5u32 {
        let backoff = Duration::from_secs(1u64 << (attempt - 2).min(5));
        h.advance_time(backoff + Duration::from_millis(100));
        if attempt < 5 {
            h.assert_workload_retry_backoff("ns", "echo");
        }
    }

    // After 5th failure: Failed
    h.assert_workload_failed("ns", "echo");
}

/// Fail 2 launches, then succeed. Workload reaches Running. consecutive_failures reset.
#[test]
fn test_pod_launch_failure_recovery_on_success() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    let fail_count = Arc::new(AtomicU32::new(0));
    let fail_count_clone = fail_count.clone();

    let config = MockWorkerConfig {
        handler: Some(Box::new(move |cmd| match cmd {
            distvirt_worker_protocol::WorkerCommand::LaunchPod {
                namespace_id,
                pod_id,
                ..
            } => {
                let n = fail_count_clone.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    Some(vec![WorkerEvent::PodFailed {
                        namespace_id: namespace_id.clone(),
                        pod_id: pod_id.clone(),
                        error: "transient failure".to_string(),
                    }])
                } else {
                    // Succeed
                    Some(vec![WorkerEvent::PodRunning {
                        namespace_id: namespace_id.clone(),
                        pod_id: pod_id.clone(),
                    }])
                }
            }
            _ => None,
        })),
        ..Default::default()
    };

    let mut h = TestHarness::new();
    h.add_worker_with(config);
    h.create_namespace("ns", always_on_spec());
    h.converge();

    // First failure → RetryBackoff
    h.assert_workload_retry_backoff("ns", "echo");

    // Advance past first backoff (1s)
    h.advance_time(Duration::from_secs(2));

    // Second failure → RetryBackoff
    h.assert_workload_retry_backoff("ns", "echo");

    // Advance past second backoff (2s)
    h.advance_time(Duration::from_secs(3));

    // Third attempt succeeds → Running
    h.assert_workload_running("ns", "echo");

    // Verify consecutive_failures reset
    let wl = h.workload_state("ns", "echo");
    assert_eq!(wl.consecutive_failures, 0);
}

/// Drive workload to Failed. Update spec with new image. Converge. Workload should retry and reach Running.
#[test]
fn test_failed_workload_recovery_via_spec_change() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    let should_fail = Arc::new(AtomicBool::new(true));
    let should_fail_clone = should_fail.clone();

    let config = MockWorkerConfig {
        handler: Some(Box::new(move |cmd| match cmd {
            distvirt_worker_protocol::WorkerCommand::LaunchPod {
                namespace_id,
                pod_id,
                ..
            } => {
                if should_fail_clone.load(Ordering::SeqCst) {
                    Some(vec![WorkerEvent::PodFailed {
                        namespace_id: namespace_id.clone(),
                        pod_id: pod_id.clone(),
                        error: "bad image".to_string(),
                    }])
                } else {
                    Some(vec![WorkerEvent::PodRunning {
                        namespace_id: namespace_id.clone(),
                        pod_id: pod_id.clone(),
                    }])
                }
            }
            _ => None,
        })),
        ..Default::default()
    };

    let mut h = TestHarness::new();
    h.add_worker_with(config);
    h.create_namespace("ns", always_on_spec());
    h.converge();

    // Drive to Failed: advance through all 5 retries
    h.drive_to_failed("ns", "echo");

    // Now switch handler to succeed and update spec with new image
    should_fail.store(false, Ordering::SeqCst);
    let mut new_spec = always_on_spec();
    new_spec.set_image("echo", "docker.io/library/alpine:new");
    h.update_namespace("ns", new_spec);
    h.converge();

    h.assert_workload_running("ns", "echo");
}

/// Worker sends PodExited (exit_code=1) while workload is Running.
/// Workload should re-launch with retry backoff.
#[test]
fn test_pod_exit_while_running() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker();
    h.create_namespace("ns", always_on_spec());
    h.converge();
    h.assert_workload_running("ns", "echo");

    // Inject PodExited with non-zero exit code
    h.inject_pod_exited(&w1, "ns", "echo", 1);

    // Should enter RetryBackoff (failure counted)
    h.assert_workload_retry_backoff("ns", "echo");

    // Advance past backoff → relaunches → Running
    h.advance_time(Duration::from_secs(2));
    h.assert_workload_running("ns", "echo");
}

/// Worker sends PodExited (exit_code=0). Should NOT increment consecutive_failures (clean exit).
/// Still relaunches because demand > 0 for always-on.
#[test]
fn test_pod_exit_code_zero_no_backoff() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker();
    h.create_namespace("ns", always_on_spec());
    h.converge();
    h.assert_workload_running("ns", "echo");

    // Inject clean exit
    h.inject_pod_exited(&w1, "ns", "echo", 0);

    // Clean exit: consecutive_failures should be 0, so immediate WaitingForCapacity → Running
    // (no backoff)
    h.assert_workload_running("ns", "echo");
    let wl = h.workload_state("ns", "echo");
    assert_eq!(
        wl.consecutive_failures, 0,
        "clean exit should not increment failures"
    );
}

// =============================================================================
// Tests from resume_failure.rs
// =============================================================================

/// Test: ResumePod fails, orchestrator should fall back to cold launch.
#[test]
fn test_resume_failure_falls_back_to_cold_launch() {
    let mut h = TestHarness::new();

    // Use a worker with a pool that fails on ResumePod but succeeds on LaunchPod.
    let w1 = h.add_worker_with(MockWorkerConfig::with_resume_failure());
    h.converge();

    // Create activation namespace.
    let spec = activation_spec(Duration::from_secs(30));
    h.create_namespace("ns1", spec);
    h.converge();
    h.assert_namespace_status("ns1", distvirt_orchestrator::types::NamespaceStatus::Active);

    // Workload starts dormant (activation-based).
    h.assert_workload_dormant("ns1", "web");
    h.assert_service_idle("ns1", "web-svc");

    // Activate → running → idle → suspended
    h.run_activation_suspend_cycle("ns1", "web-svc", "web");
    h.assert_service_idle("ns1", "web-svc");

    // Re-activate via EndpointActivation — resume will fail.
    let svc_ip = h.service_ip("ns1", "web-svc");
    h.worker(&w1)
        .send_event(distvirt_worker_protocol::WorkerEvent::EndpointDemandTraffic {
            namespace_id: h.resolve_ns("ns1"),
            ip: svc_ip,
            service_id: Some(h.proto_service_id("ns1", "web-svc")),
        });
    h.converge();

    // Reconciliation-based readiness syncing ensures demand is preserved
    // through retry. The workload should enter RetryBackoff, then after backoff,
    // relaunch via cold LaunchPod.
    h.assert_workload_retry_backoff("ns1", "web");
    h.assert_service_need_backend("ns1", "web-svc");

    // Advance past the backoff timer (1s for first retry) → cold launch.
    h.advance_time(Duration::from_secs(2));

    // Workload should be running again after cold launch.
    h.assert_workload_running("ns1", "web");

    // Verify command counts
    h.assert_worker_command_count(&w1, "ResumePod", 1, |c| {
        matches!(c, WorkerCommand::ResumePod { .. })
    });
    let launch_count =
        h.worker_command_count(&w1, |c| matches!(c, WorkerCommand::LaunchPod { .. }));
    assert!(
        launch_count >= 2,
        "expected at least 2 LaunchPod commands (initial + cold restart), got {}",
        launch_count
    );
}

// =============================================================================
// Tests from workload_conditions.rs
// =============================================================================

/// When a workload enters RetryBackoff, the `retry-backoff` condition should be set.
/// When it recovers (PodRunning), the condition should be cleared.
#[test]
fn test_retry_backoff_condition_lifecycle() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    let fail_count = Arc::new(AtomicU32::new(0));
    let fail_count_clone = fail_count.clone();

    let config = MockWorkerConfig {
        handler: Some(Box::new(move |cmd| match cmd {
            distvirt_worker_protocol::WorkerCommand::LaunchPod {
                namespace_id,
                pod_id,
                ..
            } => {
                let n = fail_count_clone.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    Some(vec![WorkerEvent::PodFailed {
                        namespace_id: namespace_id.clone(),
                        pod_id: pod_id.clone(),
                        error: "transient failure".to_string(),
                    }])
                } else {
                    Some(vec![WorkerEvent::PodRunning {
                        namespace_id: namespace_id.clone(),
                        pod_id: pod_id.clone(),
                    }])
                }
            }
            _ => None,
        })),
        ..Default::default()
    };

    let mut h = TestHarness::new();
    h.add_worker_with(config);
    h.create_namespace("ns", always_on_spec());
    h.converge();

    // After first failure: RetryBackoff with condition set.
    h.assert_workload_retry_backoff("ns", "echo");
    let conditions = h.workload_conditions("ns", "echo");
    assert!(
        conditions.contains_key("retry-backoff"),
        "retry-backoff condition should be set during backoff, got: {:?}",
        conditions
    );

    // Advance past backoffs until recovery.
    h.advance_time(Duration::from_secs(2)); // past 1s backoff → 2nd failure
    h.assert_workload_retry_backoff("ns", "echo");
    assert!(
        h.workload_conditions("ns", "echo")
            .contains_key("retry-backoff"),
        "retry-backoff should still be set after 2nd failure"
    );

    h.advance_time(Duration::from_secs(3)); // past 2s backoff → 3rd attempt succeeds

    // After successful launch: condition should be cleared.
    h.assert_workload_running("ns", "echo");
    let conditions = h.workload_conditions("ns", "echo");
    assert!(
        !conditions.contains_key("retry-backoff"),
        "retry-backoff should be cleared after successful launch, got: {:?}",
        conditions
    );
}

/// When a workload enters Failed state, the `failed` condition should be set.
/// After recovery via spec change, it should be cleared.
#[test]
fn test_failed_condition_lifecycle() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    let should_fail = Arc::new(AtomicBool::new(true));
    let should_fail_clone = should_fail.clone();

    let config = MockWorkerConfig {
        handler: Some(Box::new(move |cmd| match cmd {
            distvirt_worker_protocol::WorkerCommand::LaunchPod {
                namespace_id,
                pod_id,
                ..
            } => {
                if should_fail_clone.load(Ordering::SeqCst) {
                    Some(vec![WorkerEvent::PodFailed {
                        namespace_id: namespace_id.clone(),
                        pod_id: pod_id.clone(),
                        error: "bad image".to_string(),
                    }])
                } else {
                    Some(vec![WorkerEvent::PodRunning {
                        namespace_id: namespace_id.clone(),
                        pod_id: pod_id.clone(),
                    }])
                }
            }
            _ => None,
        })),
        ..Default::default()
    };

    let mut h = TestHarness::new();
    h.add_worker_with(config);
    h.create_namespace("ns", always_on_spec());
    h.converge();

    // Drive to Failed: advance through all 5 retries.
    h.drive_to_failed("ns", "echo");

    // Failed condition should be set.
    let conditions = h.workload_conditions("ns", "echo");
    assert!(
        conditions.contains_key("failed"),
        "failed condition should be set in Failed state, got: {:?}",
        conditions
    );

    // Recover via spec change.
    should_fail.store(false, Ordering::SeqCst);
    let mut new_spec = always_on_spec();
    new_spec.set_image("echo", "docker.io/library/alpine:fixed");
    h.update_namespace("ns", new_spec);
    h.converge();

    h.assert_workload_running("ns", "echo");
    let conditions = h.workload_conditions("ns", "echo");
    assert!(
        !conditions.contains_key("failed"),
        "failed condition should be cleared after recovery, got: {:?}",
        conditions
    );
}

// test_failed_condition_in_status_report and test_retry_backoff_condition_in_status_report
// were removed: they were strict subsets of test_failed_condition_lifecycle and
// test_retry_backoff_condition_lifecycle respectively.
