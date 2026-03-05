use std::time::Duration;

use crate::harness::*;
use crate::harness::mock_worker::MockWorkerConfig;
use distvirt_orchestrator::types::*;
use distvirt_worker_protocol::WorkerEvent;

/// Worker returns PodFailed on LaunchPod. After converge, workload enters RetryBackoff.
/// Advance time through each backoff. After 5 failures → Failed state.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_pod_launch_failure_retries() {
    let mut h = TestHarness::new();
    h.add_worker_with(MockWorkerConfig::with_launch_failure()).await;
    h.create_namespace("ns", always_on_spec()).await;
    h.converge().await;

    // After first failure: RetryBackoff (consecutive_failures=1)
    h.assert_workload_retry_backoff("ns", "echo");

    // Advance through backoffs: 1s, 2s, 4s, 8s for attempts 2-5
    for attempt in 2..=5u32 {
        let backoff = Duration::from_secs(1u64 << (attempt - 2).min(5));
        h.advance_time(backoff + Duration::from_millis(100)).await;
        if attempt < 5 {
            h.assert_workload_retry_backoff("ns", "echo");
        }
    }

    // After 5th failure: Failed
    h.assert_workload_failed("ns", "echo");
}

/// Fail 2 launches, then succeed. Workload reaches Running. consecutive_failures reset.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_pod_launch_failure_recovery_on_success() {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

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
    h.add_worker_with(config).await;
    h.create_namespace("ns", always_on_spec()).await;
    h.converge().await;

    // First failure → RetryBackoff
    h.assert_workload_retry_backoff("ns", "echo");

    // Advance past first backoff (1s)
    h.advance_time(Duration::from_secs(2)).await;

    // Second failure → RetryBackoff
    h.assert_workload_retry_backoff("ns", "echo");

    // Advance past second backoff (2s)
    h.advance_time(Duration::from_secs(3)).await;

    // Third attempt succeeds → Running
    h.assert_workload_running("ns", "echo");

    // Verify consecutive_failures reset
    let ns = h.namespace("ns");
    let wl = ns.workloads.get(&WorkloadId("echo".to_string())).unwrap();
    assert_eq!(wl.consecutive_failures, 0);
}

/// Drive workload to Failed. Update spec with new image. Converge. Workload should retry and reach Running.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_failed_workload_recovery_via_spec_change() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

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
    h.add_worker_with(config).await;
    h.create_namespace("ns", always_on_spec()).await;
    h.converge().await;

    // Drive to Failed: advance through all 5 retries
    for attempt in 1..5u32 {
        let backoff = Duration::from_secs(1u64 << (attempt - 1).min(5));
        h.advance_time(backoff + Duration::from_millis(100)).await;
    }
    h.assert_workload_failed("ns", "echo");

    // Now switch handler to succeed and update spec with new image
    should_fail.store(false, Ordering::SeqCst);
    let mut new_spec = always_on_spec();
    new_spec.workloads.get_mut(&WorkloadId("echo".to_string())).unwrap()
        .containers[0].image_ref = "docker.io/library/alpine:new".to_string();
    h.update_namespace("ns", new_spec).await;
    h.converge().await;

    h.assert_workload_running("ns", "echo");
}

/// Worker sends PodExited (exit_code=1) while workload is Running.
/// Workload should re-launch with retry backoff.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_pod_exit_while_running() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker().await;
    h.create_namespace("ns", always_on_spec()).await;
    h.converge().await;
    h.assert_workload_running("ns", "echo");

    // Get the pod_id of the running workload
    let pod_id = h.workload_state("ns", "echo").pod_id().unwrap().clone();

    // Inject PodExited with non-zero exit code
    h.worker(&w1).send_event(WorkerEvent::PodExited {
        namespace_id: "ns".into(),
        pod_id,
        exit_code: 1,
    });
    h.converge().await;

    // Should enter RetryBackoff (failure counted)
    h.assert_workload_retry_backoff("ns", "echo");

    // Advance past backoff → relaunches → Running
    h.advance_time(Duration::from_secs(2)).await;
    h.assert_workload_running("ns", "echo");
}

/// Worker sends PodExited (exit_code=0). Should NOT increment consecutive_failures (clean exit).
/// Still relaunches because demand > 0 for always-on.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_pod_exit_code_zero_no_backoff() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker().await;
    h.create_namespace("ns", always_on_spec()).await;
    h.converge().await;
    h.assert_workload_running("ns", "echo");

    let pod_id = h.workload_state("ns", "echo").pod_id().unwrap().clone();

    // Inject clean exit
    h.worker(&w1).send_event(WorkerEvent::PodExited {
        namespace_id: "ns".into(),
        pod_id,
        exit_code: 0,
    });
    h.converge().await;

    // Clean exit: consecutive_failures should be 0, so immediate WaitingForCapacity → Running
    // (no backoff)
    h.assert_workload_running("ns", "echo");
    let ns = h.namespace("ns");
    let wl = ns.workloads.get(&WorkloadId("echo".to_string())).unwrap();
    assert_eq!(wl.consecutive_failures, 0, "clean exit should not increment failures");
}
