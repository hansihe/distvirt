use std::time::Duration;

use crate::harness::*;
use crate::harness::mock_worker::MockWorkerConfig;
use distvirt_orchestrator::types::*;
use distvirt_worker_protocol::WorkerEvent;

/// When a workload enters RetryBackoff, the `retry-backoff` condition should be set.
/// When it recovers (PodRunning), the condition should be cleared.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_retry_backoff_condition_lifecycle() {
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

    // After first failure: RetryBackoff with condition set.
    h.assert_workload_retry_backoff("ns", "echo");
    let conditions = h.workload_conditions("ns", "echo");
    assert!(
        conditions.contains_key("retry-backoff"),
        "retry-backoff condition should be set during backoff, got: {:?}",
        conditions
    );

    // Advance past backoffs until recovery.
    h.advance_time(Duration::from_secs(2)).await; // past 1s backoff → 2nd failure
    h.assert_workload_retry_backoff("ns", "echo");
    assert!(
        h.workload_conditions("ns", "echo").contains_key("retry-backoff"),
        "retry-backoff should still be set after 2nd failure"
    );

    h.advance_time(Duration::from_secs(3)).await; // past 2s backoff → 3rd attempt succeeds

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
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_failed_condition_lifecycle() {
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

    // Drive to Failed: advance through all 5 retries.
    for attempt in 1..5u32 {
        let backoff = Duration::from_secs(1u64 << (attempt - 1).min(5));
        h.advance_time(backoff + Duration::from_millis(100)).await;
    }
    h.assert_workload_failed("ns", "echo");

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
    new_spec.workloads.get_mut(&WorkloadId("echo".to_string())).unwrap()
        .containers[0].image_ref = "docker.io/library/alpine:fixed".to_string();
    h.update_namespace("ns", new_spec).await;
    h.converge().await;

    h.assert_workload_running("ns", "echo");
    let conditions = h.workload_conditions("ns", "echo");
    assert!(
        !conditions.contains_key("failed"),
        "failed condition should be cleared after recovery, got: {:?}",
        conditions
    );
}

/// Verify that workload conditions appear in the status report.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_failed_condition_in_status_report() {
    let mut h = TestHarness::new();
    h.add_worker_with(MockWorkerConfig::with_launch_failure()).await;
    h.create_namespace("ns", always_on_spec()).await;
    h.converge().await;

    // Drive to Failed.
    for attempt in 1..5u32 {
        let backoff = Duration::from_secs(1u64 << (attempt - 1).min(5));
        h.advance_time(backoff + Duration::from_millis(100)).await;
    }
    h.assert_workload_failed("ns", "echo");

    // Check status report includes the failed condition.
    let report = h.namespace("ns").status_report();
    let svc_report = report.services.get(&ServiceId::from("echo-svc")).unwrap();
    assert!(
        svc_report.workload_conditions.contains_key("failed"),
        "status report should include 'failed' workload condition, got: {:?}",
        svc_report.workload_conditions
    );
}

/// Verify retry-backoff condition appears in status report during backoff.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_retry_backoff_condition_in_status_report() {
    let mut h = TestHarness::new();
    h.add_worker_with(MockWorkerConfig::with_launch_failure()).await;
    h.create_namespace("ns", always_on_spec()).await;
    h.converge().await;

    // After first failure: RetryBackoff.
    h.assert_workload_retry_backoff("ns", "echo");

    let report = h.namespace("ns").status_report();
    let svc_report = report.services.get(&ServiceId::from("echo-svc")).unwrap();
    assert!(
        svc_report.workload_conditions.contains_key("retry-backoff"),
        "status report should include 'retry-backoff' workload condition during backoff, got: {:?}",
        svc_report.workload_conditions
    );
}
