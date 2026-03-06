use std::time::Duration;

use distvirt_worker_protocol::WorkerEvent;

use distvirt_orchestrator::types::WorkloadId;

use crate::harness::mock_worker::MockWorkerConfig;
use crate::harness::*;

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
    h.assert_namespace_status("ns1", distvirt_orchestrator::types::NamespaceStatus::Active);

    // Activate → running → idle → suspending (handler hangs)
    h.activate_service("ns1", "web-svc").await;
    h.deactivate_service("ns1", "web-svc").await;
    h.advance_past_idle_timeout("ns1", "web-svc").await;
    h.assert_workload_suspending("ns1", "web");

    // Get the pod_id from the workload state so we can inject the right event.
    let pod_id = {
        let state = h.workload_state("ns1", "web");
        match state {
            distvirt_orchestrator::types::WorkloadState::Suspending { pod_id, .. } => {
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
            distvirt_orchestrator::types::WorkloadState::Suspending { pod_id, .. } => {
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
