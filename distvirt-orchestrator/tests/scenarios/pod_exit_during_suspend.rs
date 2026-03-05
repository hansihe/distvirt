use std::time::Duration;

use distvirt_worker_protocol::{BackendNeed, ServiceId, WorkerEvent};

use crate::harness::mock_worker::MockWorkerConfig;
use crate::harness::*;

/// Test: Pod crashes (exits unexpectedly) while in the Suspending state.
///
/// Flow:
/// 1. Activate → run → idle → begin suspending (suspend hangs)
/// 2. Inject PodFailed while workload is in Suspending state
/// 3. Verify: workload transitions out of Suspending cleanly (not stuck)
///
/// The orchestrator should treat this as a pod failure (PodGone), clear the
/// suspend state, and transition appropriately. Since demand is still down,
/// the workload should end up Dormant (not stuck in Suspending).
#[tokio::test(start_paused = true)]
async fn test_pod_exit_during_suspend() {
    let mut h = TestHarness::new();

    // Worker with suspend hang (no response to SuspendPod) + pool.
    let w1 = h
        .add_worker_with(MockWorkerConfig::with_suspend_hang().add_pool())
        .await;
    h.converge().await;

    // Create activation namespace with pool support.
    let spec = activation_spec(Duration::from_secs(30));
    h.create_namespace("ns1", spec).await;
    h.converge().await;
    h.assert_namespace_status("ns1", distvirt_orchestrator::types::NamespaceStatus::Active);

    // Activate via ServiceActivation.
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns1".into(),
        service_id: ServiceId::from("web-svc"),
        dst_ip: std::net::Ipv4Addr::new(172, 16, 0, 100),
    });
    h.converge().await;
    h.assert_workload_running("ns1", "web");

    // Signal no traffic → start idle timer.
    h.worker(&w1).send_event(WorkerEvent::ServiceBackendNeed {
        namespace_id: "ns1".into(),
        service_id: ServiceId::from("web-svc"),
        need: BackendNeed::None,
    });
    h.converge().await;

    // Advance past idle timeout → enters Suspending (handler hangs).
    h.advance_time(Duration::from_secs(31)).await;
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

    // Workload should NOT be stuck in Suspending.
    let state = h.workload_state("ns1", "web");
    assert!(
        !matches!(state, distvirt_orchestrator::types::WorkloadState::Suspending { .. }),
        "workload should not be stuck in Suspending after pod crash, got {:?}",
        state
    );

    // Since demand is down (no active services), the workload should be in a
    // non-running state. Depending on implementation, this could be Dormant,
    // RetryBackoff, or WaitingForCapacity.
    // The key assertion is: NOT stuck in Suspending.
    //
    // With the current implementation, PodFailed during Suspending triggers PodGone
    // which should handle the transition. Let's verify the specific state:
    let state = h.workload_state("ns1", "web");
    match state {
        distvirt_orchestrator::types::WorkloadState::Dormant
        | distvirt_orchestrator::types::WorkloadState::RetryBackoff { .. } => {
            // Both are acceptable: Dormant means clean recovery,
            // RetryBackoff means it counted as a failure (also fine).
        }
        other => {
            // Document whatever state we actually get — this test is primarily
            // about not being stuck in Suspending.
            panic!(
                "unexpected workload state after pod crash during suspend: {:?}. \
                 Expected Dormant or RetryBackoff.",
                other
            );
        }
    }
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

    // Activate via ServiceActivation → run.
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns1".into(),
        service_id: ServiceId::from("web-svc"),
        dst_ip: std::net::Ipv4Addr::new(172, 16, 0, 100),
    });
    h.converge().await;
    h.assert_workload_running("ns1", "web");

    // Signal no traffic → start idle timer.
    h.worker(&w1).send_event(WorkerEvent::ServiceBackendNeed {
        namespace_id: "ns1".into(),
        service_id: ServiceId::from("web-svc"),
        need: BackendNeed::None,
    });
    h.converge().await;
    h.advance_time(Duration::from_secs(31)).await;
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

    // Should not be stuck in Suspending.
    let state = h.workload_state("ns1", "web");
    assert!(
        !matches!(state, distvirt_orchestrator::types::WorkloadState::Suspending { .. }),
        "workload should not be stuck in Suspending after PodExited, got {:?}",
        state
    );
}
