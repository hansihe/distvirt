use std::net::Ipv4Addr;
use std::time::Duration;

use distvirt_worker_protocol::{BackendNeed, ServiceId, WorkerEvent};

use crate::harness::mock_worker::MockWorkerConfig;
use crate::harness::*;

/// Test: ResumePod fails, orchestrator should fall back to cold launch.
///
/// Flow:
/// 1. Activate → run → idle → suspend (full happy-path cycle)
/// 2. Re-activate → ResumePod sent → worker returns PodFailed
/// 3. Expected: workload enters RetryBackoff → cold launch via LaunchPod
///
/// Previously buggy: BecameUnready triggered DemandDown which zeroed demand_count
/// before retry logic could run. Fixed by queue-based output processing that
/// collects all outputs before processing side effects.
#[tokio::test(start_paused = true)]
async fn test_resume_failure_falls_back_to_cold_launch() {
    let mut h = TestHarness::new();

    // Use a worker with a pool that fails on ResumePod but succeeds on LaunchPod.
    let w1 = h.add_worker_with(MockWorkerConfig::with_resume_failure()).await;
    h.converge().await;

    // Create activation namespace.
    let spec = activation_spec(Duration::from_secs(30));
    h.create_namespace("ns1", spec).await;
    h.converge().await;
    h.assert_namespace_status("ns1", distvirt_orchestrator::types::NamespaceStatus::Active);

    // Workload starts dormant (activation-based).
    h.assert_workload_dormant("ns1", "web");
    h.assert_service_idle("ns1", "web-svc");

    // Activate via ServiceActivation (initial activation from Idle state).
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns1".into(),
        service_id: ServiceId::from("web-svc"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 100),
    });
    h.converge().await;

    // Workload should be running.
    h.assert_workload_running("ns1", "web");
    h.assert_service_active("ns1", "web-svc");

    // Signal no more traffic to start idle timer.
    h.worker(&w1).send_event(WorkerEvent::ServiceBackendNeed {
        namespace_id: "ns1".into(),
        service_id: ServiceId::from("web-svc"),
        need: BackendNeed::None,
    });
    h.converge().await;

    // Advance past idle timeout → suspend.
    h.advance_time(Duration::from_secs(31)).await;
    h.assert_workload_suspended("ns1", "web");
    h.assert_service_idle("ns1", "web-svc");

    // Re-activate via ServiceActivation.
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns1".into(),
        service_id: ServiceId::from("web-svc"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 100),
    });
    h.converge().await;

    // Fixed: Queue-based output processing ensures DemandDown from BecameUnready
    // doesn't zero demand_count before retry logic runs. The workload should enter
    // RetryBackoff, then after backoff, relaunch via cold LaunchPod.
    h.assert_workload_retry_backoff("ns1", "web");
    h.assert_service_need_backend("ns1", "web-svc");
}
