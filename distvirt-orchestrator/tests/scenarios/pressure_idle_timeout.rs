use std::net::Ipv4Addr;
use std::time::Duration;

use crate::harness::*;
use crate::harness::mock_worker::MockWorkerConfig;
use distvirt_orchestrator::types::*;
use distvirt_worker_protocol::{BackendNeed, ServiceId, WorkerEvent};

/// Helper: activate a service, wait for workload running, then signal no traffic.
async fn activate_then_idle(h: &mut TestHarness, w: &WorkerId) {
    h.worker(w).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 100),
    });
    h.converge().await;
    h.assert_workload_running("ns", "web");
    h.assert_service_active("ns", "web-svc");

    // Signal no more traffic — starts idle timer.
    h.worker(w).send_event(WorkerEvent::ServiceBackendNeed {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        need: BackendNeed::None,
    });
    h.converge().await;
}

/// Under Elevated pressure (memory=0.5), idle timeout should be 75% of configured.
/// With a 40s configured timeout, effective timeout = 30s.
/// At 31s the workload should still be active; at 31s it should have suspended.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_elevated_pressure_shortens_idle_timeout() {
    let mut h = TestHarness::new();
    // 256 MB worker: 1 pod @ 128 MB → memory pressure 0.5 → Elevated band.
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool_and_memory(256)).await;

    let timeout = Duration::from_secs(40);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;
    h.assert_workload_dormant("ns", "web");

    activate_then_idle(&mut h, &w1).await;

    // Effective timeout = 40s * 0.75 = 30s.
    // At 29s workload should still be active.
    h.advance_time(Duration::from_secs(29)).await;
    h.assert_service_active("ns", "web-svc");

    // At 31s (past 30s effective timeout), workload should have suspended.
    h.advance_time(Duration::from_secs(2)).await;
    h.assert_workload_suspended("ns", "web");
    h.assert_service_idle("ns", "web-svc");
}

/// Under Critical pressure (memory≈0.985), idle timeout should be the 5s floor
/// regardless of configured value.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_critical_pressure_uses_floor_timeout() {
    let mut h = TestHarness::new();
    // 130 MB worker: 1 pod @ 128 MB → memory pressure ≈ 0.985 → Critical band.
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool_and_memory(130)).await;

    let timeout = Duration::from_secs(60);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;
    h.assert_workload_dormant("ns", "web");

    activate_then_idle(&mut h, &w1).await;

    // Effective timeout = 5s (Critical floor).
    // At 4s workload should still be active.
    h.advance_time(Duration::from_secs(4)).await;
    h.assert_service_active("ns", "web-svc");

    // At 6s (past 5s floor), workload should have suspended.
    h.advance_time(Duration::from_secs(2)).await;
    h.assert_workload_suspended("ns", "web");
    h.assert_service_idle("ns", "web-svc");
}

/// Under Normal pressure (large worker), idle timeout should be unchanged.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_normal_pressure_keeps_full_timeout() {
    let mut h = TestHarness::new();
    // 4096 MB worker: 1 pod @ 128 MB → memory pressure ≈ 0.031 → Normal band.
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool_and_memory(4096)).await;

    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;
    h.assert_workload_dormant("ns", "web");

    activate_then_idle(&mut h, &w1).await;

    // At 29s workload should still be active (full 30s timeout).
    h.advance_time(Duration::from_secs(29)).await;
    h.assert_service_active("ns", "web-svc");

    // At 31s, workload should have suspended.
    h.advance_time(Duration::from_secs(2)).await;
    h.assert_workload_suspended("ns", "web");
    h.assert_service_idle("ns", "web-svc");
}

/// High pressure (memory=0.8): effective timeout = 25% of configured.
/// With 60s configured → 15s effective.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_high_pressure_quarter_timeout() {
    let mut h = TestHarness::new();
    // 160 MB worker: 1 pod @ 128 MB → memory pressure = 0.8 → High band.
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool_and_memory(160)).await;

    let timeout = Duration::from_secs(60);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;
    h.assert_workload_dormant("ns", "web");

    activate_then_idle(&mut h, &w1).await;

    // Effective timeout = 60s * 0.25 = 15s.
    // At 14s workload should still be active.
    h.advance_time(Duration::from_secs(14)).await;
    h.assert_service_active("ns", "web-svc");

    // At 16s (past 15s effective timeout), workload should have suspended.
    h.advance_time(Duration::from_secs(2)).await;
    h.assert_workload_suspended("ns", "web");
    h.assert_service_idle("ns", "web-svc");
}
