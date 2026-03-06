use std::time::Duration;

use crate::harness::*;
use crate::harness::mock_worker::MockWorkerConfig;

/// Helper: activate a service, wait for workload running, then signal no traffic.
async fn activate_then_idle(h: &mut TestHarness, ns_id: &str, svc_id: &str) {
    h.activate_service(ns_id, svc_id).await;
    h.deactivate_service(ns_id, svc_id).await;
}

/// Under Elevated pressure (memory=0.5), idle timeout should be 75% of configured.
/// With a 40s configured timeout, effective timeout = 30s.
// Low-level: pressure-adjusted timeouts require exact advance_time values
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_elevated_pressure_shortens_idle_timeout() {
    let mut h = TestHarness::new();
    // 256 MB worker: 1 pod @ 128 MB → memory pressure 0.5 → Elevated band.
    let _w1 = h.add_worker_with(MockWorkerConfig::with_pool_and_memory(256)).await;

    let timeout = Duration::from_secs(40);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;
    h.assert_workload_dormant("ns", "web");

    activate_then_idle(&mut h, "ns", "web-svc").await;

    // Effective timeout = 40s * 0.75 = 30s.
    // At 29s workload should still be active.
    h.advance_time(Duration::from_secs(29)).await;
    h.assert_service_active("ns", "web-svc");

    // At 31s (past 30s effective timeout), workload should have suspended.
    h.advance_time(Duration::from_secs(2)).await;
    h.assert_workload_suspended("ns", "web");
    h.assert_service_idle("ns", "web-svc");
}

/// Under Critical pressure (memory≈0.985), idle timeout should be the 5s floor.
// Low-level: pressure-adjusted timeouts require exact advance_time values
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_critical_pressure_uses_floor_timeout() {
    let mut h = TestHarness::new();
    // 130 MB worker: 1 pod @ 128 MB → memory pressure ≈ 0.985 → Critical band.
    let _w1 = h.add_worker_with(MockWorkerConfig::with_pool_and_memory(130)).await;

    let timeout = Duration::from_secs(60);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;
    h.assert_workload_dormant("ns", "web");

    activate_then_idle(&mut h, "ns", "web-svc").await;

    // Effective timeout = 5s (Critical floor).
    h.advance_time(Duration::from_secs(4)).await;
    h.assert_service_active("ns", "web-svc");

    h.advance_time(Duration::from_secs(2)).await;
    h.assert_workload_suspended("ns", "web");
    h.assert_service_idle("ns", "web-svc");
}

/// Under Normal pressure (large worker), idle timeout should be unchanged.
// Low-level: pressure-adjusted timeouts require exact advance_time values
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_normal_pressure_keeps_full_timeout() {
    let mut h = TestHarness::new();
    // 4096 MB worker: 1 pod @ 128 MB → memory pressure ≈ 0.031 → Normal band.
    let _w1 = h.add_worker_with(MockWorkerConfig::with_pool_and_memory(4096)).await;

    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;
    h.assert_workload_dormant("ns", "web");

    activate_then_idle(&mut h, "ns", "web-svc").await;

    // At 29s workload should still be active (full 30s timeout).
    h.advance_time(Duration::from_secs(29)).await;
    h.assert_service_active("ns", "web-svc");

    h.advance_time(Duration::from_secs(2)).await;
    h.assert_workload_suspended("ns", "web");
    h.assert_service_idle("ns", "web-svc");
}

/// High pressure (memory=0.8): effective timeout = 25% of configured.
// Low-level: pressure-adjusted timeouts require exact advance_time values
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_high_pressure_quarter_timeout() {
    let mut h = TestHarness::new();
    // 160 MB worker: 1 pod @ 128 MB → memory pressure = 0.8 → High band.
    let _w1 = h.add_worker_with(MockWorkerConfig::with_pool_and_memory(160)).await;

    let timeout = Duration::from_secs(60);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;
    h.assert_workload_dormant("ns", "web");

    activate_then_idle(&mut h, "ns", "web-svc").await;

    // Effective timeout = 60s * 0.25 = 15s.
    h.advance_time(Duration::from_secs(14)).await;
    h.assert_service_active("ns", "web-svc");

    h.advance_time(Duration::from_secs(2)).await;
    h.assert_workload_suspended("ns", "web");
    h.assert_service_idle("ns", "web-svc");
}
