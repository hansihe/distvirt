use std::time::Duration;

use crate::harness::TestCluster;
use crate::harness::spec_builders::{
    activation_spec, always_on_spec, two_activation_workloads_spec,
};

/// Pod goes to lower-pressure worker.
///
/// Uses an activation-based workload so both workers have Active fabric
/// before scheduling runs. With always_on workloads, scheduling fires on the
/// first FabricActive event — whichever worker responds first gets the pod,
/// regardless of pressure (since Elevated still allows scheduling).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_pressure_based_scheduling() {
    let mut cluster = TestCluster::new();
    let w1 = cluster.add_worker().await;
    let _w2 = cluster.add_worker().await;

    // Create activation-based namespace — workload starts Dormant.
    cluster
        .create_namespace("ns", activation_spec(Duration::from_secs(30)))
        .await;
    cluster.converge().await;
    cluster.assert_workload_dormant("ns", "web").await;

    // Inject elevated memory pressure on w1 (60/100 = 0.6 → Elevated band).
    // Both workers already have Active fabric for this namespace.
    cluster.inject_pressure(&w1, 60.0).await;

    // Activate — scheduling runs with both workers Active, should prefer w2 (Normal).
    cluster.send_activation_traffic("ns", "web-svc").await;
    cluster.assert_workload_running("ns", "web").await;

    let hosting = cluster.worker_id_for_workload("ns", "web").await;
    assert_ne!(hosting, w1, "workload should avoid pressured worker w1");
}

/// Elevated memory pressure reduces idle timeout.
/// Orchestrator applies a 0.75 factor at Elevated band.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_pressure_shortens_idle_timeout() {
    let mut cluster = TestCluster::new();
    let w1 = cluster.add_worker().await;

    cluster
        .create_namespace("ns", activation_spec(Duration::from_secs(40)))
        .await;
    cluster.converge().await;
    cluster.assert_workload_dormant("ns", "web").await;

    // Activate.
    cluster.send_activation_traffic("ns", "web-svc").await;
    cluster.assert_workload_running("ns", "web").await;

    // Inject elevated pressure BEFORE deactivation so idle timer picks it up.
    // 60/100 = 0.6 → Elevated → 75% factor → effective timeout = 30s.
    cluster.inject_pressure(&w1, 60.0).await;

    // Deactivate.
    cluster.deactivate_service("ns", "web-svc", &w1).await;

    // Advance 31s — past 40s * 0.75 = 30s adjusted timeout.
    tokio::time::advance(Duration::from_secs(31)).await;

    // Converge to process pending events from the 31s advance.
    cluster.converge().await;

    // Should be suspended or suspending since pressure shortening fired.
    let state = cluster.workload_status_str("ns", "web").await;
    assert!(
        state == "suspended" || state == "suspending",
        "expected Suspended/Suspending after pressure-shortened idle timeout, got {:?}",
        state
    );

    // Wait for suspended state.
    cluster.wait_workload_suspended("ns", "web").await;
    cluster.assert_workload_suspended("ns", "web").await;
}

/// When all workers are at High/Critical pressure, new pods get WaitingForCapacity.
/// When pressure is released, the stuck workload should be rescheduled.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_high_pressure_blocks_scheduling() {
    let mut cluster = TestCluster::new();
    let w1 = cluster.add_worker().await;

    // High pressure (85/100 = 0.85 → High band).
    cluster.inject_pressure(&w1, 85.0).await;

    cluster.create_namespace("ns", always_on_spec()).await;
    cluster.converge().await;
    cluster.assert_workload_waiting_for_capacity("ns", "echo").await;

    // Release pressure.
    cluster.inject_pressure(&w1, 0.0).await;

    // PressureUpdate now flows through Orchestrator::step(), which calls
    // schedule_waiting_pods() after recomputing pressure. Workloads stuck in
    // WaitingForCapacity are automatically reconsidered when pressure drops.
    cluster.converge().await;
    cluster.assert_workload_running("ns", "echo").await;
}

/// Under high pressure, activating a second workload should preempt the idle first one.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_basic_preemption_e2e() {
    let mut cluster = TestCluster::new();
    let w1 = cluster.add_worker().await;
    let spec = two_activation_workloads_spec(Duration::from_secs(30));

    cluster.create_namespace("ns", spec.clone()).await;
    cluster.converge().await;
    cluster.assert_workload_dormant("ns", "wl-a").await;
    cluster.assert_workload_dormant("ns", "wl-b").await;

    // Activate wl-a via svc-a.
    cluster.send_activation_traffic("ns", "svc-a").await;
    cluster.assert_workload_running("ns", "wl-a").await;

    // Deactivate svc-a (wl-a becomes idle but still running).
    cluster.deactivate_service("ns", "svc-a", &w1).await;

    // Inject high pressure.
    cluster.inject_pressure(&w1, 85.0).await;

    // Activate wl-b via svc-b — orchestrator should preempt idle wl-a for capacity.
    cluster.send_activation_traffic("ns", "svc-b").await;

    // wl-a should be preempted (no longer Running).
    let wl_a_state = cluster.workload_status_str("ns", "wl-a").await;
    assert!(
        wl_a_state != "running",
        "wl-a should be preempted (not Running), got {:?}",
        wl_a_state
    );

    // Simulate the host recovering: the preempted pod freed memory, so real
    // PSI would decrease. Inject released pressure.
    cluster.inject_pressure(&w1, 0.0).await;

    // PressureUpdate flows through step() → schedule_waiting_pods(), so wl-b
    // is automatically rescheduled when pressure drops after preemption.
    cluster.converge().await;
    cluster.assert_workload_running("ns", "wl-b").await;
}

/// Pressure recovery: multiple workloads stuck in WaitingForCapacity should
/// all be rescheduled when pressure drops back to Normal.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_pressure_recovery_reschedules_all_waiting() {
    let mut cluster = TestCluster::new();
    let w1 = cluster.add_worker().await;

    // High pressure blocks all scheduling.
    cluster.inject_pressure(&w1, 85.0).await;

    // Create two always-on namespaces — both should get stuck.
    cluster.create_namespace("ns-a", always_on_spec()).await;
    cluster.create_namespace("ns-b", always_on_spec()).await;
    cluster.converge().await;
    cluster.assert_workload_waiting_for_capacity("ns-a", "echo").await;
    cluster.assert_workload_waiting_for_capacity("ns-b", "echo").await;

    // Release pressure.
    cluster.inject_pressure(&w1, 0.0).await;
    cluster.converge().await;

    // Both should now be scheduled and running.
    cluster.assert_workload_running("ns-a", "echo").await;
    cluster.assert_workload_running("ns-b", "echo").await;
}
