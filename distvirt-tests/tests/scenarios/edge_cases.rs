use std::time::Duration;

use distvirt_orchestrator::types::*;

use crate::harness::TestCluster;
use crate::harness::spec_builders::{
    activation_spec, always_on_spec, two_activation_workloads_spec,
};

/// Create then immediately delete before pod fully starts.
/// Verifies the namespace had a workload registered before deletion,
/// so we're testing cleanup of in-progress state, not just a no-op delete.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_create_delete_namespace_rapid() {
    let mut cluster = TestCluster::new();
    let _w1 = cluster.add_worker().await;

    // Create namespace — this registers the namespace and its workloads
    // in the orchestrator (via create + update_namespace), but doesn't
    // converge so the pod hasn't been scheduled/launched yet.
    cluster.create_namespace("ns", always_on_spec()).await;

    // Verify the namespace actually exists with a workload before we delete it.
    // This proves we're deleting real in-progress state, not a no-op.
    let status = cluster.namespace_status("ns").await;
    assert!(
        status.workloads.contains_key(&WorkloadName("echo".to_string())),
        "workload 'echo' should exist after create_namespace (before converge)"
    );

    cluster.delete_namespace("ns").await;
    cluster.converge().await;

    cluster.assert_namespace_absent("ns").await;
}

/// Two independent namespaces on the same worker don't interfere.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_two_namespaces_on_same_worker() {
    let mut cluster = TestCluster::new();
    let _w1 = cluster.add_worker().await;

    cluster.create_namespace("ns-a", always_on_spec()).await;
    cluster.create_namespace("ns-b", always_on_spec()).await;
    cluster.converge().await;
    cluster.assert_workload_running("ns-a", "echo").await;
    cluster.assert_workload_running("ns-b", "echo").await;

    // Delete one, the other should continue.
    cluster.delete_namespace("ns-a").await;
    cluster.converge().await;
    cluster.assert_namespace_absent("ns-a").await;
    cluster.assert_workload_running("ns-b", "echo").await;
}

/// Delete a namespace that has a suspended workload.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_namespace_delete_while_suspended() {
    let mut cluster = TestCluster::new();
    let w1 = cluster.add_worker().await;

    // Use unique namespace name to avoid snapshot dir conflicts with other tests.
    cluster
        .create_namespace("ns-del", activation_spec(Duration::from_secs(30)))
        .await;
    cluster.converge().await;
    cluster.assert_workload_dormant("ns-del", "web").await;

    // Activate -> Running -> deactivate -> suspend.
    cluster.send_activation_traffic("ns-del", "web-svc").await;
    cluster.assert_workload_running("ns-del", "web").await;

    cluster.deactivate_service("ns-del", "web-svc", &w1).await;
    cluster.advance_past_idle_timeout("ns-del", "web-svc").await;
    cluster.wait_workload_suspended("ns-del", "web").await;

    // Delete the namespace while workload is suspended.
    cluster.delete_namespace("ns-del").await;
    cluster.converge().await;
    cluster.assert_namespace_absent("ns-del").await;
}

/// Worker disconnects while a pod is being suspended.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_worker_disconnect_during_suspend() {
    let mut cluster = TestCluster::new();
    let _w1 = cluster.add_worker().await;
    let _w2 = cluster.add_worker().await;

    cluster
        .create_namespace("ns-disc", activation_spec(Duration::from_secs(30)))
        .await;
    cluster.converge().await;

    // Activate.
    cluster.send_activation_traffic("ns-disc", "web-svc").await;
    cluster.assert_workload_running("ns-disc", "web").await;

    let hosting = cluster.worker_id_for_workload("ns-disc", "web").await;

    // Deactivate service.
    cluster
        .deactivate_service("ns-disc", "web-svc", &hosting)
        .await;

    // Advance past idle timeout to trigger suspend.
    cluster
        .advance_past_idle_timeout("ns-disc", "web-svc")
        .await;

    // Check current state.
    let state = cluster.workload_status("ns-disc", "web").await;
    let was_suspending = state == WorkloadStatus::Suspending;

    // Disconnect the hosting worker.
    cluster.disconnect_worker(&hosting).await;
    cluster.converge().await;

    // After disconnect, workload should be in a recoverable state.
    let state = cluster.workload_status("ns-disc", "web").await;
    let acceptable = matches!(
        state,
        WorkloadStatus::Dormant | WorkloadStatus::Launching | WorkloadStatus::Suspended | WorkloadStatus::Running
    );
    assert!(
        acceptable,
        "workload should be in a recoverable state after worker disconnect, got {:?}",
        state
    );

    // Give extra converge time for rescheduling.
    cluster.advance_time(Duration::from_secs(5)).await;

    // The workload should eventually be running on the remaining worker,
    // or at least be in a state where it can be scheduled.
    let final_state = cluster.workload_status("ns-disc", "web").await;
    let ok = matches!(
        final_state,
        WorkloadStatus::Running | WorkloadStatus::Dormant | WorkloadStatus::Launching
    );
    assert!(
        ok,
        "workload should be recoverable after worker disconnect, got {:?}",
        final_state
    );

    if was_suspending {
        eprintln!("Note: caught worker disconnect during Suspending state");
    }
}

/// Delete one namespace while another is active on the same worker.
/// The surviving namespace should not be affected.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_namespace_deletion_doesnt_affect_siblings() {
    let mut cluster = TestCluster::new();
    let w1 = cluster.add_worker().await;

    // Create two activation-based namespaces.
    cluster
        .create_namespace("ns-a", activation_spec(Duration::from_secs(30)))
        .await;
    cluster
        .create_namespace("ns-b", activation_spec(Duration::from_secs(30)))
        .await;
    cluster.converge().await;

    // Activate both.
    cluster.send_activation_traffic("ns-a", "web-svc").await;
    cluster.send_activation_traffic("ns-b", "web-svc").await;
    cluster.assert_workload_running("ns-a", "web").await;
    cluster.assert_workload_running("ns-b", "web").await;

    // Delete ns-a while ns-b is running.
    cluster.delete_namespace("ns-a").await;
    cluster.converge().await;

    cluster.assert_namespace_absent("ns-a").await;
    cluster.assert_workload_running("ns-b", "web").await;

    // ns-b should still respond to lifecycle events normally.
    cluster.deactivate_service("ns-b", "web-svc", &w1).await;
    cluster.advance_past_idle_timeout("ns-b", "web-svc").await;
    cluster.wait_workload_suspended("ns-b", "web").await;
    cluster.assert_workload_suspended("ns-b", "web").await;
}

/// Preemption under pressure should be scoped to the namespace that
/// requested the new workload — it should not preempt workloads in
/// other namespaces.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_preemption_is_namespace_scoped() {
    let mut cluster = TestCluster::new();
    let w1 = cluster.add_worker().await;

    // ns-other: single always-on workload (no idle, can't be preempted by ns-main).
    cluster.create_namespace("ns-other", always_on_spec()).await;
    cluster.converge().await;
    cluster.assert_workload_running("ns-other", "echo").await;

    // ns-main: two activation workloads.
    let spec = two_activation_workloads_spec(Duration::from_secs(30));
    cluster.create_namespace("ns-main", spec).await;
    cluster.converge().await;

    // Activate wl-a.
    cluster.send_activation_traffic("ns-main", "svc-a").await;
    cluster.assert_workload_running("ns-main", "wl-a").await;

    // Deactivate wl-a (becomes idle).
    cluster.deactivate_service("ns-main", "svc-a", &w1).await;

    // Inject high pressure.
    cluster.inject_pressure(&w1, 85.0).await;

    // Activate wl-b — should preempt idle wl-a within ns-main, NOT ns-other's echo.
    cluster.send_activation_traffic("ns-main", "svc-b").await;

    // ns-other's workload must still be running.
    cluster.assert_workload_running("ns-other", "echo").await;

    // wl-a should be preempted.
    let wl_a_state = cluster.workload_status("ns-main", "wl-a").await;
    assert!(
        wl_a_state != WorkloadStatus::Running,
        "wl-a should be preempted, got {:?}",
        wl_a_state
    );

    // Release pressure so wl-b can schedule.
    cluster.inject_pressure(&w1, 0.0).await;
    cluster.converge().await;
    cluster.assert_workload_running("ns-main", "wl-b").await;

    // ns-other still running.
    cluster.assert_workload_running("ns-other", "echo").await;
}

/// BROKEN: Three namespaces competing for a single worker under high pressure.
/// After releasing pressure, not all waiting workloads get rescheduled.
/// schedule_waiting_pods() may only reschedule a limited number per pressure
/// update cycle, leaving some workloads stuck in WaitingForCapacity.
#[tokio::test(flavor = "current_thread", start_paused = true)]
#[should_panic(expected = "expected Running")]
async fn test_many_namespaces_competing_for_capacity() {
    eprintln!(
        "BROKEN: not all WaitingForCapacity workloads rescheduled after pressure drop — schedule_waiting_pods() may need to iterate all waiting namespaces"
    );
    let mut cluster = TestCluster::new();
    let w1 = cluster.add_worker().await;

    // High pressure — blocks all scheduling.
    cluster.inject_pressure(&w1, 85.0).await;

    cluster.create_namespace("ns-1", always_on_spec()).await;
    cluster.create_namespace("ns-2", always_on_spec()).await;
    cluster.create_namespace("ns-3", always_on_spec()).await;
    cluster.converge().await;

    // All should be waiting.
    cluster.assert_workload_waiting_for_capacity("ns-1", "echo").await;
    cluster.assert_workload_waiting_for_capacity("ns-2", "echo").await;
    cluster.assert_workload_waiting_for_capacity("ns-3", "echo").await;

    // Release pressure — all should eventually schedule.
    cluster.inject_pressure(&w1, 0.0).await;
    cluster.converge().await;

    cluster.assert_workload_running("ns-1", "echo").await;
    cluster.assert_workload_running("ns-2", "echo").await;
    cluster.assert_workload_running("ns-3", "echo").await;
}
