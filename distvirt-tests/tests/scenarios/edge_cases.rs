use std::time::Duration;

use crate::harness::TestCluster;
use crate::harness::spec_builders::{activation_spec, always_on_spec};

/// Create then immediately delete before pod fully starts.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_create_delete_namespace_rapid() {
    let mut cluster = TestCluster::new();
    let _w1 = cluster.add_worker().await;

    // Create and delete without converging in between.
    cluster.create_namespace("ns", always_on_spec()).await;
    cluster.delete_namespace("ns").await;
    cluster.converge().await;

    cluster.assert_namespace_absent("ns");
}

/// Two independent namespaces on the same worker don't interfere.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_two_namespaces_on_same_worker() {
    let mut cluster = TestCluster::new();
    let _w1 = cluster.add_worker().await;

    cluster.create_namespace("ns-a", always_on_spec()).await;
    cluster.create_namespace("ns-b", always_on_spec()).await;
    cluster.converge().await;
    cluster.assert_workload_running("ns-a", "echo");
    cluster.assert_workload_running("ns-b", "echo");

    // Delete one, the other should continue.
    cluster.delete_namespace("ns-a").await;
    cluster.converge().await;
    cluster.assert_namespace_absent("ns-a");
    cluster.assert_workload_running("ns-b", "echo");
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
    cluster.assert_workload_dormant("ns-del", "web");

    // Activate -> Running -> deactivate -> suspend.
    cluster.send_activation_traffic("ns-del", "web-svc").await;
    cluster.assert_workload_running("ns-del", "web");

    cluster.deactivate_service("ns-del", "web-svc", &w1).await;
    cluster
        .advance_past_idle_timeout("ns-del", "web-svc")
        .await;
    cluster.wait_workload_suspended("ns-del", "web").await;

    // Delete the namespace while workload is suspended.
    cluster.delete_namespace("ns-del").await;
    cluster.converge().await;
    cluster.assert_namespace_absent("ns-del");
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
    cluster.assert_workload_running("ns-disc", "web");

    let hosting = cluster.worker_id_for_workload("ns-disc", "web");

    // Deactivate service.
    cluster
        .deactivate_service("ns-disc", "web-svc", &hosting)
        .await;

    // Advance past idle timeout to trigger suspend.
    cluster
        .advance_past_idle_timeout("ns-disc", "web-svc")
        .await;

    // Check current state.
    let state = cluster.workload_state("ns-disc", "web");
    let was_suspending = matches!(
        state,
        distvirt_orchestrator::types::WorkloadState::Active {
            pod: distvirt_orchestrator::types::PodSlot {
                pod_state: distvirt_orchestrator::types::PodState::Suspending { .. }, ..
            }, ..
        }
    );

    // Disconnect the hosting worker.
    cluster.disconnect_worker(&hosting).await;
    cluster.converge().await;

    // After disconnect, workload should be in a recoverable state.
    let state = cluster.workload_state("ns-disc", "web");
    let acceptable = matches!(
        state,
        distvirt_orchestrator::types::WorkloadState::Dormant
            | distvirt_orchestrator::types::WorkloadState::WaitingForCapacity
            | distvirt_orchestrator::types::WorkloadState::Suspended { .. }
            | distvirt_orchestrator::types::WorkloadState::Active { .. }
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
    let final_state = cluster.workload_state("ns-disc", "web");
    let ok = matches!(
        final_state,
        distvirt_orchestrator::types::WorkloadState::Active { .. }
            | distvirt_orchestrator::types::WorkloadState::Dormant
            | distvirt_orchestrator::types::WorkloadState::WaitingForCapacity
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
