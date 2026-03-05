use std::net::Ipv4Addr;
use std::time::Duration;

use crate::harness::*;
use crate::harness::mock_worker::MockWorkerConfig;
use distvirt_orchestrator::types::*;
use distvirt_worker_protocol::{BackendNeed, ServiceId, WorkerEvent};

/// Pod is Launching on worker. Worker disconnects.
/// Workload should go WaitingForCapacity.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_worker_disconnect_during_launch() {
    let config = MockWorkerConfig::with_launch_hang();
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(config).await;
    h.create_namespace("ns", always_on_spec()).await;
    h.converge().await;
    h.assert_workload_launching("ns", "echo");

    // Disconnect the worker hosting the launching pod
    h.disconnect_worker(&w1);
    h.converge().await;

    // Workload should be WaitingForCapacity (no other worker available)
    h.assert_workload_waiting_for_capacity("ns", "echo");
}

/// Pod is Suspending. Worker disconnects. Artifact is lost.
/// Workload should handle gracefully.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_worker_disconnect_during_suspend() {
    let config = MockWorkerConfig::with_suspend_hang().add_pool();
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(config).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;

    // Activate → running
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 100),
    });
    h.converge().await;
    h.assert_workload_running("ns", "web");

    // Idle → begin suspending
    h.worker(&w1).send_event(WorkerEvent::ServiceBackendNeed {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        need: BackendNeed::None,
    });
    h.converge().await;
    h.advance_time(timeout + Duration::from_secs(1)).await;
    h.assert_workload_suspending("ns", "web");

    // Disconnect worker during suspend
    h.disconnect_worker(&w1);
    h.converge().await;

    // Workload should not panic and should transition to a safe state
    let state = h.workload_state("ns", "web");
    assert!(
        !matches!(state, WorkloadState::Suspending { .. }),
        "workload should not be stuck in Suspending after worker disconnect, got {:?}",
        state
    );
}

/// Pod is Resuming from Suspended. Worker disconnects. Artifact may be lost.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_worker_disconnect_during_resume() {
    // Need a handler that hangs on ResumePod but handles SuspendPod normally
    let config = MockWorkerConfig {
        handler: Some(Box::new(|cmd| match cmd {
            distvirt_worker_protocol::WorkerCommand::ResumePod { .. } => Some(vec![]),
            _ => None,
        })),
        capabilities: MockWorkerConfig::with_pool().capabilities,
    };
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(config).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;

    // Activate → running
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 100),
    });
    h.converge().await;
    h.assert_workload_running("ns", "web");

    // Idle → suspend
    h.worker(&w1).send_event(WorkerEvent::ServiceBackendNeed {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        need: BackendNeed::None,
    });
    h.converge().await;
    h.advance_time(timeout + Duration::from_secs(1)).await;
    h.assert_workload_suspended("ns", "web");

    // Re-activate → resuming (hangs)
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 100),
    });
    h.converge().await;
    h.assert_workload_resuming("ns", "web");

    // Disconnect worker during resume
    h.disconnect_worker(&w1);
    h.converge().await;

    // Workload should transition out of Resuming
    let state = h.workload_state("ns", "web");
    assert!(
        !matches!(state, WorkloadState::Resuming { .. }),
        "workload should not be stuck in Resuming after worker disconnect, got {:?}",
        state
    );
}

/// Running workload, all workers disconnect. Workload goes WaitingForCapacity.
/// No panic, no infinite loop.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_all_workers_disconnect() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker().await;
    h.create_namespace("ns", always_on_spec()).await;
    h.converge().await;
    h.assert_workload_running("ns", "echo");

    // Disconnect the only worker
    h.disconnect_worker(&w1);
    h.converge().await;

    // Workload should be waiting for capacity
    h.assert_workload_waiting_for_capacity("ns", "echo");
    h.assert_worker_count(0);
}

/// Worker holds artifacts in placement table. Disconnect.
/// Verify placement entries for that worker are removed.
///
/// BUG: `handle_worker_lost` only sends WorkerLost to workloads whose `worker_id()`
/// matches the disconnected worker. `WorkloadState::Suspended` has no worker_id
/// (it only stores artifact_id), so suspended workloads with artifacts on the
/// disconnected worker are never notified. The placement table entry IS removed
/// (via `remove_by_worker`), but the workload stays in Suspended with a stale
/// artifact_id pointing to a non-existent placement.
///
/// Fix: after removing placements, also check for Suspended workloads whose
/// artifact_id was in the removed set and send them WorkerLost or transition
/// them appropriately.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_worker_disconnect_clears_placements() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;

    // Activate → running
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 100),
    });
    h.converge().await;
    h.assert_workload_running("ns", "web");

    // Idle → suspended (creates an artifact placement)
    h.worker(&w1).send_event(WorkerEvent::ServiceBackendNeed {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        need: BackendNeed::None,
    });
    h.converge().await;
    h.advance_time(timeout + Duration::from_secs(1)).await;
    h.assert_workload_suspended("ns", "web");

    // Verify there's an artifact
    let _artifact_id = match h.workload_state("ns", "web") {
        WorkloadState::Suspended { artifact_id } => artifact_id.clone(),
        other => panic!("expected Suspended, got {:?}", other),
    };

    // Disconnect worker
    h.disconnect_worker(&w1);
    h.converge().await;

    // BUG: Workload stays in Suspended with a stale artifact_id even though the
    // placement was removed from the table. See docstring for details.
    // When this bug is fixed, the workload should transition to Dormant or
    // WaitingForCapacity.
    h.assert_workload_suspended("ns", "web");
}
