use std::time::Duration;

use crate::harness::mock_worker::MockWorkerConfig;
use crate::harness::*;

/// Basic drain: set draining condition, verify it's visible, undrain clears it.
#[test]
#[ignore] // TODO: unimplemented since orchestrator refactor
fn test_drain_undrain_condition() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool());
    h.create_namespace("ns", activation_spec(Duration::from_secs(30)));
    h.converge();

    // Drain the worker.
    h.drain_worker(&w1);
    h.assert_worker_draining(&w1);

    // Undrain the worker.
    h.undrain_worker(&w1);
    h.assert_worker_not_draining(&w1);
}

/// Draining worker is excluded from scheduling: new activations go to a non-draining worker.
#[test]
#[ignore] // TODO: unimplemented since orchestrator refactor
fn test_drain_excludes_from_scheduling() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool());
    let w2 = h.add_worker_with(MockWorkerConfig::with_pool());
    h.create_namespace("ns", activation_spec(Duration::from_secs(30)));
    h.converge();

    // Drain w1.
    h.drain_worker(&w1);

    // Activate the service — workload should be scheduled on w2 (not w1).
    h.activate_service("ns", "web-svc");
    h.assert_workload_running("ns", "web");

    let worker_id = h
        .workload_global_worker_id("ns", "web")
        .expect("workload should have a worker");
    assert_eq!(
        worker_id, w2,
        "workload should be scheduled on non-draining worker w2, got {:?}",
        worker_id
    );
}

/// Existing pods on a draining worker continue running — drain doesn't force-stop them.
#[test]
#[ignore] // TODO: unimplemented since orchestrator refactor
fn test_drain_does_not_stop_existing_pods() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool());
    h.create_namespace("ns", activation_spec(Duration::from_secs(30)));
    h.converge();

    // Activate the workload on w1.
    h.activate_service("ns", "web-svc");
    h.assert_workload_running("ns", "web");

    // Drain w1 — pod should still be running.
    h.drain_worker(&w1);
    h.assert_workload_running("ns", "web");
}

/// After drain, pods deactivate on their normal idle timeout.
#[test]
#[ignore] // TODO: unimplemented since orchestrator refactor
fn test_drain_pods_deactivate_on_idle_timeout() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool());
    h.create_namespace("ns", activation_spec(Duration::from_secs(30)));
    h.converge();

    // Activate the workload.
    h.activate_service("ns", "web-svc");
    h.assert_workload_running("ns", "web");

    // Drain the worker.
    h.drain_worker(&w1);

    // Signal no more traffic, then advance past idle timeout.
    h.deactivate_service("ns", "web-svc");
    h.advance_past_idle_timeout("ns", "web-svc");

    // Workload should have suspended (suspend_on_idle=true).
    h.assert_workload_suspended("ns", "web");
}

/// When a draining worker is the only one and all pods drain, scheduling fails gracefully
/// (workload goes to WaitingForCapacity on re-activation).
#[test]
#[ignore] // TODO: unimplemented since orchestrator refactor
fn test_drain_single_worker_no_scheduling() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool());
    h.create_namespace("ns", activation_spec(Duration::from_secs(30)));
    h.converge();

    // Drain the only worker.
    h.drain_worker(&w1);

    // Try to activate — workload should be stuck in WaitingForCapacity.
    let svc_ip = h.service_ip("ns", "web-svc");
    h.worker(&w1)
        .send_event(distvirt_worker_protocol::WorkerEvent::EndpointDemandTraffic {
            namespace_id: "ns".into(),
            ip: svc_ip,
            service_id: Some(h.proto_service_id("ns", "web-svc")),
        });
    h.converge();
    h.assert_workload_waiting_for_capacity("ns", "web");

    // Undrain — workload should now be schedulable.
    h.undrain_worker(&w1);
    // Trigger scheduling by sending a pressure update.
    h.send_pressure_update(&w1, 0.0);
    h.assert_workload_running("ns", "web");
}
