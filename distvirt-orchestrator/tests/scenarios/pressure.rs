use std::time::Duration;

use crate::harness::*;
use crate::harness::mock_worker::MockWorkerConfig;
use distvirt_orchestrator::types::ServiceId;
use distvirt_worker_protocol::BackendNeed;

// ---------------------------------------------------------------------------
// pressure_scheduling tests
// ---------------------------------------------------------------------------

/// Two workers, both with same memory. After loading one worker with a pod,
/// inject Elevated PSI on that worker. A second namespace's pod should land
/// on the lower-pressure (Normal) worker.
#[test]
fn test_pod_scheduled_on_lower_pressure_worker() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool_and_memory(4096));

    // Load w1 with a pod (only worker, so it's guaranteed to land here).
    h.create_namespace("ns1", always_on_spec());
    h.converge();
    h.assert_workload_running("ns1", "echo");

    // Inject Elevated pressure on w1 via PSI (55% → Elevated band).
    h.send_pressure_update(&w1, 55.0);

    // Now add the second worker — it joins at Normal pressure.
    let w2 = h.add_worker_with(MockWorkerConfig::with_pool_and_memory(4096));

    // Create a second namespace — its pod should go to the Normal worker (w2).
    let mut spec2 = always_on_spec();
    spec2.workloads.iter_mut().for_each(|(_, wl)| {
        wl.network.ip = std::net::Ipv4Addr::new(172, 16, 0, 11);
    });
    h.create_namespace("ns2", spec2);
    h.converge();
    h.assert_workload_running("ns2", "echo");

    let ns1_worker = h.workload_global_worker_id("ns1", "echo").expect("expected worker_id");
    let ns2_worker = h.workload_global_worker_id("ns2", "echo").expect("expected worker_id");
    assert_eq!(ns1_worker, w1, "ns1 pod should be on w1 (only worker at time of creation)");
    assert_eq!(
        ns2_worker, w2,
        "ns2 pod should be scheduled on the Normal-pressure worker, not the Elevated one"
    );
}

/// When both workers are at Normal pressure, pod count is the tiebreaker.
/// Create one workload first (lands on one worker), then add a second workload via spec
/// update — now both workers are Active, so the second workload goes to the less-loaded one.
#[test]
fn test_pod_count_tiebreaker_at_same_pressure() {
    let mut h = TestHarness::new();
    let _w1 = h.add_worker_with(MockWorkerConfig::with_pool_and_memory(4096));
    let _w2 = h.add_worker_with(MockWorkerConfig::with_pool_and_memory(4096));

    // Start with a single-workload namespace so both workers become Active.
    h.create_namespace("ns", always_on_spec());
    h.converge();
    h.assert_workload_running("ns", "echo");

    // Now update to two workloads — the second should go to the other worker.
    h.update_namespace("ns", always_on_two_workloads_spec());
    h.converge();
    h.assert_workload_running("ns", "echo-a");
    h.assert_workload_running("ns", "echo-b");

    // The two workloads should be on different workers (pod count tiebreaker).
    let worker_a = h.workload_global_worker_id("ns", "echo-a").expect("expected worker_id");
    let worker_b = h.workload_global_worker_id("ns", "echo-b").expect("expected worker_id");
    assert_ne!(
        worker_a, worker_b,
        "two workloads at same pressure should be spread across workers (pod count tiebreaker)"
    );
}

/// When all workers report High PSI pressure, a new workload stays in WaitingForCapacity.
#[test]
fn test_all_workers_high_pressure_no_scheduling() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool_and_memory(4096));
    let w2 = h.add_worker_with(MockWorkerConfig::with_pool_and_memory(4096));

    // Fill each worker with a pod from separate namespaces.
    h.create_namespace("ns1", always_on_spec());
    h.converge();
    h.assert_workload_running("ns1", "echo");

    let mut spec2 = always_on_spec();
    spec2.workloads.iter_mut().for_each(|(_, wl)| {
        wl.network.ip = std::net::Ipv4Addr::new(172, 16, 0, 11);
    });
    h.create_namespace("ns2", spec2);
    h.converge();
    h.assert_workload_running("ns2", "echo");

    // Inject High PSI pressure on both workers (85% → High band).
    h.send_pressure_update(&w1, 85.0);
    h.send_pressure_update(&w2, 85.0);

    // A third namespace's workload should stay in WaitingForCapacity.
    let mut spec3 = always_on_spec();
    spec3.workloads.iter_mut().for_each(|(_, wl)| {
        wl.network.ip = std::net::Ipv4Addr::new(172, 16, 0, 12);
    });
    h.create_namespace("ns3", spec3);
    h.converge();

    h.assert_workload_waiting_for_capacity("ns3", "echo");
}

/// When a worker drops from High to lower pressure, a workload stuck in
/// WaitingForCapacity gets scheduled.
///
/// Scenario: two workers at High PSI, create a third namespace (stuck),
/// then clear pressure on one worker → third workload launches.
#[test]
fn test_pressure_relief_triggers_scheduling() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool_and_memory(4096));
    let w2 = h.add_worker_with(MockWorkerConfig::with_pool_and_memory(4096));

    // Fill both workers with pods.
    h.create_namespace("ns1", always_on_spec());
    h.converge();
    let mut spec2 = always_on_spec();
    spec2.workloads.iter_mut().for_each(|(_, wl)| {
        wl.network.ip = std::net::Ipv4Addr::new(172, 16, 0, 11);
    });
    h.create_namespace("ns2", spec2);
    h.converge();

    // Inject High PSI pressure on both workers.
    h.send_pressure_update(&w1, 85.0);
    h.send_pressure_update(&w2, 85.0);

    // Third namespace should be stuck.
    let mut spec3 = always_on_spec();
    spec3.workloads.iter_mut().for_each(|(_, wl)| {
        wl.network.ip = std::net::Ipv4Addr::new(172, 16, 0, 12);
    });
    h.create_namespace("ns3", spec3.clone());
    h.converge();
    h.assert_workload_waiting_for_capacity("ns3", "echo");

    // Clear pressure on w1 (inject 0% PSI → Normal band).
    h.send_pressure_update(&w1, 0.0);

    // The stuck workload should now be scheduled.
    h.assert_workload_running("ns3", "echo");
}

// ---------------------------------------------------------------------------
// pressure_idle_timeout tests
// ---------------------------------------------------------------------------

/// Helper: activate a service, wait for workload running, then signal no traffic.
fn activate_then_idle(h: &mut TestHarness, ns_id: &str, svc_id: &str) {
    h.activate_service(ns_id, svc_id);
    h.deactivate_service(ns_id, svc_id);
}

/// Under Elevated PSI pressure (some_avg10=55%), idle timeout should be 75% of configured.
/// With a 40s configured timeout, effective timeout = 30s.
///
/// Ignored: pressure-adjusted idle timeout is not yet plumbed into the new service SM.
#[test]
#[ignore]
fn test_elevated_pressure_shortens_idle_timeout() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool_and_memory(4096));

    let timeout = Duration::from_secs(40);
    h.create_namespace("ns", activation_spec(timeout));
    h.converge();
    h.assert_workload_dormant("ns", "web");

    // Inject Elevated pressure via PSI (55% → Elevated band).
    h.send_pressure_update(&w1, 55.0);

    activate_then_idle(&mut h, "ns", "web-svc");

    // Effective timeout = 40s * 0.75 = 30s.
    h.advance_time(Duration::from_secs(29));
    h.assert_service_active("ns", "web-svc");

    h.advance_time(Duration::from_secs(2));
    h.assert_workload_suspended("ns", "web");
    h.assert_service_idle("ns", "web-svc");
}

/// Under Critical PSI pressure (some_avg10=97%), idle timeout should be the 5s floor.
///
/// Ignored: pressure-adjusted idle timeout is not yet plumbed into the new service SM.
#[test]
#[ignore]
fn test_critical_pressure_uses_floor_timeout() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool_and_memory(4096));

    let timeout = Duration::from_secs(60);
    h.create_namespace("ns", activation_spec(timeout));
    h.converge();
    h.assert_workload_dormant("ns", "web");

    // Activate first (workload must be running before High+ PSI blocks scheduling).
    h.activate_service("ns", "web-svc");

    // Inject Critical pressure via PSI (97% → Critical band).
    h.send_pressure_update(&w1, 97.0);

    h.deactivate_service("ns", "web-svc");

    // Effective timeout = 5s (Critical floor).
    h.advance_time(Duration::from_secs(4));
    h.assert_service_active("ns", "web-svc");

    h.advance_time(Duration::from_secs(2));
    h.assert_workload_suspended("ns", "web");
    h.assert_service_idle("ns", "web-svc");
}

/// Under Normal pressure (large worker, no PSI injection), idle timeout should be unchanged.
#[test]
fn test_normal_pressure_keeps_full_timeout() {
    let mut h = TestHarness::new();
    let _w1 = h.add_worker_with(MockWorkerConfig::with_pool_and_memory(4096));

    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout));
    h.converge();
    h.assert_workload_dormant("ns", "web");

    activate_then_idle(&mut h, "ns", "web-svc");

    // At 29s workload should still be active (full 30s timeout).
    h.advance_time(Duration::from_secs(29));
    h.assert_service_active("ns", "web-svc");

    h.advance_time(Duration::from_secs(2));
    h.assert_workload_suspended("ns", "web");
    h.assert_service_idle("ns", "web-svc");
}

/// High PSI pressure (some_avg10=85%): effective timeout = 25% of configured.
///
/// Ignored: pressure-adjusted idle timeout is not yet plumbed into the new service SM.
#[test]
#[ignore]
fn test_high_pressure_quarter_timeout() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool_and_memory(4096));

    let timeout = Duration::from_secs(60);
    h.create_namespace("ns", activation_spec(timeout));
    h.converge();
    h.assert_workload_dormant("ns", "web");

    // Activate first (workload must be running before High+ PSI blocks scheduling).
    h.activate_service("ns", "web-svc");

    // Inject High pressure via PSI (85% → High band).
    h.send_pressure_update(&w1, 85.0);

    h.deactivate_service("ns", "web-svc");

    // Effective timeout = 60s * 0.25 = 15s.
    h.advance_time(Duration::from_secs(14));
    h.assert_service_active("ns", "web-svc");

    h.advance_time(Duration::from_secs(2));
    h.assert_workload_suspended("ns", "web");
    h.assert_service_idle("ns", "web-svc");
}

/// Re-activation during a pressure-shortened idle timer should cancel the timer.
/// The next idle cycle should get a fresh (still shortened) timer.
///
/// Ignored: pressure-adjusted idle timeout is not yet plumbed into the new service SM.
#[test]
#[ignore]
fn test_reactivation_cancels_shortened_idle_timer() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool_and_memory(4096));

    let timeout = Duration::from_secs(40);
    h.create_namespace("ns", activation_spec(timeout));
    h.converge();

    // Inject Elevated pressure via PSI (55% → Elevated band, 75% timeout).
    h.send_pressure_update(&w1, 55.0);

    // First activation cycle: activate, then signal idle.
    activate_then_idle(&mut h, "ns", "web-svc");

    // Effective timeout = 40s * 0.75 = 30s. Wait 20s (well within timeout).
    h.advance_time(Duration::from_secs(20));
    h.assert_service_active("ns", "web-svc");

    // Re-activate (traffic arrives again), then go idle again.
    // This should cancel the old timer and start a fresh one.
    h.send_event_to_service_worker(
        "ns",
        "web-svc",
        distvirt_worker_protocol::WorkerEvent::ServiceBackendNeed {
            namespace_id: "ns".into(),
            service_id: ServiceId::from("web-svc"),
            need: BackendNeed::Active,
        },
    );
    h.converge();
    h.assert_service_active("ns", "web-svc");

    // Go idle again — fresh timer starts now.
    h.deactivate_service("ns", "web-svc");

    // The fresh timer should be another 30s. At 29s, still active.
    h.advance_time(Duration::from_secs(29));
    h.assert_service_active("ns", "web-svc");

    // At 31s past the second idle signal, should have suspended.
    h.advance_time(Duration::from_secs(2));
    h.assert_workload_suspended("ns", "web");
}

/// When pressure changes between activation cycles, the new cycle's idle timeout
/// reflects the updated pressure band (adjustment happens at timer creation time).
///
/// Ignored: pressure-adjusted idle timeout is not yet plumbed into the new service SM.
#[test]
#[ignore]
fn test_pressure_change_between_cycles_updates_timeout() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool_and_memory(4096));

    let timeout = Duration::from_secs(40);
    h.create_namespace("ns", activation_spec(timeout));
    h.converge();

    // First cycle at Normal pressure (no PSI injection) — full 40s timeout.
    activate_then_idle(&mut h, "ns", "web-svc");
    h.advance_time(Duration::from_secs(39));
    h.assert_service_active("ns", "web-svc");
    h.advance_time(Duration::from_secs(2));
    h.assert_workload_suspended("ns", "web");

    // Inject Elevated pressure via PSI between cycles.
    h.send_pressure_update(&w1, 55.0);

    // Second cycle — should get 75% timeout = 30s.
    activate_then_idle(&mut h, "ns", "web-svc");

    h.advance_time(Duration::from_secs(29));
    h.assert_service_active("ns", "web-svc");
    h.advance_time(Duration::from_secs(2));
    h.assert_workload_suspended("ns", "web");
}

/// PSI pressure arrives after a workload is already running, then traffic stops,
/// and the shortened idle timeout kicks in.
///
/// Ignored: pressure-adjusted idle timeout is not yet plumbed into the new service SM.
#[test]
#[ignore]
fn test_psi_pressure_after_activation_shortens_idle_timeout() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool_and_memory(4096));

    let timeout = Duration::from_secs(40);
    h.create_namespace("ns", activation_spec(timeout));
    h.converge();

    // Activate service — workload starts running at Normal pressure.
    h.activate_service("ns", "web-svc");

    // PSI pressure arrives while workload is running.
    h.send_pressure_update(&w1, 85.0);

    // Deactivate — idle timer starts with High adjustment (40s * 0.25 = 10s).
    h.deactivate_service("ns", "web-svc");

    // At 9s, still active.
    h.advance_time(Duration::from_secs(9));
    h.assert_service_active("ns", "web-svc");

    // At 11s, should have suspended.
    h.advance_time(Duration::from_secs(2));
    h.assert_workload_suspended("ns", "web");
    h.assert_service_idle("ns", "web-svc");
}

// ---------------------------------------------------------------------------
// pressure_reschedule tests
// ---------------------------------------------------------------------------

/// Worker A (256 MB, Elevated pressure after 1 pod) holds a running workload.
/// Disconnect A → workload reschedules to B (4096 MB, Normal pressure).
/// After converge, workload should be running on the remaining worker.
#[test]
fn test_workload_reschedules_to_lower_pressure_worker_after_disconnect() {
    let mut h = TestHarness::new();

    // Worker A: small memory, will be Elevated after 1 pod.
    let _w_a = h.add_worker_with(MockWorkerConfig::with_pool_and_memory(256));
    // Worker B: large memory, stays Normal.
    let _w_b = h.add_worker_with(MockWorkerConfig::with_pool_and_memory(4096));

    h.create_namespace("ns", always_on_spec());
    h.converge();
    h.assert_workload_running("ns", "echo");

    let initial_worker = h.workload_global_worker_id("ns", "echo").expect("expected worker_id");

    // Disconnect the worker that has the workload.
    h.disconnect_worker(&initial_worker);
    h.converge();

    // After converge the scheduler should have placed it on the remaining worker.
    h.assert_workload_running("ns", "echo");

    let new_worker = h.workload_global_worker_id("ns", "echo").expect("expected worker_id");
    assert_ne!(
        new_worker, initial_worker,
        "workload should have moved to a different worker after disconnect"
    );
}
