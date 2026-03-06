use crate::harness::*;
use crate::harness::mock_worker::MockWorkerConfig;
use distvirt_orchestrator::types::*;

/// Two workers at different memory capacities. After loading one worker with a pod
/// (pushing it to Elevated pressure), a second namespace's pod should land on
/// the lower-pressure (Normal) worker.
///
/// We use a single-worker namespace first to deterministically load the small worker,
/// then add the second worker before creating namespace 2.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_pod_scheduled_on_lower_pressure_worker() {
    let mut h = TestHarness::new();
    // Worker 1: 256 MB → after 1 pod at 128 MB → 0.5 pressure → Elevated
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool_and_memory(256)).await;

    // Load w1 with a pod (only worker, so it's guaranteed to land here).
    h.create_namespace("ns1", always_on_spec()).await;
    h.converge().await;
    h.assert_workload_running("ns1", "echo");

    // Now add the big worker — it joins at Normal pressure.
    let w2 = h.add_worker_with(MockWorkerConfig::with_pool_and_memory(4096)).await;

    // Create a second namespace — its pod should go to the Normal worker (w2).
    let mut spec2 = always_on_spec();
    spec2.workloads.iter_mut().for_each(|(_, wl)| {
        wl.network.ip = std::net::Ipv4Addr::new(172, 16, 0, 11);
    });
    h.create_namespace("ns2", spec2).await;
    h.converge().await;
    h.assert_workload_running("ns2", "echo");

    let ns1_worker = h.workload_state("ns1", "echo").worker_id().unwrap().clone();
    let ns2_worker = h.workload_state("ns2", "echo").worker_id().unwrap().clone();
    assert_eq!(ns1_worker, w1, "ns1 pod should be on the small worker (only worker at time of creation)");
    assert_eq!(
        ns2_worker, w2,
        "ns2 pod should be scheduled on the Normal-pressure worker (4096 MB), not the Elevated one (256 MB)"
    );
}

/// When both workers are at Normal pressure, pod count (per-namespace) is the tiebreaker.
/// Create one workload first (lands on one worker), then add a second workload via spec
/// update — now both workers are Active, so the second workload goes to the less-loaded one.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_pod_count_tiebreaker_at_same_pressure() {
    let mut h = TestHarness::new();
    let _w1 = h.add_worker_with(MockWorkerConfig::with_pool_and_memory(4096)).await;
    let _w2 = h.add_worker_with(MockWorkerConfig::with_pool_and_memory(4096)).await;

    // Start with a single-workload namespace so both workers become Active.
    h.create_namespace("ns", always_on_spec()).await;
    h.converge().await;
    h.assert_workload_running("ns", "echo");

    // Now update to two workloads — the second should go to the other worker.
    h.update_namespace("ns", always_on_two_workloads_spec()).await;
    h.converge().await;
    h.assert_workload_running("ns", "echo-a");
    h.assert_workload_running("ns", "echo-b");

    // The two workloads should be on different workers (pod count tiebreaker).
    let worker_a = h.workload_state("ns", "echo-a").worker_id().unwrap().clone();
    let worker_b = h.workload_state("ns", "echo-b").worker_id().unwrap().clone();
    assert_ne!(
        worker_a, worker_b,
        "two workloads at same pressure should be spread across workers (pod count tiebreaker)"
    );
}

/// When all workers are under enough memory pressure from existing pods,
/// a new workload stays in WaitingForCapacity.
///
/// We achieve High memory pressure (≥0.80) by loading enough pods:
/// With 160 MB worker and DEFAULT_POD_MEMORY_MB=128, 1 pod = 0.8 → High.
/// So we first fill each worker with a pod from separate namespaces,
/// then create a third namespace whose workload can't be scheduled.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_all_workers_high_pressure_no_scheduling() {
    let mut h = TestHarness::new();
    // 160 MB workers: 1 pod → 128/160 = 0.8 → High.
    let _w1 = h.add_worker_with(MockWorkerConfig::with_pool_and_memory(160)).await;
    let _w2 = h.add_worker_with(MockWorkerConfig::with_pool_and_memory(160)).await;

    // Fill each worker with a pod.
    h.create_namespace("ns1", always_on_spec()).await;
    h.converge().await;
    h.assert_workload_running("ns1", "echo");

    let mut spec2 = always_on_spec();
    spec2.workloads.iter_mut().for_each(|(_, wl)| {
        wl.network.ip = std::net::Ipv4Addr::new(172, 16, 0, 11);
    });
    h.create_namespace("ns2", spec2).await;
    h.converge().await;
    h.assert_workload_running("ns2", "echo");

    // Both workers now at High pressure (1 pod × 128 MB / 160 MB = 0.8).
    // A third namespace's workload should stay in WaitingForCapacity.
    let mut spec3 = always_on_spec();
    spec3.workloads.iter_mut().for_each(|(_, wl)| {
        wl.network.ip = std::net::Ipv4Addr::new(172, 16, 0, 12);
    });
    h.create_namespace("ns3", spec3).await;
    h.converge().await;

    h.assert_workload_waiting_for_capacity("ns3", "echo");
}

/// When a worker drops from High to lower pressure (by freeing pods),
/// a workload stuck in WaitingForCapacity gets scheduled.
///
/// Scenario: fill two 160 MB workers, create a third namespace (stuck),
/// then delete one of the filling namespaces → pressure drops → third workload launches.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_pressure_relief_triggers_scheduling() {
    let mut h = TestHarness::new();
    // 160 MB workers: 1 pod → 0.8 pressure → High.
    let _w1 = h.add_worker_with(MockWorkerConfig::with_pool_and_memory(160)).await;
    let _w2 = h.add_worker_with(MockWorkerConfig::with_pool_and_memory(160)).await;

    // Fill both workers.
    h.create_namespace("ns1", always_on_spec()).await;
    h.converge().await;
    let mut spec2 = always_on_spec();
    spec2.workloads.iter_mut().for_each(|(_, wl)| {
        wl.network.ip = std::net::Ipv4Addr::new(172, 16, 0, 11);
    });
    h.create_namespace("ns2", spec2).await;
    h.converge().await;

    // Third namespace should be stuck.
    let mut spec3 = always_on_spec();
    spec3.workloads.iter_mut().for_each(|(_, wl)| {
        wl.network.ip = std::net::Ipv4Addr::new(172, 16, 0, 12);
    });
    h.create_namespace("ns3", spec3.clone()).await;
    h.converge().await;
    h.assert_workload_waiting_for_capacity("ns3", "echo");

    // Free a pod by deleting ns1 → pressure drops on one worker.
    h.delete_namespace("ns1").await;
    h.converge().await;

    // The stuck workload should now be scheduled.
    h.assert_workload_running("ns3", "echo");
}
