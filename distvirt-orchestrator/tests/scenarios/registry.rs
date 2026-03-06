use std::net::Ipv4Addr;

use distvirt_worker_protocol::WorkerCommand;

use crate::harness::mock_worker::MockWorkerConfig;
use crate::harness::*;

/// Test: Creating a namespace with a service sends RegistrySync to the worker
/// with the service's name->IP mapping.
#[tokio::test(start_paused = true)]
async fn test_registry_sync_on_namespace_create() {
    let mut h = TestHarness::new();

    let w1 = h.add_worker().await;
    h.converge().await;

    // Create namespace with always-on service.
    let spec = always_on_spec();
    h.create_namespace("ns1", spec).await;
    h.converge().await;

    h.assert_namespace_status("ns1", distvirt_orchestrator::types::NamespaceStatus::Active);

    // Worker should have received RegistrySync with "echo-svc" → 172.16.0.100.
    h.assert_worker_received_command_matching(
        &w1,
        "RegistrySync with echo-svc entry",
        |cmd| match cmd {
            WorkerCommand::RegistrySync { entries, .. } => {
                entries.iter().any(|e| e.name == "echo-svc" && e.ip == Ipv4Addr::new(172, 16, 0, 100))
            }
            _ => false,
        },
    );
}

/// Test: Adding a second worker to an active namespace sends RegistrySync to the new worker.
#[tokio::test(start_paused = true)]
async fn test_registry_sync_sent_to_new_worker() {
    let mut h = TestHarness::new();

    let _w1 = h.add_worker().await;
    h.converge().await;

    let spec = always_on_spec();
    h.create_namespace("ns1", spec).await;
    h.converge().await;
    h.assert_namespace_status("ns1", distvirt_orchestrator::types::NamespaceStatus::Active);

    // Add a second worker.
    let w2 = h.add_worker().await;
    h.converge().await;

    // Second worker should have received RegistrySync with service entries.
    h.assert_worker_received_command_matching(
        &w2,
        "RegistrySync with echo-svc entry (new worker joining)",
        |cmd| match cmd {
            WorkerCommand::RegistrySync { entries, .. } => {
                entries.iter().any(|e| e.name == "echo-svc")
            }
            _ => false,
        },
    );
}

/// Test: Adding a service via spec update sends RegistrySync with the new entry.
/// Removing a service via spec update sends updated RegistrySync without the removed entry.
#[tokio::test(start_paused = true)]
async fn test_registry_update_on_service_change() {
    use distvirt_orchestrator::types::*;

    let mut h = TestHarness::new();

    let w1 = h.add_worker().await;
    h.converge().await;

    // Start with always_on_two_workloads_spec (has svc-a and svc-b).
    let spec = always_on_two_workloads_spec();
    h.create_namespace("ns1", spec.clone()).await;
    h.converge().await;
    h.assert_namespace_status("ns1", distvirt_orchestrator::types::NamespaceStatus::Active);

    // Worker should have received RegistrySync with both services.
    h.assert_worker_received_command_matching(
        &w1,
        "RegistrySync with both svc-a and svc-b",
        |cmd| match cmd {
            WorkerCommand::RegistrySync { entries, .. } => {
                let has_a = entries.iter().any(|e| e.name == "svc-a");
                let has_b = entries.iter().any(|e| e.name == "svc-b");
                has_a && has_b
            }
            _ => false,
        },
    );

    // Now remove svc-b by updating the spec.
    let mut updated_spec = spec.clone();
    updated_spec.services.remove(&ServiceId::from("svc-b"));
    // Also remove the workload for svc-b since it's no longer needed.
    updated_spec.workloads.remove(&WorkloadId("echo-b".to_string()));
    h.update_namespace("ns1", updated_spec).await;
    h.converge().await;

    // After removing svc-b, the orchestrator should send a RegistrySync with only svc-a.
    // Get the last RegistrySync command sent to w1.
    let commands = h.worker(&w1).commands();
    let registry_syncs: Vec<_> = commands
        .iter()
        .filter(|cmd| matches!(cmd, WorkerCommand::RegistrySync { .. }))
        .collect();

    // The last RegistrySync should have only svc-a.
    let last_sync = registry_syncs.last().expect("should have at least one RegistrySync");
    match last_sync {
        WorkerCommand::RegistrySync { entries, .. } => {
            assert!(
                entries.iter().any(|e| e.name == "svc-a"),
                "last RegistrySync should contain svc-a"
            );
            assert!(
                !entries.iter().any(|e| e.name == "svc-b"),
                "last RegistrySync should NOT contain svc-b after removal"
            );
        }
        _ => unreachable!(),
    }
}

/// Test: With a single tunnel-capable worker, WorkerRegistrySync is sent but contains
/// only that worker's own entry (or is empty depending on implementation).
///
/// With two tunnel-capable workers, WorkerRegistrySync should be sent to both workers
/// containing the other's endpoint/public key info.
///
/// When one worker disconnects, an updated WorkerRegistrySync should be sent
/// to remaining workers.
#[tokio::test(start_paused = true)]
async fn test_worker_registry_sync_with_tunnel_workers() {
    let mut h = TestHarness::new();

    // Add first tunnel-capable worker.
    let key1 = [1u8; 32];
    let w1 = h
        .add_worker_with(MockWorkerConfig::with_tunnel("10.0.0.1", key1))
        .await;
    h.converge().await;

    // With only one worker, WorkerRegistrySync should still be sent (containing that worker).
    // The orchestrator pushes the full registry on every worker connect.
    h.assert_worker_received_command_matching(
        &w1,
        "WorkerRegistrySync after first worker connects",
        |cmd| matches!(cmd, WorkerCommand::WorkerRegistrySync { .. }),
    );

    // Check the registry: with 1 tunnel-capable worker, it should contain exactly 1 entry.
    let w1_commands = h.worker(&w1).commands();
    let w1_syncs: Vec<_> = w1_commands
        .iter()
        .filter_map(|cmd| match cmd {
            WorkerCommand::WorkerRegistrySync { workers } => Some(workers),
            _ => None,
        })
        .collect();
    let last_sync = w1_syncs.last().expect("should have WorkerRegistrySync");
    assert_eq!(
        last_sync.len(),
        1,
        "with 1 tunnel-capable worker, registry should have 1 entry, got: {:#?}",
        last_sync,
    );

    // Add second tunnel-capable worker.
    let key2 = [2u8; 32];
    let w2 = h
        .add_worker_with(MockWorkerConfig::with_tunnel("10.0.0.2", key2))
        .await;
    h.converge().await;

    // Both workers should have received updated WorkerRegistrySync containing 2 entries.
    let check_two_entries = |cmd: &WorkerCommand| -> bool {
        match cmd {
            WorkerCommand::WorkerRegistrySync { workers } => workers.len() == 2,
            _ => false,
        }
    };

    h.assert_worker_received_command_matching(
        &w1,
        "WorkerRegistrySync with 2 entries after second worker joins",
        check_two_entries,
    );
    h.assert_worker_received_command_matching(
        &w2,
        "WorkerRegistrySync with 2 entries (received by new worker)",
        check_two_entries,
    );

    // Verify registry entries contain the correct endpoints.
    let w1_commands = h.worker(&w1).commands();
    let latest_sync = w1_commands
        .iter()
        .filter_map(|cmd| match cmd {
            WorkerCommand::WorkerRegistrySync { workers } => Some(workers),
            _ => None,
        })
        .last()
        .expect("should have WorkerRegistrySync");

    let endpoints: Vec<_> = latest_sync.iter().map(|w| w.endpoint.as_str()).collect();
    assert!(
        endpoints.iter().any(|e| e.contains("10.0.0.1")),
        "registry should contain w1's endpoint, got: {:?}",
        endpoints,
    );
    assert!(
        endpoints.iter().any(|e| e.contains("10.0.0.2")),
        "registry should contain w2's endpoint, got: {:?}",
        endpoints,
    );

    // Disconnect w2 → remaining worker should get updated registry with only 1 entry.
    h.disconnect_worker(&w2);
    h.converge().await;

    let w1_commands = h.worker(&w1).commands();
    let syncs_after_disconnect: Vec<_> = w1_commands
        .iter()
        .filter_map(|cmd| match cmd {
            WorkerCommand::WorkerRegistrySync { workers } => Some(workers),
            _ => None,
        })
        .collect();

    let last_sync = syncs_after_disconnect
        .last()
        .expect("should have WorkerRegistrySync after disconnect");
    assert_eq!(
        last_sync.len(),
        1,
        "after w2 disconnect, registry should have 1 entry (w1 only), got: {:#?}",
        last_sync,
    );
}

/// Test: Workers without tunnel capabilities (no public_endpoint) do not appear
/// in the WorkerRegistrySync entries, but still receive the sync command.
#[tokio::test(start_paused = true)]
async fn test_non_tunnel_workers_excluded_from_registry_entries() {
    let mut h = TestHarness::new();

    // Add a non-tunnel worker (default, no public_endpoint).
    let w1 = h.add_worker().await;
    h.converge().await;

    // Add a tunnel-capable worker.
    let key2 = [2u8; 32];
    let _w2 = h
        .add_worker_with(MockWorkerConfig::with_tunnel("10.0.0.2", key2))
        .await;
    h.converge().await;

    // w1 (non-tunnel) should still receive WorkerRegistrySync.
    h.assert_worker_received_command_matching(
        &w1,
        "WorkerRegistrySync sent to non-tunnel worker",
        |cmd| matches!(cmd, WorkerCommand::WorkerRegistrySync { .. }),
    );

    // But the registry entries should only contain the tunnel-capable worker (w2).
    let w1_commands = h.worker(&w1).commands();
    let latest_sync = w1_commands
        .iter()
        .filter_map(|cmd| match cmd {
            WorkerCommand::WorkerRegistrySync { workers } => Some(workers),
            _ => None,
        })
        .last()
        .expect("should have WorkerRegistrySync");

    // Only w2 should be in the registry (it has tunnel capabilities).
    assert_eq!(
        latest_sync.len(),
        1,
        "registry should have 1 entry (only tunnel-capable w2), got: {:#?}",
        latest_sync,
    );
    assert!(
        latest_sync.iter().any(|w| w.endpoint.contains("10.0.0.2")),
        "registry entry should be w2's endpoint, got: {:#?}",
        latest_sync,
    );
}
