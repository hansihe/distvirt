use distvirt_worker_protocol::WorkerCommand;

use crate::harness::mock_worker::MockWorkerConfig;
use crate::harness::*;

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
