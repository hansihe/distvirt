use std::net::Ipv4Addr;

use distvirt_worker_protocol::WorkerCommand;

use crate::harness::*;

/// Test: Creating a namespace with a service sends RegistrySync to the worker
/// with the service's name→IP mapping.
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
