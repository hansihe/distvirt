use std::net::Ipv4Addr;
use std::time::Duration;

use crate::harness::*;
use crate::harness::mock_worker::MockWorkerConfig;
use distvirt_orchestrator::types::*;
use distvirt_worker_protocol::{BackendNeed, ServiceId, WorkerCommand, WorkerEvent};

/// Two services back same workload. Activate A → workload launches.
/// Activate B → workload already running.
///
/// Previously buggy: svc-b would get stuck in NeedBackend because readiness
/// wasn't synced when demand went from 1→2 while Running.
/// Fixed: reconcile_readiness sends WorkloadReady to services in NeedBackend
/// when the workload is already Running.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_two_services_one_workload_shared_demand() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    h.create_namespace("ns", multi_service_spec()).await;
    h.converge().await;
    h.assert_workload_dormant("ns", "shared");
    h.assert_service_idle("ns", "svc-a");
    h.assert_service_idle("ns", "svc-b");

    // Activate svc-a → workload launches
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("svc-a"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 100),
    });
    h.converge().await;
    h.assert_workload_running("ns", "shared");
    h.assert_service_active("ns", "svc-a");

    // Activate svc-b → workload already running
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("svc-b"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 101),
    });
    h.converge().await;
    h.assert_workload_running("ns", "shared");
    h.assert_service_active("ns", "svc-a");

    // Fixed: svc-b receives late-joiner WorkloadReady and transitions to Active.
    h.assert_service_active("ns", "svc-b");

    // Idle svc-a → demand drops. Workload stays running because svc-b has demand
    // (even though svc-b is in NeedBackend, it has issued DemandUp).
    h.worker(&w1).send_event(WorkerEvent::ServiceBackendNeed {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("svc-a"),
        need: BackendNeed::None,
    });
    h.converge().await;
    let timeout = Duration::from_secs(30);
    h.advance_time(timeout + Duration::from_secs(1)).await;
    // Workload should still be running (demand_count=1 from svc-b's DemandUp)
    h.assert_workload_running("ns", "shared");
}

/// Workload already running via svc-a. svc-b activates. No state change in workload.
/// svc-b receives late-joiner WorkloadReady and transitions to Active.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_service_activation_while_already_running() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    h.create_namespace("ns", multi_service_spec()).await;
    h.converge().await;

    // Activate svc-a
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("svc-a"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 100),
    });
    h.converge().await;
    h.assert_workload_running("ns", "shared");
    h.assert_service_active("ns", "svc-a");

    // Capture pod_id
    let pod_id_before = h.workload_state("ns", "shared").pod_id().unwrap().clone();

    // Activate svc-b while already running
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("svc-b"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 101),
    });
    h.converge().await;

    // Still running with same pod
    h.assert_workload_running("ns", "shared");
    let pod_id_after = h.workload_state("ns", "shared").pod_id().unwrap().clone();
    assert_eq!(pod_id_before, pod_id_after, "pod should not have changed");

    // svc-a is still Active
    h.assert_service_active("ns", "svc-a");
    // Fixed: svc-b receives late-joiner WorkloadReady and transitions to Active.
    h.assert_service_active("ns", "svc-b");
}

// ============================================================
// Bug-exposing tests: single-service-per-workload assumptions
// ============================================================

/// Issue 1+4: Two always-on services on one workload.
/// Both services should get CreateService on the worker, not just the first one found.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_always_on_multi_service_both_get_create_service() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker().await;
    h.create_namespace("ns", always_on_multi_service_spec()).await;
    h.converge().await;

    // Workload should be running (always-on).
    h.assert_workload_running("ns", "shared");

    // Both services should have received CreateService on the worker.
    h.assert_worker_received_command_matching(
        &w1,
        "CreateService for svc-a",
        |cmd| matches!(cmd, WorkerCommand::CreateService { service_id, .. } if service_id.0 == "svc-a"),
    );
    h.assert_worker_received_command_matching(
        &w1,
        "CreateService for svc-b",
        |cmd| matches!(cmd, WorkerCommand::CreateService { service_id, .. } if service_id.0 == "svc-b"),
    );

    // Both services should be Active (always-on with running workload).
    h.assert_service_active("ns", "svc-a");
    h.assert_service_active("ns", "svc-b");
}

/// Issue 3: Add a new service to a workload that is already Running via spec update.
/// The new service should transition through to Active (workload is already up).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_add_service_to_running_workload() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker().await;
    h.create_namespace("ns", always_on_spec()).await;
    h.converge().await;
    h.assert_workload_running("ns", "echo");
    h.assert_service_active("ns", "echo-svc");

    // Add a second always-on service via spec update.
    let mut new_spec = always_on_spec();
    new_spec.services.insert(
        ServiceId::from("echo-svc-2"),
        ServiceSpec {
            workload_id: WorkloadId("echo".to_string()),
            ip: Ipv4Addr::new(172, 16, 0, 101),
            policy: distvirt_worker_protocol::ServicePolicy {
                buffer_frames: 100,
                timeout_ms: 5000,
                activator: None,
            },
            activation: None,
        },
    );
    h.update_namespace("ns", new_spec).await;
    h.converge().await;

    // Workload should still be running (no restart).
    h.assert_workload_running("ns", "echo");

    // New service should have received CreateService.
    h.assert_worker_received_command_matching(
        &w1,
        "CreateService for echo-svc-2",
        |cmd| matches!(cmd, WorkerCommand::CreateService { service_id, .. } if service_id.0 == "echo-svc-2"),
    );

    // New service should be Active (workload is already Running).
    h.assert_service_active("ns", "echo-svc-2");
    // Original service unaffected.
    h.assert_service_active("ns", "echo-svc");
}

/// Issue 3: Add a new activation service to a workload that is Suspended.
/// The new service should become Idle and CreateService should be sent to workers.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_add_service_to_suspended_workload() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;

    // Activate → running → idle → suspended
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 100),
    });
    h.converge().await;
    h.assert_workload_running("ns", "web");

    h.worker(&w1).send_event(WorkerEvent::ServiceBackendNeed {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        need: BackendNeed::None,
    });
    h.converge().await;
    h.advance_time(timeout + Duration::from_secs(1)).await;
    h.assert_workload_suspended("ns", "web");
    h.assert_service_idle("ns", "web-svc");

    // Add a second activation service via spec update.
    let mut new_spec = activation_spec(timeout);
    new_spec.services.insert(
        ServiceId::from("web-svc-2"),
        ServiceSpec {
            workload_id: WorkloadId("web".to_string()),
            ip: Ipv4Addr::new(172, 16, 0, 101),
            policy: distvirt_worker_protocol::ServicePolicy {
                buffer_frames: 100,
                timeout_ms: 5000,
                activator: Some(distvirt_worker_protocol::ActivatorConfig::Tcp {
                    ports: None,
                    tcp_only: true,
                    max_flows: 100,
                }),
            },
            activation: Some(ActivationSpec {
                idle_timeout: timeout,
            }),
        },
    );
    h.update_namespace("ns", new_spec).await;
    h.converge().await;

    // New service should be Idle (activation service, workload is Suspended).
    h.assert_service_idle("ns", "web-svc-2");

    // CreateService should have been sent for the new service.
    h.assert_worker_received_command_matching(
        &w1,
        "CreateService for web-svc-2",
        |cmd| matches!(cmd, WorkerCommand::CreateService { service_id, .. } if service_id.0 == "web-svc-2"),
    );

    // Activating the new service should resume the workload.
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc-2"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 101),
    });
    h.converge().await;
    h.assert_workload_running("ns", "web");
    h.assert_service_active("ns", "web-svc-2");
}

/// Issue 5: A second worker joins an active namespace.
/// It should receive CreateService for all services that are already past Pending.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_late_joining_worker_receives_create_service() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker().await;
    h.create_namespace("ns", always_on_multi_service_spec()).await;
    h.converge().await;
    h.assert_workload_running("ns", "shared");

    // Add a second worker.
    let w2 = h.add_worker().await;
    h.converge().await;

    // The second worker should have received CreateService for both services.
    h.assert_worker_received_command_matching(
        &w2,
        "CreateService for svc-a on w2",
        |cmd| matches!(cmd, WorkerCommand::CreateService { service_id, .. } if service_id.0 == "svc-a"),
    );
    h.assert_worker_received_command_matching(
        &w2,
        "CreateService for svc-b on w2",
        |cmd| matches!(cmd, WorkerCommand::CreateService { service_id, .. } if service_id.0 == "svc-b"),
    );
}

/// Issue 6: Remove one service from a workload that has two activation services.
/// The remaining service should still function and demand should be correct.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_remove_service_updates_demand() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    h.create_namespace("ns", multi_service_spec()).await;
    h.converge().await;

    // Activate both services.
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("svc-a"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 100),
    });
    h.converge().await;
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("svc-b"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 101),
    });
    h.converge().await;
    h.assert_workload_running("ns", "shared");
    h.assert_service_active("ns", "svc-a");
    h.assert_service_active("ns", "svc-b");

    // Remove svc-b via spec update.
    let mut new_spec = multi_service_spec();
    new_spec.services.remove(&ServiceId::from("svc-b"));
    h.update_namespace("ns", new_spec).await;
    h.converge().await;

    // Workload should still be running (svc-a still has demand).
    h.assert_workload_running("ns", "shared");
    h.assert_service_active("ns", "svc-a");

    // DestroyService should have been issued for svc-b.
    h.assert_worker_received_command_matching(
        &w1,
        "DestroyService for svc-b",
        |cmd| matches!(cmd, WorkerCommand::DestroyService { service_id, .. } if service_id.0 == "svc-b"),
    );

    // svc-b should no longer exist in the namespace.
    let ns = h.namespace("ns");
    assert!(
        !ns.services.contains_key(&ServiceId::from("svc-b")),
        "removed service 'svc-b' should not exist"
    );
}

/// Issue 6 edge case: Remove the ONLY remaining demanding service.
/// Workload should eventually go idle/dormant.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_remove_only_active_service_drops_demand() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", multi_service_spec()).await;
    h.converge().await;

    // Activate svc-a only.
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("svc-a"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 100),
    });
    h.converge().await;
    h.assert_workload_running("ns", "shared");
    h.assert_service_active("ns", "svc-a");
    h.assert_service_idle("ns", "svc-b");

    // Remove svc-a (the only service with demand) via spec update.
    let mut new_spec = multi_service_spec();
    new_spec.services.remove(&ServiceId::from("svc-a"));
    h.update_namespace("ns", new_spec).await;
    h.converge().await;

    // With no demanding services, workload demand drops to 0.
    // suspend_on_idle=true, so workload should suspend.
    // Give the idle timer time to fire if needed.
    h.advance_time(timeout + Duration::from_secs(1)).await;

    // Workload should be Suspended (suspend_on_idle=true, demand=0).
    h.assert_workload_suspended("ns", "shared");
}
