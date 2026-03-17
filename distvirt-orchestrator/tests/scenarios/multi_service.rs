use std::net::Ipv4Addr;
use std::time::Duration;

use crate::harness::mock_worker::MockWorkerConfig;
use crate::harness::*;
#[allow(unused_imports)]
use distvirt_orchestrator::types::*;
use distvirt_worker_protocol::{WorkerCommand, WorkerEvent};

/// Two services back same workload. Activate A → workload launches.
/// Activate B → workload already running.
///
/// Previously buggy: svc-b would get stuck in NeedBackend because readiness
/// wasn't synced when demand went from 1→2 while Running.
/// Fixed: reconcile_readiness sends WorkloadReady to services in NeedBackend
/// when the workload is already Running.
#[test]
fn test_two_services_one_workload_shared_demand() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool());
    h.create_namespace("ns", multi_service_spec());
    h.converge();
    h.assert_workload_dormant("ns", "shared");
    h.assert_service_idle("ns", "svc-a");
    h.assert_service_idle("ns", "svc-b");

    let svc_a_id = h.proto_service_id("ns", "svc-a");
    let svc_b_id = h.proto_service_id("ns", "svc-b");

    // Activate svc-a → workload launches
    h.worker(&w1).send_event(WorkerEvent::EndpointActivation {
        namespace_id: "ns".into(),
        ip: Ipv4Addr::new(172, 16, 0, 100),
        service_id: Some(svc_a_id),
    });
    h.converge();
    h.assert_workload_running("ns", "shared");
    h.assert_service_active("ns", "svc-a");

    // Activate svc-b → workload already running
    h.worker(&w1).send_event(WorkerEvent::EndpointActivation {
        namespace_id: "ns".into(),
        ip: Ipv4Addr::new(172, 16, 0, 101),
        service_id: Some(svc_b_id),
    });
    h.converge();
    h.assert_workload_running("ns", "shared");
    h.assert_service_active("ns", "svc-a");

    // Fixed: svc-b receives late-joiner WorkloadReady and transitions to Active.
    h.assert_service_active("ns", "svc-b");

    // Idle svc-a → demand drops. Workload stays running because svc-b has demand
    // (even though svc-b is in NeedBackend, it has issued DemandUp).
    h.worker(&w1).send_event(WorkerEvent::EndpointDemand {
        namespace_id: "ns".into(),
        ip: Ipv4Addr::UNSPECIFIED,
        service_id: Some(svc_a_id),
        active: false,
    });
    h.converge();
    let timeout = Duration::from_secs(30);
    h.advance_time(timeout + Duration::from_secs(1));
    // Workload should still be running (demand_count=1 from svc-b's DemandUp)
    h.assert_workload_running("ns", "shared");
}

/// Workload already running via svc-a. svc-b activates. No state change in workload.
/// svc-b receives late-joiner WorkloadReady and transitions to Active.
#[test]
fn test_service_activation_while_already_running() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool());
    h.create_namespace("ns", multi_service_spec());
    h.converge();

    let svc_a_id = h.proto_service_id("ns", "svc-a");
    let svc_b_id = h.proto_service_id("ns", "svc-b");

    // Activate svc-a
    h.worker(&w1).send_event(WorkerEvent::EndpointActivation {
        namespace_id: "ns".into(),
        ip: Ipv4Addr::new(172, 16, 0, 100),
        service_id: Some(svc_a_id),
    });
    h.converge();
    h.assert_workload_running("ns", "shared");
    h.assert_service_active("ns", "svc-a");

    // Capture pod_id
    let pod_id_before = h.workload_state("ns", "shared").pod_id;

    // Activate svc-b while already running
    h.worker(&w1).send_event(WorkerEvent::EndpointActivation {
        namespace_id: "ns".into(),
        ip: Ipv4Addr::new(172, 16, 0, 101),
        service_id: Some(svc_b_id),
    });
    h.converge();

    // Still running with same pod
    h.assert_workload_running("ns", "shared");
    let pod_id_after = h.workload_state("ns", "shared").pod_id;
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
#[test]
fn test_always_on_multi_service_both_get_create_service() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker();
    h.create_namespace("ns", always_on_multi_service_spec());
    h.converge();

    // Workload should be running (always-on).
    h.assert_workload_running("ns", "shared");

    // Worker should have received EndpointSync or EndpointUpdate containing both services.
    h.assert_worker_received_command_matching(
        &w1,
        "EndpointSync or EndpointUpdate with endpoints",
        |cmd| {
            matches!(
                cmd,
                WorkerCommand::EndpointSync { endpoints, .. } if !endpoints.is_empty()
            ) || matches!(
                cmd,
                WorkerCommand::EndpointUpdate { upserted, .. } if !upserted.is_empty()
            )
        },
    );

    // Both services should be Active (always-on with running workload).
    h.assert_service_active("ns", "svc-a");
    h.assert_service_active("ns", "svc-b");
}

/// Issue 3: Add a new service to a workload that is already Running via spec update.
/// The new service should transition through to Active (workload is already up).
#[test]
fn test_add_service_to_running_workload() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker();
    h.create_namespace("ns", always_on_spec());
    h.converge();
    h.assert_workload_running("ns", "echo");
    h.assert_service_active("ns", "echo-svc");

    // Add a second always-on service via spec update.
    let mut new_spec = always_on_spec();
    new_spec.services.insert(
        "echo-svc-2".to_string(),
        ServiceSpec {
            workload_id: WorkloadName("echo".to_string()),
            ip: Ipv4Addr::new(172, 16, 0, 101),
            policy: distvirt_worker_protocol::ServicePolicy {
                buffer_frames: 100,
                timeout_ms: 5000,
                activator: None,
            },
            activation: None,
        },
    );
    h.update_namespace("ns", new_spec);
    h.converge();

    // Workload should still be running (no restart).
    h.assert_workload_running("ns", "echo");

    // Worker should have received EndpointSync or EndpointUpdate including the new service.
    h.assert_worker_received_command_matching(
        &w1,
        "EndpointSync or EndpointUpdate with endpoints",
        |cmd| {
            matches!(
                cmd,
                WorkerCommand::EndpointSync { endpoints, .. } if !endpoints.is_empty()
            ) || matches!(
                cmd,
                WorkerCommand::EndpointUpdate { upserted, .. } if !upserted.is_empty()
            )
        },
    );

    // New service should be Active (workload is already Running).
    h.assert_service_active("ns", "echo-svc-2");
    // Original service unaffected.
    h.assert_service_active("ns", "echo-svc");
}

/// Issue 3: Add a new activation service to a workload that is Suspended.
/// The new service should become Idle and CreateService should be sent to workers.
#[test]
fn test_add_service_to_suspended_workload() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool());
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout));
    h.converge();

    let web_svc_id = h.proto_service_id("ns", "web-svc");

    // Activate → running → idle → suspended
    h.worker(&w1).send_event(WorkerEvent::EndpointActivation {
        namespace_id: "ns".into(),
        ip: Ipv4Addr::new(172, 16, 0, 100),
        service_id: Some(web_svc_id),
    });
    h.converge();
    h.assert_workload_running("ns", "web");

    h.worker(&w1).send_event(WorkerEvent::EndpointDemand {
        namespace_id: "ns".into(),
        ip: Ipv4Addr::UNSPECIFIED,
        service_id: Some(web_svc_id),
        active: false,
    });
    h.converge();
    h.advance_time(timeout + Duration::from_secs(1));
    h.assert_workload_suspended("ns", "web");
    h.assert_service_idle("ns", "web-svc");

    // Add a second activation service via spec update.
    let mut new_spec = activation_spec(timeout);
    new_spec.services.insert(
        "web-svc-2".to_string(),
        ServiceSpec {
            workload_id: WorkloadName("web".to_string()),
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
    h.update_namespace("ns", new_spec);
    h.converge();

    // New service should be Idle (activation service, workload is Suspended).
    h.assert_service_idle("ns", "web-svc-2");

    let web_svc_2_id = h.proto_service_id("ns", "web-svc-2");

    // EndpointSync or EndpointUpdate should have been sent including the new service.
    h.assert_worker_received_command_matching(
        &w1,
        "EndpointSync or EndpointUpdate with endpoints",
        |cmd| {
            matches!(
                cmd,
                WorkerCommand::EndpointSync { endpoints, .. } if !endpoints.is_empty()
            ) || matches!(
                cmd,
                WorkerCommand::EndpointUpdate { upserted, .. } if !upserted.is_empty()
            )
        },
    );

    // Activating the new service should resume the workload.
    h.worker(&w1).send_event(WorkerEvent::EndpointActivation {
        namespace_id: "ns".into(),
        ip: Ipv4Addr::new(172, 16, 0, 101),
        service_id: Some(web_svc_2_id),
    });
    h.converge();
    h.assert_workload_running("ns", "web");
    h.assert_service_active("ns", "web-svc-2");
}

/// Issue 5: A second worker joins an active namespace.
/// It should receive CreateService for all services that are already past Pending.
#[test]
fn test_late_joining_worker_receives_create_service() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker();
    h.create_namespace("ns", always_on_multi_service_spec());
    h.converge();
    h.assert_workload_running("ns", "shared");

    // Add a second worker.
    let w2 = h.add_worker();
    h.converge();

    // The second worker should have received EndpointSync with all endpoints.
    h.assert_worker_received_command_matching(
        &w2,
        "EndpointSync or EndpointUpdate with endpoints on w2",
        |cmd| {
            matches!(
                cmd,
                WorkerCommand::EndpointSync { endpoints, .. } if !endpoints.is_empty()
            ) || matches!(
                cmd,
                WorkerCommand::EndpointUpdate { upserted, .. } if !upserted.is_empty()
            )
        },
    );
}

/// Issue 6: Remove one service from a workload that has two activation services.
/// The remaining service should still function and demand should be correct.
#[test]
fn test_remove_service_updates_demand() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool());
    h.create_namespace("ns", multi_service_spec());
    h.converge();

    let svc_a_id = h.proto_service_id("ns", "svc-a");
    let svc_b_id = h.proto_service_id("ns", "svc-b");

    // Activate both services.
    h.worker(&w1).send_event(WorkerEvent::EndpointActivation {
        namespace_id: "ns".into(),
        ip: Ipv4Addr::new(172, 16, 0, 100),
        service_id: Some(svc_a_id),
    });
    h.converge();
    h.worker(&w1).send_event(WorkerEvent::EndpointActivation {
        namespace_id: "ns".into(),
        ip: Ipv4Addr::new(172, 16, 0, 101),
        service_id: Some(svc_b_id),
    });
    h.converge();
    h.assert_workload_running("ns", "shared");
    h.assert_service_active("ns", "svc-a");
    h.assert_service_active("ns", "svc-b");

    // Remove svc-b via spec update.
    let mut new_spec = multi_service_spec();
    new_spec.services.remove(&"svc-b".to_string());
    h.update_namespace("ns", new_spec);
    h.converge();

    // Workload should still be running (svc-a still has demand).
    h.assert_workload_running("ns", "shared");
    h.assert_service_active("ns", "svc-a");

    // EndpointUpdate with removed_ips should have been issued for svc-b's IP.
    h.assert_worker_received_command_matching(
        &w1,
        "EndpointUpdate with removed_ips for svc-b",
        |cmd| {
            matches!(
                cmd,
                WorkerCommand::EndpointUpdate { removed_ips, .. } if !removed_ips.is_empty()
            )
        },
    );

    // svc-b should no longer exist in the namespace.
    let ns = h.namespace("ns");
    let spec = ns.current_spec().unwrap();
    assert!(
        !spec.services.contains_key(&"svc-b".to_string()),
        "removed service 'svc-b' should not exist"
    );
}

/// Issue 6 edge case: Remove the ONLY remaining demanding service.
/// Workload should eventually go idle/dormant.
#[test]
fn test_remove_only_active_service_drops_demand() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool());
    h.create_namespace("ns", multi_service_spec());
    h.converge();

    let svc_a_id = h.proto_service_id("ns", "svc-a");

    // Activate svc-a only.
    h.worker(&w1).send_event(WorkerEvent::EndpointActivation {
        namespace_id: "ns".into(),
        ip: Ipv4Addr::new(172, 16, 0, 100),
        service_id: Some(svc_a_id),
    });
    h.converge();
    h.assert_workload_running("ns", "shared");
    h.assert_service_active("ns", "svc-a");
    h.assert_service_idle("ns", "svc-b");

    // Remove svc-a (the only service with demand) via spec update.
    let mut new_spec = multi_service_spec();
    new_spec.services.remove(&"svc-a".to_string());
    h.update_namespace("ns", new_spec);
    h.converge();

    // Service removal should immediately drop demand to 0 via reconciliation
    // (no idle timer involved — the service is gone, not idling).
    // suspend_on_idle=true → workload should begin suspending/suspended immediately.
    let state = h.workload_state("ns", "shared");
    assert!(
        state.awaiting_suspend
            || state.artifact_port.is_some()
            || (!state.has_demand && !state.pod_running),
        "workload should begin deactivation immediately after service removal, got {:?}",
        state,
    );
}
