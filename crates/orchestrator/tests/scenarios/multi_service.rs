use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::time::Duration;

use crate::harness::mock_worker::MockWorkerConfig;
use crate::harness::*;
use distvirt_orchestrator::types::*;
use distvirt_worker_protocol::WorkerEvent;

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
    let alloc = h.create_namespace("ns", multi_service_spec());
    h.converge();
    h.assert_workload_dormant("ns", "shared");
    h.assert_service_idle("ns", "svc-a");
    h.assert_service_idle("ns", "svc-b");

    let svc_a_ip = alloc.service_ips["svc-a"].ip;
    let svc_b_ip = alloc.service_ips["svc-b"].ip;
    let svc_a_id = h.proto_service_id("ns", "svc-a");
    let svc_b_id = h.proto_service_id("ns", "svc-b");

    // Activate svc-a → workload launches
    h.worker(&w1).send_event(WorkerEvent::EndpointDemandTraffic {
        namespace_id: h.resolve_ns("ns"),
        ip: svc_a_ip,
        service_id: Some(svc_a_id),
    });
    h.converge();
    h.assert_workload_running("ns", "shared");
    h.assert_service_active("ns", "svc-a");

    // Activate svc-b with sustained demand → workload already running
    h.worker(&w1).send_event(WorkerEvent::EndpointDemandActive {
        namespace_id: h.resolve_ns("ns"),
        ip: svc_b_ip,
        service_id: Some(svc_b_id),
        active: true,
    });
    h.converge();
    h.assert_workload_running("ns", "shared");
    h.assert_service_active("ns", "svc-a");

    // Fixed: svc-b receives late-joiner WorkloadReady and transitions to Active.
    h.assert_service_active("ns", "svc-b");

    // Idle svc-a → demand drops. Workload stays running because svc-b has
    // sustained demand via EndpointDemandActive.
    h.worker(&w1).send_event(WorkerEvent::EndpointDemandActive {
        namespace_id: h.resolve_ns("ns"),
        ip: h.service_ip("ns", "svc-a"),
        service_id: Some(svc_a_id),
        active: false,
    });
    h.converge();
    let timeout = Duration::from_secs(30);
    h.advance_time(timeout + Duration::from_secs(1));
    // Workload should still be running (svc-b has sustained demand)
    h.assert_workload_running("ns", "shared");
}

/// Workload already running via svc-a. svc-b activates. No state change in workload.
/// svc-b receives late-joiner WorkloadReady and transitions to Active.
#[test]
fn test_service_activation_while_already_running() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool());
    let alloc = h.create_namespace("ns", multi_service_spec());
    h.converge();

    let svc_a_ip = alloc.service_ips["svc-a"].ip;
    let svc_b_ip = alloc.service_ips["svc-b"].ip;
    let svc_a_id = h.proto_service_id("ns", "svc-a");
    let svc_b_id = h.proto_service_id("ns", "svc-b");

    // Activate svc-a
    h.worker(&w1).send_event(WorkerEvent::EndpointDemandTraffic {
        namespace_id: h.resolve_ns("ns"),
        ip: svc_a_ip,
        service_id: Some(svc_a_id),
    });
    h.converge();
    h.assert_workload_running("ns", "shared");
    h.assert_service_active("ns", "svc-a");

    // Capture pod_id
    let pod_id_before = h.workload_state("ns", "shared").pod_id;

    // Activate svc-b while already running
    h.worker(&w1).send_event(WorkerEvent::EndpointDemandTraffic {
        namespace_id: h.resolve_ns("ns"),
        ip: svc_b_ip,
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
    let alloc = h.create_namespace("ns", always_on_multi_service_spec());
    h.converge();

    let svc_a_ip = alloc.service_ips["svc-a"].ip;
    let svc_b_ip = alloc.service_ips["svc-b"].ip;

    // Workload should be running (always-on).
    h.assert_workload_running("ns", "shared");

    // Both services should be Active (always-on with running workload).
    h.assert_service_active("ns", "svc-a");
    h.assert_service_active("ns", "svc-b");

    // Worker endpoint table should contain both service IPs with backends.
    h.assert_worker_has_service_endpoint_with_backend(
        &w1,
        "ns",
        svc_a_ip,
    );
    h.assert_worker_has_service_endpoint_with_backend(
        &w1,
        "ns",
        svc_b_ip,
    );
}

/// Issue 3: Add a new service to a workload that is already Running via spec update.
/// The new service should transition through to Active (workload is already up).
#[test]
fn test_add_service_to_running_workload() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker();
    let alloc = h.create_namespace("ns", always_on_spec());
    h.converge();
    h.assert_workload_running("ns", "echo");
    h.assert_service_active("ns", "echo-svc");

    let echo_svc_ip = alloc.service_ips["echo-svc"].ip;

    // Add a second always-on service via spec update.
    let mut new_spec = always_on_spec();
    new_spec.services.insert(
        "echo-svc-2".to_string(),
        ServiceSpec {
            workload_id: WorkloadName("echo".to_string()),
            ip: Ipv4Addr::new(172, 16, 0, 101),
            ports: vec![],
            has_activation: false,
            idle_timeout: Duration::ZERO,
            buffer_frames: 100,
            buffer_timeout_ms: 5000,
            labels: BTreeMap::new(),
        },
    );
    let alloc2 = h.update_namespace("ns", new_spec);
    h.converge();

    let echo_svc_2_ip = alloc2.service_ips["echo-svc-2"].ip;

    // Workload should still be running (no restart).
    h.assert_workload_running("ns", "echo");

    // New service should be Active (workload is already Running).
    h.assert_service_active("ns", "echo-svc-2");
    // Original service unaffected.
    h.assert_service_active("ns", "echo-svc");

    // Worker endpoint table should contain both service IPs with backends.
    h.assert_worker_has_service_endpoint_with_backend(
        &w1,
        "ns",
        echo_svc_ip,
    );
    h.assert_worker_has_service_endpoint_with_backend(
        &w1,
        "ns",
        echo_svc_2_ip,
    );
}

/// Issue 3: Add a new activation service to a workload that is Suspended.
/// The new service should become Idle and CreateService should be sent to workers.
#[test]
fn test_add_service_to_suspended_workload() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool());
    let timeout = Duration::from_secs(30);
    let alloc = h.create_namespace("ns", activation_spec(timeout));
    h.converge();

    let web_svc_ip = alloc.service_ips["web-svc"].ip;
    let web_svc_id = h.proto_service_id("ns", "web-svc");

    // Activate → running → idle → suspended
    h.worker(&w1).send_event(WorkerEvent::EndpointDemandTraffic {
        namespace_id: h.resolve_ns("ns"),
        ip: web_svc_ip,
        service_id: Some(web_svc_id),
    });
    h.converge();
    h.assert_workload_running("ns", "web");

    h.worker(&w1).send_event(WorkerEvent::EndpointDemandActive {
        namespace_id: h.resolve_ns("ns"),
        ip: h.service_ip("ns", "web-svc"),
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
            ports: vec![PortConfig {
                port: 80,
                target_port: 80,
                activator: Some(ActivatorKind::Tcp { max_flows: 100 }),
            }],
            has_activation: true,
            idle_timeout: timeout,
            buffer_frames: 100,
            buffer_timeout_ms: 5000,
            labels: BTreeMap::new(),
        },
    );
    let alloc2 = h.update_namespace("ns", new_spec);
    h.converge();

    let web_svc_2_ip = alloc2.service_ips["web-svc-2"].ip;

    // New service should be Idle (activation service, workload is Suspended).
    h.assert_service_idle("ns", "web-svc-2");

    let web_svc_2_id = h.proto_service_id("ns", "web-svc-2");

    // Worker endpoint table should contain the new service IP (without backend since workload is suspended).
    h.assert_worker_has_service_endpoint_without_backend(
        &w1,
        "ns",
        web_svc_2_ip,
    );

    // Activating the new service should resume the workload.
    h.worker(&w1).send_event(WorkerEvent::EndpointDemandTraffic {
        namespace_id: h.resolve_ns("ns"),
        ip: web_svc_2_ip,
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
    let _w1 = h.add_worker();
    let alloc = h.create_namespace("ns", always_on_multi_service_spec());
    h.converge();
    h.assert_workload_running("ns", "shared");

    let svc_a_ip = alloc.service_ips["svc-a"].ip;
    let svc_b_ip = alloc.service_ips["svc-b"].ip;

    // Add a second worker.
    let w2 = h.add_worker();
    h.converge();

    // The second worker should have received endpoint entries for both service IPs.
    h.assert_worker_has_service_endpoint_with_backend(
        &w2,
        "ns",
        svc_a_ip,
    );
    h.assert_worker_has_service_endpoint_with_backend(
        &w2,
        "ns",
        svc_b_ip,
    );
}

/// Issue 6: Remove one service from a workload that has two activation services.
/// The remaining service should still function and demand should be correct.
#[test]
fn test_remove_service_updates_demand() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool());
    let alloc = h.create_namespace("ns", multi_service_spec());
    h.converge();

    let svc_a_ip = alloc.service_ips["svc-a"].ip;
    let svc_b_ip = alloc.service_ips["svc-b"].ip;
    let svc_a_id = h.proto_service_id("ns", "svc-a");
    let svc_b_id = h.proto_service_id("ns", "svc-b");

    // Activate both services.
    h.worker(&w1).send_event(WorkerEvent::EndpointDemandTraffic {
        namespace_id: h.resolve_ns("ns"),
        ip: svc_a_ip,
        service_id: Some(svc_a_id),
    });
    h.converge();
    h.worker(&w1).send_event(WorkerEvent::EndpointDemandTraffic {
        namespace_id: h.resolve_ns("ns"),
        ip: svc_b_ip,
        service_id: Some(svc_b_id),
    });
    h.converge();
    h.assert_workload_running("ns", "shared");
    h.assert_service_active("ns", "svc-a");
    h.assert_service_active("ns", "svc-b");

    // Remove svc-b via spec update.
    let mut new_spec = multi_service_spec();
    new_spec.services.remove(&"svc-b".to_string());
    let alloc2 = h.update_namespace("ns", new_spec);
    h.converge();

    let svc_a_ip_after = alloc2.service_ips["svc-a"].ip;

    // Workload should still be running (svc-a still has demand).
    h.assert_workload_running("ns", "shared");
    h.assert_service_active("ns", "svc-a");

    // svc-b's IP should have been removed from the endpoint table.
    h.assert_worker_has_no_endpoint(&w1, "ns", svc_b_ip);
    // svc-a should still be present with a backend.
    h.assert_worker_has_service_endpoint_with_backend(&w1, "ns", svc_a_ip_after);

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
    let alloc = h.create_namespace("ns", multi_service_spec());
    h.converge();

    let svc_a_ip = alloc.service_ips["svc-a"].ip;
    let svc_a_id = h.proto_service_id("ns", "svc-a");

    // Activate svc-a only.
    h.worker(&w1).send_event(WorkerEvent::EndpointDemandTraffic {
        namespace_id: h.resolve_ns("ns"),
        ip: svc_a_ip,
        service_id: Some(svc_a_id),
    });
    h.converge();
    h.assert_workload_running("ns", "shared");
    h.assert_service_active("ns", "svc-a");
    h.assert_service_active("ns", "svc-b");

    // Remove svc-a (the only service with demand) via spec update.
    let mut new_spec = multi_service_spec();
    new_spec.services.remove(&"svc-a".to_string());
    h.update_namespace("ns", new_spec);
    h.converge();

    // Service removal should immediately drop demand to 0 via reconciliation
    // (no idle timer involved — the service is gone, not idling).
    // suspend_on_idle=true → workload should begin suspending or have already suspended.
    let status = h.workload_status("ns", "shared");
    assert!(
        matches!(
            status,
            distvirt_orchestrator::sm::WlStatus::Suspending
                | distvirt_orchestrator::sm::WlStatus::Suspended
                | distvirt_orchestrator::sm::WlStatus::Dormant
        ),
        "workload should be Suspending, Suspended, or Dormant after service removal, got {:?}",
        status,
    );
}
