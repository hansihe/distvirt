use std::net::Ipv4Addr;
use std::time::Duration;

use distvirt_orchestrator::types::WorkloadName;
use distvirt_worker_protocol::{WorkerCommand, WorkerEvent};

use crate::harness::mock_worker::MockWorkerConfig;
use crate::harness::*;

/// Test: In a two-worker setup, verify FabricRouteUpdate is sent to the non-hosting worker
/// when a pod launches, and route is removed when pod stops.
///
/// This verifies the orchestrator's route management in multi-worker namespaces.
#[test]
fn test_fabric_route_update_on_pod_launch() {
    let mut h = TestHarness::new();

    let w1 = h.add_worker();
    let w2 = h.add_worker();
    h.converge();

    // Create always-on namespace — pod will launch on one worker.
    let spec = always_on_spec();
    let alloc = h.create_namespace("ns1", spec);
    h.converge();

    h.assert_namespace_status("ns1", distvirt_orchestrator::types::NamespaceStatus::Active);
    h.assert_workload_running("ns1", "echo");

    let svc_ip = alloc.service_ips["echo-svc"].ip;

    // Determine which worker got the pod.
    let pod_worker_id = h
        .workload_global_worker_id("ns1", "echo")
        .expect("expected worker_id");

    // The other worker should have service endpoint with backend for the service IP.
    let other_worker_id = if pod_worker_id == w1 { &w2 } else { &w1 };
    h.assert_worker_has_service_endpoint_with_backend(
        other_worker_id,
        "ns1",
        svc_ip,
    );

    // The hosting worker should also have the endpoint entry.
    h.assert_worker_has_service_endpoint_with_backend(
        &pod_worker_id,
        "ns1",
        svc_ip,
    );
}

/// Test: In a two-worker activation setup, verify route changes through
/// launch → suspend → resume lifecycle.
#[test]
fn test_fabric_route_lifecycle_with_suspend_resume() {
    let mut h = TestHarness::new();

    // Need pool for suspend/resume.
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool());
    let w2 = h.add_worker_with(MockWorkerConfig::with_pool());
    h.converge();

    let spec = activation_spec(Duration::from_secs(30));
    let alloc = h.create_namespace("ns1", spec);
    h.converge();

    let svc_ip = alloc.service_ips["web-svc"].ip;

    // Activate via EndpointActivation.
    h.activate_service_on("ns1", "web-svc", &w1);

    // Determine which worker hosts the pod.
    let pod_worker_id = h
        .workload_global_worker_id("ns1", "web")
        .expect("expected worker_id");
    let other_worker_id = if pod_worker_id == w1 { w2 } else { w1 };

    // The other worker should have a service endpoint with backend after launch.
    h.assert_worker_has_service_endpoint_with_backend(
        &other_worker_id,
        "ns1",
        svc_ip,
    );

    // Idle → suspend.
    h.deactivate_service_on("ns1", "web-svc", &w1);
    h.advance_time(Duration::from_secs(31));
    h.assert_workload_suspended("ns1", "web");

    // After suspend, the service endpoint should have no backend (pod is gone).
    h.assert_worker_has_service_endpoint_without_backend(
        &other_worker_id,
        "ns1",
        svc_ip,
    );

    // Re-activate via EndpointActivation → resume.
    h.activate_service_on("ns1", "web-svc", &w1);

    // After resume, the service endpoint should have a backend again.
    h.assert_worker_has_service_endpoint_with_backend(
        &other_worker_id,
        "ns1",
        svc_ip,
    );
}

/// EndpointActivation (no service_id) on a Dormant workload should activate it (LaunchPod).
#[test]
fn test_route_miss_activates_dormant_workload() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker();
    let timeout = Duration::from_secs(30);
    let alloc = h.create_namespace("ns", activation_spec(timeout));
    h.converge();
    h.assert_workload_dormant("ns", "web");

    let wl_ip = alloc.workload_ips[&WorkloadName("web".into())].ip;

    // Low-level: EndpointActivation with no service_id targets the pod IP (not service IP)
    h.worker(&w1).send_event(WorkerEvent::EndpointDemandTraffic {
        namespace_id: h.resolve_ns("ns"),
        ip: wl_ip,
        service_id: None,
    });
    h.converge();

    h.assert_workload_running("ns", "web");
    h.assert_worker_received_command_matching(&w1, "LaunchPod", |cmd| {
        matches!(cmd, WorkerCommand::LaunchPod { .. })
    });
}

/// EndpointActivation (no service_id) on a Suspended workload should resume it (ResumePod).
#[test]
fn test_route_miss_activates_suspended_workload() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool());
    let timeout = Duration::from_secs(30);
    let alloc = h.create_namespace("ns", activation_spec(timeout));
    h.converge();
    h.assert_workload_dormant("ns", "web");

    let wl_ip = alloc.workload_ips[&WorkloadName("web".into())].ip;

    // Activate via service → run → idle → suspend
    h.activate_service("ns", "web-svc");
    h.deactivate_service("ns", "web-svc");
    h.advance_past_idle_timeout("ns", "web-svc");
    h.assert_workload_suspended("ns", "web");

    // Low-level: EndpointActivation with no service_id targets the pod IP (not service IP)
    h.worker(&w1).send_event(WorkerEvent::EndpointDemandTraffic {
        namespace_id: h.resolve_ns("ns"),
        ip: wl_ip,
        service_id: None,
    });
    h.converge();

    h.assert_workload_running("ns", "web");
    h.assert_worker_received_command_matching(&w1, "ResumePod", |cmd| {
        matches!(cmd, WorkerCommand::ResumePod { .. })
    });
}

/// EndpointActivation (no service_id) on an already-running workload should be a no-op.
#[test]
fn test_route_miss_ignored_when_already_running() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker();
    let timeout = Duration::from_secs(30);
    let alloc = h.create_namespace("ns", activation_spec(timeout));
    h.converge();

    let wl_ip = alloc.workload_ips[&WorkloadName("web".into())].ip;

    // Activate via service first
    h.activate_service("ns", "web-svc");

    // Low-level: command window slicing to verify no new commands after endpoint activation
    let cmds_before = h.worker(&w1).commands().len();

    h.worker(&w1).send_event(WorkerEvent::EndpointDemandTraffic {
        namespace_id: h.resolve_ns("ns"),
        ip: wl_ip,
        service_id: None,
    });
    h.converge();

    h.assert_workload_running("ns", "web");
    let cmds_after = h.worker(&w1).commands();
    let new_launches = cmds_after[cmds_before..]
        .iter()
        .filter(|c| matches!(c, WorkerCommand::LaunchPod { .. }))
        .count();
    assert_eq!(
        new_launches, 0,
        "no new LaunchPod should be issued when already running"
    );
}

/// EndpointActivation (no service_id) for an IP that doesn't match any workload should be ignored.
#[test]
fn test_route_miss_ignored_for_unknown_ip() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker();
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout));
    h.converge();
    h.assert_workload_dormant("ns", "web");

    // Low-level: testing with an IP that doesn't match any workload
    h.worker(&w1).send_event(WorkerEvent::EndpointDemandTraffic {
        namespace_id: h.resolve_ns("ns"),
        ip: Ipv4Addr::new(172, 16, 0, 99),
        service_id: None,
    });
    h.converge();

    h.assert_workload_dormant("ns", "web");
}

/// After route-miss wake + service activation + idle timeout, workload should suspend.
#[test]
fn test_route_miss_demand_leak() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool());
    let timeout = Duration::from_secs(30);
    let alloc = h.create_namespace("ns", activation_spec(timeout));
    h.converge();
    h.assert_workload_dormant("ns", "web");

    let wl_ip = alloc.workload_ips[&WorkloadName("web".into())].ip;
    let svc_ip = alloc.service_ips["web-svc"].ip;

    // Low-level: testing exact demand leak behavior with endpoint activation interaction
    // Step 1: EndpointActivation (no service_id) activates the workload.
    h.worker(&w1).send_event(WorkerEvent::EndpointDemandTraffic {
        namespace_id: h.resolve_ns("ns"),
        ip: wl_ip,
        service_id: None,
    });
    h.converge();
    h.assert_workload_running("ns", "web");

    // Step 2: EndpointActivation with service_id arrives (real traffic hits the service IP).
    h.worker(&w1).send_event(WorkerEvent::EndpointDemandTraffic {
        namespace_id: h.resolve_ns("ns"),
        ip: svc_ip,
        service_id: Some(h.proto_service_id("ns", "web-svc")),
    });
    h.converge();
    h.assert_workload_running("ns", "web");
    h.assert_service_active("ns", "web-svc");

    // Step 3: Signal no more demand → start idle timer.
    h.worker(&w1).send_event(WorkerEvent::EndpointDemandActive {
        namespace_id: h.resolve_ns("ns"),
        ip: wl_ip,
        service_id: Some(h.proto_service_id("ns", "web-svc")),
        active: false,
    });
    h.converge();

    // Step 4: Advance past idle timeout.
    h.advance_past_idle_timeout("ns", "web-svc");
    h.assert_service_idle("ns", "web-svc");
    h.assert_workload_suspended("ns", "web");

    // After suspension, the service endpoint should have no backend.
    h.assert_worker_has_service_endpoint_without_backend(
        &w1,
        "ns",
        svc_ip,
    );
}

// =============================================================================
// Endpoint table validation tests
// =============================================================================

/// Verify endpoint table contains correct entries after always-on namespace creation.
#[test]
fn test_endpoint_table_populated_on_always_on_create() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker();
    let alloc = h.create_namespace("ns", always_on_spec());
    h.converge();
    h.assert_workload_running("ns", "echo");

    let svc_ip = alloc.service_ips["echo-svc"].ip;

    // Service endpoint should exist with backend (workload is running).
    h.assert_worker_has_service_endpoint_with_backend(
        &w1,
        "ns",
        svc_ip,
    );
}

/// Verify that when a workload is rescheduled (hosting worker disconnects in a
/// two-worker setup), the remaining worker's endpoint table is updated with the
/// new backend placement.
#[test]
fn test_endpoint_updated_after_reschedule() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker();
    let w2 = h.add_worker();
    let alloc = h.create_namespace("ns", always_on_spec());
    h.converge();
    h.assert_workload_running("ns", "echo");

    let svc_ip = alloc.service_ips["echo-svc"].ip;

    let pod_worker = h
        .workload_global_worker_id("ns", "echo")
        .expect("expected worker");
    let other = if pod_worker == w1 { w2 } else { w1 };

    // Before disconnect: both workers should have endpoint with backend.
    h.assert_worker_has_service_endpoint_with_backend(
        &other,
        "ns",
        svc_ip,
    );

    // Disconnect the hosting worker → workload reschedules to the other.
    h.disconnect_worker(&pod_worker);
    h.converge();
    h.assert_workload_running("ns", "echo");

    // After reschedule: remaining worker should still have endpoint with backend.
    h.assert_worker_has_service_endpoint_with_backend(
        &other,
        "ns",
        svc_ip,
    );
}

/// Verify endpoint backend is set to None for activation-based services when workload is dormant,
/// and populated once the workload starts running.
#[test]
fn test_endpoint_backend_lifecycle_activation() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool());
    let timeout = Duration::from_secs(30);
    let alloc = h.create_namespace("ns", activation_spec(timeout));
    h.converge();
    h.assert_workload_dormant("ns", "web");

    let svc_ip = alloc.service_ips["web-svc"].ip;

    // Before activation: service endpoint should exist but without a backend.
    h.assert_worker_has_service_endpoint_without_backend(
        &w1,
        "ns",
        svc_ip,
    );

    // Activate → running
    h.activate_service("ns", "web-svc");

    // After activation: service endpoint should have a backend.
    h.assert_worker_has_service_endpoint_with_backend(
        &w1,
        "ns",
        svc_ip,
    );

    // Suspend
    h.deactivate_service("ns", "web-svc");
    h.advance_past_idle_timeout("ns", "web-svc");
    h.assert_workload_suspended("ns", "web");

    // After suspend: backend should be gone again.
    h.assert_worker_has_service_endpoint_without_backend(
        &w1,
        "ns",
        svc_ip,
    );

    // Resume
    h.activate_service("ns", "web-svc");

    // After resume: backend should be back.
    h.assert_worker_has_service_endpoint_with_backend(
        &w1,
        "ns",
        svc_ip,
    );
}
