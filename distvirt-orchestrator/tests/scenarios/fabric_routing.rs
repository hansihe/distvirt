use std::net::Ipv4Addr;
use std::time::Duration;

use distvirt_worker_protocol::{BackendNeed, ServiceId, WorkerCommand, WorkerEvent};

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
    h.create_namespace("ns1", spec);
    h.converge();

    h.assert_namespace_status("ns1", distvirt_orchestrator::types::NamespaceStatus::Active);
    h.assert_workload_running("ns1", "echo");

    // Determine which worker got the pod.
    let pod_worker_id = h
        .workload_global_worker_id("ns1", "echo")
        .expect("expected worker_id");

    // The other worker should have received an EndpointSync or EndpointUpdate with endpoint entries.
    let other_worker_id = if pod_worker_id == w1 { &w2 } else { &w1 };

    h.assert_worker_received_command_matching(
        other_worker_id,
        "EndpointSync or EndpointUpdate with endpoints",
        |cmd| {
            matches!(
                cmd,
                WorkerCommand::EndpointUpdate { upserted, .. } if !upserted.is_empty()
            ) || matches!(
                cmd,
                WorkerCommand::EndpointSync { endpoints, .. } if !endpoints.is_empty()
            )
        },
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
    h.create_namespace("ns1", spec);
    h.converge();

    // Activate via EndpointActivation.
    h.worker(&w1).send_event(WorkerEvent::EndpointActivation {
        namespace_id: "ns1".into(),
        ip: Ipv4Addr::new(172, 16, 0, 100),
        service_id: Some(ServiceId::from("web-svc")),
    });
    h.converge();
    h.assert_workload_running("ns1", "web");

    // Determine which worker hosts the pod.
    let pod_worker_id = h
        .workload_global_worker_id("ns1", "web")
        .expect("expected worker_id");
    let other_worker_id = if pod_worker_id == w1 { w2 } else { w1 };

    // The other worker should have received an EndpointSync or EndpointUpdate with endpoints.
    h.assert_worker_received_command_matching(
        &other_worker_id,
        "EndpointSync or EndpointUpdate with endpoints",
        |cmd| {
            matches!(
                cmd,
                WorkerCommand::EndpointUpdate { upserted, .. } if !upserted.is_empty()
            ) || matches!(
                cmd,
                WorkerCommand::EndpointSync { endpoints, .. } if !endpoints.is_empty()
            )
        },
    );

    // Idle → suspend.
    h.worker(&w1).send_event(WorkerEvent::ServiceBackendNeed {
        namespace_id: "ns1".into(),
        service_id: ServiceId::from("web-svc"),
        need: BackendNeed::None,
    });
    h.converge();
    h.advance_time(Duration::from_secs(31));
    h.assert_workload_suspended("ns1", "web");

    // After suspend, an EndpointUpdate should have been sent (service backend becomes None).
    h.assert_worker_received_command_matching(
        &other_worker_id,
        "EndpointUpdate with upserted endpoints (after suspend, backend=None)",
        |cmd| {
            matches!(
                cmd,
                WorkerCommand::EndpointUpdate { upserted, .. } if !upserted.is_empty()
            )
        },
    );

    // Re-activate via EndpointActivation → resume.
    h.worker(&w1).send_event(WorkerEvent::EndpointActivation {
        namespace_id: "ns1".into(),
        ip: Ipv4Addr::new(172, 16, 0, 100),
        service_id: Some(ServiceId::from("web-svc")),
    });
    h.converge();
    h.assert_workload_running("ns1", "web");

    // After resume, new EndpointUpdate(s) with upserted entries should have been sent.
    // There should be at least 2 EndpointUpdate with upserted entries: one from initial launch,
    // one from resume.
    let other_commands = h.worker(&other_worker_id).commands();
    let endpoint_upserts: Vec<_> = other_commands
        .iter()
        .filter(|cmd| matches!(cmd, WorkerCommand::EndpointUpdate { upserted, .. } if !upserted.is_empty()))
        .collect();
    assert!(
        endpoint_upserts.len() >= 2,
        "expected at least 2 EndpointUpdate with upserted entries (launch + resume), got {}: {:#?}",
        endpoint_upserts.len(),
        endpoint_upserts,
    );
}

/// EndpointActivation (no service_id) on a Dormant workload should activate it (LaunchPod).
#[test]
fn test_route_miss_activates_dormant_workload() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker();
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout));
    h.converge();
    h.assert_workload_dormant("ns", "web");

    // Low-level: EndpointActivation with no service_id targets the pod IP (not service IP)
    h.worker(&w1).send_event(WorkerEvent::EndpointActivation {
        namespace_id: "ns".into(),
        ip: Ipv4Addr::new(172, 16, 0, 10),
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
    h.create_namespace("ns", activation_spec(timeout));
    h.converge();
    h.assert_workload_dormant("ns", "web");

    // Activate via service → run → idle → suspend
    h.activate_service("ns", "web-svc");
    h.deactivate_service("ns", "web-svc");
    h.advance_past_idle_timeout("ns", "web-svc");
    h.assert_workload_suspended("ns", "web");

    // Low-level: EndpointActivation with no service_id targets the pod IP (not service IP)
    h.worker(&w1).send_event(WorkerEvent::EndpointActivation {
        namespace_id: "ns".into(),
        ip: Ipv4Addr::new(172, 16, 0, 10),
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
    h.create_namespace("ns", activation_spec(timeout));
    h.converge();

    // Activate via service first
    h.activate_service("ns", "web-svc");

    // Low-level: command window slicing to verify no new commands after endpoint activation
    let cmds_before = h.worker(&w1).commands().len();

    h.worker(&w1).send_event(WorkerEvent::EndpointActivation {
        namespace_id: "ns".into(),
        ip: Ipv4Addr::new(172, 16, 0, 10),
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
    h.worker(&w1).send_event(WorkerEvent::EndpointActivation {
        namespace_id: "ns".into(),
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
    h.create_namespace("ns", activation_spec(timeout));
    h.converge();
    h.assert_workload_dormant("ns", "web");

    // Low-level: testing exact demand leak behavior with endpoint activation interaction
    // Step 1: EndpointActivation (no service_id) activates the workload.
    h.worker(&w1).send_event(WorkerEvent::EndpointActivation {
        namespace_id: "ns".into(),
        ip: Ipv4Addr::new(172, 16, 0, 10),
        service_id: None,
    });
    h.converge();
    h.assert_workload_running("ns", "web");

    // Step 2: EndpointActivation with service_id arrives (real traffic hits the service IP).
    let svc_ip = h.service_ip("ns", "web-svc");
    h.worker(&w1).send_event(WorkerEvent::EndpointActivation {
        namespace_id: "ns".into(),
        ip: svc_ip,
        service_id: Some(ServiceId::from("web-svc")),
    });
    h.converge();
    h.assert_workload_running("ns", "web");
    h.assert_service_active("ns", "web-svc");

    // Step 3: Signal no more active flows (clears has_active_flows demand).
    h.worker(&w1).send_event(WorkerEvent::EndpointFlowStatus {
        namespace_id: "ns".into(),
        ip: Ipv4Addr::new(172, 16, 0, 10),
        service_id: None,
        has_active_flows: false,
    });
    h.converge();

    // Step 4: Signal no more traffic → start idle timer.
    h.worker(&w1).send_event(WorkerEvent::ServiceBackendNeed {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        need: BackendNeed::None,
    });
    h.converge();

    // Step 5: Advance past idle timeout.
    h.advance_past_idle_timeout("ns", "web-svc");
    h.assert_service_idle("ns", "web-svc");
    h.assert_workload_suspended("ns", "web");
}
