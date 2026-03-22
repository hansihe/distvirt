use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::time::Duration;

use crate::harness::mock_worker::MockWorkerConfig;
use crate::harness::*;
use distvirt_orchestrator::types::*;
use distvirt_worker_protocol::WorkerEvent;

/// Namespace with two activation-based workloads: "wl-a" (service "svc-a") and "wl-b" (service "svc-b").
/// Both have suspend_on_idle=true and activation.
fn two_activation_workloads_spec(idle_timeout: Duration) -> NamespaceSpec {
    let wl_a = WorkloadName("wl-a".to_string());
    let wl_b = WorkloadName("wl-b".to_string());

    let mut workloads = BTreeMap::new();
    workloads.insert(
        wl_a.clone(),
        WorkloadSpec {
            containers: vec![container_spec("docker.io/library/nginx:latest")],
            network: pod_network(10),
            suspend_on_idle: true,
            resources: None,
            activation: None,
            run_policy: Default::default(),
            respects_demand: true,
            volumes: vec![],
        },
    );
    workloads.insert(
        wl_b.clone(),
        WorkloadSpec {
            containers: vec![container_spec("docker.io/library/nginx:latest")],
            network: pod_network(11),
            suspend_on_idle: true,
            resources: None,
            activation: None,
            run_policy: Default::default(),
            respects_demand: true,
            volumes: vec![],
        },
    );

    let mut services = BTreeMap::new();
    services.insert(
        "svc-a".to_string(),
        ServiceSpec {
            workload_id: wl_a,
            ip: Ipv4Addr::new(172, 16, 0, 100),
            ports: vec![PortConfig {
                port: 80,
                target_port: 80,
                activator: Some(ActivatorKind::Tcp { max_flows: 100 }),
            }],
            has_activation: true,
            idle_timeout,
            buffer_frames: 100,
            buffer_timeout_ms: 5000,
        },
    );
    services.insert(
        "svc-b".to_string(),
        ServiceSpec {
            workload_id: wl_b,
            ip: Ipv4Addr::new(172, 16, 0, 101),
            ports: vec![PortConfig {
                port: 80,
                target_port: 80,
                activator: Some(ActivatorKind::Tcp { max_flows: 100 }),
            }],
            has_activation: true,
            idle_timeout,
            buffer_frames: 100,
            buffer_timeout_ms: 5000,
        },
    );

    NamespaceSpec {
        network: default_network(),
        workloads,
        services,
    }
}

/// Basic preemption: wl-a is idle (BackendNeed::None), wl-b activates but no capacity
/// → wl-a gets preempted → wl-b eventually runs.
#[test]
#[ignore] // TODO: unimplemented since orchestrator refactor
fn test_basic_preemption() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool());
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", two_activation_workloads_spec(timeout));
    h.converge();

    // Both workloads start dormant.
    h.assert_workload_dormant("ns", "wl-a");
    h.assert_workload_dormant("ns", "wl-b");

    // Activate wl-a via svc-a.
    h.activate_service("ns", "svc-a");
    h.assert_workload_running("ns", "wl-a");

    // Signal no more traffic on svc-a (idle, but still active with BackendNeed::None).
    h.deactivate_service("ns", "svc-a");

    // Now make the worker high-pressure so select_worker_for_pod returns None.
    h.send_pressure_update(&w1, 85.0);

    // Activate wl-b via svc-b — should trigger preemption of wl-a.
    let svc_b_ip = h.service_ip("ns", "svc-b");
    h.worker(&w1).send_event(WorkerEvent::EndpointDemandTraffic {
        namespace_id: "ns".into(),
        ip: svc_b_ip,
        service_id: Some(h.proto_service_id("ns", "svc-b")),
    });
    h.converge();

    // wl-a should be preempted (deactivating/dormant).
    // wl-b should be in WaitingForCapacity since worker is still high-pressure.
    let wl_a_state = h.workload_state("ns", "wl-a");
    assert!(
        wl_a_state.awaiting_suspend
            || wl_a_state.artifact_port.is_some()
            || (!wl_a_state.has_demand && !wl_a_state.pod_running),
        "wl-a should be deactivating/dormant after preemption, got {:?}",
        wl_a_state
    );

    // Check preempted condition is set on wl-a.
    let conditions = h.workload_conditions("ns", "wl-a");
    assert!(
        conditions.contains_key("preempted"),
        "wl-a should have 'preempted' condition, got {:?}",
        conditions
    );

    // Now relax pressure so wl-b can be scheduled.
    h.send_pressure_update(&w1, 0.0);

    // Need to trigger schedule_waiting_pods. Send a no-op event to converge.
    h.converge();

    // wl-b may need another scheduling round. Let's trigger it by sending a
    // PressureUpdate which will cause recompute + scheduling.
    h.send_pressure_update(&w1, 0.0);

    h.assert_workload_running("ns", "wl-b");
}

/// No preemption of active workloads: workload with BackendNeed::Active/Traffic is not preempted.
#[test]
fn test_no_preemption_of_active_traffic_workloads() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool());
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", two_activation_workloads_spec(timeout));
    h.converge();

    // Activate wl-a via svc-a — it stays active with traffic (BackendNeed::Traffic).
    h.activate_service("ns", "svc-a");
    h.assert_workload_running("ns", "wl-a");

    // Keep svc-a with active traffic (don't deactivate).

    // Make worker high-pressure.
    h.send_pressure_update(&w1, 85.0);

    // Activate wl-b via svc-b.
    let svc_b_ip = h.service_ip("ns", "svc-b");
    h.worker(&w1).send_event(WorkerEvent::EndpointDemandTraffic {
        namespace_id: "ns".into(),
        ip: svc_b_ip,
        service_id: Some(h.proto_service_id("ns", "svc-b")),
    });
    h.converge();

    // wl-a should still be running (not preempted, has active traffic).
    h.assert_workload_running("ns", "wl-a");

    // wl-b should be stuck in WaitingForCapacity.
    h.assert_workload_waiting_for_capacity("ns", "wl-b");

    // No preempted condition on wl-a.
    let conditions = h.workload_conditions("ns", "wl-a");
    assert!(
        !conditions.contains_key("preempted"),
        "wl-a should NOT have 'preempted' condition"
    );
}

/// No preemption when capacity exists: when a worker has capacity, no preemption occurs.
#[test]
fn test_no_preemption_when_capacity_exists() {
    let mut h = TestHarness::new();
    let _w1 = h.add_worker_with(MockWorkerConfig::with_pool());
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", two_activation_workloads_spec(timeout));
    h.converge();

    // Activate wl-a.
    h.activate_service("ns", "svc-a");
    h.assert_workload_running("ns", "wl-a");
    h.deactivate_service("ns", "svc-a");

    // Worker is Normal pressure — plenty of capacity.
    // Activate wl-b — should just work, no preemption needed.
    h.activate_service("ns", "svc-b");
    h.assert_workload_running("ns", "wl-b");

    // Both workloads running, no preemption.
    h.assert_workload_running("ns", "wl-a");
    let conditions = h.workload_conditions("ns", "wl-a");
    assert!(
        !conditions.contains_key("preempted"),
        "wl-a should NOT have 'preempted' condition when capacity exists"
    );
}

/// Preempted workload can reactivate: after preemption, if traffic arrives for the
/// preempted workload's service, it reactivates normally.
#[test]
#[ignore] // TODO: unimplemented since orchestrator refactor
fn test_preempted_workload_can_reactivate() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool());
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", two_activation_workloads_spec(timeout));
    h.converge();

    // Activate wl-a, then make it idle.
    h.activate_service("ns", "svc-a");
    h.deactivate_service("ns", "svc-a");

    // High pressure, activate wl-b to trigger preemption of wl-a.
    h.send_pressure_update(&w1, 85.0);
    let svc_b_ip = h.service_ip("ns", "svc-b");
    h.worker(&w1).send_event(WorkerEvent::EndpointDemandTraffic {
        namespace_id: "ns".into(),
        ip: svc_b_ip,
        service_id: Some(h.proto_service_id("ns", "svc-b")),
    });
    h.converge();

    // wl-a should be preempted.
    let wl_a_state = h.workload_state("ns", "wl-a");
    assert!(
        !wl_a_state.pod_running,
        "wl-a should not be Running after preemption, got {:?}",
        wl_a_state
    );

    // Now restore pressure and let wl-b schedule.
    h.send_pressure_update(&w1, 0.0);
    h.send_pressure_update(&w1, 0.0);
    h.assert_workload_running("ns", "wl-b");

    // Now deactivate wl-b and reactivate wl-a — it should come back.
    h.deactivate_service("ns", "svc-b");

    // Activate wl-a again.
    let svc_a_ip = h.service_ip("ns", "svc-a");
    h.worker(&w1).send_event(WorkerEvent::EndpointDemandTraffic {
        namespace_id: "ns".into(),
        ip: svc_a_ip,
        service_id: Some(h.proto_service_id("ns", "svc-a")),
    });
    h.converge();

    h.assert_workload_running("ns", "wl-a");
    h.assert_service_active("ns", "svc-a");
}
