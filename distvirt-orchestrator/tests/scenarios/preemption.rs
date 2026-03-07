use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::time::Duration;

use crate::harness::*;
use crate::harness::mock_worker::MockWorkerConfig;
use distvirt_orchestrator::types::*;
use distvirt_worker_protocol::{
    ActivatorConfig, ServiceId, ServicePolicy, WorkerEvent,
};

/// Namespace with two activation-based workloads: "wl-a" (service "svc-a") and "wl-b" (service "svc-b").
/// Both have suspend_on_idle=true and activation.
fn two_activation_workloads_spec(idle_timeout: Duration) -> NamespaceSpec {
    let wl_a = WorkloadId("wl-a".to_string());
    let wl_b = WorkloadId("wl-b".to_string());

    let mut workloads = BTreeMap::new();
    workloads.insert(
        wl_a.clone(),
        WorkloadSpec {
            containers: vec![container_spec("docker.io/library/nginx:latest")],
            network: pod_network(10),
            suspend_on_idle: true,
            resources: None,
        },
    );
    workloads.insert(
        wl_b.clone(),
        WorkloadSpec {
            containers: vec![container_spec("docker.io/library/nginx:latest")],
            network: pod_network(11),
            suspend_on_idle: true,
            resources: None,
        },
    );

    let activation = Some(ActivationSpec { idle_timeout });
    let policy = ServicePolicy {
        buffer_frames: 100,
        timeout_ms: 5000,
        activator: Some(ActivatorConfig::Tcp {
            ports: None,
            tcp_only: true,
            max_flows: 100,
        }),
    };

    let mut services = BTreeMap::new();
    services.insert(
        ServiceId::from("svc-a"),
        ServiceSpec {
            workload_id: wl_a,
            ip: Ipv4Addr::new(172, 16, 0, 100),
            policy: policy.clone(),
            activation: activation.clone(),
        },
    );
    services.insert(
        ServiceId::from("svc-b"),
        ServiceSpec {
            workload_id: wl_b,
            ip: Ipv4Addr::new(172, 16, 0, 101),
            policy: policy,
            activation: activation,
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
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_basic_preemption() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", two_activation_workloads_spec(timeout)).await;
    h.converge().await;

    // Both workloads start dormant.
    h.assert_workload_dormant("ns", "wl-a");
    h.assert_workload_dormant("ns", "wl-b");

    // Activate wl-a via svc-a.
    h.activate_service("ns", "svc-a").await;
    h.assert_workload_running("ns", "wl-a");

    // Signal no more traffic on svc-a (idle, but still active with BackendNeed::None).
    h.deactivate_service("ns", "svc-a").await;

    // Now make the worker high-pressure so select_worker_for_pod returns None.
    h.orchestrator_mut().workers.get_mut(&w1).unwrap().pressure_bands.memory = PressureBand::High;

    // Activate wl-b via svc-b — should trigger preemption of wl-a.
    let svc_b_ip = h.service_ip("ns", "svc-b");
    h.worker(&w1).send_event(WorkerEvent::EndpointActivation {
        namespace_id: "ns".into(),
        ip: svc_b_ip,
        service_id: Some(ServiceId::from("svc-b")),
    });
    h.converge().await;

    // wl-a should be preempted (deactivating/dormant).
    // wl-b should be in WaitingForCapacity since worker is still high-pressure.
    let wl_a_state = h.workload_state("ns", "wl-a");
    assert!(
        matches!(wl_a_state, WorkloadState::Dormant | WorkloadState::Suspending { .. } | WorkloadState::Suspended { .. }),
        "wl-a should be deactivating/dormant after preemption, got {:?}", wl_a_state
    );

    // Check preempted condition is set on wl-a.
    let conditions = h.workload_conditions("ns", "wl-a");
    assert!(
        conditions.contains_key("preempted"),
        "wl-a should have 'preempted' condition, got {:?}", conditions
    );

    // Now relax pressure so wl-b can be scheduled.
    h.orchestrator_mut().workers.get_mut(&w1).unwrap().pressure_bands.memory = PressureBand::Normal;

    // Need to trigger schedule_waiting_pods. Send a no-op event to converge.
    h.converge().await;

    // wl-b may need another scheduling round. Let's trigger it by sending a
    // PressureUpdate which will cause recompute + scheduling.
    h.send_pressure_update(&w1, 0.0).await;

    h.assert_workload_running("ns", "wl-b");
}

/// No preemption of active workloads: workload with BackendNeed::Active/Traffic is not preempted.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_no_preemption_of_active_traffic_workloads() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", two_activation_workloads_spec(timeout)).await;
    h.converge().await;

    // Activate wl-a via svc-a — it stays active with traffic (BackendNeed::Traffic).
    h.activate_service("ns", "svc-a").await;
    h.assert_workload_running("ns", "wl-a");

    // Keep svc-a with active traffic (don't deactivate).

    // Make worker high-pressure.
    h.orchestrator_mut().workers.get_mut(&w1).unwrap().pressure_bands.memory = PressureBand::High;

    // Activate wl-b via svc-b.
    let svc_b_ip = h.service_ip("ns", "svc-b");
    h.worker(&w1).send_event(WorkerEvent::EndpointActivation {
        namespace_id: "ns".into(),
        ip: svc_b_ip,
        service_id: Some(ServiceId::from("svc-b")),
    });
    h.converge().await;

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
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_no_preemption_when_capacity_exists() {
    let mut h = TestHarness::new();
    let _w1 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", two_activation_workloads_spec(timeout)).await;
    h.converge().await;

    // Activate wl-a.
    h.activate_service("ns", "svc-a").await;
    h.assert_workload_running("ns", "wl-a");
    h.deactivate_service("ns", "svc-a").await;

    // Worker is Normal pressure — plenty of capacity.
    // Activate wl-b — should just work, no preemption needed.
    h.activate_service("ns", "svc-b").await;
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
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_preempted_workload_can_reactivate() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", two_activation_workloads_spec(timeout)).await;
    h.converge().await;

    // Activate wl-a, then make it idle.
    h.activate_service("ns", "svc-a").await;
    h.deactivate_service("ns", "svc-a").await;

    // High pressure, activate wl-b to trigger preemption of wl-a.
    h.orchestrator_mut().workers.get_mut(&w1).unwrap().pressure_bands.memory = PressureBand::High;
    let svc_b_ip = h.service_ip("ns", "svc-b");
    h.worker(&w1).send_event(WorkerEvent::EndpointActivation {
        namespace_id: "ns".into(),
        ip: svc_b_ip,
        service_id: Some(ServiceId::from("svc-b")),
    });
    h.converge().await;

    // wl-a should be preempted.
    let wl_a_state = h.workload_state("ns", "wl-a");
    assert!(
        !matches!(wl_a_state, WorkloadState::Running { .. }),
        "wl-a should not be Running after preemption, got {:?}", wl_a_state
    );

    // Now restore pressure and let wl-b schedule.
    h.orchestrator_mut().workers.get_mut(&w1).unwrap().pressure_bands.memory = PressureBand::Normal;
    h.send_pressure_update(&w1, 0.0).await;
    h.assert_workload_running("ns", "wl-b");

    // Now deactivate wl-b and reactivate wl-a — it should come back.
    h.deactivate_service("ns", "svc-b").await;

    // Activate wl-a again.
    let svc_a_ip = h.service_ip("ns", "svc-a");
    h.worker(&w1).send_event(WorkerEvent::EndpointActivation {
        namespace_id: "ns".into(),
        ip: svc_a_ip,
        service_id: Some(ServiceId::from("svc-a")),
    });
    h.converge().await;

    h.assert_workload_running("ns", "wl-a");
    h.assert_service_active("ns", "svc-a");
}
