use std::net::Ipv4Addr;
use std::time::Duration;

use crate::harness::*;
use crate::harness::mock_worker::MockWorkerConfig;
use distvirt_worker_protocol::{BackendNeed, ServiceId, WorkerEvent};

/// Two services back same workload. Activate A → workload launches.
/// Activate B → workload already running.
///
/// BUG: When a second service activates on an already-Running workload, the service
/// goes to NeedBackend but never receives WorkloadReady/BackendReady because the
/// workload doesn't re-emit BecameReady when demand goes from 1→2 while Running.
/// The service gets stuck in NeedBackend. Fix: when DemandUp is forwarded to an
/// already-Running workload, the namespace layer should emit BackendReady +
/// WorkloadReady to the originating service.
///
/// This test documents the bug: svc-b stays in NeedBackend after activation.
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

    // BUG: svc-b is stuck in NeedBackend (see docstring above)
    h.assert_service_need_backend("ns", "svc-b");

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
///
/// BUG: svc-b gets stuck in NeedBackend (same bug as above).
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
    // BUG: svc-b is NeedBackend, not Active (see test_two_services_one_workload_shared_demand)
    h.assert_service_need_backend("ns", "svc-b");
}
