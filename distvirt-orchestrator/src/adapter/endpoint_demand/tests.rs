use super::*;
use crate::core::EndpointDemandSignal;
use crate::sm::{
    DRouter, SCHEDULE_REQUEST, ServiceId, ServiceSm, ServiceSpec, TIMER, WorkerId, WorkerInfo,
    WorkloadId, WorkloadSm, WorkloadSpec,
};

const W1: WorkloadId = WorkloadId(1);
const S1: ServiceId = ServiceId(1);
const S2: ServiceId = ServiceId(2);
const WK1: WorkerId = WorkerId(1);
const WK2: WorkerId = WorkerId(2);

/// Set up a router with one service (activation mode) and one worker.
/// Returns (worker_id, endpoint_id).
fn setup_activation_service(router: &mut DRouter) -> (WorkerId, EndpointId) {
    router.create_timer(TIMER);
    let worker = WK1;
    router.create_worker(worker);
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new());
    router.set_workload_config_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v1".into(),
            ..Default::default()
        },
    );

    router.create_service(S1, ServiceSm::new());
    router.set_service_config_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: true,
            ..Default::default()
        },
    );
    router.propagate();

    let endpoint_id = router
        .get_service(&S1)
        .unwrap()
        .endpoint_id
        .expect("service should have created an endpoint");

    (worker, endpoint_id)
}

// ============================================================================
// 1. Push need creates port and sets level
// ============================================================================

#[test]
fn push_need_creates_port() {
    let mut router = DRouter::new_traced(16, distvirt_sm_router::trace::PanicTracer::new());
    let (worker, endpoint_id) = setup_activation_service(&mut router);

    let mut adapter = EndpointDemandAdapter::new();
    adapter.push_demand(
        &mut router,
        worker,
        endpoint_id,
        EndpointDemandSignal::Traffic,
    );
    router.propagate();

    // The service should see the backend need via its aggregated input.
    // Verify the adapter tracks the port.
    assert_eq!(adapter.ports.len(), 1);
    assert!(adapter.ports.contains_key(&(worker, endpoint_id)));
}

// ============================================================================
// 2. Push need twice reuses same port
// ============================================================================

#[test]
fn push_need_reuses_port() {
    let mut router = DRouter::new_traced(16, distvirt_sm_router::trace::PanicTracer::new());
    let (worker, endpoint_id) = setup_activation_service(&mut router);

    let mut adapter = EndpointDemandAdapter::new();
    adapter.push_demand(
        &mut router,
        worker,
        endpoint_id,
        EndpointDemandSignal::Traffic,
    );
    let port_id_first = adapter.ports[&(worker, endpoint_id)];

    adapter.push_demand(
        &mut router,
        worker,
        endpoint_id,
        EndpointDemandSignal::Active { active: true },
    );
    let port_id_second = adapter.ports[&(worker, endpoint_id)];

    assert_eq!(port_id_first, port_id_second);
    assert_eq!(adapter.ports.len(), 1);
}

// ============================================================================
// 3. Multiple workers create separate ports for same endpoint
// ============================================================================

#[test]
fn multiple_workers_separate_ports() {
    let mut router = DRouter::new_traced(16, distvirt_sm_router::trace::PanicTracer::new());
    let (worker1, endpoint_id) = setup_activation_service(&mut router);
    let worker2 = WK2;
    router.create_worker(worker2);
    router.set_worker_info(worker2, WorkerInfo { capacity: 10 });

    let mut adapter = EndpointDemandAdapter::new();
    adapter.push_demand(
        &mut router,
        worker1,
        endpoint_id,
        EndpointDemandSignal::Traffic,
    );
    adapter.push_demand(
        &mut router,
        worker2,
        endpoint_id,
        EndpointDemandSignal::Traffic,
    );

    assert_eq!(adapter.ports.len(), 2);
    assert_ne!(
        adapter.ports[&(worker1, endpoint_id)],
        adapter.ports[&(worker2, endpoint_id)]
    );
}

// ============================================================================
// 4. Remove worker cleans up all its ports
// ============================================================================

#[test]
fn remove_worker_cleans_up() {
    let mut router = DRouter::new_traced(16, distvirt_sm_router::trace::PanicTracer::new());
    router.create_timer(TIMER);
    let worker1 = WK1;
    router.create_worker(worker1);
    router.set_worker_info(worker1, WorkerInfo { capacity: 10 });
    let worker2 = WK2;
    router.create_worker(worker2);
    router.set_worker_info(worker2, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new());
    router.set_workload_config_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v1".into(),
            ..Default::default()
        },
    );

    router.create_service(S1, ServiceSm::new());
    router.create_service(S2, ServiceSm::new());
    router.set_service_config_edges(mgmt, vec![S1, S2]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: true,
            ..Default::default()
        },
    );
    router.propagate();

    let ep1 = router
        .get_service(&S1)
        .unwrap()
        .endpoint_id
        .expect("S1 should have an endpoint");
    let ep2 = router
        .get_service(&S2)
        .unwrap()
        .endpoint_id
        .expect("S2 should have an endpoint");

    let mut adapter = EndpointDemandAdapter::new();
    // Worker1 has need for both endpoints
    adapter.push_demand(&mut router, worker1, ep1, EndpointDemandSignal::Traffic);
    adapter.push_demand(
        &mut router,
        worker1,
        ep2,
        EndpointDemandSignal::Active { active: true },
    );
    // Worker2 has need for ep1 only
    adapter.push_demand(&mut router, worker2, ep1, EndpointDemandSignal::Traffic);
    assert_eq!(adapter.ports.len(), 3);

    // Remove worker1 — should remove its 2 ports, leave worker2's port
    adapter.remove_worker(&mut router, &worker1);
    assert_eq!(adapter.ports.len(), 1);
    assert!(adapter.ports.contains_key(&(worker2, ep1)));
}

// ============================================================================
// 5. Setting need to None keeps port (level is set, not port removed)
// ============================================================================

#[test]
fn need_none_keeps_port() {
    let mut router = DRouter::new_traced(16, distvirt_sm_router::trace::PanicTracer::new());
    let (worker, endpoint_id) = setup_activation_service(&mut router);

    let mut adapter = EndpointDemandAdapter::new();
    adapter.push_demand(
        &mut router,
        worker,
        endpoint_id,
        EndpointDemandSignal::Traffic,
    );
    adapter.push_demand(
        &mut router,
        worker,
        endpoint_id,
        EndpointDemandSignal::Active { active: false },
    );

    // Port still exists — only removed on worker disconnect
    assert_eq!(adapter.ports.len(), 1);
}
