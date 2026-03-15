use super::*;
use crate::sm_new::{
    BackendNeed, Router, ServiceSm, ServiceSpec, WorkerInfo, WorkloadId, WorkloadSm, WorkloadSpec,
    SCHEDULE_REQUEST, TIMER,
};

const W1: WorkloadId = WorkloadId(1);
const S1: ServiceId = ServiceId(1);

/// Set up a router with one activation-mode service and one worker.
fn setup(router: &mut Router) -> WorkerId {
    router.create_timer(TIMER);
    let worker = router.create_worker();
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new());
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v1".into() });

    router.create_service(S1, ServiceSm::new(true));
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: true,
        },
    );
    router.propagate();

    worker
}

#[test]
fn set_active_creates_port() {
    let mut router = Router::new(16);
    let worker = setup(&mut router);

    let mut adapter = FlowDemandAdapter::new();
    adapter.set_active(&mut router, worker, S1);
    router.propagate();

    assert_eq!(adapter.ports.len(), 1);
    assert!(adapter.ports.contains_key(&(worker, S1)));
}

#[test]
fn set_active_twice_reuses_port() {
    let mut router = Router::new(16);
    let worker = setup(&mut router);

    let mut adapter = FlowDemandAdapter::new();
    adapter.set_active(&mut router, worker, S1);
    let first = adapter.ports[&(worker, S1)];
    adapter.set_active(&mut router, worker, S1);
    let second = adapter.ports[&(worker, S1)];

    assert_eq!(first, second);
    assert_eq!(adapter.ports.len(), 1);
}

#[test]
fn set_inactive_keeps_port() {
    let mut router = Router::new(16);
    let worker = setup(&mut router);

    let mut adapter = FlowDemandAdapter::new();
    adapter.set_active(&mut router, worker, S1);
    adapter.set_inactive(&mut router, worker, S1);

    assert_eq!(adapter.ports.len(), 1);
}

#[test]
fn remove_worker_cleans_up() {
    let mut router = Router::new(16);
    let worker = setup(&mut router);

    let mut adapter = FlowDemandAdapter::new();
    adapter.set_active(&mut router, worker, S1);
    assert_eq!(adapter.ports.len(), 1);

    adapter.remove_worker(&mut router, &worker);
    assert_eq!(adapter.ports.len(), 0);
}
