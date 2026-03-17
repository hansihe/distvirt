use super::*;
use crate::sm::{
    DRouter, DNS_REGISTRY, ENDPOINT, SCHEDULE_REQUEST, ServiceSm, ServiceSpec, TIMER,
    WorkloadId, WorkloadSm, WorkloadSpec,
};

const W1: WorkloadId = WorkloadId(1);
const S1: ServiceId = ServiceId(1);

/// Set up a router with the DNS registry port and a service with DNS info.
fn setup_with_dns(router: &mut DRouter) -> DnsRegistryAdapter {
    router.create_timer(TIMER);
    router.create_schedule_request(SCHEDULE_REQUEST);
    router.create_endpoint(ENDPOINT);
    router.create_dns_registry(DNS_REGISTRY);

    let adapter = DnsRegistryAdapter::new(DNS_REGISTRY);
    adapter
}

#[test]
fn service_with_dns_produces_add_action() {
    let mut router = DRouter::new_traced(16, distvirt_sm_router::trace::PanicTracer::new());
    let mut adapter = setup_with_dns(&mut router);

    // Create workload and service with DNS info.
    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new());
    router.set_workload_config_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec {
        image: "app:v1".into(),
        ..Default::default()
    });

    router.create_service(S1, ServiceSm::new(false));
    router.set_service_config_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(mgmt, ServiceSpec {
        workload: W1,
        has_activation: false,
        dns_name: Some("echo-svc".into()),
        dns_ip: Some(std::net::Ipv4Addr::new(172, 16, 0, 100)),
        ..Default::default()
    });

    router.propagate();
    let (actions, _) = adapter.reconcile(&mut router);

    assert_eq!(actions.len(), 1);
    assert_eq!(
        actions[0],
        DnsRegistryAction::Add {
            name: "echo-svc".into(),
            ip: std::net::Ipv4Addr::new(172, 16, 0, 100),
        }
    );

    // Cache should reflect the entry.
    let sync = adapter.build_sync();
    assert_eq!(sync.len(), 1);
    assert!(sync.iter().any(|(n, ip)| n == "echo-svc" && *ip == std::net::Ipv4Addr::new(172, 16, 0, 100)));
}

#[test]
fn service_removal_produces_remove_action() {
    let mut router = DRouter::new_traced(16, distvirt_sm_router::trace::PanicTracer::new());
    let mut adapter = setup_with_dns(&mut router);

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new());
    router.set_workload_config_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec {
        image: "app:v1".into(),
        ..Default::default()
    });

    router.create_service(S1, ServiceSm::new(false));
    router.set_service_config_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(mgmt, ServiceSpec {
        workload: W1,
        has_activation: false,
        dns_name: Some("echo-svc".into()),
        dns_ip: Some(std::net::Ipv4Addr::new(172, 16, 0, 100)),
        ..Default::default()
    });

    router.propagate();
    let _ = adapter.reconcile(&mut router).0;

    // Now destroy the management port → service self-destructs.
    router.destroy_management(mgmt);
    router.propagate();

    let (actions, _) = adapter.reconcile(&mut router);
    assert!(
        actions.iter().any(|a| matches!(a, DnsRegistryAction::Remove { name } if name == "echo-svc")),
        "expected Remove action for echo-svc, got {:?}",
        actions
    );

    // Cache should be empty.
    assert!(adapter.build_sync().is_empty());
}

#[test]
fn no_dns_info_no_actions() {
    let mut router = DRouter::new_traced(16, distvirt_sm_router::trace::PanicTracer::new());
    let mut adapter = setup_with_dns(&mut router);

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new());
    router.set_workload_config_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec {
        image: "app:v1".into(),
        ..Default::default()
    });

    // Service without DNS info.
    router.create_service(S1, ServiceSm::new(false));
    router.set_service_config_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(mgmt, ServiceSpec {
        workload: W1,
        has_activation: false,
        dns_name: None,
        dns_ip: None,
        ..Default::default()
    });

    router.propagate();
    let (actions, _) = adapter.reconcile(&mut router);
    assert!(actions.is_empty());
}
