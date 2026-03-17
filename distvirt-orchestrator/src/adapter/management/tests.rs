use std::net::Ipv4Addr;

use crate::sm::{AdminCmd, DRouter, SCHEDULE_REQUEST, TIMER};
use crate::types::{ActivationSpec, NamespaceSpec, ServiceSpec, WorkloadSpec};

use super::ManagementAdapter;

fn make_router() -> DRouter {
    let mut router = DRouter::new_traced(16, distvirt_sm_router::trace::PanicTracer::new());
    router.create_timer(TIMER);
    router.create_schedule_request(SCHEDULE_REQUEST);
    router
}

fn simple_workload_spec() -> WorkloadSpec {
    WorkloadSpec {
        containers: vec![distvirt_worker_protocol::ContainerSpec {
            container_id: "main".into(),
            image_ref: "app:v1".into(),
            config: distvirt_worker_protocol::ContainerConfig {
                entrypoint: vec![],
                args: vec![],
                env: vec![],
                working_dir: None,
                uid: None,
                gid: None,
                hostname: None,
                capture_output: false,
                stdin: false,
            },
        }],
        network: distvirt_worker_protocol::PodNetworkConfig {
            ip: Ipv4Addr::new(10, 0, 0, 1),
            mac: [0; 6],
            gateway: Ipv4Addr::new(10, 0, 0, 1),
            netmask: "255.255.0.0".into(),
        },
        suspend_on_idle: false,
        resources: None,
        activation: None,
    }
}

fn simple_service_spec(workload_name: &str) -> ServiceSpec {
    ServiceSpec {
        workload_id: crate::types::WorkloadName(workload_name.into()),
        ip: Ipv4Addr::new(10, 0, 1, 1),
        policy: distvirt_worker_protocol::ServicePolicy {
            buffer_frames: 0,
            timeout_ms: 0,
            activator: None,
        },
        activation: None,
    }
}

fn make_namespace_spec(
    workloads: Vec<(&str, WorkloadSpec)>,
    services: Vec<(&str, ServiceSpec)>,
) -> NamespaceSpec {
    NamespaceSpec {
        network: distvirt_worker_protocol::NetworkConfig {
            subnet: Ipv4Addr::new(10, 0, 0, 0),
            gateway: Ipv4Addr::new(10, 0, 0, 1),
            prefix_len: 16,
            segment_id: None,
        },
        workloads: workloads
            .into_iter()
            .map(|(n, s)| (crate::types::WorkloadName(n.into()), s))
            .collect(),
        services: services
            .into_iter()
            .map(|(n, s)| (n.to_string(), s))
            .collect(),
    }
}

// ============================================================================
// 1. Create workload via spec, verify SM exists and has spec signal
// ============================================================================

#[test]
fn create_workload_from_spec() {
    let mut router = make_router();
    let mut adapter = ManagementAdapter::new();

    let spec = make_namespace_spec(vec![("web", simple_workload_spec())], vec![]);

    adapter.apply_namespace_spec(&mut router, None, &spec);
    router.propagate();

    let wl_id = adapter
        .lookup_workload("web")
        .expect("workload should be mapped");
    let wl = router
        .get_workload(&wl_id)
        .expect("workload should exist in router");
    assert!(wl.has_spec, "workload should have spec after apply");
}

// ============================================================================
// 2. Update workload spec, verify signal updated
// ============================================================================

#[test]
fn update_workload_spec() {
    let mut router = make_router();
    let mut adapter = ManagementAdapter::new();

    let spec1 = make_namespace_spec(vec![("web", simple_workload_spec())], vec![]);
    adapter.apply_namespace_spec(&mut router, None, &spec1);
    router.propagate();

    let wl_id = adapter.lookup_workload("web").unwrap();
    let wl_before = router.get_workload(&wl_id).unwrap().clone();

    // Update the spec (different image)
    let mut spec2_wl = simple_workload_spec();
    spec2_wl.containers[0].image_ref = "app:v2".into();
    let spec2 = make_namespace_spec(vec![("web", spec2_wl)], vec![]);
    adapter.apply_namespace_spec(&mut router, Some(&spec1), &spec2);
    router.propagate();

    let wl_after = router.get_workload(&wl_id).unwrap();
    assert!(
        wl_after.spec_version > wl_before.spec_version,
        "spec version should increment on update"
    );
}

// ============================================================================
// 3. Remove workload, verify Management port destroyed
// ============================================================================

#[test]
fn remove_workload_destroys_management() {
    let mut router = make_router();
    let mut adapter = ManagementAdapter::new();

    let spec1 = make_namespace_spec(vec![("web", simple_workload_spec())], vec![]);
    adapter.apply_namespace_spec(&mut router, None, &spec1);
    router.propagate();

    let wl_id = adapter.lookup_workload("web").unwrap();
    assert!(router.get_workload(&wl_id).is_some());

    let spec2 = make_namespace_spec(vec![], vec![]);
    adapter.apply_namespace_spec(&mut router, Some(&spec1), &spec2);
    router.propagate();

    assert!(adapter.lookup_workload("web").is_none());
}

// ============================================================================
// 4. Create service linked to workload
// ============================================================================

#[test]
fn create_service_linked_to_workload() {
    let mut router = make_router();
    let mut adapter = ManagementAdapter::new();

    let spec = make_namespace_spec(
        vec![("web", simple_workload_spec())],
        vec![("web-svc", simple_service_spec("web"))],
    );

    adapter.apply_namespace_spec(&mut router, None, &spec);
    router.propagate();

    let _wl_id = adapter.lookup_workload("web").expect("workload mapped");
    let svc_id = adapter.lookup_service("web-svc").expect("service mapped");
    let svc = router
        .get_service(&svc_id)
        .expect("service exists in router");

    assert!(!svc.has_activation);
}

// ============================================================================
// 5. AdminCommand dispatch
// ============================================================================

#[test]
fn admin_command_dispatched() {
    let mut router = make_router();
    let mut adapter = ManagementAdapter::new();

    let spec = make_namespace_spec(
        vec![("web", simple_workload_spec())],
        vec![("web-svc", simple_service_spec("web"))],
    );

    adapter.apply_namespace_spec(&mut router, None, &spec);
    router.propagate();

    let wl_id = adapter.lookup_workload("web").unwrap();
    let wl = router.get_workload(&wl_id).unwrap();
    assert!(
        wl.pod_id.is_some(),
        "should have created a pod from demand+spec"
    );

    adapter.send_admin_command(&mut router, "web", AdminCmd::Restart);
    router.propagate();

    let wl_after = router.get_workload(&wl_id).unwrap();
    assert!(wl_after.has_spec);
}

// ============================================================================
// 6. ActivateService command
// ============================================================================

#[test]
fn activate_service_command() {
    let mut router = make_router();
    let mut adapter = ManagementAdapter::new();

    let mut svc_spec = simple_service_spec("web");
    svc_spec.activation = Some(ActivationSpec {
        idle_timeout: std::time::Duration::from_secs(60),
    });

    let spec = make_namespace_spec(
        vec![("web", simple_workload_spec())],
        vec![("web-svc", svc_spec)],
    );

    adapter.apply_namespace_spec(&mut router, None, &spec);
    router.propagate();

    let svc_id = adapter.lookup_service("web-svc").unwrap();
    let svc = router.get_service(&svc_id).unwrap();
    assert!(svc.has_activation);

    adapter.send_activate_service(&mut router, "web-svc", true);
    router.propagate();

    let wl_id = adapter.lookup_workload("web").unwrap();
    let wl = router.get_workload(&wl_id).unwrap();
    assert!(
        wl.has_demand,
        "workload should have demand after service activation"
    );
}

// ============================================================================
// 7. Multiple workloads and services
// ============================================================================

#[test]
fn multiple_workloads_and_services() {
    let mut router = make_router();
    let mut adapter = ManagementAdapter::new();

    let spec = make_namespace_spec(
        vec![
            ("web", simple_workload_spec()),
            ("api", simple_workload_spec()),
        ],
        vec![
            ("web-svc", simple_service_spec("web")),
            ("api-svc", simple_service_spec("api")),
        ],
    );

    adapter.apply_namespace_spec(&mut router, None, &spec);
    router.propagate();

    assert!(adapter.lookup_workload("web").is_some());
    assert!(adapter.lookup_workload("api").is_some());
    assert!(adapter.lookup_service("web-svc").is_some());
    assert!(adapter.lookup_service("api-svc").is_some());

    let spec2 = make_namespace_spec(
        vec![("web", simple_workload_spec())],
        vec![("web-svc", simple_service_spec("web"))],
    );

    adapter.apply_namespace_spec(&mut router, Some(&spec), &spec2);
    router.propagate();

    assert!(adapter.lookup_workload("web").is_some());
    assert!(adapter.lookup_workload("api").is_none());
    assert!(adapter.lookup_service("web-svc").is_some());
    assert!(adapter.lookup_service("api-svc").is_none());
}

// ============================================================================
// 8. Proto name lookups
// ============================================================================

#[test]
fn proto_name_roundtrip() {
    let mut router = make_router();
    let mut adapter = ManagementAdapter::new();

    let spec = make_namespace_spec(
        vec![("my-workload", simple_workload_spec())],
        vec![("my-service", simple_service_spec("my-workload"))],
    );

    adapter.apply_namespace_spec(&mut router, None, &spec);

    let wl_id = adapter.lookup_workload("my-workload").unwrap();
    assert_eq!(adapter.workload_proto_name(&wl_id), Some("my-workload"));

    let svc_id = adapter.lookup_service("my-service").unwrap();
    assert_eq!(adapter.service_proto_name(&svc_id), Some("my-service"));
}
