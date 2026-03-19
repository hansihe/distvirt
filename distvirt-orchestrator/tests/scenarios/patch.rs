use std::collections::BTreeMap;
use std::net::Ipv4Addr;

use crate::harness::*;
use distvirt_orchestrator::types::*;
use distvirt_worker_protocol::{ServicePolicy, WorkerCommand};

/// Patch: add a new workload and service to an existing namespace.
#[test]
fn test_patch_add_workload() {
    let mut h = TestHarness::new();
    h.add_worker();
    h.create_namespace("ns", always_on_spec());
    h.converge();
    h.assert_workload_running("ns", "echo");

    // Patch in a second workload + service
    let mut workloads = BTreeMap::new();
    workloads.insert(
        WorkloadName("echo-b".to_string()),
        WorkloadSpec {
            containers: vec![container_spec("docker.io/library/alpine:latest")],
            network: pod_network(11),
            suspend_on_idle: false,
            resources: None,
            activation: None,
            run_policy: Default::default(),
            respects_demand: false,
        },
    );
    let mut services = BTreeMap::new();
    services.insert(
        "svc-b".to_string(),
        ServiceSpec {
            workload_id: WorkloadName("echo-b".to_string()),
            ip: Ipv4Addr::new(172, 16, 0, 101),
            policy: ServicePolicy {
                buffer_frames: 100,
                timeout_ms: 5000,
                activator: None,
            },
            activation: None,
        },
    );
    h.patch_namespace(
        "ns",
        NamespacePatch {
            workloads,
            services,
            remove_workloads: vec![],
            remove_services: vec![],
        },
    );
    h.converge();

    // Both workloads should be running
    h.assert_workload_running("ns", "echo");
    h.assert_workload_running("ns", "echo-b");
}

/// Patch: remove a workload from a two-workload namespace.
#[test]
fn test_patch_remove_workload() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker();
    h.create_namespace("ns", always_on_two_workloads_spec());
    h.converge();
    h.assert_workload_running("ns", "echo-a");
    h.assert_workload_running("ns", "echo-b");

    // Patch: remove echo-b and its service
    h.patch_namespace(
        "ns",
        NamespacePatch {
            workloads: BTreeMap::new(),
            services: BTreeMap::new(),
            remove_workloads: vec![WorkloadName("echo-b".to_string())],
            remove_services: vec!["svc-b".to_string()],
        },
    );
    h.converge();

    // echo-a still running, echo-b gone from spec
    h.assert_workload_running("ns", "echo-a");
    let ns = h.namespace("ns");
    let spec = ns.current_spec().unwrap();
    assert!(
        !spec
            .workloads
            .contains_key(&WorkloadName("echo-b".to_string())),
        "removed workload 'echo-b' should not exist in spec"
    );
    assert!(
        !spec.services.contains_key("svc-b"),
        "removed service 'svc-b' should not exist in spec"
    );

    // StopPod should have been issued
    let stop_count = h.worker_command_count(&w1, |c| matches!(c, WorkerCommand::StopPod { .. }));
    assert!(stop_count >= 1, "expected StopPod for removed workload");
}

/// Patch: replace an existing workload's image (upsert over existing key).
#[test]
fn test_patch_replace_workload_image() {
    let mut h = TestHarness::new();
    h.add_worker();
    h.create_namespace("ns", always_on_spec());
    h.converge();
    h.assert_workload_running("ns", "echo");

    let old_pod_id = h.workload_state("ns", "echo").pod_id;

    // Patch: upsert "echo" with a new image
    let mut workloads = BTreeMap::new();
    workloads.insert(
        WorkloadName("echo".to_string()),
        WorkloadSpec {
            containers: vec![container_spec("docker.io/library/alpine:v2")],
            network: pod_network(10),
            suspend_on_idle: false,
            resources: None,
            activation: None,
            run_policy: Default::default(),
            respects_demand: false,
        },
    );
    h.patch_namespace(
        "ns",
        NamespacePatch {
            workloads,
            services: BTreeMap::new(),
            remove_workloads: vec![],
            remove_services: vec![],
        },
    );
    h.converge();

    // Should be running with a new pod
    h.assert_workload_running("ns", "echo");
    let new_pod_id = h.workload_state("ns", "echo").pod_id;
    assert_ne!(
        old_pod_id, new_pod_id,
        "pod should have been replaced after image change via patch"
    );
}
