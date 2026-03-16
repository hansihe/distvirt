use std::time::Duration;

use crate::adapter::timer::TimerConfig;
use crate::sm_new::PodStatus;
use crate::task::{ClientCommand, GlobalWorkerId};
use crate::types::{NamespaceId, NamespaceSpec, WorkloadSpec, ServiceSpec, ActivationSpec};

use super::{MockWorkerConfig, SyncShell};

fn test_timer_config() -> TimerConfig {
    TimerConfig {
        retry_backoff: Duration::from_millis(500),
        launch_timeout: Duration::from_secs(30),
        suspend_timeout: Duration::from_secs(30),
        idle_timeout: Duration::from_secs(60),
    }
}

fn default_network() -> distvirt_worker_protocol::NetworkConfig {
    distvirt_worker_protocol::NetworkConfig {
        subnet: std::net::Ipv4Addr::new(172, 16, 0, 0),
        gateway: std::net::Ipv4Addr::new(172, 16, 0, 1),
        prefix_len: 24,
        segment_id: None,
    }
}

fn pod_network(host_octet: u8) -> distvirt_worker_protocol::PodNetworkConfig {
    distvirt_worker_protocol::PodNetworkConfig {
        ip: std::net::Ipv4Addr::new(172, 16, 0, host_octet),
        mac: [0; 6],
        gateway: std::net::Ipv4Addr::new(172, 16, 0, 1),
        netmask: "255.255.255.0".to_string(),
    }
}

fn container_spec(image: &str) -> distvirt_worker_protocol::ContainerSpec {
    distvirt_worker_protocol::ContainerSpec {
        container_id: "main".to_string(),
        image_ref: image.to_string(),
        config: distvirt_worker_protocol::ContainerConfig {
            entrypoint: vec!["/bin/echo".to_string()],
            args: vec!["hello".to_string()],
            env: vec![],
            working_dir: None,
            uid: None,
            gid: None,
            hostname: None,
            capture_output: false,
            stdin: false,
        },
    }
}

fn ns(name: &str) -> NamespaceId {
    NamespaceId::from(name)
}

/// Always-on spec: 1 workload "echo" + 1 always-on service "echo-svc".
fn always_on_spec() -> NamespaceSpec {
    use std::collections::BTreeMap;
    use crate::types::WorkloadId;
    use distvirt_worker_protocol::{ServiceId, ServicePolicy};

    let wl_id = WorkloadId("echo".to_string());
    let svc_id = ServiceId::from("echo-svc");

    let mut workloads = BTreeMap::new();
    workloads.insert(
        wl_id.clone(),
        WorkloadSpec {
            containers: vec![container_spec("docker.io/library/alpine:latest")],
            network: pod_network(10),
            suspend_on_idle: false,
            resources: None,
            activation: None,
        },
    );

    let mut services = BTreeMap::new();
    services.insert(
        svc_id,
        ServiceSpec {
            workload_id: wl_id,
            ip: std::net::Ipv4Addr::new(172, 16, 0, 100),
            policy: ServicePolicy {
                buffer_frames: 100,
                timeout_ms: 5000,
                activator: None,
            },
            activation: None,
        },
    );

    NamespaceSpec {
        network: default_network(),
        workloads,
        services,
    }
}

// =============================================================================
// Tests
// =============================================================================

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn basic_pod_lifecycle() {
    let mut shell = SyncShell::new(test_timer_config());
    let w1 = shell.add_worker_default();

    shell.create_namespace(ns("test"), default_network());
    shell.client_command(&ns("test"), ClientCommand::UpdateSpec(always_on_spec()));
    shell.drain();

    // The workload should have been created and launched.
    let ns_core = shell.namespace(&ns("test")).expect("namespace should exist");
    let router = ns_core.router();
    let mgmt = ns_core.management();

    let wl_id = mgmt.lookup_workload("echo").expect("workload 'echo' should exist");
    let wl = router.get_workload(&wl_id).expect("workload should exist in router");

    // With mock worker auto-responding PodRunning, the workload should be running.
    assert!(wl.pod_running, "workload should have a running pod");
    assert!(wl.has_spec, "workload should have a spec");
    assert!(wl.pod_id.is_some(), "workload should have a pod");

    let pod_id = wl.pod_id.unwrap();
    let pod = router.get_pod(&pod_id).expect("pod should exist");
    assert_eq!(pod.status, PodStatus::Running, "pod should be Running");

    // Worker should have received LaunchPod.
    let cmds = shell.worker_commands(&w1);
    let has_launch = cmds.iter().any(|c| matches!(c, distvirt_worker_protocol::WorkerCommand::LaunchPod { .. }));
    assert!(has_launch, "worker should have received a LaunchPod command");
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn namespace_create_destroy() {
    let mut shell = SyncShell::new(test_timer_config());
    let _w1 = shell.add_worker_default();

    shell.create_namespace(ns("test"), default_network());
    assert!(shell.namespace(&ns("test")).is_some());

    shell.destroy_namespace(&ns("test"));
    assert!(shell.namespace(&ns("test")).is_none());
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn worker_disconnect_and_recovery() {
    let mut shell = SyncShell::new(test_timer_config());
    let w1 = shell.add_worker_default();

    shell.create_namespace(ns("test"), default_network());
    shell.client_command(&ns("test"), ClientCommand::UpdateSpec(always_on_spec()));
    shell.drain();

    // Verify running.
    let wl_id = shell.namespace(&ns("test")).unwrap().management().lookup_workload("echo").unwrap();
    assert!(shell.namespace(&ns("test")).unwrap().router().get_workload(&wl_id).unwrap().pod_running);

    // Disconnect the worker.
    shell.disconnect_worker(w1);
    shell.drain();

    // Pod should be gone or failed — workload should want a new pod but have no worker.
    let wl = shell.namespace(&ns("test")).unwrap().router().get_workload(&wl_id).unwrap();
    // Worker disconnect removes the worker from the router, causing pods to fail.
    // The workload should want a pod but it won't be running (no available workers).
    assert!(wl.wants_pod, "workload should still want a pod");

    // Add new worker — workload should recover.
    let _w2 = shell.add_worker_default();
    shell.drain();

    let wl = shell.namespace(&ns("test")).unwrap().router().get_workload(&wl_id).unwrap();
    assert!(wl.pod_running, "workload should recover after new worker added");
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn launch_hang_triggers_timeout() {
    let mut shell = SyncShell::new(test_timer_config());
    let _w1 = shell.add_worker(MockWorkerConfig::with_launch_hang());

    shell.create_namespace(ns("test"), default_network());
    shell.client_command(&ns("test"), ClientCommand::UpdateSpec(always_on_spec()));
    shell.drain();

    let wl_id = shell.namespace(&ns("test")).unwrap().management().lookup_workload("echo").unwrap();

    // Pod should be pending (launched but no PodRunning response).
    let wl = shell.namespace(&ns("test")).unwrap().router().get_workload(&wl_id).unwrap();
    assert!(wl.pod_id.is_some(), "workload should have created a pod");
    assert!(!wl.pod_running, "pod should not be running (launch is hung)");

    let pod_id = wl.pod_id.unwrap();
    let pod = shell.namespace(&ns("test")).unwrap().router().get_pod(&pod_id).unwrap();
    assert_eq!(pod.status, PodStatus::Pending, "pod should still be Pending");

    // Advance past launch timeout.
    tokio::time::advance(Duration::from_secs(31)).await;
    shell.drain();

    // After timeout, the pod should have failed and the workload should be in backoff.
    let wl = shell.namespace(&ns("test")).unwrap().router().get_workload(&wl_id).unwrap();
    assert!(!wl.pod_running, "pod should not be running after timeout");
    assert!(wl.in_backoff || wl.consecutive_failures > 0,
        "workload should be in backoff or have failures: in_backoff={}, failures={}",
        wl.in_backoff, wl.consecutive_failures);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn launch_failure_retries() {
    let mut shell = SyncShell::new(test_timer_config());
    let _w1 = shell.add_worker(MockWorkerConfig::with_launch_failure());

    shell.create_namespace(ns("test"), default_network());
    shell.client_command(&ns("test"), ClientCommand::UpdateSpec(always_on_spec()));
    shell.drain();

    let wl_id = shell.namespace(&ns("test")).unwrap().management().lookup_workload("echo").unwrap();

    // Pod should have failed and workload should be in backoff.
    let wl = shell.namespace(&ns("test")).unwrap().router().get_workload(&wl_id).unwrap();
    assert!(!wl.pod_running, "pod should not be running after launch failure");
    assert!(wl.consecutive_failures > 0, "should have failures recorded");
}
