use std::time::Duration;

use crate::harness::mock_worker::MockWorkerConfig;
use crate::harness::*;
use distvirt_orchestrator::types::NamespaceStatus;
use distvirt_worker_protocol::{ServiceId, WorkerCommand, WorkerEvent};

// === Tests from activation.rs ===

#[test]
fn test_activation_idle_cycle() {
    let mut h = TestHarness::new();
    let _w1 = h.add_worker_with(MockWorkerConfig::with_pool());
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout));
    h.converge();
    h.assert_workload_dormant("ns", "web");
    h.assert_service_idle("ns", "web-svc");

    // Activate → running
    h.activate_service("ns", "web-svc");

    // Signal no more traffic
    h.deactivate_service("ns", "web-svc");

    // Advance past idle timeout
    h.advance_past_idle_timeout("ns", "web-svc");
    h.assert_workload_suspended("ns", "web");
    h.assert_service_idle("ns", "web-svc");
}

// === Tests from activation_conditions.rs ===

/// Verify that the `activation-pending` condition is set when a service
/// transitions to NeedBackend (on activation) and cleared when it becomes Active.
#[test]
fn test_activation_pending_condition_lifecycle() {
    let mut h = TestHarness::new();
    // Suppress both LaunchPod and ResumePod so we can observe NeedBackend mid-flow.
    let config = MockWorkerConfig {
        handler: Some(Box::new(|cmd| match cmd {
            WorkerCommand::LaunchPod { .. } | WorkerCommand::ResumePod { .. } => Some(vec![]),
            _ => None,
        })),
        capabilities: MockWorkerConfig::with_pool().capabilities,
        ..Default::default()
    };
    let w1 = h.add_worker_with(config);
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout));
    h.converge();

    // Initially idle — no activation-pending condition.
    h.assert_service_idle("ns", "web-svc");
    h.assert_service_condition_clear("ns", "web-svc", "activation-pending");

    // Send EndpointActivation manually (activate_service helper asserts Running, which won't happen).
    let svc_ip = h.service_ip("ns", "web-svc");
    h.worker(&w1).send_event(WorkerEvent::EndpointActivation {
        namespace_id: "ns".into(),
        ip: svc_ip,
        service_id: Some(ServiceId::from("web-svc")),
    });
    h.converge();

    // Workload is Launching (hung), service should be in NeedBackend with condition set.
    h.assert_workload_launching("ns", "web");
    h.assert_service_need_backend("ns", "web-svc");
    h.assert_service_condition_set("ns", "web-svc", "activation-pending");

    // Complete the launch by injecting PodRunning.
    let pod_id = h
        .workload_proto_pod_id("ns", "web")
        .expect("expected pod_id");
    h.worker(&w1).send_event(WorkerEvent::PodRunning {
        namespace_id: "ns".into(),
        pod_id,
    });
    h.converge();

    // Now active — activation-pending should be cleared.
    h.assert_service_active("ns", "web-svc");
    h.assert_service_condition_clear("ns", "web-svc", "activation-pending");

    // Idle timeout → suspend (default handler handles SuspendPod).
    h.deactivate_service("ns", "web-svc");
    h.advance_past_idle_timeout("ns", "web-svc");
    h.assert_service_idle("ns", "web-svc");
    h.assert_service_condition_clear("ns", "web-svc", "activation-pending");

    // Re-activate — ResumePod is also hung, so condition should reappear.
    h.worker(&w1).send_event(WorkerEvent::EndpointActivation {
        namespace_id: "ns".into(),
        ip: svc_ip,
        service_id: Some(ServiceId::from("web-svc")),
    });
    h.converge();

    // During resume, service should be in NeedBackend with activation-pending set again.
    h.assert_service_need_backend("ns", "web-svc");
    h.assert_service_condition_set("ns", "web-svc", "activation-pending");
}

/// Verify that activation-pending appears in the status report while the condition is active.
#[test]
fn test_activation_pending_in_status_report() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_launch_hang().add_pool());
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout));
    h.converge();

    // Check status report before activation.
    // status_report() not available in new system — check service conditions stub
    let conditions = h.service_conditions("ns", "web-svc");
    assert!(conditions.is_empty());

    // Send activation manually so we can observe mid-flow.
    let svc_ip = h.service_ip("ns", "web-svc");
    h.worker(&w1).send_event(WorkerEvent::EndpointActivation {
        namespace_id: "ns".into(),
        ip: svc_ip,
        service_id: Some(ServiceId::from("web-svc")),
    });
    h.converge();

    // While in NeedBackend, status report should contain activation-pending.
    let conditions = h.service_conditions("ns", "web-svc");
    assert!(
        conditions.contains_key("activation-pending"),
        "activation-pending should be present in status report while service is in NeedBackend"
    );

    // Complete the launch.
    let pod_id = h
        .workload_proto_pod_id("ns", "web")
        .expect("expected pod_id");
    h.worker(&w1).send_event(WorkerEvent::PodRunning {
        namespace_id: "ns".into(),
        pod_id,
    });
    h.converge();

    // After becoming active, condition should be cleared in status report.
    let conditions = h.service_conditions("ns", "web-svc");
    assert!(
        !conditions.contains_key("activation-pending"),
        "activation-pending should be cleared after service becomes active"
    );
}

// === Tests from lifecycle.rs ===

#[test]
fn test_always_on_service_lifecycle() {
    let mut h = TestHarness::new();
    h.add_worker();
    h.create_namespace("ns", always_on_spec());
    h.converge();
    h.assert_namespace_status("ns", NamespaceStatus::Active);
    h.assert_workload_running("ns", "echo");
    h.assert_service_active("ns", "echo-svc");
    h.delete_namespace("ns");
    h.converge();
    h.assert_namespace_absent("ns");
}
