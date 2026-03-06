use std::time::Duration;

use crate::harness::*;
use crate::harness::mock_worker::MockWorkerConfig;
use distvirt_worker_protocol::{ServiceId, WorkerCommand, WorkerEvent};

/// Verify that the `activation-pending` condition is set when a service
/// transitions to NeedBackend (on activation) and cleared when it becomes Active.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_activation_pending_condition_lifecycle() {
    let mut h = TestHarness::new();
    // Suppress both LaunchPod and ResumePod so we can observe NeedBackend mid-flow.
    let config = MockWorkerConfig {
        handler: Some(Box::new(|cmd| match cmd {
            WorkerCommand::LaunchPod { .. } | WorkerCommand::ResumePod { .. } => Some(vec![]),
            _ => None,
        })),
        capabilities: MockWorkerConfig::with_pool().capabilities,
    };
    let w1 = h.add_worker_with(config).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;

    // Initially idle — no activation-pending condition.
    h.assert_service_idle("ns", "web-svc");
    h.assert_service_condition_clear("ns", "web-svc", "activation-pending");

    // Send ServiceActivation manually (activate_service helper asserts Running, which won't happen).
    let svc_ip = h.service_ip("ns", "web-svc");
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        dst_ip: svc_ip,
    });
    h.converge().await;

    // Workload is Launching (hung), service should be in NeedBackend with condition set.
    h.assert_workload_launching("ns", "web");
    h.assert_service_need_backend("ns", "web-svc");
    h.assert_service_condition_set("ns", "web-svc", "activation-pending");

    // Complete the launch by injecting PodRunning.
    let pod_id = h.workload_state("ns", "web").pod_id().unwrap().clone();
    h.worker(&w1).send_event(WorkerEvent::PodRunning {
        namespace_id: "ns".into(),
        pod_id,
    });
    h.converge().await;

    // Now active — activation-pending should be cleared.
    h.assert_service_active("ns", "web-svc");
    h.assert_service_condition_clear("ns", "web-svc", "activation-pending");

    // Idle timeout → suspend (default handler handles SuspendPod).
    h.deactivate_service("ns", "web-svc").await;
    h.advance_past_idle_timeout("ns", "web-svc").await;
    h.assert_service_idle("ns", "web-svc");
    h.assert_service_condition_clear("ns", "web-svc", "activation-pending");

    // Re-activate — ResumePod is also hung, so condition should reappear.
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        dst_ip: svc_ip,
    });
    h.converge().await;

    // During resume, service should be in NeedBackend with activation-pending set again.
    h.assert_service_need_backend("ns", "web-svc");
    h.assert_service_condition_set("ns", "web-svc", "activation-pending");
}

/// Verify that activation-pending appears in the status report while the condition is active.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_activation_pending_in_status_report() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker_with(MockWorkerConfig::with_launch_hang().add_pool()).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;

    // Check status report before activation.
    let report = h.namespace("ns").status_report();
    let svc_report = report.services.get(&ServiceId::from("web-svc")).unwrap();
    assert!(svc_report.service_conditions.is_empty());

    // Send activation manually so we can observe mid-flow.
    let svc_ip = h.service_ip("ns", "web-svc");
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns".into(),
        service_id: ServiceId::from("web-svc"),
        dst_ip: svc_ip,
    });
    h.converge().await;

    // While in NeedBackend, status report should contain activation-pending.
    let report = h.namespace("ns").status_report();
    let svc_report = report.services.get(&ServiceId::from("web-svc")).unwrap();
    assert!(
        svc_report.service_conditions.contains_key("activation-pending"),
        "activation-pending should be present in status report while service is in NeedBackend"
    );

    // Complete the launch.
    let pod_id = h.workload_state("ns", "web").pod_id().unwrap().clone();
    h.worker(&w1).send_event(WorkerEvent::PodRunning {
        namespace_id: "ns".into(),
        pod_id,
    });
    h.converge().await;

    // After becoming active, condition should be cleared in status report.
    let report = h.namespace("ns").status_report();
    let svc_report = report.services.get(&ServiceId::from("web-svc")).unwrap();
    assert!(
        !svc_report.service_conditions.contains_key("activation-pending"),
        "activation-pending should be cleared after service becomes active"
    );
}
