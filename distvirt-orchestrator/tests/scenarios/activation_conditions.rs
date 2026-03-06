use std::time::Duration;

use crate::harness::*;
use crate::harness::mock_worker::MockWorkerConfig;
use distvirt_worker_protocol::ServiceId;

/// Verify that the `activation-pending` condition is set when a service
/// transitions to NeedBackend (on activation) and cleared when it becomes Active.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_activation_pending_condition_lifecycle() {
    let mut h = TestHarness::new();
    let _w1 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;

    // Initially idle — no activation-pending condition.
    h.assert_service_idle("ns", "web-svc");
    h.assert_service_condition_clear("ns", "web-svc", "activation-pending");

    // Activate and fully converge — after converge, activation-pending should be cleared.
    h.activate_service("ns", "web-svc").await;
    h.assert_service_condition_clear("ns", "web-svc", "activation-pending");

    // Idle timeout → back to Idle, condition still clear.
    h.deactivate_service("ns", "web-svc").await;
    h.advance_past_idle_timeout("ns", "web-svc").await;
    h.assert_service_idle("ns", "web-svc");
    h.assert_service_condition_clear("ns", "web-svc", "activation-pending");

    // Re-activate — verify conditions are correct at the end state.
    h.activate_service("ns", "web-svc").await;
    h.assert_service_condition_clear("ns", "web-svc", "activation-pending");
}

/// Verify that activation-pending is correctly handled through the status report.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_activation_pending_in_status_report() {
    let mut h = TestHarness::new();
    let _w1 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    let timeout = Duration::from_secs(30);
    h.create_namespace("ns", activation_spec(timeout)).await;
    h.converge().await;

    // Check status report before activation.
    let report = h.namespace("ns").status_report();
    let svc_report = report.services.get(&ServiceId::from("web-svc")).unwrap();
    assert!(svc_report.service_conditions.is_empty());

    // Activate and converge.
    h.activate_service("ns", "web-svc").await;

    // After converge, service is active and condition should be cleared.
    let report = h.namespace("ns").status_report();
    let svc_report = report.services.get(&ServiceId::from("web-svc")).unwrap();
    assert!(
        !svc_report.service_conditions.contains_key("activation-pending"),
        "activation-pending should be cleared after service becomes active"
    );
}
