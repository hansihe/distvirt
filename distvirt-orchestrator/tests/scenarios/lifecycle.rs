use crate::harness::*;
use distvirt_orchestrator::types::NamespaceStatus;

#[tokio::test]
async fn test_always_on_service_lifecycle() {
    let mut h = TestHarness::new();
    h.add_worker().await;
    h.create_namespace("ns", always_on_spec()).await;
    h.converge().await;
    h.assert_namespace_status("ns", NamespaceStatus::Active);
    h.assert_workload_running("ns", "echo");
    h.assert_service_active("ns", "echo-svc");
    h.delete_namespace("ns").await;
    h.converge().await;
    h.assert_namespace_absent("ns");
}
