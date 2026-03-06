use std::time::Duration;

use std::net::Ipv4Addr;

use distvirt_worker_protocol::{BackendNeed, ServiceId, WorkerCommand, WorkerEvent};

use crate::harness::mock_worker::MockWorkerConfig;
use crate::harness::*;

/// Test: In a two-worker setup, verify FabricRouteUpdate is sent to the non-hosting worker
/// when a pod launches, and route is removed when pod stops.
///
/// This verifies the orchestrator's route management in multi-worker namespaces.
#[tokio::test(start_paused = true)]
async fn test_fabric_route_update_on_pod_launch() {
    let mut h = TestHarness::new();

    let w1 = h.add_worker().await;
    let w2 = h.add_worker().await;
    h.converge().await;

    // Create always-on namespace — pod will launch on one worker.
    let spec = always_on_spec();
    h.create_namespace("ns1", spec).await;
    h.converge().await;

    h.assert_namespace_status("ns1", distvirt_orchestrator::types::NamespaceStatus::Active);
    h.assert_workload_running("ns1", "echo");

    // Determine which worker got the pod.
    let state = h.workload_state("ns1", "echo");
    let pod_worker_id = match state {
        distvirt_orchestrator::types::WorkloadState::Running { worker_id, .. } => {
            worker_id.clone()
        }
        _ => panic!("expected Running"),
    };

    // The other worker should have received a FabricRouteUpdate with the pod's route.
    let other_worker_id = if pod_worker_id == w1 { &w2 } else { &w1 };

    h.assert_worker_received_command_matching(
        other_worker_id,
        "FabricRouteUpdate or FabricRouteSync with route entry",
        |cmd| matches!(
            cmd,
            WorkerCommand::FabricRouteUpdate { added, .. } if !added.is_empty()
        ) || matches!(
            cmd,
            WorkerCommand::FabricRouteSync { routes, .. } if !routes.is_empty()
        ),
    );

    // The hosting worker should NOT have a route to its own pod
    // (neither via incremental FabricRouteUpdate nor full FabricRouteSync).
    h.assert_worker_did_not_receive_command_matching(
        &pod_worker_id,
        "FabricRouteUpdate or FabricRouteSync adding a route to self",
        |cmd| matches!(
            cmd,
            WorkerCommand::FabricRouteUpdate { added, .. } if !added.is_empty()
        ) || matches!(
            cmd,
            WorkerCommand::FabricRouteSync { routes, .. } if !routes.is_empty()
        ),
    );
}

/// Test: In a two-worker activation setup, verify route changes through
/// launch → suspend → resume lifecycle.
#[tokio::test(start_paused = true)]
async fn test_fabric_route_lifecycle_with_suspend_resume() {
    let mut h = TestHarness::new();

    // Need pool for suspend/resume.
    let w1 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    let w2 = h.add_worker_with(MockWorkerConfig::with_pool()).await;
    h.converge().await;

    let spec = activation_spec(Duration::from_secs(30));
    h.create_namespace("ns1", spec).await;
    h.converge().await;

    // Activate via ServiceActivation.
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns1".into(),
        service_id: ServiceId::from("web-svc"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 100),
    });
    h.converge().await;
    h.assert_workload_running("ns1", "web");

    // Determine which worker hosts the pod.
    let state = h.workload_state("ns1", "web");
    let pod_worker_id = match state {
        distvirt_orchestrator::types::WorkloadState::Running { worker_id, .. } => {
            worker_id.clone()
        }
        _ => panic!("expected Running"),
    };
    let other_worker_id = if pod_worker_id == w1 {
        w2.clone()
    } else {
        w1.clone()
    };

    // The other worker should have a route entry pointing to the hosting worker.
    h.assert_worker_received_command_matching(
        &other_worker_id,
        "FabricRouteUpdate or FabricRouteSync with route",
        |cmd| matches!(
            cmd,
            WorkerCommand::FabricRouteUpdate { added, .. } if !added.is_empty()
        ) || matches!(
            cmd,
            WorkerCommand::FabricRouteSync { routes, .. } if !routes.is_empty()
        ),
    );

    // Idle → suspend.
    h.worker(&w1).send_event(WorkerEvent::ServiceBackendNeed {
        namespace_id: "ns1".into(),
        service_id: ServiceId::from("web-svc"),
        need: BackendNeed::None,
    });
    h.converge().await;
    h.advance_time(Duration::from_secs(31)).await;
    h.assert_workload_suspended("ns1", "web");

    // After suspend, the route should have been removed (FabricRouteUpdate with removed_ips).
    // Check the other worker received a route removal.
    h.assert_worker_received_command_matching(
        &other_worker_id,
        "FabricRouteUpdate with removed_ips (after suspend)",
        |cmd| matches!(
            cmd,
            WorkerCommand::FabricRouteUpdate { removed_ips, .. } if !removed_ips.is_empty()
        ),
    );

    // Re-activate via ServiceActivation → resume.
    h.worker(&w1).send_event(WorkerEvent::ServiceActivation {
        namespace_id: "ns1".into(),
        service_id: ServiceId::from("web-svc"),
        dst_ip: Ipv4Addr::new(172, 16, 0, 100),
    });
    h.converge().await;
    h.assert_workload_running("ns1", "web");

    // After resume, a new route should have been added for the pod.
    // We can check that the other worker got another FabricRouteUpdate with added entries
    // (there should be at least 2 FabricRouteUpdate with added entries: one from initial launch,
    // one from resume).
    let other_commands = h.worker(&other_worker_id).commands();
    let route_adds: Vec<_> = other_commands
        .iter()
        .filter(|cmd| matches!(cmd, WorkerCommand::FabricRouteUpdate { added, .. } if !added.is_empty()))
        .collect();
    assert!(
        route_adds.len() >= 2,
        "expected at least 2 FabricRouteUpdate with added entries (launch + resume), got {}: {:#?}",
        route_adds.len(),
        route_adds,
    );
}
