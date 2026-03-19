//! Integration tests: observability events flow through the async shell's EventBus
//! with real workers connected.

use std::time::Duration;

use distvirt_orchestrator::adapter::observability::{
    EndpointEventKind, ObservabilityEvent, PodEventKind, WorkloadEventKind,
};
use distvirt_orchestrator::sm::{PodStatus, WlStatus, endpoint::EndpointStatus};
use distvirt_worker_protocol::NamespaceId;

use crate::harness::TestCluster;
use crate::harness::spec_builders::{activation_spec, always_on_spec};

/// Subscribe to the event bus and return all historical events for a namespace.
fn drain_events(cluster: &TestCluster, ns_id: &str) -> Vec<ObservabilityEvent> {
    let (historical, _rx) = cluster.event_bus.subscribe(&NamespaceId::from(ns_id));
    historical
}

/// Always-on workload with a real worker: events should capture the full
/// Dormant → Launching → Running lifecycle.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_always_on_events_e2e() {
    let mut cluster = TestCluster::new();
    let _w1 = cluster.add_worker().await;

    cluster.create_namespace("ns", always_on_spec()).await;
    cluster.converge().await;
    cluster.assert_workload_running("ns", "echo").await;

    let events = drain_events(&cluster, "ns");

    // Workload events: intermediate transitions (Dormant→WaitingForSpec→Launching)
    // may coalesce within a single propagation round. The only guaranteed
    // observable event is the final →Running transition.
    let wl_events: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            ObservabilityEvent::Workload(we) => Some(&we.event),
            _ => None,
        })
        .collect();
    assert!(
        wl_events.iter().any(|e| matches!(
            e,
            WorkloadEventKind::StatusChanged {
                new: WlStatus::Running,
                ..
            }
        )),
        "expected workload Running event, got: {:?}",
        wl_events
    );

    // Pod events: Created + StatusChanged → Running.
    let pod_events: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            ObservabilityEvent::Pod(pe) => Some(&pe.event),
            _ => None,
        })
        .collect();
    assert!(
        pod_events
            .iter()
            .any(|e| matches!(e, PodEventKind::Created)),
        "expected Pod Created event"
    );
    assert!(
        pod_events.iter().any(|e| matches!(
            e,
            PodEventKind::StatusChanged {
                new: PodStatus::Running,
                ..
            }
        )),
        "expected Pod Running event"
    );

    // IdRegistry should resolve "echo" workload.
    let registry = cluster
        .id_registry_map
        .get(&NamespaceId::from("ns"))
        .expect("registry should exist for namespace");

    // Find the workload ID from any workload event.
    let wl_id = events
        .iter()
        .find_map(|e| match e {
            ObservabilityEvent::Workload(we) => Some(we.workload_id),
            _ => None,
        })
        .expect("should have at least one workload event");
    assert_eq!(registry.workload_name(&wl_id), Some("echo".to_string()));
}

/// Activation lifecycle with a real worker: traffic activates, deactivation +
/// idle timeout suspends. Verify events cover the full cycle.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_activation_events_e2e() {
    let mut cluster = TestCluster::new();
    let w1 = cluster.add_worker().await;

    cluster
        .create_namespace("ns", activation_spec(Duration::from_secs(30)))
        .await;
    cluster.converge().await;

    // Subscribe before activation to get live events.
    let (_historical, mut rx) = cluster.event_bus.subscribe(&NamespaceId::from("ns"));

    // Activate via traffic.
    cluster.send_activation_traffic("ns", "web-svc").await;
    cluster.assert_workload_running("ns", "web").await;

    // Deactivate + idle timeout → suspend.
    cluster.deactivate_service("ns", "web-svc", &w1).await;
    cluster.advance_past_idle_timeout("ns", "web-svc").await;
    cluster.wait_workload_suspended("ns", "web").await;

    // Collect all live events.
    let mut live_events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        live_events.push(event);
    }

    // Should see the full activation cycle.
    let has_ep_active = live_events.iter().any(|e| matches!(
        e,
        ObservabilityEvent::Endpoint(ee)
            if matches!(&ee.event, EndpointEventKind::StatusChanged { new: EndpointStatus::Active, .. })
    ));
    assert!(has_ep_active, "expected endpoint Active event during activation");

    let has_wl_running = live_events.iter().any(|e| matches!(
        e,
        ObservabilityEvent::Workload(we)
            if matches!(&we.event, WorkloadEventKind::StatusChanged { new: WlStatus::Running, .. })
    ));
    assert!(has_wl_running, "expected workload Running event");

    // Should see suspend cycle.
    let has_wl_suspended = live_events.iter().any(|e| matches!(
        e,
        ObservabilityEvent::Workload(we)
            if matches!(&we.event, WorkloadEventKind::StatusChanged { new: WlStatus::Suspended, .. })
    ));
    assert!(has_wl_suspended, "expected workload Suspended event");

    let has_ep_idle = live_events.iter().any(|e| matches!(
        e,
        ObservabilityEvent::Endpoint(ee)
            if matches!(&ee.event, EndpointEventKind::StatusChanged { new: EndpointStatus::Idle, .. })
    ));
    assert!(has_ep_idle, "expected endpoint Idle event after deactivation");
}
