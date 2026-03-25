//! Tests that observability events flow end-to-end through the event bus
//! and that the IdRegistry resolves router IDs to protocol-level names.

use std::time::Duration;

use distvirt_orchestrator::adapter::observability::{
    EndpointEventKind, ObservabilityEvent, PodEventKind, WorkloadEventKind,
};
use distvirt_orchestrator::sm::{PodStatus, WlStatus, endpoint::EndpointStatus};
use distvirt_worker_protocol::{MemoryConstraintReason, NamespaceId};

use distvirt_orchestrator::shell::sync::MockWorkerConfig;

use crate::harness::spec_builders::{activation_spec, always_on_spec};
use crate::harness::TestHarness;

/// Drain all events currently in the event bus for a namespace.
fn drain_events(h: &TestHarness, ns_id: &str) -> Vec<ObservabilityEvent> {
    let (historical, _rx) = h.shell.event_bus().subscribe(&NamespaceId::from(ns_id));
    historical
}

/// Check that the IdRegistry resolves a workload name for the given namespace.
fn assert_registry_has_workload(h: &TestHarness, ns_id: &str, wl_name: &str) {
    let registry = h
        .shell
        .id_registry_map()
        .get(&NamespaceId::from(ns_id))
        .expect("namespace should have a registry");
    let ns = h.namespace(ns_id);
    let wl_id = ns.management().lookup_workload(wl_name).unwrap();
    assert_eq!(
        registry.workload_name(&wl_id),
        Some(wl_name.to_string()),
        "registry should resolve workload ID to '{}'",
        wl_name
    );
}

// =============================================================================
// Test: always-on workload emits lifecycle events with resolvable names
// =============================================================================

#[test]
fn test_always_on_lifecycle_events() {
    let mut h = TestHarness::new();
    let _w1 = h.add_worker();

    h.create_namespace("ns", always_on_spec());

    // The workload "echo" should be running now. Check events were emitted.
    let events = drain_events(&h, "ns");

    // Should have workload status events (Dormant → Launching → Running).
    let wl_events: Vec<_> = events
        .iter()
        .filter(|e| matches!(e, ObservabilityEvent::Workload(_)))
        .collect();
    assert!(
        !wl_events.is_empty(),
        "expected workload observability events, got none"
    );

    // Verify transitions include a Running status event.
    // Note: intermediate states (WaitingForSpec, Launching) may be collapsed
    // within a single propagation round for always-on workloads.
    let has_running = wl_events.iter().any(|e| match e {
        ObservabilityEvent::Workload(we) => matches!(
            &we.event,
            WorkloadEventKind::StatusChanged {
                new: WlStatus::Running,
                ..
            }
        ),
        _ => false,
    });
    assert!(has_running, "expected Running transition event");

    // Should have pod events (Created, StatusChanged, WorkerChanged).
    let pod_events: Vec<_> = events
        .iter()
        .filter(|e| matches!(e, ObservabilityEvent::Pod(_)))
        .collect();
    assert!(
        !pod_events.is_empty(),
        "expected pod observability events, got none"
    );

    let has_pod_created = pod_events.iter().any(|e| match e {
        ObservabilityEvent::Pod(pe) => matches!(&pe.event, PodEventKind::Created),
        _ => false,
    });
    assert!(has_pod_created, "expected Pod::Created event");

    let has_pod_running = pod_events.iter().any(|e| match e {
        ObservabilityEvent::Pod(pe) => matches!(
            &pe.event,
            PodEventKind::StatusChanged {
                new: PodStatus::Running,
                ..
            }
        ),
        _ => false,
    });
    assert!(has_pod_running, "expected Pod StatusChanged → Running event");

    // Verify the IdRegistry has resolved names.
    assert_registry_has_workload(&h, "ns", "echo");
}

// =============================================================================
// Test: activation cycle emits service + workload + pod events
// =============================================================================

#[test]
fn test_activation_cycle_events() {
    let mut h = TestHarness::new();
    let _w1 = h.add_worker_with(MockWorkerConfig::with_pool());

    h.create_namespace("ns", activation_spec(Duration::from_secs(30)));

    // Workload "web" should be dormant — no demand yet.
    h.assert_workload_dormant("ns", "web");

    // Subscribe to events *before* activation so we capture the full cycle.
    let (pre_events, mut rx) = h.shell.event_bus().subscribe(&NamespaceId::from("ns"));
    // Pre-events should be empty or minimal (no lifecycle yet for dormant workload).
    drop(pre_events);

    // Activate the service — triggers workload launch.
    h.activate_service("ns", "web-svc");

    // Collect live events that were pushed during activation.
    let mut live_events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        live_events.push(event);
    }

    // Should see workload Dormant→Launching, endpoint Idle→NeedBackend or Active.
    let has_wl_launch = live_events.iter().any(|e| match e {
        ObservabilityEvent::Workload(we) => matches!(
            &we.event,
            WorkloadEventKind::StatusChanged {
                old: WlStatus::Dormant,
                new: WlStatus::Launching,
            }
        ),
        _ => false,
    });
    assert!(
        has_wl_launch,
        "expected workload Dormant→Launching during activation"
    );

    let has_wl_running = live_events.iter().any(|e| match e {
        ObservabilityEvent::Workload(we) => matches!(
            &we.event,
            WorkloadEventKind::StatusChanged {
                new: WlStatus::Running,
                ..
            }
        ),
        _ => false,
    });
    assert!(
        has_wl_running,
        "expected workload →Running during activation"
    );

    // Endpoint should have become active.
    let has_ep_active = live_events.iter().any(|e| match e {
        ObservabilityEvent::Endpoint(ee) => matches!(
            &ee.event,
            EndpointEventKind::StatusChanged {
                new: EndpointStatus::Active,
                ..
            }
        ),
        _ => false,
    });
    assert!(has_ep_active, "expected endpoint →Active during activation");

    // Now deactivate and let idle timeout fire → should get suspension events.
    h.deactivate_service("ns", "web-svc");
    h.advance_past_idle_timeout("ns", "web-svc");

    // Drain remaining live events.
    while let Ok(event) = rx.try_recv() {
        live_events.push(event);
    }

    // Should see workload → Suspending and → Suspended.
    let has_suspending = live_events.iter().any(|e| match e {
        ObservabilityEvent::Workload(we) => matches!(
            &we.event,
            WorkloadEventKind::StatusChanged {
                new: WlStatus::Suspending,
                ..
            }
        ),
        _ => false,
    });
    assert!(has_suspending, "expected workload →Suspending after idle timeout");

    let has_suspended = live_events.iter().any(|e| match e {
        ObservabilityEvent::Workload(we) => matches!(
            &we.event,
            WorkloadEventKind::StatusChanged {
                new: WlStatus::Suspended,
                ..
            }
        ),
        _ => false,
    });
    assert!(has_suspended, "expected workload →Suspended after suspend completes");

    // Endpoint should have deactivated.
    let has_ep_idle = live_events.iter().any(|e| match e {
        ObservabilityEvent::Endpoint(ee) => matches!(
            &ee.event,
            EndpointEventKind::StatusChanged {
                new: EndpointStatus::Idle,
                ..
            }
        ),
        _ => false,
    });
    assert!(
        has_ep_idle,
        "expected endpoint →Idle after deactivation"
    );

    // Verify registry resolves workload and service names.
    assert_registry_has_workload(&h, "ns", "web");
    let registry = h
        .shell
        .id_registry_map()
        .get(&NamespaceId::from("ns"))
        .unwrap();
    let ns = h.namespace("ns");
    let svc_id = ns.management().lookup_service("web-svc").unwrap();
    assert_eq!(registry.service_name(&svc_id), Some("web-svc".to_string()));
}

// =============================================================================
// Test: memory constraint and OOM kill events flow through event bus
// =============================================================================

#[test]
fn test_memory_constraint_events() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker();

    h.create_namespace("ns", always_on_spec());
    h.assert_workload_running("ns", "echo");

    // Subscribe to events before injecting memory events.
    let (_pre, mut rx) = h.shell.event_bus().subscribe(&NamespaceId::from("ns"));

    // 1. Inject PodMemoryConstrained (balloon exhausted).
    h.inject_pod_memory_constrained(
        &w1,
        "ns",
        "echo",
        MemoryConstraintReason::BalloonExhausted,
    );

    let mut live_events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        live_events.push(event);
    }

    let has_constrained = live_events.iter().any(|e| matches!(
        e,
        ObservabilityEvent::Pod(pe)
            if matches!(
                &pe.event,
                PodEventKind::MemoryConstrained {
                    reason: MemoryConstraintReason::BalloonExhausted,
                }
            )
    ));
    assert!(
        has_constrained,
        "expected MemoryConstrained(BalloonExhausted) event, got: {:?}",
        live_events
    );

    // 2. Inject PodOomKill.
    h.inject_pod_oom_kill(&w1, "ns", "echo", 3);

    while let Ok(event) = rx.try_recv() {
        live_events.push(event);
    }

    let has_oom = live_events.iter().any(|e| matches!(
        e,
        ObservabilityEvent::Pod(pe)
            if matches!(&pe.event, PodEventKind::OomKill { count: 3 })
    ));
    assert!(
        has_oom,
        "expected OomKill {{ count: 3 }} event, got: {:?}",
        live_events
    );

    // 3. Inject PodMemoryConstraintCleared.
    h.inject_pod_memory_constraint_cleared(&w1, "ns", "echo");

    while let Ok(event) = rx.try_recv() {
        live_events.push(event);
    }

    let has_cleared = live_events.iter().any(|e| matches!(
        e,
        ObservabilityEvent::Pod(pe)
            if matches!(&pe.event, PodEventKind::MemoryConstraintCleared)
    ));
    assert!(
        has_cleared,
        "expected MemoryConstraintCleared event, got: {:?}",
        live_events
    );
}

#[test]
fn test_memory_constraint_deflation_stalled() {
    let mut h = TestHarness::new();
    let w1 = h.add_worker();

    h.create_namespace("ns", always_on_spec());
    h.assert_workload_running("ns", "echo");

    let (_pre, mut rx) = h.shell.event_bus().subscribe(&NamespaceId::from("ns"));

    // Inject DeflationStalled variant.
    h.inject_pod_memory_constrained(
        &w1,
        "ns",
        "echo",
        MemoryConstraintReason::DeflationStalled,
    );

    let mut live_events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        live_events.push(event);
    }

    let has_stalled = live_events.iter().any(|e| matches!(
        e,
        ObservabilityEvent::Pod(pe)
            if matches!(
                &pe.event,
                PodEventKind::MemoryConstrained {
                    reason: MemoryConstraintReason::DeflationStalled,
                }
            )
    ));
    assert!(
        has_stalled,
        "expected MemoryConstrained(DeflationStalled) event, got: {:?}",
        live_events
    );
}
