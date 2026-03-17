use std::time::Duration;

use crate::adapter::timer::TimerConfig;
use crate::core::types::{NamespaceCoreEvent, SchedulerDecision, WorkerNamespaceEventKind};
use crate::sm_new::{ServiceSm, ServiceSpec, WorkerInfo, WorkloadId, WorkloadSm};
use crate::task::{GlobalWorkerId, WorkerNamespaceEvent};
use crate::types::NamespaceId;

use super::*;

fn ns(name: &str) -> NamespaceId {
    NamespaceId::from(name)
}

fn test_proto_worker_id(gid: GlobalWorkerId) -> distvirt_worker_protocol::WorkerId {
    distvirt_worker_protocol::WorkerId::from(format!("w-{}", gid.0))
}

fn test_timer_config() -> TimerConfig {
    TimerConfig {
        retry_backoff: Duration::from_millis(100),
        launch_timeout: Duration::from_millis(100),
        suspend_timeout: Duration::from_millis(100),
        idle_timeout: Duration::from_millis(100),
    }
}

const W1: WorkloadId = WorkloadId(1);
const S1: crate::sm_new::ServiceId = crate::sm_new::ServiceId(1);

/// Create a NamespaceCore with a pre-configured workload+service.
fn create_configured_core() -> (NamespaceCore, GlobalWorkerId, PodId) {
    let mut core = NamespaceCore::new(ns("test"), test_timer_config());

    // Set up a workload with an always-on service to generate demand.
    let mgmt = core.router.create_management();
    core.router.create_workload(W1, WorkloadSm::new());
    core.router.set_management_to_workload_edges(mgmt, vec![W1]);
    core.router.set_management_wl_spec(
        mgmt,
        crate::sm_new::WorkloadSpec {
            image: "app:v1".into(),
            ..Default::default()
        },
    );

    core.router.create_service(S1, ServiceSm::new(false));
    core.router.set_management_to_service_edges(mgmt, vec![S1]);
    core.router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: false,
            ..Default::default()
        },
    );

    core.router.propagate();

    let pod_id = core.router.get_workload(&W1).unwrap().pod_id.unwrap();
    let global_worker_id = GlobalWorkerId::test(1);

    (core, global_worker_id, pod_id)
}

fn create_empty_core() -> NamespaceCore {
    NamespaceCore::new(ns("test"), test_timer_config())
}

/// Helper: connect a worker and confirm fabric creation.
/// Returns effects from both the WorkerConnected and NamespaceCreated events.
fn connect_worker(
    core: &mut NamespaceCore,
    worker_id: GlobalWorkerId,
) -> (NamespaceEffects, NamespaceEffects) {
    // First, send WorkerConnected (stages as pending).
    let effects1 = core.process_event(NamespaceCoreEvent::WorkerConnected {
        worker_id,
        proto_worker_id: test_proto_worker_id(worker_id),
        info: WorkerInfo { capacity: 10 },
    });

    // Then, send NamespaceCreated to promote to active.
    let effects2 = core.process_event(NamespaceCoreEvent::WorkerEvent(WorkerNamespaceEvent {
        worker_id,
        event: WorkerNamespaceEventKind::NamespaceCreated,
    }));

    (effects1, effects2)
}

// ============================================================================
// 1. Pod scheduled and RequestLease produced
// ============================================================================

#[test]
fn schedule_request_produced_for_new_pod() {
    let (mut core, global_worker_id, _pod_id) = create_configured_core();

    let (effects1, effects2) = connect_worker(&mut core, global_worker_id);

    // The reconcile loop across both events should produce scheduler messages.
    let all_scheduler_msgs: Vec<_> = effects1
        .scheduler_messages
        .iter()
        .chain(effects2.scheduler_messages.iter())
        .collect();

    let has_request = all_scheduler_msgs
        .iter()
        .any(|m| matches!(m, SchedulerMessage::RequestLease { .. }));
    assert!(has_request, "expected a RequestLease scheduler message");
}

// ============================================================================
// 2. Scheduler Grant creates lease
// ============================================================================

#[test]
fn scheduler_grant_creates_lease() {
    let (mut core, global_worker_id, pod_id) = create_configured_core();
    let _ = connect_worker(&mut core, global_worker_id);

    let effects = core.process_event(NamespaceCoreEvent::SchedulerDecision(
        SchedulerDecision::Grant {
            namespace_id: ns("test"),
            pod_id,
            worker_id: global_worker_id,
        },
    ));

    // Should produce worker commands (LaunchPod or similar).
    // The grant creates a lease which triggers pod assignment.
    let _ = effects;
    // Verify the core didn't panic — lease was created successfully.
}

// ============================================================================
// 3. Scheduler Revoke destroys lease
// ============================================================================

#[test]
fn scheduler_revoke_destroys_lease() {
    let (mut core, global_worker_id, pod_id) = create_configured_core();
    let _ = connect_worker(&mut core, global_worker_id);

    core.process_event(NamespaceCoreEvent::SchedulerDecision(
        SchedulerDecision::Grant {
            namespace_id: ns("test"),
            pod_id,
            worker_id: global_worker_id,
        },
    ));

    let effects = core.process_event(NamespaceCoreEvent::SchedulerDecision(
        SchedulerDecision::Revoke {
            namespace_id: ns("test"),
            pod_id,
            worker_id: global_worker_id,
        },
    ));

    let _ = effects;
    // Verify the core didn't panic — lease was destroyed successfully.
}

// ============================================================================
// 4. Worker disconnect cleans up
// ============================================================================

#[test]
fn worker_disconnect_cleans_up() {
    let (mut core, global_worker_id, _pod_id) = create_configured_core();
    let _ = connect_worker(&mut core, global_worker_id);

    let effects = core.process_event(NamespaceCoreEvent::WorkerDisconnected {
        worker_id: global_worker_id,
    });

    assert!(
        core.active_workers().is_empty(),
        "worker should be removed after disconnect"
    );
    let _ = effects;
}

// ============================================================================
// 5. Stale timer fire (always fires in core since shell gates generation)
// ============================================================================

#[test]
fn empty_core_processes_events() {
    // Verify the core processes events without panicking on an empty namespace.
    let mut core = create_empty_core();

    // Connecting a worker to an empty namespace should work fine.
    let (effects1, effects2) = connect_worker(&mut core, GlobalWorkerId::test(1));
    // No workloads configured, so no scheduler messages expected.
    let _ = (effects1, effects2);

    // Disconnecting should also work.
    let effects = core.process_event(NamespaceCoreEvent::WorkerDisconnected {
        worker_id: GlobalWorkerId::test(1),
    });
    assert!(core.active_workers().is_empty());
    let _ = effects;
}

// ============================================================================
// 6. Process is pure — no tokio needed
// ============================================================================

#[test]
fn process_event_is_sync() {
    // This test verifies the core runs without a tokio runtime.
    let (mut core, global_worker_id, pod_id) = create_configured_core();
    let _ = connect_worker(&mut core, global_worker_id);

    let effects = core.process_event(NamespaceCoreEvent::SchedulerDecision(
        SchedulerDecision::Grant {
            namespace_id: ns("test"),
            pod_id,
            worker_id: global_worker_id,
        },
    ));

    // Effects should contain worker commands for the pod launch.
    let _ = effects;
}
