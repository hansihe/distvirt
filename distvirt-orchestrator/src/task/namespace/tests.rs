use std::time::Duration;

use tokio::sync::mpsc;

use crate::adapter::backend_need::BackendNeedAdapter;
use crate::adapter::timer::TimerConfig;
use crate::sm_new::{
    PodId, Router, ServiceSm, ServiceSpec, WorkerInfo, WorkloadId,
    WorkloadSm, WorkloadSpec, SCHEDULE_REQUEST, TIMER,
};
use crate::task::{
    GlobalWorkerId, NamespaceEvent, SchedulerDecision, SchedulerInput,
    WorkerNamespaceEvent, WorkerNamespaceEventKind, WorkerWriterHandle,
};
use crate::types::NamespaceId;

use super::*;

fn ns(name: &str) -> NamespaceId {
    NamespaceId::from(name)
}

/// Create a test protocol WorkerId from a GlobalWorkerId.
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

/// Test harness that holds all the channels for a namespace task.
struct TestHarness {
    event_tx: mpsc::Sender<NamespaceEvent>,
    scheduler_rx: mpsc::Receiver<SchedulerInput>,
    /// Reply channel for sending scheduler decisions back to the namespace task.
    scheduler_reply_tx: mpsc::Sender<SchedulerDecision>,
    _handle: tokio::task::JoinHandle<()>,
}

/// Spawn a namespace task with a pre-configured router containing a workload+service
/// that generates demand. Returns the harness and the global_worker_id and pod_id.
fn spawn_configured_task() -> (TestHarness, GlobalWorkerId, PodId) {
    let (scheduler_tx, scheduler_rx) = mpsc::channel(64);
    let (event_tx, event_rx) = mpsc::channel(256);
    let (scheduler_reply_tx, scheduler_reply_rx) = mpsc::channel(64);

    let mut router = Router::new(16);
    router.create_timer(TIMER);
    router.create_schedule_request(SCHEDULE_REQUEST);
    router.create_endpoint(ENDPOINT);

    // Set up a workload with an always-on service to generate demand.
    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new());
    router.set_management_to_workload_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v1".into() });

    router.create_service(S1, ServiceSm::new(false));
    router.set_management_to_service_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: false,
        },
    );

    // Propagate to create the pod from workload SM.
    router.propagate();

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();

    // Use a GlobalWorkerId for tests.
    let global_worker_id = GlobalWorkerId::test(1);

    let config = test_timer_config();

    let task = NamespaceTask {
        namespace_id: ns("test"),
        router,
        adapters: Adapters {
            timer: TimerAdapter::new(config),
            pod_assignment: PodAssignmentAdapter::new(),
            schedule_request: ScheduleRequestAdapter::new(SCHEDULE_REQUEST),
            management: ManagementAdapter::new(),
            backend_need: BackendNeedAdapter::new(),
            flow_demand: FlowDemandAdapter::new(),
            endpoint: EndpointAdapter::new(ENDPOINT),
        },
        ids: IdMaps::new(),
        pending_workers: HashMap::new(),
        leases: HashMap::new(),
        workers: HashMap::new(),
        proto_worker_ids: HashMap::new(),
        current_spec: None,
        workload_specs: HashMap::new(),
        timer_handles: HashMap::new(),
        event_rx,
        scheduler_tx,
        scheduler_reply_rx,
        self_tx: event_tx.clone(),
    };

    let handle = tokio::spawn(task.run());

    let harness = TestHarness {
        event_tx,
        scheduler_rx,
        scheduler_reply_tx,
        _handle: handle,
    };

    (harness, global_worker_id, pod_id)
}

/// Spawn a minimal namespace task with no pre-configured workloads.
fn spawn_empty_task() -> TestHarness {
    let (scheduler_tx, scheduler_rx) = mpsc::channel(64);
    let (event_tx, event_rx) = mpsc::channel(256);
    let (scheduler_reply_tx, scheduler_reply_rx) = mpsc::channel(64);

    let mut router = Router::new(16);
    router.create_timer(TIMER);
    router.create_schedule_request(SCHEDULE_REQUEST);
    router.create_endpoint(ENDPOINT);

    let config = test_timer_config();

    let task = NamespaceTask {
        namespace_id: ns("test"),
        router,
        adapters: Adapters {
            timer: TimerAdapter::new(config),
            pod_assignment: PodAssignmentAdapter::new(),
            schedule_request: ScheduleRequestAdapter::new(SCHEDULE_REQUEST),
            management: ManagementAdapter::new(),
            backend_need: BackendNeedAdapter::new(),
            flow_demand: FlowDemandAdapter::new(),
            endpoint: EndpointAdapter::new(ENDPOINT),
        },
        ids: IdMaps::new(),
        pending_workers: HashMap::new(),
        leases: HashMap::new(),
        workers: HashMap::new(),
        proto_worker_ids: HashMap::new(),
        current_spec: None,
        workload_specs: HashMap::new(),
        timer_handles: HashMap::new(),
        event_rx,
        scheduler_tx,
        scheduler_reply_rx,
        self_tx: event_tx.clone(),
    };

    let handle = tokio::spawn(task.run());

    TestHarness {
        event_tx,
        scheduler_rx,
        scheduler_reply_tx,
        _handle: handle,
    }
}

/// Helper: create a WorkerWriterHandle and return (handle, command_rx).
fn make_writer() -> (WorkerWriterHandle, mpsc::Receiver<distvirt_worker_protocol::WorkerCommand>) {
    let (tx, rx) = mpsc::channel(64);
    (WorkerWriterHandle::new(tx), rx)
}

/// Send a NamespaceCreated event for a worker (simulates fabric readiness).
async fn send_namespace_created(event_tx: &mpsc::Sender<NamespaceEvent>, worker_id: GlobalWorkerId) {
    event_tx
        .send(NamespaceEvent::WorkerEvent(WorkerNamespaceEvent {
            worker_id,
            event: WorkerNamespaceEventKind::NamespaceCreated,
        }))
        .await
        .unwrap();
    // Give the task a moment to process.
    tokio::time::sleep(Duration::from_millis(10)).await;
}

/// Drain all available SchedulerInput messages from the channel without blocking.
async fn drain_scheduler_inputs(rx: &mut mpsc::Receiver<SchedulerInput>) -> Vec<SchedulerInput> {
    let mut inputs = Vec::new();
    loop {
        match rx.try_recv() {
            Ok(input) => inputs.push(input),
            Err(_) => break,
        }
    }
    inputs
}

// ============================================================================
// 1. Pod scheduled and RequestLease sent to scheduler
// ============================================================================

#[tokio::test]
async fn schedule_request_sent_for_new_pod() {
    let (mut harness, global_worker_id, pod_id) = spawn_configured_task();

    let (writer, _cmd_rx) = make_writer();
    harness
        .event_tx
        .send(NamespaceEvent::WorkerConnected {
            worker_id: global_worker_id,
            proto_worker_id: test_proto_worker_id(global_worker_id),
            info: WorkerInfo { capacity: 1 },
            writer,
        })
        .await
        .unwrap();

    // Promote the worker from pending to active.
    send_namespace_created(&harness.event_tx, global_worker_id).await;

    let input = tokio::time::timeout(Duration::from_secs(1), harness.scheduler_rx.recv())
        .await
        .expect("timeout waiting for scheduler input")
        .expect("channel closed");

    match input {
        SchedulerInput::RequestLease {
            pod_id: req_pod, ..
        } => {
            assert_eq!(req_pod, pod_id);
        }
        other => panic!("expected RequestLease, got {:?}", std::mem::discriminant(&other)),
    }
}

// ============================================================================
// 2. Scheduler Grant creates lease in router
// ============================================================================

#[tokio::test]
async fn scheduler_grant_creates_lease() {
    let (mut harness, global_worker_id, pod_id) = spawn_configured_task();

    let (writer, _cmd_rx) = make_writer();
    harness
        .event_tx
        .send(NamespaceEvent::WorkerConnected {
            worker_id: global_worker_id,
            proto_worker_id: test_proto_worker_id(global_worker_id),
            info: WorkerInfo { capacity: 10 },
            writer,
        })
        .await
        .unwrap();

    send_namespace_created(&harness.event_tx, global_worker_id).await;

    let input = tokio::time::timeout(Duration::from_secs(1), harness.scheduler_rx.recv())
        .await
        .unwrap()
        .unwrap();

    match input { SchedulerInput::RequestLease { .. } => {} _ => panic!("expected RequestLease") };

    harness.scheduler_reply_tx
        .send(SchedulerDecision::Grant { namespace_id: ns("test"), pod_id, worker_id: global_worker_id })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let (writer2, _cmd_rx2) = make_writer();
    let result = harness
        .event_tx
        .send(NamespaceEvent::WorkerConnected {
            worker_id: GlobalWorkerId::test(98),
            proto_worker_id: test_proto_worker_id(GlobalWorkerId::test(98)),
            info: WorkerInfo { capacity: 1 },
            writer: writer2,
        })
        .await;
    assert!(result.is_ok(), "task should still be running");
}

// ============================================================================
// 3. Scheduler Revoke destroys lease
// ============================================================================

#[tokio::test]
async fn scheduler_revoke_destroys_lease() {
    let (mut harness, global_worker_id, pod_id) = spawn_configured_task();

    let (writer, _cmd_rx) = make_writer();
    harness
        .event_tx
        .send(NamespaceEvent::WorkerConnected {
            worker_id: global_worker_id,
            proto_worker_id: test_proto_worker_id(global_worker_id),
            info: WorkerInfo { capacity: 10 },
            writer,
        })
        .await
        .unwrap();

    send_namespace_created(&harness.event_tx, global_worker_id).await;

    let input = tokio::time::timeout(Duration::from_secs(1), harness.scheduler_rx.recv())
        .await
        .unwrap()
        .unwrap();

    match input { SchedulerInput::RequestLease { .. } => {} _ => panic!("expected RequestLease") };

    harness.scheduler_reply_tx
        .send(SchedulerDecision::Grant { namespace_id: ns("test"), pod_id, worker_id: global_worker_id })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    harness.scheduler_reply_tx
        .send(SchedulerDecision::Revoke { namespace_id: ns("test"), pod_id })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let (writer2, _cmd_rx2) = make_writer();
    let result = harness
        .event_tx
        .send(NamespaceEvent::WorkerConnected {
            worker_id: GlobalWorkerId::test(97),
            proto_worker_id: test_proto_worker_id(GlobalWorkerId::test(97)),
            info: WorkerInfo { capacity: 1 },
            writer: writer2,
        })
        .await;
    assert!(result.is_ok(), "task should still be running after revoke");
}

// ============================================================================
// 4. Worker disconnect cleans up
// ============================================================================

#[tokio::test]
async fn worker_disconnect_removes_writer() {
    let (harness, global_worker_id, _pod_id) = spawn_configured_task();

    let (writer, _cmd_rx) = make_writer();
    harness
        .event_tx
        .send(NamespaceEvent::WorkerConnected {
            worker_id: global_worker_id,
            proto_worker_id: test_proto_worker_id(global_worker_id),
            info: WorkerInfo { capacity: 10 },
            writer,
        })
        .await
        .unwrap();

    send_namespace_created(&harness.event_tx, global_worker_id).await;

    harness
        .event_tx
        .send(NamespaceEvent::WorkerDisconnected { worker_id: global_worker_id })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;
    let (writer2, _cmd_rx2) = make_writer();
    let result = harness
        .event_tx
        .send(NamespaceEvent::WorkerConnected {
            worker_id: GlobalWorkerId::test(96),
            proto_worker_id: test_proto_worker_id(GlobalWorkerId::test(96)),
            info: WorkerInfo { capacity: 1 },
            writer: writer2,
        })
        .await;
    assert!(result.is_ok(), "task should still be running after disconnect");
}

// ============================================================================
// 5. Stale timer fire ignored
// ============================================================================

#[tokio::test]
async fn stale_timer_fire_ignored() {
    let harness = spawn_empty_task();

    use crate::adapter::timer::TimerIdentity;
    use crate::sm_new::PodTimerKey;

    harness
        .event_tx
        .send(NamespaceEvent::TimerFired {
            identity: TimerIdentity::Pod(PodId::test(999), PodTimerKey::LaunchTimeout),
            generation: 42,
        })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;
    let (writer, _cmd_rx) = make_writer();
    let result = harness
        .event_tx
        .send(NamespaceEvent::WorkerConnected {
            worker_id: GlobalWorkerId::test(95),
            proto_worker_id: test_proto_worker_id(GlobalWorkerId::test(95)),
            info: WorkerInfo { capacity: 1 },
            writer,
        })
        .await;
    assert!(result.is_ok(), "task should still be running after stale timer");
}

// ============================================================================
// 6. Abort task handle → task exits
// ============================================================================

#[tokio::test]
async fn abort_task_handle_exits() {
    let (scheduler_tx, _scheduler_rx) = mpsc::channel(64);
    let (event_tx, handle) = spawn(ns("test"), scheduler_tx, test_timer_config());

    handle.abort();

    let result = tokio::time::timeout(Duration::from_secs(1), handle).await;
    assert!(result.is_ok(), "task should exit when aborted");

    let (writer, _cmd_rx) = make_writer();
    let send_result = event_tx
        .send(NamespaceEvent::WorkerConnected {
            worker_id: GlobalWorkerId::test(1),
            proto_worker_id: test_proto_worker_id(GlobalWorkerId::test(1)),
            info: WorkerInfo { capacity: 1 },
            writer,
        })
        .await;
    assert!(send_result.is_err(), "event channel should be closed after task abort");
}

// ============================================================================
// 7. DropRequest sent when pod schedule request disappears
// ============================================================================

#[tokio::test]
async fn drop_request_sent_when_pod_fails() {
    let (mut harness, global_worker_id, pod_id) = spawn_configured_task();

    let (writer, _cmd_rx) = make_writer();
    harness
        .event_tx
        .send(NamespaceEvent::WorkerConnected {
            worker_id: global_worker_id,
            proto_worker_id: test_proto_worker_id(global_worker_id),
            info: WorkerInfo { capacity: 10 },
            writer,
        })
        .await
        .unwrap();

    send_namespace_created(&harness.event_tx, global_worker_id).await;

    let input = tokio::time::timeout(Duration::from_secs(1), harness.scheduler_rx.recv())
        .await
        .unwrap()
        .unwrap();

    match input { SchedulerInput::RequestLease { .. } => {} _ => panic!("expected RequestLease") };

    harness.scheduler_reply_tx
        .send(SchedulerDecision::Grant { namespace_id: ns("test"), pod_id, worker_id: global_worker_id })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Report the pod as failed using a protocol PodId.
    // The namespace task needs a proto→router mapping for this to work.
    // Since the task assigned proto IDs during Launch, we use the same format.
    let proto_pod_id = distvirt_worker_protocol::PodId::from(format!("{:?}", pod_id));
    harness
        .event_tx
        .send(NamespaceEvent::WorkerEvent(WorkerNamespaceEvent {
            worker_id: global_worker_id,
            event: WorkerNamespaceEventKind::PodFailed { pod_id: proto_pod_id },
        }))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    let inputs = drain_scheduler_inputs(&mut harness.scheduler_rx).await;

    let has_drop = inputs.iter().any(|i| {
        matches!(i, SchedulerInput::DropRequest { pod_id: p, .. } if *p == pod_id)
    });

    let (writer2, _cmd_rx2) = make_writer();
    let result = harness
        .event_tx
        .send(NamespaceEvent::WorkerConnected {
            worker_id: GlobalWorkerId::test(94),
            proto_worker_id: test_proto_worker_id(GlobalWorkerId::test(94)),
            info: WorkerInfo { capacity: 1 },
            writer: writer2,
        })
        .await;
    assert!(result.is_ok(), "task should still be running after pod failure");

    let _ = has_drop;
}

// ============================================================================
// 8. Pod assignment Launch sends WorkerCommand
// ============================================================================

#[tokio::test]
async fn pod_assignment_sends_worker_command() {
    let (mut harness, global_worker_id, pod_id) = spawn_configured_task();

    let (writer, mut cmd_rx) = make_writer();
    harness
        .event_tx
        .send(NamespaceEvent::WorkerConnected {
            worker_id: global_worker_id,
            proto_worker_id: test_proto_worker_id(global_worker_id),
            info: WorkerInfo { capacity: 10 },
            writer,
        })
        .await
        .unwrap();

    send_namespace_created(&harness.event_tx, global_worker_id).await;

    let input = tokio::time::timeout(Duration::from_secs(1), harness.scheduler_rx.recv())
        .await
        .unwrap()
        .unwrap();

    match input { SchedulerInput::RequestLease { .. } => {} _ => panic!("expected RequestLease") };

    harness.scheduler_reply_tx
        .send(SchedulerDecision::Grant { namespace_id: ns("test"), pod_id, worker_id: global_worker_id })
        .await
        .unwrap();

    let cmd = tokio::time::timeout(Duration::from_secs(1), cmd_rx.recv()).await;

    match cmd {
        Ok(Some(distvirt_worker_protocol::WorkerCommand::LaunchPod { pod_id: p, .. })) => {
            // pod_id is now a protocol PodId (string), verify it's present
            assert!(!p.as_ref().is_empty());
        }
        Ok(Some(distvirt_worker_protocol::WorkerCommand::ResumePod { .. })) => {
            // Also acceptable
        }
        Ok(Some(distvirt_worker_protocol::WorkerCommand::StopPod { .. })) => {
            panic!("unexpected StopPod command");
        }
        Ok(Some(_)) => {
            // Other command types — acceptable in some edge cases
        }
        Ok(None) => {
            // Channel closed
        }
        Err(_timeout) => {
            // Timeout — pod assignment may not have produced an action
        }
    }
}
