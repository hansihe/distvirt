use tokio::sync::mpsc;

use crate::sm_new::PodId;
use crate::task::{ArtifactPlacementEvent, GlobalWorkerId, SchedulerDecision, SchedulerInput};
use crate::types::PressureBand;

use super::WorkerCandidate;

fn ns(name: &str) -> crate::types::NamespaceId {
    crate::types::NamespaceId::from(name)
}

fn worker_candidate(
    worker_id: GlobalWorkerId,
    pressure: PressureBand,
    pod_count: usize,
) -> WorkerCandidate {
    WorkerCandidate {
        worker_id,
        max_pressure_band: pressure,
        pod_count,
        draining: false,
        active: true,
    }
}

/// Helper: register a namespace and return the reply receiver.
async fn register_namespace(
    tx: &mpsc::Sender<SchedulerInput>,
    namespace: &str,
) -> mpsc::Receiver<SchedulerDecision> {
    let (reply_tx, reply_rx) = mpsc::channel(16);
    tx.send(SchedulerInput::RegisterNamespace {
        namespace_id: ns(namespace),
        reply_tx,
    })
    .await
    .unwrap();
    reply_rx
}

/// Helper: send a RequestLease (namespace must already be registered).
async fn send_request_to(
    tx: &mpsc::Sender<SchedulerInput>,
    namespace: &str,
    pod_id: PodId,
) {
    tx.send(SchedulerInput::RequestLease {
        namespace_id: ns(namespace),
        pod_id,
        proto_resume_artifact: None,
    })
    .await
    .unwrap();
}

/// Helper: register a namespace and send a RequestLease, return the reply receiver.
async fn send_request(
    tx: &mpsc::Sender<SchedulerInput>,
    namespace: &str,
    pod_id: PodId,
) -> mpsc::Receiver<SchedulerDecision> {
    let reply_rx = register_namespace(tx, namespace).await;
    send_request_to(tx, namespace, pod_id).await;
    reply_rx
}

/// Helper: send a WorkerUpdate.
async fn send_worker_update(
    tx: &mpsc::Sender<SchedulerInput>,
    worker_id: GlobalWorkerId,
    pressure: PressureBand,
    pod_count: usize,
) {
    tx.send(SchedulerInput::WorkerUpdate(
        worker_id,
        worker_candidate(worker_id, pressure, pod_count),
    ))
    .await
    .unwrap();
}

/// Spawn the scheduler and return the input sender.
fn spawn_scheduler() -> mpsc::Sender<SchedulerInput> {
    let (tx, rx) = mpsc::channel(64);
    super::spawn(rx);
    tx
}

// ============================================================================
// 1. Request with available worker → immediate Grant
// ============================================================================

#[tokio::test]
async fn request_with_worker_grants_immediately() {
    let tx = spawn_scheduler();
    let w1 = GlobalWorkerId::test(1);
    let p1 = PodId::test(10);

    send_worker_update(&tx, w1, PressureBand::Normal, 0).await;

    let mut reply_rx = send_request(&tx, "ns1", p1).await;

    let decision = reply_rx.recv().await.unwrap();
    match decision {
        SchedulerDecision::Grant { pod_id, worker_id, .. } => {
            assert_eq!(pod_id, p1);
            assert_eq!(worker_id, w1);
        }
        other => panic!("expected Grant, got {:?}", other),
    }
}

// ============================================================================
// 2. Request with no workers → pending, then Grant on WorkerUpdate
// ============================================================================

#[tokio::test]
async fn request_pending_then_grant_on_worker_update() {
    let tx = spawn_scheduler();
    let w1 = GlobalWorkerId::test(1);
    let p1 = PodId::test(10);

    let mut reply_rx = send_request(&tx, "ns1", p1).await;

    // No workers yet, so no grant. Add one.
    send_worker_update(&tx, w1, PressureBand::Normal, 0).await;

    let decision = reply_rx.recv().await.unwrap();
    match decision {
        SchedulerDecision::Grant { pod_id, worker_id, .. } => {
            assert_eq!(pod_id, p1);
            assert_eq!(worker_id, w1);
        }
        other => panic!("expected Grant, got {:?}", other),
    }
}

// ============================================================================
// 3. DropRequest for pending pod → silently removed
// ============================================================================

#[tokio::test]
async fn drop_pending_removes_silently() {
    let tx = spawn_scheduler();
    let w1 = GlobalWorkerId::test(1);
    let p1 = PodId::test(10);

    let mut reply_rx = send_request(&tx, "ns1", p1).await;

    // Drop the request before any worker is available.
    tx.send(SchedulerInput::DropRequest {
        namespace_id: ns("ns1"),
        pod_id: p1,
    })
    .await
    .unwrap();

    // Now add a worker — should not grant the dropped pod.
    send_worker_update(&tx, w1, PressureBand::Normal, 0).await;

    // Drop the sender so the scheduler task ends, closing reply channels.
    drop(tx);

    // reply_rx should get None (channel closed) — no Grant was sent.
    assert!(reply_rx.recv().await.is_none());
}

// ============================================================================
// 4. DropRequest for granted pod → Revoke sent
// ============================================================================

#[tokio::test]
async fn drop_granted_sends_revoke() {
    let tx = spawn_scheduler();
    let w1 = GlobalWorkerId::test(1);
    let p1 = PodId::test(10);

    send_worker_update(&tx, w1, PressureBand::Normal, 0).await;

    let mut reply_rx = send_request(&tx, "ns1", p1).await;

    // Should receive Grant first.
    let decision = reply_rx.recv().await.unwrap();
    assert!(matches!(decision, SchedulerDecision::Grant { .. }));

    // Now drop the request.
    tx.send(SchedulerInput::DropRequest {
        namespace_id: ns("ns1"),
        pod_id: p1,
    })
    .await
    .unwrap();

    let decision = reply_rx.recv().await.unwrap();
    match decision {
        SchedulerDecision::Revoke { pod_id, .. } => {
            assert_eq!(pod_id, p1);
        }
        other => panic!("expected Revoke, got {:?}", other),
    }
}

// ============================================================================
// 5. WorkerRemoved → no immediate effect on grants
// ============================================================================

#[tokio::test]
async fn worker_removed_no_revoke() {
    let tx = spawn_scheduler();
    let w1 = GlobalWorkerId::test(1);
    let p1 = PodId::test(10);

    send_worker_update(&tx, w1, PressureBand::Normal, 0).await;

    let mut reply_rx = send_request(&tx, "ns1", p1).await;

    let decision = reply_rx.recv().await.unwrap();
    assert!(matches!(decision, SchedulerDecision::Grant { .. }));

    // Remove the worker.
    tx.send(SchedulerInput::WorkerRemoved(w1)).await.unwrap();

    // Drop sender to end task.
    drop(tx);

    // No Revoke should have been sent.
    assert!(reply_rx.recv().await.is_none());
}

// ============================================================================
// 6. Multiple pending pods → retry schedules all eligible
// ============================================================================

#[tokio::test]
async fn multiple_pending_all_granted() {
    let tx = spawn_scheduler();
    let w1 = GlobalWorkerId::test(1);

    let mut reply1 = register_namespace(&tx, "ns1").await;
    send_request_to(&tx, "ns1", PodId::test(1)).await;
    send_request_to(&tx, "ns1", PodId::test(2)).await;
    let mut reply3 = register_namespace(&tx, "ns2").await;
    send_request_to(&tx, "ns2", PodId::test(1)).await;

    // Add a worker — all three should get granted.
    send_worker_update(&tx, w1, PressureBand::Normal, 0).await;

    let d1 = reply1.recv().await.unwrap();
    let d2 = reply1.recv().await.unwrap();
    let d3 = reply3.recv().await.unwrap();

    assert!(matches!(d1, SchedulerDecision::Grant { .. }));
    assert!(matches!(d2, SchedulerDecision::Grant { .. }));
    assert!(matches!(d3, SchedulerDecision::Grant { .. }));
}

// ============================================================================
// 7. Worker at High pressure → not selected
// ============================================================================

#[tokio::test]
async fn high_pressure_worker_not_selected() {
    let tx = spawn_scheduler();
    let w1 = GlobalWorkerId::test(1);
    let p1 = PodId::test(10);

    send_worker_update(&tx, w1, PressureBand::High, 0).await;

    let mut reply_rx = send_request(&tx, "ns1", p1).await;

    // Drop sender to end task — should not have received a Grant.
    drop(tx);
    assert!(reply_rx.recv().await.is_none());
}

// ============================================================================
// 8. Channel closed (namespace dropped) → send fails gracefully
// ============================================================================

#[tokio::test]
async fn closed_reply_channel_no_panic() {
    let tx = spawn_scheduler();
    let w1 = GlobalWorkerId::test(1);

    let (reply_tx, reply_rx) = mpsc::channel(16);
    tx.send(SchedulerInput::RegisterNamespace {
        namespace_id: ns("ns1"),
        reply_tx,
    })
    .await
    .unwrap();
    tx.send(SchedulerInput::RequestLease {
        namespace_id: ns("ns1"),
        pod_id: PodId::test(10),
        proto_resume_artifact: None,
    })
    .await
    .unwrap();

    // Drop the receiver before the scheduler can send.
    drop(reply_rx);

    // Add a worker — scheduler will try to send Grant but reply is closed.
    // This should not panic.
    send_worker_update(&tx, w1, PressureBand::Normal, 0).await;

    // Send another request to verify the scheduler is still alive.
    let mut reply2 = send_request(&tx, "ns2", PodId::test(20)).await;
    let decision = reply2.recv().await.unwrap();
    assert!(matches!(decision, SchedulerDecision::Grant { .. }));
}

// ============================================================================
// 9. Artifact affinity: resume request prefers worker with artifact
// ============================================================================

#[tokio::test]
async fn artifact_affinity_prefers_worker_with_artifact() {
    let tx = spawn_scheduler();
    let w1 = GlobalWorkerId::test(1);
    let w2 = GlobalWorkerId::test(2);
    let p1 = PodId::test(10);

    // Both workers available, w1 has lower ID (would normally win).
    send_worker_update(&tx, w1, PressureBand::Normal, 0).await;
    send_worker_update(&tx, w2, PressureBand::Normal, 0).await;

    // Artifact "test-ns-art-42" is ready on w2.
    let artifact_id = distvirt_worker_protocol::ArtifactId::from("test-ns-art-42");
    tx.send(SchedulerInput::ArtifactEvent {
        worker_id: w2,
        event: ArtifactPlacementEvent::WriteCommitted {
            artifact_id: artifact_id.clone(),
            pool_id: distvirt_worker_protocol::PoolId::from("pool-1"),
            size_bytes: 1024,
        },
    })
    .await
    .unwrap();

    // Request lease with resume artifact.
    let mut reply_rx = register_namespace(&tx, "ns1").await;
    tx.send(SchedulerInput::RequestLease {
        namespace_id: ns("ns1"),
        pod_id: p1,
        proto_resume_artifact: Some(artifact_id),
    })
    .await
    .unwrap();

    let decision = reply_rx.recv().await.unwrap();
    match decision {
        SchedulerDecision::Grant { worker_id, .. } => {
            assert_eq!(worker_id, w2, "should prefer w2 which has the artifact");
        }
        other => panic!("expected Grant, got {:?}", other),
    }
}

// ============================================================================
// 10. Artifact affinity: falls back when artifact worker is unavailable
// ============================================================================

#[tokio::test]
async fn artifact_affinity_falls_back_when_worker_unavailable() {
    let tx = spawn_scheduler();
    let w1 = GlobalWorkerId::test(1);
    let w2 = GlobalWorkerId::test(2);
    let p1 = PodId::test(10);

    // w1 is available, w2 is draining (has the artifact but can't be used).
    send_worker_update(&tx, w1, PressureBand::Normal, 0).await;
    tx.send(SchedulerInput::WorkerUpdate(
        w2,
        WorkerCandidate {
            worker_id: w2,
            max_pressure_band: PressureBand::Normal,
            pod_count: 0,
            draining: true,
            active: true,
        },
    ))
    .await
    .unwrap();

    // Artifact on w2.
    let artifact_id = distvirt_worker_protocol::ArtifactId::from("test-ns-art-42");
    tx.send(SchedulerInput::ArtifactEvent {
        worker_id: w2,
        event: ArtifactPlacementEvent::WriteCommitted {
            artifact_id: artifact_id.clone(),
            pool_id: distvirt_worker_protocol::PoolId::from("pool-1"),
            size_bytes: 1024,
        },
    })
    .await
    .unwrap();

    // Request with resume artifact — w2 has it but is draining, should fall back to w1.
    let mut reply_rx = register_namespace(&tx, "ns1").await;
    tx.send(SchedulerInput::RequestLease {
        namespace_id: ns("ns1"),
        pod_id: p1,
        proto_resume_artifact: Some(artifact_id),
    })
    .await
    .unwrap();

    let decision = reply_rx.recv().await.unwrap();
    match decision {
        SchedulerDecision::Grant { worker_id, .. } => {
            assert_eq!(worker_id, w1, "should fall back to w1 since w2 is draining");
        }
        other => panic!("expected Grant, got {:?}", other),
    }
}

// ============================================================================
// 11. WorkerRemoved purges placement entries
// ============================================================================

#[tokio::test]
async fn worker_removed_purges_placements() {
    let tx = spawn_scheduler();
    let w1 = GlobalWorkerId::test(1);
    let w2 = GlobalWorkerId::test(2);
    let p1 = PodId::test(10);

    send_worker_update(&tx, w1, PressureBand::Normal, 0).await;
    send_worker_update(&tx, w2, PressureBand::Normal, 0).await;

    // Artifact on w2.
    let artifact_id = distvirt_worker_protocol::ArtifactId::from("test-ns-art-42");
    tx.send(SchedulerInput::ArtifactEvent {
        worker_id: w2,
        event: ArtifactPlacementEvent::WriteCommitted {
            artifact_id: artifact_id.clone(),
            pool_id: distvirt_worker_protocol::PoolId::from("pool-1"),
            size_bytes: 1024,
        },
    })
    .await
    .unwrap();

    // Remove w2.
    tx.send(SchedulerInput::WorkerRemoved(w2)).await.unwrap();

    // Request with resume artifact — w2 is gone, should use w1 (no affinity effect).
    let mut reply_rx = register_namespace(&tx, "ns1").await;
    tx.send(SchedulerInput::RequestLease {
        namespace_id: ns("ns1"),
        pod_id: p1,
        proto_resume_artifact: Some(artifact_id),
    })
    .await
    .unwrap();

    let decision = reply_rx.recv().await.unwrap();
    match decision {
        SchedulerDecision::Grant { worker_id, .. } => {
            assert_eq!(worker_id, w1);
        }
        other => panic!("expected Grant, got {:?}", other),
    }
}
