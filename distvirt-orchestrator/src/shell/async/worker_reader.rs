//! Per-connection wire decoder for the new async shell.
//!
//! The reader receives wire events, classifies them using the pure
//! `core::worker_event::classify` function, and forwards to the shell channel.
//! No domain logic lives here.

use distvirt_worker_protocol::OrchestratorReader;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::core::{
    GlobalWorkerId,
    worker_event::{ClassifiedWorkerEvent, classify},
};
use crate::core::types::{NamespaceCoreEvent, OrchestratorToNamespace};

use super::ShellEvent;

/// Spawn a worker reader task.
pub(crate) fn spawn(
    global_worker_id: GlobalWorkerId,
    reader: OrchestratorReader,
    shell_tx: mpsc::Sender<ShellEvent>,
) -> JoinHandle<()> {
    tokio::spawn(run(global_worker_id, reader, shell_tx))
}

async fn run(
    global_worker_id: GlobalWorkerId,
    mut reader: OrchestratorReader,
    shell_tx: mpsc::Sender<ShellEvent>,
) {
    loop {
        match reader.recv_event().await {
            Ok(event) => {
                let shell_event = match classify(global_worker_id, event) {
                    ClassifiedWorkerEvent::Namespace {
                        namespace_id,
                        event,
                    } => ShellEvent::NamespaceEvent {
                        namespace_id,
                        event: core_event_to_ns_message(event),
                    },
                    ClassifiedWorkerEvent::WorkerState(event) => {
                        ShellEvent::WorkerStateEvent(event)
                    }
                    ClassifiedWorkerEvent::Scheduler(input) => ShellEvent::SchedulerInput(input),
                    ClassifiedWorkerEvent::Ignored => continue,
                };
                if shell_tx.send(shell_event).await.is_err() {
                    break;
                }
            }
            Err(e) => {
                eprintln!("worker reader error for {:?}: {}", global_worker_id, e);
                break;
            }
        }
    }

    let _ = shell_tx
        .send(ShellEvent::WorkerDisconnected {
            worker_id: global_worker_id,
        })
        .await;
}

/// Convert a classified NamespaceCoreEvent to OrchestratorToNamespace.
fn core_event_to_ns_message(event: NamespaceCoreEvent) -> OrchestratorToNamespace {
    match event {
        NamespaceCoreEvent::WorkerEvent(e) => OrchestratorToNamespace::WorkerEvent(e),
        NamespaceCoreEvent::SchedulerDecision(d) => OrchestratorToNamespace::SchedulerDecision(d),
        NamespaceCoreEvent::WorkerConnected {
            worker_id,
            proto_worker_id,
            info,
        } => OrchestratorToNamespace::WorkerConnected {
            worker_id,
            proto_worker_id,
            info,
        },
        NamespaceCoreEvent::WorkerDisconnected { worker_id } => {
            OrchestratorToNamespace::WorkerDisconnected { worker_id }
        }
        NamespaceCoreEvent::ClientCommand(c) => OrchestratorToNamespace::ClientCommand(c),
        NamespaceCoreEvent::ArtifactInvalidated { artifact_port_id } => {
            OrchestratorToNamespace::ArtifactInvalidated { artifact_port_id }
        }
        NamespaceCoreEvent::TimerFired { .. } => {
            unreachable!("TimerFired events should not come from worker events")
        }
    }
}
