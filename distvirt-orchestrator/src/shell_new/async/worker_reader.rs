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
                        event,
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

    // On exit: notify shell of disconnection.
    let _ = shell_tx
        .send(ShellEvent::WorkerDisconnected {
            worker_id: global_worker_id,
        })
        .await;
}
