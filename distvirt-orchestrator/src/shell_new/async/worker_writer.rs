//! Per-connection wire encoder for the new async shell.
//!
//! Identical to `task/worker_writer.rs` — re-used here so shell_new/
//! is self-contained.

use distvirt_worker_protocol::{OrchestratorWriter, WorkerCommand};
use tokio::sync::mpsc;

/// Writer task: receives fully-formed protocol WorkerCommands and sends them on the wire.
/// Exits on channel close or write error.
pub(crate) async fn run(mut rx: mpsc::Receiver<WorkerCommand>, mut writer: OrchestratorWriter) {
    while let Some(cmd) = rx.recv().await {
        if let Err(e) = writer.send_command(&cmd).await {
            eprintln!("worker writer error: {e}");
            break;
        }
    }
}

// =============================================================================
// Worker writer
// =============================================================================

/// Handle for sending commands to a specific worker.
/// Sends fully-formed protocol commands (built by the namespace task).
#[derive(Clone)]
pub(crate) struct WorkerWriterHandle {
    tx: mpsc::Sender<distvirt_worker_protocol::WorkerCommand>,
}

impl WorkerWriterHandle {
    pub fn new(tx: mpsc::Sender<distvirt_worker_protocol::WorkerCommand>) -> Self {
        Self { tx }
    }

    pub async fn send(&self, cmd: distvirt_worker_protocol::WorkerCommand) {
        let _ = self.tx.send(cmd).await;
    }
}
