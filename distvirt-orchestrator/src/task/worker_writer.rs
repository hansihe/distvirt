use distvirt_worker_protocol::{OrchestratorWriter, WorkerCommand};
use tokio::sync::mpsc;

/// Writer task: receives fully-formed protocol WorkerCommands and sends them on the wire.
/// Exits on channel close or write error.
pub(crate) async fn run(
    mut rx: mpsc::Receiver<WorkerCommand>,
    mut writer: OrchestratorWriter,
) {
    while let Some(cmd) = rx.recv().await {
        if let Err(e) = writer.send_command(&cmd).await {
            eprintln!("worker writer error: {e}");
            break;
        }
    }
}
