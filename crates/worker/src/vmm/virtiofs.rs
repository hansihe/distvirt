use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::Context;
use crate::task_handle::TaskHandle;

use super::wait_for_file;

/// A running virtiofsd process that shares a host directory with the guest.
///
/// Killed on drop, same as the CH process itself.
pub(crate) struct VirtiofsdProcess {
    child: tokio::process::Child,
    #[allow(dead_code)]
    socket_path: PathBuf,
    _stderr_task: TaskHandle<()>,
}

impl Drop for VirtiofsdProcess {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

/// Spawn a virtiofsd process for a single virtiofs mount.
///
/// The socket is placed in `working_dir` and named `virtiofs-<tag>.sock`.
/// Waits for the socket to appear before returning.
pub(crate) async fn spawn_virtiofsd(
    bin: &Path,
    working_dir: &Path,
    tag: &str,
    source_dir: &Path,
    read_only: bool,
) -> anyhow::Result<VirtiofsdProcess> {
    let socket_path = working_dir.join(format!("virtiofs-{}.sock", tag));

    let mut cmd = tokio::process::Command::new(bin);
    cmd.arg(format!("--socket-path={}", socket_path.display()))
        .arg(format!("--shared-dir={}", source_dir.display()))
        .arg("--announce-submounts")
        .arg("--sandbox=none")
        .arg("--migration-mode=find-paths")
        .arg("--migration-on-error=abort");
    if read_only {
        cmd.arg("--readonly");
    }

    let mut child = cmd
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("spawn virtiofsd for tag '{}'", tag))?;

    // Spawn a task to forward virtiofsd stderr to our logs.
    let stderr = child.stderr.take().expect("stderr was piped");
    let tag_owned = tag.to_owned();
    let _stderr_task = TaskHandle::spawn(async move {
        use tokio::io::{AsyncBufReadExt, BufReader};
        let mut lines = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            log::trace!("[virtiofsd:{}] {}", tag_owned, line);
        }
    });

    if let Err(e) = wait_for_file(&socket_path, Duration::from_secs(5)).await {
        return Err(e).context(format!(
            "waiting for virtiofsd socket for tag '{}' (check virtiofsd logs above)",
            tag,
        ));
    }

    log::info!(
        "virtiofsd: started for tag '{}' (source={})",
        tag,
        source_dir.display()
    );

    Ok(VirtiofsdProcess {
        child,
        socket_path,
        _stderr_task,
    })
}
