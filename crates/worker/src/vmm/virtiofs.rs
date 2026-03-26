use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, bail};

use super::wait_for_file;

/// A running virtiofsd process that shares a host directory with the guest.
///
/// Killed on drop, same as the CH process itself.
pub(crate) struct VirtiofsdProcess {
    child: tokio::process::Child,
    #[allow(dead_code)]
    socket_path: PathBuf,
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
) -> anyhow::Result<VirtiofsdProcess> {
    let socket_path = working_dir.join(format!("virtiofs-{}.sock", tag));

    let child = tokio::process::Command::new(bin)
        .arg(format!("--socket-path={}", socket_path.display()))
        .arg(format!("--shared-dir={}", source_dir.display()))
        .arg("--announce-submounts")
        .arg("--sandbox=none")
        .arg("--readonly")
        .arg("--migration-mode=find-paths")
        .arg("--migration-on-error=abort")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("spawn virtiofsd for tag '{}'", tag))?;

    if let Err(e) = wait_for_file(&socket_path, Duration::from_secs(5)).await {
        // Socket didn't appear — virtiofsd likely crashed. Try to grab stderr.
        let output = child.wait_with_output().await;
        let stderr = output
            .as_ref()
            .ok()
            .map(|o| String::from_utf8_lossy(&o.stderr))
            .unwrap_or_default();
        if stderr.is_empty() {
            return Err(e).context(format!("waiting for virtiofsd socket for tag '{}'", tag));
        } else {
            bail!(
                "virtiofsd for tag '{}' failed to start: {}",
                tag,
                stderr.trim()
            );
        }
    }

    log::info!(
        "virtiofsd: started for tag '{}' (source={})",
        tag,
        source_dir.display()
    );

    Ok(VirtiofsdProcess {
        child,
        socket_path,
    })
}
