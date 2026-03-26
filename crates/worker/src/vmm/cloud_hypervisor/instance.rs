use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::time::Duration;

use anyhow::{Context, bail};
use tokio::net::UnixStream;
use tokio::sync::watch;

use super::api_client::ApiClient;
use crate::fabric::FabricPort;
use crate::image_provider::containerd_overlayfs::OverlayfsCleanup;
use crate::task_handle::TaskHandle;
use crate::vmm::{
    SnapshotArtifacts, SnapshotMetadata, VmInstance, try_vsock_connect,
};
use crate::vmm::virtiofs::VirtiofsdProcess;

pub struct CloudHypervisorInstance {
    // Cleanup (Drop order matters — child MUST be first).
    child: tokio::process::Child,
    /// virtiofsd processes sharing host directories with the guest.
    /// Dropped after child (field order) — virtiofsd exit is harmless once CH is dead.
    _virtiofsd_processes: Vec<VirtiofsdProcess>,
    /// Overlayfs view cleanup — unmounts + removes containerd view on drop.
    _overlayfs_cleanup: Option<OverlayfsCleanup>,
    /// Containerd lease — keeps blobs alive. Dropped after overlayfs cleanup.
    _lease: Option<crate::image_provider::ContainerdLease>,
    /// Config volume temp directories — kept alive for virtiofsd.
    _config_vol_tmpdirs: Vec<tempfile::TempDir>,
    _serial_task: Option<TaskHandle<()>>,
    _exit_monitor: TaskHandle<()>,
    _tmpdir: tempfile::TempDir,

    // Runtime
    vsock_uds_path: PathBuf,
    pub(super) api: ApiClient,
    fabric_port: Option<FabricPort>,
    exit_rx: watch::Receiver<Option<ExitStatus>>,

    // Snapshot (pre-built metadata, cloned on snapshot())
    pub(super) snapshot_metadata: SnapshotMetadata,
}

/// Arguments for constructing a `CloudHypervisorInstance`.
///
/// Bundles everything produced during launch/restore into a single struct
/// to avoid a constructor with 15+ positional parameters.
pub(super) struct InstanceArgs {
    pub child: tokio::process::Child,
    pub virtiofsd_processes: Vec<VirtiofsdProcess>,
    pub overlayfs_cleanup: Option<OverlayfsCleanup>,
    pub lease: Option<crate::image_provider::ContainerdLease>,
    pub config_vol_tmpdirs: Vec<tempfile::TempDir>,
    pub serial_task: Option<TaskHandle<()>>,
    pub exit_monitor: TaskHandle<()>,
    pub tmpdir: tempfile::TempDir,
    pub vsock_uds_path: PathBuf,
    pub api: ApiClient,
    pub fabric_port: Option<FabricPort>,
    pub exit_rx: watch::Receiver<Option<ExitStatus>>,
    pub snapshot_metadata: SnapshotMetadata,
}

impl CloudHypervisorInstance {
    pub(super) fn new(args: InstanceArgs) -> Self {
        CloudHypervisorInstance {
            child: args.child,
            _virtiofsd_processes: args.virtiofsd_processes,
            _overlayfs_cleanup: args.overlayfs_cleanup,
            _lease: args.lease,
            _config_vol_tmpdirs: args.config_vol_tmpdirs,
            _serial_task: args.serial_task,
            _exit_monitor: args.exit_monitor,
            _tmpdir: args.tmpdir,
            vsock_uds_path: args.vsock_uds_path,
            api: args.api,
            fabric_port: args.fabric_port,
            exit_rx: args.exit_rx,
            snapshot_metadata: args.snapshot_metadata,
        }
    }
}

impl Drop for CloudHypervisorInstance {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

impl VmInstance for CloudHypervisorInstance {
    async fn connect_vsock(&self, port: u32) -> anyhow::Result<UnixStream> {
        let sock_path = self.vsock_uds_path.clone();
        log::info!(
            "connecting to guest vsock port {} via {}",
            port,
            sock_path.display()
        );
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            match try_vsock_connect(&sock_path, port).await {
                Ok(stream) => {
                    log::info!("vsock connected");
                    return Ok(stream);
                }
                Err(_) => {
                    if tokio::time::Instant::now() >= deadline {
                        bail!(
                            "timeout connecting to guest vsock port {} via {}",
                            port,
                            sock_path.display()
                        );
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }

    fn take_fabric_port(&mut self) -> Option<FabricPort> {
        self.fabric_port.take()
    }

    async fn wait(&mut self) -> anyhow::Result<ExitStatus> {
        self.exit_rx
            .wait_for(|s| s.is_some())
            .await
            .map_err(|_| anyhow::anyhow!("exit monitor task dropped"))?;
        let status = self
            .child
            .wait()
            .await
            .context("wait for cloud-hypervisor")?;
        Ok(status)
    }

    async fn kill(&mut self) -> anyhow::Result<()> {
        self.child
            .kill()
            .await
            .context("kill cloud-hypervisor")?;
        Ok(())
    }

    fn take_exit_signal(&mut self) -> Option<watch::Receiver<Option<ExitStatus>>> {
        Some(self.exit_rx.clone())
    }

    async fn set_balloon(&mut self, amount_mib: u32) -> anyhow::Result<()> {
        if !self.snapshot_metadata.balloon_configured {
            bail!("balloon device not configured for this VM");
        }
        self.api
            .request(
                "PUT",
                "/api/v1/vm.resize",
                Some(&serde_json::json!({"desired_balloon": (amount_mib as u64) * 1024 * 1024})),
            )
            .await
            .context("set balloon size")?;
        Ok(())
    }

    async fn snapshot(&mut self, snapshot_dir: &Path) -> anyhow::Result<SnapshotArtifacts> {
        tokio::fs::create_dir_all(snapshot_dir)
            .await
            .with_context(|| format!("create snapshot dir {}", snapshot_dir.display()))?;

        self.api
            .request("PUT", "/api/v1/vm.pause", None)
            .await
            .context("pause VM")?;

        let destination_url = format!("file://{}", snapshot_dir.display());
        self.api
            .request(
                "PUT",
                "/api/v1/vm.snapshot",
                Some(&serde_json::json!({"destination_url": destination_url})),
            )
            .await
            .context("create snapshot")?;

        let tmpdir_path = self._tmpdir.path();

        tokio::fs::copy(
            tmpdir_path.join("overlay.ext4"),
            snapshot_dir.join("overlay.ext4"),
        )
        .await
        .context("copy overlay.ext4 to snapshot dir")?;

        for vd in &self.snapshot_metadata.volume_drives {
            tokio::fs::copy(
                tmpdir_path.join(&vd.filename),
                snapshot_dir.join(&vd.filename),
            )
            .await
            .with_context(|| format!("copy volume '{}' to snapshot dir", vd.filename))?;
        }

        let metadata = self.snapshot_metadata.clone();
        let metadata_json =
            serde_json::to_vec_pretty(&metadata).context("serialize snapshot metadata")?;
        tokio::fs::write(snapshot_dir.join("metadata.json"), &metadata_json)
            .await
            .context("write metadata.json")?;

        log::info!("snapshot created at {}", snapshot_dir.display());

        Ok(SnapshotArtifacts {
            snapshot_dir: snapshot_dir.to_owned(),
            metadata,
        })
    }
}
