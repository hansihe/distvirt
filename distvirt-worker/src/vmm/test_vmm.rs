//! Test VMM implementation using the guest simulator.
//!
//! Provides a `Vmm` + `VmInstance` that runs entirely in-process using
//! `UnixStream::pair()` for vsock and `ChannelPort` for the fabric port.
//! No root, no real VMs, millisecond test times.

use std::path::Path;
use std::process::ExitStatus;
use std::sync::Mutex;

use anyhow::Context;
use tokio::net::UnixStream;
use tokio::sync::{mpsc, watch};

use super::guest_sim::{ContainerBehavior, GuestSimConfig, run_guest_sim};
use super::{NetConfig, SnapshotArtifacts, SnapshotMetadata, VmConfig, VmInstance, Vmm};
use crate::fabric::FabricPort;
use crate::task_handle::TaskHandle;

/// Handle that allows test code to simulate a VM crash.
#[derive(Clone)]
pub struct CrashHandle(watch::Sender<Option<ExitStatus>>);

impl CrashHandle {
    /// Trigger a simulated VM crash (fires the exit signal).
    pub fn crash(&self) {
        let _ = self.0.send(Some(zero_exit_status()));
    }
}

/// Test VMM that launches in-process guest simulators.
pub struct TestVmm {
    pub container_behavior: ContainerBehavior,
    crash_handle_tx: Option<mpsc::UnboundedSender<CrashHandle>>,
}

impl TestVmm {
    /// Create a TestVmm without crash handle support.
    pub fn new(container_behavior: ContainerBehavior) -> Self {
        TestVmm {
            container_behavior,
            crash_handle_tx: None,
        }
    }

    /// Create a TestVmm that sends a `CrashHandle` for each launched VM
    /// through the returned receiver.
    pub fn with_crash_handles(
        container_behavior: ContainerBehavior,
    ) -> (Self, mpsc::UnboundedReceiver<CrashHandle>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            TestVmm {
                container_behavior,
                crash_handle_tx: Some(tx),
            },
            rx,
        )
    }
}

/// A running test VM instance backed by a guest simulator task.
pub struct TestVmInstance {
    vsock_socket: Mutex<Option<UnixStream>>,
    fabric_port: Option<FabricPort>,
    sim_task: Option<TaskHandle<anyhow::Result<()>>>,
    exit_tx: watch::Sender<Option<ExitStatus>>,
}

/// Create a synthetic zero `ExitStatus`.
fn zero_exit_status() -> ExitStatus {
    std::process::Command::new("true")
        .status()
        .expect("failed to run `true` for synthetic ExitStatus")
}

/// Spawn a guest sim and return the instance fields.
fn spawn_guest_sim(
    behavior: ContainerBehavior,
) -> anyhow::Result<(
    UnixStream,
    TaskHandle<anyhow::Result<()>>,
    watch::Sender<Option<ExitStatus>>,
)> {
    let (host_socket, guest_socket) =
        UnixStream::pair().context("create UnixStream pair for vsock")?;

    let (exit_tx, _exit_rx) = watch::channel(None);

    let cloned_behavior = behavior.clone();
    let exit_tx_clone = exit_tx.clone();
    let sim_task = TaskHandle::spawn(async move {
        let result = run_guest_sim(
            guest_socket,
            GuestSimConfig {
                container_behavior: cloned_behavior,
            },
        )
        .await;
        let _ = exit_tx_clone.send(Some(zero_exit_status()));
        result
    });

    Ok((host_socket, sim_task, exit_tx))
}

impl Vmm for TestVmm {
    type Instance = TestVmInstance;

    async fn launch(&self, _config: &VmConfig) -> anyhow::Result<TestVmInstance> {
        let (host_socket, sim_task, exit_tx) = spawn_guest_sim(self.container_behavior.clone())?;

        if let Some(ref tx) = self.crash_handle_tx {
            let _ = tx.send(CrashHandle(exit_tx.clone()));
        }

        Ok(TestVmInstance {
            vsock_socket: Mutex::new(Some(host_socket)),
            fabric_port: None,
            sim_task: Some(sim_task),
            exit_tx,
        })
    }

    async fn restore(
        &self,
        snapshot: &SnapshotArtifacts,
        _net: Option<&NetConfig>,
    ) -> anyhow::Result<TestVmInstance> {
        // Validate snapshot exists by reading metadata.json.
        let metadata_path = snapshot.snapshot_dir.join("metadata.json");
        let _bytes = tokio::fs::read(&metadata_path)
            .await
            .context("read metadata.json from snapshot dir")?;

        let (host_socket, sim_task, exit_tx) = spawn_guest_sim(self.container_behavior.clone())?;

        if let Some(ref tx) = self.crash_handle_tx {
            let _ = tx.send(CrashHandle(exit_tx.clone()));
        }

        Ok(TestVmInstance {
            vsock_socket: Mutex::new(Some(host_socket)),
            fabric_port: None,
            sim_task: Some(sim_task),
            exit_tx,
        })
    }
}

impl VmInstance for TestVmInstance {
    async fn connect_vsock(&self, _port: u32) -> anyhow::Result<UnixStream> {
        self.vsock_socket
            .lock()
            .expect("poisoned")
            .take()
            .context("TestVmInstance: vsock socket already taken")
    }

    fn take_fabric_port(&mut self) -> Option<FabricPort> {
        self.fabric_port.take()
    }

    async fn wait(&mut self) -> anyhow::Result<ExitStatus> {
        if let Some(task) = self.sim_task.take() {
            let _ = task.await;
        }
        Ok(zero_exit_status())
    }

    async fn kill(&mut self) -> anyhow::Result<()> {
        if let Some(task) = self.sim_task.take() {
            task.abort();
        }
        let _ = self.exit_tx.send(Some(zero_exit_status()));
        Ok(())
    }

    fn take_exit_signal(&mut self) -> Option<watch::Receiver<Option<ExitStatus>>> {
        Some(self.exit_tx.subscribe())
    }

    async fn snapshot(&mut self, snapshot_dir: &Path) -> anyhow::Result<SnapshotArtifacts> {
        tokio::fs::create_dir_all(snapshot_dir)
            .await
            .context("create snapshot dir")?;

        let metadata = SnapshotMetadata {
            kernel_path: "/dev/null".into(),
            rootfs_source_path: "/dev/null".into(),
            balloon_configured: false,
            serial_console: false,
        };

        // Write metadata.json.
        let metadata_json =
            serde_json::to_vec_pretty(&metadata).context("serialize metadata")?;
        tokio::fs::write(snapshot_dir.join("metadata.json"), &metadata_json)
            .await
            .context("write metadata.json")?;

        // Write a small dummy snapshot.bin so dir_size() returns non-zero.
        tokio::fs::write(snapshot_dir.join("snapshot.bin"), b"test-snapshot")
            .await
            .context("write snapshot.bin")?;

        Ok(SnapshotArtifacts {
            snapshot_dir: snapshot_dir.to_path_buf(),
            metadata,
        })
    }
}
