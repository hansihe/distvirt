//! Test VMM implementation using the guest simulator.
//!
//! Provides a `Vmm` + `VmInstance` that runs entirely in-process using
//! `UnixStream::pair()` for vsock and `ChannelPort` for the fabric port.
//! No root, no real VMs, millisecond test times.

use std::path::Path;
use std::process::ExitStatus;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Context;
use tokio::net::UnixStream;
use tokio::sync::{mpsc, watch};

use super::guest_sim::{ContainerBehavior, GuestSimConfig, SuspendBehavior, run_guest_sim};
use super::{SnapshotArtifacts, SnapshotMetadata, VmConfig, VmInstance, Vmm};
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
    pub suspend_behavior: SuspendBehavior,
    crash_handle_tx: Option<mpsc::UnboundedSender<CrashHandle>>,
    fail_counter: Option<(Arc<AtomicU32>, u32)>,
}

impl TestVmm {
    /// Create a TestVmm without crash handle support.
    pub fn new(container_behavior: ContainerBehavior) -> Self {
        TestVmm {
            container_behavior,
            suspend_behavior: SuspendBehavior::Immediate,
            crash_handle_tx: None,
            fail_counter: None,
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
                suspend_behavior: SuspendBehavior::Immediate,
                crash_handle_tx: Some(tx),
                fail_counter: None,
            },
            rx,
        )
    }

    /// Create a TestVmm where `PrepareSuspend` hangs (never responds).
    pub fn with_suspend_hang(container_behavior: ContainerBehavior) -> Self {
        TestVmm {
            container_behavior,
            suspend_behavior: SuspendBehavior::Hang,
            crash_handle_tx: None,
            fail_counter: None,
        }
    }

    /// Create a TestVmm that fails the first `fail_times` launches, then succeeds.
    pub fn with_fail_then_run(fail_times: u32) -> (Self, Arc<AtomicU32>) {
        let counter = Arc::new(AtomicU32::new(0));
        (
            TestVmm {
                container_behavior: ContainerBehavior::RunUntilSignaled,
                suspend_behavior: SuspendBehavior::Immediate,
                crash_handle_tx: None,
                fail_counter: Some((counter.clone(), fail_times)),
            },
            counter,
        )
    }

    fn make_config(&self) -> GuestSimConfig {
        let fail_before_ready = if let Some((ref counter, fail_times)) = self.fail_counter {
            counter.fetch_add(1, Ordering::SeqCst) < fail_times
        } else {
            false
        };
        GuestSimConfig {
            container_behavior: self.container_behavior.clone(),
            suspend_behavior: self.suspend_behavior.clone(),
            fail_before_ready,
        }
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
    config: GuestSimConfig,
) -> anyhow::Result<(
    UnixStream,
    TaskHandle<anyhow::Result<()>>,
    watch::Sender<Option<ExitStatus>>,
)> {
    let (host_socket, guest_socket) =
        UnixStream::pair().context("create UnixStream pair for vsock")?;

    let (exit_tx, _exit_rx) = watch::channel(None);

    let exit_tx_clone = exit_tx.clone();
    let sim_task = TaskHandle::spawn(async move {
        let result = run_guest_sim(guest_socket, config).await;
        let _ = exit_tx_clone.send(Some(zero_exit_status()));
        result
    });

    Ok((host_socket, sim_task, exit_tx))
}

impl Vmm for TestVmm {
    type Instance = TestVmInstance;

    async fn launch(
        &self,
        _config: VmConfig,
    ) -> anyhow::Result<(TestVmInstance, super::LaunchResult)> {
        let (host_socket, sim_task, exit_tx) = spawn_guest_sim(self.make_config())?;

        if let Some(ref tx) = self.crash_handle_tx {
            let _ = tx.send(CrashHandle(exit_tx.clone()));
        }

        let instance = TestVmInstance {
            vsock_socket: Mutex::new(Some(host_socket)),
            fabric_port: None,
            sim_task: Some(sim_task),
            exit_tx,
        };

        let launch_result = super::LaunchResult {
            container_rootfs: distvirt_guest_protocol::ContainerRootfs::VirtioFsOverlay {
                tag: "container-rootfs".to_string(),
                overlay_device: "/dev/vdb".to_string(),
            },
            volume_mounts: Vec::new(),
        };

        Ok((instance, launch_result))
    }

    async fn restore(
        &self,
        snapshot: &SnapshotArtifacts,
        _ctx: super::RestoreContext,
    ) -> anyhow::Result<TestVmInstance> {
        // Validate snapshot exists by reading metadata.json.
        // Uses std::fs instead of tokio::fs because tokio::fs dispatches to
        // spawn_blocking, which causes flaky tests under `current_thread` +
        // `start_paused` (the blocking pool is shared across test runtimes and
        // doesn't advance with fake time).
        let metadata_path = snapshot.snapshot_dir.join("metadata.json");
        let _bytes =
            std::fs::read(&metadata_path).context("read metadata.json from snapshot dir")?;

        let (host_socket, sim_task, exit_tx) = spawn_guest_sim(self.make_config())?;

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
        // Uses std::fs instead of tokio::fs because tokio::fs dispatches to
        // spawn_blocking, which causes flaky tests under `current_thread` +
        // `start_paused` (the blocking pool is shared across test runtimes and
        // doesn't advance with fake time).
        std::fs::create_dir_all(snapshot_dir).context("create snapshot dir")?;

        let metadata = SnapshotMetadata {
            kernel_path: "/dev/null".into(),
            rootfs_source_path: "/dev/null".into(),
            balloon_configured: false,
            serial_console: false,
            volume_drives: vec![],
            virtiofs_mounts: vec![],
            container_image_ref: None,
            config_volumes: vec![],
        };

        // Write metadata.json.
        let metadata_json = serde_json::to_vec_pretty(&metadata).context("serialize metadata")?;
        std::fs::write(snapshot_dir.join("metadata.json"), &metadata_json)
            .context("write metadata.json")?;

        // Write a small dummy snapshot.bin so dir_size() returns non-zero.
        std::fs::write(snapshot_dir.join("snapshot.bin"), b"test-snapshot")
            .context("write snapshot.bin")?;

        Ok(SnapshotArtifacts {
            snapshot_dir: snapshot_dir.to_path_buf(),
            metadata,
        })
    }
}
