//! VMM implementation that runs real guest-init supervisor code in-process.
//!
//! Unlike `TestVmm` (which uses `guest_sim`), this VMM runs the actual
//! guest-init supervisor with `TestContainerBackend` and `NullPlatform`.
//! This tests the real guest protocol handling, session handshake, output
//! pipeline, and container lifecycle — without needing root or real VMs.
//!
//! ## Suspend/Restore
//!
//! On suspend, the supervisor task is gracefully shut down (transport channel
//! closed) and its state is captured into a `SupervisorSnapshot`. On restore,
//! a new supervisor is spawned from the snapshot with the same container state,
//! output buffers, and event buffer contents. The snapshot is `Clone`, enabling
//! N restores from one snapshot (for future VM clone support).

use std::collections::HashMap;
use std::path::Path;
use std::process::ExitStatus;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use parking_lot::Mutex;
use tokio::sync::watch;

use distvirt_worker::task_handle::TaskHandle;
use distvirt_worker::vmm::{
    BaseVmConfig, GuestDevice, MountRequest, MountRestoreInfo, PlannedMount, ProvidedAccess,
    ResolvedEntry, ResolvedMounts, RestoreContext, SnapshotArtifacts, VmArtifacts, VmBuilder,
    VmInstance, Vmm,
};

use guest_init::buffer::{EventBuffer, OutputBuffer};
use guest_init::config::{GuestConfig, ShutdownMode, TransportConfig};
use guest_init::container::ContainerManager;
use guest_init::platform::NullPlatform;
use guest_init::supervisor::run_supervisor;
use guest_init::test_support::{
    BackendHandle, TestContainerBackend, TestContainerSnapshot, TokioSpawner,
};
use guest_init::transport::{BoxedStream, TransportListener};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Handle for controlling the guest-init instance from tests.
pub struct GuestInitHandle {
    pub backend: BackendHandle,
}

/// Shared slot for extracting the GuestInitHandle after launch.
type HandleSlot = Arc<Mutex<Option<GuestInitHandle>>>;

/// Shared slot for supervisor snapshots (suspend → restore).
type SnapshotSlot = Arc<Mutex<Option<SupervisorSnapshot>>>;

/// Captured supervisor state for suspend/restore/clone.
#[derive(Clone)]
struct SupervisorSnapshot {
    container_snapshots: Vec<TestContainerSnapshot>,
    next_pid: u32,
    /// Drained output buffer contents per container.
    output_buffers: HashMap<String, Vec<Vec<u8>>>,
    /// Drained event buffer contents.
    events: Vec<distvirt_guest_protocol::GuestEvent>,
}

// ---------------------------------------------------------------------------
// GuestInitVmm
// ---------------------------------------------------------------------------

/// VMM that runs real guest-init supervisor code in-process.
pub struct GuestInitVmm {
    handle_slot: HandleSlot,
    snapshot_slot: SnapshotSlot,
}

impl GuestInitVmm {
    pub fn new() -> Self {
        GuestInitVmm {
            handle_slot: Arc::new(Mutex::new(None)),
            snapshot_slot: Arc::new(Mutex::new(None)),
        }
    }

    /// Take the most recently produced GuestInitHandle.
    pub fn take_handle(&self) -> Option<GuestInitHandle> {
        self.handle_slot.lock().take()
    }
}

// ---------------------------------------------------------------------------
// GuestInitVmmBuilder
// ---------------------------------------------------------------------------

pub struct GuestInitVmmBuilder {
    mount_plans: Vec<(String, ProvidedAccess)>,
    scratch_plans: Vec<String>,
    _mount_restore_info: Vec<MountRestoreInfo>,
    handle_slot: HandleSlot,
    snapshot_slot: SnapshotSlot,
}

impl VmBuilder for GuestInitVmmBuilder {
    type Instance = GuestInitVmInstance;
    fn add_mount(&mut self, request: MountRequest) -> anyhow::Result<PlannedMount> {
        let provided = match &request.source {
            distvirt_worker::vmm::VmMountSource::ContainerdImage { .. }
            | distvirt_worker::vmm::VmMountSource::Directory { .. } => {
                ProvidedAccess::VirtioFs { read_only: true }
            }
            distvirt_worker::vmm::VmMountSource::BlockImage { read_only, .. } => {
                ProvidedAccess::BlockDevice {
                    read_only: *read_only,
                }
            }
        };
        self.mount_plans
            .push((request.tag.clone(), provided.clone()));
        Ok(PlannedMount {
            tag: request.tag,
            provided,
        })
    }

    fn add_scratch_device(&mut self, tag: &str, _size_mib: u32) -> anyhow::Result<()> {
        self.scratch_plans.push(tag.to_string());
        Ok(())
    }

    fn set_snapshot_context(&mut self, mount_restore_info: Vec<MountRestoreInfo>) {
        self._mount_restore_info = mount_restore_info;
    }

    async fn launch(
        self,
    ) -> anyhow::Result<(VmArtifacts<GuestInitVmInstance>, ResolvedMounts)> {
        let (instance, host_socket, exit_rx) = spawn_guest_init_supervisor(
            &self.handle_slot,
            &self.snapshot_slot,
            None,
        )?;

        // Build resolved mounts (same logic as TestVmm).
        let mut entries = Vec::new();
        let mut block_idx: u8 = 1;

        for tag in &self.scratch_plans {
            let device = format!("/dev/vd{}", (b'a' + block_idx) as char);
            entries.push(ResolvedEntry {
                tag: tag.clone(),
                guest: GuestDevice::Device { path: device },
            });
            block_idx += 1;
        }

        for (tag, provided) in &self.mount_plans {
            match provided {
                ProvidedAccess::VirtioFs { .. } => {
                    entries.push(ResolvedEntry {
                        tag: tag.clone(),
                        guest: GuestDevice::VirtioFs {
                            virtiofs_tag: tag.clone(),
                        },
                    });
                }
                ProvidedAccess::BlockDevice { .. } => {
                    let device = format!("/dev/vd{}", (b'a' + block_idx) as char);
                    entries.push(ResolvedEntry {
                        tag: tag.clone(),
                        guest: GuestDevice::Device { path: device },
                    });
                    block_idx += 1;
                }
            }
        }

        Ok((
            VmArtifacts {
                instance,
                vsock_stream: host_socket,
                fabric_port: None,
                exit_signal: exit_rx,
            },
            ResolvedMounts { entries },
        ))
    }
}

// ---------------------------------------------------------------------------
// Supervisor spawning (fresh or from snapshot)
// ---------------------------------------------------------------------------

fn spawn_guest_init_supervisor(
    handle_slot: &HandleSlot,
    snapshot_slot: &SnapshotSlot,
    restore_from: Option<SupervisorSnapshot>,
) -> anyhow::Result<(GuestInitVmInstance, tokio::net::UnixStream, watch::Receiver<Option<ExitStatus>>)> {
    let (host_socket, guest_socket) = tokio::net::UnixStream::pair()?;
    let (exit_tx, exit_rx) = watch::channel(None);

    // Build or restore state.
    let (containers, event_buffer, backend_handle) = match restore_from {
        Some(snap) => {
            let event_buffer = Arc::new(EventBuffer::new());
            event_buffer.repopulate(snap.events);

            // Create output buffers and collect their senders for the backend.
            let mut output_txs = HashMap::new();
            let mut output_bufs = HashMap::new();
            for (id, chunks) in snap.output_buffers {
                let buf = OutputBuffer::new(256);
                let tx = buf.sender();
                for chunk in chunks {
                    let _ = tx.try_send(chunk);
                }
                output_txs.insert(id.clone(), tx);
                output_bufs.insert(id, buf);
            }

            let container_ids: Vec<String> =
                snap.container_snapshots.iter().map(|s| s.id.clone()).collect();
            let (backend, bh) = TestContainerBackend::new_from_snapshot(
                &snap.container_snapshots,
                snap.next_pid,
                output_txs,
            );
            let cm = ContainerManager::new_from_snapshot(backend, container_ids, output_bufs);
            (Arc::new(Mutex::new(cm)), event_buffer, bh)
        }
        None => {
            let (backend, bh) = TestContainerBackend::new();
            (
                Arc::new(Mutex::new(ContainerManager::new(backend))),
                Arc::new(EventBuffer::new()),
                bh,
            )
        }
    };

    {
        let mut slot = handle_slot.lock();
        *slot = Some(GuestInitHandle {
            backend: backend_handle,
        });
    }

    // Create transport channel. Send initial stream synchronously via try_send
    // so the task never holds a Sender — the instance is the sole owner.
    let (transport_tx, transport_rx) = async_channel::bounded::<BoxedStream>(4);
    {
        use tokio_util::compat::TokioAsyncReadCompatExt;
        let compat_stream = guest_socket.compat();
        transport_tx
            .try_send(Box::new(compat_stream) as BoxedStream)
            .map_err(|_| anyhow::anyhow!("transport channel full on initial send"))?;
    }

    let containers_clone = Arc::clone(&containers);
    let event_buffer_clone = Arc::clone(&event_buffer);

    let supervisor_task = TaskHandle::new(tokio::spawn(async move {
        let spawner = TokioSpawner;
        let platform = NullPlatform;
        let config = GuestConfig {
            balloon_mib: None,
            transport: TransportConfig::Vsock { port: 0 },
            config_device: None,
            shutdown_mode: ShutdownMode::Reboot,
            shutdown_timeout: Duration::from_millis(5),
            shutdown_kill_timeout: Duration::from_millis(5),
        };
        let listener = TransportListener::Test(transport_rx);

        if let Err(e) = run_supervisor(
            &config,
            &platform,
            containers_clone,
            &event_buffer_clone,
            &listener,
            &spawner,
        )
        .await
        {
            log::error!("[guest-init-vmm] supervisor error: {:#}", e);
        }
    }));

    let instance = GuestInitVmInstance {
        supervisor_task: Some(supervisor_task),
        exit_tx,
        transport_tx: Some(transport_tx),
        containers,
        event_buffer,
        snapshot_slot: Arc::clone(snapshot_slot),
    };

    Ok((instance, host_socket, exit_rx))
}

// ---------------------------------------------------------------------------
// Vmm impl
// ---------------------------------------------------------------------------

impl Vmm for GuestInitVmm {
    type Builder = GuestInitVmmBuilder;
    type Instance = GuestInitVmInstance;
    fn builder(&self, _base: BaseVmConfig) -> anyhow::Result<GuestInitVmmBuilder> {
        Ok(GuestInitVmmBuilder {
            mount_plans: Vec::new(),
            scratch_plans: Vec::new(),
            _mount_restore_info: Vec::new(),
            handle_slot: Arc::clone(&self.handle_slot),
            snapshot_slot: Arc::clone(&self.snapshot_slot),
        })
    }

    async fn restore(
        &self,
        snapshot: &SnapshotArtifacts,
        _ctx: RestoreContext,
    ) -> anyhow::Result<VmArtifacts<GuestInitVmInstance>> {
        let metadata_path = snapshot.snapshot_dir.join("metadata.json");
        let _bytes =
            std::fs::read(&metadata_path).context("read metadata.json from snapshot dir")?;

        // Clone snapshot (not take) — supports N restores from one snapshot.
        let supervisor_snapshot = self
            .snapshot_slot
            .lock()
            .clone()
            .context("no supervisor snapshot available for restore")?;

        let (instance, host_socket, exit_rx) = spawn_guest_init_supervisor(
            &self.handle_slot,
            &self.snapshot_slot,
            Some(supervisor_snapshot),
        )?;

        Ok(VmArtifacts {
            instance,
            vsock_stream: host_socket,
            fabric_port: None,
            exit_signal: exit_rx,
        })
    }
}

// ---------------------------------------------------------------------------
// GuestInitVmInstance
// ---------------------------------------------------------------------------

/// A running guest-init instance (supervisor task + shared state).
pub struct GuestInitVmInstance {
    supervisor_task: Option<TaskHandle<()>>,
    exit_tx: watch::Sender<Option<ExitStatus>>,
    /// Dropping this closes the transport channel, causing the supervisor to exit.
    transport_tx: Option<async_channel::Sender<BoxedStream>>,
    /// Shared with the supervisor task for snapshot extraction.
    containers: Arc<Mutex<ContainerManager<TestContainerBackend>>>,
    /// Shared with the supervisor task for snapshot extraction.
    event_buffer: Arc<EventBuffer>,
    /// Where to store the snapshot on suspend.
    snapshot_slot: SnapshotSlot,
}

fn zero_exit_status() -> ExitStatus {
    std::process::Command::new("true")
        .status()
        .expect("failed to run `true` for synthetic ExitStatus")
}

fn write_snapshot_metadata(snapshot_dir: &Path) -> anyhow::Result<SnapshotArtifacts> {
    let metadata = distvirt_worker::vmm::SnapshotMetadata {
        kernel_path: "/dev/null".into(),
        rootfs_source_path: "/dev/null".into(),
        balloon_configured: false,
        serial_console: false,
        volume_drives: vec![],
        virtiofs_mounts: vec![],
        mount_restore_info: vec![],
        container_image_ref: None,
        config_volumes: vec![],
    };
    let metadata_path = snapshot_dir.join("metadata.json");
    std::fs::create_dir_all(snapshot_dir)?;
    std::fs::write(&metadata_path, serde_json::to_string_pretty(&metadata)?)?;
    Ok(SnapshotArtifacts {
        snapshot_dir: snapshot_dir.to_path_buf(),
        metadata,
    })
}

impl GuestInitVmInstance {
    /// Extract a SupervisorSnapshot from the shared state.
    /// Call only after the supervisor task has exited.
    fn extract_snapshot(&self) -> SupervisorSnapshot {
        let cm = self.containers.lock();
        let container_snapshots = cm.backend().snapshot_containers();
        let next_pid = cm.backend().next_pid();

        let mut output_buffers = HashMap::new();
        for snap in &container_snapshots {
            if cm.has_output_buffer(&snap.id) {
                let chunks = cm.drain_output_buffer(&snap.id);
                output_buffers.insert(snap.id.clone(), chunks);
            }
        }

        let mut events = self.event_buffer.drain();
        // Filter out the spurious TaskError from transport channel closure.
        events.retain(|e| {
            !matches!(e, distvirt_guest_protocol::GuestEvent::TaskError { .. })
        });

        SupervisorSnapshot {
            container_snapshots,
            next_pid,
            output_buffers,
            events,
        }
    }
}

impl VmInstance for GuestInitVmInstance {
    async fn wait(&mut self) -> anyhow::Result<ExitStatus> {
        if let Some(ref mut task) = self.supervisor_task {
            let _ = task.await;
        }
        Ok(zero_exit_status())
    }

    async fn kill(&mut self) -> anyhow::Result<()> {
        // Close transport to trigger graceful shutdown, then abort if needed.
        drop(self.transport_tx.take());
        if let Some(task) = self.supervisor_task.take() {
            drop(task); // Abort-on-drop if still running
        }
        let _ = self.exit_tx.send(Some(zero_exit_status()));
        Ok(())
    }

    async fn snapshot(
        &mut self,
        snapshot_dir: &Path,
    ) -> anyhow::Result<SnapshotArtifacts> {
        write_snapshot_metadata(snapshot_dir)
    }

    async fn suspend(mut self, snapshot_dir: &Path) -> anyhow::Result<SnapshotArtifacts> {
        // 1. Close transport channel → supervisor's listener.accept() fails → task exits.
        drop(self.transport_tx.take());

        // 2. Wait for the supervisor task to exit cleanly.
        if let Some(ref mut task) = self.supervisor_task {
            let _ = task.await;
        }
        self.supervisor_task = None;

        // 3. Extract and store snapshot.
        let snapshot = self.extract_snapshot();
        *self.snapshot_slot.lock() = Some(snapshot);

        // 4. Write metadata.
        write_snapshot_metadata(snapshot_dir)
    }
}
