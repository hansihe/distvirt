use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use distvirt_worker_protocol::{
    ArtifactId, ContainerSpec, LogStreamOpener, NamespaceId, PodId,
    PodNetworkConfig, PoolId, WorkerEvent,
};
use futures_lite::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::fabric::{Fabric, FabricPort};
use crate::image_provider::ImageProvider;
use crate::io_session::IoEvent;
use crate::managed_vm::ManagedVm;
use crate::pod::wait_for_vm_exit;
use crate::task_handle::TaskHandle;
use crate::vmm::{
    SnapshotArtifacts, VmInstance, Vmm,
};

// Timeout escalation chain for pod shutdown. These must satisfy:
//   GRACEFUL_SHUTDOWN_TIMEOUT < STOP_POD_TIMEOUT
// so the graceful shutdown attempt completes before we give up waiting
// for the supervisor. FORCE_STOP_TIMEOUT is a last-resort cleanup window
// after the supervisor task is aborted.

/// Timeout for graceful guest shutdown (SIGTERM → wait for exit) before force-killing.
pub(crate) const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// Outer timeout for awaiting a pod supervisor after graceful cancellation.
/// Must be greater than GRACEFUL_SHUTDOWN_TIMEOUT to give the supervisor time
/// to complete its graceful path before we abort it.
pub(crate) const STOP_POD_TIMEOUT: Duration = Duration::from_secs(15);

/// Timeout for non-graceful (force) stop cleanup after aborting the supervisor.
/// Short because at this point we've already given up on graceful shutdown.
pub(crate) const FORCE_STOP_TIMEOUT: Duration = Duration::from_secs(2);

/// Request to suspend a running pod.
pub(crate) struct SuspendRequest {
    pub(crate) artifact_id: ArtifactId,
    pub(crate) snapshot_dir: PathBuf,
    pub(crate) pool_id: PoolId,
    pub(crate) reply: oneshot::Sender<Result<SnapshotArtifacts, String>>,
}

/// Per-pod state: cancellation token, supervisor task handle, and suspend channel.
pub(crate) struct PodState {
    pub(crate) cancel: CancellationToken,
    pub(crate) supervisor: TaskHandle<()>,
    pub(crate) suspend_tx: mpsc::Sender<SuspendRequest>,
}

/// Send an event to the worker main loop, or log and return if the worker is shutting down.
pub(crate) async fn send_event(tx: &mpsc::Sender<WorkerEvent>, event: WorkerEvent) {
    if tx.send(event).await.is_err() {
        log::warn!("failed to send event, worker already shut down");
    }
}

/// Top-level pod supervisor: launches the pod and monitors it.
///
/// On launch failure, sends `PodFailed` and returns.
/// On success, sends `PodRunning` then delegates to `pod_monitor`.
pub(crate) async fn pod_supervisor<
    V: Vmm + 'static,
    P: ImageProvider + 'static,
    F: crate::fs::Fs,
>(
    vmm: Arc<V>,
    image_provider: Arc<P>,
    fabric: Arc<Fabric<FabricPort>>,
    kernel_path: PathBuf,
    rootfs_image_path: PathBuf,
    log_opener: LogStreamOpener,
    cancel: CancellationToken,
    event_tx: mpsc::Sender<WorkerEvent>,
    namespace_id: NamespaceId,
    pod_id: PodId,
    network: PodNetworkConfig,
    containers: Vec<ContainerSpec>,
    resources: Option<distvirt_worker_protocol::ResourceRequirements>,
    volumes: Vec<distvirt_worker_protocol::VolumeSpec>,
    suspend_rx: mpsc::Receiver<SuspendRequest>,
    activity: Arc<distvirt_common::ActivityTracker>,
) {
    let result = {
        let _busy = activity.busy_guard();
        crate::pod::pod_launch(
            &*vmm,
            &*image_provider,
            &fabric,
            &kernel_path,
            &rootfs_image_path,
            &log_opener,
            &event_tx,
            &namespace_id,
            &pod_id,
            network,
            containers,
            resources,
            volumes,
            &cancel,
        )
        .await
    };
    let (result, held_resources): (anyhow::Result<_>, Box<dyn std::any::Any + Send>) = match result {
        Ok((vm, io, port, resources)) => (Ok((vm, io, port)), Box::new(resources)),
        Err(e) => (Err(e), Box::new(())),
    };
    run_pod_supervisor::<_, F>(
        result,
        held_resources,
        cancel,
        event_tx,
        namespace_id,
        pod_id,
        suspend_rx,
        "launch",
    )
    .await;
}

/// Top-level resume supervisor: restores a pod from a snapshot and monitors it.
///
/// Similar to `pod_supervisor` but calls `vmm.restore()` instead of launching fresh.
/// Prepares virtiofs shares (container rootfs via image provider, ConfigData via
/// snapshot metadata) so virtiofsd can serve them on the destination host.
pub(crate) async fn pod_resume_supervisor<
    V: Vmm + 'static,
    P: ImageProvider + 'static,
    F: crate::fs::Fs,
>(
    vmm: Arc<V>,
    image_provider: Arc<P>,
    fabric: Arc<Fabric<FabricPort>>,
    cancel: CancellationToken,
    event_tx: mpsc::Sender<WorkerEvent>,
    namespace_id: NamespaceId,
    pod_id: PodId,
    network: PodNetworkConfig,
    snapshot: SnapshotArtifacts,
    suspend_rx: mpsc::Receiver<SuspendRequest>,
    activity: Arc<distvirt_common::ActivityTracker>,
) {
    let (result, held_resources): (anyhow::Result<_>, Box<dyn std::any::Any + Send>) = {
        let _busy = activity.busy_guard();
        match crate::pod::pod_restore(
            &*vmm,
            &*image_provider,
            &fabric,
            &event_tx,
            &namespace_id,
            &pod_id,
            network,
            snapshot,
            &cancel,
        )
        .await
        {
            Ok((vm, port_task, resources)) => {
                (Ok((vm, None, port_task)), Box::new(resources))
            }
            Err(e) => (Err(e), Box::new(())),
        }
    };
    run_pod_supervisor::<_, F>(
        result,
        held_resources,
        cancel,
        event_tx,
        namespace_id,
        pod_id,
        suspend_rx,
        "resume",
    )
    .await;
}

/// Shared supervisor logic: on success emits `PodRunning` and delegates to `pod_monitor`;
/// on failure emits `PodExited` (if cancelled) or `PodFailed`.
///
/// `_held_resources` keeps RAII handles alive for the pod's lifetime (e.g.
/// `PodResources` from launch, which holds the containerd lease, overlay
/// mount, and volume temp directories). For restore, pass `()`.
async fn run_pod_supervisor<I: VmInstance, F: crate::fs::Fs>(
    setup_result: anyhow::Result<(
        ManagedVm<I>,
        Option<(crate::io_session::IoSession, yamux::Stream)>,
        Option<TaskHandle<()>>,
    )>,
    _held_resources: Box<dyn std::any::Any + Send>,
    cancel: CancellationToken,
    event_tx: mpsc::Sender<WorkerEvent>,
    namespace_id: NamespaceId,
    pod_id: PodId,
    suspend_rx: mpsc::Receiver<SuspendRequest>,
    phase: &str,
) {
    match setup_result {
        Ok((vm, io_session, port_task)) => {
            send_event(
                &event_tx,
                WorkerEvent::PodRunning {
                    namespace_id: namespace_id.clone(),
                    pod_id: pod_id.clone(),
                },
            )
            .await;
            pod_monitor::<_, F>(
                vm,
                io_session,
                port_task,
                cancel,
                event_tx,
                namespace_id,
                pod_id,
                suspend_rx,
            )
            .await;
        }
        Err(e) => {
            if cancel.is_cancelled() {
                log::info!("pod '{}': {} cancelled", pod_id, phase);
                send_event(
                    &event_tx,
                    WorkerEvent::PodExited {
                        namespace_id,
                        pod_id: pod_id.clone(),
                        exit_code: -1,
                    },
                )
                .await;
            } else {
                log::error!("pod '{}': {} failed: {:#}", pod_id, phase, e);
                send_event(
                    &event_tx,
                    WorkerEvent::PodFailed {
                        namespace_id,
                        pod_id: pod_id.clone(),
                        error: format!("{:#}", e),
                    },
                )
                .await;
            }
        }
    }
}

/// Timeout for suspend handshake with guest.
const SUSPEND_TIMEOUT: Duration = Duration::from_secs(10);

/// Timeout for waiting for the output stream to drain after container exit.
/// This is a safety valve — normally the drain completes promptly once the
/// container exits and the guest sends EOF on the yamux output stream.
const OUTPUT_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Wait for the log streaming task to complete (output stream EOF), with a timeout.
///
/// On success, all container output has been delivered to the log stream.
/// On timeout or error, some output may have been lost.
async fn await_log_drain(pod_id: &PodId, log_task: &mut Option<TaskHandle<bool>>) {
    let task = match log_task.take() {
        Some(t) => t,
        None => return,
    };
    match tokio::time::timeout(OUTPUT_DRAIN_TIMEOUT, task).await {
        Ok(Ok(true)) => {
            log::info!("pod '{}': output stream drained completely", pod_id);
        }
        Ok(Ok(false)) => {
            log::warn!("pod '{}': output stream ended before EOF (output may be incomplete)", pod_id);
        }
        Ok(Err(e)) => {
            log::warn!("pod '{}': log task panicked: {}", pod_id, e);
        }
        Err(_) => {
            log::warn!(
                "pod '{}': output drain timed out after {:?} (output may be incomplete)",
                pod_id, OUTPUT_DRAIN_TIMEOUT
            );
        }
    }
}

/// Pod monitor: watches a running pod's sub-tasks and handles cleanup.
///
/// This owns the `ManagedVm` and coordinates between container exit,
/// yamux driver health, log streaming, suspend requests, and cancellation.
async fn pod_monitor<I: VmInstance, F: crate::fs::Fs>(
    mut vm: ManagedVm<I>,
    io_session: Option<(crate::io_session::IoSession, yamux::Stream)>,
    port_task: Option<TaskHandle<()>>,
    cancel: CancellationToken,
    event_tx: mpsc::Sender<WorkerEvent>,
    namespace_id: NamespaceId,
    pod_id: PodId,
    mut suspend_rx: mpsc::Receiver<SuspendRequest>,
) {
    // Spawn log streaming as a sub-task. Returns whether output was fully
    // drained (EOF received) or incomplete (I/O error / log write failure).
    // On the normal and cancel exit paths we await this task before shutting
    // down the VM so all output is flushed. On fatal paths (VM crash, yamux
    // driver death) the task is dropped and aborted via TaskHandle.
    let mut log_task: Option<TaskHandle<bool>> = io_session.map(|(mut session, mut log_stream)| {
        let log_pod_id = pod_id.clone();
        TaskHandle::spawn(async move {
            let complete = loop {
                match session.next_event().await {
                    Ok(IoEvent::Stdout(seq, data)) | Ok(IoEvent::Stderr(seq, data)) => {
                        if distvirt_worker_protocol::codec::send_log_frame(
                            &mut log_stream,
                            seq,
                            &data,
                        )
                        .await
                        .is_err()
                        {
                            break false;
                        }
                    }
                    Ok(IoEvent::Eof) => break true,
                    Err(e) => {
                        log::warn!("pod '{}' log stream error: {:#}", log_pod_id, e);
                        break false;
                    }
                }
            };
            let _ = log_stream.close().await;
            complete
        })
    });

    // Take the event dispatch and get our own receiver for watching state.
    // _dispatch must be kept alive so the background task continues running.
    let _dispatch = vm.take_event_dispatch();
    let mut rx = match _dispatch {
        Some(ref d) => d.subscribe(),
        None => {
            // This shouldn't happen, but handle gracefully.
            log::error!("pod '{}': no event dispatch available", pod_id);
            let _ = vm.force_kill().await;
            send_event(
                &event_tx,
                WorkerEvent::PodFailed {
                    namespace_id,
                    pod_id,
                    error: "no event dispatch available".to_string(),
                },
            )
            .await;
            return;
        }
    };
    let mut last_balloon: Option<u32> = None;
    let mut last_memory_constrained: bool = false;
    let mut last_oom_kill_count: u64 = 0;

    // Take the driver exit signal so we can select on driver death without
    // moving the TaskHandle out of vm. drain_yamux_driver() still works.
    let mut driver_exit = vm.take_driver_exit_signal();
    let mut driver_exit_fut = std::pin::pin!(async {
        match driver_exit.as_mut() {
            Some(rx) => rx.await,
            None => std::future::pending().await,
        }
    });

    // Take the VM process exit signal so we can detect unexpected VM death.
    let mut vm_exit_rx = vm.take_exit_signal();
    let mut vm_exit_fut = std::pin::pin!(wait_for_vm_exit(&mut vm_exit_rx));

    // Create a future that completes when the port task exits, or pends forever if there is none.
    let mut port_task = port_task;
    let mut port_task_fut = std::pin::pin!(async {
        match port_task.as_mut() {
            Some(task) => {
                let _ = task.await;
            }
            None => std::future::pending::<()>().await,
        }
    });

    let event = loop {
        tokio::select! {
            // Event-driven path: react to state changes from EventDispatch.
            _ = rx.changed() => {
                let state = rx.borrow().clone();

                // 1. Fatal task error → force kill.
                if let Some((ref task, ref message)) = state.task_error {
                    log::error!("pod '{}': guest task error: task={}, message={}", pod_id, task, message);
                    let _ = vm.force_kill().await;
                    break WorkerEvent::PodFailed {
                        namespace_id: namespace_id.clone(),
                        pod_id: pod_id.clone(),
                        error: format!("guest task error: task={}, message={}", task, message),
                    };
                }

                // 2. Container exit → drain output, then graceful shutdown.
                if !state.exited.is_empty() {
                    // Use the first exited container's code as the pod exit code.
                    let (id, code) = state.exited.iter().next().unwrap();
                    let code = *code;
                    log::info!("pod '{}': container {} exited with code {}", pod_id, id, code);
                    drop(state);

                    // Wait for the output stream to drain (EOF) before tearing
                    // down the VM. This ensures all container output is delivered.
                    await_log_drain(&pod_id, &mut log_task).await;

                    match tokio::time::timeout(GRACEFUL_SHUTDOWN_TIMEOUT, vm.graceful_shutdown(Duration::from_secs(8), &mut rx)).await {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => {
                            log::warn!("pod '{}': shutdown error: {:#}, force killing", pod_id, e);
                            let _ = vm.force_kill().await;
                        }
                        Err(_) => {
                            log::warn!("pod '{}': shutdown timed out, force killing", pod_id);
                            let _ = vm.force_kill().await;
                        }
                    }
                    break WorkerEvent::PodExited {
                        namespace_id: namespace_id.clone(),
                        pod_id: pod_id.clone(),
                        exit_code: code,
                    };
                }

                // 3. Balloon adjustment.
                if state.balloon_mib != last_balloon {
                    if let Some(amount_mib) = state.balloon_mib {
                        log::info!("pod '{}': guest requests balloon={} MiB", pod_id, amount_mib);
                        if let Err(e) = vm.set_balloon(amount_mib).await {
                            log::warn!("pod '{}': set_balloon failed: {:#}", pod_id, e);
                        }
                    }
                    last_balloon = state.balloon_mib;
                }

                // 3b. Memory constraint transitions.
                if state.memory_constrained && !last_memory_constrained {
                    let reason = state.memory_constraint_reason
                        .map(|r| match r {
                            distvirt_guest_protocol::ConstraintReason::BalloonExhausted =>
                                distvirt_worker_protocol::MemoryConstraintReason::BalloonExhausted,
                            distvirt_guest_protocol::ConstraintReason::DeflationStalled =>
                                distvirt_worker_protocol::MemoryConstraintReason::DeflationStalled,
                        })
                        .unwrap_or(distvirt_worker_protocol::MemoryConstraintReason::BalloonExhausted);
                    send_event(
                        &event_tx,
                        WorkerEvent::PodMemoryConstrained {
                            namespace_id: namespace_id.clone(),
                            pod_id: pod_id.clone(),
                            reason,
                        },
                    ).await;
                } else if !state.memory_constrained && last_memory_constrained {
                    send_event(
                        &event_tx,
                        WorkerEvent::PodMemoryConstraintCleared {
                            namespace_id: namespace_id.clone(),
                            pod_id: pod_id.clone(),
                        },
                    ).await;
                }
                last_memory_constrained = state.memory_constrained;

                // 3c. OOM kill count increase.
                if state.oom_kill_count > last_oom_kill_count {
                    let delta = state.oom_kill_count - last_oom_kill_count;
                    send_event(
                        &event_tx,
                        WorkerEvent::PodOomKill {
                            namespace_id: namespace_id.clone(),
                            pod_id: pod_id.clone(),
                            count: delta,
                        },
                    ).await;
                    last_oom_kill_count = state.oom_kill_count;
                }

                // 4. Stream closed → force kill.
                if state.stream_closed {
                    let error = state.stream_error.clone()
                        .unwrap_or_else(|| "event stream closed unexpectedly".to_string());
                    log::error!("pod '{}': {}", pod_id, error);
                    let _ = vm.force_kill().await;
                    break WorkerEvent::PodFailed {
                        namespace_id: namespace_id.clone(),
                        pod_id: pod_id.clone(),
                        error,
                    };
                }
            }

            // Fatal: yamux driver died unexpectedly.
            result = &mut driver_exit_fut => {
                let error = match result {
                    Ok(Ok(())) => "yamux driver exited unexpectedly".to_string(),
                    Ok(Err(msg)) => format!("yamux driver error: {}", msg),
                    Err(_) => "yamux driver task dropped exit signal".to_string(),
                };
                log::error!("pod '{}': {}", pod_id, error);
                let _ = vm.force_kill().await;
                break WorkerEvent::PodFailed {
                    namespace_id: namespace_id.clone(),
                    pod_id: pod_id.clone(),
                    error,
                };
            }

            // Fatal: port read task died (TAP error, etc.).
            _ = &mut port_task_fut => {
                log::error!("pod '{}': port task exited, network dead — force killing VM", pod_id);
                let _ = vm.force_kill().await;
                break WorkerEvent::PodFailed {
                    namespace_id: namespace_id.clone(),
                    pod_id: pod_id.clone(),
                    error: "port task exited unexpectedly".to_string(),
                };
            }

            // Fatal: VM process exited unexpectedly.
            _ = &mut vm_exit_fut => {
                log::error!("pod '{}': VM process exited unexpectedly", pod_id);
                vm.drain_yamux_driver();
                break WorkerEvent::PodFailed {
                    namespace_id: namespace_id.clone(),
                    pod_id: pod_id.clone(),
                    error: "VM process exited unexpectedly".to_string(),
                };
            }

            // Suspend request: snapshot the VM and exit.
            Some(req) = suspend_rx.recv() => {
                log::info!("pod '{}': suspend requested, artifact_id={}", pod_id, req.artifact_id);
                // Emit ArtifactWriteStarted before beginning the snapshot write.
                send_event(
                    &event_tx,
                    WorkerEvent::ArtifactWriteStarted {
                        namespace_id: namespace_id.clone(),
                        artifact_id: req.artifact_id.clone(),
                        pool_id: req.pool_id.clone(),
                    },
                )
                .await;
                match vm.suspend(&req.snapshot_dir, SUSPEND_TIMEOUT).await {
                    Ok(artifacts) => {
                        // Calculate snapshot size.
                        let artifact_size_bytes = match F::dir_size(&req.snapshot_dir).await {
                            Ok(size) => size,
                            Err(e) => {
                                log::warn!("pod '{}': failed to calculate artifact size: {:#}", pod_id, e);
                                0
                            }
                        };
                        let _ = req.reply.send(Ok(artifacts));
                        // Emit ArtifactWriteCommitted now that snapshot is durable.
                        send_event(
                            &event_tx,
                            WorkerEvent::ArtifactWriteCommitted {
                                namespace_id: namespace_id.clone(),
                                artifact_id: req.artifact_id.clone(),
                                pool_id: req.pool_id.clone(),
                                size_bytes: artifact_size_bytes,
                            },
                        )
                        .await;
                        send_event(
                            &event_tx,
                            WorkerEvent::PodSuspended {
                                namespace_id: namespace_id.clone(),
                                pod_id: pod_id.clone(),
                                artifact_id: req.artifact_id,
                                artifact_size_bytes,
                                pool_id: req.pool_id,
                            },
                        )
                        .await;
                        return; // VM is dead after suspend, exit monitor.
                    }
                    Err(e) => {
                        let err_msg = format!("{:#}", e);
                        log::error!("pod '{}': suspend failed: {}", pod_id, err_msg);
                        let _ = req.reply.send(Err(err_msg.clone()));
                        let _ = vm.force_kill().await;
                        break WorkerEvent::PodSuspendFailed {
                            namespace_id: namespace_id.clone(),
                            pod_id: pod_id.clone(),
                            error: err_msg,
                        };
                    }
                }
            }

            // Cancellation: stop containers, drain output, then shutdown VM.
            _ = cancel.cancelled() => {
                log::info!("pod '{}': cancellation received, shutting down gracefully", pod_id);

                // Phase 1: SIGTERM containers and wait for them to exit.
                vm.stop_containers(Duration::from_secs(8), &mut rx).await;

                // Phase 2: Wait for output stream to drain.
                await_log_drain(&pod_id, &mut log_task).await;

                // Phase 3: Shutdown VM.
                match tokio::time::timeout(GRACEFUL_SHUTDOWN_TIMEOUT, vm.shutdown()).await {
                    Ok(Ok(())) => {
                        log::info!("pod '{}': graceful shutdown complete", pod_id);
                    }
                    Ok(Err(e)) => {
                        log::warn!("pod '{}': shutdown error: {:#}, force killing", pod_id, e);
                        let _ = vm.force_kill().await;
                    }
                    Err(_) => {
                        log::warn!("pod '{}': shutdown timed out, force killing", pod_id);
                        let _ = vm.force_kill().await;
                    }
                }
                break WorkerEvent::PodExited {
                    namespace_id: namespace_id.clone(),
                    pod_id: pod_id.clone(),
                    exit_code: -1,
                };
            }
        }
    };

    // log_task is dropped here. On normal/cancel paths it was already awaited
    // (and is None). On fatal paths it's aborted via TaskHandle::drop.

    // Send the event back to the main loop.
    send_event(&event_tx, event).await;
}

/// Calculate the total size of files in a directory (recursive).
pub(crate) async fn dir_size(path: &std::path::Path) -> anyhow::Result<u64> {
    let mut total = 0u64;
    let mut stack = vec![path.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let mut entries = tokio::fs::read_dir(&dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let meta = entry.metadata().await?;
            if meta.is_file() {
                total += meta.len();
            } else if meta.is_dir() {
                stack.push(entry.path());
            }
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;
    use std::path::PathBuf;

    use distvirt_worker_protocol::{ContainerConfig, ContainerSpec, PodNetworkConfig};
    use tokio::net::UnixStream;

    use crate::fabric::{Fabric, FabricPort};
    use crate::image_provider::{ImageProvider, PreparedArtifact};
    use crate::vmm::{
        BaseVmConfig, MountRequest, MountRestoreInfo, PlannedMount, ProvidedAccess,
        ResolvedEntry, ResolvedMounts, GuestDevice, VmBuilder, VmInstance, Vmm,
    };

    // -----------------------------------------------------------------------
    // Stubs & Mocks
    // -----------------------------------------------------------------------

    struct StubVmm;

    struct StubVmmBuilder;

    impl VmBuilder for StubVmmBuilder {
        type Instance = StubVmInstance;
        fn add_mount(&mut self, _request: MountRequest) -> anyhow::Result<PlannedMount> {
            panic!("StubVmmBuilder::add_mount should not be called");
        }
        fn add_scratch_device(&mut self, _tag: &str, _size_mib: u32) -> anyhow::Result<()> {
            panic!("StubVmmBuilder::add_scratch_device should not be called");
        }
        fn set_snapshot_context(&mut self, _mount_restore_info: Vec<MountRestoreInfo>) {
            panic!("StubVmmBuilder::set_snapshot_context should not be called");
        }
        async fn launch(self) -> anyhow::Result<(StubVmInstance, ResolvedMounts)> {
            panic!("StubVmmBuilder::launch should not be called");
        }
    }

    impl Vmm for StubVmm {
        type Builder = StubVmmBuilder;
        type Instance = StubVmInstance;
        fn builder(&self, _base: BaseVmConfig) -> anyhow::Result<StubVmmBuilder> {
            Ok(StubVmmBuilder)
        }
    }

    struct StubVmInstance;

    impl VmInstance for StubVmInstance {
        async fn connect_vsock(&self, _port: u32) -> anyhow::Result<UnixStream> {
            panic!("StubVmInstance::connect_vsock called");
        }
        fn take_fabric_port(&mut self) -> Option<FabricPort> {
            None
        }
        async fn wait(&mut self) -> anyhow::Result<std::process::ExitStatus> {
            std::future::pending().await
        }
        async fn kill(&mut self) -> anyhow::Result<()> {
            Ok(())
        }
    }

    struct MockVmm {
        /// If Some, builder/launch returns this error.
        launch_error: Option<String>,
        /// The mock VM's vsock socket (worker side).
        vm_socket: tokio::sync::Mutex<Option<UnixStream>>,
    }

    struct MockVmmBuilder {
        launch_error: Option<String>,
        vm_socket: Option<UnixStream>,
    }

    impl VmBuilder for MockVmmBuilder {
        type Instance = MockVmInstance;
        fn add_mount(&mut self, request: MountRequest) -> anyhow::Result<PlannedMount> {
            // Return a VirtioFs read-only plan for "container", BlockDevice for volumes.
            let provided = if request.tag == "container" {
                ProvidedAccess::VirtioFs { read_only: true }
            } else {
                ProvidedAccess::BlockDevice { read_only: false }
            };
            Ok(PlannedMount {
                tag: request.tag,
                provided,
            })
        }
        fn add_scratch_device(&mut self, _tag: &str, _size_mib: u32) -> anyhow::Result<()> {
            Ok(())
        }
        fn set_snapshot_context(&mut self, _mount_restore_info: Vec<MountRestoreInfo>) {}
        async fn launch(self) -> anyhow::Result<(MockVmInstance, ResolvedMounts)> {
            if let Some(ref err) = self.launch_error {
                return Err(anyhow::anyhow!("{}", err));
            }
            let socket = self
                .vm_socket
                .expect("MockVmmBuilder: socket already taken");
            let instance = MockVmInstance {
                vsock_socket: tokio::sync::Mutex::new(Some(socket)),
                killed: tokio::sync::Mutex::new(false),
            };
            let resolved = ResolvedMounts {
                entries: vec![
                    ResolvedEntry {
                        tag: "container".to_string(),
                        guest: GuestDevice::VirtioFs {
                            virtiofs_tag: "container-rootfs".to_string(),
                        },
                    },
                    ResolvedEntry {
                        tag: "container-overlay".to_string(),
                        guest: GuestDevice::Device {
                            path: "/dev/vdb".to_string(),
                        },
                    },
                ],
            };
            Ok((instance, resolved))
        }
    }

    struct MockVmInstance {
        vsock_socket: tokio::sync::Mutex<Option<UnixStream>>,
        killed: tokio::sync::Mutex<bool>,
    }

    impl Vmm for MockVmm {
        type Builder = MockVmmBuilder;
        type Instance = MockVmInstance;
        fn builder(&self, _base: BaseVmConfig) -> anyhow::Result<MockVmmBuilder> {
            // We need to take the socket synchronously. Use try_lock since
            // we know no contention exists in tests.
            let socket = self
                .vm_socket
                .try_lock()
                .expect("MockVmm: lock contention")
                .take();
            Ok(MockVmmBuilder {
                launch_error: self.launch_error.clone(),
                vm_socket: socket,
            })
        }
    }

    impl VmInstance for MockVmInstance {
        async fn connect_vsock(&self, _port: u32) -> anyhow::Result<UnixStream> {
            self.vsock_socket
                .lock()
                .await
                .take()
                .ok_or_else(|| anyhow::anyhow!("MockVmInstance: vsock already connected"))
        }
        fn take_fabric_port(&mut self) -> Option<FabricPort> {
            None
        }
        async fn wait(&mut self) -> anyhow::Result<std::process::ExitStatus> {
            std::future::pending().await
        }
        async fn kill(&mut self) -> anyhow::Result<()> {
            *self.killed.lock().await = true;
            Ok(())
        }
    }

    struct FailingImageProvider {
        error_msg: String,
    }

    impl ImageProvider for FailingImageProvider {
        async fn prepare(&self, _image_ref: &str) -> anyhow::Result<PreparedArtifact> {
            Err(anyhow::anyhow!("{}", self.error_msg))
        }
    }

    struct MockImageProvider;

    impl ImageProvider for MockImageProvider {
        async fn prepare(&self, _image_ref: &str) -> anyhow::Result<PreparedArtifact> {
            Ok(PreparedArtifact::Directory {
                path: PathBuf::from("/fake/image.ext4"),
                oci_config: None,
                _cleanup: None,
            })
        }
    }

    fn make_pod_network() -> PodNetworkConfig {
        PodNetworkConfig {
            ip: Ipv4Addr::new(172, 16, 0, 10),
            mac: [0x02, 0, 0, 0, 0, 0x10],
            gateway: Ipv4Addr::new(172, 16, 0, 1),
            netmask: "255.255.255.0".to_string(),
        }
    }

    fn make_containers() -> Vec<ContainerSpec> {
        vec![ContainerSpec {
            container_id: "main".to_string(),
            image_ref: "test-image:latest".to_string(),
            config: ContainerConfig {
                command: Some(vec!["/bin/echo".to_string()]),
                args: Some(vec!["hello".to_string()]),
                env: vec![],
                working_dir: None,
                user: None,
                hostname: None,
                capture_output: false,
                stdin: false,
                volume_mounts: vec![],
            },
        }]
    }

    fn make_log_opener() -> LogStreamOpener {
        LogStreamOpener::disconnected()
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    const POD_ID: PodId = PodId(11);

    #[tokio::test]
    async fn image_provider_failure_sends_pod_failed() {
        let (bg_event_tx, mut bg_event_rx) = mpsc::channel(256);
        let image_provider = Arc::new(FailingImageProvider {
            error_msg: "image not found".to_string(),
        });
        let vmm = Arc::new(StubVmm);
        let fabric = Arc::new(Fabric::<FabricPort>::new(Ipv4Addr::new(172, 16, 0, 0), 16));
        let cancel = CancellationToken::new();

        let log_opener = make_log_opener();

        let ns_id = NamespaceId::from("ns1");
        let pod_id = POD_ID;

        // Run pod_supervisor directly.
        tokio::spawn({
            let ns_id = ns_id.clone();
            let pod_id = pod_id.clone();
            let cancel = cancel.clone();
            async move {
                let (_suspend_tx, suspend_rx) = mpsc::channel(1);
                pod_supervisor::<_, _, crate::fs::SyncFs>(
                    vmm,
                    image_provider,
                    fabric,
                    PathBuf::from("/fake/kernel"),
                    PathBuf::from("/fake/rootfs"),
                    log_opener,
                    cancel,
                    bg_event_tx,
                    ns_id,
                    pod_id,
                    make_pod_network(),
                    make_containers(),
                    None,
                    vec![],
                    suspend_rx,
                    Arc::new(distvirt_common::ActivityTracker::new()),
                )
                .await;
            }
        });

        // Should receive PodFailed.
        let event = tokio::time::timeout(Duration::from_secs(5), bg_event_rx.recv())
            .await
            .expect("timeout waiting for event")
            .expect("channel closed");

        match event {
            WorkerEvent::PodFailed {
                namespace_id,
                pod_id,
                error,
            } => {
                assert_eq!(namespace_id, "ns1");
                assert_eq!(pod_id, POD_ID);
                assert!(
                    error.contains("image not found"),
                    "error should mention image failure: {}",
                    error
                );
            }
            other => panic!("expected PodFailed, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn vm_launch_failure_sends_pod_failed() {
        let (worker_socket, _guest_socket) = UnixStream::pair().unwrap();
        let vmm = Arc::new(MockVmm {
            launch_error: Some("VM exploded".to_string()),
            vm_socket: tokio::sync::Mutex::new(Some(worker_socket)),
        });
        let image_provider = Arc::new(MockImageProvider);
        let fabric = Arc::new(Fabric::<FabricPort>::new(Ipv4Addr::new(172, 16, 0, 0), 16));
        let cancel = CancellationToken::new();

        let log_opener = make_log_opener();
        let (bg_event_tx, mut bg_event_rx) = mpsc::channel(256);

        let ns_id = NamespaceId::from("ns1");
        let pod_id = POD_ID;

        tokio::spawn({
            let ns_id = ns_id.clone();
            let pod_id = pod_id.clone();
            let cancel = cancel.clone();
            async move {
                let (_suspend_tx, suspend_rx) = mpsc::channel(1);
                pod_supervisor::<_, _, crate::fs::SyncFs>(
                    vmm,
                    image_provider,
                    fabric,
                    PathBuf::from("/fake/kernel"),
                    PathBuf::from("/fake/rootfs"),
                    log_opener,
                    cancel,
                    bg_event_tx,
                    ns_id,
                    pod_id,
                    make_pod_network(),
                    make_containers(),
                    None,
                    vec![],
                    suspend_rx,
                    Arc::new(distvirt_common::ActivityTracker::new()),
                )
                .await;
            }
        });

        let event = tokio::time::timeout(Duration::from_secs(5), bg_event_rx.recv())
            .await
            .expect("timeout waiting for event")
            .expect("channel closed");

        match event {
            WorkerEvent::PodFailed { error, .. } => {
                assert!(
                    error.contains("VM exploded"),
                    "error should mention VM failure: {}",
                    error
                );
            }
            other => panic!("expected PodFailed, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn cancel_during_image_prepare_sends_pod_exited() {
        // Use a slow image provider that waits forever.
        struct HangingImageProvider;
        impl ImageProvider for HangingImageProvider {
            async fn prepare(&self, _image_ref: &str) -> anyhow::Result<PreparedArtifact> {
                std::future::pending().await
            }
        }

        let vmm = Arc::new(StubVmm);
        let image_provider = Arc::new(HangingImageProvider);
        let fabric = Arc::new(Fabric::<FabricPort>::new(Ipv4Addr::new(172, 16, 0, 0), 16));
        let cancel = CancellationToken::new();

        let log_opener = make_log_opener();
        let (bg_event_tx, mut bg_event_rx) = mpsc::channel(256);

        let cancel_clone = cancel.clone();
        tokio::spawn(async move {
            let (_suspend_tx, suspend_rx) = mpsc::channel(1);
            pod_supervisor::<_, _, crate::fs::SyncFs>(
                vmm,
                image_provider,
                fabric,
                PathBuf::from("/fake/kernel"),
                PathBuf::from("/fake/rootfs"),
                log_opener,
                cancel_clone,
                bg_event_tx,
                NamespaceId::from("ns1"),
                POD_ID,
                make_pod_network(),
                make_containers(),
                None,
                vec![],
                suspend_rx,
                Arc::new(distvirt_common::ActivityTracker::new()),
            )
            .await;
        });

        // Cancel after a short delay.
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();

        let event = tokio::time::timeout(Duration::from_secs(5), bg_event_rx.recv())
            .await
            .expect("timeout")
            .expect("channel closed");

        match event {
            WorkerEvent::PodExited { exit_code, .. } => {
                assert_eq!(exit_code, -1, "cancelled pod should exit with -1");
            }
            other => panic!("expected PodExited(-1), got {:?}", other),
        }
    }

    #[tokio::test]
    async fn pod_lifecycle_with_test_vmm() {
        use crate::vmm::guest_sim::ContainerBehavior;
        use crate::vmm::test_vmm::TestVmm;

        let vmm = Arc::new(TestVmm::new(ContainerBehavior::ExitImmediately(0)));
        let image_provider = Arc::new(MockImageProvider);
        let fabric = Arc::new(Fabric::<FabricPort>::new(Ipv4Addr::new(172, 16, 0, 1), 24));
        let cancel = CancellationToken::new();
        let log_opener = make_log_opener();
        let (bg_event_tx, mut bg_event_rx) = mpsc::channel(256);

        let ns_id = NamespaceId::from("ns1");
        let pod_id = POD_ID;

        tokio::spawn({
            let ns_id = ns_id.clone();
            let pod_id = pod_id.clone();
            let cancel = cancel.clone();
            async move {
                let (_suspend_tx, suspend_rx) = mpsc::channel(1);
                pod_supervisor::<_, _, crate::fs::SyncFs>(
                    vmm,
                    image_provider,
                    fabric,
                    PathBuf::from("/fake/kernel"),
                    PathBuf::from("/fake/rootfs"),
                    log_opener,
                    cancel,
                    bg_event_tx,
                    ns_id,
                    pod_id,
                    make_pod_network(),
                    make_containers(),
                    None,
                    vec![],
                    suspend_rx,
                    Arc::new(distvirt_common::ActivityTracker::new()),
                )
                .await;
            }
        });

        // First event should be PodRunning.
        let event = tokio::time::timeout(Duration::from_secs(10), bg_event_rx.recv())
            .await
            .expect("timeout waiting for PodRunning")
            .expect("channel closed");
        match event {
            WorkerEvent::PodRunning {
                namespace_id,
                pod_id,
            } => {
                assert_eq!(namespace_id, "ns1");
                assert_eq!(pod_id, POD_ID);
            }
            other => panic!("expected PodRunning, got {:?}", other),
        }

        // Second event should be PodExited with code 0.
        let event = tokio::time::timeout(Duration::from_secs(10), bg_event_rx.recv())
            .await
            .expect("timeout waiting for PodExited")
            .expect("channel closed");
        match event {
            WorkerEvent::PodExited {
                namespace_id,
                pod_id,
                exit_code,
            } => {
                assert_eq!(namespace_id, "ns1");
                assert_eq!(pod_id, POD_ID);
                assert_eq!(exit_code, 0);
            }
            other => panic!("expected PodExited(0), got {:?}", other),
        }
    }
}
