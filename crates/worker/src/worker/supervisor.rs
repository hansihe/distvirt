use std::path::PathBuf;
use std::process::ExitStatus;
use std::sync::Arc;
use std::time::Duration;

use distvirt_worker_protocol::{
    ArtifactId, ContainerSpec, LogStreamOpener, NamespaceId, PodId,
    PodNetworkConfig, PoolId, WorkerEvent,
};
use futures_lite::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

use crate::fabric::{Fabric, FabricPort};
use crate::image_provider::ImageProvider;
use crate::io_session::IoEvent;
use crate::managed_vm::{EventDispatchState, ManagedVm};
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
    let busy = activity.busy_guard();
    let result = crate::pod::pod_launch(
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
    .await;
    let (result, held_resources): (anyhow::Result<_>, Box<dyn std::any::Any + Send>) = match result {
        Ok((vm, io, port, resources)) => (Ok((vm, io, port)), Box::new(resources)),
        Err(e) => (Err(e), Box::new(())),
    };
    run_pod_supervisor::<_, F>(
        result,
        held_resources,
        busy,
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
    let busy = activity.busy_guard();
    let (result, held_resources): (anyhow::Result<_>, Box<dyn std::any::Any + Send>) =
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
        };
    run_pod_supervisor::<_, F>(
        result,
        held_resources,
        busy,
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
    launch_busy: distvirt_common::BusyGuard<'_>,
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
            drop(launch_busy);
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


/// Outcome from processing guest state changes.
enum MonitorOutcome {
    /// A container exited normally.
    ContainerExited { exit_code: i32 },
    /// A fatal error occurred (task error or stream closed).
    Fatal { error: String },
}

/// Events that the pod monitor loop reacts to.
enum PodEvent {
    GuestStateChanged,
    DriverExited(Result<Result<(), String>, oneshot::error::RecvError>),
    PortTaskExited,
    VmExited,
    SuspendRequested(SuspendRequest),
    Cancelled,
}

/// What caused the monitor loop to exit.
enum LoopOutcome {
    ContainerExited { exit_code: i32 },
    Fatal { error: String },
    VmExited,
    SuspendRequested(SuspendRequest),
    Cancelled,
}

/// Tracks guest state changes and detects transitions for telemetry events.
#[derive(Default)]
struct StateTracker {
    last_balloon: Option<u32>,
    last_memory_constrained: bool,
    last_oom_kill_count: u64,
}

impl StateTracker {
    /// Process a guest state snapshot. Returns `Some` if the pod should exit,
    /// `None` if the pod should keep running (non-fatal telemetry was handled).
    async fn process<I: VmInstance>(
        &mut self,
        state: &EventDispatchState,
        vm: &mut ManagedVm<I>,
        event_tx: &mpsc::Sender<WorkerEvent>,
        namespace_id: &NamespaceId,
        pod_id: &PodId,
    ) -> Option<MonitorOutcome> {
        // 1. Fatal task error.
        if let Some((ref task, ref message)) = state.task_error {
            log::error!(
                "pod '{}': guest task error: task={}, message={}",
                pod_id, task, message
            );
            return Some(MonitorOutcome::Fatal {
                error: format!("guest task error: task={}, message={}", task, message),
            });
        }

        // 2. Container exit.
        if !state.exited.is_empty() {
            let (id, code) = state.exited.iter().next().unwrap();
            let code = *code;
            log::info!("pod '{}': container {} exited with code {}", pod_id, id, code);
            return Some(MonitorOutcome::ContainerExited { exit_code: code });
        }

        // 3. Balloon adjustment.
        if state.balloon_mib != self.last_balloon {
            if let Some(amount_mib) = state.balloon_mib {
                log::info!("pod '{}': guest requests balloon={} MiB", pod_id, amount_mib);
                if let Err(e) = vm.set_balloon(amount_mib).await {
                    log::warn!("pod '{}': set_balloon failed: {:#}", pod_id, e);
                }
            }
            self.last_balloon = state.balloon_mib;
        }

        // 4. Memory constraint transitions.
        if state.memory_constrained && !self.last_memory_constrained {
            let reason = state
                .memory_constraint_reason
                .map(|r| match r {
                    distvirt_guest_protocol::ConstraintReason::BalloonExhausted => {
                        distvirt_worker_protocol::MemoryConstraintReason::BalloonExhausted
                    }
                    distvirt_guest_protocol::ConstraintReason::DeflationStalled => {
                        distvirt_worker_protocol::MemoryConstraintReason::DeflationStalled
                    }
                })
                .unwrap_or(distvirt_worker_protocol::MemoryConstraintReason::BalloonExhausted);
            send_event(
                event_tx,
                WorkerEvent::PodMemoryConstrained {
                    namespace_id: namespace_id.clone(),
                    pod_id: pod_id.clone(),
                    reason,
                },
            )
            .await;
        } else if !state.memory_constrained && self.last_memory_constrained {
            send_event(
                event_tx,
                WorkerEvent::PodMemoryConstraintCleared {
                    namespace_id: namespace_id.clone(),
                    pod_id: pod_id.clone(),
                },
            )
            .await;
        }
        self.last_memory_constrained = state.memory_constrained;

        // 5. OOM kill count increase.
        if state.oom_kill_count > self.last_oom_kill_count {
            let delta = state.oom_kill_count - self.last_oom_kill_count;
            send_event(
                event_tx,
                WorkerEvent::PodOomKill {
                    namespace_id: namespace_id.clone(),
                    pod_id: pod_id.clone(),
                    count: delta,
                },
            )
            .await;
            self.last_oom_kill_count = state.oom_kill_count;
        }

        // 6. Stream closed.
        if state.stream_closed {
            let error = state
                .stream_error
                .clone()
                .unwrap_or_else(|| "event stream closed unexpectedly".to_string());
            log::error!("pod '{}': {}", pod_id, error);
            return Some(MonitorOutcome::Fatal { error });
        }

        None
    }
}

/// Pod monitor: watches a running pod's sub-tasks and handles cleanup.
///
/// This owns the `ManagedVm` and coordinates between container exit,
/// yamux driver health, log streaming, suspend requests, and cancellation.
///
/// Structured in two phases:
/// 1. **Event loop** — multiplexes all signal sources into `PodEvent`, dispatches
///    through `StateTracker` for guest state, and breaks with a `LoopOutcome`.
/// 2. **Outcome handling** — each outcome delegates to a focused handler that
///    manages the multi-step cleanup (drain, shutdown, snapshot, etc.), racing
///    against unexpected VM death where appropriate.
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

    // Take signals out of vm so we can select on them without borrowing vm.
    // Declared outside the loop scope so handlers can reuse them after the
    // pinned futures are dropped.
    let mut driver_exit = vm.take_driver_exit_signal();
    let mut vm_exit_rx = vm.exit_signal();
    let mut port_task = port_task;
    let mut tracker = StateTracker::default();

    // Phase 1: Event loop — determine what happened.
    //
    // Pinned one-shot futures are scoped in a block so they're dropped when
    // the loop breaks, freeing the underlying receivers for the exit handlers.
    let outcome = {
        let mut driver_exit_fut = std::pin::pin!(async {
            match driver_exit.as_mut() {
                Some(rx) => rx.await,
                None => std::future::pending().await,
            }
        });
        let mut vm_exit_fut = std::pin::pin!(wait_for_vm_exit(&mut vm_exit_rx));
        let mut port_task_fut = std::pin::pin!(async {
            match port_task.as_mut() {
                Some(task) => {
                    let _ = task.await;
                }
                None => std::future::pending::<()>().await,
            }
        });

        loop {
            let pod_event = tokio::select! {
                _ = rx.changed() => PodEvent::GuestStateChanged,
                result = &mut driver_exit_fut => PodEvent::DriverExited(result),
                _ = &mut port_task_fut => PodEvent::PortTaskExited,
                _ = &mut vm_exit_fut => PodEvent::VmExited,
                Some(req) = suspend_rx.recv() => PodEvent::SuspendRequested(req),
                _ = cancel.cancelled() => PodEvent::Cancelled,
            };

            match pod_event {
                PodEvent::GuestStateChanged => {
                    let state = rx.borrow().clone();
                    match tracker
                        .process(&state, &mut vm, &event_tx, &namespace_id, &pod_id)
                        .await
                    {
                        Some(MonitorOutcome::ContainerExited { exit_code }) => {
                            break LoopOutcome::ContainerExited { exit_code };
                        }
                        Some(MonitorOutcome::Fatal { error }) => {
                            break LoopOutcome::Fatal { error };
                        }
                        None => {}
                    }
                }
                PodEvent::DriverExited(result) => {
                    let error = match result {
                        Ok(Ok(())) => "yamux driver exited unexpectedly".to_string(),
                        Ok(Err(msg)) => format!("yamux driver error: {}", msg),
                        Err(_) => "yamux driver task dropped exit signal".to_string(),
                    };
                    log::error!("pod '{}': {}", pod_id, error);
                    break LoopOutcome::Fatal { error };
                }
                PodEvent::PortTaskExited => {
                    log::error!(
                        "pod '{}': port task exited, network dead — force killing VM",
                        pod_id
                    );
                    break LoopOutcome::Fatal {
                        error: "port task exited unexpectedly".to_string(),
                    };
                }
                PodEvent::VmExited => {
                    log::error!("pod '{}': VM process exited unexpectedly", pod_id);
                    break LoopOutcome::VmExited;
                }
                PodEvent::SuspendRequested(req) => {
                    break LoopOutcome::SuspendRequested(req);
                }
                PodEvent::Cancelled => {
                    break LoopOutcome::Cancelled;
                }
            }
        }
    };

    // Phase 2: Handle the outcome. Each arm either produces a WorkerEvent
    // to send, or returns early (suspend success sends its own events).
    //
    // Suspend is handled first because it consumes `vm`. The remaining arms
    // borrow `vm` mutably.
    if let LoopOutcome::SuspendRequested(req) = outcome {
        match handle_suspend::<_, F>(vm, req, &event_tx, &namespace_id, &pod_id).await {
            Some(event) => {
                send_event(&event_tx, event).await;
            }
            None => {} // Successful suspend, events already sent.
        }
        return;
    }

    let event = match outcome {
        LoopOutcome::ContainerExited { exit_code } => {
            handle_container_exit(
                &mut vm,
                exit_code,
                &mut log_task,
                &mut vm_exit_rx,
                &pod_id,
                &namespace_id,
                &mut rx,
            )
            .await
        }
        LoopOutcome::Fatal { error } => {
            let _ = vm.force_kill().await;
            WorkerEvent::PodFailed {
                namespace_id: namespace_id.clone(),
                pod_id: pod_id.clone(),
                error,
            }
        }
        LoopOutcome::VmExited => {
            vm.drain_yamux_driver();
            WorkerEvent::PodFailed {
                namespace_id: namespace_id.clone(),
                pod_id: pod_id.clone(),
                error: "VM process exited unexpectedly".to_string(),
            }
        }
        LoopOutcome::SuspendRequested(_) => unreachable!(),
        LoopOutcome::Cancelled => {
            handle_cancel(
                &mut vm,
                &mut rx,
                &mut log_task,
                &mut vm_exit_rx,
                &pod_id,
                &namespace_id,
            )
            .await
        }
    };

    send_event(&event_tx, event).await;
}

/// Handle container exit: drain output and gracefully shut down the VM,
/// racing each phase against unexpected VM death.
async fn handle_container_exit<I: VmInstance>(
    vm: &mut ManagedVm<I>,
    exit_code: i32,
    log_task: &mut Option<TaskHandle<bool>>,
    vm_exit_rx: &mut watch::Receiver<Option<ExitStatus>>,
    pod_id: &PodId,
    namespace_id: &NamespaceId,
    rx: &mut watch::Receiver<EventDispatchState>,
) -> WorkerEvent {
    // Drain output, racing against VM death.
    let vm_died = tokio::select! {
        _ = await_log_drain(pod_id, log_task) => false,
        _ = wait_for_vm_exit(vm_exit_rx) => true,
    };
    if vm_died {
        log::warn!("pod '{}': VM exited during log drain", pod_id);
        vm.drain_yamux_driver();
        return WorkerEvent::PodExited {
            namespace_id: namespace_id.clone(),
            pod_id: pod_id.clone(),
            exit_code,
        };
    }

    // Graceful shutdown, racing against VM death.
    let result = tokio::select! {
        result = tokio::time::timeout(
            GRACEFUL_SHUTDOWN_TIMEOUT,
            vm.graceful_shutdown(Duration::from_secs(8), rx),
        ) => Some(result),
        _ = wait_for_vm_exit(vm_exit_rx) => None,
    };
    match result {
        Some(Ok(Ok(()))) => {}
        Some(Ok(Err(e))) => {
            log::warn!("pod '{}': shutdown error: {:#}, force killing", pod_id, e);
            let _ = vm.force_kill().await;
        }
        Some(Err(_)) => {
            log::warn!("pod '{}': shutdown timed out, force killing", pod_id);
            let _ = vm.force_kill().await;
        }
        None => {
            log::warn!("pod '{}': VM exited during graceful shutdown", pod_id);
            vm.drain_yamux_driver();
        }
    }

    WorkerEvent::PodExited {
        namespace_id: namespace_id.clone(),
        pod_id: pod_id.clone(),
        exit_code,
    }
}

/// Handle cancellation: stop containers, drain output, then shut down the VM.
/// Each phase races against unexpected VM death.
async fn handle_cancel<I: VmInstance>(
    vm: &mut ManagedVm<I>,
    rx: &mut watch::Receiver<EventDispatchState>,
    log_task: &mut Option<TaskHandle<bool>>,
    vm_exit_rx: &mut watch::Receiver<Option<ExitStatus>>,
    pod_id: &PodId,
    namespace_id: &NamespaceId,
) -> WorkerEvent {
    log::info!(
        "pod '{}': cancellation received, shutting down gracefully",
        pod_id
    );

    // Phase 1: SIGTERM containers and wait for exit.
    vm.stop_containers(Duration::from_secs(8), rx).await;

    // Phase 2: Drain output, racing against VM death.
    let vm_died = tokio::select! {
        _ = await_log_drain(pod_id, log_task) => false,
        _ = wait_for_vm_exit(vm_exit_rx) => true,
    };
    if vm_died {
        log::warn!("pod '{}': VM exited during log drain", pod_id);
        vm.drain_yamux_driver();
        return WorkerEvent::PodExited {
            namespace_id: namespace_id.clone(),
            pod_id: pod_id.clone(),
            exit_code: -1,
        };
    }

    // Phase 3: Shutdown VM, racing against VM death.
    let result = tokio::select! {
        result = tokio::time::timeout(GRACEFUL_SHUTDOWN_TIMEOUT, vm.shutdown()) => Some(result),
        _ = wait_for_vm_exit(vm_exit_rx) => None,
    };
    match result {
        Some(Ok(Ok(()))) => {
            log::info!("pod '{}': graceful shutdown complete", pod_id);
        }
        Some(Ok(Err(e))) => {
            log::warn!("pod '{}': shutdown error: {:#}, force killing", pod_id, e);
            let _ = vm.force_kill().await;
        }
        Some(Err(_)) => {
            log::warn!("pod '{}': shutdown timed out, force killing", pod_id);
            let _ = vm.force_kill().await;
        }
        None => {
            log::warn!("pod '{}': VM exited during shutdown", pod_id);
            vm.drain_yamux_driver();
        }
    }

    WorkerEvent::PodExited {
        namespace_id: namespace_id.clone(),
        pod_id: pod_id.clone(),
        exit_code: -1,
    }
}

/// Handle suspend request: snapshot the VM and emit artifact events.
/// Returns `None` on success (events sent inline), or `Some(event)` on failure.
///
/// Takes `vm` by value — on success the VM is consumed by `suspend()`.
/// On failure, `vm` is dropped, which kills the child process via `Drop`.
async fn handle_suspend<I: VmInstance, F: crate::fs::Fs>(
    vm: ManagedVm<I>,
    req: SuspendRequest,
    event_tx: &mpsc::Sender<WorkerEvent>,
    namespace_id: &NamespaceId,
    pod_id: &PodId,
) -> Option<WorkerEvent> {
    log::info!(
        "pod '{}': suspend requested, artifact_id={}",
        pod_id, req.artifact_id
    );

    send_event(
        event_tx,
        WorkerEvent::ArtifactWriteStarted {
            namespace_id: namespace_id.clone(),
            artifact_id: req.artifact_id.clone(),
            pool_id: req.pool_id.clone(),
        },
    )
    .await;

    match vm.suspend(&req.snapshot_dir, SUSPEND_TIMEOUT).await {
        Ok(artifacts) => {
            let artifact_size_bytes = match F::dir_size(&req.snapshot_dir).await {
                Ok(size) => size,
                Err(e) => {
                    log::warn!(
                        "pod '{}': failed to calculate artifact size: {:#}",
                        pod_id, e
                    );
                    0
                }
            };
            let _ = req.reply.send(Ok(artifacts));
            send_event(
                event_tx,
                WorkerEvent::ArtifactWriteCommitted {
                    namespace_id: namespace_id.clone(),
                    artifact_id: req.artifact_id.clone(),
                    pool_id: req.pool_id.clone(),
                    size_bytes: artifact_size_bytes,
                },
            )
            .await;
            send_event(
                event_tx,
                WorkerEvent::PodSuspended {
                    namespace_id: namespace_id.clone(),
                    pod_id: pod_id.clone(),
                    artifact_id: req.artifact_id,
                    artifact_size_bytes,
                    pool_id: req.pool_id,
                },
            )
            .await;
            None
        }
        Err(e) => {
            let err_msg = format!("{:#}", e);
            log::error!("pod '{}': suspend failed: {}", pod_id, err_msg);
            let _ = req.reply.send(Err(err_msg.clone()));
            // vm is dropped here — Drop on instance kills the child process.
            Some(WorkerEvent::PodSuspendFailed {
                namespace_id: namespace_id.clone(),
                pod_id: pod_id.clone(),
                error: err_msg,
            })
        }
    }
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
        ResolvedEntry, ResolvedMounts, GuestDevice, VmArtifacts, VmBuilder, VmInstance, Vmm,
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
        async fn launch(self) -> anyhow::Result<(VmArtifacts<StubVmInstance>, ResolvedMounts)> {
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
        async fn launch(self) -> anyhow::Result<(VmArtifacts<MockVmInstance>, ResolvedMounts)> {
            if let Some(ref err) = self.launch_error {
                return Err(anyhow::anyhow!("{}", err));
            }
            let socket = self
                .vm_socket
                .expect("MockVmmBuilder: socket already taken");
            let instance = MockVmInstance {
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
            Ok((VmArtifacts {
                instance,
                vsock_stream: socket,
                fabric_port: None,
                exit_signal: tokio::sync::watch::channel(None).1,
            }, resolved))
        }
    }

    struct MockVmInstance {
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
