use std::path::PathBuf;
use std::process::ExitStatus;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use distvirt_worker_protocol::{
    ArtifactId, ContainerSpec, LogStreamHeader, LogStreamOpener, NamespaceId, PodId,
    PodNetworkConfig, PoolId, WorkerEvent,
};
use futures_lite::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

use crate::fabric::{Fabric, FabricPort};
use crate::image_provider::ImageProvider;
use crate::io_session::IoEvent;
use crate::managed_vm::ManagedVm;
use crate::oci;
use crate::task_handle::TaskHandle;
use crate::vmm::{BalloonConfig, NetConfig, SnapshotArtifacts, VmConfig, VmInstance, Vmm};

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
) {
    let result = pod_launch(
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
    run_pod_supervisor::<_, F>(
        result,
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
pub(crate) async fn pod_resume_supervisor<V: Vmm + 'static, F: crate::fs::Fs>(
    vmm: Arc<V>,
    fabric: Arc<Fabric<FabricPort>>,
    cancel: CancellationToken,
    event_tx: mpsc::Sender<WorkerEvent>,
    namespace_id: NamespaceId,
    pod_id: PodId,
    network: PodNetworkConfig,
    snapshot: SnapshotArtifacts,
    suspend_rx: mpsc::Receiver<SuspendRequest>,
) {
    let result = pod_restore(
        &*vmm,
        &fabric,
        &event_tx,
        &namespace_id,
        &pod_id,
        network,
        snapshot,
        &cancel,
    )
    .await
    .map(|(vm, port_task)| (vm, None, port_task));
    run_pod_supervisor::<_, F>(
        result,
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
async fn run_pod_supervisor<I: VmInstance, F: crate::fs::Fs>(
    setup_result: anyhow::Result<(
        ManagedVm<I>,
        Option<(crate::io_session::IoSession, yamux::Stream)>,
        Option<TaskHandle<()>>,
    )>,
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

/// Perform all fallible restore: restore VM from snapshot, vsock connect,
/// add TAP to fabric. Containers are already running in the restored VM.
async fn pod_restore<V: Vmm + 'static>(
    vmm: &V,
    fabric: &Fabric<FabricPort>,
    _event_tx: &mpsc::Sender<WorkerEvent>,
    _namespace_id: &NamespaceId,
    pod_id: &PodId,
    network: PodNetworkConfig,
    snapshot: SnapshotArtifacts,
    cancel: &CancellationToken,
) -> anyhow::Result<(ManagedVm<V::Instance>, Option<TaskHandle<()>>)> {
    let net_config = NetConfig::from(&network);

    let instance = tokio::select! {
        result = vmm.restore(&snapshot, Some(&net_config)) => {
            result.context("restore VM from snapshot")?
        }
        _ = cancel.cancelled() => {
            anyhow::bail!("cancelled during VM restore");
        }
    };
    log::info!("worker: pod '{}' VM restored from snapshot", pod_id);

    let (vm, port_task) = setup_instance(instance, fabric, pod_id, &network, cancel).await?;

    Ok((vm, port_task))
}

/// Perform all fallible pod setup: image prep, VM launch, vsock connect,
/// network config, container start, log stream setup.
async fn pod_launch<V: Vmm + 'static, P: ImageProvider + 'static>(
    vmm: &V,
    image_provider: &P,
    fabric: &Fabric<FabricPort>,
    kernel_path: &PathBuf,
    rootfs_image_path: &PathBuf,
    log_opener: &LogStreamOpener,
    event_tx: &mpsc::Sender<WorkerEvent>,
    namespace_id: &NamespaceId,
    pod_id: &PodId,
    network: PodNetworkConfig,
    containers: Vec<ContainerSpec>,
    resources: Option<distvirt_worker_protocol::ResourceRequirements>,
    volumes: Vec<distvirt_worker_protocol::VolumeSpec>,
    cancel: &CancellationToken,
) -> anyhow::Result<(
    ManagedVm<V::Instance>,
    Option<(crate::io_session::IoSession, yamux::Stream)>,
    Option<TaskHandle<()>>,
)> {
    let container = containers
        .into_iter()
        .next()
        .context("pod must have at least one container")?;

    let artifact = tokio::select! {
        result = image_provider.prepare(&container.image_ref) => {
            result.context("preparing image")?
        }
        _ = cancel.cancelled() => {
            anyhow::bail!("cancelled during image prepare");
        }
    };

    let container_id = container.container_id.clone();
    let container_volume_mounts = container.config.volume_mounts.clone();
    let config = if let Some(ref oci_config) = artifact.oci_config {
        oci::merge_config(oci_config, &container.config)?
    } else {
        container.config
    };

    let net_config = NetConfig::from(&network);

    let (vcpu_count, mem_size_mib) = resources
        .as_ref()
        .and_then(|r| r.limits.as_ref())
        .map(|l| {
            (
                if l.vcpus > 0 { l.vcpus } else { 1 },
                if l.memory_mib > 0 {
                    l.memory_mib as u32
                } else {
                    128u32
                },
            )
        })
        .unwrap_or((1, 128));

    let balloon = resources.as_ref().and_then(|r| {
        let limits = r.limits.as_ref()?;
        let requests = r.requests.as_ref()?;
        if requests.memory_mib < limits.memory_mib && limits.memory_mib > 0 {
            Some(BalloonConfig {
                amount_mib: (limits.memory_mib - requests.memory_mib) as u32,
                deflate_on_oom: true,
                stats_polling_interval_s: 1,
            })
        } else {
            None
        }
    });

    // Prepare volume images in a temp directory.
    let vol_tmpdir = tempfile::tempdir().context("create tmpdir for volumes")?;
    let prepared_volumes = crate::volume::prepare_volumes(&volumes, vol_tmpdir.path())
        .await
        .context("prepare volumes")?;

    let additional_drives: Vec<crate::vmm::AdditionalDrive> = prepared_volumes
        .iter()
        .enumerate()
        .map(|(i, pv)| crate::vmm::AdditionalDrive {
            drive_id: format!("vol{}", i),
            image_path: pv.image_path.clone(),
            read_only: pv.read_only,
        })
        .collect();

    let vm_config = VmConfig {
        kernel_path: kernel_path.clone(),
        rootfs_image_path: rootfs_image_path.clone(),
        container_image_path: artifact.image_path.clone(),
        vcpu_count,
        mem_size_mib,
        net: Some(net_config.clone()),
        serial_console: true,
        balloon,
        initial_commands: vec![],
        additional_drives,
    };

    let instance = tokio::select! {
        result = vmm.launch(&vm_config) => {
            result.context("launch VM")?
        }
        _ = cancel.cancelled() => {
            anyhow::bail!("cancelled during VM launch");
        }
    };
    log::info!("worker: pod '{}' VM launched", pod_id);

    let (mut vm, port_task) = setup_instance(instance, fabric, pod_id, &network, cancel).await?;

    // Take exit signal again for the setup phase below (setup_instance consumed
    // the first one for the connect phase).
    let mut vm_exit_rx = vm.take_exit_signal();
    let mut vm_died = std::pin::pin!(wait_for_vm_exit(&mut vm_exit_rx));

    let io_session = tokio::select! {
        result = async {
            vm.configure_network("eth0", &net_config).await?;

            // Mount pod-scoped volumes before adding containers.
            // Device assignment: vda=rootfs, vdb=container, vdc=config (if present),
            // then volumes start at the next available device letter.
            let vol_device_offset: u8 = 2 + if vm_config.initial_commands.is_empty() { 0 } else { 1 };
            for (i, pv) in prepared_volumes.iter().enumerate() {
                let device = format!("/dev/vd{}", (b'a' + vol_device_offset + i as u8) as char);
                vm.mount_volume(&pv.name, &device, pv.read_only).await?;
            }

            let dns_servers = vec![network.gateway.to_string()];

            // Build volume mounts for this container from the container's config.
            let volume_mounts: Vec<distvirt_guest_protocol::VolumeMount> = container_volume_mounts
                .iter()
                .map(|vm| distvirt_guest_protocol::VolumeMount {
                    name: vm.name.clone(),
                    mount_path: vm.mount_path.clone(),
                })
                .collect();

            vm.add_container(&container_id, "/dev/vdb", &dns_servers, volume_mounts)
                .await?;

            vm.start_container(&container_id, &config).await?;

            // Set up log streaming via yamux log streams.
            let io_session = if config.capture_output {
                match vm.accept_output_stream().await {
                    Ok((_cid, session)) => {
                        let header = LogStreamHeader {
                            namespace_id: namespace_id.clone(),
                            pod_id: pod_id.clone(),
                            container_id: container_id.to_string(),
                        };
                        match log_opener.open_log_stream(&header).await {
                            Ok(log_stream) => Some((session, log_stream)),
                            Err(e) => {
                                log::error!("pod '{}': failed to open log stream: {:#}", pod_id, e);
                                send_event(
                                    event_tx,
                                    WorkerEvent::PodLogStreamError {
                                        namespace_id: namespace_id.clone(),
                                        pod_id: pod_id.clone(),
                                        container_id: container_id.to_string(),
                                        phase: "open_stream".to_string(),
                                        error: format!("{:#}", e),
                                    },
                                )
                                .await;
                                None
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("pod '{}': failed to accept output stream: {:#}", pod_id, e);
                        send_event(
                            event_tx,
                            WorkerEvent::PodLogStreamError {
                                namespace_id: namespace_id.clone(),
                                pod_id: pod_id.clone(),
                                container_id: container_id.to_string(),
                                phase: "connect".to_string(),
                                error: format!("{:#}", e),
                            },
                        )
                        .await;
                        None
                    }
                }
            } else {
                None
            };

            Ok::<_, anyhow::Error>(io_session)
        } => { result? }
        _ = cancel.cancelled() => {
            anyhow::bail!("cancelled during VM setup");
        }
        _ = &mut vm_died => {
            anyhow::bail!("VM process exited during setup");
        }
    };

    Ok((vm, io_session, port_task))
}

/// Wire a freshly launched/restored VM instance into the fabric and establish
/// the yamux control connection.
///
/// Create a future that resolves when the VM process exits, or pends forever if no signal available.
async fn wait_for_vm_exit(rx: &mut Option<watch::Receiver<Option<ExitStatus>>>) {
    match rx.as_mut() {
        Some(rx) => {
            let _ = rx.wait_for(|s| s.is_some()).await;
        }
        None => std::future::pending::<()>().await,
    }
}

/// Shared between `pod_launch` and `pod_restore`: takes the TAP, adds it to
/// the fabric, then connects `ManagedVm` while racing against cancellation
/// and unexpected VM death.
async fn setup_instance<I: VmInstance>(
    mut instance: I,
    fabric: &Fabric<FabricPort>,
    pod_id: &PodId,
    network: &PodNetworkConfig,
    cancel: &CancellationToken,
) -> anyhow::Result<(ManagedVm<I>, Option<TaskHandle<()>>)> {
    let port_task = if let Some(port) = instance.take_fabric_port() {
        let (_port_id, task) = fabric.add_port_raw_with_ip(port, network.ip);
        log::info!("worker: pod '{}' network port added to fabric", pod_id);
        Some(task)
    } else {
        None
    };

    // Take exit signal before instance is moved into ManagedVm::connect,
    // so we can detect VM death during setup immediately.
    let mut vm_exit_rx = instance.take_exit_signal();
    let mut vm_died = std::pin::pin!(wait_for_vm_exit(&mut vm_exit_rx));

    let vm = tokio::select! {
        result = ManagedVm::connect(instance) => { result? }
        _ = cancel.cancelled() => {
            anyhow::bail!("cancelled during VM connect");
        }
        _ = &mut vm_died => {
            anyhow::bail!("VM process exited during setup");
        }
    };

    Ok((vm, port_task))
}

/// Timeout for suspend handshake with guest.
const SUSPEND_TIMEOUT: Duration = Duration::from_secs(10);

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
    // Spawn log streaming as a non-fatal sub-task.
    // Uses TaskHandle so it's automatically aborted when monitor exits.
    let _log_task = io_session.map(|(mut session, mut log_stream)| {
        let log_pod_id = pod_id.clone();
        TaskHandle::spawn(async move {
            loop {
                match session.next_event().await {
                    Ok(IoEvent::Stdout(data)) | Ok(IoEvent::Stderr(data)) => {
                        if log_stream.write_all(&data).await.is_err() {
                            break;
                        }
                    }
                    Ok(IoEvent::Eof) => break,
                    Err(e) => {
                        log::warn!("pod '{}' log stream error: {:#}", log_pod_id, e);
                        break;
                    }
                }
            }
            let _ = log_stream.close().await;
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

                // 2. Container exit → graceful shutdown.
                if !state.exited.is_empty() {
                    // Use the first exited container's code as the pod exit code.
                    let (id, code) = state.exited.iter().next().unwrap();
                    let code = *code;
                    log::info!("pod '{}': container {} exited with code {}", pod_id, id, code);
                    drop(state);
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

            // Cancellation: graceful shutdown requested.
            _ = cancel.cancelled() => {
                log::info!("pod '{}': cancellation received, shutting down gracefully", pod_id);
                match tokio::time::timeout(GRACEFUL_SHUTDOWN_TIMEOUT, vm.graceful_shutdown(Duration::from_secs(8), &mut rx)).await {
                    Ok(Ok(())) => {
                        log::info!("pod '{}': graceful shutdown complete", pod_id);
                    }
                    Ok(Err(e)) => {
                        log::warn!("pod '{}': graceful shutdown error: {:#}, force killing", pod_id, e);
                        let _ = vm.force_kill().await;
                    }
                    Err(_) => {
                        log::warn!("pod '{}': graceful shutdown timed out, force killing", pod_id);
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

    // _log_task is dropped here, automatically aborting via TaskHandle.

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
    use crate::vmm::{VmConfig, VmInstance, Vmm};

    // -----------------------------------------------------------------------
    // Stubs & Mocks
    // -----------------------------------------------------------------------

    struct StubVmm;

    impl Vmm for StubVmm {
        type Instance = StubVmInstance;
        async fn launch(&self, _config: &VmConfig) -> anyhow::Result<StubVmInstance> {
            panic!("StubVmm::launch should not be called");
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
        /// If Some, launch() returns this error.
        launch_error: Option<String>,
        /// The mock VM's vsock socket (worker side).
        vm_socket: tokio::sync::Mutex<Option<UnixStream>>,
    }

    struct MockVmInstance {
        vsock_socket: tokio::sync::Mutex<Option<UnixStream>>,
        killed: tokio::sync::Mutex<bool>,
    }

    impl Vmm for MockVmm {
        type Instance = MockVmInstance;
        async fn launch(&self, _config: &VmConfig) -> anyhow::Result<MockVmInstance> {
            if let Some(ref err) = self.launch_error {
                return Err(anyhow::anyhow!("{}", err));
            }
            let socket = self
                .vm_socket
                .lock()
                .await
                .take()
                .expect("MockVmm: socket already taken");
            Ok(MockVmInstance {
                vsock_socket: tokio::sync::Mutex::new(Some(socket)),
                killed: tokio::sync::Mutex::new(false),
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
            Ok(PreparedArtifact::new(
                PathBuf::from("/fake/image.ext4"),
                None, // no OCI config
                (),   // no cleanup
            ))
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
                entrypoint: vec!["/bin/echo".to_string()],
                args: vec!["hello".to_string()],
                env: vec![],
                working_dir: None,
                uid: None,
                gid: None,
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
