mod buffer;
mod cgroup;
mod config_drive;
mod container;
mod init;
mod memory;
mod net;
mod output;
mod session;
mod transport;
mod util;
mod vsock;
mod yamux_driver;

use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::os::unix::io::OwnedFd;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use anyhow::Context as _;
use async_executor::LocalExecutor;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};

use buffer::EventBuffer;
use container::{ContainerManager, ContainerTaskRequest};
use distvirt_guest_protocol::{
    GuestEvent, GuestMessage, HostMessage, StreamHeader, VSOCK_CONTROL_PORT,
};
use memory::MemoryManager;

use session::{CommandResult, LoopExit, Session};

// ---------------------------------------------------------------------------
// TaggedTask — wrapper for FuturesUnordered that returns (id, result)
// ---------------------------------------------------------------------------

struct TaggedTask {
    id: String,
    task: async_executor::Task<anyhow::Result<()>>,
}

impl Future for TaggedTask {
    type Output = (String, anyhow::Result<()>);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.task).poll(cx) {
            Poll::Ready(result) => Poll::Ready((self.id.clone(), result)),
            Poll::Pending => Poll::Pending,
        }
    }
}

// ---------------------------------------------------------------------------
// Connection loop — runs for process lifetime, reconnects internally
// ---------------------------------------------------------------------------

/// Handle an inbound yamux stream (stdin relay setup).
/// Returns a task if a stdin relay was spawned (caller must hold it for cancellation).
async fn handle_yamux_inbound(
    mut stream: yamux::Stream,
    stdin_streams: &mut HashMap<String, OwnedFd>,
    ex: &LocalExecutor<'_>,
) -> Option<async_executor::Task<()>> {
    match vsock::recv_msg::<StreamHeader>(&mut stream).await {
        Ok(StreamHeader::ContainerInput { container_id }) => {
            log::info!(
                "received inbound stdin stream for container {}",
                container_id
            );
            if let Some(stdin_fd) = stdin_streams.remove(&container_id) {
                Some(ex.spawn(output::relay_stdin(stream, stdin_fd)))
            } else {
                log::warn!(
                    "no stdin pipe for container {}, dropping stream",
                    container_id
                );
                None
            }
        }
        Ok(other) => {
            log::warn!("unexpected inbound stream header: {:?}, dropping", other);
            None
        }
        Err(e) => {
            log::warn!("failed to read inbound stream header: {:#}", e);
            None
        }
    }
}

/// Post-start setup for a newly started container: open output drain stream,
/// dup stdin pipe into per-connection map, and notify the supervisor.
async fn handle_container_started(
    id: &str,
    pid: u32,
    containers: &Rc<RefCell<ContainerManager>>,
    handle: &yamux_driver::YamuxHandle,
    conn_tasks: &mut Vec<async_executor::Task<()>>,
    stdin_streams: &mut HashMap<String, OwnedFd>,
    container_task_tx: &async_channel::Sender<ContainerTaskRequest>,
    ex: &LocalExecutor<'_>,
) {
    if let Some(buffer_rx) = containers.borrow().output_buffer_receiver(id) {
        match handle.open_stream().await {
            Ok(mut stream) => {
                if let Err(e) = vsock::send_msg(
                    &mut stream,
                    &StreamHeader::ContainerOutput {
                        container_id: id.to_string(),
                    },
                )
                .await
                {
                    log::warn!("send ContainerOutput header for {}: {:#}", id, e);
                } else {
                    conn_tasks.push(ex.spawn(output::drain_output_to_yamux(
                        id.to_string(),
                        buffer_rx,
                        stream,
                    )));
                }
            }
            Err(e) => {
                log::warn!("open yamux output stream for {}: {:#}", id, e);
            }
        }
    }
    // Dup stdin pipe into per-connection map (original stays for reconnect).
    if let Some(fd) = containers.borrow().dup_stdin_fd(id) {
        stdin_streams.insert(id.to_string(), fd);
    }
    // Send container task request to supervisor.
    let _ = container_task_tx
        .send(ContainerTaskRequest {
            id: id.to_string(),
            pid: pid as libc::pid_t,
        })
        .await;
}

/// Graceful shutdown: SIGTERM all containers, reactively wait for exits
/// via pidfd-based container tasks, then SIGKILL any stragglers.
async fn shutdown_containers(
    containers: &Rc<RefCell<ContainerManager>>,
    container_tasks: &mut FuturesUnordered<TaggedTask>,
) {
    if !containers.borrow().has_running_containers() {
        return;
    }

    {
        let cm = containers.borrow();
        let running = cm.running_container_ids();
        log::info!(
            "sending SIGTERM to {} running containers: {:?}",
            running.len(),
            running
        );
        cm.signal_all_running(libc::SIGTERM);
    }

    // Reactively wait for container exits with a 2s timeout.
    // Containers are PID 1 in their own PID namespaces, so they only
    // receive SIGTERM if they registered a handler — most won't. We keep
    // the timeout short and escalate to SIGKILL quickly.
    let deadline = async_io::Timer::after(std::time::Duration::from_secs(2));
    futures::pin_mut!(deadline);

    while containers.borrow().has_running_containers() {
        let next_exit = async {
            if container_tasks.is_empty() {
                futures::future::pending::<(String, anyhow::Result<()>)>().await
            } else {
                container_tasks.next().await.unwrap()
            }
        };

        match futures::future::select(std::pin::pin!(next_exit), &mut deadline).await {
            futures::future::Either::Left(((id, result), _)) => {
                match result {
                    Ok(()) => log::info!("shutdown: container task {} exited after SIGTERM", id),
                    Err(e) => log::error!(
                        "shutdown: container task {} failed after SIGTERM: {:#}",
                        id,
                        e
                    ),
                }
                log::info!(
                    "shutdown: {} containers still running",
                    containers.borrow().running_container_ids().len()
                );
            }
            futures::future::Either::Right(_) => {
                log::warn!(
                    "shutdown: timed out (2s) waiting for containers to exit, {} still running: {:?}",
                    containers.borrow().running_container_ids().len(),
                    containers.borrow().running_container_ids(),
                );
                break;
            }
        }
    }

    if containers.borrow().has_running_containers() {
        log::warn!("sending SIGKILL to remaining containers");
        containers.borrow_mut().signal_all_running(libc::SIGKILL);

        // Brief poll for SIGKILL exits.
        let kill_deadline = async_io::Timer::after(std::time::Duration::from_millis(200));
        futures::pin_mut!(kill_deadline);
        while containers.borrow().has_running_containers() {
            let next_exit = async {
                if container_tasks.is_empty() {
                    futures::future::pending::<(String, anyhow::Result<()>)>().await
                } else {
                    container_tasks.next().await.unwrap()
                }
            };
            match futures::future::select(std::pin::pin!(next_exit), &mut kill_deadline).await {
                futures::future::Either::Left(((id, result), _)) => match result {
                    Ok(()) => log::info!("shutdown: container task {} exited after SIGKILL", id),
                    Err(e) => log::error!(
                        "shutdown: container task {} failed after SIGKILL: {:#}",
                        id,
                        e
                    ),
                },
                futures::future::Either::Right(_) => break,
            }
        }
    }
}

/// The connection loop: accepts vsock connections, dispatches commands,
/// reconnects on disconnect. Runs for the process lifetime.
/// Workaround for Firecracker not re-enabling THRE interrupt after snapshot
/// restore (firecracker#5730). Only applies to x86_64 where the 8250 UART
/// is used; arm64 uses PL011 which is not affected.
///
/// Before suspend: `uart_suspend()` calls `tcflow(TCOOFF)` which makes the
/// kernel's 8250 driver call `serial8250_stop_tx`, clearing the THRI bit in
/// IER.
///
/// After resume: `uart_resume()` calls `tcflow(TCOON)` which triggers
/// `serial8250_start_tx`, setting THRI in IER — a clean 0→1 transition that
/// properly re-arms the THRE interrupt in Firecracker's emulated UART.
#[cfg(target_arch = "x86_64")]
fn uart_tty_flow(action: libc::c_int) {
    let path = std::ffi::CString::new("/dev/ttyS0").unwrap();
    let fd = unsafe { libc::open(path.as_ptr(), libc::O_RDWR | libc::O_NOCTTY) };
    if fd < 0 {
        return;
    }
    unsafe {
        libc::tcflow(fd, action);
        libc::close(fd);
    }
}

#[cfg(target_arch = "x86_64")]
fn uart_suspend() {
    uart_tty_flow(libc::TCOOFF);
    log::info!("uart: suspended serial TX (TCOOFF)");
}

#[cfg(target_arch = "x86_64")]
fn uart_resume() {
    uart_tty_flow(libc::TCOON);
    log::info!("uart: resumed serial TX (TCOON)");
}

#[cfg(not(target_arch = "x86_64"))]
fn uart_suspend() {}

#[cfg(not(target_arch = "x86_64"))]
fn uart_resume() {}

///
/// When a container is started, sends a `ContainerTaskRequest` through
/// `container_task_tx` so the root supervisor can spawn the container task.
async fn connection_loop(
    listener: &transport::TransportListener,
    containers: Rc<RefCell<ContainerManager>>,
    event_buffer: &EventBuffer,
    pre_config_responses: &[GuestMessage],
    container_task_tx: async_channel::Sender<ContainerTaskRequest>,
    ex: &LocalExecutor<'_>,
) -> anyhow::Result<LoopExit> {
    loop {
        log::info!("waiting for host connection");
        let running_containers = containers.borrow().running_container_ids();
        let Session {
            handle,
            yamux_task,
            mut control,
            event_stream,
        } = Session::connect(listener, running_containers, pre_config_responses, ex).await?;

        // All per-connection tasks go here. Dropping this vec cancels them.
        let mut conn_tasks: Vec<async_executor::Task<()>> = Vec::new();
        conn_tasks.push(yamux_task);

        // On resume, kick UART and release packets buffered by the plug qdisc.
        if containers.borrow().has_running_containers() {
            uart_resume();
            if let Err(e) = net::resume() {
                log::warn!("failed to unplug qdisc on resume: {:#}", e);
            }
        }

        // Per-connection stdin streams — die with the connection.
        // On reconnect, populate from all running containers that have stdin.
        let mut stdin_streams: HashMap<String, OwnedFd> = HashMap::new();
        for id in containers.borrow().running_container_ids() {
            if let Some(fd) = containers.borrow().dup_stdin_fd(&id) {
                stdin_streams.insert(id, fd);
            }
        }

        // Spawn event drain task for this connection.
        conn_tasks.push(ex.spawn(output::drain_events_to_yamux(
            event_buffer.receiver(),
            event_stream,
        )));

        // Spawn output drain tasks for all containers that have output buffers.
        for (id, buffer_rx) in containers.borrow().containers_with_output() {
            match handle.open_stream().await {
                Ok(mut stream) => {
                    if let Err(e) = vsock::send_msg(
                        &mut stream,
                        &StreamHeader::ContainerOutput {
                            container_id: id.clone(),
                        },
                    )
                    .await
                    {
                        log::warn!("send ContainerOutput header for {}: {:#}", id, e);
                        continue;
                    }
                    conn_tasks.push(ex.spawn(output::drain_output_to_yamux(id, buffer_rx, stream)));
                }
                Err(e) => {
                    log::warn!("open yamux output stream for {}: {:#}", id, e);
                }
            }
        }

        let loop_exit = 'event_loop: loop {
            let yamux_inbound = async { handle.next_inbound().await };
            let ctrl = std::future::poll_fn(|cx| control.poll_recv::<HostMessage>(cx));

            futures::select! {
                msg = ctrl.fuse() => {
                    match msg {
                        Ok(msg) => {
                            log::info!("received: {:?}", msg);
                            let resp = {
                                let mut cm = containers.borrow_mut();
                                session::execute_command(msg, &mut cm, ex)
                            };
                            match resp {
                                CommandResult::Response(resp) => {
                                    if let Err(e) = control.send(&resp).await {
                                        log::error!("send response: {:#}", e);
                                        break 'event_loop LoopExit::Disconnected;
                                    }
                                    if let GuestMessage::ContainerStarted { ref id, pid } = resp {
                                        handle_container_started(
                                            id, pid, &containers, &handle,
                                            &mut conn_tasks, &mut stdin_streams,
                                            &container_task_tx, ex,
                                        ).await;
                                    }
                                }
                                CommandResult::PrepareSuspend => {
                                    if let Err(e) = control.send(&GuestMessage::SuspendReady).await {
                                        log::error!("send SuspendReady: {:#}", e);
                                    }
                                    log::info!("sent SuspendReady, flushing yamux and closing connection");
                                    uart_suspend();
                                    if let Err(e) = handle.close().await {
                                        log::warn!("yamux close after SuspendReady: {}", e);
                                    }
                                    break 'event_loop LoopExit::Disconnected;
                                }
                                CommandResult::Shutdown => {
                                    break 'event_loop LoopExit::Shutdown;
                                }
                            }
                        }
                        Err(e) => {
                            log::error!("control stream error: {:#}", e);
                            break 'event_loop LoopExit::Disconnected;
                        }
                    }
                }
                stream = yamux_inbound.fuse() => {
                    match stream {
                        Some(stream) => {
                            if let Some(task) = handle_yamux_inbound(stream, &mut stdin_streams, ex).await {
                                conn_tasks.push(task);
                            }
                        }
                        None => {
                            log::info!("yamux connection closed");
                            break 'event_loop LoopExit::Disconnected;
                        }
                    }
                }
            }
        };

        match loop_exit {
            LoopExit::Shutdown => {
                // Close yamux cleanly so the host-side driver sees a
                // clean close rather than a broken pipe from VM death.
                log::info!("connection_loop: received Shutdown, closing yamux before returning");
                if let Err(e) = handle.close().await {
                    log::warn!("yamux close during shutdown: {}", e);
                }
                // Cancel all per-connection tasks (drain, stdin relay, yamux driver).
                drop(conn_tasks);
                return Ok(LoopExit::Shutdown);
            }
            LoopExit::Disconnected => {
                // Connection lost (suspend or unexpected disconnect).
                // Cancel all per-connection tasks — old drain tasks, stdin
                // relays, and the yamux driver are cleaned up here.
                // Fill tasks keep running. Output buffers retain data.
                drop(conn_tasks);
                log::info!(
                    "connection lost, waiting for reconnect ({} containers still running)",
                    containers.borrow().running_container_ids().len()
                );
                continue;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Root supervisor
// ---------------------------------------------------------------------------

fn run() -> anyhow::Result<()> {
    init::mount_essential_filesystems();

    if let Err(e) = net::bring_up_loopback() {
        log::warn!("failed to bring up loopback: {:#}", e);
    }

    // Allow unlimited overcommit so that large virtual allocations succeed
    // immediately. Physical memory pressure is handled by cgroup limits and
    // balloon deflation — rejecting allocations at the virtual level would
    // bypass that mechanism entirely.
    if let Err(e) = std::fs::write("/proc/sys/vm/overcommit_memory", "1") {
        log::warn!("failed to set vm.overcommit_memory=1: {}", e);
    }

    let vm_mem_mib = memory::init::read_memtotal_mib()?;
    let vm_config = memory::init::VmMemoryConfig::from_vm_mem(vm_mem_mib);
    memory::init::setup_zram_swap(&vm_config);
    memory::init::set_tcp_memory_caps(&vm_config);

    let listener = match memory::init::read_cmdline_param("distvirt.transport") {
        Some(val) if val == "virtio-serial" => {
            let path = memory::init::read_cmdline_param("distvirt.transport_device")
                .map(std::path::PathBuf::from)
                .map_or_else(
                    || transport::find_virtio_serial_port("transport"),
                    Ok,
                )
                .context("find virtio-serial transport device")?;
            log::info!("using virtio-serial transport: {}", path.display());
            transport::TransportListener::VirtioSerial { path }
        }
        _ => {
            log::info!("starting vsock listener on port {}", VSOCK_CONTROL_PORT);
            let vsock_listener =
                vsock::VsockListener::bind(VSOCK_CONTROL_PORT).context("bind vsock listener")?;
            transport::TransportListener::Vsock(vsock_listener)
        }
    };

    let ex = LocalExecutor::new();

    async_io::block_on(ex.run(async {
        // Containers persist across reconnects — they keep running through
        // suspend/resume cycles.
        let containers = Rc::new(RefCell::new(ContainerManager::new()));

        // Event buffer persists across reconnects. Events are produced by
        // container exits and the balloon task, drained to yamux per-connection.
        let event_buffer = EventBuffer::new();

        // Parse balloon size from kernel cmdline (set by host Firecracker config).
        let memory_manager = match memory::init::read_cmdline_param("distvirt.balloon_mib") {
            Some(v) => {
                log::info!("found distvirt.balloon_mib={} on cmdline", v);
                match v.parse::<u32>() {
                    Ok(balloon_mib) => {
                        log::info!(
                            "memory manager: balloon={} MiB, vm_mem={} MiB, initial_limit=~{} MiB",
                            balloon_mib,
                            vm_mem_mib,
                            vm_mem_mib
                                .saturating_sub(balloon_mib)
                                .saturating_sub(memory::KERNEL_BUFFER_MIB),
                        );
                        Some(Rc::new(RefCell::new(MemoryManager::new(
                            balloon_mib,
                            vm_mem_mib,
                        ))))
                    }
                    Err(e) => {
                        log::warn!("failed to parse distvirt.balloon_mib: {}", e);
                        None
                    }
                }
            }
            None => {
                log::info!("no distvirt.balloon_mib on cmdline, memory manager disabled");
                None
            }
        };

        // Set initial cgroup memory limits BEFORE creating PSI triggers,
        // so PSI triggers don't fire spuriously on an unconstrained cgroup.
        if let Some(ref mm) = memory_manager {
            let (high_bytes, max_bytes) = mm.borrow_mut().initial_limits();
            log::info!(
                "[balloon] setting initial cgroup limits: path={}, high={} MiB, max={} MiB",
                cgroup::CGROUP_ROOT,
                high_bytes / (1024 * 1024),
                max_bytes / (1024 * 1024),
            );
            if let Err(e) =
                cgroup::set_memory_limits(cgroup::CGROUP_ROOT, high_bytes, max_bytes)
            {
                log::warn!("failed to set initial cgroup limits: {:#}", e);
            }
        }

        // Now that memory limits are set, create async PSI monitor.
        let async_psi = match cgroup::setup_psi_monitor(cgroup::CGROUP_ROOT) {
            Ok(triggers) => match cgroup::AsyncPsiMonitor::new(triggers) {
                Ok(monitor) => Some(Rc::new(monitor)),
                Err(e) => {
                    log::warn!("failed to create async PSI monitor: {:#}", e);
                    None
                }
            },
            Err(e) => {
                log::warn!("failed to set up PSI triggers: {:#}", e);
                None
            }
        };

        // Set up inotify-based memory.events monitor (wrapped in Rc<RefCell>
        // so it survives across connection reconnects).
        let mem_events_holder = Rc::new(RefCell::new(
            match cgroup::AsyncMemoryEventsMonitor::new(cgroup::CGROUP_ROOT) {
                Ok(monitor) => Some(monitor),
                Err(e) => {
                    log::warn!("failed to create memory.events monitor: {:#}", e);
                    None
                }
            },
        ));

        // Create channel for balloon sysfs monitor and spawn both monitor
        // and balloon task only when memory management is enabled (i.e.
        // distvirt.balloon_mib was present on the kernel command line).
        let (balloon_monitor_tx, balloon_monitor_rx) =
            async_channel::bounded::<memory::monitor::BalloonChange>(8);
        let balloon_monitor_task = if memory_manager.is_some() {
            Some(ex.spawn(async {
                if let Err(e) = memory::monitor::run(balloon_monitor_tx).await {
                    log::warn!("balloon monitor exited: {:#}", e);
                }
            }))
        } else {
            drop(balloon_monitor_tx);
            log::info!("balloon monitor disabled (no distvirt.balloon_mib)");
            None
        };

        // Spawn the balloon task (supervised, persists across reconnects).
        let balloon_task = if let (Some(mm), Some(psi)) = (&memory_manager, &async_psi) {
            let event_tx = event_buffer.sender();
            Some(ex.spawn(memory::task::run(
                mm.clone(),
                psi.clone(),
                event_tx,
                mem_events_holder.clone(),
                balloon_monitor_rx.clone(),
            )))
        } else {
            None
        };

        // Execute pre-vsock config drive commands (if a config device is present).
        let pre_config_responses = config_drive::execute_pre_config(
            &mut containers.borrow_mut(),
            &ex,
        );

        // Channel for connection loop to request container task spawning.
        let (container_task_tx, container_task_rx) =
            async_channel::bounded::<ContainerTaskRequest>(16);

        // Container tasks, dynamically spawned.
        let mut container_tasks: FuturesUnordered<TaggedTask> = FuturesUnordered::new();

        // Supervisor loop: runs the connection loop inline and selects over
        // child completions + new container requests.
        //
        // We run connection_loop as a pinned future rather than spawning it,
        // so it can borrow local state (event_buffer, pre_config_responses, ex)
        // without lifetime issues.
        let mut conn_loop = std::pin::pin!(connection_loop(
            &listener,
            containers.clone(),
            &event_buffer,
            &pre_config_responses,
            container_task_tx,
            &ex,
        ).fuse());

        // For supervised tasks: wrap in fused futures that go pending if
        // the task was not spawned (balloon disabled).
        let balloon_monitor_fut = async {
            match balloon_monitor_task {
                Some(task) => task.await,
                None => futures::future::pending::<()>().await,
            }
        };
        let mut balloon_monitor_fut = std::pin::pin!(balloon_monitor_fut.fuse());

        let balloon_task_fut = async {
            match balloon_task {
                Some(task) => task.await,
                None => futures::future::pending::<()>().await,
            }
        };
        let mut balloon_task_fut = std::pin::pin!(balloon_task_fut.fuse());

        loop {
            // Build a future for container task completions that goes pending
            // when FuturesUnordered is empty (instead of returning None).
            let container_next = async {
                if container_tasks.is_empty() {
                    futures::future::pending::<(String, anyhow::Result<()>)>().await
                } else {
                    container_tasks.next().await.unwrap()
                }
            };

            futures::select! {
                req = container_task_rx.recv().fuse() => {
                    match req {
                        Ok(req) => {
                            let id = req.id.clone();
                            let pid = req.pid;
                            let c = containers.clone();
                            let task = ex.spawn(container::container_task(
                                req.id,
                                req.pid,
                                c,
                                event_buffer.sender(),
                            ));
                            log::info!("supervisor: spawned container task for {} (pid {})", id, pid);
                            container_tasks.push(TaggedTask { id, task });
                        }
                        Err(_) => {
                            // Channel closed — connection loop exited.
                            log::info!("supervisor: container task channel closed");
                        }
                    }
                }
                result = container_next.fuse() => {
                    let (id, result) = result;
                    match result {
                        Ok(()) => {
                            log::info!("supervisor: container task {} completed normally", id);
                        }
                        Err(e) => {
                            log::error!("supervisor: container task {} failed: {:#}", id, e);
                            event_buffer.send(GuestEvent::TaskError {
                                task: format!("container:{}", id),
                                message: format!("{:#}", e),
                            }).await;
                            break;
                        }
                    }
                }
                result = conn_loop => {
                    match result {
                        Ok(LoopExit::Shutdown) => {
                            log::info!("supervisor: connection loop returned shutdown, beginning container shutdown");

                            // Signal containers to exit, reactively waiting for pidfd exits.
                            shutdown_containers(&containers, &mut container_tasks).await;
                            log::info!("supervisor: shutdown_containers complete");

                            // container_task_tx was moved into connection_loop
                            // and is now dropped, so container_task_rx will
                            // close once existing sends complete.

                            // Drain remaining container tasks with a timeout.
                            if !container_tasks.is_empty() {
                                log::info!(
                                    "supervisor: waiting for {} container task(s) to finish",
                                    container_tasks.len()
                                );
                                let drain_all = async {
                                    while let Some((id, result)) = container_tasks.next().await {
                                        match result {
                                            Ok(()) => log::info!("supervisor: container task {} completed during shutdown", id),
                                            Err(e) => log::error!("supervisor: container task {} failed during shutdown: {:#}", id, e),
                                        }
                                    }
                                };
                                let timeout = async_io::Timer::after(
                                    std::time::Duration::from_secs(5)
                                );
                                futures::pin_mut!(drain_all);
                                futures::pin_mut!(timeout);
                                match futures::future::select(drain_all, timeout).await {
                                    futures::future::Either::Left(_) => {
                                        log::info!("supervisor: all container tasks drained");
                                    }
                                    futures::future::Either::Right(_) => {
                                        log::warn!("supervisor: timed out waiting for container tasks, {} remaining", container_tasks.len());
                                    }
                                }
                            }

                            // Brief sleep to let virtio-net flush outgoing packets.
                            async_io::Timer::after(std::time::Duration::from_millis(200)).await;
                        }
                        Ok(LoopExit::Disconnected) => {
                            // connection_loop should never return Disconnected
                            // (it reconnects internally), but handle it gracefully.
                            log::warn!("supervisor: connection loop returned Disconnected unexpectedly");
                        }
                        Err(e) => {
                            log::error!("supervisor: connection loop failed: {:#}", e);
                            event_buffer.send(GuestEvent::TaskError {
                                task: "connection_loop".to_string(),
                                message: format!("{:#}", e),
                            }).await;
                        }
                    }
                    break;
                }
                _ = balloon_monitor_fut => {
                    log::error!("supervisor: balloon monitor exited unexpectedly");
                    event_buffer.send(GuestEvent::TaskError {
                        task: "balloon_monitor".to_string(),
                        message: "balloon monitor exited unexpectedly".to_string(),
                    }).await;
                    break;
                }
                _ = balloon_task_fut => {
                    log::error!("supervisor: balloon task exited unexpectedly");
                    event_buffer.send(GuestEvent::TaskError {
                        task: "balloon_task".to_string(),
                        message: "balloon task exited unexpectedly".to_string(),
                    }).await;
                    break;
                }
            }
        }

        Ok(())
    }))
}

fn main() {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .parse_default_env()
        .init();

    log::info!("guest-init started");

    if let Err(e) = run() {
        log::error!("fatal: {:#}", e);
    }

    log::info!("shutting down");
    unsafe {
        libc::sync();
    }
    // distvirt.shutdown=poweroff uses ACPI power-off (for VMMs that support it,
    // e.g. Cloud Hypervisor). Default is RB_AUTOBOOT which triggers a triple
    // fault — needed for Firecracker which doesn't support ACPI power-off.
    let cmd = match memory::init::read_cmdline_param("distvirt.shutdown").as_deref() {
        Some("poweroff") => {
            log::info!("using ACPI power-off (distvirt.shutdown=poweroff)");
            libc::RB_POWER_OFF
        }
        _ => {
            log::info!("using reboot/triple-fault shutdown");
            libc::RB_AUTOBOOT
        }
    };
    unsafe {
        libc::reboot(cmd);
    }
    loop {
        unsafe {
            libc::pause();
        }
    }
}
