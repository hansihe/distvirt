//! Runtime-agnostic supervisor: connection lifecycle + container exit handling.
//!
//! This module contains the core supervisor loop that can run on any async runtime
//! (smol in production, tokio in tests). It handles:
//! - Connection lifecycle (accept, handshake, control message dispatch)
//! - Container exit processing
//! - Graceful shutdown (SIGTERM -> wait -> SIGKILL)
//!
//! It does NOT include balloon/memory management (VM-specific). The production
//! entry point composes those on top via `futures::select!`.

use std::collections::HashMap;
use std::os::unix::io::OwnedFd;
use std::sync::Arc;

use parking_lot::Mutex;

use futures::FutureExt;

use distvirt_guest_protocol::{GuestEvent, GuestMessage, HostMessage, StreamHeader};

use crate::buffer::EventBuffer;
use crate::config::GuestConfig;
use crate::container::{ContainerBackend, ContainerManager};
use crate::output;
use crate::platform::Platform;
use crate::session::{CommandResult, LoopExit, Session};
use crate::spawner::{LocalSpawner, TaskHandle};
use crate::transport::TransportListener;
use crate::vsock;
use crate::yamux_driver;

// ---------------------------------------------------------------------------
// Connection loop — runs for process lifetime, reconnects internally
// ---------------------------------------------------------------------------

/// Handle an inbound yamux stream (stdin relay setup).
/// Returns a task if a stdin relay was spawned (caller must hold it for cancellation).
async fn handle_yamux_inbound(
    mut stream: yamux::Stream,
    stdin_streams: &mut HashMap<String, OwnedFd>,
    spawner: &impl LocalSpawner,
) -> Option<TaskHandle> {
    match vsock::recv_msg::<StreamHeader>(&mut stream).await {
        Ok(StreamHeader::ContainerInput { container_id }) => {
            log::info!(
                "received inbound stdin stream for container {}",
                container_id
            );
            if let Some(stdin_fd) = stdin_streams.remove(&container_id) {
                Some(spawner.spawn_local(output::relay_stdin(stream, stdin_fd)))
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
/// dup stdin pipe into per-connection map.
async fn handle_container_started<B: ContainerBackend>(
    id: &str,
    containers: &Arc<Mutex<ContainerManager<B>>>,
    handle: &yamux_driver::YamuxHandle,
    conn_tasks: &mut Vec<TaskHandle>,
    stdin_streams: &mut HashMap<String, OwnedFd>,
    spawner: &impl LocalSpawner,
) {
    if let Some(buffer_rx) = containers.lock().output_buffer_receiver(id) {
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
                    conn_tasks.push(spawner.spawn_local(output::drain_output_to_yamux(
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
    if let Some(fd) = containers.lock().dup_stdin_fd(id) {
        stdin_streams.insert(id.to_string(), fd);
    }
}

/// Graceful shutdown: SIGTERM all containers, wait for exits via the exit
/// channel, then SIGKILL any stragglers.
async fn shutdown_containers<B: ContainerBackend>(
    containers: &Arc<Mutex<ContainerManager<B>>>,
    exit_rx: &async_channel::Receiver<crate::container::ContainerExit>,
    event_buffer: &EventBuffer,
    config: &GuestConfig,
) {
    if !containers.lock().has_running_containers() {
        return;
    }

    {
        let mut cm = containers.lock();
        let running = cm.running_container_ids();
        log::info!(
            "sending SIGTERM to {} running containers: {:?}",
            running.len(),
            running
        );
        cm.signal_all_running(libc::SIGTERM);
    }

    // Reactively wait for container exits with a configurable timeout.
    let deadline = crate::timer::sleep(config.shutdown_timeout);
    futures::pin_mut!(deadline);

    while containers.lock().has_running_containers() {
        let next_exit = exit_rx.recv();

        match futures::future::select(std::pin::pin!(next_exit), &mut deadline).await {
            futures::future::Either::Left((Ok(exit), _)) => {
                log::info!("shutdown: container {} exited after SIGTERM (code {})", exit.id, exit.code);
                handle_container_exit(&exit, containers, event_buffer).await;
                log::info!(
                    "shutdown: {} containers still running",
                    containers.lock().running_container_ids().len()
                );
            }
            futures::future::Either::Left((Err(_), _)) => {
                log::warn!("shutdown: exit channel closed unexpectedly");
                break;
            }
            futures::future::Either::Right(_) => {
                log::warn!(
                    "shutdown: timed out ({:?}) waiting for containers to exit, {} still running: {:?}",
                    config.shutdown_timeout,
                    containers.lock().running_container_ids().len(),
                    containers.lock().running_container_ids(),
                );
                break;
            }
        }
    }

    if containers.lock().has_running_containers() {
        log::warn!("sending SIGKILL to remaining containers");
        containers.lock().signal_all_running(libc::SIGKILL);

        // Brief poll for SIGKILL exits.
        let kill_deadline = crate::timer::sleep(config.shutdown_kill_timeout);
        futures::pin_mut!(kill_deadline);
        while containers.lock().has_running_containers() {
            let next_exit = exit_rx.recv();
            match futures::future::select(std::pin::pin!(next_exit), &mut kill_deadline).await {
                futures::future::Either::Left((Ok(exit), _)) => {
                    log::info!("shutdown: container {} exited after SIGKILL (code {})", exit.id, exit.code);
                    handle_container_exit(&exit, containers, event_buffer).await;
                }
                futures::future::Either::Left((Err(_), _)) => break,
                futures::future::Either::Right(_) => break,
            }
        }
    }
}

/// Process a container exit: mark exited, push event, remove container.
async fn handle_container_exit<B: ContainerBackend>(
    exit: &crate::container::ContainerExit,
    containers: &Arc<Mutex<ContainerManager<B>>>,
    event_buffer: &EventBuffer,
) {
    containers.lock().mark_exited(&exit.id);
    event_buffer
        .send(GuestEvent::ContainerExited {
            id: exit.id.clone(),
            code: exit.code,
            output_bytes_dropped: exit.output_bytes_dropped,
        })
        .await;
    containers.lock().remove(&exit.id);
}

/// Connection loop — runs for process lifetime, reconnects internally.
///
/// When a container is started, the backend spawns exit monitoring internally.
/// The supervisor receives exits through the exit channel.
async fn connection_loop<B: ContainerBackend>(
    listener: &TransportListener,
    containers: Arc<Mutex<ContainerManager<B>>>,
    event_buffer: &EventBuffer,
    platform: &impl Platform,
    spawner: &impl LocalSpawner,
) -> anyhow::Result<LoopExit> {
    loop {
        log::info!("waiting for host connection");
        let running_containers = containers.lock().running_container_ids();
        let Session {
            handle,
            yamux_task,
            mut control,
            event_stream,
        } = Session::connect(listener, running_containers, spawner).await?;

        // All per-connection tasks go here. Dropping this vec cancels them.
        let mut conn_tasks: Vec<TaskHandle> = Vec::new();
        conn_tasks.push(yamux_task);

        // On resume, release packets buffered by the plug qdisc.
        if containers.lock().has_running_containers() {
            platform.on_resume();
        }

        // Per-connection stdin streams — die with the connection.
        // N.B. Lock must be released before the loop body re-locks.
        let mut stdin_streams: HashMap<String, OwnedFd> = HashMap::new();
        let running_ids = containers.lock().running_container_ids();
        for id in running_ids {
            if let Some(fd) = containers.lock().dup_stdin_fd(&id) {
                stdin_streams.insert(id, fd);
            }
        }

        // Spawn event drain task for this connection.
        conn_tasks.push(spawner.spawn_local(output::drain_events_to_yamux(
            event_buffer.receiver(),
            event_stream,
        )));

        // Spawn output drain tasks for all containers that have output buffers.
        for (id, buffer_rx) in containers.lock().containers_with_output() {
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
                    conn_tasks.push(spawner.spawn_local(output::drain_output_to_yamux(id, buffer_rx, stream)));
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
                                let mut cm = containers.lock();
                                crate::session::execute_command(msg, &mut cm, platform, spawner)
                            };
                            match resp {
                                CommandResult::Response(resp) => {
                                    if let Err(e) = control.send(&resp).await {
                                        log::error!("send response: {:#}", e);
                                        break 'event_loop LoopExit::Disconnected;
                                    }
                                    if let GuestMessage::ContainerStarted { ref id, pid: _ } = resp {
                                        handle_container_started(
                                            id, &containers, &handle,
                                            &mut conn_tasks, &mut stdin_streams,
                                            spawner,
                                        ).await;
                                    }
                                }
                                CommandResult::PrepareSuspend => {
                                    platform.on_suspend();
                                    if let Err(e) = control.send(&GuestMessage::SuspendReady).await {
                                        log::error!("send SuspendReady: {:#}", e);
                                    }
                                    log::info!("sent SuspendReady, flushing yamux and closing connection");
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
                            if let Some(task) = handle_yamux_inbound(stream, &mut stdin_streams, spawner).await {
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
                log::info!("connection_loop: received Shutdown, closing yamux before returning");
                if let Err(e) = handle.close().await {
                    log::warn!("yamux close during shutdown: {}", e);
                }
                drop(conn_tasks);
                return Ok(LoopExit::Shutdown);
            }
            LoopExit::Disconnected => {
                drop(conn_tasks);
                log::info!(
                    "connection lost, waiting for reconnect ({} containers still running)",
                    containers.lock().running_container_ids().len()
                );
                continue;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Core supervisor loop: connection lifecycle + container exit handling.
///
/// This is the runtime-agnostic entry point for the guest-init supervisor.
/// It takes all dependencies as parameters so it can run on any async runtime
/// (smol in production, tokio in tests).
///
/// Does NOT include balloon/memory management (VM-specific). The production
/// entry point composes those on top via `futures::select!`.
pub async fn run_supervisor<B: ContainerBackend>(
    config: &GuestConfig,
    platform: &impl Platform,
    containers: Arc<Mutex<ContainerManager<B>>>,
    event_buffer: &EventBuffer,
    listener: &TransportListener,
    spawner: &impl LocalSpawner,
) -> anyhow::Result<()> {
    let exit_rx = containers.lock().exit_receiver();

    let mut conn_loop = std::pin::pin!(connection_loop(
        listener,
        containers.clone(),
        event_buffer,
        platform,
        spawner,
    )
    .fuse());

    loop {
        futures::select! {
            exit = exit_rx.recv().fuse() => {
                match exit {
                    Ok(exit) => {
                        log::info!(
                            "supervisor: container {} exited with code {}",
                            exit.id, exit.code
                        );
                        handle_container_exit(&exit, &containers, event_buffer).await;
                    }
                    Err(_) => {
                        log::info!("supervisor: exit channel closed");
                        break;
                    }
                }
            }
            result = conn_loop => {
                match result {
                    Ok(LoopExit::Shutdown) => {
                        log::info!("supervisor: connection loop returned shutdown, beginning container shutdown");
                        shutdown_containers(&containers, &exit_rx, event_buffer, config).await;
                        log::info!("supervisor: shutdown_containers complete");

                        // Brief sleep to let virtio-net flush outgoing packets.
                        crate::timer::sleep(std::time::Duration::from_millis(200)).await;
                    }
                    Ok(LoopExit::Disconnected) => {
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
        }
    }

    Ok(())
}
