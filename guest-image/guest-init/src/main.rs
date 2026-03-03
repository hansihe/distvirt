mod container;
mod net;
mod util;
mod vsock;

use std::collections::HashMap;
use std::future::Future;
use std::os::unix::io::AsRawFd;
use std::pin::Pin;
use std::task::Poll;

use anyhow::Context;
use async_executor::LocalExecutor;
use async_io::Async;
use futures_lite::future;
use futures_lite::io::{AsyncRead, AsyncWriteExt};

use container::ContainerManager;
use distvirt_guest_protocol::{
    GuestMessage, HostMessage, StreamHeader, VSOCK_CONTROL_PORT,
    STREAM_STDOUT, STREAM_STDERR, encode_output_chunk,
};
use std::os::unix::io::OwnedFd;
use util::ReadPipeResult;

fn mount_essential_filesystems() {
    let mounts: &[(&str, &str, &str, libc::c_ulong, Option<&str>)] = &[
        ("proc", "/proc", "proc", libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC, None),
        ("sysfs", "/sys", "sysfs", libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC, None),
        ("tmpfs", "/tmp", "tmpfs", libc::MS_NOSUID | libc::MS_NODEV, None),
        ("devpts", "/dev/pts", "devpts", libc::MS_NOSUID | libc::MS_NOEXEC, Some("gid=5,mode=620")),
        ("tmpfs", "/dev/shm", "tmpfs", libc::MS_NOSUID | libc::MS_NODEV, None),
    ];

    for &(source, target, fstype, flags, data) in mounts {
        if let Err(err) = util::mount(source, target, fstype, flags, data) {
            log::warn!("{:#}", err);
        }
    }
}

/// Buffered reader for the yamux control stream.
///
/// Accumulates bytes from `poll_read` and yields complete length-prefixed JSON
/// messages. Safe to use inside a droppable select arm because partial read
/// progress is preserved in the struct, not in stack temporaries.
struct ControlReader {
    stream: yamux::Stream,
    buf: Vec<u8>,
}

impl ControlReader {
    fn new(stream: yamux::Stream) -> Self {
        ControlReader { stream, buf: Vec::new() }
    }

    /// Poll for a complete length-prefixed JSON message.
    ///
    /// Reads available bytes from the yamux stream into an internal buffer,
    /// then checks if a complete message is present. Returns `Pending` if
    /// only a partial message has arrived.
    fn poll_recv<T: serde::de::DeserializeOwned>(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<anyhow::Result<T>> {
        // Read whatever is available from the yamux stream.
        loop {
            let mut tmp = [0u8; 8192];
            match Pin::new(&mut self.stream).poll_read(cx, &mut tmp) {
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(anyhow::anyhow!("control stream EOF")));
                }
                Poll::Ready(Ok(n)) => {
                    self.buf.extend_from_slice(&tmp[..n]);
                }
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Err(anyhow::Error::from(e).context("read control stream")));
                }
                Poll::Pending => break,
            }
        }

        // Check for a complete message.
        if self.buf.len() < 4 {
            return Poll::Pending;
        }
        let len = u32::from_le_bytes(self.buf[..4].try_into().unwrap()) as usize;
        if len > 1024 * 1024 {
            return Poll::Ready(Err(anyhow::anyhow!("message too large: {} bytes", len)));
        }
        if self.buf.len() < 4 + len {
            return Poll::Pending;
        }

        let result = serde_json::from_slice(&self.buf[4..4 + len])
            .context("deserialize message");
        self.buf.drain(..4 + len);
        Poll::Ready(result)
    }

    /// Send a length-prefixed JSON message on the control stream.
    async fn send<T: serde::Serialize>(&mut self, msg: &T) -> anyhow::Result<()> {
        vsock::send_msg(&mut self.stream, msg).await
    }
}

/// Why the inner event loop exited.
enum LoopExit {
    /// Host sent Shutdown — kill containers and reboot.
    Shutdown,
    /// Yamux connection lost — wait for reconnect (suspend/resume path).
    Disconnected,
}

async fn handle_message(
    msg: HostMessage,
    control: &mut ControlReader,
    containers: &mut ContainerManager,
    conn: &mut yamux::Connection<Async<std::fs::File>>,
    output_streams: &mut HashMap<String, yamux::Stream>,
    stdin_streams: &mut HashMap<String, OwnedFd>,
) -> anyhow::Result<Option<LoopExit>> {
    match msg {
        HostMessage::AddContainer {
            id,
            device,
            dns_servers,
        } => {
            log::info!("AddContainer: id={}, device={}", id, device);
            match containers.add(id.clone(), device, &dns_servers) {
                Ok(()) => {
                    control.send(&GuestMessage::ContainerAdded { id }).await?;
                }
                Err(e) => {
                    log::error!("AddContainer failed: {:#}", e);
                    control.send(&GuestMessage::Error {
                        message: format!("{:#}", e),
                    }).await?;
                }
            }
        }
        HostMessage::StartContainer { id, entrypoint, args, env, working_dir, uid, gid, hostname, capture_output, stdin } => {
            log::info!("StartContainer: id={}, entrypoint={}, capture_output={}, stdin={}", id, entrypoint, capture_output, stdin);

            // If capture_output, open a yamux output stream before forking.
            if capture_output {
                let mut stream = std::future::poll_fn(|cx| conn.poll_new_outbound(cx))
                    .await
                    .context("open yamux output stream")?;

                vsock::send_msg(&mut stream, &StreamHeader::ContainerOutput {
                    container_id: id.clone(),
                }).await.context("send StreamHeader::ContainerOutput")?;

                output_streams.insert(id.clone(), stream);
            }

            match containers.start(&id, &entrypoint, &args, &env, working_dir.as_deref(), uid, gid, hostname.as_deref(), capture_output, stdin) {
                Ok(pid) => {
                    // If stdin was requested, move the stdin pipe write-end into the stdin_streams map.
                    if stdin {
                        if let Some(fd) = containers.take_stdin_fd(&id) {
                            stdin_streams.insert(id.clone(), fd);
                        }
                    }
                    control.send(&GuestMessage::ContainerStarted { id, pid }).await?;
                }
                Err(e) => {
                    log::error!("StartContainer failed: {:#}", e);
                    output_streams.remove(&id);
                    control.send(&GuestMessage::Error {
                        message: format!("{:#}", e),
                    }).await?;
                }
            }
        }
        HostMessage::ConfigureNetwork { interface, ip, netmask, gateway } => {
            log::info!("ConfigureNetwork: {}={}, netmask={}, gw={}", interface, ip, netmask, gateway);
            match net::configure_network(&interface, &ip, &netmask, &gateway) {
                Ok(()) => {
                    control.send(&GuestMessage::NetworkConfigured).await?;
                }
                Err(e) => {
                    log::error!("ConfigureNetwork failed: {:#}", e);
                    control.send(&GuestMessage::Error {
                        message: format!("{:#}", e),
                    }).await?;
                }
            }
        }
        HostMessage::SignalContainer { id, signal } => {
            log::info!("SignalContainer: id={}, signal={}", id, signal);
            match containers.signal_container(&id, signal) {
                Ok(()) => {
                    control.send(&GuestMessage::ContainerSignaled { id }).await?;
                }
                Err(e) => {
                    log::error!("SignalContainer failed: {:#}", e);
                    control.send(&GuestMessage::Error {
                        message: format!("{:#}", e),
                    }).await?;
                }
            }
        }
        HostMessage::SetClock { epoch_secs, epoch_nanos } => {
            log::info!("SetClock: epoch_secs={}, epoch_nanos={}", epoch_secs, epoch_nanos);
            let ts = libc::timespec {
                tv_sec: epoch_secs as libc::time_t,
                tv_nsec: epoch_nanos as libc::c_long,
            };
            let ret = unsafe { libc::clock_settime(libc::CLOCK_REALTIME, &ts) };
            if ret == 0 {
                log::info!("system clock set successfully");
                control.send(&GuestMessage::ClockSet).await?;
            } else {
                let err = std::io::Error::last_os_error();
                log::error!("clock_settime failed: {}", err);
                control.send(&GuestMessage::Error {
                    message: format!("clock_settime failed: {}", err),
                }).await?;
            }
        }
        HostMessage::PrepareSuspend => {
            log::info!("PrepareSuspend: flushing container output");
            // Flush all captured container output to yamux before signaling ready.
            let captured_ids = containers.captured_container_ids();
            for id in &captured_ids {
                drain_container_pipes(containers, output_streams, id).await;
            }
            control.send(&GuestMessage::SuspendReady).await?;
            log::info!("sent SuspendReady, waiting for vCPU freeze");
        }
        HostMessage::Shutdown => {
            log::info!("shutdown requested");
            return Ok(Some(LoopExit::Shutdown));
        }
    }
    Ok(None)
}

/// Write pipe data to a container's yamux output stream using output chunk framing.
async fn write_output_chunk(
    stream: &mut yamux::Stream,
    stream_id: u8,
    data: &[u8],
) -> anyhow::Result<()> {
    let chunk = encode_output_chunk(stream_id, data);
    stream.write_all(&chunk).await.context("write output chunk")?;
    Ok(())
}

/// Read available data from a container's stdout/stderr pipes and forward to yamux output streams.
async fn drain_container_pipes(
    containers: &mut ContainerManager,
    output_streams: &mut HashMap<String, yamux::Stream>,
    id: &str,
) {
    if let Some(fd) = containers.stdout_raw_fd(id) {
        match util::read_pipe(fd) {
            Ok(ReadPipeResult::Data(data)) => {
                if let Some(stream) = output_streams.get_mut(id) {
                    if let Err(e) = write_output_chunk(stream, STREAM_STDOUT, &data).await {
                        log::warn!("write stdout to yamux for {}: {:#}", id, e);
                    }
                }
            }
            Ok(ReadPipeResult::Eof) => {
                containers.take_stdout_fd(id);
            }
            Ok(ReadPipeResult::WouldBlock) => {}
            Err(e) => log::warn!("read stdout pipe for {}: {}", id, e),
        }
    }
    if let Some(fd) = containers.stderr_raw_fd(id) {
        match util::read_pipe(fd) {
            Ok(ReadPipeResult::Data(data)) => {
                if let Some(stream) = output_streams.get_mut(id) {
                    if let Err(e) = write_output_chunk(stream, STREAM_STDERR, &data).await {
                        log::warn!("write stderr to yamux for {}: {:#}", id, e);
                    }
                }
            }
            Ok(ReadPipeResult::Eof) => {
                containers.take_stderr_fd(id);
            }
            Ok(ReadPipeResult::WouldBlock) => {}
            Err(e) => log::warn!("read stderr pipe for {}: {}", id, e),
        }
    }
}

/// Drain all remaining data from a container's pipes and close the yamux output stream (= EOF).
async fn drain_container_pipes_final(
    containers: &mut ContainerManager,
    output_streams: &mut HashMap<String, yamux::Stream>,
    id: &str,
) {
    if let Some(pipe) = containers.take_stdout_fd(id) {
        loop {
            match util::read_pipe(pipe.as_raw_fd()) {
                Ok(ReadPipeResult::Data(data)) => {
                    if let Some(stream) = output_streams.get_mut(id) {
                        if let Err(e) = write_output_chunk(stream, STREAM_STDOUT, &data).await {
                            log::warn!("write final stdout for {}: {:#}", id, e);
                            break;
                        }
                    }
                }
                _ => break,
            }
        }
    }
    if let Some(pipe) = containers.take_stderr_fd(id) {
        loop {
            match util::read_pipe(pipe.as_raw_fd()) {
                Ok(ReadPipeResult::Data(data)) => {
                    if let Some(stream) = output_streams.get_mut(id) {
                        if let Err(e) = write_output_chunk(stream, STREAM_STDERR, &data).await {
                            log::warn!("write final stderr for {}: {:#}", id, e);
                            break;
                        }
                    }
                }
                _ => break,
            }
        }
    }

    // Close the yamux output stream — the host sees EOF.
    if let Some(mut stream) = output_streams.remove(id) {
        if let Err(e) = stream.close().await {
            log::warn!("close yamux output stream for {}: {:#}", id, e);
        }
    }
}

/// Relay data from a yamux inbound stream to a container's stdin pipe.
///
/// Reads from the yamux stream and writes to the pipe fd. When the yamux
/// stream reaches EOF, the pipe write-end is dropped so the container sees
/// EOF on stdin.
async fn relay_stdin(mut yamux_stream: yamux::Stream, stdin_write_fd: OwnedFd) {
    use std::os::unix::io::AsRawFd;
    let fd = stdin_write_fd.as_raw_fd();
    let mut buf = [0u8; 8192];
    loop {
        let result = std::future::poll_fn(|cx| {
            Pin::new(&mut yamux_stream).poll_read(cx, &mut buf)
        }).await;
        match result {
            Ok(0) => break, // EOF from host
            Ok(n) => {
                // Write to pipe — blocking write is OK, pipe buffer is 64KB and chunks are small.
                let written = unsafe {
                    libc::write(fd, buf.as_ptr() as *const libc::c_void, n)
                };
                if written < 0 {
                    log::warn!("write to stdin pipe: {}", std::io::Error::last_os_error());
                    break;
                }
            }
            Err(e) => {
                log::warn!("read from yamux stdin stream: {}", e);
                break;
            }
        }
    }
    // Drop stdin_write_fd → container sees EOF on stdin.
}

/// Which event source became ready.
enum Ready {
    Signal,
    ControlMsg(anyhow::Result<HostMessage>),
    PipeReady,
    YamuxEvent,
}

fn run() -> anyhow::Result<()> {
    mount_essential_filesystems();

    let sigfd = util::setup_signalfd().context("setup signalfd")?;

    log::info!("starting vsock listener on port {}", VSOCK_CONTROL_PORT);
    let listener = vsock::VsockListener::bind(VSOCK_CONTROL_PORT)
        .context("bind vsock listener")?;

    let ex = LocalExecutor::new();

    future::block_on(ex.run(async {
        let sigfd = Async::new_nonblocking(sigfd).context("wrap signalfd in Async")?;

        // Containers persist across reconnects — they keep running through
        // suspend/resume cycles.
        let mut containers = ContainerManager::new();

        // Outer reconnect loop: each iteration accepts a new vsock connection.
        // On cold boot this runs once; on resume after suspend the guest loops
        // back here to accept the new host connection.
        loop {
            log::info!("waiting for host connection");
            // Blocking accept — OK because nothing useful can happen without
            // a host connection. Spawned tasks (relay_stdin) from a previous
            // session are dead (their yamux streams are gone).
            let accepted = listener.accept().context("accept vsock connection")?;
            let async_socket = Async::new(accepted).context("wrap vsock fd in Async")?;

            let mut conn = yamux::Connection::new(
                async_socket,
                yamux::Config::default(),
                yamux::Mode::Server,
            );

            // Accept the first inbound stream — the control stream.
            let mut control_stream = match std::future::poll_fn(|cx| conn.poll_next_inbound(cx)).await {
                Some(Ok(stream)) => stream,
                Some(Err(e)) => anyhow::bail!("yamux error accepting control stream: {}", e),
                None => anyhow::bail!("yamux connection closed before control stream"),
            };

            // Read the stream header and send Ready while driving the yamux connection.
            {
                let running_containers = containers.running_container_ids();
                let drive = async {
                    loop {
                        match std::future::poll_fn(|cx| conn.poll_next_inbound(cx)).await {
                            Some(Ok(_)) => log::warn!("unexpected inbound stream during init"),
                            Some(Err(e)) => return Err::<(), _>(anyhow::Error::from(e)),
                            None => return Err(anyhow::anyhow!("yamux closed during init")),
                        }
                    }
                };
                let init = async {
                    let header: StreamHeader = vsock::recv_msg(&mut control_stream)
                        .await
                        .context("read StreamHeader on control stream")?;
                    match header {
                        StreamHeader::Control => log::info!("control stream established"),
                        other => anyhow::bail!("expected StreamHeader::Control, got {:?}", other),
                    }
                    if running_containers.is_empty() {
                        log::info!("host connected, sending Ready (cold boot)");
                    } else {
                        log::info!("host reconnected, sending Ready (resume, running: {:?})", running_containers);
                    }
                    vsock::send_msg(&mut control_stream, &GuestMessage::Ready {
                        running_containers,
                    }).await?;
                    Ok(())
                };
                future::or(drive, init).await?;
            }

            let mut control = ControlReader::new(control_stream);

            // Per-connection state — yamux streams die with the connection.
            let mut output_streams: HashMap<String, yamux::Stream> = HashMap::new();
            let mut stdin_streams: HashMap<String, OwnedFd> = HashMap::new();

            let loop_exit = 'event_loop: loop {
                let ready = {
                    let sig_ready = async {
                        sigfd.readable().await.ok();
                        Ready::Signal
                    };
                    let yamux_drive = async {
                        match std::future::poll_fn(|cx| conn.poll_next_inbound(cx)).await {
                            Some(Ok(mut stream)) => {
                                match vsock::recv_msg::<StreamHeader>(&mut stream).await {
                                    Ok(StreamHeader::ContainerInput { container_id }) => {
                                        log::info!("received inbound stdin stream for container {}", container_id);
                                        if let Some(stdin_fd) = stdin_streams.remove(&container_id) {
                                            ex.spawn(relay_stdin(stream, stdin_fd)).detach();
                                        } else {
                                            log::warn!("no stdin pipe for container {}, dropping stream", container_id);
                                        }
                                    }
                                    Ok(other) => {
                                        log::warn!("unexpected inbound stream header: {:?}, dropping", other);
                                    }
                                    Err(e) => {
                                        log::warn!("failed to read inbound stream header: {:#}", e);
                                    }
                                }
                            }
                            Some(Err(e)) => {
                                log::error!("yamux connection error: {}", e);
                                return Ready::YamuxEvent;
                            }
                            None => {
                                log::info!("yamux connection closed");
                                return Ready::YamuxEvent;
                            }
                        }
                        Ready::PipeReady
                    };
                    let ctrl = std::future::poll_fn(|cx| {
                        control.poll_recv::<HostMessage>(cx).map(Ready::ControlMsg)
                    });
                    let pipe_ready = async {
                        let pipes = containers.pipe_refs();
                        if pipes.is_empty() {
                            future::pending::<()>().await;
                            return Ready::PipeReady;
                        }
                        let mut readables: Vec<_> = pipes.iter()
                            .map(|p| p.readable())
                            .collect();
                        std::future::poll_fn(|cx| {
                            for r in readables.iter_mut() {
                                if Pin::new(r).poll(cx).is_ready() {
                                    return Poll::Ready(());
                                }
                            }
                            Poll::Pending
                        }).await;
                        Ready::PipeReady
                    };

                    future::or(
                        future::or(sig_ready, yamux_drive),
                        future::or(ctrl, pipe_ready),
                    ).await
                };

                match ready {
                    Ready::Signal => {
                        util::drain_signalfd(&sigfd);

                        let exits = containers.reap_children();
                        let mut control_broken = false;
                        for exit in exits {
                            drain_container_pipes_final(&mut containers, &mut output_streams, &exit.id).await;
                            stdin_streams.remove(&exit.id);
                            containers.remove(&exit.id);

                            if let Err(e) = control.send(&GuestMessage::ContainerExited {
                                id: exit.id,
                                code: exit.code,
                            }).await {
                                log::error!("failed to send ContainerExited: {:#}", e);
                                control_broken = true;
                                break;
                            }
                        }
                        if control_broken {
                            break 'event_loop LoopExit::Disconnected;
                        }
                    }
                    Ready::ControlMsg(Ok(msg)) => {
                        log::info!("received: {:?}", msg);
                        match handle_message(msg, &mut control, &mut containers, &mut conn, &mut output_streams, &mut stdin_streams).await {
                            Ok(Some(exit)) => break 'event_loop exit,
                            Ok(None) => {}
                            Err(e) => {
                                log::error!("error handling message: {:#}", e);
                                let _ = control.send(&GuestMessage::Error {
                                    message: format!("{:#}", e),
                                }).await;
                            }
                        }
                    }
                    Ready::ControlMsg(Err(e)) => {
                        log::error!("control stream error: {:#}", e);
                        break 'event_loop LoopExit::Disconnected;
                    }
                    Ready::PipeReady => {
                        let captured_ids = containers.captured_container_ids();
                        for id in &captured_ids {
                            drain_container_pipes(&mut containers, &mut output_streams, id).await;
                        }
                    }
                    Ready::YamuxEvent => {
                        break 'event_loop LoopExit::Disconnected;
                    }
                }
            };

            match loop_exit {
                LoopExit::Shutdown => {
                    // Graceful shutdown: SIGTERM all containers, wait up to 5s, then SIGKILL.
                    if containers.has_running_containers() {
                        log::info!("sending SIGTERM to all running containers");
                        containers.signal_all_running(libc::SIGTERM);

                        let deadline = async_io::Timer::after(std::time::Duration::from_secs(5));
                        futures_lite::pin!(deadline);

                        loop {
                            if !containers.has_running_containers() {
                                log::info!("all containers exited after SIGTERM");
                                break;
                            }

                            let wait_result = future::or(
                                async {
                                    sigfd.readable().await.ok();
                                    true
                                },
                                async {
                                    (&mut deadline).await;
                                    false
                                },
                            ).await;

                            if !wait_result {
                                log::warn!("graceful shutdown timeout expired");
                                break;
                            }

                            util::drain_signalfd(&sigfd);
                            let exits = containers.reap_children();
                            for exit in exits {
                                drain_container_pipes_final(&mut containers, &mut output_streams, &exit.id).await;
                                stdin_streams.remove(&exit.id);
                                containers.remove(&exit.id);
                                // Control stream is dead in shutdown path, skip sending.
                            }
                        }

                        if containers.has_running_containers() {
                            log::warn!("sending SIGKILL to remaining containers");
                            containers.signal_all_running(libc::SIGKILL);

                            async_io::Timer::after(std::time::Duration::from_millis(100)).await;
                            util::drain_signalfd(&sigfd);
                            let exits = containers.reap_children();
                            for exit in exits {
                                drain_container_pipes_final(&mut containers, &mut output_streams, &exit.id).await;
                                stdin_streams.remove(&exit.id);
                                containers.remove(&exit.id);
                            }
                        }
                    }

                    // Brief sleep to let virtio-net flush outgoing packets.
                    async_io::Timer::after(std::time::Duration::from_millis(200)).await;
                    break; // Exit outer reconnect loop → reboot.
                }
                LoopExit::Disconnected => {
                    // Connection lost (suspend or unexpected disconnect).
                    // Drop yamux connection and per-connection state, loop back
                    // to accept a new connection. Containers keep running.
                    log::info!("connection lost, waiting for reconnect ({} containers still running)",
                        containers.running_container_ids().len());
                    // output_streams and stdin_streams are dropped here.
                    // Spawned relay_stdin tasks will fail on next poll (dead yamux)
                    // and exit, dropping their OwnedFd stdin pipe write-ends.
                    continue;
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
    unsafe { libc::sync(); }
    // Use reboot (not power-off): Firecracker doesn't support ACPI power-off,
    // so RB_POWER_OFF halts the vCPU but leaves the process running.
    // RB_AUTOBOOT triggers a triple fault which causes KVM/Firecracker to exit.
    unsafe { libc::reboot(libc::RB_AUTOBOOT); }
    loop {
        unsafe { libc::pause(); }
    }
}
