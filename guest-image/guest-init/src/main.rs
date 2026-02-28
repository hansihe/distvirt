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

async fn handle_message(
    msg: HostMessage,
    control: &mut ControlReader,
    containers: &mut ContainerManager,
    conn: &mut yamux::Connection<Async<std::fs::File>>,
    output_streams: &mut HashMap<String, yamux::Stream>,
) -> anyhow::Result<bool> {
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
        HostMessage::StartContainer { id, entrypoint, args, env, working_dir, uid, gid, hostname, capture_output } => {
            log::info!("StartContainer: id={}, entrypoint={}, capture_output={}", id, entrypoint, capture_output);

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

            match containers.start(&id, &entrypoint, &args, &env, working_dir.as_deref(), uid, gid, hostname.as_deref(), capture_output) {
                Ok(pid) => {
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
        HostMessage::Shutdown => {
            log::info!("shutdown requested");
            return Ok(true);
        }
    }
    Ok(false)
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

    log::info!("waiting for host connection");
    let accepted = listener.accept().context("accept vsock connection")?;

    // Wrap accepted fd in Async for yamux.
    let async_socket = Async::new(accepted).context("wrap vsock fd in Async")?;

    // Create yamux connection in server mode (guest = server).
    let mut conn = yamux::Connection::new(
        async_socket,
        yamux::Config::default(),
        yamux::Mode::Server,
    );

    let ex = LocalExecutor::new();

    future::block_on(ex.run(async {
        // Accept the first inbound stream from the host — this is the control stream.
        // poll_next_inbound both drives the connection and accepts streams, so
        // no additional driver is needed for this step.
        let mut control_stream = match std::future::poll_fn(|cx| conn.poll_next_inbound(cx)).await {
            Some(Ok(stream)) => stream,
            Some(Err(e)) => anyhow::bail!("yamux error accepting control stream: {}", e),
            None => anyhow::bail!("yamux connection closed before control stream"),
        };

        // Read the stream header and send Ready while driving the yamux connection.
        // The connection must be polled for stream reads/writes to make progress,
        // so we race the init work against the connection driver.
        {
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
                log::info!("host connected, sending Ready");
                vsock::send_msg(&mut control_stream, &GuestMessage::Ready).await?;
                Ok(())
            };
            future::or(drive, init).await?;
        }

        // Wrap in ControlReader for safe buffered reads inside the main select.
        let mut control = ControlReader::new(control_stream);

        let mut containers = ContainerManager::new();
        let mut output_streams: HashMap<String, yamux::Stream> = HashMap::new();

        let sigfd = Async::new_nonblocking(sigfd).context("wrap signalfd in Async")?;

        loop {
            // Wait for the first event source to become ready.
            //
            // The yamux_drive arm polls poll_next_inbound which drives the yamux
            // connection (reads from the socket, dispatches data to stream buffers).
            // The ctrl arm's poll_recv then finds data in the control stream buffer.
            //
            // future::or polls all arms each time the task is polled, so yamux is
            // driven even while we wait for signals or pipe data.
            let ready = {
                let sig_ready = async {
                    sigfd.readable().await.ok();
                    Ready::Signal
                };
                let yamux_drive = async {
                    match std::future::poll_fn(|cx| conn.poll_next_inbound(cx)).await {
                        Some(Ok(_stream)) => {
                            log::warn!("unexpected inbound yamux stream, dropping");
                        }
                        Some(Err(e)) => {
                            log::error!("yamux connection error: {}", e);
                        }
                        None => {
                            log::info!("yamux connection closed");
                        }
                    }
                    Ready::YamuxEvent
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
                        break;
                    }
                }
                Ready::ControlMsg(Ok(msg)) => {
                    log::info!("received: {:?}", msg);
                    match handle_message(msg, &mut control, &mut containers, &mut conn, &mut output_streams).await {
                        Ok(true) => break,
                        Ok(false) => {}
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
                    break;
                }
                Ready::PipeReady => {
                    let captured_ids = containers.captured_container_ids();
                    for id in &captured_ids {
                        drain_container_pipes(&mut containers, &mut output_streams, id).await;
                    }
                }
                Ready::YamuxEvent => {
                    // Connection closed or unexpected inbound — break out of loop.
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

    log::info!("powering off");
    unsafe { libc::sync(); }
    unsafe { libc::reboot(libc::RB_POWER_OFF); }
    loop {
        unsafe { libc::pause(); }
    }
}
