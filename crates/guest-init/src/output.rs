use std::os::unix::io::{AsRawFd, OwnedFd};
use std::pin::Pin;

use async_executor::LocalExecutor;
use async_io::Async;
use futures::FutureExt;
use futures::io::{AsyncRead, AsyncWriteExt};

use crate::container::PipeFd;
use crate::util::ReadPipeResult;
use crate::vsock;
use distvirt_guest_protocol::{GuestEvent, STREAM_STDERR, STREAM_STDOUT, encode_output_chunk};

// ---------------------------------------------------------------------------
// Fill task — per-container lifetime, survives reconnects
// ---------------------------------------------------------------------------

/// Handle for a spawned per-container fill task.
pub struct FillTaskHandle {
    exit_tx: async_channel::Sender<()>,
    done_rx: async_channel::Receiver<u64>,
}

impl FillTaskHandle {
    /// Signal the fill task to perform a final drain of pipes into the buffer,
    /// then wait for it to complete.
    ///
    /// Returns the number of output bytes dropped during the final drain
    /// (zero if everything was buffered successfully).
    pub async fn signal_exit(self) -> u64 {
        let _ = self.exit_tx.send(()).await;
        self.done_rx.recv().await.unwrap_or(0)
    }
}

/// Spawn a fill task that reads from stdout/stderr pipes, encodes output
/// chunks, and sends them into the output buffer channel.
///
/// The task runs for the lifetime of the container. On disconnect, it keeps
/// running — chunks accumulate in the buffer until a new connection drains them.
pub fn spawn_fill_task(
    id: String,
    stdout: Option<Async<PipeFd>>,
    stderr: Option<Async<PipeFd>>,
    buffer_tx: async_channel::Sender<Vec<u8>>,
    ex: &LocalExecutor<'_>,
) -> FillTaskHandle {
    let (exit_tx, exit_rx) = async_channel::bounded::<()>(1);
    let (done_tx, done_rx) = async_channel::bounded::<u64>(1);

    ex.spawn(fill_loop(id, stdout, stderr, buffer_tx, exit_rx, done_tx))
        .detach();

    FillTaskHandle { exit_tx, done_rx }
}

/// Internal fill loop: read pipes, encode chunks, send to buffer.
///
/// Backpressure chain: when the output buffer is full, `buffer_tx.send().await`
/// blocks → fill task stops reading pipes → kernel pipe buffer fills →
/// container's write() to stdout/stderr blocks. This is intentional — it
/// prevents unbounded memory growth while the guest is disconnected.
async fn fill_loop(
    id: String,
    mut stdout: Option<Async<PipeFd>>,
    mut stderr: Option<Async<PipeFd>>,
    buffer_tx: async_channel::Sender<Vec<u8>>,
    exit_rx: async_channel::Receiver<()>,
    done_tx: async_channel::Sender<u64>,
) {
    loop {
        let stdout_ready = async {
            if let Some(ref p) = stdout {
                p.readable().await.ok();
            } else {
                futures::future::pending::<()>().await;
            }
        };
        let stderr_ready = async {
            if let Some(ref p) = stderr {
                p.readable().await.ok();
            } else {
                futures::future::pending::<()>().await;
            }
        };
        let exit = async { exit_rx.recv().await };

        // If both pipes are gone, just wait for exit signal.
        if stdout.is_none() && stderr.is_none() {
            match exit_rx.recv().await {
                Ok(()) | Err(_) => {
                    let _ = done_tx.send(0).await;
                    return;
                }
            }
        }

        enum Action {
            Stdout,
            Stderr,
            ExitSignal,
            Disconnected,
        }

        let action = futures::future::select(
            std::pin::pin!(
                futures::future::select(
                    std::pin::pin!(stdout_ready.map(|_| Action::Stdout)),
                    std::pin::pin!(stderr_ready.map(|_| Action::Stderr)),
                )
                .map(|either| either.factor_first().0)
            ),
            std::pin::pin!(exit.map(|r| match r {
                Ok(()) => Action::ExitSignal,
                Err(_) => Action::Disconnected,
            })),
        )
        .await
        .factor_first()
        .0;

        match action {
            Action::Stdout => {
                if !drain_pipe_to_buffer(&mut stdout, &buffer_tx, STREAM_STDOUT, &id).await {
                    stdout = None;
                }
            }
            Action::Stderr => {
                if !drain_pipe_to_buffer(&mut stderr, &buffer_tx, STREAM_STDERR, &id).await {
                    stderr = None;
                }
            }
            Action::ExitSignal | Action::Disconnected => {
                // Final drain: read all remaining pipe data into the buffer.
                // Track bytes that couldn't be buffered (e.g. buffer full while disconnected).
                let d1 = final_drain_to_buffer(&mut stdout, &buffer_tx, STREAM_STDOUT, &id).await;
                let d2 = final_drain_to_buffer(&mut stderr, &buffer_tx, STREAM_STDERR, &id).await;
                let _ = done_tx.send(d1 + d2).await;
                return;
            }
        }
    }
}

/// Read available data from a pipe, encode as an output chunk, and send to the buffer.
/// Returns false if the pipe reached EOF.
async fn drain_pipe_to_buffer(
    pipe: &mut Option<Async<PipeFd>>,
    buffer_tx: &async_channel::Sender<Vec<u8>>,
    stream_id: u8,
    container_id: &str,
) -> bool {
    let p = match pipe {
        Some(p) => p,
        None => return false,
    };
    match crate::util::read_pipe(p.as_raw_fd()) {
        Ok(ReadPipeResult::Data(data)) => {
            let chunk = encode_output_chunk(stream_id, &data);
            // Backpressure: blocks when buffer is full.
            if buffer_tx.send(chunk).await.is_err() {
                log::warn!("output buffer closed for {}", container_id);
            }
            true
        }
        Ok(ReadPipeResult::Eof) => false,
        Ok(ReadPipeResult::WouldBlock) => true,
        Err(e) => {
            let name = if stream_id == STREAM_STDOUT {
                "stdout"
            } else {
                "stderr"
            };
            log::warn!("read {} pipe for {}: {}", name, container_id, e);
            true
        }
    }
}

/// Drain all remaining data from a pipe until EOF, encoding into the buffer.
///
/// Keeps the `Async` wrapper alive so we can await readability on WouldBlock
/// instead of losing data still in kernel pipe buffers. Uses `try_send` to
/// avoid deadlocking if the output buffer is full (container has exited, no
/// drain task may be running).
///
/// Returns the number of payload bytes that could not be buffered (dropped).
/// The pipe is always drained to EOF so the total loss is accurately counted.
async fn final_drain_to_buffer(
    pipe: &mut Option<Async<PipeFd>>,
    buffer_tx: &async_channel::Sender<Vec<u8>>,
    stream_id: u8,
    container_id: &str,
) -> u64 {
    let p = match pipe.take() {
        Some(p) => p,
        None => return 0,
    };
    let mut bytes_dropped: u64 = 0;
    loop {
        match crate::util::read_pipe(p.as_raw_fd()) {
            Ok(ReadPipeResult::Data(data)) => {
                let len = data.len() as u64;
                let chunk = encode_output_chunk(stream_id, &data);
                if buffer_tx.try_send(chunk).is_err() {
                    bytes_dropped += len;
                    // Continue draining pipe to count total loss.
                }
            }
            Ok(ReadPipeResult::WouldBlock) => {
                // Wait for more data to arrive in the kernel pipe buffer.
                if p.readable().await.is_err() {
                    break;
                }
            }
            Ok(ReadPipeResult::Eof) | Err(_) => break,
        }
    }
    if bytes_dropped > 0 {
        log::warn!(
            "output buffer full during final drain for {}: {} bytes dropped",
            container_id, bytes_dropped
        );
    }
    bytes_dropped
}

// ---------------------------------------------------------------------------
// Drain tasks — per-connection lifetime, spawned on connect
// ---------------------------------------------------------------------------

/// Drain pre-framed output chunks from a buffer to a yamux stream.
///
/// Runs until the buffer channel is closed (container exited and fill task
/// finished) or the yamux write fails (disconnect). On yamux error, chunks
/// remain in the channel for the next connection's drain task.
pub async fn drain_output_to_yamux(
    id: String,
    buffer_rx: async_channel::Receiver<Vec<u8>>,
    mut stream: yamux::Stream,
) {
    while let Ok(chunk) = buffer_rx.recv().await {
        if let Err(e) = stream.write_all(&chunk).await {
            log::warn!("drain output to yamux for {}: {:#}", id, e);
            // Yamux dead — return. Remaining chunks stay in the channel.
            return;
        }
    }
    // Channel closed — container exited and fill task drained everything.
    if let Err(e) = stream.close().await {
        log::warn!("close yamux output stream for {}: {:#}", id, e);
    }
}

/// Drain guest events from the event buffer to the yamux event stream.
pub async fn drain_events_to_yamux(
    event_rx: async_channel::Receiver<GuestEvent>,
    mut stream: yamux::Stream,
) {
    while let Ok(event) = event_rx.recv().await {
        if let Err(e) = vsock::send_msg(&mut stream, &event).await {
            log::error!("drain event to yamux: {:#}", e);
            return;
        }
    }
}

// ---------------------------------------------------------------------------
// Stdin relay — unchanged
// ---------------------------------------------------------------------------

/// Relay data from a yamux inbound stream to a container's stdin pipe.
///
/// The stdin pipe fd is already O_NONBLOCK (set at creation time in
/// ContainerManager::start — dup shares the file description). We wrap it
/// in `Async` so writes yield to the reactor instead of blocking.
pub async fn relay_stdin(mut yamux_stream: yamux::Stream, stdin_write_fd: OwnedFd) {
    use std::os::unix::io::AsRawFd;

    let async_fd = match Async::new_nonblocking(stdin_write_fd) {
        Ok(fd) => fd,
        Err(e) => {
            log::warn!("wrap stdin pipe in Async: {}", e);
            return;
        }
    };

    let mut buf = [0u8; 8192];
    loop {
        let result =
            std::future::poll_fn(|cx| Pin::new(&mut yamux_stream).poll_read(cx, &mut buf)).await;
        match result {
            Ok(0) => break, // EOF from host
            Ok(n) => {
                let mut offset = 0;
                while offset < n {
                    match async_fd
                        .write_with(|fd| {
                            let written = unsafe {
                                libc::write(
                                    fd.as_raw_fd(),
                                    buf[offset..n].as_ptr() as *const libc::c_void,
                                    n - offset,
                                )
                            };
                            if written < 0 {
                                Err(std::io::Error::last_os_error())
                            } else {
                                Ok(written as usize)
                            }
                        })
                        .await
                    {
                        Ok(written) => offset += written,
                        Err(e) => {
                            log::warn!("write to stdin pipe: {}", e);
                            return;
                        }
                    }
                }
            }
            Err(e) => {
                log::warn!("read from yamux stdin stream: {}", e);
                break;
            }
        }
    }
    // Drop async_fd -> closes stdin_write_fd -> container sees EOF on stdin.
}
