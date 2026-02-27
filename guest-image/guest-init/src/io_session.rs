use std::collections::{HashMap, HashSet, VecDeque};
use std::os::unix::io::RawFd;

use anyhow::Context;

use distvirt_guest_protocol::{
    IoSessionRequest, IoSessionResponse, STREAM_STDERR, STREAM_STDOUT, VSOCK_IO_PORT,
    encode_eof_frame, encode_io_frames,
};

use crate::vsock::{VsockListener, VsockStream};

/// Maximum buffer size per stream before dropping oldest data.
const MAX_BUFFER_SIZE: usize = 64 * 1024;

/// Per-container output buffer (used when no session is connected).
struct OutputBuffer {
    stdout: VecDeque<u8>,
    stderr: VecDeque<u8>,
}

impl OutputBuffer {
    fn new() -> Self {
        OutputBuffer {
            stdout: VecDeque::new(),
            stderr: VecDeque::new(),
        }
    }

    fn push(&mut self, stream_id: u8, data: &[u8]) {
        let buf = match stream_id {
            STREAM_STDOUT => &mut self.stdout,
            STREAM_STDERR => &mut self.stderr,
            _ => return,
        };
        // If adding this data would exceed max, drop from head.
        let overflow = (buf.len() + data.len()).saturating_sub(MAX_BUFFER_SIZE);
        if overflow > 0 {
            buf.drain(..overflow.min(buf.len()));
        }
        buf.extend(data);
    }

    fn drain_stdout(&mut self) -> Vec<u8> {
        self.stdout.drain(..).collect()
    }

    fn drain_stderr(&mut self) -> Vec<u8> {
        self.stderr.drain(..).collect()
    }

    fn is_empty(&self) -> bool {
        self.stdout.is_empty() && self.stderr.is_empty()
    }
}

/// An active I/O session connected to a container.
struct IoSession {
    stream: VsockStream,
    #[allow(dead_code)]
    container_id: String,
}

/// Manages I/O sessions on the vsock I/O port.
pub struct IoSessionManager {
    listener: VsockListener,
    /// Active session per container (only one session per container at a time).
    sessions: HashMap<String, IoSession>,
    /// Buffered output for containers without an active session.
    buffers: HashMap<String, OutputBuffer>,
    /// Containers that have exited but whose buffer hasn't been consumed by a session yet.
    exited_containers: HashSet<String>,
}

impl IoSessionManager {
    pub fn new() -> anyhow::Result<Self> {
        let listener = VsockListener::bind(VSOCK_IO_PORT)
            .context("bind vsock I/O listener")?;
        listener.set_nonblocking()
            .context("set I/O listener non-blocking")?;
        log::info!("I/O session listener bound on port {}", VSOCK_IO_PORT);
        Ok(IoSessionManager {
            listener,
            sessions: HashMap::new(),
            buffers: HashMap::new(),
            exited_containers: HashSet::new(),
        })
    }

    /// Get the listener fd for polling.
    pub fn listener_fd(&self) -> RawFd {
        self.listener.as_raw_fd()
    }

    /// Get all active session fds for polling (to detect disconnects).
    pub fn session_fds(&self) -> Vec<(RawFd, String)> {
        self.sessions
            .iter()
            .map(|(id, s)| (s.stream.as_raw_fd(), id.clone()))
            .collect()
    }

    /// Try to accept a new connection and perform handshake.
    /// Returns Ok(true) if a session was accepted, Ok(false) if EAGAIN.
    pub fn try_accept(&mut self) -> anyhow::Result<bool> {
        let stream = match self.listener.accept_nonblocking() {
            Ok(Some(s)) => s,
            Ok(None) => return Ok(false),
            Err(e) => return Err(e).context("accept I/O connection"),
        };

        // Read the handshake request (length-prefixed JSON).
        let mut stream = stream;
        let request: IoSessionRequest = match stream.recv() {
            Ok(r) => r,
            Err(e) => {
                log::warn!("I/O session handshake failed: {:#}", e);
                return Ok(true);
            }
        };

        log::info!(
            "I/O session request: container={}, mode={:?}",
            request.container_id,
            request.mode
        );

        // Remove any existing session for this container.
        self.sessions.remove(&request.container_id);

        // Send success response.
        if let Err(e) = stream.send(&IoSessionResponse {
            ok: true,
            error: None,
        }) {
            log::warn!("failed to send I/O session response: {:#}", e);
            return Ok(true);
        }

        // Flush any buffered data for this container.
        if let Some(mut buf) = self.buffers.remove(&request.container_id) {
            if !buf.is_empty() {
                let stdout_data = buf.drain_stdout();
                if !stdout_data.is_empty() {
                    if let Err(e) = write_frames(&mut stream, STREAM_STDOUT, &stdout_data) {
                        log::warn!("failed to flush buffered stdout for {}: {:#}", request.container_id, e);
                    }
                }
                let stderr_data = buf.drain_stderr();
                if !stderr_data.is_empty() {
                    if let Err(e) = write_frames(&mut stream, STREAM_STDERR, &stderr_data) {
                        log::warn!("failed to flush buffered stderr for {}: {:#}", request.container_id, e);
                    }
                }
            }
        }

        // If the container already exited, send EOF and don't keep the session.
        if self.exited_containers.remove(&request.container_id) {
            log::info!("I/O session: container {} already exited, sending EOF", request.container_id);
            if let Err(e) = write_eof_frame(&mut stream) {
                log::warn!("failed to send EOF for {}: {:#}", request.container_id, e);
            }
            return Ok(true);
        }
        self.sessions.insert(
            request.container_id.clone(),
            IoSession {
                stream,
                container_id: request.container_id,
            },
        );

        Ok(true)
    }

    /// Forward data from a pipe to the appropriate session or buffer.
    pub fn forward_pipe_data(
        &mut self,
        container_id: &str,
        stream_id: u8,
        data: &[u8],
    ) {
        if data.is_empty() {
            return;
        }

        if let Some(session) = self.sessions.get_mut(container_id) {
            if let Err(e) = write_frames(&mut session.stream, stream_id, data) {
                log::warn!(
                    "failed to write to I/O session for {}: {:#}",
                    container_id,
                    e
                );
                // Session is broken, remove it and buffer instead.
                self.sessions.remove(container_id);
                let buf = self.buffers
                    .entry(container_id.to_string())
                    .or_insert_with(OutputBuffer::new);
                buf.push(stream_id, data);
            }
        } else {
            // No active session — buffer the data.
            let buf = self.buffers
                .entry(container_id.to_string())
                .or_insert_with(OutputBuffer::new);
            buf.push(stream_id, data);
        }
    }

    /// Send EOF frame and clean up session for a container that has exited.
    /// If no session is connected, keep the buffer so a late-connecting session can consume it.
    pub fn container_exited(&mut self, container_id: &str) {
        if let Some(mut session) = self.sessions.remove(container_id) {
            if let Err(e) = write_eof_frame(&mut session.stream) {
                log::warn!("failed to send EOF for {}: {:#}", container_id, e);
            }
            self.buffers.remove(container_id);
        } else {
            // No active session — mark as exited so try_accept() can send EOF after flushing.
            self.exited_containers.insert(container_id.to_string());
        }
    }

    /// Check if a session fd has disconnected. Returns container IDs of disconnected sessions.
    pub fn check_disconnects(&mut self, readable_session_fds: &[RawFd]) -> Vec<String> {
        let mut disconnected = Vec::new();
        for &fd in readable_session_fds {
            // Find which session this fd belongs to.
            let container_id = self
                .sessions
                .iter()
                .find(|(_, s)| s.stream.as_raw_fd() == fd)
                .map(|(id, _)| id.clone());

            if let Some(id) = container_id {
                // Try to read — if we get 0 bytes or error, it's disconnected.
                let mut buf = [0u8; 1];
                let n = unsafe {
                    libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len())
                };
                if n <= 0 {
                    log::info!("I/O session for {} disconnected", id);
                    disconnected.push(id);
                }
                // If n > 0, we got data — for now we ignore stdin.
            }
        }
        for id in &disconnected {
            self.sessions.remove(id);
        }
        disconnected
    }
}

/// Write data as one or more I/O frames.
fn write_frames(stream: &mut VsockStream, stream_id: u8, data: &[u8]) -> anyhow::Result<()> {
    for frame in encode_io_frames(stream_id, data) {
        stream.write_raw(&frame).context("write I/O frame")?;
    }
    Ok(())
}

/// Write an EOF frame (stream_id=0, length=0).
fn write_eof_frame(stream: &mut VsockStream) -> anyhow::Result<()> {
    stream.write_raw(&encode_eof_frame()).context("write EOF frame")?;
    Ok(())
}
