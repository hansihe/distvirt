use anyhow::{Context, bail};
use futures_lite::io::AsyncReadExt;

use distvirt_guest_protocol::{
    OUTPUT_CHUNK_HEADER_SIZE, STREAM_STDERR, STREAM_STDOUT, parse_output_chunk_header,
};

/// An event received from a container's output stream.
#[derive(Debug)]
pub enum IoEvent {
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
    Eof,
}

/// Host-side I/O session wrapping a yamux output stream from the guest.
///
/// The stream header has already been consumed by `GuestSession::accept_output_stream()`.
/// This reads output chunks `[stream_id: u8][u32 LE length][payload]` until EOF.
pub struct IoSession {
    stream: yamux::Stream,
}

impl IoSession {
    pub fn new(stream: yamux::Stream) -> Self {
        IoSession { stream }
    }

    /// Read the next output event from the stream.
    ///
    /// Returns `IoEvent::Eof` when the guest closes the yamux stream
    /// (container has exited and all output has been sent).
    pub async fn next_event(&mut self) -> anyhow::Result<IoEvent> {
        let mut header = [0u8; OUTPUT_CHUNK_HEADER_SIZE];
        match self.stream.read_exact(&mut header).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(IoEvent::Eof);
            }
            Err(e) => {
                return Err(e).context("read output chunk header");
            }
        }

        let (stream_id, payload_len) = parse_output_chunk_header(&header);
        let payload_len = payload_len as usize;

        // Safety limit: reject chunks > 16 MiB to prevent a malicious or
        // buggy guest from exhausting host memory with a single allocation.
        if payload_len > 16 * 1024 * 1024 {
            bail!("output chunk payload too large: {} bytes", payload_len);
        }

        let mut payload = vec![0u8; payload_len];
        if payload_len > 0 {
            self.stream
                .read_exact(&mut payload)
                .await
                .context("read output chunk payload")?;
        }

        match stream_id {
            STREAM_STDOUT => Ok(IoEvent::Stdout(payload)),
            STREAM_STDERR => Ok(IoEvent::Stderr(payload)),
            other => bail!("unknown output stream id: {}", other),
        }
    }
}
