use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::UnixStream;

use anyhow::{bail, Context};

use distvirt_guest_protocol::{
    IoMode, IoSessionRequest, IoSessionResponse, STREAM_EOF, STREAM_STDERR, STREAM_STDOUT,
    IO_FRAME_HEADER_SIZE, IO_FRAME_MAX_PAYLOAD, parse_io_frame_header,
};

/// An event received from the guest I/O stream.
#[derive(Debug)]
pub enum IoEvent {
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
    Eof,
}

/// Host-side I/O session connected to a container in the guest.
pub struct IoSession {
    reader: BufReader<tokio::net::unix::OwnedReadHalf>,
    #[allow(dead_code)]
    writer: BufWriter<tokio::net::unix::OwnedWriteHalf>,
}

impl IoSession {
    /// Connect to a container's I/O stream via vsock.
    ///
    /// Performs the handshake (send IoSessionRequest, receive IoSessionResponse),
    /// then returns a session ready for reading I/O events.
    pub async fn connect(stream: UnixStream, container_id: &str, mode: IoMode) -> anyhow::Result<Self> {
        let (read_half, write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);
        let mut writer = BufWriter::new(write_half);

        // Send handshake request (length-prefixed JSON, same format as control channel).
        let request = IoSessionRequest {
            container_id: container_id.to_string(),
            mode,
        };
        let json = serde_json::to_vec(&request).context("serialize IoSessionRequest")?;
        let len = (json.len() as u32).to_le_bytes();
        writer.write_all(&len).await.context("write request length")?;
        writer.write_all(&json).await.context("write request payload")?;
        writer.flush().await.context("flush request")?;

        // Read handshake response.
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf).await.context("read response length")?;
        let resp_len = u32::from_le_bytes(len_buf) as usize;
        if resp_len > 1024 * 1024 {
            bail!("I/O session response too large: {} bytes", resp_len);
        }
        let mut resp_buf = vec![0u8; resp_len];
        reader.read_exact(&mut resp_buf).await.context("read response payload")?;
        let response: IoSessionResponse =
            serde_json::from_slice(&resp_buf).context("deserialize IoSessionResponse")?;

        if !response.ok {
            bail!(
                "I/O session rejected: {}",
                response.error.unwrap_or_else(|| "unknown error".to_string())
            );
        }

        Ok(IoSession { reader, writer })
    }

    /// Read the next I/O event from the stream.
    ///
    /// Returns `IoEvent::Eof` when the container has exited and all output has been sent.
    /// Returns an error if the connection is broken.
    pub async fn next_event(&mut self) -> anyhow::Result<IoEvent> {
        // Read frame header: [1 byte stream_id] [2 bytes LE length]
        let mut header = [0u8; IO_FRAME_HEADER_SIZE];
        self.reader
            .read_exact(&mut header)
            .await
            .context("read I/O frame header")?;

        let (stream_id, payload_len_u16) = parse_io_frame_header(&header);
        let payload_len = payload_len_u16 as usize;

        if stream_id == STREAM_EOF {
            return Ok(IoEvent::Eof);
        }

        if payload_len > IO_FRAME_MAX_PAYLOAD {
            bail!("I/O frame payload too large: {} bytes", payload_len);
        }

        let mut payload = vec![0u8; payload_len];
        if payload_len > 0 {
            self.reader
                .read_exact(&mut payload)
                .await
                .context("read I/O frame payload")?;
        }

        match stream_id {
            STREAM_STDOUT => Ok(IoEvent::Stdout(payload)),
            STREAM_STDERR => Ok(IoEvent::Stderr(payload)),
            other => bail!("unknown I/O stream id: {}", other),
        }
    }
}
