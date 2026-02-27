use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::UnixStream;

use anyhow::{bail, Context};

/// Host-side connection to the guest over vsock.
///
/// Uses 4-byte LE length-prefixed JSON, matching the guest's wire format.
pub struct GuestConnection {
    reader: BufReader<tokio::net::unix::OwnedReadHalf>,
    writer: BufWriter<tokio::net::unix::OwnedWriteHalf>,
}

impl GuestConnection {
    pub fn new(stream: UnixStream) -> Self {
        let (read_half, write_half) = stream.into_split();
        let reader = BufReader::new(read_half);
        let writer = BufWriter::new(write_half);
        GuestConnection { reader, writer }
    }

    /// Send a length-prefixed JSON message.
    pub async fn send<T: serde::Serialize>(&mut self, msg: &T) -> anyhow::Result<()> {
        let json = serde_json::to_vec(msg).context("serialize message")?;
        let len = (json.len() as u32).to_le_bytes();
        self.writer.write_all(&len).await.context("write length")?;
        self.writer.write_all(&json).await.context("write payload")?;
        self.writer.flush().await.context("flush")?;
        Ok(())
    }

    /// Receive a length-prefixed JSON message.
    pub async fn recv<T: serde::de::DeserializeOwned>(&mut self) -> anyhow::Result<T> {
        let mut len_buf = [0u8; 4];
        self.reader
            .read_exact(&mut len_buf)
            .await
            .context("read length")?;
        let len = u32::from_le_bytes(len_buf) as usize;
        if len > 1024 * 1024 {
            bail!("message too large: {} bytes", len);
        }
        let mut buf = vec![0u8; len];
        self.reader.read_exact(&mut buf).await.context("read payload")?;
        serde_json::from_slice(&buf).context("deserialize message")
    }
}
