use anyhow::{bail, Context};
use futures_lite::io::{AsyncReadExt, AsyncWriteExt};
use serde::{Deserialize, Serialize};

/// Maximum message size: 16 MiB.
const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Send a length-prefixed postcard-serialized message.
pub async fn send_msg<W: AsyncWriteExt + Unpin, T: Serialize>(
    writer: &mut W,
    msg: &T,
) -> anyhow::Result<()> {
    let payload = postcard::to_allocvec(msg).context("serialize message")?;
    let len = (payload.len() as u32).to_le_bytes();
    writer.write_all(&len).await.context("write length")?;
    writer.write_all(&payload).await.context("write payload")?;
    writer.flush().await.context("flush")?;
    Ok(())
}

/// Receive a length-prefixed postcard-serialized message.
pub async fn recv_msg<R: AsyncReadExt + Unpin, T: for<'de> Deserialize<'de>>(
    reader: &mut R,
) -> anyhow::Result<T> {
    let mut len_buf = [0u8; 4];
    reader
        .read_exact(&mut len_buf)
        .await
        .context("read length")?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > MAX_MESSAGE_SIZE {
        bail!("message too large: {} bytes (max {})", len, MAX_MESSAGE_SIZE);
    }
    let mut buf = vec![0u8; len];
    reader
        .read_exact(&mut buf)
        .await
        .context("read payload")?;
    postcard::from_bytes(&buf).context("deserialize message")
}
