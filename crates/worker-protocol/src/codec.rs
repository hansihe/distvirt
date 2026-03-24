//! Length-prefixed Cap'n Proto codec for the control stream.
//!
//! All messages on the yamux control stream are framed as
//! `[u32 LE length][capnp message bytes]`. Log streams use the same framing
//! for the initial [`LogStreamHeader`](crate::LogStreamHeader), then raw bytes.

use anyhow::{Context, bail};
use capnp::message::{self, ReaderOptions};
use futures_lite::io::{AsyncReadExt, AsyncWriteExt};

use crate::convert;
use crate::types::*;
use crate::worker_protocol_capnp as schema;

/// Maximum message size: 16 MiB.
const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Send a capnp message with length-prefix framing.
async fn send_capnp_msg<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    msg: &message::Builder<message::HeapAllocator>,
) -> anyhow::Result<()> {
    let mut payload = Vec::new();
    capnp::serialize::write_message(&mut payload, msg).context("serialize capnp message")?;
    let len = (payload.len() as u32).to_le_bytes();
    writer.write_all(&len).await.context("write length")?;
    writer.write_all(&payload).await.context("write payload")?;
    writer.flush().await.context("flush")?;
    Ok(())
}

/// Receive a capnp message with length-prefix framing.
async fn recv_capnp_msg<R: AsyncReadExt + Unpin>(
    reader: &mut R,
) -> anyhow::Result<message::Reader<capnp::serialize::OwnedSegments>> {
    let mut len_buf = [0u8; 4];
    reader
        .read_exact(&mut len_buf)
        .await
        .context("read length")?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > MAX_MESSAGE_SIZE {
        bail!(
            "message too large: {} bytes (max {})",
            len,
            MAX_MESSAGE_SIZE
        );
    }
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await.context("read payload")?;
    let msg = capnp::serialize::read_message(&mut buf.as_slice(), ReaderOptions::new())
        .context("deserialize capnp message")?;
    Ok(msg)
}

// --- WorkerCommand ---

/// Send a [`WorkerCommand`] as a length-prefixed capnp message.
pub async fn send_worker_command<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    cmd: &WorkerCommand,
) -> anyhow::Result<()> {
    let mut msg = message::Builder::new_default();
    convert::write_worker_command(msg.init_root::<schema::worker_command::Builder<'_>>(), cmd);
    send_capnp_msg(writer, &msg).await
}

/// Receive a [`WorkerCommand`] from a length-prefixed capnp message.
pub async fn recv_worker_command<R: AsyncReadExt + Unpin>(
    reader: &mut R,
) -> anyhow::Result<WorkerCommand> {
    let msg = recv_capnp_msg(reader).await?;
    let root = msg
        .get_root::<schema::worker_command::Reader<'_>>()
        .context("read WorkerCommand root")?;
    convert::read_worker_command(root).context("decode WorkerCommand")
}

// --- WorkerEvent ---

/// Send a [`WorkerEvent`] as a length-prefixed capnp message.
pub async fn send_worker_event<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    event: &WorkerEvent,
) -> anyhow::Result<()> {
    let mut msg = message::Builder::new_default();
    convert::write_worker_event(msg.init_root::<schema::worker_event::Builder<'_>>(), event);
    send_capnp_msg(writer, &msg).await
}

/// Receive a [`WorkerEvent`] from a length-prefixed capnp message.
pub async fn recv_worker_event<R: AsyncReadExt + Unpin>(
    reader: &mut R,
) -> anyhow::Result<WorkerEvent> {
    let msg = recv_capnp_msg(reader).await?;
    let root = msg
        .get_root::<schema::worker_event::Reader<'_>>()
        .context("read WorkerEvent root")?;
    convert::read_worker_event(root).context("decode WorkerEvent")
}

// --- LogStreamHeader ---

/// Send a [`LogStreamHeader`] as a length-prefixed capnp message.
pub async fn send_log_header<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    header: &LogStreamHeader,
) -> anyhow::Result<()> {
    let mut msg = message::Builder::new_default();
    convert::write_log_stream_header(
        &mut msg.init_root::<schema::log_stream_header::Builder<'_>>(),
        header,
    );
    send_capnp_msg(writer, &msg).await
}

/// Receive a [`LogStreamHeader`] from a length-prefixed capnp message.
pub async fn recv_log_header<R: AsyncReadExt + Unpin>(
    reader: &mut R,
) -> anyhow::Result<LogStreamHeader> {
    let msg = recv_capnp_msg(reader).await?;
    let root = msg
        .get_root::<schema::log_stream_header::Reader<'_>>()
        .context("read LogStreamHeader root")?;
    convert::read_log_stream_header(root).context("decode LogStreamHeader")
}

// --- Handshake Messages ---

/// Send a [`WorkerHello`] as a length-prefixed capnp message.
pub async fn send_worker_hello<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    hello: &WorkerHello,
) -> anyhow::Result<()> {
    let mut msg = message::Builder::new_default();
    convert::write_worker_hello(
        &mut msg.init_root::<schema::worker_hello::Builder<'_>>(),
        hello,
    );
    send_capnp_msg(writer, &msg).await
}

/// Receive a [`WorkerHello`] from a length-prefixed capnp message.
pub async fn recv_worker_hello<R: AsyncReadExt + Unpin>(
    reader: &mut R,
) -> anyhow::Result<WorkerHello> {
    let msg = recv_capnp_msg(reader).await?;
    let root = msg
        .get_root::<schema::worker_hello::Reader<'_>>()
        .context("read WorkerHello root")?;
    convert::read_worker_hello(root).context("decode WorkerHello")
}

/// Send a [`WorkerAccepted`] as a length-prefixed capnp message.
pub async fn send_worker_accepted<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    accepted: &WorkerAccepted,
) -> anyhow::Result<()> {
    let mut msg = message::Builder::new_default();
    convert::write_worker_accepted(
        &mut msg.init_root::<schema::worker_accepted::Builder<'_>>(),
        accepted,
    );
    send_capnp_msg(writer, &msg).await
}

/// Receive a [`WorkerAccepted`] from a length-prefixed capnp message.
pub async fn recv_worker_accepted<R: AsyncReadExt + Unpin>(
    reader: &mut R,
) -> anyhow::Result<WorkerAccepted> {
    let msg = recv_capnp_msg(reader).await?;
    let root = msg
        .get_root::<schema::worker_accepted::Reader<'_>>()
        .context("read WorkerAccepted root")?;
    convert::read_worker_accepted(root).context("decode WorkerAccepted")
}

/// Send a [`WorkerReady`] as a length-prefixed capnp message.
pub async fn send_worker_ready<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    ready: &WorkerReady,
) -> anyhow::Result<()> {
    let mut msg = message::Builder::new_default();
    convert::write_worker_ready(
        &mut msg.init_root::<schema::worker_ready::Builder<'_>>(),
        ready,
    );
    send_capnp_msg(writer, &msg).await
}

/// Receive a [`WorkerReady`] from a length-prefixed capnp message.
pub async fn recv_worker_ready<R: AsyncReadExt + Unpin>(
    reader: &mut R,
) -> anyhow::Result<WorkerReady> {
    let msg = recv_capnp_msg(reader).await?;
    let root = msg.get_root::<schema::worker_ready::Reader<'_>>()?;
    Ok(convert::read_worker_ready(root))
}

// --- Log Data Frames ---
//
// After the initial LogStreamHeader, log streams carry framed chunks:
// `[seq: u64 LE][length: u32 LE][payload]` (12-byte header).

/// Log data frame header size: [seq: u64 LE][length: u32 LE] = 12 bytes.
pub const LOG_FRAME_HEADER_SIZE: usize = 12;

/// Send a log data frame with sequence number.
pub async fn send_log_frame<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    seq: u64,
    payload: &[u8],
) -> anyhow::Result<()> {
    let mut header = [0u8; LOG_FRAME_HEADER_SIZE];
    header[..8].copy_from_slice(&seq.to_le_bytes());
    header[8..12].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    writer
        .write_all(&header)
        .await
        .context("write log frame header")?;
    writer
        .write_all(payload)
        .await
        .context("write log frame payload")?;
    Ok(())
}

/// Receive a log data frame. Returns `None` on clean EOF.
pub async fn recv_log_frame<R: AsyncReadExt + Unpin>(
    reader: &mut R,
) -> anyhow::Result<Option<(u64, Vec<u8>)>> {
    let mut header = [0u8; LOG_FRAME_HEADER_SIZE];
    match reader.read_exact(&mut header).await {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e).context("read log frame header"),
    }
    let seq = u64::from_le_bytes(header[..8].try_into().unwrap());
    let len = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
    if len > MAX_MESSAGE_SIZE {
        bail!("log frame too large: {} bytes (max {})", len, MAX_MESSAGE_SIZE);
    }
    let mut payload = vec![0u8; len];
    if len > 0 {
        reader
            .read_exact(&mut payload)
            .await
            .context("read log frame payload")?;
    }
    Ok(Some((seq, payload)))
}
