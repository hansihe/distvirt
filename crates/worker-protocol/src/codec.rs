//! Length-prefixed Protocol Buffer codec for the control stream.
//!
//! All messages on the yamux control stream are framed as
//! `[u32 LE length][protobuf message bytes]`. Log streams use the same framing
//! for the initial [`LogStreamHeader`](crate::LogStreamHeader), then raw bytes.

use anyhow::{Context, bail};
use futures_lite::io::{AsyncReadExt, AsyncWriteExt};
use prost::Message;

use crate::convert;
use crate::types::*;
use crate::proto;

/// Maximum message size: 16 MiB.
const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Send a protobuf message with length-prefix framing.
async fn send_proto_msg<W: AsyncWriteExt + Unpin, M: Message>(
    writer: &mut W,
    msg: &M,
) -> anyhow::Result<()> {
    let payload = msg.encode_to_vec();
    let len = (payload.len() as u32).to_le_bytes();
    writer.write_all(&len).await.context("write length")?;
    writer.write_all(&payload).await.context("write payload")?;
    writer.flush().await.context("flush")?;
    Ok(())
}

/// Receive a protobuf message with length-prefix framing.
async fn recv_proto_msg<R: AsyncReadExt + Unpin, M: Message + Default>(
    reader: &mut R,
) -> anyhow::Result<M> {
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
    M::decode(buf.as_slice()).context("decode protobuf message")
}

// --- WorkerCommand ---

/// Send a [`WorkerCommand`] as a length-prefixed protobuf message.
pub async fn send_worker_command<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    cmd: &WorkerCommand,
) -> anyhow::Result<()> {
    let proto_msg = convert::worker_command_to_proto(cmd);
    send_proto_msg(writer, &proto_msg).await
}

/// Receive a [`WorkerCommand`] from a length-prefixed protobuf message.
pub async fn recv_worker_command<R: AsyncReadExt + Unpin>(
    reader: &mut R,
) -> anyhow::Result<WorkerCommand> {
    let proto_msg: proto::WorkerCommand = recv_proto_msg(reader).await?;
    convert::worker_command_from_proto(proto_msg).context("decode WorkerCommand")
}

// --- WorkerEvent ---

/// Send a [`WorkerEvent`] as a length-prefixed protobuf message.
pub async fn send_worker_event<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    event: &WorkerEvent,
) -> anyhow::Result<()> {
    let proto_msg = convert::worker_event_to_proto(event);
    send_proto_msg(writer, &proto_msg).await
}

/// Receive a [`WorkerEvent`] from a length-prefixed protobuf message.
pub async fn recv_worker_event<R: AsyncReadExt + Unpin>(
    reader: &mut R,
) -> anyhow::Result<WorkerEvent> {
    let proto_msg: proto::WorkerEvent = recv_proto_msg(reader).await?;
    convert::worker_event_from_proto(proto_msg).context("decode WorkerEvent")
}

// --- LogStreamHeader ---

/// Send a [`LogStreamHeader`] as a length-prefixed protobuf message.
pub async fn send_log_header<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    header: &LogStreamHeader,
) -> anyhow::Result<()> {
    let proto_msg = convert::log_stream_header_to_proto(header);
    send_proto_msg(writer, &proto_msg).await
}

/// Receive a [`LogStreamHeader`] from a length-prefixed protobuf message.
pub async fn recv_log_header<R: AsyncReadExt + Unpin>(
    reader: &mut R,
) -> anyhow::Result<LogStreamHeader> {
    let proto_msg: proto::LogStreamHeader = recv_proto_msg(reader).await?;
    convert::log_stream_header_from_proto(proto_msg).context("decode LogStreamHeader")
}

// --- Handshake Messages ---

/// Send a [`WorkerHello`] as a length-prefixed protobuf message.
pub async fn send_worker_hello<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    hello: &WorkerHello,
) -> anyhow::Result<()> {
    let proto_msg = convert::worker_hello_to_proto(hello);
    send_proto_msg(writer, &proto_msg).await
}

/// Receive a [`WorkerHello`] from a length-prefixed protobuf message.
pub async fn recv_worker_hello<R: AsyncReadExt + Unpin>(
    reader: &mut R,
) -> anyhow::Result<WorkerHello> {
    let proto_msg: proto::WorkerHello = recv_proto_msg(reader).await?;
    convert::worker_hello_from_proto(proto_msg).context("decode WorkerHello")
}

/// Send a [`WorkerAccepted`] as a length-prefixed protobuf message.
pub async fn send_worker_accepted<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    accepted: &WorkerAccepted,
) -> anyhow::Result<()> {
    let proto_msg = convert::worker_accepted_to_proto(accepted);
    send_proto_msg(writer, &proto_msg).await
}

/// Receive a [`WorkerAccepted`] from a length-prefixed protobuf message.
pub async fn recv_worker_accepted<R: AsyncReadExt + Unpin>(
    reader: &mut R,
) -> anyhow::Result<WorkerAccepted> {
    let proto_msg: proto::WorkerAccepted = recv_proto_msg(reader).await?;
    convert::worker_accepted_from_proto(proto_msg).context("decode WorkerAccepted")
}

/// Send a [`WorkerReady`] as a length-prefixed protobuf message.
pub async fn send_worker_ready<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    ready: &WorkerReady,
) -> anyhow::Result<()> {
    let proto_msg = convert::worker_ready_to_proto(ready);
    send_proto_msg(writer, &proto_msg).await
}

/// Receive a [`WorkerReady`] from a length-prefixed protobuf message.
pub async fn recv_worker_ready<R: AsyncReadExt + Unpin>(
    reader: &mut R,
) -> anyhow::Result<WorkerReady> {
    let proto_msg: proto::WorkerReady = recv_proto_msg(reader).await?;
    Ok(convert::worker_ready_from_proto(proto_msg))
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
