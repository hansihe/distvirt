use futures::io::AsyncWriteExt;

use crate::vsock;
use distvirt_guest_protocol::GuestEvent;

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
