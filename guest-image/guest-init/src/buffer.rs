use distvirt_guest_protocol::GuestEvent;

/// Unbounded buffer for guest events (ContainerExited, BalloonSet).
///
/// Lives for the entire guest lifetime. Events are produced by container exits
/// and the balloon task, and drained to yamux by a per-connection drain task.
///
/// Unbounded because events are small and infrequent — a bounded buffer risks
/// deadlock when the drain task is dropped on disconnect and producers block.
pub struct EventBuffer {
    tx: async_channel::Sender<GuestEvent>,
    rx: async_channel::Receiver<GuestEvent>,
}

impl EventBuffer {
    pub fn new() -> Self {
        let (tx, rx) = async_channel::unbounded();
        EventBuffer { tx, rx }
    }

    /// Send an event into the buffer. Never blocks (unbounded).
    pub async fn send(&self, event: GuestEvent) {
        if let Err(e) = self.tx.send(event).await {
            log::error!("event buffer send failed (closed): {}", e);
        }
    }

    /// Clone the receiver for a drain task.
    pub fn receiver(&self) -> async_channel::Receiver<GuestEvent> {
        self.rx.clone()
    }

    /// Clone the sender (e.g. for the balloon task).
    pub fn sender(&self) -> async_channel::Sender<GuestEvent> {
        self.tx.clone()
    }
}

/// Bounded buffer for pre-framed container output chunks.
///
/// One per container, lives for the container's lifetime. The fill task
/// reads from stdout/stderr pipes and encodes chunks into this buffer.
/// A per-connection drain task forwards chunks to yamux.
pub struct OutputBuffer {
    tx: async_channel::Sender<Vec<u8>>,
    rx: async_channel::Receiver<Vec<u8>>,
}

impl OutputBuffer {
    pub fn new(capacity: usize) -> Self {
        let (tx, rx) = async_channel::bounded(capacity);
        OutputBuffer { tx, rx }
    }

    /// Clone the sender for the fill task.
    pub fn sender(&self) -> async_channel::Sender<Vec<u8>> {
        self.tx.clone()
    }

    /// Clone the receiver for a drain task.
    pub fn receiver(&self) -> async_channel::Receiver<Vec<u8>> {
        self.rx.clone()
    }
}
