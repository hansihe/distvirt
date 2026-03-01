use crate::frame::*;

/// Phase of the client-side H2 connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Waiting for the client to send the 24-byte connection preface.
    AwaitingPreface,
    /// Preface received, exchanging SETTINGS.
    Handshaking,
    /// Fully active, forwarding frames.
    Active,
    /// GOAWAY sent or received, draining.
    Closing,
}

/// State of the upstream (backend) connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpstreamState {
    /// No upstream connection yet.
    None,
    /// `upstream-connect` issued, waiting for result.
    Connecting,
    /// Upstream connected, exchanging H2 preface/SETTINGS.
    Handshaking,
    /// Upstream H2 handshake complete, ready to forward.
    Ready,
    /// Upstream connection failed.
    Failed,
}

/// Tracked state of a single H2 stream.
#[derive(Debug)]
struct H2Stream {
    client_end_stream: bool,
    backend_end_stream: bool,
}

/// Actions the connection wants the caller to perform.
pub enum ConnAction {
    /// Send data to the downstream (client) TCP connection.
    DownstreamSend(Vec<u8>),
    /// Send data to the upstream (backend) TCP connection.
    UpstreamSend(Vec<u8>),
    /// Close the downstream TCP connection.
    DownstreamClose,
    /// Close the upstream TCP connection.
    UpstreamClose,
    /// An H2 stream was opened (increment global count).
    StreamOpened,
    /// An H2 stream was closed (decrement global count).
    StreamClosed,
    /// Log a message.
    Log(String),
}

pub struct Connection {
    phase: Phase,
    upstream_state: UpstreamState,

    /// Buffer for accumulating incoming client bytes until full frames are available.
    recv_buf: Vec<u8>,
    /// Buffer for accumulating incoming upstream bytes.
    upstream_recv_buf: Vec<u8>,

    /// Active H2 streams (keyed by stream ID).
    streams: Vec<(u32, H2Stream)>,

    /// Frames buffered while upstream is not ready.
    buffered_frames: Vec<Vec<u8>>,

    /// Whether we've received a SETTINGS ACK from the client.
    client_settings_acked: bool,

    /// Whether we've received a SETTINGS ACK from the upstream.
    upstream_settings_acked: bool,

    /// Highest client-initiated stream ID seen (for GOAWAY).
    last_client_stream_id: u32,
}

impl Connection {
    pub fn new() -> Self {
        Self {
            phase: Phase::AwaitingPreface,
            upstream_state: UpstreamState::None,
            recv_buf: Vec::new(),
            upstream_recv_buf: Vec::new(),
            streams: Vec::new(),
            buffered_frames: Vec::new(),
            client_settings_acked: false,
            upstream_settings_acked: false,
            last_client_stream_id: 0,
        }
    }

    pub fn phase(&self) -> Phase {
        self.phase
    }

    pub fn upstream_state(&self) -> UpstreamState {
        self.upstream_state
    }

    pub fn set_upstream_state(&mut self, state: UpstreamState) {
        self.upstream_state = state;
    }

    pub fn has_buffered_frames(&self) -> bool {
        !self.buffered_frames.is_empty()
    }

    /// Process data received from the client.
    pub fn on_client_data(&mut self, data: &[u8], actions: &mut Vec<ConnAction>) {
        self.recv_buf.extend_from_slice(data);

        if self.phase == Phase::AwaitingPreface {
            if self.recv_buf.len() < CLIENT_PREFACE.len() {
                return;
            }
            if &self.recv_buf[..CLIENT_PREFACE.len()] != CLIENT_PREFACE {
                actions.push(ConnAction::Log("invalid H2 preface".into()));
                actions.push(ConnAction::DownstreamClose);
                self.phase = Phase::Closing;
                return;
            }
            self.recv_buf.drain(..CLIENT_PREFACE.len());
            self.phase = Phase::Handshaking;

            // Send our SETTINGS to client
            actions.push(ConnAction::DownstreamSend(our_settings()));
        }

        // Parse frames from recv_buf
        loop {
            let buf = &self.recv_buf;
            let Some((frame, consumed)) = parse_frame(buf) else {
                break;
            };

            // Copy payload before we drain (borrow issue)
            let frame_type = frame.frame_type;
            let flags = frame.flags;
            let stream_id = frame.stream_id;
            let payload = frame.payload.to_vec();

            // Also keep the raw frame bytes for forwarding
            let raw_frame = self.recv_buf[..consumed].to_vec();
            self.recv_buf.drain(..consumed);

            if self.phase == Phase::Closing {
                continue;
            }

            self.handle_client_frame(frame_type, flags, stream_id, &payload, &raw_frame, actions);
        }
    }

    fn handle_client_frame(
        &mut self,
        frame_type: FrameType,
        flags: u8,
        stream_id: u32,
        payload: &[u8],
        raw_frame: &[u8],
        actions: &mut Vec<ConnAction>,
    ) {
        match frame_type {
            FrameType::Settings => {
                if flags & FLAG_ACK != 0 {
                    // Client ACKed our SETTINGS
                    self.client_settings_acked = true;
                    self.check_handshake_complete();
                } else {
                    // Client's SETTINGS — ACK it
                    actions.push(ConnAction::DownstreamSend(build_settings_ack()));
                }
            }
            FrameType::Ping => {
                if flags & FLAG_ACK != 0 {
                    // PING ACK from client, ignore
                } else {
                    // Send PING ACK with same opaque data
                    let mut opaque = [0u8; 8];
                    if payload.len() >= 8 {
                        opaque.copy_from_slice(&payload[..8]);
                    }
                    actions.push(ConnAction::DownstreamSend(build_ping_ack(&opaque)));
                }
            }
            FrameType::Headers => {
                // New or continuing H2 stream
                if stream_id > 0 && !self.has_stream(stream_id) {
                    self.open_stream(stream_id);
                    actions.push(ConnAction::StreamOpened);
                    if stream_id > self.last_client_stream_id {
                        self.last_client_stream_id = stream_id;
                    }
                }
                if flags & FLAG_END_STREAM != 0 {
                    self.set_client_end_stream(stream_id);
                    if self.check_stream_closed(stream_id) {
                        actions.push(ConnAction::StreamClosed);
                    }
                }
                self.buffer_or_forward(raw_frame.to_vec(), actions);
            }
            FrameType::Data => {
                // Send WINDOW_UPDATE back to client for flow control
                let len = payload.len() as u32;
                if len > 0 {
                    // Connection-level WINDOW_UPDATE
                    actions.push(ConnAction::DownstreamSend(build_window_update(0, len)));
                    // Stream-level WINDOW_UPDATE
                    if stream_id > 0 {
                        actions.push(ConnAction::DownstreamSend(build_window_update(stream_id, len)));
                    }
                }
                if flags & FLAG_END_STREAM != 0 {
                    self.set_client_end_stream(stream_id);
                    if self.check_stream_closed(stream_id) {
                        actions.push(ConnAction::StreamClosed);
                    }
                }
                self.buffer_or_forward(raw_frame.to_vec(), actions);
            }
            FrameType::RstStream => {
                if self.has_stream(stream_id) {
                    self.remove_stream(stream_id);
                    actions.push(ConnAction::StreamClosed);
                }
                self.buffer_or_forward(raw_frame.to_vec(), actions);
            }
            FrameType::Goaway => {
                self.phase = Phase::Closing;
                self.buffer_or_forward(raw_frame.to_vec(), actions);
            }
            FrameType::WindowUpdate => {
                // Forward to upstream (each side's flow control is independent)
                self.buffer_or_forward(raw_frame.to_vec(), actions);
            }
            FrameType::Continuation => {
                // CONTINUATION frames are part of a HEADERS block, forward as-is
                self.buffer_or_forward(raw_frame.to_vec(), actions);
            }
            FrameType::Priority => {
                // Forward priority frames
                self.buffer_or_forward(raw_frame.to_vec(), actions);
            }
            _ => {
                // Unknown frame types: forward
                self.buffer_or_forward(raw_frame.to_vec(), actions);
            }
        }
    }

    /// Called when the upstream TCP connection is established.
    pub fn on_upstream_connected(&mut self, actions: &mut Vec<ConnAction>) {
        self.upstream_state = UpstreamState::Handshaking;
        // Send H2 connection preface + our SETTINGS to backend
        let mut preface_and_settings = Vec::with_capacity(CLIENT_PREFACE.len() + 50);
        preface_and_settings.extend_from_slice(CLIENT_PREFACE);
        preface_and_settings.extend_from_slice(&our_settings());
        actions.push(ConnAction::UpstreamSend(preface_and_settings));
    }

    /// Process data received from the upstream (backend).
    pub fn on_upstream_data(&mut self, data: &[u8], actions: &mut Vec<ConnAction>) {
        self.upstream_recv_buf.extend_from_slice(data);

        loop {
            let buf = &self.upstream_recv_buf;
            let Some((frame, consumed)) = parse_frame(buf) else {
                break;
            };

            let frame_type = frame.frame_type;
            let flags = frame.flags;
            let stream_id = frame.stream_id;
            let payload = frame.payload.to_vec();
            let raw_frame = self.upstream_recv_buf[..consumed].to_vec();
            self.upstream_recv_buf.drain(..consumed);

            self.handle_upstream_frame(frame_type, flags, stream_id, &payload, &raw_frame, actions);
        }
    }

    fn handle_upstream_frame(
        &mut self,
        frame_type: FrameType,
        flags: u8,
        stream_id: u32,
        payload: &[u8],
        raw_frame: &[u8],
        actions: &mut Vec<ConnAction>,
    ) {
        match frame_type {
            FrameType::Settings => {
                if flags & FLAG_ACK != 0 {
                    // Backend ACKed our SETTINGS
                    self.upstream_settings_acked = true;
                    if self.upstream_state == UpstreamState::Handshaking {
                        self.upstream_state = UpstreamState::Ready;
                        self.flush_buffered(actions);
                    }
                } else {
                    // Backend's SETTINGS — ACK it
                    actions.push(ConnAction::UpstreamSend(build_settings_ack()));
                    // If this is the first SETTINGS from backend, consider handshake progressing
                    if self.upstream_state == UpstreamState::Handshaking {
                        self.upstream_state = UpstreamState::Ready;
                        self.flush_buffered(actions);
                    }
                }
            }
            FrameType::Ping => {
                if flags & FLAG_ACK != 0 {
                    // PING ACK from backend, ignore
                } else {
                    let mut opaque = [0u8; 8];
                    if payload.len() >= 8 {
                        opaque.copy_from_slice(&payload[..8]);
                    }
                    actions.push(ConnAction::UpstreamSend(build_ping_ack(&opaque)));
                }
            }
            FrameType::Data => {
                // Send WINDOW_UPDATE back to upstream for flow control
                let len = payload.len() as u32;
                if len > 0 {
                    actions.push(ConnAction::UpstreamSend(build_window_update(0, len)));
                    if stream_id > 0 {
                        actions.push(ConnAction::UpstreamSend(build_window_update(stream_id, len)));
                    }
                }
                if flags & FLAG_END_STREAM != 0 {
                    self.set_backend_end_stream(stream_id);
                    if self.check_stream_closed(stream_id) {
                        actions.push(ConnAction::StreamClosed);
                    }
                }
                // Forward to client
                actions.push(ConnAction::DownstreamSend(raw_frame.to_vec()));
            }
            FrameType::Headers => {
                if flags & FLAG_END_STREAM != 0 {
                    self.set_backend_end_stream(stream_id);
                    if self.check_stream_closed(stream_id) {
                        actions.push(ConnAction::StreamClosed);
                    }
                }
                actions.push(ConnAction::DownstreamSend(raw_frame.to_vec()));
            }
            FrameType::RstStream => {
                if self.has_stream(stream_id) {
                    self.remove_stream(stream_id);
                    actions.push(ConnAction::StreamClosed);
                }
                actions.push(ConnAction::DownstreamSend(raw_frame.to_vec()));
            }
            FrameType::Goaway => {
                self.phase = Phase::Closing;
                actions.push(ConnAction::DownstreamSend(raw_frame.to_vec()));
            }
            FrameType::WindowUpdate => {
                actions.push(ConnAction::DownstreamSend(raw_frame.to_vec()));
            }
            FrameType::Continuation => {
                actions.push(ConnAction::DownstreamSend(raw_frame.to_vec()));
            }
            _ => {
                actions.push(ConnAction::DownstreamSend(raw_frame.to_vec()));
            }
        }
    }

    /// Handle upstream connection closed.
    pub fn on_upstream_closed(&mut self, actions: &mut Vec<ConnAction>) {
        self.upstream_state = UpstreamState::Failed;
        if self.phase != Phase::Closing {
            // Send GOAWAY to client
            actions.push(ConnAction::DownstreamSend(build_goaway(
                self.last_client_stream_id,
                ERROR_NO_ERROR,
            )));
            actions.push(ConnAction::DownstreamClose);
            self.phase = Phase::Closing;
        }
        // Count remaining streams as closed
        let remaining = self.streams.len();
        self.streams.clear();
        for _ in 0..remaining {
            actions.push(ConnAction::StreamClosed);
        }
    }

    /// Handle upstream connection failed.
    pub fn on_upstream_failed(&mut self, actions: &mut Vec<ConnAction>) {
        self.upstream_state = UpstreamState::Failed;
        actions.push(ConnAction::DownstreamSend(build_goaway(
            0,
            ERROR_INTERNAL_ERROR,
        )));
        actions.push(ConnAction::DownstreamClose);
        self.phase = Phase::Closing;
        let remaining = self.streams.len();
        self.streams.clear();
        for _ in 0..remaining {
            actions.push(ConnAction::StreamClosed);
        }
    }

    /// Flush buffered frames to upstream.
    fn flush_buffered(&mut self, actions: &mut Vec<ConnAction>) {
        let frames = std::mem::take(&mut self.buffered_frames);
        for frame in frames {
            actions.push(ConnAction::UpstreamSend(frame));
        }
    }

    /// Buffer a frame or forward it immediately if upstream is ready.
    fn buffer_or_forward(&mut self, frame: Vec<u8>, actions: &mut Vec<ConnAction>) {
        if self.upstream_state == UpstreamState::Ready {
            actions.push(ConnAction::UpstreamSend(frame));
        } else {
            self.buffered_frames.push(frame);
        }
    }

    fn check_handshake_complete(&mut self) {
        if self.client_settings_acked && self.phase == Phase::Handshaking {
            self.phase = Phase::Active;
        }
    }

    /// Get the number of active H2 streams on this connection.
    pub fn stream_count(&self) -> usize {
        self.streams.len()
    }

    fn has_stream(&self, stream_id: u32) -> bool {
        self.streams.iter().any(|(id, _)| *id == stream_id)
    }

    fn open_stream(&mut self, stream_id: u32) {
        self.streams.push((
            stream_id,
            H2Stream {
                client_end_stream: false,
                backend_end_stream: false,
            },
        ));
    }

    fn remove_stream(&mut self, stream_id: u32) {
        self.streams.retain(|(id, _)| *id != stream_id);
    }

    fn set_client_end_stream(&mut self, stream_id: u32) {
        if let Some((_, stream)) = self.streams.iter_mut().find(|(id, _)| *id == stream_id) {
            stream.client_end_stream = true;
        }
    }

    fn set_backend_end_stream(&mut self, stream_id: u32) {
        if let Some((_, stream)) = self.streams.iter_mut().find(|(id, _)| *id == stream_id) {
            stream.backend_end_stream = true;
        }
    }

    /// Check if both sides have ended the stream. If so, remove it and return true.
    fn check_stream_closed(&mut self, stream_id: u32) -> bool {
        let closed = self
            .streams
            .iter()
            .any(|(id, s)| *id == stream_id && s.client_end_stream && s.backend_end_stream);
        if closed {
            self.remove_stream(stream_id);
        }
        closed
    }
}
