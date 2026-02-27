use serde::{Deserialize, Serialize};

pub const VSOCK_CONTROL_PORT: u32 = 1024;
pub const VSOCK_IO_PORT: u32 = 1025;

/// Backwards compatibility alias.
pub const VSOCK_PORT: u32 = VSOCK_CONTROL_PORT;

/// Messages sent from host to guest over vsock.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum HostMessage {
    AddContainer {
        id: String,
        device: String,
        #[serde(default)]
        dns_servers: Vec<String>,
    },
    StartContainer {
        id: String,
        entrypoint: String,
        args: Vec<String>,
        #[serde(default)]
        env: Vec<String>,
        #[serde(default)]
        working_dir: Option<String>,
        #[serde(default)]
        uid: Option<u32>,
        #[serde(default)]
        gid: Option<u32>,
        #[serde(default)]
        hostname: Option<String>,
        #[serde(default)]
        capture_output: bool,
    },
    ConfigureNetwork {
        interface: String,
        ip: String,
        netmask: String,
        gateway: String,
    },
    Shutdown,
}

/// Messages sent from guest to host over vsock.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum GuestMessage {
    Ready,
    ContainerAdded { id: String },
    ContainerStarted { id: String, pid: u32 },
    ContainerExited { id: String, code: i32 },
    NetworkConfigured,
    Error { message: String },
}

/// I/O session mode.
#[derive(Debug, Serialize, Deserialize)]
pub enum IoMode {
    Logs,
}

/// Sent by host on a new I/O connection (length-prefixed JSON).
#[derive(Debug, Serialize, Deserialize)]
pub struct IoSessionRequest {
    pub container_id: String,
    pub mode: IoMode,
}

/// Guest response to an I/O session request (length-prefixed JSON).
#[derive(Debug, Serialize, Deserialize)]
pub struct IoSessionResponse {
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Stream identifiers for I/O frames.
pub const STREAM_EOF: u8 = 0;
pub const STREAM_STDOUT: u8 = 1;
pub const STREAM_STDERR: u8 = 2;

/// Maximum payload size for an I/O frame.
pub const IO_FRAME_MAX_PAYLOAD: usize = 8192;

/// Size of an I/O frame header: [stream_id: u8][length: u16 LE].
pub const IO_FRAME_HEADER_SIZE: usize = 3;

/// Encode a single I/O frame: `[stream_id][u16 LE length][payload]`.
///
/// Panics if `payload.len() > IO_FRAME_MAX_PAYLOAD`.
pub fn encode_io_frame(stream_id: u8, payload: &[u8]) -> Vec<u8> {
    assert!(
        payload.len() <= IO_FRAME_MAX_PAYLOAD,
        "payload exceeds IO_FRAME_MAX_PAYLOAD"
    );
    let mut frame = Vec::with_capacity(IO_FRAME_HEADER_SIZE + payload.len());
    frame.push(stream_id);
    frame.extend_from_slice(&(payload.len() as u16).to_le_bytes());
    frame.extend_from_slice(payload);
    frame
}

/// Chunk `data` into one or more frames, each with at most `IO_FRAME_MAX_PAYLOAD` bytes.
pub fn encode_io_frames(stream_id: u8, data: &[u8]) -> Vec<Vec<u8>> {
    let mut frames = Vec::new();
    let mut offset = 0;
    while offset < data.len() {
        let chunk_len = (data.len() - offset).min(IO_FRAME_MAX_PAYLOAD);
        frames.push(encode_io_frame(stream_id, &data[offset..offset + chunk_len]));
        offset += chunk_len;
    }
    frames
}

/// Encode an EOF frame (stream_id=STREAM_EOF, length=0).
pub fn encode_eof_frame() -> [u8; IO_FRAME_HEADER_SIZE] {
    [STREAM_EOF, 0, 0]
}

/// Parse a 3-byte frame header into `(stream_id, payload_length)`.
pub fn parse_io_frame_header(header: &[u8; IO_FRAME_HEADER_SIZE]) -> (u8, u16) {
    let stream_id = header[0];
    let length = u16::from_le_bytes([header[1], header[2]]);
    (stream_id, length)
}
