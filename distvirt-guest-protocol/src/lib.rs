use serde::{Deserialize, Serialize};

pub const VSOCK_CONTROL_PORT: u32 = 1024;

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

/// Stream header sent as the first message on any new yamux stream.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum StreamHeader {
    Control,
    ContainerOutput { container_id: String },
}

/// Stream identifiers for output chunk framing.
pub const STREAM_STDOUT: u8 = 1;
pub const STREAM_STDERR: u8 = 2;

/// Output chunk header size: [stream_id: u8][length: u32 LE] = 5 bytes.
pub const OUTPUT_CHUNK_HEADER_SIZE: usize = 5;

/// Encode an output chunk: `[stream_id: u8][u32 LE length][payload]`.
pub fn encode_output_chunk(stream_id: u8, payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(OUTPUT_CHUNK_HEADER_SIZE + payload.len());
    frame.push(stream_id);
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(payload);
    frame
}

/// Parse a 5-byte output chunk header into `(stream_id, payload_length)`.
pub fn parse_output_chunk_header(header: &[u8; OUTPUT_CHUNK_HEADER_SIZE]) -> (u8, u32) {
    let stream_id = header[0];
    let length = u32::from_le_bytes([header[1], header[2], header[3], header[4]]);
    (stream_id, length)
}
