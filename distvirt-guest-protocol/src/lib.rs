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
