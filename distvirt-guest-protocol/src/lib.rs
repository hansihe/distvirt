use serde::{Deserialize, Serialize};

pub const VSOCK_PORT: u32 = 1024;

/// Messages sent from host to guest over vsock.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum HostMessage {
    AddContainer {
        id: String,
        device: String,
    },
    StartContainer {
        id: String,
        entrypoint: String,
        args: Vec<String>,
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
    Error { message: String },
}
