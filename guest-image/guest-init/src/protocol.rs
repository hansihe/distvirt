use serde::{Deserialize, Serialize};

/// Messages sent from host to guest over vsock.
#[derive(Debug, Deserialize)]
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
#[derive(Debug, Serialize)]
#[serde(tag = "type")]
pub enum GuestMessage {
    Ready,
    ContainerAdded { id: String },
    ContainerStarted { id: String, pid: u32 },
    ContainerExited { id: String, code: i32 },
    Error { message: String },
}
