use serde::{Deserialize, Serialize};

pub const VSOCK_CONTROL_PORT: u32 = 1024;

/// A volume mount specification for binding a volume into a container.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeMount {
    pub name: String,
    pub mount_path: String,
}

/// Messages sent from host to guest over vsock.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum HostMessage {
    AddContainer {
        id: String,
        device: String,
        #[serde(default)]
        dns_servers: Vec<String>,
        #[serde(default)]
        volume_mounts: Vec<VolumeMount>,
    },
    MountVolume {
        name: String,
        device: String,
        read_only: bool,
    },
    StartContainer {
        id: String,
        argv: Vec<String>,
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
        #[serde(default)]
        stdin: bool,
    },
    ConfigureNetwork {
        interface: String,
        ip: String,
        netmask: String,
        gateway: String,
    },
    SignalContainer {
        id: String,
        signal: i32,
    },
    /// Set the guest's system clock.
    /// Guest should respond with `ClockSet`.
    SetClock {
        /// Seconds since Unix epoch.
        epoch_secs: u64,
        /// Nanoseconds within the current second.
        epoch_nanos: u32,
    },
    /// Tells the guest to flush output buffers in preparation for suspend.
    /// Guest should respond with `SuspendReady` when done.
    PrepareSuspend,
    Shutdown,
}

/// Messages sent from guest to host over vsock.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum GuestMessage {
    Ready {
        /// IDs of containers still running (non-empty on resume after suspend).
        #[serde(default)]
        running_containers: Vec<String>,
        /// Responses from commands executed via the config drive before vsock connect.
        #[serde(default)]
        pre_config_responses: Vec<GuestMessage>,
    },
    /// Guest has flushed output and is ready for vCPU freeze.
    SuspendReady,
    ContainerAdded {
        id: String,
    },
    VolumeMounted {
        name: String,
    },
    ContainerStarted {
        id: String,
        pid: u32,
    },
    ContainerSignaled {
        id: String,
    },
    NetworkConfigured,
    ClockSet,
    Error {
        message: String,
    },
}

/// Stream header sent as the first message on any new yamux stream.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum StreamHeader {
    Control,
    Events,
    ContainerOutput { container_id: String },
    ContainerInput { container_id: String },
}

/// Why the guest's memory control loop cannot resolve pressure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConstraintReason {
    /// Balloon fully deflated but pressure persists — workload needs more
    /// memory than the VM has.
    BalloonExhausted,
    /// Deflation was requested but the host didn't respond in time.
    DeflationStalled,
}

/// Async events sent from guest to host on the dedicated event stream.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum GuestEvent {
    ContainerExited {
        id: String,
        code: i32,
        /// Number of output bytes dropped during final pipe drain (e.g. buffer
        /// was full while disconnected). Zero means all output was delivered.
        #[serde(default)]
        output_bytes_dropped: u64,
    },
    /// Guest requests the host to set the balloon to this size.
    BalloonSet {
        amount_mib: u32,
    },
    /// A supervised task failed unexpectedly.
    TaskError {
        task: String,
        message: String,
    },
    /// The memory control loop failed to resolve pressure.
    MemoryConstrained {
        reason: ConstraintReason,
    },
    /// Memory pressure has been resolved after a constrained state.
    MemoryConstraintCleared,
    /// One or more processes were killed by the OOM killer.
    OomKill {
        count: u64,
    },
}

/// Stream identifiers for output chunk framing.
pub const STREAM_STDIN: u8 = 0;
pub const STREAM_STDOUT: u8 = 1;
pub const STREAM_STDERR: u8 = 2;

/// Output chunk header size: [stream_id: u8][seq: u64 LE][length: u32 LE] = 13 bytes.
pub const OUTPUT_CHUNK_HEADER_SIZE: usize = 13;

/// Encode an output chunk: `[stream_id: u8][seq: u64 LE][u32 LE length][payload]`.
pub fn encode_output_chunk(stream_id: u8, seq: u64, payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(OUTPUT_CHUNK_HEADER_SIZE + payload.len());
    frame.push(stream_id);
    frame.extend_from_slice(&seq.to_le_bytes());
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(payload);
    frame
}

/// Parse a 13-byte output chunk header into `(stream_id, seq, payload_length)`.
pub fn parse_output_chunk_header(header: &[u8; OUTPUT_CHUNK_HEADER_SIZE]) -> (u8, u64, u32) {
    let stream_id = header[0];
    let seq = u64::from_le_bytes([
        header[1], header[2], header[3], header[4], header[5], header[6], header[7], header[8],
    ]);
    let length = u32::from_le_bytes([header[9], header[10], header[11], header[12]]);
    (stream_id, seq, length)
}
