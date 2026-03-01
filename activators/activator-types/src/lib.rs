//! Shared types for distvirt activators.
//!
//! These types are portable across both native and WASM targets. IP addresses
//! use `Vec<u8>` (4 bytes for IPv4, 16 bytes for IPv6) instead of `std::net::IpAddr`.

#[cfg(feature = "test-helpers")]
pub mod test_helpers;

/// L3 flow identifier — fabric-tracked packet correlation.
pub type PacketFlow = u64;

/// L4 stream identifier — fabric-managed TCP connection.
pub type Stream = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpProtocol {
    Tcp,
    Udp,
    Other,
}

#[derive(Debug, Clone)]
pub struct PacketInfo {
    pub flow: PacketFlow,
    pub src_addr: Vec<u8>,
    pub dst_addr: Vec<u8>,
    pub src_port: u16,
    pub dst_port: u16,
    pub protocol: IpProtocol,
    pub tcp_flags: Option<u8>,
    pub payload_len: usize,
    pub raw_frame: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketDecision {
    Buffered,
    Drop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendNeed {
    None,
    Traffic,
    Active,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone)]
pub struct LogAction {
    pub level: LogLevel,
    pub message: String,
}

/// Events delivered from the fabric to the activator.
#[derive(Debug, Clone)]
pub enum Event {
    BackendAvailable(bool),
    Tick,
    Packet(PacketInfo),
    StreamOpen(Stream),
    StreamData { stream: Stream, data: Vec<u8> },
    StreamClose(Stream),
    UpstreamConnectResult { stream: Stream, ok: bool },
    UpstreamData { stream: Stream, data: Vec<u8> },
    UpstreamClose(Stream),
}

/// Actions returned from the activator to the fabric.
#[derive(Debug, Clone)]
pub enum Action {
    SetBackendNeed(BackendNeed),
    Log(LogAction),
    PacketDecision { flow: PacketFlow, decision: PacketDecision },
    PacketReply { flow: PacketFlow, data: Vec<u8> },
    ReplayPacket(Vec<u8>),
    DownstreamSend { stream: Stream, data: Vec<u8> },
    DownstreamClose(Stream),
    PauseDownstream(Stream),
    ResumeDownstream(Stream),
    UpstreamConnect { port: u16 },
    UpstreamSend { stream: Stream, data: Vec<u8> },
    UpstreamClose(Stream),
    PauseUpstream(Stream),
    ResumeUpstream(Stream),
}

/// Trait that all activators implement for native testing.
pub trait Activator {
    fn process_events(&mut self, events: Vec<Event>) -> Vec<Action>;
}
