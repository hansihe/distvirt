//! Rust-side event/action types mirroring the WIT interface.
//!
//! Re-exports portable types from `activator_types` and adds host-specific
//! types that use `std::net::IpAddr`.

use std::net::IpAddr;

// Re-export all shared types.
pub use activator_types::{
    Action, Activator, BackendNeed, IpProtocol, LogAction, LogLevel, PacketDecision, PacketFlow,
    Stream,
};

/// Host-side packet info using `IpAddr` for addresses.
///
/// This wraps the portable `activator_types::PacketInfo` with `IpAddr` fields
/// for ergonomic use on the host side.
#[derive(Debug, Clone)]
pub struct PacketInfo {
    pub flow: PacketFlow,
    pub src_addr: IpAddr,
    pub dst_addr: IpAddr,
    pub src_port: u16,
    pub dst_port: u16,
    pub protocol: IpProtocol,
    pub tcp_flags: Option<u8>,
    pub payload_len: usize,
    pub raw_frame: Vec<u8>,
}

/// Host-side events using `IpAddr`-based `PacketInfo`.
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

impl Event {
    /// Convert host Event to portable activator_types::Event.
    pub fn to_shared(&self) -> activator_types::Event {
        match self {
            Event::BackendAvailable(b) => activator_types::Event::BackendAvailable(*b),
            Event::Tick => activator_types::Event::Tick,
            Event::Packet(info) => {
                let src_addr = match info.src_addr {
                    IpAddr::V4(v4) => v4.octets().to_vec(),
                    IpAddr::V6(v6) => v6.octets().to_vec(),
                };
                let dst_addr = match info.dst_addr {
                    IpAddr::V4(v4) => v4.octets().to_vec(),
                    IpAddr::V6(v6) => v6.octets().to_vec(),
                };
                activator_types::Event::Packet(activator_types::PacketInfo {
                    flow: info.flow,
                    src_addr,
                    dst_addr,
                    src_port: info.src_port,
                    dst_port: info.dst_port,
                    protocol: info.protocol,
                    tcp_flags: info.tcp_flags,
                    payload_len: info.payload_len,
                    raw_frame: info.raw_frame.clone(),
                })
            }
            Event::StreamOpen(s) => activator_types::Event::StreamOpen(*s),
            Event::StreamData { stream, data } => activator_types::Event::StreamData {
                stream: *stream,
                data: data.clone(),
            },
            Event::StreamClose(s) => activator_types::Event::StreamClose(*s),
            Event::UpstreamConnectResult { stream, ok } => {
                activator_types::Event::UpstreamConnectResult {
                    stream: *stream,
                    ok: *ok,
                }
            }
            Event::UpstreamData { stream, data } => activator_types::Event::UpstreamData {
                stream: *stream,
                data: data.clone(),
            },
            Event::UpstreamClose(s) => activator_types::Event::UpstreamClose(*s),
        }
    }
}
