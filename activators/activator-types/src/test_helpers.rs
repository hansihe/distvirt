//! Test helpers for activator native tests and WASM integration tests.
//!
//! Provides packet factories, H2 frame builders, and frame parsers.

use crate::{IpProtocol, PacketInfo};

// --- Packet helpers ---

/// Create a TCP packet with the given flow, flags, and raw frame data.
pub fn make_tcp_packet(flow: u64, tcp_flags: u8, raw_frame: Vec<u8>) -> PacketInfo {
    PacketInfo {
        flow,
        src_addr: vec![10, 0, 0, 1],
        dst_addr: vec![10, 0, 0, 2],
        src_port: 12345,
        dst_port: 80,
        protocol: IpProtocol::Tcp,
        tcp_flags: Some(tcp_flags),
        payload_len: 0,
        raw_frame,
    }
}

/// Create a SYN packet (tcp_flags = 0x02).
pub fn make_syn_packet(flow: u64, raw_frame: Vec<u8>) -> PacketInfo {
    make_tcp_packet(flow, 0x02, raw_frame)
}

// --- H2 frame builders ---

/// H2 connection preface (client sends this before anything else).
pub const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// Build a raw H2 frame.
pub fn h2_frame(frame_type: u8, flags: u8, stream_id: u32, payload: &[u8]) -> Vec<u8> {
    let len = payload.len();
    let mut buf = Vec::with_capacity(9 + len);
    buf.push((len >> 16) as u8);
    buf.push((len >> 8) as u8);
    buf.push(len as u8);
    buf.push(frame_type);
    buf.push(flags);
    let sid = stream_id & 0x7fff_ffff;
    buf.extend_from_slice(&sid.to_be_bytes());
    buf.extend_from_slice(payload);
    buf
}

/// Build a SETTINGS frame with given (id, value) pairs.
pub fn h2_settings(params: &[(u16, u32)]) -> Vec<u8> {
    let mut payload = Vec::new();
    for &(id, val) in params {
        payload.extend_from_slice(&id.to_be_bytes());
        payload.extend_from_slice(&val.to_be_bytes());
    }
    h2_frame(0x4, 0, 0, &payload)
}

/// Build a SETTINGS ACK frame.
pub fn h2_settings_ack() -> Vec<u8> {
    h2_frame(0x4, 0x01, 0, &[])
}

/// Build a HEADERS frame (with END_HEADERS flag set).
pub fn h2_headers(stream_id: u32, end_stream: bool, header_block: &[u8]) -> Vec<u8> {
    let flags = 0x04 | if end_stream { 0x01 } else { 0 }; // END_HEADERS | END_STREAM
    h2_frame(0x1, flags, stream_id, header_block)
}

/// Build a DATA frame.
pub fn h2_data(stream_id: u32, end_stream: bool, data: &[u8]) -> Vec<u8> {
    let flags = if end_stream { 0x01 } else { 0 };
    h2_frame(0x0, flags, stream_id, data)
}

/// Build a PING frame.
pub fn h2_ping(opaque: &[u8; 8]) -> Vec<u8> {
    h2_frame(0x6, 0, 0, opaque)
}

/// Build a WINDOW_UPDATE frame.
pub fn h2_window_update(stream_id: u32, increment: u32) -> Vec<u8> {
    let val = increment & 0x7fff_ffff;
    h2_frame(0x8, 0, stream_id, &val.to_be_bytes())
}

/// Build a RST_STREAM frame.
pub fn h2_rst_stream(stream_id: u32, error_code: u32) -> Vec<u8> {
    h2_frame(0x3, 0, stream_id, &error_code.to_be_bytes())
}

// --- H2 frame parsers ---

/// Parse a SETTINGS frame payload. Returns list of (id, value) pairs.
pub fn parse_settings_payload(payload: &[u8]) -> Vec<(u16, u32)> {
    let mut result = Vec::new();
    let mut i = 0;
    while i + 6 <= payload.len() {
        let id = u16::from_be_bytes([payload[i], payload[i + 1]]);
        let val =
            u32::from_be_bytes([payload[i + 2], payload[i + 3], payload[i + 4], payload[i + 5]]);
        result.push((id, val));
        i += 6;
    }
    result
}

/// Parse frame header, returns (length, type, flags, stream_id).
pub fn parse_h2_frame_header(buf: &[u8]) -> (usize, u8, u8, u32) {
    let length = ((buf[0] as usize) << 16) | ((buf[1] as usize) << 8) | (buf[2] as usize);
    let frame_type = buf[3];
    let flags = buf[4];
    let stream_id = u32::from_be_bytes([buf[5] & 0x7f, buf[6], buf[7], buf[8]]);
    (length, frame_type, flags, stream_id)
}

/// Collect all frames from a concatenated byte buffer.
/// Returns Vec of (frame_type, flags, stream_id, payload).
pub fn collect_frames(data: &[u8]) -> Vec<(u8, u8, u32, Vec<u8>)> {
    let mut frames = Vec::new();
    let mut pos = 0;
    while pos + 9 <= data.len() {
        let (length, ftype, flags, stream_id) = parse_h2_frame_header(&data[pos..]);
        let payload = data[pos + 9..pos + 9 + length].to_vec();
        frames.push((ftype, flags, stream_id, payload));
        pos += 9 + length;
    }
    frames
}

// --- Action helpers ---

use crate::Action;

/// Extract all downstream-send data for a given stream from actions.
pub fn downstream_data_for(actions: &[Action], stream: u64) -> Vec<u8> {
    let mut result = Vec::new();
    for action in actions {
        if let Action::DownstreamSend {
            stream: s,
            data,
        } = action
        {
            if *s == stream {
                result.extend_from_slice(data);
            }
        }
    }
    result
}

/// Extract all upstream-send data for a given stream from actions.
pub fn upstream_data_for(actions: &[Action], stream: u64) -> Vec<u8> {
    let mut result = Vec::new();
    for action in actions {
        if let Action::UpstreamSend {
            stream: s,
            data,
        } = action
        {
            if *s == stream {
                result.extend_from_slice(data);
            }
        }
    }
    result
}
