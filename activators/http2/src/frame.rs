/// H2 frame types (RFC 7540 Section 6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameType {
    Data = 0x0,
    Headers = 0x1,
    Priority = 0x2,
    RstStream = 0x3,
    Settings = 0x4,
    PushPromise = 0x5,
    Ping = 0x6,
    Goaway = 0x7,
    WindowUpdate = 0x8,
    Continuation = 0x9,
    Unknown(u8),
}

// We need a custom representation since Unknown carries data.
impl FrameType {
    pub fn from_u8(v: u8) -> Self {
        match v {
            0x0 => Self::Data,
            0x1 => Self::Headers,
            0x2 => Self::Priority,
            0x3 => Self::RstStream,
            0x4 => Self::Settings,
            0x5 => Self::PushPromise,
            0x6 => Self::Ping,
            0x7 => Self::Goaway,
            0x8 => Self::WindowUpdate,
            0x9 => Self::Continuation,
            other => Self::Unknown(other),
        }
    }

    pub fn to_u8(self) -> u8 {
        match self {
            Self::Data => 0x0,
            Self::Headers => 0x1,
            Self::Priority => 0x2,
            Self::RstStream => 0x3,
            Self::Settings => 0x4,
            Self::PushPromise => 0x5,
            Self::Ping => 0x6,
            Self::Goaway => 0x7,
            Self::WindowUpdate => 0x8,
            Self::Continuation => 0x9,
            Self::Unknown(v) => v,
        }
    }
}

// Flag constants
pub const FLAG_END_STREAM: u8 = 0x01;
pub const FLAG_ACK: u8 = 0x01;
pub const FLAG_END_HEADERS: u8 = 0x04;
pub const FLAG_PADDED: u8 = 0x08;
pub const FLAG_PRIORITY: u8 = 0x20;

// H2 error codes
pub const ERROR_NO_ERROR: u32 = 0x0;
pub const _ERROR_PROTOCOL_ERROR: u32 = 0x1;
pub const ERROR_INTERNAL_ERROR: u32 = 0x2;
pub const _ERROR_FLOW_CONTROL_ERROR: u32 = 0x3;
pub const _ERROR_FRAME_SIZE_ERROR: u32 = 0x6;
pub const _ERROR_ENHANCE_YOUR_CALM: u32 = 0xb;

// H2 settings identifiers
pub const SETTINGS_HEADER_TABLE_SIZE: u16 = 0x1;
pub const SETTINGS_ENABLE_PUSH: u16 = 0x2;
pub const SETTINGS_MAX_CONCURRENT_STREAMS: u16 = 0x3;
pub const SETTINGS_INITIAL_WINDOW_SIZE: u16 = 0x4;
pub const SETTINGS_MAX_FRAME_SIZE: u16 = 0x5;

pub const FRAME_HEADER_LEN: usize = 9;

/// H2 connection preface (client sends this before anything else).
pub const CLIENT_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// Parsed H2 frame (borrowed payload).
#[derive(Debug)]
pub struct Frame<'a> {
    pub frame_type: FrameType,
    pub flags: u8,
    pub stream_id: u32,
    pub payload: &'a [u8],
}

/// Parse a single H2 frame from a buffer.
/// Returns `Some((frame, total_consumed))` or `None` if incomplete.
pub fn parse_frame(buf: &[u8]) -> Option<(Frame<'_>, usize)> {
    if buf.len() < FRAME_HEADER_LEN {
        return None;
    }

    let length = ((buf[0] as u32) << 16 | (buf[1] as u32) << 8 | (buf[2] as u32)) as usize;
    let frame_type = FrameType::from_u8(buf[3]);
    let flags = buf[4];
    let stream_id = u32::from_be_bytes([buf[5] & 0x7f, buf[6], buf[7], buf[8]]);

    let total = FRAME_HEADER_LEN + length;
    if buf.len() < total {
        return None;
    }

    let payload = &buf[FRAME_HEADER_LEN..total];
    Some((
        Frame {
            frame_type,
            flags,
            stream_id,
            payload,
        },
        total,
    ))
}

/// Serialize a frame into bytes.
pub fn serialize_frame(frame_type: FrameType, flags: u8, stream_id: u32, payload: &[u8]) -> Vec<u8> {
    let length = payload.len();
    let mut buf = Vec::with_capacity(FRAME_HEADER_LEN + length);
    buf.push((length >> 16) as u8);
    buf.push((length >> 8) as u8);
    buf.push(length as u8);
    buf.push(frame_type.to_u8());
    buf.push(flags);
    let sid = stream_id & 0x7fff_ffff;
    buf.extend_from_slice(&sid.to_be_bytes());
    buf.extend_from_slice(payload);
    buf
}

/// Build a SETTINGS frame with the given parameters.
pub fn build_settings(params: &[(u16, u32)]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(params.len() * 6);
    for &(id, value) in params {
        payload.extend_from_slice(&id.to_be_bytes());
        payload.extend_from_slice(&value.to_be_bytes());
    }
    serialize_frame(FrameType::Settings, 0, 0, &payload)
}

/// Build a SETTINGS ACK frame.
pub fn build_settings_ack() -> Vec<u8> {
    serialize_frame(FrameType::Settings, FLAG_ACK, 0, &[])
}

/// Build a PING ACK frame echoing the 8 opaque bytes.
pub fn build_ping_ack(opaque: &[u8; 8]) -> Vec<u8> {
    serialize_frame(FrameType::Ping, FLAG_ACK, 0, opaque)
}

/// Build a WINDOW_UPDATE frame.
pub fn build_window_update(stream_id: u32, increment: u32) -> Vec<u8> {
    let val = increment & 0x7fff_ffff;
    serialize_frame(FrameType::WindowUpdate, 0, stream_id, &val.to_be_bytes())
}

/// Build a GOAWAY frame.
pub fn build_goaway(last_stream_id: u32, error_code: u32) -> Vec<u8> {
    let mut payload = Vec::with_capacity(8);
    payload.extend_from_slice(&(last_stream_id & 0x7fff_ffff).to_be_bytes());
    payload.extend_from_slice(&error_code.to_be_bytes());
    serialize_frame(FrameType::Goaway, 0, 0, &payload)
}

/// Our SETTINGS parameters to advertise to both client and backend.
pub fn our_settings() -> Vec<u8> {
    build_settings(&[
        (SETTINGS_HEADER_TABLE_SIZE, 0),
        (SETTINGS_ENABLE_PUSH, 0),
        (SETTINGS_MAX_CONCURRENT_STREAMS, 100),
        (SETTINGS_INITIAL_WINDOW_SIZE, 65535),
        (SETTINGS_MAX_FRAME_SIZE, 16384),
    ])
}
