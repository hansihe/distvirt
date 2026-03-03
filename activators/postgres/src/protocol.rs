/// Postgres wire protocol message parsing.
///
/// After the initial startup phase, every message has the format:
///   - 1-byte type tag
///   - 4-byte length (big-endian, inclusive of the length field itself)
///   - (length - 4) bytes of payload
///
/// Startup messages (client→backend, first message) lack the type tag:
///   - 4-byte length (inclusive)
///   - payload (protocol version + parameters, or SSLRequest magic)

/// SSLRequest magic: length=8, code=80877103
pub const SSL_REQUEST_CODE: u32 = 80877103;

/// Protocol version 3.0 = 196608
pub const PROTOCOL_VERSION_3_0: u32 = 196608;

/// Backend message tags we care about.
pub const MSG_READY_FOR_QUERY: u8 = b'Z';
pub const MSG_AUTHENTICATION: u8 = b'R';
pub const MSG_BACKEND_KEY_DATA: u8 = b'K';
pub const MSG_COMMAND_COMPLETE: u8 = b'C';

/// Frontend message tags we care about.
pub const MSG_QUERY: u8 = b'Q';
pub const MSG_PARSE: u8 = b'P';
pub const MSG_BIND: u8 = b'B';
pub const MSG_EXECUTE: u8 = b'E';
pub const MSG_SYNC: u8 = b'S';
pub const MSG_TERMINATE: u8 = b'X';

/// Transaction status indicators from ReadyForQuery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    /// `I` — idle, no transaction
    Idle,
    /// `T` — in a transaction block
    InTransaction,
    /// `E` — in a failed transaction block
    Failed,
}

impl TransactionStatus {
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            b'I' => Some(Self::Idle),
            b'T' => Some(Self::InTransaction),
            b'E' => Some(Self::Failed),
            _ => None,
        }
    }
}

/// A parsed startup message from the client (no type tag).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupMessage {
    /// SSLRequest — client wants to negotiate SSL.
    SslRequest,
    /// Regular StartupMessage with protocol version 3.0.
    Startup {
        /// Raw bytes of the entire startup message (length prefix + payload).
        raw: Vec<u8>,
    },
}

/// Try to parse a startup message from the buffer.
/// Returns `Some((message, bytes_consumed))` if a complete message is available.
pub fn parse_startup_message(buf: &[u8]) -> Option<(StartupMessage, usize)> {
    if buf.len() < 4 {
        return None;
    }

    let length = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;

    // Minimum valid length is 8 (length field + version/code)
    if length < 8 {
        return None;
    }

    if buf.len() < length {
        return None;
    }

    let code = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);

    if code == SSL_REQUEST_CODE {
        Some((StartupMessage::SslRequest, length))
    } else {
        Some((
            StartupMessage::Startup {
                raw: buf[..length].to_vec(),
            },
            length,
        ))
    }
}

/// Try to parse a regular (tagged) Postgres message from the buffer.
/// Format: 1-byte tag + 4-byte length (inclusive) + payload.
/// Returns `Some((tag, payload_slice_range, total_consumed))`.
pub fn parse_tagged_message(buf: &[u8]) -> Option<(u8, usize)> {
    if buf.len() < 5 {
        return None;
    }

    let tag = buf[0];
    let length = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;

    // Length includes itself (4 bytes) but not the tag byte
    let total = 1 + length;

    if buf.len() < total {
        return None;
    }

    Some((tag, total))
}

/// Extract the transaction status byte from a ReadyForQuery message payload.
/// The message body (after tag + length) is exactly 1 byte: the status indicator.
pub fn parse_ready_for_query_status(msg_bytes: &[u8]) -> Option<TransactionStatus> {
    // msg_bytes is the full message: tag(1) + length(4) + status(1) = 6 bytes
    if msg_bytes.len() < 6 {
        return None;
    }
    if msg_bytes[0] != MSG_READY_FOR_QUERY {
        return None;
    }
    TransactionStatus::from_byte(msg_bytes[5])
}

/// Check if a frontend message tag represents query activity.
pub fn is_activity_message(tag: u8) -> bool {
    matches!(tag, MSG_QUERY | MSG_PARSE | MSG_BIND | MSG_EXECUTE | MSG_SYNC)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ssl_request() {
        // SSLRequest: length=8, code=80877103
        let mut buf = Vec::new();
        buf.extend_from_slice(&8u32.to_be_bytes());
        buf.extend_from_slice(&SSL_REQUEST_CODE.to_be_bytes());

        let (msg, consumed) = parse_startup_message(&buf).unwrap();
        assert_eq!(consumed, 8);
        assert_eq!(msg, StartupMessage::SslRequest);
    }

    #[test]
    fn test_parse_startup_message() {
        // StartupMessage: length + version(3.0) + "user\0test\0\0"
        let params = b"user\0test\0\0";
        let length = (4 + 4 + params.len()) as u32;
        let mut buf = Vec::new();
        buf.extend_from_slice(&length.to_be_bytes());
        buf.extend_from_slice(&PROTOCOL_VERSION_3_0.to_be_bytes());
        buf.extend_from_slice(params);

        let (msg, consumed) = parse_startup_message(&buf).unwrap();
        assert_eq!(consumed, length as usize);
        match msg {
            StartupMessage::Startup { raw } => {
                assert_eq!(raw.len(), length as usize);
            }
            _ => panic!("expected Startup"),
        }
    }

    #[test]
    fn test_parse_startup_message_incomplete() {
        // Only 3 bytes — not enough for length
        assert!(parse_startup_message(&[0, 0, 0]).is_none());

        // Length says 16 but only 8 bytes available
        let mut buf = Vec::new();
        buf.extend_from_slice(&16u32.to_be_bytes());
        buf.extend_from_slice(&PROTOCOL_VERSION_3_0.to_be_bytes());
        assert!(parse_startup_message(&buf).is_none());
    }

    #[test]
    fn test_parse_tagged_message() {
        // ReadyForQuery: tag='Z', length=5 (4+1), status='I'
        let msg = [b'Z', 0, 0, 0, 5, b'I'];
        let (tag, consumed) = parse_tagged_message(&msg).unwrap();
        assert_eq!(tag, b'Z');
        assert_eq!(consumed, 6);
    }

    #[test]
    fn test_parse_tagged_message_incomplete() {
        // Only tag + partial length
        assert!(parse_tagged_message(&[b'Z', 0, 0]).is_none());

        // Length says 10 but only 6 bytes total
        assert!(parse_tagged_message(&[b'Q', 0, 0, 0, 10, 0]).is_none());
    }

    #[test]
    fn test_parse_ready_for_query_status() {
        let idle = [b'Z', 0, 0, 0, 5, b'I'];
        assert_eq!(
            parse_ready_for_query_status(&idle),
            Some(TransactionStatus::Idle)
        );

        let in_txn = [b'Z', 0, 0, 0, 5, b'T'];
        assert_eq!(
            parse_ready_for_query_status(&in_txn),
            Some(TransactionStatus::InTransaction)
        );

        let failed = [b'Z', 0, 0, 0, 5, b'E'];
        assert_eq!(
            parse_ready_for_query_status(&failed),
            Some(TransactionStatus::Failed)
        );

        // Wrong tag
        let wrong = [b'Q', 0, 0, 0, 5, b'I'];
        assert_eq!(parse_ready_for_query_status(&wrong), None);

        // Too short
        assert_eq!(parse_ready_for_query_status(&[b'Z', 0, 0, 0, 5]), None);
    }

    #[test]
    fn test_is_activity_message() {
        assert!(is_activity_message(MSG_QUERY));
        assert!(is_activity_message(MSG_PARSE));
        assert!(is_activity_message(MSG_BIND));
        assert!(is_activity_message(MSG_EXECUTE));
        assert!(is_activity_message(MSG_SYNC));
        assert!(!is_activity_message(MSG_TERMINATE));
        assert!(!is_activity_message(MSG_READY_FOR_QUERY));
    }

    #[test]
    fn test_parse_multiple_tagged_messages() {
        // Two messages concatenated: Query + Sync
        let mut buf = Vec::new();
        // Query: tag='Q', length=10 (4 + 6 bytes payload)
        buf.push(b'Q');
        buf.extend_from_slice(&10u32.to_be_bytes());
        buf.extend_from_slice(b"SELECT");
        // Sync: tag='S', length=4 (no payload)
        buf.push(b'S');
        buf.extend_from_slice(&4u32.to_be_bytes());

        let (tag1, consumed1) = parse_tagged_message(&buf).unwrap();
        assert_eq!(tag1, b'Q');
        assert_eq!(consumed1, 11);

        let (tag2, consumed2) = parse_tagged_message(&buf[consumed1..]).unwrap();
        assert_eq!(tag2, b'S');
        assert_eq!(consumed2, 5);
    }

    #[test]
    fn test_transaction_status_from_byte() {
        assert_eq!(TransactionStatus::from_byte(b'I'), Some(TransactionStatus::Idle));
        assert_eq!(TransactionStatus::from_byte(b'T'), Some(TransactionStatus::InTransaction));
        assert_eq!(TransactionStatus::from_byte(b'E'), Some(TransactionStatus::Failed));
        assert_eq!(TransactionStatus::from_byte(b'X'), None);
    }
}
